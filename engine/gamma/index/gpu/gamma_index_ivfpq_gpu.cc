/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "gamma_index_ivfpq_gpu.h"

#include <faiss/IndexFlat.h>
#include <faiss/gpu/GpuAutoTune.h>
#include <faiss/gpu/GpuCloner.h>
#include <faiss/gpu/GpuClonerOptions.h>
#include <faiss/gpu/StandardGpuResources.h>
#include <faiss/gpu/utils/DeviceUtils.h>
#include <faiss/utils/Heap.h>
#include <faiss/utils/utils.h>
#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <fstream>
#include <mutex>
#include <set>
#include <vector>
#include "bitmap.h"
#include "gamma_index_ivfpq.h"

using std::string;
using std::vector;

namespace tig_gamma {
namespace gamma_gpu {

namespace {
const int kMaxBatch = 200;       // max search batch num
const int kMaxRecallNum = 1024;  // max recall num
const int kMaxReqNum = 200;
}  // namespace

template <typename T>
class BlockingQueue {
 public:
  BlockingQueue() : mutex_(), condvar_(), queue_() {}

  void Put(const T &task) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      queue_.push_back(task);
    }
    condvar_.notify_all();
  }

  T Take() {
    std::unique_lock<std::mutex> lock(mutex_);
    condvar_.wait(lock, [this] { return !queue_.empty(); });
    assert(!queue_.empty());
    T front(queue_.front());
    queue_.pop_front();

    return front;
  }

  T TakeBatch(vector<T> &vec, int &n) {
    std::unique_lock<std::mutex> lock(mutex_);
    condvar_.wait(lock, [this] { return !queue_.empty(); });
    assert(!queue_.empty());

    int i = 0;
    while (!queue_.empty() && i < kMaxBatch) {
      T front(queue_.front());
      queue_.pop_front();
      vec[i++] = front;
    }
    n = i;

    return nullptr;
  }

  size_t Size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.size();
  }

 private:
  BlockingQueue(const BlockingQueue &);
  BlockingQueue &operator=(const BlockingQueue &);

 private:
  mutable std::mutex mutex_;
  std::condition_variable condvar_;
  std::list<T> queue_;
};

class GPUItem {
 public:
  GPUItem(int n, const float *x, int k, float *dis, long *label) : x_(x) {
    n_ = n;
    k_ = k;
    dis_ = dis;
    label_ = label;
    done_ = false;
  }

  void Notify() {
    done_ = true;
    cv_.notify_one();
  }

  int WaitForDone() {
    std::unique_lock<std::mutex> lck(mtx_);
    while (not done_) {
      cv_.wait_for(lck, std::chrono::seconds(1),
                   [this]() -> bool { return done_; });
    }
    return 0;
  }

  int n_;
  const float *x_;
  float *dis_;
  int k_;
  long *label_;

  int batch_size;  // for perfomance test

 private:
  std::condition_variable cv_;
  std::mutex mtx_;
  bool done_;
};

GammaIVFPQGPUIndex::GammaIVFPQGPUIndex(size_t d, size_t nlist, size_t M,
                                       size_t nbits_per_idx,
                                       const char *docids_bitmap,
                                       RawVector *raw_vec, int nprobe)
    : GammaIndex(d, docids_bitmap, raw_vec) {
  this->nlist_ = nlist;
  this->M_ = M;
  this->nbits_per_idx_ = nbits_per_idx;
  this->nprobe_ = nprobe;
  indexed_vec_count_ = 0;
  search_idx_ = 0;
  cur_qid_ = 0;
  b_exited_ = false;
  use_standard_resource_ = true;
  is_indexed_ = false;
#ifdef PERFORMANCE_TESTING
  search_count_ = 0;
#endif
}

GammaIVFPQGPUIndex::~GammaIVFPQGPUIndex() {
  b_exited_ = true;
  std::this_thread::sleep_for(std::chrono::seconds(2));
  delete gpu_index_;
}

faiss::Index *GammaIVFPQGPUIndex::CreateGPUIndex(faiss::Index *cpu_index) {
  int ngpus = faiss::gpu::getNumDevices();

  vector<int> gpus;
  for (int i = 0; i < ngpus; ++i) {
    gpus.push_back(i);
  }

  if (use_standard_resource_) {
    id_queues_.push_back(
        new moodycamel::BlockingConcurrentQueue<GPUItem *>);  // only use one
                                                              // queue
  } else {
    GammaMemManager manager;
    tmp_mem_num_ = manager.Init(ngpus);
    LOG(INFO) << "Resource num [" << tmp_mem_num_ << "]";

    for (int i = 0; i < tmp_mem_num_; ++i) {
      id_queues_.push_back(new moodycamel::BlockingConcurrentQueue<GPUItem *>);
    }
  }

  vector<int> devs;

  for (int i : gpus) {
    if (use_standard_resource_) {
      auto res = new faiss::gpu::StandardGpuResources;
      res->initializeForDevice(i);
      res->setTempMemory((size_t)1536 * 1024 * 1024);  // 1.5 GiB
      resources_.push_back(res);
    } else {
      auto res = new faiss::gpu::GammaGpuResources;
      res->initializeForDevice(i);
      resources_.push_back(res);
    }
    devs.push_back(i);
  }

  faiss::gpu::GpuMultipleClonerOptions *options =
      new faiss::gpu::GpuMultipleClonerOptions();

  options->indicesOptions = faiss::gpu::INDICES_32_BIT;
  options->useFloat16CoarseQuantizer = false;
  options->useFloat16 = true;
  options->usePrecomputed = false;
  options->reserveVecs = 0;
  options->storeTransposed = true;
  options->verbose = true;

  // shard the index across GPUs
  options->shard = true;
  options->shard_type = 1;

  faiss::Index *gpu_index = faiss::gpu::index_cpu_to_gpu_multiple(
      resources_, devs, cpu_index, options);
  return gpu_index;
}

int GammaIVFPQGPUIndex::CreateSearchThread() {
  auto func_search =
      std::bind(&GammaIVFPQGPUIndex::GPUThread, this, std::placeholders::_1);

  if (use_standard_resource_) {
    gpu_threads_.push_back(std::thread(func_search, id_queues_[0]));
    gpu_threads_[0].detach();
  } else {
    for (int i = 0; i < tmp_mem_num_; ++i) {
      gpu_threads_.push_back(std::thread(func_search, id_queues_[i]));
    }

    for (int i = 0; i < tmp_mem_num_; ++i) {
      gpu_threads_[i].detach();
    }
  }
  return 0;
}

int GammaIVFPQGPUIndex::Indexing() {
  int vectors_count = raw_vec_->GetVectorNum();
  if (vectors_count < 8192) {
    LOG(ERROR) << "vector total count [" << vectors_count
               << "] less then 8192, failed!";
    return -1;
  }

  if (is_indexed_) {
    while (indexed_vec_count_ < vectors_count) {
      AddRTVecsToIndex();
    }

    // dump index
    {
      faiss::Index *cpu_index = faiss::gpu::index_gpu_to_cpu(gpu_index_);
      string file_name = "gpu.index";
      faiss::write_index(cpu_index, file_name.c_str());
      LOG(INFO) << "GPUIndex dump successed!";
    }
    return 0;
  }

  faiss::IndexFlatL2 *quantizer = new faiss::IndexFlatL2(d_);
  faiss::IndexIVFPQ *cpu_index =
      new faiss::IndexIVFPQ(quantizer, d_, nlist_, M_, nbits_per_idx_);
  cpu_index->nprobe = nprobe_;

  gpu_index_ = CreateGPUIndex(dynamic_cast<faiss::Index *>(cpu_index));

  // gpu_index_->train(num, scope_vec.Get());

  std::vector<std::thread> workers;
  workers.push_back(std::thread([&]() {
    int num = vectors_count > 100000 ? 100000 : vectors_count;
    ScopeVector scope_vec;
    raw_vec_->GetVectorHeader(0, num, scope_vec);
    LOG(INFO) << num;
    gpu_index_->train(num, scope_vec.Get());
  }));

  std::for_each(workers.begin(), workers.end(),
                [](std::thread &t) { t.join(); });
  LOG(INFO) << "Index GPU successed!";

  while (indexed_vec_count_ < vectors_count) {
    AddRTVecsToIndex();
  }

  // dump index
  {
    faiss::Index *cpu_index = faiss::gpu::index_gpu_to_cpu(gpu_index_);
    string file_name = "gpu.index";
    faiss::write_index(cpu_index, file_name.c_str());
    LOG(INFO) << "GPUIndex dump successed!";
  }

  CreateSearchThread();

  is_indexed_ = true;
  return 0;
}

int GammaIVFPQGPUIndex::AddRTVecsToIndex() {
  int ret = 0;
  int total_stored_vecs = raw_vec_->GetVectorNum();
  if (indexed_vec_count_ > total_stored_vecs) {
    LOG(ERROR) << "internal error : indexed_vec_count should not greater than "
                  "total_stored_vecs!";
    ret = -1;
  } else if (indexed_vec_count_ == total_stored_vecs) {
    ;
#ifdef DEBUG_
    LOG(INFO) << "no extra vectors existed for indexing";
#endif
  } else {
    int MAX_NUM_PER_INDEX = 20480;
    int index_count =
        (total_stored_vecs - indexed_vec_count_) / MAX_NUM_PER_INDEX + 1;

    for (int i = 0; i < index_count; ++i) {
      int start_docid = indexed_vec_count_;
      int count_per_index =
          (i == (index_count - 1) ? total_stored_vecs - start_docid
                                  : MAX_NUM_PER_INDEX);
      ScopeVector vector_head;
      raw_vec_->GetVectorHeader(indexed_vec_count_,
                                indexed_vec_count_ + count_per_index,
                                vector_head);

      if (!Add(count_per_index, vector_head.Get())) {
        LOG(ERROR) << "add index from docid " << start_docid << " error!";
        ret = -2;
      }
    }
  }
  return ret;
}

bool GammaIVFPQGPUIndex::Add(int n, const float *vec) {
  // gpu_index_->add(n, vec);

  std::vector<std::thread> workers;
  workers.push_back(std::thread([&]() { gpu_index_->add(n, vec); }));
  std::for_each(workers.begin(), workers.end(),
                [](std::thread &t) { t.join(); });

  indexed_vec_count_ += n;
  LOG(INFO) << "Indexed vec count [" << indexed_vec_count_ << "]";
  return true;
}

int GammaIVFPQGPUIndex::Search(const VectorQuery *query,
                               const GammaSearchCondition *condition,
                               VectorResult &result) {
  if (gpu_threads_.size() == 0) {
    LOG(ERROR) << "gpu index not indexed!";
    return -1;
  }

  float *xq = reinterpret_cast<float *>(query->value->value);
  int n = query->value->len / (d_ * sizeof(float));
  if (n > kMaxReqNum) {
    LOG(ERROR) << "req num [" << n << "] should not larger than [" << kMaxReqNum
               << "]";
    return -1;
  }

  std::stringstream perf_ss;
#ifdef PERFORMANCE_TESTING
  double start = utils::getmillisecs();
#endif
  GPUSearch(n, xq, condition->topn, result.dists, result.docids, condition,
            perf_ss);

#ifdef PERFORMANCE_TESTING
  double gpu_search_end = utils::getmillisecs();
#endif

  for (int i = 0; i < n; i++) {
    int pos = 0;

    std::map<int, int> docid2count;
    for (int j = 0; j < condition->topn; j++) {
      long *docid = result.docids + i * condition->topn + j;
      if (docid[0] == -1) continue;
      int vector_id = (int)docid[0];
      int real_docid = this->raw_vec_->vid2docid_[vector_id];
      if (docid2count.find(real_docid) == docid2count.end()) {
        int real_pos = i * condition->topn + pos;
        result.docids[real_pos] = real_docid;
        int ret = this->raw_vec_->GetSource(vector_id, result.sources[real_pos],
                                            result.source_lens[real_pos]);
        if (ret != 0) {
          result.sources[real_pos] = nullptr;
          result.source_lens[real_pos] = 0;
        }
        result.dists[real_pos] = result.dists[i * condition->topn + j];

        pos++;
        docid2count[real_docid] = 1;
      }
    }

    if (pos > 0) {
      result.idx[i] = 0;  // init start id of seeking
    }

    for (; pos < condition->topn; pos++) {
      result.docids[i * condition->topn + pos] = -1;
      result.dists[i * condition->topn + pos] = -1;
    }
  }

#ifdef PERFORMANCE_TESTING
  double end = utils::getmillisecs();
  perf_ss << "reorder cost [" << end - gpu_search_end << "]ms, "
          << "total cost [" << end - start << "]ms ";
  LOG(INFO) << perf_ss.str();
#endif
  return 0;
}

long GammaIVFPQGPUIndex::GetTotalMemBytes() { return 0; }

int GammaIVFPQGPUIndex::Dump(const string &dir, int max_vid) {
  // faiss::Index *cpu_index = faiss::gpu::index_gpu_to_cpu(gpu_index_);
  // string file_name = dir + "/gpu.index";
  // faiss::write_index(cpu_index, file_name.c_str());
  return 0;
}

int GammaIVFPQGPUIndex::Load(const vector<string> &index_dirs) {
  // string file_name = index_dirs[index_dirs.size() - 1] + "/gpu.index";
  string file_name = "gpu.index";
  faiss::Index *cpu_index = faiss::read_index(file_name.c_str());
  gpu_index_ = CreateGPUIndex(cpu_index);

  indexed_vec_count_ = gpu_index_->ntotal;
  is_indexed_ = true;
  LOG(INFO) << "GPUIndex load successed, num [" << indexed_vec_count_ << "]";

  CreateSearchThread();
  return 0;
}

int GammaIVFPQGPUIndex::GPUThread(
    moodycamel::BlockingConcurrentQueue<GPUItem *> *items_q) {
  GammaMemManager manager;
  std::thread::id tid = std::this_thread::get_id();
  float *xx = new float[kMaxBatch * d_ * kMaxReqNum];
  long *label = new long[kMaxBatch * kMaxRecallNum * kMaxReqNum];
  float *dis = new float[kMaxBatch * kMaxRecallNum * kMaxReqNum];

  while (not b_exited_) {
    int size = 0;
    GPUItem *items[kMaxBatch];

    while (size == 0 && not b_exited_) {
      size = items_q->wait_dequeue_bulk_timed(items, kMaxBatch, 1000);
    }

    if (size > 1) {
      int max_recallnum = 0;
      int cur = 0, total = 0;

      for (int i = 0; i < size; ++i) {
        max_recallnum = std::max(max_recallnum, items[i]->k_);
        total += items[i]->n_;
        memcpy(xx + cur, items[i]->x_, d_ * sizeof(float) * items[i]->n_);
        cur += d_ * items[i]->n_;
      }

      gpu_index_->search(total, xx, max_recallnum, dis, label);

      cur = 0;
      for (int i = 0; i < size; ++i) {
        memcpy(items[i]->dis_, dis + cur,
               max_recallnum * sizeof(float) * items[i]->n_);
        memcpy(items[i]->label_, label + cur,
               max_recallnum * sizeof(long) * items[i]->n_);
        items[i]->batch_size = size;
        cur += max_recallnum * items[i]->n_;
        items[i]->Notify();
      }
    } else if (size == 1) {
      gpu_index_->search(items[0]->n_, items[0]->x_, items[0]->k_,
                         items[0]->dis_, items[0]->label_);
      items[0]->batch_size = size;
      items[0]->Notify();
    }
    if (not use_standard_resource_) {
      manager.ReturnMem(tid);
    }
  }

  delete xx;
  delete label;
  delete dis;
  LOG(INFO) << "thread exit";
  return 0;
}

int GammaIVFPQGPUIndex::GPUSearch(int n, const float *x, int k,
                                  float *distances, long *labels,
                                  const GammaSearchCondition *condition,
                                  std::stringstream &perf_ss) {
  auto recall_num = condition->recall_num;
  if (recall_num > kMaxRecallNum) {
    LOG(WARNING) << "recall_num should less than [" << kMaxRecallNum << "]";
    recall_num = kMaxRecallNum;
  }

  vector<float> D(n * kMaxRecallNum);
  vector<long> I(n * kMaxRecallNum);

  double start = utils::getmillisecs();
  GPUItem *item = new GPUItem(n, x, recall_num, D.data(), I.data());

  if (use_standard_resource_) {
    id_queues_[0]->enqueue(item);
  } else {
    int cur = ++cur_qid_;
    if (cur >= tmp_mem_num_) {
      cur = 0;
      cur_qid_ = 0;
    }

    id_queues_[cur]->enqueue(item);
  }

  item->WaitForDone();
  int batch_size = item->batch_size;

  delete item;

  double end1 = utils::getmillisecs();

  // set filter
  auto is_filterable = [this, condition](long docid) -> bool {
    auto *num = condition->range_query_result;

    return bitmap::test(docids_bitmap_, docid) || (num && not num->Has(docid));
  };

  using HeapForIP = faiss::CMin<float, idx_t>;
  using HeapForL2 = faiss::CMax<float, idx_t>;

  auto init_result = [&](int topk, float *simi, idx_t *idxi) {
    if (condition->metric_type == DistanceMetricType::InnerProduct) {
      faiss::heap_heapify<HeapForIP>(topk, simi, idxi);
    } else {
      faiss::heap_heapify<HeapForL2>(topk, simi, idxi);
    }
  };

  auto reorder_result = [&](int topk, float *simi, idx_t *idxi) {
    if (condition->metric_type == DistanceMetricType::InnerProduct) {
      faiss::heap_reorder<HeapForIP>(topk, simi, idxi);
    } else {
      faiss::heap_reorder<HeapForL2>(topk, simi, idxi);
    }
  };

  int raw_d = raw_vec_->GetDimension();

  std::function<void(const float **)> compute_vec;

  if (condition->has_rank) {
    compute_vec = [&](const float **vecs) {
      for (int i = 0; i < n; ++i) {
        const float *xi = x + i * d_;  // query

        float *simi = distances + i * k;
        long *idxi = labels + i * k;
        init_result(k, simi, idxi);

        for (int j = 0; j < recall_num; ++j) {
          long vid = I[i * recall_num + j];
          if (vid < 0) {
            continue;
          }

          int docid = raw_vec_->vid2docid_[vid];
          if (is_filterable(docid)) {
            continue;
          }

          float dist = -1;
          if (condition->metric_type == DistanceMetricType::InnerProduct) {
            dist =
                faiss::fvec_inner_product(xi, vecs[i * recall_num + j], raw_d);
          } else {
            dist = faiss::fvec_L2sqr(xi, vecs[i * recall_num + j], raw_d);
          }

          if (((condition->min_dist >= 0 && dist >= condition->min_dist) &&
               (condition->max_dist >= 0 && dist <= condition->max_dist)) ||
              (condition->min_dist == -1 && condition->max_dist == -1)) {
            if (condition->metric_type == DistanceMetricType::InnerProduct) {
              if (HeapForIP::cmp(simi[0], dist)) {
                faiss::heap_pop<HeapForIP>(k, simi, idxi);
                faiss::heap_push<HeapForIP>(k, simi, idxi, dist, vid);
              }
            } else {
              if (HeapForL2::cmp(simi[0], dist)) {
                faiss::heap_pop<HeapForL2>(k, simi, idxi);
                faiss::heap_push<HeapForL2>(k, simi, idxi, dist, vid);
              }
            }
          }
        }

        if (condition->sort_by_docid) {
          vector<std::pair<long, float>> id_sim_pairs(k);
          for (int z = 0; z < k; ++z) {
            id_sim_pairs[z] = std::move(std::make_pair(idxi[z], simi[z]));
          }
          std::sort(id_sim_pairs.begin(), id_sim_pairs.end());
          for (int z = 0; z < k; ++z) {
            idxi[z] = id_sim_pairs[z].first;
            simi[z] = id_sim_pairs[z].second;
          }
        } else {
          reorder_result(k, simi, idxi);
        }
      }  // parallel
    };
  } else {
    compute_vec = [&](const float **vecs) {
      for (int i = 0; i < n; ++i) {
        float *simi = distances + i * k;
        long *idxi = labels + i * k;

        memset(simi, -1, sizeof(float) * recall_num);
        memset(idxi, -1, sizeof(long) * recall_num);

        for (int j = 0; j < recall_num; ++j) {
          long vid = I[i * recall_num + j];
          if (vid < 0) {
            continue;
          }

          int docid = raw_vec_->vid2docid_[vid];
          if (is_filterable(docid)) {
            continue;
          }

          float dist = D[i * recall_num + j];

          if (((condition->min_dist >= 0 && dist >= condition->min_dist) &&
               (condition->max_dist >= 0 && dist <= condition->max_dist)) ||
              (condition->min_dist == -1 && condition->max_dist == -1)) {
            simi[j] = dist;
            idxi[j] = vid;
          }
        }
      }
    };
  }

  std::function<void()> compute_dis;

  if (condition->has_rank) {
    // calculate inner product for selected possible vectors
    compute_dis = [&]() {
      ScopeVectors scope_vecs(recall_num * n);
      raw_vec_->Gets(recall_num * n, I.data(), scope_vecs);

      const float **vecs = scope_vecs.Get();
      compute_vec(vecs);
    };
  } else {
    compute_dis = [&]() { compute_vec(nullptr); };
  }

  compute_dis();

#ifdef PERFORMANCE_TESTING
  double end_sort = utils::getmillisecs();
  perf_ss << "GPU cost [" << end1 - start << "]ms, batch size [" << batch_size
          << "] inner cost [" << end_sort - end1 << "]ms ";
#endif
  return 0;
}

}  // namespace gamma_gpu
}  // namespace tig_gamma
