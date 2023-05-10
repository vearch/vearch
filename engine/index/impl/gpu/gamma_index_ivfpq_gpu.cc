/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "gamma_index_ivfpq_gpu.h"

#include <faiss/IndexFlat.h>
#include <faiss/IndexShards.h>
#include <faiss/gpu/GpuAutoTune.h>
#include <faiss/gpu/GpuClonerOptions.h>
#include <faiss/gpu/GpuIndexIVFPQ.h>
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

#include "c_api/gamma_api.h"
#include "gamma_gpu_cloner.h"
#include "index/impl/gamma_index_ivfpq.h"
#include "util/bitmap.h"

using std::string;
using std::vector;

namespace tig_gamma {
namespace gamma_gpu {

static inline void ConvertVectorDim(size_t num, int raw_d, int d,
                                    const float *raw_vec, float *vec) {
  memset(vec, 0, num * d * sizeof(float));

#pragma omp parallel for
  for (size_t i = 0; i < num; ++i) {
    for (int j = 0; j < raw_d; ++j) {
      vec[i * d + j] = raw_vec[i * raw_d + j];
    }
  }
}

namespace {
const int kMaxBatch = 200;  // max search batch num
const int kMaxReqNum = 200;
const char *kDelim = "\001";
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
  GPUItem(int n, const float *x, int k, float *dis, long *label, int nprobe)
      : x_(x) {
    n_ = n;
    k_ = k;
    dis_ = dis;
    label_ = label;
    nprobe_ = nprobe;
    done_ = false;
    batch_size = 1;
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
  int nprobe_;

  int batch_size;  // for perfomance test

 private:
  std::condition_variable cv_;
  std::mutex mtx_;
  bool done_;
};

struct IVFPQModelParams {
  int ncentroids;     // coarse cluster center number
  int nsubvector;     // number of sub cluster center
  int nbits_per_idx;  // bit number of sub cluster center
  DistanceComputeType metric_type;

  IVFPQModelParams() {
    ncentroids = 2048;
    nsubvector = 64;
    nbits_per_idx = 8;
    metric_type = DistanceComputeType::INNER_PRODUCT;
  }

  int Parse(const char *str) {
    utils::JsonParser jp;
    if (jp.Parse(str)) {
      LOG(ERROR) << "parse IVFPQ retrieval parameters error: " << str;
      return -1;
    }

    int ncentroids;
    int nsubvector;
    int nbits_per_idx;

    // -1 as default
    if (!jp.GetInt("ncentroids", ncentroids)) {
      if (ncentroids < -1) {
        LOG(ERROR) << "invalid ncentroids =" << ncentroids;
        return -1;
      }
      if (ncentroids > 0) this->ncentroids = ncentroids;
    } else {
      LOG(ERROR) << "cannot get ncentroids for ivfpq, set it when create space";
      return -1;
    }

    if (!jp.GetInt("nsubvector", nsubvector)) {
      if (nsubvector < -1) {
        LOG(ERROR) << "invalid nsubvector =" << nsubvector;
        return -1;
      }
      if (nsubvector > 0) this->nsubvector = nsubvector;
    } else {
      LOG(ERROR) << "cannot get nsubvector for ivfpq, set it when create space";
      return -1;
    }

    if (!jp.GetInt("nbits_per_idx", nbits_per_idx)) {
      if (nbits_per_idx < -1) {
        LOG(ERROR) << "invalid nbits_per_idx =" << nbits_per_idx;
        return -1;
      }
      if (nbits_per_idx > 0) this->nbits_per_idx = nbits_per_idx;
    }

    std::string metric_type;

    if (!jp.GetString("metric_type", metric_type)) {
      if (strcasecmp("L2", metric_type.c_str()) &&
          strcasecmp("InnerProduct", metric_type.c_str())) {
        LOG(ERROR) << "invalid metric_type = " << metric_type;
        return -1;
      }
      if (!strcasecmp("L2", metric_type.c_str()))
        this->metric_type = DistanceComputeType::L2;
      else
        this->metric_type = DistanceComputeType::INNER_PRODUCT;
    }

    if (!Validate()) return -1;
    return 0;
  }

  bool Validate() {
    if (ncentroids <= 0 || nsubvector <= 0 || nbits_per_idx <= 0) return false;
    if (nsubvector % 4 != 0) {
      LOG(ERROR) << "only support multiple of 4 now, nsubvector=" << nsubvector;
      return false;
    }
    if (nbits_per_idx != 8) {
      LOG(ERROR) << "only support 8 now, nbits_per_idx=" << nbits_per_idx;
      return false;
    }
    return true;
  }

  std::string ToString() {
    std::stringstream ss;
    ss << "ncentroids =" << ncentroids << ", ";
    ss << "nsubvector =" << nsubvector << ", ";
    ss << "nbits_per_idx =" << nbits_per_idx;
    return ss.str();
  }
};

REGISTER_MODEL(GPU, GammaIVFPQGPUIndex)

GammaIVFPQGPUIndex::GammaIVFPQGPUIndex() : RetrievalModel() {
  M_ = 0;
  nbits_per_idx_ = 0;
  tmp_mem_num_ = 0;
  is_trained_ = false;
}

GammaIVFPQGPUIndex::~GammaIVFPQGPUIndex() {
  std::lock_guard<std::mutex> lock(indexing_mutex_);
  b_exited_ = true;
  std::this_thread::sleep_for(std::chrono::seconds(2));
  if (cpu_index_) {
    delete cpu_index_;
    cpu_index_ = nullptr;
  }

  if (gpu_index_) {
    delete gpu_index_;
    gpu_index_ = nullptr;
  }

  for (auto &resource : resources_) {
    delete resource;
    resource = nullptr;
  }
  resources_.clear();
}

int GammaIVFPQGPUIndex::Init(const std::string &model_parameters, int indexing_size) {
  IVFPQModelParams ivfpq_param;
  if (model_parameters != "" && ivfpq_param.Parse(model_parameters.c_str())) {
    return -1;
  }
  LOG(INFO) << ivfpq_param.ToString();
  int d = vector_->MetaInfo()->Dimension();

  if (d % ivfpq_param.nsubvector != 0) {
    this->d_ = (d / ivfpq_param.nsubvector + 1) * ivfpq_param.nsubvector;
    LOG(INFO) << "Dimension [" << vector_->MetaInfo()->Dimension()
              << "] cannot divide by nsubvector [" << ivfpq_param.nsubvector
              << "], adjusted to [" << d_ << "]";
  }

  this->nlist_ = ivfpq_param.ncentroids;
  this->M_ = ivfpq_param.nsubvector;
  this->nbits_per_idx_ = ivfpq_param.nbits_per_idx;
  this->nprobe_ = 80;

  metric_type_ = ivfpq_param.metric_type;

  b_exited_ = false;

  gpu_index_ = nullptr;
  cpu_index_ = new GammaIVFPQIndex();
  cpu_index_->vector_ = vector_;
  cpu_index_->Init(model_parameters, indexing_size);

#ifdef PERFORMANCE_TESTING
  search_count_ = 0;
#endif
  return 0;
}

RetrievalParameters *GammaIVFPQGPUIndex::Parse(const std::string &parameters) {
  if (parameters == "") {
    return new GPURetrievalParameters();
  }

  utils::JsonParser jp;
  if (jp.Parse(parameters.c_str())) {
    LOG(ERROR) << "parse retrieval parameters error: " << parameters;
    return nullptr;
  }

  std::string metric_type;
  GPURetrievalParameters *retrieval_params = new GPURetrievalParameters();
  if (!jp.GetString("metric_type", metric_type)) {
    if (strcasecmp("L2", metric_type.c_str()) &&
        strcasecmp("InnerProduct", metric_type.c_str())) {
      LOG(ERROR) << "invalid metric_type = " << metric_type
                 << ", so use default value.";
    }
    if (!strcasecmp("L2", metric_type.c_str()))
      retrieval_params->SetDistanceComputeType(DistanceComputeType::L2);
    else
      retrieval_params->SetDistanceComputeType(
          DistanceComputeType::INNER_PRODUCT);
  } else {
    retrieval_params->SetDistanceComputeType(metric_type_);
  }

  int recall_num;
  int nprobe;
  if (!jp.GetInt("recall_num", recall_num)) {
    if (recall_num > 0) {
      retrieval_params->SetRecallNum(recall_num);
    }
  }
  if (!jp.GetInt("nprobe", nprobe)) {
    if (nprobe > 0) {
      retrieval_params->SetNprobe(nprobe);
    }
  }
  return retrieval_params;
}

faiss::Index *GammaIVFPQGPUIndex::CreateGPUIndex() {
  int ngpus = faiss::gpu::getNumDevices();

  vector<int> devs;
  for (int i = 0; i < ngpus; ++i) {
    devs.push_back(i);
  }

  if (resources_.size() == 0) {
    for (int i : devs) {
      auto res = new faiss::gpu::StandardGpuResources;
      res->getResources()->initializeForDevice(i);
      res->setTempMemory((size_t)1536 * 1024 * 1024);  // 1.5 GiB
      resources_.push_back(res);
    }
  }

  faiss::gpu::GpuMultipleClonerOptions *options =
      new faiss::gpu::GpuMultipleClonerOptions();

  options->indicesOptions = faiss::gpu::INDICES_64_BIT;
  options->useFloat16CoarseQuantizer = false;
  options->useFloat16 = true;
  options->usePrecomputed = false;
  options->reserveVecs = 0;
  options->storeTransposed = true;
  options->verbose = true;

  // shard the index across GPUs
  options->shard = true;
  options->shard_type = 1;

  std::lock_guard<std::mutex> lock(cpu_mutex_);
  faiss::Index *gpu_index =
      gamma_index_cpu_to_gpu_multiple(resources_, devs, cpu_index_, options);

  delete options;
  return gpu_index;
}

int GammaIVFPQGPUIndex::CreateSearchThread() {
  auto func_search = std::bind(&GammaIVFPQGPUIndex::GPUThread, this);

  gpu_threads_.push_back(std::thread(func_search));
  gpu_threads_[0].detach();

  return 0;
}

int GammaIVFPQGPUIndex::Indexing() {
  std::lock_guard<std::mutex> lock(indexing_mutex_);

  LOG(INFO) << "GPU indexing";

  if (not is_trained_) {
    int ret = cpu_index_->Indexing();
    if (ret != 0) {
      return ret;
    }
    AddRTVecsToIndex();
    gpu_index_ = CreateGPUIndex();
    CreateSearchThread();
    is_trained_ = true;
    LOG(INFO) << "GPU indexed.";
    return 0;
  }

  if (gpu_threads_.size() == 0) {
    CreateSearchThread();
  }

  faiss::Index *index = CreateGPUIndex();

  auto old_index = gpu_index_;
  gpu_index_ = index;

  std::this_thread::sleep_for(std::chrono::seconds(2));
  delete old_index;
  LOG(INFO) << "GPU indexed.";
  return 0;
}

bool GammaIVFPQGPUIndex::Add(int n, const uint8_t *vec) {
  std::lock_guard<std::mutex> lock(indexing_mutex_);
  bool ret = cpu_index_->Add(n, vec);
  return ret;
}

int GammaIVFPQGPUIndex::AddRTVecsToIndex() {
  std::lock_guard<std::mutex> lock(cpu_mutex_);
  int ret = 0;
  RawVector *raw_vec = dynamic_cast<RawVector *>(cpu_index_->vector_);
  int total_stored_vecs = raw_vec->MetaInfo()->Size();
  if (cpu_index_->indexed_vec_count_ > total_stored_vecs) {
    LOG(ERROR) << "internal error : indexed_vec_count="
               << cpu_index_->indexed_vec_count_
               << " should not greater than total_stored_vecs="
               << total_stored_vecs;
    ret = -1;
  } else if (cpu_index_->indexed_vec_count_ == total_stored_vecs) {
#ifdef DEBUG
    LOG(INFO) << "no extra vectors existed for indexing";
#endif
    cpu_index_->rt_invert_index_ptr_->CompactIfNeed();
  } else {
    int MAX_NUM_PER_INDEX = 1000;
    int index_count = (total_stored_vecs - cpu_index_->indexed_vec_count_) /
                          MAX_NUM_PER_INDEX +
                      1;

    for (int i = 0; i < index_count; i++) {
      int start_docid = cpu_index_->indexed_vec_count_;
      size_t count_per_index =
          (i == (index_count - 1) ? total_stored_vecs - start_docid
                                  : MAX_NUM_PER_INDEX);

      ScopeVectors scope_vec;
      std::vector<int> lens;
      raw_vec->GetVectorHeader(start_docid, count_per_index, scope_vec, lens);

      const uint8_t *add_vec = nullptr;
      utils::ScopeDeleter1<uint8_t> del_vec;
      if (lens.size() == 1) {
        add_vec = scope_vec.Get(0);
      } else {
        int raw_d = raw_vec->MetaInfo()->Dimension();
        add_vec = new uint8_t[raw_d * count_per_index * sizeof(float)];
        del_vec.set(add_vec);
        size_t offset = 0;
        for (size_t i = 0; i < scope_vec.Size(); ++i) {
          memcpy((void *)(add_vec + offset), (void *)scope_vec.Get(i),
                 sizeof(float) * raw_d * lens[i]);
          offset += sizeof(float) * raw_d * lens[i];
        }
      }
      if (not cpu_index_->Add(count_per_index, add_vec)) {
        LOG(ERROR) << "add index from docid " << start_docid << " error!";
        ret = -2;
      }
    }
  }

  // warning: it's not recommended to access indexed_count_ and updated_vids_ in
  // sub-class
  this->indexed_count_ = cpu_index_->indexed_vec_count_;
  std::vector<int64_t> vids;
  int vid;
  while (this->updated_vids_.try_pop(vid)) {
    if (raw_vec->Bitmap()->Test(raw_vec->VidMgr()->VID2DocID(vid)))
      continue;
    if (vid >= this->indexed_count_) {
      this->updated_vids_.push(vid);
      break;
    } else {
      vids.push_back(vid);
    }  
    if (vids.size() >= 20000) break;
  }
  if (vids.size() == 0) return 0;
  ScopeVectors scope_vecs;
  if (raw_vec->Gets(vids, scope_vecs)) {
    LOG(ERROR) << "get update vector error!";
    ret = -3;
    return ret;
  }
  if (cpu_index_->Update(vids, scope_vecs.Get())) {
    LOG(ERROR) << "update index error!";
    ret = -4;
  }

  return ret;
}

int GammaIVFPQGPUIndex::Update(const std::vector<int64_t> &ids,
                               const std::vector<const uint8_t *> &vecs) {
  std::lock_guard<std::mutex> lock(indexing_mutex_);
  int ret = cpu_index_->Update(ids, vecs);
  return ret;
}

int GammaIVFPQGPUIndex::Delete(const std::vector<int64_t> &ids) {
  std::lock_guard<std::mutex> lock(indexing_mutex_);
  return cpu_index_->Delete(ids);
}

long GammaIVFPQGPUIndex::GetTotalMemBytes() {
  return cpu_index_->GetTotalMemBytes();
}

int GammaIVFPQGPUIndex::Dump(const string &dir) {
  return cpu_index_->Dump(dir);
}

int GammaIVFPQGPUIndex::Load(const string &index_dir) {
  int ret = cpu_index_->Load(index_dir);
  is_trained_ = cpu_index_->is_trained;
  d_ = cpu_index_->d_;
  metric_type_ = cpu_index_->metric_type_;
  return ret;
}

int GammaIVFPQGPUIndex::GPUThread() {
  std::thread::id tid = std::this_thread::get_id();
  float *xx = new float[kMaxBatch * d_ * kMaxReqNum];
  size_t max_recallnum = (size_t)faiss::gpu::getMaxKSelection();
  long *label = new long[kMaxBatch * max_recallnum * kMaxReqNum];
  float *dis = new float[kMaxBatch * max_recallnum * kMaxReqNum];

  while (not b_exited_) {
    int size = 0;
    GPUItem *items[kMaxBatch];

    while (size == 0 && not b_exited_) {
      size = id_queue_.wait_dequeue_bulk_timed(items, kMaxBatch, 1000);
    }

    if (size > 1) {
      std::map<int, std::vector<int>> nprobe_map;
      for (int i = 0; i < size; ++i) {
        nprobe_map[items[i]->nprobe_].push_back(i);
      }

      for (auto nprobe_ids : nprobe_map) {
        int recallnum = 0, cur = 0, total = 0;
        for (size_t j = 0; j < nprobe_ids.second.size(); ++j) {
          recallnum = std::max(recallnum, items[nprobe_ids.second[j]]->k_);
          total += items[nprobe_ids.second[j]]->n_;
          memcpy(xx + cur, items[nprobe_ids.second[j]]->x_,
                 d_ * sizeof(float) * items[nprobe_ids.second[j]]->n_);
          cur += d_ * items[nprobe_ids.second[j]]->n_;
        }

        int ngpus = faiss::gpu::getNumDevices();
        if (ngpus > 1) {
          auto indexShards = dynamic_cast<faiss::IndexShards *>(gpu_index_);
          if (indexShards != nullptr) {
            for (int j = 0; j < indexShards->count(); ++j) {
              auto ivfpq =
                  dynamic_cast<faiss::gpu::GpuIndexIVFPQ *>(indexShards->at(j));
              ivfpq->setNumProbes(nprobe_ids.first);
            }
          }
        } else {
          auto ivfpq = dynamic_cast<faiss::gpu::GpuIndexIVFPQ *>(gpu_index_);
          if (ivfpq != nullptr) ivfpq->setNumProbes(nprobe_ids.first);
        }
        gpu_index_->search(total, xx, recallnum, dis, label);

        cur = 0;
        for (size_t j = 0; j < nprobe_ids.second.size(); ++j) {
          memcpy(items[nprobe_ids.second[j]]->dis_, dis + cur,
                 recallnum * sizeof(float) * items[nprobe_ids.second[j]]->n_);
          memcpy(items[nprobe_ids.second[j]]->label_, label + cur,
                 recallnum * sizeof(long) * items[nprobe_ids.second[j]]->n_);
          items[nprobe_ids.second[j]]->batch_size = nprobe_ids.second.size();
          cur += recallnum * items[nprobe_ids.second[j]]->n_;
          items[nprobe_ids.second[j]]->Notify();
        }
      }
    } else if (size == 1) {
      int ngpus = faiss::gpu::getNumDevices();
      if (ngpus > 1) {
        auto indexShards = dynamic_cast<faiss::IndexShards *>(gpu_index_);
        if (indexShards != nullptr) {
          for (int j = 0; j < indexShards->count(); ++j) {
            auto ivfpq =
                dynamic_cast<faiss::gpu::GpuIndexIVFPQ *>(indexShards->at(j));
            ivfpq->setNumProbes(items[0]->nprobe_);
          }
        }
      } else {
        auto ivfpq = dynamic_cast<faiss::gpu::GpuIndexIVFPQ *>(gpu_index_);
        if (ivfpq != nullptr) ivfpq->setNumProbes(items[0]->nprobe_);
      }
      gpu_index_->search(items[0]->n_, items[0]->x_, items[0]->k_,
                         items[0]->dis_, items[0]->label_);
      items[0]->batch_size = size;
      items[0]->Notify();
    }
  }

  delete[] xx;
  delete[] label;
  delete[] dis;
  LOG(INFO) << "thread exit";
  return 0;
}

namespace {

int ParseFilters(GammaSearchCondition *condition,
                 vector<enum DataType> &range_filter_types,
                 vector<vector<string>> &all_term_items) {
  for (size_t i = 0; i < condition->range_filters.size(); ++i) {
    auto range = condition->range_filters[i];

    enum DataType type;
    if (condition->table->GetFieldType(range.field, type)) {
      LOG(ERROR) << "Can't get " << range.field << " data type";
      return -1;
    }

    if (type == DataType::STRING) {
      LOG(ERROR) << range.field << " can't be range filter";
      return -1;
    }
    range_filter_types[i] = type;
  }

  for (size_t i = 0; i < condition->term_filters.size(); ++i) {
    auto term = condition->term_filters[i];

    enum DataType type;
    if (condition->table->GetFieldType(term.field, type)) {
      LOG(ERROR) << "Can't get " << term.field << " data type";
      return -1;
    }

    if (type != DataType::STRING) {
      LOG(ERROR) << term.field << " can't be term filter";
      return -1;
    }

    vector<string> term_items = utils::split(term.value, kDelim);
    all_term_items[i] = term_items;
  }
  return 0;
}

template <class T>
bool IsInRange(Table *table, RangeFilter &range, long docid) {
  T value = 0;
  std::string field_value;
  int field_id = table->GetAttrIdx(range.field);
  table->GetFieldRawValue(docid, field_id, field_value);
  memcpy(&value, field_value.c_str(), sizeof(value));

  T lower_value, upper_value;
  memcpy(&lower_value, range.lower_value.c_str(), range.lower_value.size());
  memcpy(&upper_value, range.upper_value.c_str(), range.upper_value.size());

  if (range.include_lower != 0 && range.include_upper != 0) {
    if (value >= lower_value && value <= upper_value) return true;
  } else if (range.include_lower != 0 && range.include_upper == 0) {
    if (value >= lower_value && value < upper_value) return true;
  } else if (range.include_lower == 0 && range.include_upper != 0) {
    if (value > lower_value && value <= upper_value) return true;
  } else {
    if (value > lower_value && value < upper_value) return true;
  }
  return false;
}

bool FilteredByRangeFilter(GammaSearchCondition *condition,
                           vector<enum DataType> &range_filter_types,
                           long docid) {
  for (size_t i = 0; i < condition->range_filters.size(); ++i) {
    auto range = condition->range_filters[i];

    if (range_filter_types[i] == DataType::INT) {
      if (!IsInRange<int>(condition->table, range, docid)) return true;
    } else if (range_filter_types[i] == DataType::LONG) {
      if (!IsInRange<long>(condition->table, range, docid)) return true;
    } else if (range_filter_types[i] == DataType::FLOAT) {
      if (!IsInRange<float>(condition->table, range, docid)) return true;
    } else {
      if (!IsInRange<double>(condition->table, range, docid)) return true;
    }
  }
  return false;
}

bool FilteredByTermFilter(GammaSearchCondition *condition,
                          vector<vector<string>> &all_term_items, long docid) {
  for (size_t i = 0; i < condition->term_filters.size(); ++i) {
    auto term = condition->term_filters[i];

    std::string field_value;
    int field_id = condition->table->GetAttrIdx(term.field);
    condition->table->GetFieldRawValue(docid, field_id, field_value);
    vector<string> field_items;
    if (field_value.size() >= 0)
      field_items = utils::split(field_value, kDelim);

    bool all_in_field_items;
    if (term.is_union == static_cast<int>(FilterOperator::Or))
      all_in_field_items = false;
    else
      all_in_field_items = true;

    for (auto term_item : all_term_items[i]) {
      bool in_field_items = false;
      for (size_t j = 0; j < field_items.size(); j++) {
        if (term_item == field_items[j]) {
          in_field_items = true;
          break;
        }
      }
      if (term.is_union == static_cast<int>(FilterOperator::Or))
        all_in_field_items |= in_field_items;
      else
        all_in_field_items &= in_field_items;
    }
    if (!all_in_field_items) return true;
  }
  return false;
};

}  // namespace

int GammaIVFPQGPUIndex::Search(RetrievalContext *retrieval_context, int n,
                               const uint8_t *x, int k, float *distances,
                               long *labels) {
  if (gpu_threads_.size() == 0) {
    LOG(ERROR) << "gpu index not indexed!";
    return -1;
  }

  if (n > kMaxReqNum) {
    LOG(ERROR) << "req num [" << n << "] should not larger than [" << kMaxReqNum
               << "]";
    return -1;
  }

  GPURetrievalParameters *retrieval_params =
      dynamic_cast<GPURetrievalParameters *>(
          retrieval_context->RetrievalParams());
  utils::ScopeDeleter1<GPURetrievalParameters> del_params;
  if (retrieval_params == nullptr) {
    retrieval_params = new GPURetrievalParameters(metric_type_);
    del_params.set(retrieval_params);
  }
  const float *xq = reinterpret_cast<const float *>(x);
  if (xq == nullptr) {
    LOG(ERROR) << "search feature is null";
    return -1;
  }

  RawVector *raw_vec = dynamic_cast<RawVector *>(vector_);
  int raw_d = raw_vec->MetaInfo()->Dimension();
  const float *vec_q = nullptr;
  utils::ScopeDeleter1<float> del_vec_q;
  if (d_ > raw_d) {
    float *vec = new float[n * d_];
    ConvertVectorDim(n, raw_d, d_, xq, vec);
    vec_q = vec;
    del_vec_q.set(vec_q);
  } else {
    vec_q = xq;
  }

  int recall_num = retrieval_params->RecallNum();

  int max_recallnum = faiss::gpu::getMaxKSelection();
  if (recall_num > max_recallnum) {
    LOG(WARNING) << "recall_num should less than [" << max_recallnum << "]";
    recall_num = max_recallnum;
  } else if (recall_num < k) {
    LOG(WARNING) << "recall_num = " << recall_num
                 << " should't less than topK = " << k;
    recall_num = k;
  }

  int nprobe = this->nprobe_;
  if (retrieval_params->Nprobe() > 0 &&
      (size_t)retrieval_params->Nprobe() <= this->nlist_ &&
      retrieval_params->Nprobe() <= max_recallnum) {
    nprobe = retrieval_params->Nprobe();
  } else {
    LOG(WARNING) << "Error nprobe for search, so using default value:"
                 << this->nprobe_;
  }

  vector<float> D(n * max_recallnum);
  vector<long> I(n * max_recallnum);

#ifdef PERFORMANCE_TESTING
  retrieval_context->GetPerfTool().Perf("GPUSearch prepare");
#endif
  GPUItem *item = new GPUItem(n, vec_q, recall_num, D.data(), I.data(), nprobe);

  id_queue_.enqueue(item);

  item->WaitForDone();

  delete item;

#ifdef PERFORMANCE_TESTING
  retrieval_context->GetPerfTool().Perf("GPU thread");
#endif

  bool right_filter = false;
  GammaSearchCondition *condition =
      dynamic_cast<GammaSearchCondition *>(retrieval_context);

  vector<enum DataType> range_filter_types(condition->range_filters.size());

  vector<vector<string>> all_term_items(condition->term_filters.size());

  if (!ParseFilters(condition, range_filter_types, all_term_items)) {
    right_filter = true;
  }

  // set filter
  auto is_filterable = [&](long vid) -> bool {
    RawVector *raw_vec = dynamic_cast<RawVector *>(vector_);
    int docid = raw_vec->VidMgr()->VID2DocID(vid);
    return (retrieval_context->IsValid(vid) == false) ||
           (right_filter &&
            (FilteredByRangeFilter(condition, range_filter_types, docid) ||
             FilteredByTermFilter(condition, all_term_items, docid)));
  };

  using HeapForIP = faiss::CMin<float, idx_t>;
  using HeapForL2 = faiss::CMax<float, idx_t>;

  auto init_result = [&](int topk, float *simi, idx_t *idxi) {
    if (retrieval_params->GetDistanceComputeType() ==
        DistanceComputeType::INNER_PRODUCT) {
      faiss::heap_heapify<HeapForIP>(topk, simi, idxi);
    } else {
      faiss::heap_heapify<HeapForL2>(topk, simi, idxi);
    }
  };

  auto reorder_result = [&](int topk, float *simi, idx_t *idxi) {
    if (retrieval_params->GetDistanceComputeType() ==
        DistanceComputeType::INNER_PRODUCT) {
      faiss::heap_reorder<HeapForIP>(topk, simi, idxi);
    } else {
      faiss::heap_reorder<HeapForL2>(topk, simi, idxi);
    }
  };

  std::function<void(std::vector<const uint8_t *>)> compute_vec;

  if (condition->has_rank == true) {
    compute_vec = [&](std::vector<const uint8_t *> vecs) {
      for (int i = 0; i < n; ++i) {
        const float *xi = xq + i * d_;  // query

        float *simi = distances + i * k;
        long *idxi = labels + i * k;
        init_result(k, simi, idxi);

        for (int j = 0; j < recall_num; ++j) {
          long vid = I[i * recall_num + j];
          if (vid < 0) {
            continue;
          }

          if (is_filterable(vid) == true) {
            continue;
          }
          const float *vec =
              reinterpret_cast<const float *>(vecs[i * recall_num + j]);
          float dist = -1;
          if (retrieval_params->GetDistanceComputeType() ==
              DistanceComputeType::INNER_PRODUCT) {
            dist = faiss::fvec_inner_product(xi, vec, raw_d);
          } else {
            dist = faiss::fvec_L2sqr(xi, vec, raw_d);
          }

          if (retrieval_context->IsSimilarScoreValid(dist) == true) {
            if (retrieval_params->GetDistanceComputeType() ==
                DistanceComputeType::INNER_PRODUCT) {
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
        reorder_result(k, simi, idxi);
      }  // parallel
    };
  } else {
    compute_vec = [&](std::vector<const uint8_t *> vecs) {
      for (int i = 0; i < n; ++i) {
        float *simi = distances + i * k;
        long *idxi = labels + i * k;
        int idx = 0;
        memset(simi, -1, sizeof(float) * k);
        memset(idxi, -1, sizeof(long) * k);

        for (int j = 0; j < recall_num; ++j) {
          long vid = I[i * recall_num + j];
          if (vid < 0) {
            continue;
          }

          if (is_filterable(vid) == true) {
            continue;
          }

          float dist = D[i * recall_num + j];

          if (retrieval_context->IsSimilarScoreValid(dist) == true) {
            simi[idx] = dist;
            idxi[idx] = vid;
            idx++;
          }
          if (idx >= k) break;
        }
      }
    };
  }

  std::function<void()> compute_dis;

  if (condition->has_rank == true) {
    // calculate inner product for selected possible vectors
    compute_dis = [&]() {
      RawVector *raw_vec = dynamic_cast<RawVector *>(vector_);
      ScopeVectors scope_vecs;
      if (raw_vec->Gets(I, scope_vecs)) {
          LOG(ERROR) << "get raw vector error!";
          return;
      }
      compute_vec(scope_vecs.Get());
    };
  } else {
    compute_dis = [&]() {
      std::vector<const uint8_t *> vecs;
      compute_vec(vecs);
    };
  }

  compute_dis();

#ifdef PERFORMANCE_TESTING
  retrieval_context->GetPerfTool().Perf("reorder");
#endif
  return 0;
}

}  // namespace gamma_gpu
}  // namespace tig_gamma
