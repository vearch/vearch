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
#include <faiss/gpu/impl/IndexUtils.h>
#include <faiss/gpu/utils/DeviceUtils.h>
#include <faiss/utils/Heap.h>
#include <faiss/utils/utils.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <fstream>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <vector>

#include "c_api/gamma_api.h"
#include "gamma_gpu_cloner.h"
#include "index/impl/gamma_index_ivfpq.h"
#include "third_party/nlohmann/json.hpp"
#include "util/bitmap.h"

using std::string;
using std::vector;

namespace vearch {
namespace gpu {

namespace {
const int kMaxBatch = 200;  // max search batch num
const int kMaxReqNum = 200;
}  // namespace

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

  Status Parse(const char *str) {
    nlohmann::json j;
    try {
      j = nlohmann::json::parse(str);
    } catch (const nlohmann::json::parse_error &e) {
      LOG(ERROR) << "failed to parse IVFPQ retrieval parameters: " << e.what();
      return Status::ParamError("failed to parse IVFPQ retrieval parameters");
    }

    int ncentroids;
    int nsubvector;
    int nbits_per_idx;

    if (j.contains("ncentroids")) {
      ncentroids = j.value("ncentroids", 0);
      if (ncentroids <= 0) {
        std::string msg =
            std::string("invalid ncentroids =") + std::to_string(ncentroids);
        LOG(ERROR) << msg;
        return Status::ParamError(msg);
      }
      this->ncentroids = ncentroids;
    }

    if (j.contains("nsubvector")) {
      nsubvector = j.value("nsubvector", 0);
      if (nsubvector <= 0) {
        std::string msg =
            std::string("invalid nsubvector =") + std::to_string(nsubvector);
        LOG(ERROR) << msg;
        return Status::ParamError(msg);
      }
      this->nsubvector = nsubvector;
    }

    if (j.contains("nbits_per_idx")) {
      nbits_per_idx = j.value("nbits_per_idx", 0);
      if (nbits_per_idx <= 0) {
        std::string msg = std::string("invalid nbits_per_idx =") +
                          std::to_string(nbits_per_idx);
        LOG(ERROR) << msg;
        return Status::ParamError(msg);
      }
      this->nbits_per_idx = nbits_per_idx;
    }

    std::string metric_type;

    if (j.contains("metric_type")) {
      metric_type = j.value("metric_type", "");
      if (strcasecmp("L2", metric_type.c_str()) &&
          strcasecmp("InnerProduct", metric_type.c_str())) {
        std::string msg = std::string("invalid metric_type = ") + metric_type;
        LOG(ERROR) << msg;
        return Status::ParamError(msg);
      }
      if (!strcasecmp("L2", metric_type.c_str()))
        this->metric_type = DistanceComputeType::L2;
      else
        this->metric_type = DistanceComputeType::INNER_PRODUCT;
    }

    if (!Validate()) return Status::ParamError();
    return Status::OK();
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

REGISTER_INDEX(GPU_IVFPQ, GammaIVFPQGPUIndex)

GammaIVFPQGPUIndex::GammaIVFPQGPUIndex() {}

GammaIVFPQGPUIndex::~GammaIVFPQGPUIndex() {
  std::lock_guard<std::mutex> lock(indexing_mutex_);
  b_exited_ = true;
  std::this_thread::sleep_for(std::chrono::seconds(2));
  delete cpu_index_;
  cpu_index_ = nullptr;
  delete gpu_index_;
  gpu_index_ = nullptr;

  for (auto &resource : resources_) {
    delete resource;
    resource = nullptr;
  }
  resources_.clear();
}

Status GammaIVFPQGPUIndex::Init(const std::string &model_parameters,
                                int training_threshold) {
  IVFPQModelParams ivfpq_param;
  if (model_parameters != "") {
    Status status = ivfpq_param.Parse(model_parameters.c_str());
    if (!status.ok()) return status;
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
  this->nprobe_ = 80;

  metric_type_ = ivfpq_param.metric_type;

  b_exited_ = false;

  gpu_index_ = nullptr;
  cpu_index_ = new GammaIVFPQIndex();
  cpu_index_->vector_ = vector_;
  return cpu_index_->Init(model_parameters, training_threshold);
}

RetrievalParameters *GammaIVFPQGPUIndex::Parse(const std::string &parameters) {
  if (parameters == "") {
    return new GPURetrievalParameters();
  }

  nlohmann::json j;
  try {
    j = nlohmann::json::parse(parameters);
  } catch (const nlohmann::json::parse_error &e) {
    LOG(ERROR) << "failed to parse IVFPQ retrieval parameters: " << e.what();
    return nullptr;
  }

  std::string metric_type;
  GPURetrievalParameters *retrieval_params = new GPURetrievalParameters();
  if (j.contains("metric_type")) {
    metric_type = j.value("metric_type", "");
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
  if (j.contains("recall_num")) {
    recall_num = j.value("recall_num", 0);
    if (recall_num > 0) {
      retrieval_params->SetRecallNum(recall_num);
    }
  }
  if (j.contains("nprobe")) {
    nprobe = j.value("nprobe", 0);
    if (nprobe > 0) {
      retrieval_params->SetNprobe(nprobe);
    }
  }
  return retrieval_params;
}

faiss::Index *GammaIVFPQGPUIndex::CreateGPUIndex() {
  int ngpus = faiss::gpu::getNumDevices();
  LOG(INFO) << "number of GPUs available: " << ngpus;

  vector<int> devs;
  for (int i = 0; i < ngpus; ++i) {
    devs.push_back(i);
  }

  std::lock_guard<std::mutex> lock(cpu_mutex_);

  if (resources_.size() == 0) {
    for (int i : devs) {
      auto res = new faiss::gpu::StandardGpuResources;
      res->getResources()->initializeForDevice(i);
      res->setTempMemory((size_t)1536 * 1024 * 1024);  // 1.5 GiB
      resources_.push_back(res);
    }
  }

  faiss::gpu::GpuMultipleClonerOptions options;

  options.indicesOptions = faiss::gpu::INDICES_64_BIT;
  options.useFloat16CoarseQuantizer = false;
  options.useFloat16 = true;
  options.usePrecomputed = false;
  options.reserveVecs = 0;
  options.storeTransposed = true;
  options.verbose = true;

  // shard the index across GPUs
  options.shard = true;
  options.shard_type = 1;

  faiss::Index *gpu_index =
      gamma_index_cpu_to_gpu_multiple(resources_, devs, cpu_index_, &options);
  return gpu_index;
}

int GammaIVFPQGPUIndex::CreateSearchThread() {
  auto func_search = std::bind(&GammaIVFPQGPUIndex::GPUThread, this);

  gpu_threads_.push_back(std::thread(func_search));
  gpu_threads_.back().detach();
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

  {
    std::unique_lock<std::shared_mutex> lock(gpu_index_mutex_);
    delete gpu_index_;
    gpu_index_ = CreateGPUIndex();
  }

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
      int64_t start_docid = cpu_index_->indexed_vec_count_;
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
  int64_t vid;
  while (this->updated_vids_.try_pop(vid)) {
    if (raw_vec->Bitmap()->Test(vid)) continue;
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

Status GammaIVFPQGPUIndex::Dump(const string &dir) {
  return cpu_index_->Dump(dir);
}

Status GammaIVFPQGPUIndex::Load(const string &index_dir, int64_t &load_num) {
  Status ret = cpu_index_->Load(index_dir, load_num);
  is_trained_ = cpu_index_->is_trained;
  d_ = cpu_index_->d_;
  metric_type_ = cpu_index_->metric_type_;
  return ret;
}

int GammaIVFPQGPUIndex::GPUThread() {
  float *xx = new float[kMaxBatch * d_ * kMaxReqNum];
  size_t max_recallnum = (size_t)faiss::gpu::getMaxKSelection();
  LOG(INFO) << "max_recallnum: " << max_recallnum;
  long *label = new long[kMaxBatch * max_recallnum * kMaxReqNum];
  float *dis = new float[kMaxBatch * max_recallnum * kMaxReqNum];

  while (not b_exited_) {
    int size = 0;
    GPUSearchItem *items[kMaxBatch];

    while (size == 0 && not b_exited_) {
      size = search_queue_.wait_dequeue_bulk_timed(items, kMaxBatch, 1000);
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

        {
          std::shared_lock<std::shared_mutex> lock(gpu_index_mutex_);
          int ngpus = faiss::gpu::getNumDevices();
          if (ngpus > 1) {
            auto indexShards = dynamic_cast<faiss::IndexShards *>(gpu_index_);
            if (indexShards != nullptr) {
              for (int j = 0; j < indexShards->count(); ++j) {
                auto ivfpq = dynamic_cast<faiss::gpu::GpuIndexIVFPQ *>(
                    indexShards->at(j));
                if (ivfpq != nullptr) ivfpq->nprobe = nprobe_ids.first;
              }
            }
          } else {
            auto ivfpq = dynamic_cast<faiss::gpu::GpuIndexIVFPQ *>(gpu_index_);
            if (ivfpq != nullptr) ivfpq->nprobe = nprobe_ids.first;
          }
          gpu_index_->search(total, xx, recallnum, dis, label);
        }

        cur = 0;
        for (size_t j = 0; j < nprobe_ids.second.size(); ++j) {
          memcpy(items[nprobe_ids.second[j]]->dis_, dis + cur,
                 recallnum * sizeof(float) * items[nprobe_ids.second[j]]->n_);
          memcpy(items[nprobe_ids.second[j]]->label_, label + cur,
                 recallnum * sizeof(long) * items[nprobe_ids.second[j]]->n_);
          cur += recallnum * items[nprobe_ids.second[j]]->n_;
          items[nprobe_ids.second[j]]->Notify();
        }
      }
    } else if (size == 1) {
      {
        std::shared_lock<std::shared_mutex> lock(gpu_index_mutex_);
        int ngpus = faiss::gpu::getNumDevices();
        if (ngpus > 1) {
          auto indexShards = dynamic_cast<faiss::IndexShards *>(gpu_index_);
          if (indexShards != nullptr) {
            for (int j = 0; j < indexShards->count(); ++j) {
              auto ivfpq =
                  dynamic_cast<faiss::gpu::GpuIndexIVFPQ *>(indexShards->at(j));
              if (ivfpq != nullptr) ivfpq->nprobe = items[0]->nprobe_;
            }
          }
        } else {
          auto ivfpq = dynamic_cast<faiss::gpu::GpuIndexIVFPQ *>(gpu_index_);
          if (ivfpq != nullptr) ivfpq->nprobe = items[0]->nprobe_;
        }
        gpu_index_->search(items[0]->n_, items[0]->x_, items[0]->k_,
                           items[0]->dis_, items[0]->label_);
      }
      items[0]->Notify();
    }
  }

  delete[] xx;
  delete[] label;
  delete[] dis;
  LOG(INFO) << "thread exit";
  return 0;
}

int GammaIVFPQGPUIndex::Search(RetrievalContext *retrieval_context, int n,
                               const uint8_t *x, int k, float *distances,
                               long *labels) {
  return CommonSearch(retrieval_context, n, x, k, distances, labels, nprobe_,
                      nlist_, true);  // IVFPQ supports rerank
}

GPURetrievalParameters *GammaIVFPQGPUIndex::CreateDefaultRetrievalParams(
    int default_nprobe) {
  return new GPURetrievalParameters(metric_type_);
}

int GammaIVFPQGPUIndex::GetRecallNum(GPURetrievalParameters *params, int k,
                                     bool enable_rerank) {
  int recall_num = params->RecallNum();

  int max_recallnum = faiss::gpu::getMaxKSelection();
  if (recall_num > max_recallnum) {
    LOG(WARNING) << "recall_num should less than [" << max_recallnum << "]";
    recall_num = max_recallnum;
  } else if (recall_num < k) {
    LOG(WARNING) << "recall_num = " << recall_num
                 << " should't less than topK = " << k;
    recall_num = k;
  }

  return recall_num;
}

int GammaIVFPQGPUIndex::GetNprobe(GPURetrievalParameters *params,
                                  int default_nprobe, size_t nlist) {
  int max_recallnum = faiss::gpu::getMaxKSelection();

  if (params->Nprobe() > 0 && (size_t)params->Nprobe() <= nlist &&
      params->Nprobe() <= max_recallnum) {
    return params->Nprobe();
  } else {
    LOG(WARNING) << "Error nprobe for search, so using default value: "
                 << default_nprobe;
    return default_nprobe;
  }
}

}  // namespace gpu
}  // namespace vearch
