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
#include "index/impl/gamma_index_ivfpq.h"
#include "third_party/nlohmann/json.hpp"
#include "util/bitmap.h"

using std::string;
using std::vector;

namespace vearch {
namespace gpu {

namespace {
const int kMaxBatch = 512;  // max search batch num
const int kMaxReqNum = 512; // max request num
const int kMaxRecallNum = faiss::gpu::getMaxKSelection(); // max recall num or max Nprobe
}  // namespace

struct IVFPQGPUModelParams {
  int ncentroids;     // coarse cluster center number
  int nsubvector;     // number of sub cluster center
  int nbits_per_idx;  // bit number of sub cluster center
  int nprobe;         // search how many bucket
  DistanceComputeType metric_type;

  IVFPQGPUModelParams() {
    ncentroids = 2048;
    nsubvector = 64;
    nbits_per_idx = 8;
    nprobe = 80;
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
    int nprobe;

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

    if (j.contains("nprobe")) {
      nprobe = j.value("nprobe", 0);
      if (nprobe <= 0) {
        std::string msg =
            std::string("invalid nprobe =") + std::to_string(nprobe);
        LOG(ERROR) << msg;
        return Status::ParamError(msg);
      }
      if (nprobe > 0) this->nprobe = nprobe;
      if (this->nprobe > this->ncentroids) {
        std::string msg = "nprobe should less than ncentroids";
        LOG(ERROR) << msg;
        return Status::ParamError(msg);
      }
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
    if (ncentroids <= 0 || nsubvector <= 0 || nbits_per_idx <= 0 || nprobe <= 0) return false;
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
    ss << "ncentroids=" << ncentroids << ", ";
    ss << "nsubvector=" << nsubvector << ", ";
    ss << "nbits_per_idx=" << nbits_per_idx << ", ";
    ss << "nprobe=" << nprobe;

    return ss.str();
  }
};

REGISTER_INDEX(GPU_IVFPQ, GammaIVFPQGPUIndex)

GammaIVFPQGPUIndex::GammaIVFPQGPUIndex()
  : GammaGPUSearchBase<GammaIVFPQIndex, IVFPQGPURetrievalParameters>() {
  nlist_ = 2048;
  nprobe_ = 80;
  vectors_added_since_last_log_ = 0;
  }

GammaIVFPQGPUIndex::~GammaIVFPQGPUIndex() {
  // Base class destructor will handle cleanup
}

Status GammaIVFPQGPUIndex::Init(const std::string &model_parameters,
                                int training_threshold) {
  IVFPQGPUModelParams ivfpq_param;
  if (model_parameters != "") {
    Status status = ivfpq_param.Parse(model_parameters.c_str());
    if (!status.ok()) return status;
  }
  LOG(INFO) << ivfpq_param.ToString();
  int d = vector_->MetaInfo()->Dimension();
  this->d_ = d;

  if (d % ivfpq_param.nsubvector != 0) {
    std::string msg = std::string("Dimension [") +
                      std::to_string(vector_->MetaInfo()->Dimension()) +
                      "] cannot divide by nsubvector [" +
                      std::to_string(ivfpq_param.nsubvector) + "].";
    LOG(ERROR) << msg;
    return Status::ParamError(msg);
  }

  this->nlist_ = ivfpq_param.ncentroids;
  this->nprobe_ = ivfpq_param.nprobe;
  this->nsubvector_ = ivfpq_param.nsubvector;
  this->nbits_per_idx_ = ivfpq_param.nbits_per_idx;

  metric_type_ = ivfpq_param.metric_type;
  if (training_threshold) {
    training_threshold_ = training_threshold;
  } else {
    training_threshold_ = nlist_ * max_points_per_centroid;
  }

  // Call base class initialization
  return GammaGPUIndexBase<GammaIVFPQIndex>::Init(model_parameters,
                                                    training_threshold);
}

RetrievalParameters *GammaIVFPQGPUIndex::Parse(const std::string &parameters) {
  if (parameters == "") {
    return new IVFPQGPURetrievalParameters();
  }

  nlohmann::json j;
  try {
    j = nlohmann::json::parse(parameters);
  } catch (const nlohmann::json::parse_error &e) {
    LOG(ERROR) << "failed to parse IVFPQ GPU retrieval parameters: "
               << e.what();
    return nullptr;
  }

  std::string metric_type;
  IVFPQGPURetrievalParameters *retrieval_params = new IVFPQGPURetrievalParameters();
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
  int num_gpus = faiss::gpu::getNumDevices();
  LOG(INFO) << "number of GPUs available: " << num_gpus;

  vector<int> devs;
  for (int i = 0; i < num_gpus; ++i) {
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

  std::vector<faiss::Index *> gpu_indexes;
  for (int i = 0; i < num_gpus; i++) {
    faiss::gpu::GpuIndexIVFPQConfig config;
    config.device = i;

    auto gpu_index = new faiss::gpu::GpuIndexIVFPQ(
        resources_[i], d_, nlist_, nsubvector_, nbits_per_idx_,
        (faiss::MetricType)metric_type_, config);
    gpu_indexes.push_back(gpu_index);
  }

  // faiss::IndexShards *multi_gpu_index = new faiss::IndexShards(d_, true);
  faiss::IndexShards *multi_gpu_index = new faiss::IndexShards(d_);
  multi_gpu_index->successive_ids = false;
  multi_gpu_index->own_indices = true;
  for (auto *idx : gpu_indexes) {
    multi_gpu_index->add_shard(idx);
  }
  faiss::Index *gpu_index = multi_gpu_index;
  return gpu_index;
}

int GammaIVFPQGPUIndex::CreateSearchThread() {
  auto func_search = std::bind(&GammaIVFPQGPUIndex::GPUThread, this);

  gpu_threads_.push_back(std::thread(func_search));
  gpu_threads_.back().detach();
  return 0;
}

int GammaIVFPQGPUIndex::Indexing() {
  std::unique_lock<std::shared_mutex> lock(gpu_index_mutex_);

  LOG(INFO) << "GPU indexing";

  if (is_trained_) {
    is_trained_ = false;
    delete gpu_index_;
    indexed_count_ = 0;
  }

  if (!is_trained_) {
    gpu_index_ = CreateGPUIndex();
    {
      RawVector *raw_vec = dynamic_cast<RawVector *>(vector_);
      size_t vectors_count = raw_vec->MetaInfo()->Size();

      size_t num;
      if (vectors_count <= training_threshold_) {
        num = vectors_count;
        LOG(INFO) << "force merge all vectors for training, num=" << num;
      } else {
        if ((size_t)training_threshold_ < nlist_ * 39) {
          num = nlist_ * 39;
          LOG(WARNING) << "Because training_threshold[" << training_threshold_
                       << "] < ncentroids[" << nlist_
                       << "], training_threshold becomes ncentroids * 39[" << num
                       << "].";
        } else {
          num = training_threshold_;
        }
      }
      if (num > vectors_count) {
        LOG(ERROR) << "vector total count [" << vectors_count
                   << "] less then training_threshold[" << num << "], failed!";
        return -1;
      }

      ScopeVectors headers;
      std::vector<int> lens;
      raw_vec->GetVectorHeader(0, num, headers, lens);

      // merge vectors
      int raw_d = raw_vec->MetaInfo()->Dimension();
      const uint8_t *train_raw_vec = nullptr;
      utils::ScopeDeleter1<uint8_t> del_train_raw_vec;
      size_t n_get = 0;
      if (lens.size() == 1) {
        train_raw_vec = headers.Get(0);
        n_get = lens[0];
        if (num > training_threshold_ && num > n_get) {
          LOG(ERROR) << "training vector get count [" << n_get
                     << "] less then training_threshold[" << num
                     << "], failed!";
          return -2;
        }
      } else {
        train_raw_vec = new uint8_t[raw_d * num * sizeof(float)];
        del_train_raw_vec.set(train_raw_vec);
        size_t offset = 0;
        for (size_t i = 0; i < headers.Size(); ++i) {
          memcpy((void *)(train_raw_vec + offset), (void *)headers.Get(i),
                 sizeof(float) * raw_d * lens[i]);
          offset += sizeof(float) * raw_d * lens[i];
        }
      }
      LOG(INFO) << "train vector wanted num=" << num << ", real num=" << n_get;

      gpu_index_->train(n_get, reinterpret_cast<const float *>(train_raw_vec));
    }
    is_trained_ = true;
  }

  if (gpu_threads_.size() == 0) {
    CreateSearchThread();
  }

  LOG(INFO) << "GPU indexed.";
  return 0;
}

bool GammaIVFPQGPUIndex::Add(int n, const uint8_t *vec) {
  std::vector<long> new_keys;
  std::vector<uint8_t> new_codes;
  size_t code_size = d_ * sizeof(float);
  long vid = indexed_count_;
  int n_add = 0;
  RawVector *raw_vec = dynamic_cast<RawVector *>(vector_);

  for (int i = 0; i < n; i++) {
    if (raw_vec->Bitmap()->Test(vid + i)) {
      continue;
    }
    uint8_t *code = (uint8_t *)vec + code_size * i;
    new_keys.push_back(vid + i);
    size_t ofs = new_codes.size();
    new_codes.resize(ofs + code_size);
    memcpy((void *)(new_codes.data() + ofs), (void *)code, code_size);
    n_add +=1;
  }

  std::unique_lock<std::shared_mutex> lock(gpu_index_mutex_);

  if (start_docid_ != indexed_count_) {
    return false;
  }

  gpu_index_->add_with_ids(n_add, reinterpret_cast<const float *>(new_codes.data()), new_keys.data());
  vectors_added_since_last_log_ += n;
  if (vectors_added_since_last_log_ >= 10000) {
    LOG(DEBUG) << "GPU indexed count: " << indexed_count_;
    vectors_added_since_last_log_ = 0;
  }
  return true;
}

int GammaIVFPQGPUIndex::GPUThread() {
  float *xx = new float[kMaxBatch * d_ * kMaxReqNum];
  long *label = new long[kMaxBatch * kMaxRecallNum * kMaxReqNum];
  float *dis = new float[kMaxBatch * kMaxRecallNum * kMaxReqNum];

  thread_local std::vector<int> batch_offsets;
  thread_local std::vector<int> result_offsets;
  batch_offsets.reserve(kMaxBatch);
  result_offsets.reserve(kMaxBatch);

  while (!b_exited_) {
    int size = 0;
    GPUSearchItem *items[kMaxBatch];

    while (size == 0 && !b_exited_) {
      size = search_queue_.wait_dequeue_bulk_timed(items, kMaxBatch, 100);
    }

    if (size > 1) {
      std::unordered_map<int, std::vector<int>> nprobe_map;
      nprobe_map.reserve(8);

      for (int i = 0; i < size; ++i) {
        nprobe_map[items[i]->nprobe_].emplace_back(i);
      }

      for (auto &nprobe_ids : nprobe_map) {
        if (nprobe_ids.second.empty()) continue;

        // Pre-calculate total vectors and max k to reduce redundant computation
        int recallnum = 0, total = 0;
        batch_offsets.clear();
        result_offsets.clear();

        int data_offset = 0;
        for (size_t j = 0; j < nprobe_ids.second.size(); ++j) {
          int idx = nprobe_ids.second[j];
          recallnum = std::max(recallnum, items[idx]->k_);
          total += items[idx]->n_;
          batch_offsets.push_back(data_offset);
          data_offset += d_ * items[idx]->n_;
        }

        for (size_t j = 0; j < nprobe_ids.second.size(); ++j) {
          int idx = nprobe_ids.second[j];
          const size_t copy_size = d_ * sizeof(float) * items[idx]->n_;
          std::memcpy(xx + batch_offsets[j], items[idx]->x_, copy_size);
        }

        {
          std::shared_lock<std::shared_mutex> lock(gpu_index_mutex_);
          if (gpu_index_ == nullptr || b_exited_) {
            LOG(WARNING) << "GPU index is null or exiting";
            // Notify all items in this batch
            for (size_t j = 0; j < nprobe_ids.second.size(); ++j) {
              items[nprobe_ids.second[j]]->Notify();
            }
            continue;
          }

          auto indexShards = dynamic_cast<faiss::IndexShards *>(gpu_index_);
          if (indexShards != nullptr) {
            for (int j = 0; j < indexShards->count(); ++j) {
              auto ivfpq = dynamic_cast<faiss::gpu::GpuIndexIVFPQ *>(
                  indexShards->at(j));
              if (ivfpq != nullptr) ivfpq->nprobe = nprobe_ids.first;
            }
          }

          try {
            gpu_index_->search(total, xx, recallnum, dis, label);
          } catch (const std::exception &e) {
            LOG(ERROR) << "GPU batch search failed: " << e.what();
            // Notify all items even on failure
            for (size_t j = 0; j < nprobe_ids.second.size(); ++j) {
              items[nprobe_ids.second[j]]->Notify();
            }
            continue;
          }
        }

        int result_offset = 0;
        for (size_t j = 0; j < nprobe_ids.second.size(); ++j) {
          int idx = nprobe_ids.second[j];
          const size_t dis_size = sizeof(float) * items[idx]->n_ * items[idx]->k_;
          const size_t label_size = sizeof(long) * items[idx]->n_ * items[idx]->k_;

          std::memcpy(items[idx]->dis_, dis + result_offset, dis_size);
          std::memcpy(items[idx]->label_, label + result_offset, label_size);
          result_offset += recallnum * items[idx]->n_;

          // Notify immediately after copying results for this item
          items[idx]->Notify();
        }
      }
    } else if (size == 1) {
      try {
        std::shared_lock<std::shared_mutex> lock(gpu_index_mutex_);
        if (gpu_index_ == nullptr || b_exited_) {
          LOG(WARNING) << "GPU index is null or exiting";
          items[0]->Notify();
          continue;
        }

        auto indexShards = dynamic_cast<faiss::IndexShards *>(gpu_index_);
        if (indexShards != nullptr) {
          for (int j = 0; j < indexShards->count(); ++j) {
            auto ivfpq =
                dynamic_cast<faiss::gpu::GpuIndexIVFPQ *>(indexShards->at(j));
            if (ivfpq != nullptr) ivfpq->nprobe = items[0]->nprobe_;
          }
        }

        gpu_index_->search(items[0]->n_, items[0]->x_, items[0]->k_,
                           items[0]->dis_, items[0]->label_);
      } catch (const std::exception &e) {
        LOG(ERROR) << "GPU search failed: " << e.what();
      }
      items[0]->Notify();
    }
  }

  delete[] xx;
  delete[] label;
  delete[] dis;
  LOG(INFO) << "GPU thread exit";
  return 0;
}

int GammaIVFPQGPUIndex::Search(RetrievalContext *retrieval_context, int n,
                               const uint8_t *x, int k, float *distances,
                               long *labels) {
  return CommonSearch(retrieval_context, n, x, k, distances, labels, nprobe_,
                      nlist_, false);  // IVFPQ doesn't support rerank
}

IVFPQGPURetrievalParameters *GammaIVFPQGPUIndex::CreateDefaultRetrievalParams(
    int default_nprobe) {
  return new IVFPQGPURetrievalParameters(100, default_nprobe, metric_type_);
}

int GammaIVFPQGPUIndex::GetRecallNum(IVFPQGPURetrievalParameters *params, int k,
                                     bool enable_rerank) {
  int recall_num = params->RecallNum();

  if (recall_num < k) {
    recall_num = k;
  }

  return recall_num;
}

int GammaIVFPQGPUIndex::GetNprobe(IVFPQGPURetrievalParameters *params,
                                  int default_nprobe, size_t nlist) {
  if (params->Nprobe() > 0 && (size_t)params->Nprobe() <= nlist &&
      params->Nprobe() <= kMaxRecallNum) {
    return params->Nprobe();
  } else {
    LOG(WARNING) << "Error nprobe for search, so using default value: "
                 << default_nprobe;
    return default_nprobe;
  }
}

}  // namespace gpu
}  // namespace vearch
