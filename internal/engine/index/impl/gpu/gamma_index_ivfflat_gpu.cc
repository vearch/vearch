/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "gamma_index_ivfflat_gpu.h"

#include <faiss/IndexFlat.h>
#include <faiss/IndexShards.h>
#include <faiss/gpu/GpuAutoTune.h>
#include <faiss/gpu/GpuClonerOptions.h>
#include <faiss/gpu/GpuIndexIVFFlat.h>
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
#include "util/bitmap.h"
#include "util/utils.h"

using faiss::idx_t;
using std::string;
using std::vector;

namespace vearch {
namespace gpu {

namespace {
const int kMaxBatch = 200;  // max search batch num
const int kMaxReqNum = 200;
}  // namespace

REGISTER_INDEX(GPU_IVFFLAT, GammaIVFFlatGPUIndex)

GammaIVFFlatGPUIndex::GammaIVFFlatGPUIndex()
    : GammaGPUSearchBase<GammaIVFFlatIndex, IVFFlatGPURetrievalParameters>() {
  nlist_ = 2048;
  nprobe_ = 80;
}

GammaIVFFlatGPUIndex::~GammaIVFFlatGPUIndex() {
  // Base class destructor will handle cleanup
}

Status GammaIVFFlatGPUIndex::Init(const std::string &model_parameters,
                                  int training_threshold) {
  IVFFlatGPUModelParams params;
  if (!model_parameters.empty()) {
    Status status = params.Parse(model_parameters.c_str());
    if (!status.ok()) return status;
  }

  LOG(INFO) << params.ToString();

  int d = vector_->MetaInfo()->Dimension();
  this->d_ = d;
  this->nlist_ = params.ncentroids;
  this->nprobe_ = params.nprobe;
  this->metric_type_ = params.metric_type;

  // Call base class initialization
  return GammaGPUIndexBase<GammaIVFFlatIndex>::Init(model_parameters,
                                                    training_threshold);
}

RetrievalParameters *GammaIVFFlatGPUIndex::Parse(
    const std::string &parameters) {
  if (parameters.empty()) {
    return new IVFFlatGPURetrievalParameters();
  }

  nlohmann::json j;
  try {
    j = nlohmann::json::parse(parameters);
  } catch (const nlohmann::json::parse_error &e) {
    LOG(ERROR) << "failed to parse IVFFLAT GPU retrieval parameters: "
               << e.what();
    return nullptr;
  }

  std::string metric_type;
  IVFFlatGPURetrievalParameters *retrieval_params =
      new IVFFlatGPURetrievalParameters();

  if (j.contains("metric_type")) {
    metric_type = j.value("metric_type", "");
    if (strcasecmp("L2", metric_type.c_str()) == 0) {
      retrieval_params->SetDistanceComputeType(DistanceComputeType::L2);
    } else if (strcasecmp("InnerProduct", metric_type.c_str()) == 0) {
      retrieval_params->SetDistanceComputeType(
          DistanceComputeType::INNER_PRODUCT);
    } else if (!metric_type.empty()) {
      LOG(ERROR) << "invalid metric_type = " << metric_type
                 << ", so use default value.";
    }
  } else {
    retrieval_params->SetDistanceComputeType(metric_type_);
  }

  int nprobe;
  if (j.contains("nprobe")) {
    nprobe = j.value("nprobe", 0);
    if (nprobe > 0) {
      retrieval_params->SetNprobe(nprobe);
    }
  }

  bool parallel_on_queries;
  if (j.contains("parallel_on_queries")) {
    parallel_on_queries = j.value("parallel_on_queries", true);
    retrieval_params->SetParallelOnQueries(parallel_on_queries);
  }

  return retrieval_params;
}

faiss::Index *GammaIVFFlatGPUIndex::CreateGPUIndex() {
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
  options.useFloat16 = false;  // IVFFLAT typically uses float32
  options.usePrecomputed = false;
  options.reserveVecs = 0;
  options.storeTransposed = true;
  options.verbose = true;

  // shard the index across GPUs
  options.shard = true;
  options.shard_type = 1;

  // Convert CPU IVFFLAT index to GPU index
  faiss::Index *gpu_index =
      gamma_index_cpu_to_gpu_multiple(resources_, devs, cpu_index_, &options);

  return gpu_index;
}

int GammaIVFFlatGPUIndex::CreateSearchThread() {
  auto func_search = std::bind(&GammaIVFFlatGPUIndex::GPUThread, this);
  gpu_threads_.push_back(std::thread(func_search));
  gpu_threads_.back().detach();
  return 0;
}

int GammaIVFFlatGPUIndex::Indexing() {
  std::lock_guard<std::mutex> lock(indexing_mutex_);

  LOG(INFO) << "GPU indexing";

  if (!is_trained_) {
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

int GammaIVFFlatGPUIndex::AddRTVecsToIndex() {
  std::lock_guard<std::mutex> lock(cpu_mutex_);
  int ret = 0;

  RawVector *raw_vec = dynamic_cast<RawVector *>(cpu_index_->vector_);
  if (!raw_vec) {
    LOG(ERROR) << "Raw vector cast failed";
    return -1;
  }

  int total_stored_vecs = raw_vec->MetaInfo()->Size();
  if (cpu_index_->GetIndexedVecCount() > total_stored_vecs) {
    LOG(ERROR) << "indexed_vec_count [" << cpu_index_->GetIndexedVecCount()
               << "] > total_stored_vecs [" << total_stored_vecs << "]";
    ret = -1;
  } else if (cpu_index_->GetIndexedVecCount() == total_stored_vecs) {
#ifdef DEBUG
    LOG(INFO) << "no extra vectors existed for indexing";
#endif
    // For IVFFLAT, we don't have rt_invert_index_ptr, so skip this
  } else {
    // Add new vectors to index
    int MAX_NUM_PER_INDEX = 1000;
    int index_count = (total_stored_vecs - cpu_index_->GetIndexedVecCount()) /
                          MAX_NUM_PER_INDEX +
                      1;

    for (int i = 0; i < index_count; i++) {
      int64_t start_docid = cpu_index_->GetIndexedVecCount();
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
      if (!cpu_index_->Add(count_per_index, add_vec)) {
        LOG(ERROR) << "add index from docid " << start_docid << " error!";
        ret = -2;
      }
    }
  }

  // Update indexed count
  this->indexed_count_ = cpu_index_->GetIndexedVecCount();

  // Process updated vector IDs
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
  if (vids.size() == 0) return ret;

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

int GammaIVFFlatGPUIndex::GPUThread() {
  float *xx = new float[kMaxBatch * d_ * kMaxReqNum];
  size_t max_recallnum = (size_t)faiss::gpu::getMaxKSelection();
  long *label = new long[kMaxBatch * max_recallnum * kMaxReqNum];
  float *dis = new float[kMaxBatch * max_recallnum * kMaxReqNum];

  while (!b_exited_) {
    int size = 0;
    GPUSearchItem *items[kMaxBatch];

    while (size == 0 && !b_exited_) {
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
          if (gpu_index_ != nullptr) {
            int ngpus = faiss::gpu::getNumDevices();
            if (ngpus > 1) {
              auto indexShards = dynamic_cast<faiss::IndexShards *>(gpu_index_);
              if (indexShards != nullptr) {
                for (int j = 0; j < indexShards->count(); ++j) {
                  auto ivfflat = dynamic_cast<faiss::gpu::GpuIndexIVFFlat *>(
                      indexShards->at(j));
                  if (ivfflat != nullptr) ivfflat->nprobe = nprobe_ids.first;
                }
              }
            } else {
              auto ivfflat =
                  dynamic_cast<faiss::gpu::GpuIndexIVFFlat *>(gpu_index_);
              if (ivfflat != nullptr) ivfflat->nprobe = nprobe_ids.first;
            }
            gpu_index_->search(total, xx, recallnum, dis, label);
          } else {
            LOG(WARNING) << "GPU index is null, using CPU index";
            // For CPU fallback, process items individually
            for (size_t j = 0; j < nprobe_ids.second.size(); ++j) {
              cpu_index_->Search(nullptr, items[nprobe_ids.second[j]]->n_,
                                 reinterpret_cast<const uint8_t *>(
                                     items[nprobe_ids.second[j]]->x_),
                                 items[nprobe_ids.second[j]]->k_,
                                 items[nprobe_ids.second[j]]->dis_,
                                 items[nprobe_ids.second[j]]->label_);
            }
            continue;
          }
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
      try {
        std::shared_lock<std::shared_mutex> lock(gpu_index_mutex_);
        if (gpu_index_ != nullptr) {
          int ngpus = faiss::gpu::getNumDevices();
          if (ngpus > 1) {
            auto indexShards = dynamic_cast<faiss::IndexShards *>(gpu_index_);
            if (indexShards != nullptr) {
              for (int j = 0; j < indexShards->count(); ++j) {
                auto ivfflat = dynamic_cast<faiss::gpu::GpuIndexIVFFlat *>(
                    indexShards->at(j));
                if (ivfflat != nullptr) ivfflat->nprobe = items[0]->nprobe_;
              }
            }
          } else {
            auto ivfflat =
                dynamic_cast<faiss::gpu::GpuIndexIVFFlat *>(gpu_index_);
            if (ivfflat != nullptr) ivfflat->nprobe = items[0]->nprobe_;
          }
          // Perform GPU search
          gpu_index_->search(items[0]->n_, items[0]->x_, items[0]->k_,
                             items[0]->dis_, items[0]->label_);
        } else {
          LOG(WARNING) << "GPU index is null, using CPU index";
          cpu_index_->Search(nullptr, items[0]->n_,
                             reinterpret_cast<const uint8_t *>(items[0]->x_),
                             items[0]->k_, items[0]->dis_, items[0]->label_);
        }
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

int GammaIVFFlatGPUIndex::Search(RetrievalContext *retrieval_context, int n,
                                 const uint8_t *x, int k, float *distances,
                                 long *labels) {
  return CommonSearch(retrieval_context, n, x, k, distances, labels, nprobe_,
                      nlist_, false);  // IVFFLAT doesn't need rerank
}

IVFFlatGPURetrievalParameters *
GammaIVFFlatGPUIndex::CreateDefaultRetrievalParams(int default_nprobe) {
  return new IVFFlatGPURetrievalParameters(default_nprobe, true, metric_type_);
}

int GammaIVFFlatGPUIndex::GetRecallNum(IVFFlatGPURetrievalParameters *params,
                                       int k, bool enable_rerank) {
  // For IVFFLAT, we typically don't have a separate recall_num parameter
  // We use k directly as the recall number
  return k;
}

int GammaIVFFlatGPUIndex::GetNprobe(IVFFlatGPURetrievalParameters *params,
                                    int default_nprobe, size_t nlist) {
  if (params->Nprobe() > 0 && (size_t)params->Nprobe() <= nlist) {
    return params->Nprobe();
  } else {
    LOG(WARNING) << "Error nprobe for search, so using default value: "
                 << default_nprobe;
    return default_nprobe;
  }
}

}  // namespace gpu
}  // namespace vearch
