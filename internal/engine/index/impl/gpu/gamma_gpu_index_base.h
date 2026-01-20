/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <faiss/gpu/impl/IndexUtils.h>
#include <faiss/gpu/GpuClonerOptions.h>
#include <faiss/gpu/StandardGpuResources.h>

#include <condition_variable>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <vector>

#include "common/gamma_common_data.h"
#include "concurrentqueue/blockingconcurrentqueue.h"
#include "index/impl/gamma_index_flat.h"
#include "index/index_model.h"
#include "util/log.h"
#include "util/utils.h"
#include "vector/raw_vector.h"

namespace vearch {
namespace gpu {

using faiss::gpu::GpuClonerOptions;
using faiss::gpu::GpuMultipleClonerOptions;
using faiss::gpu::StandardGpuResources;

/**
 * Base class for GPU search items
 */
class GPUSearchItem {
 public:
  GPUSearchItem(int n, const float *x, int k, float *dis, long *label,
                int nprobe)
      : n_(n),
        x_(x),
        k_(k),
        dis_(dis),
        label_(label),
        nprobe_(nprobe),
        done_(false) {}

  virtual ~GPUSearchItem() = default;

  void Notify() {
    std::lock_guard<std::mutex> lock(mtx_);
    done_ = true;
    cv_.notify_one();
  }

  int WaitForDone() {
    std::unique_lock<std::mutex> lck(mtx_);
    while (!done_) {
      cv_.wait(lck);
    }
    return 0;
  }

  // Search parameters
  int n_;
  const float *x_;
  int k_;
  float *dis_;
  long *label_;
  int nprobe_;

 private:
  std::condition_variable cv_;
  std::mutex mtx_;
  bool done_;
};

/**
 * Base GPU retrieval parameters
 */
class GPURetrievalParametersBase : public RetrievalParameters {
 public:
  GPURetrievalParametersBase() : RetrievalParameters() { nprobe_ = 80; }

  GPURetrievalParametersBase(int nprobe, DistanceComputeType type)
      : RetrievalParameters(type), nprobe_(nprobe) {}

  virtual ~GPURetrievalParametersBase() = default;

  int Nprobe() const { return nprobe_; }
  void SetNprobe(int nprobe) { nprobe_ = nprobe; }

 protected:
  int nprobe_;
};

/**
 * Base class for GPU index implementations
 */
template <typename CPUIndexType>
class GammaGPUIndexBase : public IndexModel {
 public:
  GammaGPUIndexBase()
      : IndexModel(),
        gpu_index_(nullptr),
        b_exited_(false),
        is_trained_(false),
        d_(0) {}

  virtual ~GammaGPUIndexBase() { Cleanup(); }

  virtual Status Init(const std::string &model_parameters,
                      int training_threshold) override {
    b_exited_ = false;
    gpu_index_ = nullptr;
    return Status::OK();
  }

  virtual int Indexing() override { return 0; }

  virtual int Update(const std::vector<int64_t> &ids,
                     const std::vector<const uint8_t *> &vecs) override {
    return 0;
  }

  virtual int Delete(const std::vector<int64_t> &ids) override { return 0; }

  virtual long GetTotalMemBytes() override { return 0; }

  virtual Status Dump(const std::string &dir) override { return Status::OK(); }

  virtual Status Load(const std::string &index_dir,
                      int64_t &load_num) override {
    return Status::OK();
  }

 protected:
  virtual faiss::Index *CreateGPUIndex() = 0;
  virtual int CreateSearchThread() = 0;
  virtual int GPUThread() = 0;

  void InitGPUResources() {
    int ngpus = faiss::gpu::getNumDevices();
    LOG(INFO) << "number of GPUs available: " << ngpus;

    devices_.clear();
    for (int i = 0; i < ngpus; ++i) {
      devices_.push_back(i);
    }

    std::lock_guard<std::mutex> lock(cpu_mutex_);
    if (resources_.size() == 0) {
      for (int i : devices_) {
        auto res = new StandardGpuResources;
        res->getResources()->initializeForDevice(i);
        res->setTempMemory((size_t)1536 * 1024 * 1024);  // 1.5 GiB
        resources_.push_back(res);
      }
    }
  }

  void Cleanup() {
    std::unique_lock<std::shared_mutex> lock(gpu_index_mutex_);
    b_exited_ = true;
    std::this_thread::sleep_for(std::chrono::seconds(2));

    delete gpu_index_;
    gpu_index_ = nullptr;

    for (auto &resource : resources_) {
      delete resource;
      resource = nullptr;
    }
    resources_.clear();
  }

  moodycamel::BlockingConcurrentQueue<GPUSearchItem *> search_queue_;

  // GPU and CPU indices
  faiss::Index *gpu_index_;

  // GPU resources
  std::vector<StandardGpuResources *> resources_;
  std::vector<int> devices_;
  std::vector<std::thread> gpu_threads_;

  // State variables
  bool b_exited_;
  bool is_trained_;
  int d_;
  DistanceComputeType metric_type_;

  // Synchronization
  std::mutex cpu_mutex_;
  std::shared_mutex gpu_index_mutex_;

  int vectors_added_since_last_log_;

  // Constants
  static constexpr int kMaxBatch = 512;
  static constexpr int kMaxReqNum = 512;
  const int kMaxRecallNum = faiss::gpu::getMaxKSelection();
  static constexpr const char *kDelim = "\001";

 protected:
  // Common filter processing methods
  int ParseFilters(SearchCondition *condition,
                   std::vector<enum DataType> &range_filter_types,
                   std::vector<std::vector<std::string>> &all_term_items) {
    for (size_t i = 0; i < condition->range_filters.size(); ++i) {
      auto range = condition->range_filters[i];

      enum DataType type;
      if (condition->table->GetFieldType(range.field, type)) {
        LOG(ERROR) << "Can't get " << range.field << " data type";
        return -1;
      }

      if (type == DataType::STRING || type == DataType::STRINGARRAY) {
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

      if (type != DataType::STRING && type != DataType::STRINGARRAY) {
        LOG(ERROR) << term.field << " can't be term filter";
        return -1;
      }

      std::vector<std::string> term_items = utils::split(term.value, kDelim);
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

  bool FilteredByRangeFilter(SearchCondition *condition,
                             std::vector<enum DataType> &range_filter_types,
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

  bool FilteredByTermFilter(
      SearchCondition *condition,
      std::vector<std::vector<std::string>> &all_term_items, long docid) {
    for (size_t i = 0; i < condition->term_filters.size(); ++i) {
      auto term = condition->term_filters[i];

      std::string field_value;
      int field_id = condition->table->GetAttrIdx(term.field);
      condition->table->GetFieldRawValue(docid, field_id, field_value);
      std::vector<std::string> field_items;
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
  }
};

}  // namespace gpu
}  // namespace vearch
