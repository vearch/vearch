/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tbb/concurrent_queue.h>

#include <vector>

// #include "concurrentqueue/concurrentqueue.h"
#include "reflector.h"
#include "util/utils.h"

enum class VectorValueType : std::uint8_t { FLOAT = 0, BINARY = 1, INT8 = 2 };

enum class DistanceComputeType : std::uint8_t { INNER_PRODUCT = 0, L2, Cosine };

// Performance tool, record performance info
class PerfTool {
 public:
  PerfTool() {
    start_time = utils::getmillisecs();
    cur_time = start_time;
  }

  double cur_time;
  double start_time;
  std::stringstream perf_ss;

  // Record point of time with msg
  void Perf(const std::string &msg) { Perf(msg.c_str()); }

  // Record point of time with msg
  void Perf(const char *msg) {
    double old_time = cur_time;
    cur_time = utils::getmillisecs();
    perf_ss << msg << " [" << cur_time - old_time << "]ms ";
  }

  // Return perf summary
  const std::stringstream &OutputPerf() {
    cur_time = utils::getmillisecs();
    perf_ss << " total cost [" << cur_time - start_time << "]ms ";
    return perf_ss;
  }
};

// RetrievalParameters is a base class, each model should implement it to parse
// retrieval parameters from string(serialized by JSON or protocol-buffers)
class RetrievalParameters {
 public:
  RetrievalParameters() : distance_compute_type_(DistanceComputeType::L2) {}

  RetrievalParameters(const DistanceComputeType &type)
      : distance_compute_type_(type) {}

  virtual ~RetrievalParameters(){};

  DistanceComputeType GetDistanceComputeType() {
    return distance_compute_type_;
  }
  void SetDistanceComputeType(DistanceComputeType type) {
    distance_compute_type_ = type;
  }

 protected:
  enum DistanceComputeType distance_compute_type_;
};

// Retrieval context used in search,
// it provides id and score valid filter to improve retrieval efficiency,
// it also provides performance tool to record performance info
class RetrievalContext {
 public:
  RetrievalContext() { 
    retrieval_params_ = nullptr;
    perf_tool_ = nullptr;
  }

  virtual ~RetrievalContext() {
    if (retrieval_params_ != nullptr) {
      delete retrieval_params_;
      retrieval_params_ = nullptr;
    }
  }

  RetrievalParameters *RetrievalParams() { return retrieval_params_; }

  // ID valid filter
  virtual bool IsValid(int id) const = 0;

  // Score filter
  virtual bool IsSimilarScoreValid(float score) const = 0;

  PerfTool &GetPerfTool() { return *perf_tool_; }

  RetrievalParameters *retrieval_params_;
  PerfTool *perf_tool_;
};

// Store vector meta infos
class VectorMetaInfo {
 public:
  VectorMetaInfo(const std::string &name, int dimension,
                 const VectorValueType &type, int version = 0)
      : name_(name),
        dimension_(dimension),
        data_type_(type),
        size_(0),
        mem_bytes_(0),
        version_(version) {
    if (data_type_ == VectorValueType::FLOAT) {
      data_size_ = sizeof(float);
    } else if (data_type_ == VectorValueType::BINARY) {
      data_size_ = sizeof(uint8_t);
    } else if (data_type_ == VectorValueType::INT8) {
      data_size_ = sizeof(uint8_t);
    }
  }

  ~VectorMetaInfo() {}

  std::string &Name() { return name_; }

  int Dimension() { return dimension_; }

  VectorValueType DataType() { return data_type_; }

  size_t Size() { return size_; }

  long MemBytes() { return mem_bytes_; }

  int DataSize() { return data_size_; }

  std::string AbsoluteName() {
    char v[4];
    snprintf(v, sizeof(v), "%03d", version_);
    return name_ + "." + v;
  }

  std::string name_;           // vector name
  int dimension_;              // vector dimension
  VectorValueType data_type_;  // vector data type
  size_t size_;                // vector number
  long mem_bytes_;             // memory usage
  int data_size_;              // each vector element size(byte)
  int version_;
  bool with_io_ = true;
};

/** Scoped raw vectors (for automatic deallocation)
 *
 * Example:
 *
 *  ScopeVectors scope_vecs(vids.size());
 *  vector_->Gets(vids, scope_vecs);
 *  const std::vector<const uint8_t *> &vecs = scope_vecs.Get()
 *
 *  Release called automatically when codes goes out of scope
 */

class ScopeVectors {
 public:
  explicit ScopeVectors() {}

  ~ScopeVectors() {
    for (size_t i = 0; i < deletable_.size(); i++) {
      if (deletable_[i] && ptr_[i]) delete[] ptr_[i];
    }
  }

  void Add(const uint8_t *ptr_in, bool deletable = true) {
    ptr_.push_back(ptr_in);
    deletable_.push_back(deletable);
  }

  const std::vector<const uint8_t *> &Get() { return ptr_; }

  const uint8_t *Get(int idx) { return ptr_[idx]; }

  size_t Size() { return ptr_.size(); }

  std::vector<const uint8_t *> ptr_;
  std::vector<bool> deletable_;
};

// VectorReader provides access to raw vectors
class VectorReader {
 public:
  VectorReader(VectorMetaInfo *meta_info) : meta_info_(meta_info) {}

  virtual ~VectorReader() { 
    delete meta_info_;
    meta_info_ = nullptr;
  };

  /** Get vectors by vecotor id list
   *
   * @param vids   vector id list
   * @param vecs  (output) vectors
   * @return 0 if successed
   */
  virtual int Gets(const std::vector<int64_t> &vids,
                   ScopeVectors &vecs) const = 0;

  // Return meta info
  VectorMetaInfo *MetaInfo() { return meta_info_; };

 protected:
  VectorMetaInfo *meta_info_;
};

// RetrievalModel is a virtual base class, each model should implement it
class RetrievalModel {
 public:
  RetrievalModel() {
    vector_ = nullptr;
    indexed_count_ = 0;
    indexing_size_ = 0;
  }

  virtual ~RetrievalModel() {}

  /** Init retrieval model
   *
   * @param model_parameters   include model params, need parse by yourself
   * @return 0 if successed
   */
  virtual int Init(const std::string &model_parameters, int indexing_size) = 0;

  /** Parse parameters for dynamic retrieval
   *
   * @param parameters format of json or pb
   * @return RetrievalParameters pointer
   */
  virtual RetrievalParameters *Parse(const std::string &parameters) = 0;

  /** Build index
   *
   * @return 0 if successed
   */
  virtual int Indexing() = 0;

  /** Add vectors into retrieval model
   *
   * @param n     number of vectors
   * @param vec   vectors to add
   * @return true if successed
   */
  virtual bool Add(int n, const uint8_t *vec) = 0;

  /** Update vectors from retrieval model
   *
   * @param ids   vectors ids to be updated
   * @param vecs  vectors value to be updated
   * @return true if successed
   */
  virtual int Update(const std::vector<int64_t> &ids,
                     const std::vector<const uint8_t *> &vecs) = 0;

  /** Delete from retrieval model
   *
   * @param ids ids to be deleted
   * @return 0 if successed
   */
  virtual int Delete(const std::vector<int64_t> &ids) = 0;

  /** Search interface for each retrieval model
   *
   * @param retrieval_context retrieval context, contains
   *                          RetrievalParameters and valid info
   * @param n           number of retrieval vectors
   * @param k           topk
   * @param distances   retrieval result distances
   * @param ids         retrieval ids
   * @return 0 if successed
   */
  virtual int Search(RetrievalContext *retrieval_context, int n,
                     const uint8_t *x, int k, float *distances,
                     int64_t *ids) = 0;

  // Return model memory usage
  virtual long GetTotalMemBytes() = 0;

  /** Dump model and index
   *
   * @param dir   dump directory
   * @return 0 if successed
   */
  virtual int Dump(const std::string &dir) = 0;

  /** Load model and index
   *
   * @param dir   load directory
   * @return load number(>=0) if successed
   */
  virtual int Load(const std::string &dir) = 0;

  virtual void train(int64_t n, const float *x) {}

  VectorReader *vector_;
  tbb::concurrent_bounded_queue<int> updated_vids_;
  // warining: indexed_count_ is only used by framework, sub-class cann't use it
  int indexed_count_;
  int indexing_size_;
};
