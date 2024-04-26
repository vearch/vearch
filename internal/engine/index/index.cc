/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "index.h"

#include <assert.h>

#include <chrono>
#include <iostream>
#include <sstream>

#include "c_api/gamma_api.h"
#include "common/gamma_common_data.h"
#include "faiss/IndexIVFFlat.h"
#include "faiss/IndexIVFPQ.h"
#include "search/engine.h"
#include "table/table_io.h"
#include "util/log.h"
#include "util/utils.h"
#include "vector/raw_vector_factory.h"
#ifdef USE_SCANN
#include "index/impl/scann/scann_api.h"
#endif  // USE_SCANN

namespace vearch {

Index::Index() {}

Index::~Index() {}

IndexIVFFlat::IndexIVFFlat(faiss::Index *quantizer, size_t d, size_t nlist,
                           faiss::MetricType metric) {
  this->d = d;
  this->nlist = nlist;
  this->metric_type = metric;
  this->quantizer = quantizer;
  if (metric_type == faiss::METRIC_L2)
    index_param =
        "{\"metric_type\" : \"L2\", \"ncentroids\" : " + std::to_string(nlist) +
        "}";
  else
    index_param = "{\"metric_type\" : \"InnerProduct\", \"ncentroids\" : " +
                  std::to_string(nlist) + "}";
  this->init();
}

IndexIVFFlat::~IndexIVFFlat() {
  delete raw_vector_;
  raw_vector_ = nullptr;
  delete docids_bitmap_;
  docids_bitmap_ = nullptr;
}

Status IndexIVFFlat::init(const std::string &index_param) {
  if (index_param != "") {
    this->index_param = index_param;
  }
  LOG(INFO) << "index params=" << this->index_param;

  VectorStorageType storage_type = VectorStorageType::MemoryOnly;
  vec_name = "gamma";

  std::string index_root_path_ = ".";
  VectorValueType value_type = VectorValueType::FLOAT;

  std::string vec_root_path = index_root_path_ + "/vectors";
  if (utils::make_dir(vec_root_path.c_str())) {
    std::string msg =
        std::string("make directory error, path=") + vec_root_path;
    LOG(ERROR) << msg;
    return Status::PathNotFound(msg);
  }
  VectorMetaInfo *meta_info = new VectorMetaInfo(vec_name, d, value_type);
  meta_info->with_io_ = false;

  StoreParams store_params(meta_info->AbsoluteName());
  std::string store_param = "";
  if (store_param != "") {
    Status status = store_params.Parse(store_param.c_str());
    if (!status.ok()) return status;
  }

  LOG(INFO) << "store params=" << store_params.ToJsonStr();

  docids_bitmap_ = new bitmap::RocksdbBitmapManager();
  int init_bitmap_size = 1000 * 10000;
  if (docids_bitmap_->Init(init_bitmap_size, index_root_path_ + "/bitmap") != 0) {
    std::string msg = "Cannot create bitmap!";
    LOG(ERROR) << msg;
    return Status::IOError(msg);
  }

  raw_vector_ = RawVectorFactory::Create(meta_info, storage_type, vec_root_path,
                                         store_params, docids_bitmap_);
  if (raw_vector_ == nullptr) {
    std::string msg = "create raw vector error";
    LOG(ERROR) << msg;
    return Status::IOError(msg);
  }
  LOG(INFO) << "create raw vector success, vec_name[" << vec_name << "]";

  int ret = raw_vector_->Init(vec_name, false);
  if (ret != 0) {
    std::string msg = std::string("Raw vector ") + vec_name +
                      " init error, code [" + std::to_string(ret) + "]!";
    LOG(ERROR) << msg;
    delete raw_vector_;
    return Status::IOError(msg);
  }

  vector_ = raw_vector_;

  Status status = GammaIndexIVFFlat::Init(this->index_param, 100000);
  if (!status.ok()) {
    LOG(ERROR) << "gamma index init " << vec_name << " error!";
    return status;
  }
  // init indexed count
  indexed_count_ = 0;
  return Status::OK();
}

Status IndexIVFFlat::init() { return init(index_param); }

void IndexIVFFlat::train(idx_t n, const float *x) {
  faiss::IndexIVFFlat::train(n, x);
}

void IndexIVFFlat::add(idx_t n, const float *x) { Add(n, (const uint8_t *)x); }

void IndexIVFFlat::search(idx_t n, const float *x, idx_t k, float *distances,
                          idx_t *labels) {
  PerfTool perf_tool;
  SearchCondition *condition = new SearchCondition(&perf_tool);
  condition->topn = k;
  condition->Init(std::numeric_limits<float>::lowest(),
                  std::numeric_limits<float>::max(), nullptr, nullptr);
  Search(condition, n, (const uint8_t *)x, k, distances, labels);
  delete condition;
}

Status IndexIVFFlat::dump(const std::string &dir) {
  if (utils::isFolderExist(dir.c_str()) == 0) {
    utils::make_dir(dir.c_str());
  }
  std::string index_param_file_path = dir + "/index_param_file.txt";
  utils::FileIO index_param_io(index_param_file_path);
  index_param_io.Open("w");
  index_param_io.Write(index_param.c_str(), index_param.length(), 1);
  return Dump(dir);
}

Status IndexIVFFlat::load(const std::string &dir, int &load_num) {
  std::string index_param_file_path = dir + "/index_param_file.txt";
  long file_size = utils::get_file_size(index_param_file_path);
  char index_param_str[file_size];
  utils::FileIO index_param_io(index_param_file_path);
  index_param_io.Open("r");
  index_param_io.Read(index_param_str, file_size, 1);
  index_param = std::string(index_param_str, file_size);
  init(index_param);
  return Load(dir, load_num);
}

IndexIVFPQ::IndexIVFPQ(faiss::Index *quantizer, size_t d, size_t nlist,
                       size_t M, size_t nbits_per_idx,
                       faiss::MetricType metric) {
  this->d = d;
  this->nlist = nlist;
  this->metric_type = metric;
  this->quantizer = quantizer;
  if (metric_type == faiss::METRIC_L2)
    index_param =
        "{\"metric_type\" : \"L2\", \"ncentroids\" : " + std::to_string(nlist) +
        ", \"nsubvector\":" + std::to_string(M) + "}";
  else
    index_param = "{\"metric_type\" : \"InnerProduct\", \"ncentroids\" : " +
                  std::to_string(nlist) +
                  ", \"nsubvector\":" + std::to_string(M) + "}";
  this->init();
}

IndexIVFPQ::~IndexIVFPQ() {
  delete raw_vector_;
  raw_vector_ = nullptr;
  delete docids_bitmap_;
  docids_bitmap_ = nullptr;
}

Status IndexIVFPQ::init(const std::string &index_param) {
  if (index_param != "") {
    this->index_param = index_param;
  }
  LOG(INFO) << "index params=" << this->index_param;

  VectorStorageType storage_type = VectorStorageType::MemoryOnly;
  vec_name = "gamma";

  std::string index_root_path_ = ".";
  VectorValueType value_type = VectorValueType::FLOAT;

  std::string vec_root_path = index_root_path_ + "/vectors";
  if (utils::make_dir(vec_root_path.c_str())) {
    std::string msg =
        std::string("make directory error, path=") + vec_root_path;
    LOG(ERROR) << msg;
    return Status::PathNotFound(msg);
  }
  VectorMetaInfo *meta_info = new VectorMetaInfo(vec_name, d, value_type);
  meta_info->with_io_ = false;

  StoreParams store_params(meta_info->AbsoluteName());
  std::string store_param = "";
  if (store_param != "") {
    Status status = store_params.Parse(store_param.c_str());
    if (!status.ok()) return status;
  }

  LOG(INFO) << "store params=" << store_params.ToJsonStr();

  docids_bitmap_ = new bitmap::RocksdbBitmapManager();
  int init_bitmap_size = 1000 * 10000;
  if (docids_bitmap_->Init(init_bitmap_size, index_root_path_ + "/bitmap") != 0) {
    std::string msg = "Cannot create bitmap!";
    LOG(ERROR) << msg;
    return Status::IOError(msg);
  }

  raw_vector_ = RawVectorFactory::Create(meta_info, storage_type, vec_root_path,
                                         store_params, docids_bitmap_);
  if (raw_vector_ == nullptr) {
    std::string msg = "create raw vector error";
    LOG(ERROR) << msg;
    return Status::IOError(msg);
  }
  LOG(INFO) << "create raw vector success, vec_name[" << vec_name << "]";

  int ret = raw_vector_->Init(vec_name, false);
  if (ret != 0) {
    std::string msg = std::string("Raw vector ") + vec_name +
                      " init error, code [" + std::to_string(ret) + "]!";
    LOG(ERROR) << msg;
    delete raw_vector_;
    return Status::IOError(msg);
  }

  vector_ = raw_vector_;
  Status status;

#ifdef OPT_IVFPQ_RELAYOUT
  status = GammaIndexIVFPQRelayout::Init(this->index_param, 100000);
  if (!status.ok()) {
    LOG(ERROR) << "gamma index init " << vec_name << " error!";
    return status;
  }
#else
  status = GammaIVFPQIndex::Init(this->index_param, 100000);
  if (!status.ok()) {
    LOG(ERROR) << "gamma index init " << vec_name << " error!";
    return status;
  }
#endif
  // init indexed count
  indexed_count_ = 0;
  return Status::OK();
}

Status IndexIVFPQ::init() { return init(index_param); }

void IndexIVFPQ::train(idx_t n, const float *x) {
  faiss::IndexIVFPQ::train(n, x);
}

void IndexIVFPQ::add(idx_t n, const float *x) { Add(n, (const uint8_t *)x); }

void IndexIVFPQ::search(idx_t n, const float *x, idx_t k, float *distances,
                        idx_t *labels) {
  PerfTool perf_tool;
  SearchCondition *condition = new SearchCondition(&perf_tool);
  condition->topn = k;
  condition->Init(std::numeric_limits<float>::lowest(),
                  std::numeric_limits<float>::max(), nullptr, nullptr);
  Search(condition, n, (const uint8_t *)x, k, distances, labels);
  delete condition;
}

Status IndexIVFPQ::dump(const std::string &dir) {
  if (utils::isFolderExist(dir.c_str()) == 0) {
    utils::make_dir(dir.c_str());
  }
  std::string index_param_file_path = dir + "/index_param_file.txt";
  utils::FileIO index_param_io(index_param_file_path);
  index_param_io.Open("w");
  index_param_io.Write(index_param.c_str(), index_param.length(), 1);
  return Dump(dir);
}

Status IndexIVFPQ::load(const std::string &dir, int &load_num) {
  std::string index_param_file_path = dir + "/index_param_file.txt";
  long file_size = utils::get_file_size(index_param_file_path);
  char index_param_str[file_size];
  utils::FileIO index_param_io(index_param_file_path);
  index_param_io.Open("r");
  index_param_io.Read(index_param_str, file_size, 1);
  index_param = std::string(index_param_str, file_size);
  init(index_param);
#ifdef OPT_IVFPQ_RELAYOUT
  check_vector_size_ = false;
#endif
  return Load(dir, load_num);
}

#ifdef USE_SCANN
IndexScann::IndexScann(size_t d, size_t nlist, size_t M,
                       faiss::MetricType metric) {
  this->d_ = d;
  if (metric == faiss::METRIC_L2)
    index_param =
        "{\"metric_type\" : \"L2\", \"ncentroids\" : " + std::to_string(nlist) +
        ", \"nsubvector\":" + std::to_string(M) + "}";
  else
    index_param = "{\"metric_type\" : \"InnerProduct\", \"ncentroids\" : " +
                  std::to_string(nlist) +
                  ", \"nsubvector\":" + std::to_string(M) + "}";
  this->init();
}

IndexScann::~IndexScann() {
  delete raw_vector_;
  raw_vector_ = nullptr;
  delete docids_bitmap_;
  docids_bitmap_ = nullptr;
}

int IndexScann::init(const std::string &index_param) {
  if (index_param != "") {
    this->index_param = index_param;
  }
  LOG(INFO) << "index params=" << this->index_param;

  VectorStorageType storage_type = VectorStorageType::MemoryOnly;
  vec_name = "gamma";

  std::string index_root_path_ = ".";
  VectorValueType value_type = VectorValueType::FLOAT;

  std::string vec_root_path = index_root_path_ + "/vectors";
  if (utils::make_dir(vec_root_path.c_str())) {
    LOG(ERROR) << "make directory error, path=" << vec_root_path;
    return -2;
  }
  VectorMetaInfo *meta_info = new VectorMetaInfo(vec_name, d_, value_type);
  meta_info->with_io_ = false;

  StoreParams store_params(meta_info->AbsoluteName());
  std::string store_param = "";
  if (store_param != "" && store_params.Parse(store_param.c_str())) {
    return PARAM_ERR;
  }

  LOG(INFO) << "store params=" << store_params.ToJsonStr();

  docids_bitmap_ = new bitmap::RocksdbBitmapManager();
  int init_bitmap_size = 1000 * 10000;
  if (docids_bitmap_->Init(init_bitmap_size, index_root_path_ + "/bitmap") != 0) {
    LOG(ERROR) << "Cannot create bitmap!";
    return INTERNAL_ERR;
  }

  raw_vector_ = RawVectorFactory::Create(meta_info, storage_type, vec_root_path,
                                         store_params, docids_bitmap_);
  if (raw_vector_ == nullptr) {
    LOG(ERROR) << "create raw vector error";
    return -1;
  }
  LOG(INFO) << "create raw vector success, vec_name[" << vec_name << "]";

  int ret = raw_vector_->Init(vec_name, false, false);
  if (ret != 0) {
    LOG(ERROR) << "Raw vector " << vec_name << " init error, code [" << ret
               << "]!";
    delete raw_vector_;
    return -1;
  }

  vector_ = raw_vector_;

  if (GammaVearchIndex::Init(this->index_param, 100000) != 0) {
    LOG(ERROR) << "gamma index init " << vec_name << " error!";
    return -1;
  }
  // init indexed count
  indexed_count_ = 0;
  return 0;
}

int IndexScann::init() { return init(index_param); }

void IndexScann::train(idx_t n, const float *x) {
  ScannTraining(vearch_index_, (const char *)x, n * d_ * sizeof(float), d_, 0);
  is_trained_ = true;
}

void IndexScann::add(idx_t n, const float *x) { Add(n, (const uint8_t *)x); }

void IndexScann::search(idx_t n, const float *x, idx_t k, float *distances,
                        idx_t *labels) {
  PerfTool perf_tool;
  SearchCondition *condition = new SearchCondition(&perf_tool);
  condition->topn = k;
  condition->Init(std::numeric_limits<float>::lowest(),
                  std::numeric_limits<float>::max(), nullptr, nullptr);
  Search(condition, n, (const uint8_t *)x, k, distances, labels);
  delete condition;
}

int IndexScann::dump(const std::string &dir) {
  LOG(INFO) << "load not support now!";
  return -1;
}

int IndexScann::load(const std::string &dir) {
  LOG(INFO) << "load not support now!";
  return -1;
}
#endif  // USE_SCANN

vearch::Index *index_factory(int d, const char *description_in,
                             faiss::MetricType metric) {
  faiss::Index *coarse_quantizer = nullptr;
  vearch::Index *index = nullptr;

  faiss::ScopeDeleter1<faiss::Index> del_coarse_quantizer;
  faiss::ScopeDeleter1<vearch::Index> del_index;

  std::string description(description_in);
  char *ptr;

  int64_t ncentroids = -1;

  for (char *tok = strtok_r(&description[0], " ,", &ptr); tok;
       tok = strtok_r(nullptr, " ,", &ptr)) {
    int nbit, M;
    std::string stok(tok);
    nbit = 8;

    // to avoid mem leaks with exceptions:
    // do all tests before any instanciation

    faiss::Index *coarse_quantizer_1 = nullptr;
    vearch::Index *index_1 = nullptr;

    // coarse quantizers
    if (!coarse_quantizer && sscanf(tok, "IVF%ld", &ncentroids) == 1) {
      if (metric == faiss::METRIC_L2) {
        coarse_quantizer_1 = new faiss::IndexFlatL2(d);
      } else {
        coarse_quantizer_1 = new faiss::IndexFlatIP(d);
      }
      // IVFs
    } else if (!index && (stok == "Flat")) {
      if (coarse_quantizer) {
        // if there was an IVF in front, then it is an IVFFlat
        IndexIVFFlat *index_ivf =
            new IndexIVFFlat(coarse_quantizer, d, ncentroids, metric);
        index_ivf->quantizer_trains_alone = 0;
        index_ivf->cp.spherical = metric == faiss::METRIC_INNER_PRODUCT;
        del_coarse_quantizer.release();
        index_ivf->own_fields = true;
        index_1 = index_ivf;
      }
      //  else {
      //     index_1 = new IndexFlat (d, metric);
      // }
    } else if (!index && (sscanf(tok, "PQ%dx%d", &M, &nbit) == 2 ||
                          sscanf(tok, "PQ%d", &M) == 1)) {
      if (coarse_quantizer) {
        IndexIVFPQ *index_ivf =
            new IndexIVFPQ(coarse_quantizer, d, ncentroids, M, nbit);
        index_ivf->quantizer_trains_alone = 0;
        index_ivf->metric_type = metric;
        index_ivf->cp.spherical = metric == faiss::METRIC_INNER_PRODUCT;
        del_coarse_quantizer.release();
        index_ivf->own_fields = true;
        index_ivf->do_polysemous_training = false;
        index_1 = index_ivf;
      }
    } else {
      printf("could not parse token \"%s\" in %s\n", tok, description_in);
    }

    if (coarse_quantizer_1) {
      coarse_quantizer = coarse_quantizer_1;
      del_coarse_quantizer.set(coarse_quantizer);
    }

    if (index_1) {
      index = index_1;
      del_index.set(index);
    }
  }

  assert(index != nullptr);

  // nothing can go wrong now
  del_index.release();
  del_coarse_quantizer.release();

  return index;
}

}  // namespace vearch
