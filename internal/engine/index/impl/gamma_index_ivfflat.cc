/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This faiss source code is licensed under the MIT license.
 * https://github.com/facebookresearch/faiss/blob/master/LICENSE
 *
 *
 * The works below are modified based on faiss:
 * 1. Replace the static batch indexing with real time indexing
 * 2. Add the numeric field and bitmap filters in the process of searching
 *
 * Modified works copyright 2019 The Gamma Authors.
 *
 * The modified codes are licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 *
 */

#include "gamma_index_ivfflat.h"

#include <faiss/IndexFlat.h>
#include <faiss/IndexIVFFlat.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/utils/Heap.h>
#include <faiss/utils/utils.h>
#include <omp.h>

#include <cstdio>
#include <cstdlib>
#include <memory>

#include "common/gamma_common_data.h"
#include "index/index_io.h"
#include "vector/rocksdb_raw_vector.h"
namespace vearch {

using namespace faiss;

struct IVFFlatModelParams {
  int ncentroids;  // coarse cluster center number
  int nprobe;      // search how many bucket
  DistanceComputeType metric_type;
  int bucket_init_size;  // original size of RTInvertIndex bucket
  int bucket_max_size;   // max size of RTInvertIndex bucket
  int training_threshold;

  IVFFlatModelParams() {
    ncentroids = 2048;
    nprobe = 80;
    metric_type = DistanceComputeType::INNER_PRODUCT;
    bucket_init_size = 1000;
    bucket_max_size = 1280000;
  }

  Status Parse(const char *str) {
    utils::JsonParser jp;
    if (jp.Parse(str)) {
      std::string msg =
          std::string("parse IVFFLAT model parameters error: ") + str;
      LOG(ERROR) << msg;
      return Status::ParamError(msg);
    }

    // -1 as default
    int nc = 0;
    int nprobe = 0;
    if (jp.Contains("ncentroids")) {
      if (jp.GetInt("ncentroids", nc)) {
        std::string msg = "parse ncentroids error";
        LOG(ERROR) << msg;
        return Status::ParamError(msg);
      }
      if (nc > 0) {
        ncentroids = nc;
      }
    }
    if (!jp.GetInt("nprobe", nprobe)) {
      if (nprobe < -1) {
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

    int bucket_init_size;
    int bucket_max_size;

    // -1 as default
    if (!jp.GetInt("bucket_init_size", bucket_init_size)) {
      if (bucket_init_size < -1) {
        std::string msg = std::string("invalid bucket_init_size =") +
                          std::to_string(bucket_init_size);
        return Status::ParamError(msg);
      }
      if (bucket_init_size > 0) this->bucket_init_size = bucket_init_size;
    }

    if (!jp.GetInt("bucket_max_size", bucket_max_size)) {
      if (bucket_max_size < -1) {
        std::string msg = std::string("invalid bucket_max_size =") +
                          std::to_string(bucket_max_size);
        LOG(ERROR) << msg;
        return Status::ParamError(msg);
      }
      if (bucket_max_size > 0) this->bucket_max_size = bucket_max_size;
    }

    std::string metric_type;

    if (!jp.GetString("metric_type", metric_type)) {
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

    return Status::OK();
  }

  std::string ToString() {
    std::stringstream ss;
    ss << "ncentroids =" << ncentroids << ", ";
    ss << "nprobe =" << nprobe << ", ";
    ss << "metric_type =" << (int)metric_type << ", ";
    ss << "bucket_init_size =" << bucket_init_size << ", ";
    ss << "bucket_max_size =" << bucket_max_size << ", ";
    ss << "training_threshold = " << training_threshold;

    return ss.str();
  }
};

REGISTER_INDEX(IVFFLAT, GammaIVFFlatIndex);

GammaIVFFlatIndex::GammaIVFFlatIndex() {
  indexed_vec_count_ = 0;
  updated_num_ = 0;
  rt_invert_index_ptr_ = nullptr;
#ifdef PERFORMANCE_TESTING
  add_count_ = 0;
#endif
}

GammaIVFFlatIndex::~GammaIVFFlatIndex() {
  CHECK_DELETE(rt_invert_index_ptr_);
  CHECK_DELETE(invlists);
  CHECK_DELETE(quantizer);
}

Status GammaIVFFlatIndex::Init(const std::string &model_parameters,
                               int training_threshold) {
  IVFFlatModelParams params;
  if (model_parameters != "") {
    Status status = params.Parse(model_parameters.c_str());
    if (!status.ok()) {
      LOG(ERROR) << status.ToString();
      return status;
    }
  }

  RawVector *raw_vec = nullptr;
  raw_vec = dynamic_cast<RawVector *>(vector_);
  if (raw_vec == nullptr) {
    std::string msg = "IVFFlat needs store type=RocksDB";
    LOG(ERROR) << msg;
    return Status::ParamError(msg);
  }

  d = vector_->MetaInfo()->Dimension();
  nlist = params.ncentroids;
  if (training_threshold) {
    training_threshold_ = training_threshold;
  } else {
    training_threshold_ = nlist * default_points_per_centroid;
  }
  params.training_threshold = training_threshold_;

  LOG(INFO) << params.ToString();

  quantizer = new faiss::IndexFlatL2(d);
  own_fields = false;
  code_size = sizeof(float) * d;
  is_trained = false;

  rt_invert_index_ptr_ = new realtime::RTInvertIndex(
      this->nlist, this->code_size, raw_vec->Bitmap(), params.bucket_init_size,
      params.bucket_max_size);

  delete this->invlists;
  this->invlists = nullptr;

  bool ret = rt_invert_index_ptr_->Init();
  if (!ret) {
    std::string msg = "init realtime invert index error";
    LOG(ERROR) << msg;
    return Status::ParamError(msg);
  }
  this->invlists =
      new realtime::RTInvertedLists(rt_invert_index_ptr_, nlist, code_size);
  own_invlists = false;

  metric_type_ = params.metric_type;
  if (metric_type_ == DistanceComputeType::INNER_PRODUCT) {
    metric_type = faiss::METRIC_INNER_PRODUCT;
  } else {
    metric_type = faiss::METRIC_L2;
  }

  if ((size_t)params.nprobe <= this->nlist)
    this->nprobe = params.nprobe;
  else
    this->nprobe = size_t(this->nlist / 2);
  return Status::OK();
}

RetrievalParameters *GammaIVFFlatIndex::Parse(const std::string &parameters) {
  if (parameters == "") {
    return new IVFFlatRetrievalParameters(this->nprobe, metric_type_);
  }

  utils::JsonParser jp;
  if (jp.Parse(parameters.c_str())) {
    LOG(ERROR) << "parse retrieval parameters error: " << parameters;
    return nullptr;
  }

  std::string metric_type;
  IVFFlatRetrievalParameters *retrieval_params =
      new IVFFlatRetrievalParameters(this->nprobe, metric_type_);
  if (!jp.GetString("metric_type", metric_type)) {
    if (!strcasecmp("L2", metric_type.c_str())) {
      retrieval_params->SetDistanceComputeType(DistanceComputeType::L2);
    } else if (!strcasecmp("InnerProduct", metric_type.c_str())) {
      retrieval_params->SetDistanceComputeType(
          DistanceComputeType::INNER_PRODUCT);
    } else {
      LOG(ERROR) << "invalid metric_type = " << metric_type
                 << ", so use default value.";
      retrieval_params->SetDistanceComputeType(metric_type_);
    }
  } else {
    retrieval_params->SetDistanceComputeType(metric_type_);
  }

  int nprobe;
  int parallel_on_queries;

  if (!jp.GetInt("nprobe", nprobe)) {
    if (nprobe > 0) {
      retrieval_params->SetNprobe(nprobe);
    }
  }

  if (!jp.GetInt("parallel_on_queries", parallel_on_queries)) {
    if (parallel_on_queries != 0) {
      retrieval_params->SetParallelOnQueries(true);
    } else {
      retrieval_params->SetParallelOnQueries(false);
    }
  }

  return retrieval_params;
}

int GammaIVFFlatIndex::Indexing() {
  if (this->is_trained) {
    LOG(INFO) << "gamma GammaIVFFlatIndex is already trained, skip indexing";
    return 0;
  }
  RawVector *raw_vec = dynamic_cast<RawVector *>(vector_);
  size_t vectors_count = raw_vec->MetaInfo()->Size();

  size_t num;
  if ((size_t)training_threshold_ < nlist) {
    num = nlist * 39;
    LOG(WARNING) << "Because training_threshold[" << training_threshold_
                 << "] < ncentroids[" << nlist
                 << "], training_threshold becomes ncentroids * 39[" << num
                 << "].";
  } else if ((size_t)training_threshold_ <= nlist * 256) {
    if ((size_t)training_threshold_ < nlist * 39) {
      LOG(WARNING)
          << "training_threshold[" << training_threshold_ << "] is too small. "
          << "The appropriate range is [ncentroids * 39, ncentroids * 256]";
    }
    num = training_threshold_;
  } else {
    num = nlist * 256;
    LOG(WARNING)
        << "training_threshold[" << training_threshold_ << "] is too big. "
        << "The appropriate range is [ncentroids * 39, ncentroids * 256]."
        << "training_threshold becomes ncentroids * 256[" << num << "].";
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
    if (num > n_get) {
      LOG(ERROR) << "training vector get count [" << n_get
                 << "] less then training_threshold[" << num << "], failed!";
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

  IndexIVFFlat::train(n_get, (const float *)train_raw_vec);

  LOG(INFO) << "train successed!";
  return 0;
}

bool GammaIVFFlatIndex::Add(int n, const uint8_t *vec) {
#ifdef PERFORMANCE_TESTING
  double t0 = faiss::getmillisecs();
#endif
  if (not is_trained) {
    return 0;
  }
  std::map<int, std::vector<long>> new_keys;
  std::map<int, std::vector<uint8_t>> new_codes;

  idx_t *idx;
  utils::ScopeDeleter<idx_t> del_idx;

  idx_t *idx0 = new idx_t[n];
  quantizer->assign(n, (const float *)vec, idx0);
  idx = idx0;
  del_idx.set(idx);

  uint8_t *xcodes = new uint8_t[n * code_size];
  utils::ScopeDeleter<uint8_t> del_xcodes(xcodes);

  long vid = indexed_vec_count_;
  int n_add = 0;
  RawVector *raw_vec = dynamic_cast<RawVector *>(vector_);
  for (int i = 0; i < n; i++) {
    long key = idx[i];
    if (raw_vec->Bitmap()->Test(vid + i)) {
      continue;
    }
    if (key >= (long)nlist) {
      LOG(WARNING) << "ivfflat add invalid key=" << key << ", vid=" << vid + i;
      continue;
    }
    if (key < 0) {
      LOG(WARNING) << "ivfflat add invalid key=" << key << ", vid=" << vid + i;
      key = vid % nlist;
    }
    uint8_t *code = (uint8_t *)vec + this->code_size * i;
    new_keys[key].push_back(vid + i);
    size_t ofs = new_codes[key].size();
    new_codes[key].resize(ofs + code_size);
    memcpy((void *)(new_codes[key].data() + ofs), (void *)code, code_size);
    n_add +=1;
  }

  /* stage 2 : add invert info to invert index */
  if (!rt_invert_index_ptr_->AddKeys(new_keys, new_codes)) {
    return false;
  }
  indexed_vec_count_ += n;
#ifdef PERFORMANCE_TESTING
  add_count_ += n;
  if (add_count_ >= 10000) {
    double t1 = faiss::getmillisecs();
    LOG(DEBUG) << "Add time [" << (t1 - t0) / n << "]ms, count "
               << indexed_vec_count_ << " wanted n=" << n
               << " real add=" << n_add;
    add_count_ = 0;
  }
#endif
  return true;
}

void GammaIVFFlatIndex::Describe() {
  if (rt_invert_index_ptr_) rt_invert_index_ptr_->PrintBucketSize();
}

int GammaIVFFlatIndex::Update(const std::vector<int64_t> &ids,
                              const std::vector<const uint8_t *> &vecs) {
  if (not is_trained) {
    return 0;
  }
  int n_update = 0;
  for (size_t i = 0; i < ids.size(); i++) {
    if (ids[i] < 0) {
      LOG(WARNING) << "ivfflat update invalid id=" << ids[i];
      continue;
    }
    if (vecs[i] == nullptr) {
      continue;
    }
    const float *vec = reinterpret_cast<const float *>(vecs[i]);
    if (vec == nullptr) {
      continue;
    }
    idx_t idx = -1;
    quantizer->assign(1, vec, &idx);
    std::vector<uint8_t> code;
    code.resize(this->code_size);
    memcpy(code.data(), (void *)vec, this->code_size);
    rt_invert_index_ptr_->Update(idx, ids[i], code);
    n_update++;
  }
  updated_num_ += n_update;
  LOG(DEBUG) << "update index success! size=" << ids.size()
             << ", n_update=" << n_update << ", updated_num="
             << updated_num_;
  // now check id need to do compaction
  rt_invert_index_ptr_->CompactIfNeed();
  return 0;
}

int GammaIVFFlatIndex::Delete(const std::vector<int64_t> &ids) {
  if (not is_trained) {
    return 0;
  }
  std::vector<int64_t> vids(ids.begin(), ids.end());
  rt_invert_index_ptr_->Delete(vids.data(), vids.size());
  return 0;
}

int GammaIVFFlatIndex::Search(RetrievalContext *retrieval_context, int n,
                              const uint8_t *rx, int k, float *distances,
                              idx_t *labels) {
#ifndef FAISSLIKE_INDEX
  IVFFlatRetrievalParameters *retrieval_params =
      dynamic_cast<IVFFlatRetrievalParameters *>(
          retrieval_context->RetrievalParams());

  utils::ScopeDeleter1<IVFFlatRetrievalParameters> del_params;
  if (retrieval_params == nullptr) {
    retrieval_params =
        new IVFFlatRetrievalParameters(this->nprobe, metric_type_);
    del_params.set(retrieval_params);
  }

  SearchCondition *condition =
      dynamic_cast<SearchCondition *>(retrieval_context);
  if (condition->brute_force_search == true || is_trained == false) {
    // reset retrieval_params
    delete retrieval_context->RetrievalParams();
    retrieval_context->retrieval_params_ =
        new FlatRetrievalParameters(retrieval_params->ParallelOnQueries(),
                                    retrieval_params->GetDistanceComputeType());
    int ret =
        GammaFLATIndex::Search(retrieval_context, n, rx, k, distances, labels);
    return ret;
  }
  int nprobe = this->nprobe;
  if (retrieval_params->Nprobe() > 0 &&
      (size_t)retrieval_params->Nprobe() <= this->nlist) {
    nprobe = retrieval_params->Nprobe();
  } else {
    LOG(WARNING) << "nlist = " << this->nlist
                 << ", nprobe = " << retrieval_params->Nprobe()
                 << ", invalid, now use:" << this->nprobe;
  }
#else
  int nprobe = this->nprobe;
#endif
  const float *x = reinterpret_cast<const float *>(rx);

  std::unique_ptr<idx_t[]> idx(new idx_t[n * nprobe]);
  std::unique_ptr<float[]> coarse_dis(new float[n * nprobe]);

  quantizer->search(n, x, nprobe, coarse_dis.get(), idx.get());

  search_preassigned(retrieval_context, n, x, k, idx.get(), coarse_dis.get(),
                     distances, labels, nprobe, false);
  if (RequestContext::is_killed()) {
    return -2;
  }

  return 0;
}

void GammaIVFFlatIndex::search_preassigned(RetrievalContext *retrieval_context,
                                           idx_t n, const float *x, int k,
                                           const idx_t *keys,
                                           const float *coarse_dis,
                                           float *distances, idx_t *labels,
                                           int nprobe, bool store_pairs) {
  IVFFlatRetrievalParameters *retrieval_params =
      dynamic_cast<IVFFlatRetrievalParameters *>(
          retrieval_context->RetrievalParams());
  utils::ScopeDeleter1<IVFFlatRetrievalParameters> del_params;
  if (retrieval_params == nullptr) {
    retrieval_params =
        new IVFFlatRetrievalParameters(this->nprobe, metric_type_);
    del_params.set(retrieval_params);
  }

  faiss::MetricType metric_type;
  if (retrieval_params->GetDistanceComputeType() ==
      DistanceComputeType::INNER_PRODUCT) {
    metric_type = faiss::METRIC_INNER_PRODUCT;
  } else {
    metric_type = faiss::METRIC_L2;
  }

  long max_codes = 1000000000;
  size_t nlistv = 0, ndis = 0, nheap = 0;

  using HeapForIP = CMin<float, idx_t>;
  using HeapForL2 = CMax<float, idx_t>;

  bool interrupt = false;

  int pmode = retrieval_params->ParallelOnQueries() ? 0 : 1;
  bool do_parallel = pmode == 0 ? n > 1 : nprobe > 1;

  if (RequestContext::is_killed()) {
    return;
  }
  RawData *request = RequestContext::get_current_request();
  int partition_id = RequestContext::get_partition_id();

#pragma omp parallel if (do_parallel) reduction(+ : nlistv, ndis, nheap)
  {
    faiss::InvertedListScanner *scanner =
        GetGammaInvertedListScanner(store_pairs, nullptr, retrieval_context, metric_type);
    utils::ScopeDeleter1<faiss::InvertedListScanner> del(scanner);

    if (RequestContext::get_current_request() == nullptr) {
      RequestContext::ScopedContext(request, partition_id);
    }

    /*****************************************************
     * Depending on parallel_mode, there are two possible ways
     * to organize the search. Here we define local functions
     * that are in common between the two
     ******************************************************/

    // intialize + reorder a result heap

    auto init_result = [&](float *simi, idx_t *idxi) {
      if (metric_type == METRIC_INNER_PRODUCT) {
        heap_heapify<HeapForIP>(k, simi, idxi);
      } else {
        heap_heapify<HeapForL2>(k, simi, idxi);
      }
    };

    auto reorder_result = [&](float *simi, idx_t *idxi) {
      if (metric_type == METRIC_INNER_PRODUCT) {
        heap_reorder<HeapForIP>(k, simi, idxi);
      } else {
        heap_reorder<HeapForL2>(k, simi, idxi);
      }
    };

    // single list scan using the current scanner (with query
    // set porperly) and storing results in simi and idxi
    auto scan_one_list = [&](idx_t key, float coarse_dis_i, float *simi,
                             idx_t *idxi) {
      if (key < 0) {
        // not enough centroids for multiprobe
        return (size_t)0;
      }
      FAISS_THROW_IF_NOT_FMT(key < (idx_t)nlist, "Invalid key=%ld nlist=%ld\n",
                             key, nlist);

      size_t list_size = invlists->list_size(key);

      // don't waste time on empty lists
      if (list_size == 0) {
        return (size_t)0;
      }

      scanner->set_list(key, coarse_dis_i);

      nlistv++;

      InvertedLists::ScopedCodes scodes(invlists, key);

      std::unique_ptr<InvertedLists::ScopedIds> sids;
      const faiss::idx_t *ids = nullptr;

      if (!store_pairs) {
        sids.reset(new InvertedLists::ScopedIds(invlists, key));
        ids = sids->get();
      }

      nheap += scanner->scan_codes(list_size, scodes.get(), ids, simi, idxi, k);

      return list_size;
    };

    /****************************************************
     * Actual loops, depending on parallel_mode
     ****************************************************/

    if (pmode == 0) {
#pragma omp for
      for (idx_t i = 0; i < n; i++) {
        if (!RequestContext::is_killed()) {
          if (interrupt) {
            continue;
          }

          // loop over queries
          scanner->set_query(x + i * d);
          float *simi = distances + i * k;
          idx_t *idxi = labels + i * k;

          init_result(simi, idxi);

          long nscan = 0;

          // loop over probes
          for (idx_t ik = 0; ik < nprobe; ik++) {
            if (RequestContext::is_killed()) {
              break;
            }
            nscan += scan_one_list(keys[i * nprobe + ik],
                                 coarse_dis[i * nprobe + ik], simi, idxi);

            if (max_codes && nscan >= max_codes) {
              break;
            }
          }

          ndis += nscan;

          reorder_result(simi, idxi);

        // if (faiss::InterruptCallback::is_interrupted()) {
        //   interrupt = true;
        // }
        }
      }  // parallel for
    } else if (pmode == 1) {
      std::vector<idx_t> local_idx(k);
      std::vector<float> local_dis(k);

      for (idx_t i = 0; i < n; i++) {
        scanner->set_query(x + i * d);
        init_result(local_dis.data(), local_idx.data());

#pragma omp for schedule(dynamic)
        for (idx_t ik = 0; ik < nprobe; ik++) {
          if (RequestContext::is_killed()) {
            continue;
          }
          ndis +=
              scan_one_list(keys[i * nprobe + ik], coarse_dis[i * nprobe + ik],
                            local_dis.data(), local_idx.data());

          // can't do the test on max_codes
        }
        // merge thread-local results

        float *simi = distances + i * k;
        idx_t *idxi = labels + i * k;
#pragma omp single
        init_result(simi, idxi);

#pragma omp barrier
#pragma omp critical
        {
          if (!RequestContext::is_killed()) {
            if (metric_type == METRIC_INNER_PRODUCT) {
              heap_addn<HeapForIP>(k, simi, idxi, local_dis.data(),
                                   local_idx.data(), k);
            } else {
              heap_addn<HeapForL2>(k, simi, idxi, local_dis.data(),
                                   local_idx.data(), k);
            }
          }
        }
#pragma omp barrier
#pragma omp single
        if (!RequestContext::is_killed()) {
          reorder_result(simi, idxi);
        }
      }
    } else {
      FAISS_THROW_FMT("parallel_mode %d not supported\n", pmode);
    }
  }  // parallel section

  if (interrupt) {
    FAISS_THROW_MSG("computation interrupted");
  }
}

std::string IVFFlatToString(const faiss::IndexIVFFlat *ivfl) {
  std::stringstream ss;
  ss << "d=" << ivfl->d << ", ntotal=" << ivfl->ntotal
     << ", is_trained=" << ivfl->is_trained
     << ", metric_type=" << ivfl->metric_type << ", nlist=" << ivfl->nlist
     << ", nprobe=" << ivfl->nprobe;

  return ss.str();
}

Status GammaIVFFlatIndex::Dump(const std::string &dir) {
  if (!this->is_trained) {
    LOG(INFO) << "gamma index is not trained, skip dumping";
    return Status::OK();
  }
  std::string index_name = vector_->MetaInfo()->AbsoluteName();
  std::string index_dir = dir + "/" + index_name;
  if (utils::make_dir(index_dir.c_str())) {
    std::string msg = std::string("mkdir error, index dir=") + index_dir;
    LOG(ERROR) << msg;
    return Status::PathNotFound(msg);
  }

  std::string index_file = index_dir + "/ivfflat.index";
  faiss::IOWriter *f = new FileIOWriter(index_file.c_str());
  utils::ScopeDeleter1<FileIOWriter> del((FileIOWriter *)f);
  const IndexIVFFlat *ivfl = static_cast<const IndexIVFFlat *>(this);
  uint32_t h = faiss::fourcc("IvFl");
  WRITE1(h);
  vearch::write_ivf_header(ivfl, f);

  int indexed_count = indexed_vec_count_;
  if (WriteInvertedLists(f, rt_invert_index_ptr_)) {
    std::string msg =
        std::string("write invert list error, index name=") + index_name;
    LOG(ERROR) << msg;
    return Status::IOError(msg);
  }
  WRITE1(indexed_count);

  LOG(INFO) << "dump:" << IVFFlatToString(ivfl)
            << ", indexed count=" << indexed_count;
  return Status::OK();
};

Status GammaIVFFlatIndex::Load(const std::string &dir, int64_t &load_num) {
  std::string index_name = vector_->MetaInfo()->AbsoluteName();
  std::string index_file = dir + "/" + index_name + "/ivfflat.index";
  if (!utils::file_exist(index_file)) {
    LOG(INFO) << index_file << " isn't existed, skip loading";
    load_num = 0;
    return Status::OK();  // it should train again after load
  }

  faiss::IOReader *f = new FileIOReader(index_file.c_str());
  utils::ScopeDeleter1<FileIOReader> del((FileIOReader *)f);
  uint32_t h;
  READ1(h);
  assert(h == faiss::fourcc("IvFl"));
  IndexIVFFlat *ivfl = static_cast<IndexIVFFlat *>(this);
  vearch::read_ivf_header(ivfl, f, nullptr);  // not legacy

  int64_t indexed_vec_count = 0;
  Status status = ReadInvertedLists(f, rt_invert_index_ptr_, indexed_vec_count);
  if (status.code() == status::kIndexError) {
    indexed_vec_count_ = 0;
    load_num = 0;
    LOG(INFO) << "unsupported inverted list format, it need rebuilding!";
  } else if (status.ok()) {
    READ1(indexed_vec_count_);
    if (indexed_vec_count_ < 0) {
      std::string msg = std::string("invalid indexed count [") +
                        std::to_string(indexed_vec_count_) + "] vector size [" +
                        std::to_string(vector_->MetaInfo()->size_) + "]";
      LOG(ERROR) << msg;
      return Status::IndexError(msg);
    }
    LOG(INFO) << "load: " << IVFFlatToString(ivfl)
              << ", indexed vector count=" << indexed_vec_count_;
  } else {
    std::string msg =
        std::string("read invert list error, index name=") + index_name;
    LOG(ERROR) << msg;
    return Status::IndexError(msg);
  }
  assert(this->is_trained);
  load_num = indexed_vec_count_;
  return Status::OK();
};

faiss::InvertedListScanner *GammaIVFFlatIndex::GetGammaInvertedListScanner(
  bool store_pairs,
  const faiss::IDSelector* sel,
  const RetrievalContext* retrieval_context,
  faiss::MetricType metric_type) const {
  if (sel) {
    return get_InvertedListScanner1<true>(this, store_pairs, sel, retrieval_context, metric_type);
  } else {
    return get_InvertedListScanner1<false>(this, store_pairs, sel, retrieval_context, metric_type);
  }
}

}  // namespace vearch
