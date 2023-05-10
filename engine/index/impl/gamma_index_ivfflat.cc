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
#include "index/gamma_index_io.h"
#include "search/error_code.h"
#include "vector/rocksdb_raw_vector.h"

namespace tig_gamma {

using namespace faiss;

struct IVFFlatModelParams {
  int ncentroids;  // coarse cluster center number
  int nprobe;      // search how many bucket
  DistanceComputeType metric_type;

  IVFFlatModelParams() {
    ncentroids = 2048;
    nprobe = 80;
    metric_type = DistanceComputeType::INNER_PRODUCT;
  }

  int Parse(const char *str) {
    utils::JsonParser jp;
    if (jp.Parse(str)) {
      LOG(ERROR) << "parse IVFPQ model parameters error: " << str;
      return -1;
    }

    // -1 as default
    int nc = 0;
    int nprobe = 0;
    if (jp.Contains("ncentroids")) {
      if (jp.GetInt("ncentroids", nc)) {
        LOG(ERROR) << "parse ncentroids error";
        return PARAM_ERR;
      }
      if (nc > 0) {
        ncentroids = nc;
      } else if (nc <= 0 && nc != -1) {
        LOG(ERROR) << "invalid ncentroids=" << nc;
        return PARAM_ERR;
      }
    }
    if (!jp.GetInt("nprobe", nprobe)) {
      if (nprobe < -1) {
        LOG(ERROR) << "invalid nprobe =" << nprobe;
        return -1;
      }
      if (nprobe > 0) this->nprobe = nprobe;
      if (this->nprobe > this->ncentroids) {
        LOG(ERROR) << "nprobe should less than ncentroids";
        return -1;
      }
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

    return 0;
  }

  std::string ToString() {
    std::stringstream ss;
    ss << "ncentroids =" << ncentroids << ", ";
    ss << "nprobe =" << nprobe;
    return ss.str();
  }
};

REGISTER_MODEL(IVFFLAT, GammaIndexIVFFlat);

GammaIndexIVFFlat::GammaIndexIVFFlat() {
  indexed_vec_count_ = 0;
  updated_num_ = 0;
  rt_invert_index_ptr_ = nullptr;
#ifdef PERFORMANCE_TESTING
  add_count_ = 0;
#endif
}

GammaIndexIVFFlat::~GammaIndexIVFFlat() {
  CHECK_DELETE(rt_invert_index_ptr_);
  CHECK_DELETE(invlists);
  CHECK_DELETE(quantizer);
}

int GammaIndexIVFFlat::Init(const std::string &model_parameters,
                            int indexing_size) {
  indexing_size_ = indexing_size;
  IVFFlatModelParams params;
  if (params.Parse(model_parameters.c_str())) {
    LOG(ERROR) << "parse model parameters error";
    return PARAM_ERR;
  }
  LOG(INFO) << params.ToString();

  RawVector *raw_vec = nullptr;
  if (check_vector_) {
#ifdef WITH_ROCKSDB
    raw_vec = dynamic_cast<RocksDBRawVector *>(vector_);
#endif
  } else {
    raw_vec = dynamic_cast<RawVector *>(vector_);
  }
  if (raw_vec == nullptr) {
    LOG(ERROR) << "IVFFlat needs store type=RocksDB";
    return PARAM_ERR;
  }

  d = vector_->MetaInfo()->Dimension();
  nlist = params.ncentroids;
  quantizer = new faiss::IndexFlatL2(d);
  own_fields = false;
  code_size = sizeof(float) * d;
  is_trained = false;

  rt_invert_index_ptr_ = new realtime::RTInvertIndex(
      this->nlist, this->code_size, raw_vec->VidMgr(), raw_vec->Bitmap(),
      100000, 12800000);

  if (this->invlists) {
    delete this->invlists;
    this->invlists = nullptr;
  }

  bool ret = rt_invert_index_ptr_->Init();
  if (!ret) {
    LOG(ERROR) << "init realtime invert index error";
    return INTERNAL_ERR;
  }
  this->invlists =
      new realtime::RTInvertedLists(rt_invert_index_ptr_, nlist, code_size);
  own_invlists = false;

  if (params.metric_type == DistanceComputeType::INNER_PRODUCT) {
    metric_type = faiss::METRIC_INNER_PRODUCT;
  } else {
    metric_type = faiss::METRIC_L2;
  }
  this->nprobe = params.nprobe;

  LOG(INFO) << "d=" << d << ", nlist=" << nlist
            << ", metric_type=" << metric_type;
  return 0;
}

RetrievalParameters *GammaIndexIVFFlat::Parse(const std::string &parameters) {
  enum DistanceComputeType type;
  if (this->metric_type == faiss::METRIC_L2) {
    type = DistanceComputeType::L2;
  } else {
    type = DistanceComputeType::INNER_PRODUCT;
  }

  if (parameters == "") {
    return new IVFFlatRetrievalParameters(type);
  }

  utils::JsonParser jp;
  if (jp.Parse(parameters.c_str())) {
    LOG(ERROR) << "parse retrieval parameters error: " << parameters;
    return nullptr;
  }

  std::string metric_type;
  IVFFlatRetrievalParameters *retrieval_params =
      new IVFFlatRetrievalParameters();
  if (!jp.GetString("metric_type", metric_type)) {
    if (!strcasecmp("L2", metric_type.c_str())) {
      retrieval_params->SetDistanceComputeType(DistanceComputeType::L2);
    } else if (!strcasecmp("InnerProduct", metric_type.c_str())) {
      retrieval_params->SetDistanceComputeType(
          DistanceComputeType::INNER_PRODUCT);
    } else {
      LOG(ERROR) << "invalid metric_type = " << metric_type
                 << ", so use default value.";
      retrieval_params->SetDistanceComputeType(type);
    }
  } else {
    retrieval_params->SetDistanceComputeType(type);
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

int GammaIndexIVFFlat::Indexing() {
  if (this->is_trained) {
    LOG(INFO) << "gamma GammaIndexIVFFlat is already trained, skip indexing";
    return 0;
  }
  RawVector *raw_vec = dynamic_cast<RawVector *>(vector_);
  size_t vectors_count = raw_vec->MetaInfo()->Size();

  size_t num;
  if ((size_t)indexing_size_ < nlist) {
    num = nlist * 39;
    LOG(WARNING) << "Because index_size[" << indexing_size_ << "] < ncentroids["
                 << nlist << "], index_size becomes ncentroids * 39[" << num
                 << "].";
  } else if ((size_t)indexing_size_ <= nlist * 256) {
    if ((size_t)indexing_size_ < nlist * 39) {
      LOG(WARNING)
          << "Index_size[" << indexing_size_ << "] is too small. "
          << "The appropriate range is [ncentroids * 39, ncentroids * 256]";
    }
    num = indexing_size_;
  } else {
    num = nlist * 256;
    LOG(WARNING)
        << "Index_size[" << indexing_size_ << "] is too big. "
        << "The appropriate range is [ncentroids * 39, ncentroids * 256]."
        << "index_size becomes ncentroids * 256[" << num << "].";
  }
  if (num > vectors_count) {
    LOG(ERROR) << "vector total count [" << vectors_count
               << "] less then index_size[" << num << "], failed!";
    return -1;
  }

  ScopeVectors headers;
  std::vector<int> lens;
  raw_vec->GetVectorHeader(0, num, headers, lens);

  // merge vectors
  int raw_d = raw_vec->MetaInfo()->Dimension();
  const uint8_t *train_raw_vec = nullptr;
  utils::ScopeDeleter1<uint8_t> del_train_raw_vec;
  if (lens.size() == 1) {
    train_raw_vec = headers.Get(0);
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

  IndexIVFFlat::train(num, (const float *)train_raw_vec);

  LOG(INFO) << "train successed!";
  return 0;
}

bool GammaIndexIVFFlat::Add(int n, const uint8_t *vec) {
#ifdef PERFORMANCE_TESTING
  double t0 = faiss::getmillisecs();
#endif
  std::map<int, std::vector<long>> new_keys;
  std::map<int, std::vector<uint8_t>> new_codes;

  idx_t *idx;
  faiss::ScopeDeleter<idx_t> del_idx;

  idx_t *idx0 = new idx_t[n];
  quantizer->assign(n, (const float *)vec, idx0);
  idx = idx0;
  del_idx.set(idx);

  uint8_t *xcodes = new uint8_t[n * code_size];
  faiss::ScopeDeleter<uint8_t> del_xcodes(xcodes);

  size_t n_ignore = 0;
  long vid = indexed_vec_count_;
  for (int i = 0; i < n; i++) {
    long key = idx[i];
    assert(key < (long)nlist);
    if (key < 0) {
      n_ignore++;
      continue;
    }
    uint8_t *code = (uint8_t *)vec + this->code_size * i;
    new_keys[key].push_back(vid++);
    size_t ofs = new_codes[key].size();
    new_codes[key].resize(ofs + code_size);
    memcpy((void *)(new_codes[key].data() + ofs), (void *)code, code_size);
  }

  /* stage 2 : add invert info to invert index */
  if (!rt_invert_index_ptr_->AddKeys(new_keys, new_codes)) {
    return false;
  }
  indexed_vec_count_ = vid;
#ifdef PERFORMANCE_TESTING
  add_count_ += n;
  if (add_count_ >= 10000) {
    double t1 = faiss::getmillisecs();
    LOG(INFO) << "Add time [" << (t1 - t0) / n << "]ms, count "
              << indexed_vec_count_;
    // rt_invert_index_ptr_->PrintBucketSize();
    add_count_ = 0;
  }
#endif
  return true;
}

int GammaIndexIVFFlat::Update(const std::vector<int64_t> &ids,
                              const std::vector<const uint8_t *> &vecs) {
  for (size_t i = 0; i < ids.size(); i++) {
    const float *vec = reinterpret_cast<const float *>(vecs[i]);
    idx_t idx = -1;
    quantizer->assign(1, vec, &idx);
    std::vector<uint8_t> code;
    code.resize(this->code_size);
    memcpy(code.data(), (void *)vec, this->code_size);
    rt_invert_index_ptr_->Update(idx, ids[i], code);
  }
  updated_num_ += ids.size();
  LOG(INFO) << "update index success! size=" << ids.size()
            << ", total=" << updated_num_;
  // now check id need to do compaction
  rt_invert_index_ptr_->CompactIfNeed();
  return 0;
}

int GammaIndexIVFFlat::Delete(const std::vector<int64_t> &ids) {
  std::vector<int> vids(ids.begin(), ids.end());
  rt_invert_index_ptr_->Delete(vids.data(), vids.size());
  return 0;
}

void GammaIndexIVFFlat::SearchPreassgined(RetrievalContext *retrieval_context,
                                          idx_t n, const float *x, int k,
                                          const idx_t *keys,
                                          const float *coarse_dis,
                                          float *distances, idx_t *labels,
                                          int nprobe, bool store_pairs) {
  return search_preassigned(retrieval_context, n, x, k, keys, coarse_dis,
                            distances, labels, nprobe, store_pairs);
}

int GammaIndexIVFFlat::Search(RetrievalContext *retrieval_context, int n,
                              const uint8_t *rx, int k, float *distances,
                              idx_t *labels) {
#ifndef FAISSLIKE_INDEX
  IVFFlatRetrievalParameters *retrieval_params =
      dynamic_cast<IVFFlatRetrievalParameters *>(
          retrieval_context->RetrievalParams());

  utils::ScopeDeleter1<IVFFlatRetrievalParameters> del_params;
  if (retrieval_params == nullptr) {
    retrieval_params = new IVFFlatRetrievalParameters();
    del_params.set(retrieval_params);
  }

  int nprobe = retrieval_params->Nprobe();
#else
  int nprobe = this->nprobe;
#endif
  const float *x = reinterpret_cast<const float *>(rx);

  std::unique_ptr<idx_t[]> idx(new idx_t[n * nprobe]);
  std::unique_ptr<float[]> coarse_dis(new float[n * nprobe]);

  quantizer->search(n, x, nprobe, coarse_dis.get(), idx.get());

  SearchPreassgined(retrieval_context, n, x, k, idx.get(), coarse_dis.get(),
                    distances, labels, nprobe, false);

  return 0;
}

void GammaIndexIVFFlat::search_preassigned(RetrievalContext *retrieval_context,
                                           idx_t n, const float *x, int k,
                                           const idx_t *keys,
                                           const float *coarse_dis,
                                           float *distances, idx_t *labels,
                                           int nprobe, bool store_pairs) const {
  IVFFlatRetrievalParameters *retrieval_params =
      dynamic_cast<IVFFlatRetrievalParameters *>(
          retrieval_context->RetrievalParams());
  utils::ScopeDeleter1<IVFFlatRetrievalParameters> del_params;
  if (retrieval_params == nullptr) {
    retrieval_params = new IVFFlatRetrievalParameters();
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

#pragma omp parallel if (do_parallel) reduction(+ : nlistv, ndis, nheap)
  {
    GammaInvertedListScanner *scanner =
        GetGammaInvertedListScanner(store_pairs, metric_type);
    faiss::ScopeDeleter1<GammaInvertedListScanner> del(scanner);
    scanner->set_search_context(retrieval_context);

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
      const Index::idx_t *ids = nullptr;

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

      }  // parallel for
    } else if (pmode == 1) {
      std::vector<idx_t> local_idx(k);
      std::vector<float> local_dis(k);

      for (idx_t i = 0; i < n; i++) {
        scanner->set_query(x + i * d);
        init_result(local_dis.data(), local_idx.data());

#pragma omp for schedule(dynamic)
        for (idx_t ik = 0; ik < nprobe; ik++) {
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
          if (metric_type == METRIC_INNER_PRODUCT) {
            heap_addn<HeapForIP>(k, simi, idxi, local_dis.data(),
                                 local_idx.data(), k);
          } else {
            heap_addn<HeapForL2>(k, simi, idxi, local_dis.data(),
                                 local_idx.data(), k);
          }
        }
#pragma omp barrier
#pragma omp single
        reorder_result(simi, idxi);
      }
    } else {
      FAISS_THROW_FMT("parallel_mode %d not supported\n", pmode);
    }
  }  // parallel section

  if (interrupt) {
    FAISS_THROW_MSG("computation interrupted");
  }
}

string IVFFlatToString(const faiss::IndexIVFFlat *ivfl) {
  std::stringstream ss;
  ss << "d=" << ivfl->d << ", ntotal=" << ivfl->ntotal
     << ", is_trained=" << ivfl->is_trained
     << ", metric_type=" << ivfl->metric_type << ", nlist=" << ivfl->nlist
     << ", nprobe=" << ivfl->nprobe;

  return ss.str();
}

int GammaIndexIVFFlat::Dump(const std::string &dir) {
  if (!this->is_trained) {
    LOG(INFO) << "gamma index is not trained, skip dumping";
    return 0;
  }
  std::string index_name = vector_->MetaInfo()->AbsoluteName();
  string index_dir = dir + "/" + index_name;
  if (utils::make_dir(index_dir.c_str())) {
    LOG(ERROR) << "mkdir error, index dir=" << index_dir;
    return IO_ERR;
  }

  string index_file = index_dir + "/ivfflat.index";
  faiss::IOWriter *f = new FileIOWriter(index_file.c_str());
  utils::ScopeDeleter1<FileIOWriter> del((FileIOWriter *)f);
  const IndexIVFFlat *ivfl = static_cast<const IndexIVFFlat *>(this);
  uint32_t h = faiss::fourcc("IvFl");
  WRITE1(h);
  tig_gamma::write_ivf_header(ivfl, f);

  int indexed_count = indexed_vec_count_;
  if (WriteInvertedLists(f, rt_invert_index_ptr_)) {
    LOG(ERROR) << "write invert list error, index name=" << index_name;
    return INTERNAL_ERR;
  }
  WRITE1(indexed_count);

  LOG(INFO) << "dump:" << IVFFlatToString(ivfl)
            << ", indexed count=" << indexed_count;
  return 0;
};

int GammaIndexIVFFlat::Load(const std::string &dir) {
  std::string index_name = vector_->MetaInfo()->AbsoluteName();
  string index_file = dir + "/" + index_name + "/ivfflat.index";
  if (!utils::file_exist(index_file)) {
    LOG(INFO) << index_file << " isn't existed, skip loading";
    return 0;  // it should train again after load
  }

  faiss::IOReader *f = new FileIOReader(index_file.c_str());
  utils::ScopeDeleter1<FileIOReader> del((FileIOReader *)f);
  uint32_t h;
  READ1(h);
  assert(h == faiss::fourcc("IvFl"));
  IndexIVFFlat *ivfl = static_cast<IndexIVFFlat *>(this);
  tig_gamma::read_ivf_header(ivfl, f, nullptr);  // not legacy

  int indexed_vec_count = 0;
  int ret = ReadInvertedLists(f, rt_invert_index_ptr_, indexed_vec_count);
  if (ret == FORMAT_ERR) {
    indexed_vec_count_ = 0;
    LOG(INFO) << "unsupported inverted list format, it need rebuilding!";
  } else if (ret == 0) {
    READ1(indexed_vec_count_);
    if (indexed_vec_count_ < 0 ||
        (check_vector_ &&
         indexed_vec_count_ > (int)vector_->MetaInfo()->size_)) {
      LOG(ERROR) << "invalid indexed count [" << indexed_vec_count_
                 << "] vector size [" << vector_->MetaInfo()->size_ << "]";
      return INTERNAL_ERR;
    }
    LOG(INFO) << "load: " << IVFFlatToString(ivfl)
              << ", indexed vector count=" << indexed_vec_count_;
  } else {
    LOG(ERROR) << "read invert list error, index name=" << index_name;
    return INTERNAL_ERR;
  }
  assert(this->is_trained);
  return indexed_vec_count_;
};

GammaInvertedListScanner *GammaIndexIVFFlat::GetGammaInvertedListScanner(
    bool store_pairs, faiss::MetricType metric_type) const {
  if (metric_type == faiss::METRIC_INNER_PRODUCT) {
    auto scanner = new GammaIVFFlatScanner1<faiss::METRIC_INNER_PRODUCT,
                                            faiss::CMin<float, idx_t>>(this->d);
    return scanner;
  } else if (metric_type == faiss::METRIC_L2) {
    auto scanner =
        new GammaIVFFlatScanner1<faiss::METRIC_L2, faiss::CMax<float, idx_t>>(
            this->d);
    return scanner;
  }
  return nullptr;
}

}  // namespace tig_gamma
