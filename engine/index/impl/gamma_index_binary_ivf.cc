/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "gamma_index_binary_ivf.h"

#include "faiss/IndexBinaryFlat.h"
#include "faiss/utils/hamming.h"
#include "search/error_code.h"

namespace tig_gamma {

struct BinaryModelParams {
  int ncentroids;  // coarse cluster center number

  BinaryModelParams() { ncentroids = 256; }

  int Parse(const char *str) {
    utils::JsonParser jp;
    if (jp.Parse(str)) {
      LOG(ERROR) << "parse IVF model parameters error: " << str;
      return -1;
    }

    int ncentroids;

    // -1 as default
    if (!jp.GetInt("ncentroids", ncentroids)) {
      if (ncentroids < -1) {
        LOG(ERROR) << "invalid ncentroids =" << ncentroids;
        return -1;
      }
      if (ncentroids > 0) this->ncentroids = ncentroids;
    } else {
      LOG(ERROR) << "cannot get ncentroids for ivf, set it when create space";
      return -1;
    }

    return 0;
  }

  bool Validate() {
    if (ncentroids <= 0) return false;
    return true;
  }

  std::string ToString() {
    std::stringstream ss;
    ss << "ncentroids =" << ncentroids << ", ";
    return ss.str();
  }
};

REGISTER_MODEL(BINARYIVF, GammaIndexBinaryIVF)

GammaIndexBinaryIVF::GammaIndexBinaryIVF() {
  indexed_vec_count_ = 0;
  rt_invert_index_ptr_ = nullptr;
#ifdef PERFORMANCE_TESTING
  add_count_ = 0;
#endif
}

GammaIndexBinaryIVF::~GammaIndexBinaryIVF() {
  if (rt_invert_index_ptr_) {
    delete rt_invert_index_ptr_;
    rt_invert_index_ptr_ = nullptr;
  }
  if (invlists) {
    delete invlists;
    invlists = nullptr;
  }
  if (quantizer) {
    delete quantizer;  // it will not be delete in parent class
    quantizer = nullptr;
  }
}

int GammaIndexBinaryIVF::Init(const std::string &model_parameters,
                              int indexing_size) {
  indexing_size_ = indexing_size;
  BinaryModelParams binary_param;
  if (model_parameters != "" && binary_param.Parse(model_parameters.c_str())) {
    return -1;
  }
  LOG(INFO) << binary_param.ToString();
  nlist = binary_param.ncentroids;

  cp.niter = 10;
  clustering_index = nullptr;

  RawVector *raw_vec = dynamic_cast<RawVector *>(vector_);
  d = raw_vec->MetaInfo()->Dimension() * 8;
  quantizer = new faiss::IndexBinaryFlat(d);

  code_size = d / 8;
  verbose = false;

  int bucket_keys =
      std::max(1000, this->indexing_size_ / binary_param.ncentroids);
  rt_invert_index_ptr_ = new realtime::RTInvertIndex(
      this->nlist, this->code_size, raw_vec->VidMgr(), raw_vec->Bitmap(),
      bucket_keys, 1280000);
  // default is true in faiss
  is_trained = false;

  this->nprobe = 20;

  if (this->invlists) {
    delete this->invlists;
    this->invlists = nullptr;
  }

  bool ret = rt_invert_index_ptr_->Init();

  if (ret) {
    this->invlists =
        new realtime::RTInvertedLists(rt_invert_index_ptr_, nlist, code_size);
  }
  return 0;
}

RetrievalParameters *GammaIndexBinaryIVF::Parse(const std::string &parameters) {
  if (parameters == "") {
    return new BinaryIVFRetrievalParameters();
  }

  utils::JsonParser jp;
  if (jp.Parse(parameters.c_str())) {
    LOG(ERROR) << "parse retrieval parameters error: " << parameters;
    return nullptr;
  }

  BinaryIVFRetrievalParameters *retrieval_params =
      new BinaryIVFRetrievalParameters();
  int nprobe = 0;
  if (!jp.GetInt("nprobe", nprobe)) {
    if (nprobe > 0) {
      retrieval_params->SetNprobe(nprobe);
    }
  }
  return retrieval_params;
}

bool GammaIndexBinaryIVF::Add(int n, const uint8_t *vec) {
#ifdef PERFORMANCE_TESTING
  double t0 = faiss::getmillisecs();
#endif
  FAISS_THROW_IF_NOT(is_trained);
  assert(invlists);

  std::map<int, std::vector<long>> new_keys;
  std::map<int, std::vector<uint8_t>> new_codes;

  const idx_t *idx;

  std::unique_ptr<idx_t[]> scoped_idx;

  scoped_idx.reset(new idx_t[n]);
  quantizer->assign(n, vec, scoped_idx.get());
  idx = scoped_idx.get();

  size_t n_ignore = 0;
  size_t n_add = 0;
  long vid = indexed_vec_count_;
  for (int i = 0; i < n; i++) {
    long list_no = idx[i];
    assert(list_no < (long)nlist);
    if (list_no < 0) {
      n_ignore++;
      continue;
    }

    // long id = (long)(indexed_vec_count_++);
    const uint8_t *code = vec + i * code_size;

    new_keys[list_no].push_back(vid++);

    size_t ofs = new_codes[list_no].size();
    new_codes[list_no].resize(ofs + code_size);
    memcpy((void *)(new_codes[list_no].data() + ofs), (void *)code, code_size);

    n_add++;
  }

  ntotal += n_add;

  if (!rt_invert_index_ptr_->AddKeys(new_keys, new_codes)) {
    return false;
  }
  indexed_vec_count_ = vid;
#ifdef PERFORMANCE_TESTING
  add_count_ += n;
  if (add_count_ >= 100000) {
    double t1 = faiss::getmillisecs();
    LOG(INFO) << "Add time [" << (t1 - t0) / n << "]ms, count "
              << indexed_vec_count_;
    rt_invert_index_ptr_->PrintBucketSize();
    add_count_ = 0;
  }
#endif
  return true;
}

int GammaIndexBinaryIVF::Indexing() {
  if (this->is_trained) {
    LOG(INFO) << "gamma ivfpq index is already trained, skip indexing";
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
    num = (size_t)indexing_size_;
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

  const uint8_t *train_vec = nullptr;
  utils::ScopeDeleter1<uint8_t> del_vec;
  if (lens.size() == 1) {
    train_vec = headers.Get(0);
  } else {
    int raw_d = raw_vec->MetaInfo()->Dimension();
    train_vec = new uint8_t[raw_d * num];
    del_vec.set(train_vec);
    size_t offset = 0;
    for (size_t i = 0; i < headers.Size(); ++i) {
      memcpy((void *)(train_vec + offset), (void *)headers.Get(i),
             sizeof(char) * raw_d * lens[i]);
      offset += raw_d * lens[i];
    }
  }

  faiss::IndexBinaryIVF::train(num, train_vec);

  LOG(INFO) << "train successed!";
  return 0;
}

long GammaIndexBinaryIVF::GetTotalMemBytes() {
  if (!rt_invert_index_ptr_) {
    return 0;
  }
  return rt_invert_index_ptr_->GetTotalMemBytes();
}

int GammaIndexBinaryIVF::Delete(const std::vector<int64_t> &ids) {
  std::vector<int> vids(ids.begin(), ids.end());
  int ret = rt_invert_index_ptr_->Delete(vids.data(), ids.size());
  return ret;
}

int GammaIndexBinaryIVF::Search(RetrievalContext *retrieval_context, int n,
                                const uint8_t *x, int k, float *distances,
                                int64_t *labels) {
  BinaryIVFRetrievalParameters *retrieval_params =
      dynamic_cast<BinaryIVFRetrievalParameters *>(
          retrieval_context->RetrievalParams());
  utils::ScopeDeleter1<BinaryIVFRetrievalParameters> del_params;
  if (retrieval_params == nullptr) {
    retrieval_params = new BinaryIVFRetrievalParameters();
    retrieval_context->retrieval_params_ = retrieval_params;
    del_params.set(retrieval_params);
  }

  int nprobe = this->nprobe;
  if (retrieval_params->Nprobe() > 0 &&
      (size_t)retrieval_params->Nprobe() <= this->nlist) {
    nprobe = retrieval_params->Nprobe();
  } else {
    LOG(WARNING) << "Error nprobe for search, so using default value:"
                 << this->nprobe;
    retrieval_params->SetNprobe(this->nprobe);
  }

  std::unique_ptr<idx_t[]> idx(new idx_t[n * nprobe]);
  std::unique_ptr<int32_t[]> coarse_dis(new int32_t[n * nprobe]);

  quantizer->search(n, x, nprobe, coarse_dis.get(), idx.get());

  invlists->prefetch_lists(idx.get(), n * nprobe);

  idx_t *ids = reinterpret_cast<idx_t *>(labels);

  int32_t dists[n * k];
  search_preassigned(retrieval_context, n, x, k, idx.get(), coarse_dis.get(),
                     dists, ids, nprobe, false);
  for (int i = 0; i < n; i++) {
    for (int j = 0; j < k; j++) {
      distances[i * k + j] = dists[i * k + j];
    }
  }
  return 0;
}

void GammaIndexBinaryIVF::search_preassigned(
    RetrievalContext *retrieval_context, int n, const uint8_t *x, int k,
    const idx_t *idx, const int32_t *coarse_dis, int32_t *distances,
    idx_t *labels, int nprobe, bool store_pairs,
    const faiss::IVFSearchParameters *params) {
  search_knn_hamming_heap(retrieval_context, n, x, k, idx, coarse_dis,
                          distances, labels, nprobe, store_pairs, params);
}

void GammaIndexBinaryIVF::search_knn_hamming_heap(
    RetrievalContext *retrieval_context, size_t n, const uint8_t *x, int k,
    const idx_t *keys, const int32_t *coarse_dis, int32_t *distances,
    idx_t *labels, int nprobe, bool store_pairs,
    const faiss::IVFSearchParameters *params) {
  long max_codes = params ? params->max_codes : this->max_codes;

  // almost verbatim copy from IndexIVF::search_preassigned

  using HeapForIP = faiss::CMin<int32_t, idx_t>;
  using HeapForL2 = faiss::CMax<int32_t, idx_t>;

#pragma omp parallel if (n > 1)
  {
    std::unique_ptr<GammaBinaryInvertedListScanner> scanner(
        get_GammaInvertedListScanner(store_pairs));
    scanner->set_search_context(retrieval_context);

#pragma omp for
    for (size_t i = 0; i < n; i++) {
      const uint8_t *xi = x + i * code_size;
      scanner->set_query(xi);

      const idx_t *keysi = keys + i * nprobe;
      int32_t *simi = distances + k * i;
      idx_t *idxi = labels + k * i;

      if (metric_type == faiss::METRIC_INNER_PRODUCT) {
        faiss::heap_heapify<HeapForIP>(k, simi, idxi);
      } else {
        faiss::heap_heapify<HeapForL2>(k, simi, idxi);
      }

      size_t nscan = 0;

      for (long ik = 0; ik < nprobe; ik++) {
        idx_t key = keysi[ik]; /* select the list  */
        if (key < 0) {
          // not enough centroids for multiprobe
          continue;
        }
        FAISS_THROW_IF_NOT_FMT(key < (idx_t)nlist,
                               "Invalid key=%ld  at ik=%ld nlist=%ld\n", key,
                               ik, nlist);

        scanner->set_list(key, coarse_dis[i * nprobe + ik]);

        size_t list_size = invlists->list_size(key);
        faiss::InvertedLists::ScopedCodes scodes(invlists, key);
        std::unique_ptr<faiss::InvertedLists::ScopedIds> sids;
        const faiss::Index::idx_t *ids = nullptr;

        if (!store_pairs) {
          sids.reset(new faiss::InvertedLists::ScopedIds(invlists, key));
          ids = sids->get();
        }

        scanner->scan_codes(list_size, scodes.get(), ids, simi, idxi, k);

        nscan += list_size;
        if (max_codes && nscan >= (size_t)max_codes) break;
      }

      if (metric_type == faiss::METRIC_INNER_PRODUCT) {
        faiss::heap_reorder<HeapForIP>(k, simi, idxi);
      } else {
        faiss::heap_reorder<HeapForL2>(k, simi, idxi);
      }

    }  // parallel for
  }    // parallel
}

template <class HammingComputer, bool store_pairs>
struct GammaIVFBinaryScannerL2 : GammaBinaryInvertedListScanner {
  HammingComputer hc;
  size_t code_size;

  explicit GammaIVFBinaryScannerL2(size_t code_size) : code_size(code_size) {}

  void set_query(const uint8_t *query_vector) override {
    hc.set(query_vector, code_size);
  }

  idx_t list_no;
  void set_list(idx_t list_no, uint8_t /* coarse_dis */) override {
    this->list_no = list_no;
  }

  // uint32_t distance_to_code(const uint8_t *code) const override {
  //   return hc.hamming(code);
  // }

  size_t scan_codes(size_t n, const uint8_t *codes, const idx_t *ids,
                    int32_t *simi, idx_t *idxi, size_t k) const override {
    using C = faiss::CMax<int32_t, idx_t>;

    size_t nup = 0;
    for (size_t j = 0; j < n; j++, codes += code_size) {
      idx_t id = store_pairs ? (list_no << 32 | j) : ids[j];
      if (retrieval_context_->IsValid(id) == false) {
        continue;
      }
      int32_t dis = hc.hamming(codes);
      if (!retrieval_context_->IsSimilarScoreValid(dis)) {
        continue;
      }
      if (dis < simi[0]) {
        faiss::heap_pop<C>(k, simi, idxi);
        faiss::heap_push<C>(k, simi, idxi, dis, id);
        nup++;
      }
    }
    return nup;
  }
};

template <bool store_pairs>
GammaBinaryInvertedListScanner *select_IVFBinaryScannerL2(size_t code_size) {
  switch (code_size) {
#define HANDLE_CS(cs)                                              \
  case cs:                                                         \
    return new GammaIVFBinaryScannerL2<faiss::HammingComputer##cs, \
                                       store_pairs>(cs);
    HANDLE_CS(4);
    HANDLE_CS(8);
    HANDLE_CS(16);
    HANDLE_CS(20);
    HANDLE_CS(32);
    HANDLE_CS(64);
#undef HANDLE_CS
    default:
      if (code_size % 8 == 0) {
        return new GammaIVFBinaryScannerL2<faiss::HammingComputerM8,
                                           store_pairs>(code_size);
      } else if (code_size % 4 == 0) {
        return new GammaIVFBinaryScannerL2<faiss::HammingComputerM4,
                                           store_pairs>(code_size);
      } else {
        return new GammaIVFBinaryScannerL2<faiss::HammingComputerDefault,
                                           store_pairs>(code_size);
      }
  }
}

GammaBinaryInvertedListScanner *
GammaIndexBinaryIVF::get_GammaInvertedListScanner(bool store_pairs) const {
  if (store_pairs) {
    return select_IVFBinaryScannerL2<true>(code_size);
  } else {
    return select_IVFBinaryScannerL2<false>(code_size);
  }
}

}  // namespace tig_gamma
