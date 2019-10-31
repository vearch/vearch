/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This faiss source code is licensed under the MIT license.
 * https://github.com/facebookresearch/faiss/blob/master/LICENSE
 *
 *
 * The works below are modified based on faiss:
 * 1. Replace the static batch indexing with real time indexing
 * 2. Add the fine-grained sort after PQ coarse sort
 * 3. Add the numeric field and bitmap filters in the process of searching
 *
 * Modified works copyright 2019 The Gamma Authors.
 *
 * The modified codes are licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 *
 */

#include "gamma_index_ivfpq.h"

#include <algorithm>
#include <stdexcept>
#include <vector>

#include "bitmap.h"
#include "faiss/Heap.h"
#include "faiss/utils.h"
#include "utils.h"

#include "omp.h"

namespace tig_gamma {

IndexIVFPQStats indexIVFPQ_stats;

GammaIVFPQIndex::GammaIVFPQIndex(faiss::Index *quantizer, size_t d,
                                 size_t nlist, size_t M, size_t nbits_per_idx,
                                 const char *docids_bitmap, RawVector *raw_vec,
                                 int nprobe)
    : GammaIndex(d, docids_bitmap, raw_vec), faiss::IndexIVFPQ(quantizer, d,
                                                               nlist, M,
                                                               nbits_per_idx),
      indexed_vec_count_(0) {
  assert(raw_vec != nullptr);
  int max_vec_size = raw_vec->GetMaxVectorSize();

  rt_invert_index_ptr_ =
      new realtime::RTInvertIndex(this, max_vec_size, 10000, 1000000);

  if (this->invlists) {
    delete this->invlists;
    this->invlists = nullptr;
  }

  bool ret = rt_invert_index_ptr_->Init();

  if (ret) {
    this->invlists =
        new RTInvertedLists(rt_invert_index_ptr_, nlist, code_size);
  }
  this->nprobe = nprobe;

#ifdef PERFORMANCE_TESTING
  search_count_ = 0;
#endif
}

faiss::InvertedListScanner *
GammaIVFPQIndex::get_InvertedListScanner(bool store_pairs) const {
  if (metric_type == faiss::METRIC_INNER_PRODUCT) {
    if (store_pairs) {
      faiss::InvertedListScanner *scanner =
          new GammaIndexScanner<faiss::METRIC_INNER_PRODUCT, true,
                                faiss::CMin<float, idx_t>, 2>(*this);
      ((GammaIndexScanner<faiss::METRIC_INNER_PRODUCT, true,
                          faiss::CMin<float, idx_t>, 2> *)scanner)
          ->SetVecFilter(this->docids_bitmap_, this->raw_vec_);
      return scanner;
    } else {
      faiss::InvertedListScanner *scanner =
          new GammaIndexScanner<faiss::METRIC_INNER_PRODUCT, false,
                                faiss::CMin<float, idx_t>, 2>(*this);
      ((GammaIndexScanner<faiss::METRIC_INNER_PRODUCT, false,
                          faiss::CMin<float, idx_t>, 2> *)scanner)
          ->SetVecFilter(this->docids_bitmap_, this->raw_vec_);
      return scanner;
    }
  } else if (metric_type == faiss::METRIC_L2) {
    if (store_pairs) {
      faiss::InvertedListScanner *scanner =
          new GammaIndexScanner<faiss::METRIC_L2, true,
                                faiss::CMax<float, idx_t>, 2>(*this);

      ((GammaIndexScanner<faiss::METRIC_L2, true, faiss::CMax<float, idx_t>, 2>
            *)scanner)
          ->SetVecFilter(this->docids_bitmap_, this->raw_vec_);
      return scanner;
    } else {
      faiss::InvertedListScanner *scanner =
          new GammaIndexScanner<faiss::METRIC_L2, false,
                                faiss::CMax<float, idx_t>, 2>(*this);
      ((GammaIndexScanner<faiss::METRIC_L2, false, faiss::CMax<float, idx_t>, 2>
            *)scanner)
          ->SetVecFilter(this->docids_bitmap_, this->raw_vec_);
      return scanner;
    }
  }
  return nullptr;
}

int GammaIVFPQIndex::Indexing() {
  if (this->is_trained) {
    LOG(INFO) << "gamma ivfpq index is already trained, skip indexing";
    return 0;
  }
  int vectors_count = raw_vec_->GetVectorNum();
  if (vectors_count < 8192) {
    LOG(ERROR) << "vector total count [" << vectors_count
               << "] less then 8192, failed!";
    return -1;
  }
  int num = vectors_count > 100000 ? 100000 : vectors_count;
  const float *header = raw_vec_->GetVectorHeader(0, num);
  train(num, header);
  raw_vec_->Destroy(header, true);
  LOG(INFO) << "train successed!";
  return 0;
}

int GammaIVFPQIndex::AddRTVecsToIndex() {
  int ret = 0;
  int total_stored_vecs = raw_vec_->GetVectorNum();
  if (indexed_vec_count_ > total_stored_vecs) {
    LOG(ERROR) << "internal error : indexed_vec_count=" << indexed_vec_count_
               << " should not greater than total_stored_vecs="
               << total_stored_vecs;
    ret = -1;
  } else if (indexed_vec_count_ == total_stored_vecs) {
    ;
#ifdef DEBUG
    LOG(INFO) << "no extra vectors existed for indexing";
#endif
  } else {
    int MAX_NUM_PER_INDEX = 1000;
    int index_count =
        (total_stored_vecs - indexed_vec_count_) / MAX_NUM_PER_INDEX + 1;

    for (int i = 0; i < index_count; i++) {
      int start_docid = indexed_vec_count_;
      int count_per_index =
          (i == (index_count - 1) ? total_stored_vecs - start_docid
                                  : MAX_NUM_PER_INDEX);
      // const float *index_ptr = raw_vec_->GetVector(start_docid);
      const float *vector_head = raw_vec_->GetVectorHeader(indexed_vec_count_, indexed_vec_count_ + count_per_index);
      // const float *index_ptr = vector_head + (uint64_t)start_docid * d_;
      const float *index_ptr = vector_head;
      if (!Add(count_per_index, index_ptr)) {
        LOG(ERROR) << "add index from docid " << start_docid << " error!";
        ret = -2;
      }
      raw_vec_->Destroy(vector_head, true);
    }
  }
  return ret;
}

static float *compute_residuals(const faiss::Index *quantizer, long n,
                                const float *x, const idx_t *list_nos) {
  size_t d = quantizer->d;
  float *residuals = new float[n * d];
  for (int i = 0; i < n; i++) {
    if (list_nos[i] < 0)
      memset(residuals + i * d, 0, sizeof(*residuals) * d);
    else
      quantizer->compute_residual(x + i * d, residuals + i * d, list_nos[i]);
  }
  return residuals;
}

bool GammaIVFPQIndex::Add(int n, const float *vec) {
#ifdef PERFORMANCE_TESTING
  double t0 = faiss::getmillisecs();
#endif
  std::map<int, std::vector<long>> new_keys;
  std::map<int, std::vector<uint8_t>> new_codes;

  idx_t *idx;
  faiss::ScopeDeleter<idx_t> del_idx;

  idx_t *idx0 = new idx_t[n];
  quantizer->assign(n, vec, idx0);
  idx = idx0;
  del_idx.set(idx);

  uint8_t *xcodes = new uint8_t[n * code_size];
  faiss::ScopeDeleter<uint8_t> del_xcodes(xcodes);

  const float *to_encode = nullptr;
  faiss::ScopeDeleter<float> del_to_encode;

  if (by_residual) {
    to_encode = compute_residuals(quantizer, n, vec, idx);
    del_to_encode.set(to_encode);
  } else {
    to_encode = vec;
  }
  pq.compute_codes(to_encode, xcodes, n);

  size_t n_ignore = 0;
  size_t n_add = 0;
  long vid = indexed_vec_count_;
  for (int i = 0; i < n; i++) {
    long key = idx[i];
    assert(key < (long)nlist);
    if (key < 0) {
      n_ignore++;
      continue;
    }

    // long id = (long)(indexed_vec_count_++);
    uint8_t *code = xcodes + i * code_size;

    new_keys[key].push_back(vid++);

    size_t ofs = new_codes[key].size();
    new_codes[key].resize(ofs + code_size);
    memcpy((void *)(new_codes[key].data() + ofs), (void *)code, code_size);

    n_add++;
  }

  /* stage 2 : add invert info to invert index */
  if (!rt_invert_index_ptr_->AddKeys(new_keys, new_codes)) {
    return false;
  }
  indexed_vec_count_ = vid;
#ifdef PERFORMANCE_TESTING
  double t1 = faiss::getmillisecs();
  if (indexed_vec_count_ % 10000 == 0) {
    LOG(INFO) << "Add time [" << (t1 - t0) / n << "]ms, count " << indexed_vec_count_;
  }
#endif
  return true;
}

void GammaIVFPQIndex::SearchIVFPQ(int n, const float *x,
                                  const GammaSearchCondition *condition,
                                  float *distances, idx_t *labels, int *total) {
  idx_t *idx = new idx_t[n * nprobe];
  faiss::ScopeDeleter<idx_t> del(idx);
  float *coarse_dis = new float[n * nprobe];
  faiss::ScopeDeleter<float> del2(coarse_dis);

  quantizer->search(n, x, nprobe, coarse_dis, idx);

  this->invlists->prefetch_lists(idx, n * nprobe);

  search_preassigned(n, x, condition, idx, coarse_dis, distances, labels, total,
                     false);
}

void GammaIVFPQIndex::search_preassigned(
    int n, const float *x, const GammaSearchCondition *condition,
    const idx_t *keys, const float *coarse_dis, float *distances, idx_t *labels,
    int *total, bool store_pairs, const faiss::IVFSearchParameters *params) {
  int nprobe = params ? params->nprobe : this->nprobe;
  long max_codes = params ? params->max_codes : this->max_codes;

  long k = condition->topn; // topK

  size_t nlistv = 0, ndis = 0, nheap = 0;

  using HeapForIP = faiss::CMin<float, idx_t>;
  using HeapForL2 = faiss::CMax<float, idx_t>;

  int recall_num = condition->recall_num;

  float *recall_distances =
      utils::NewArray<float>(n * recall_num, "recall_distances");
  idx_t *recall_labels =
      utils::NewArray<idx_t>(n * recall_num, "recall_labels");
  faiss::ScopeDeleter<float> del1(recall_distances);
  faiss::ScopeDeleter<idx_t> del2(recall_labels);

  // intialize + reorder a result heap
  auto init_result = [&](int topk, float *simi, idx_t *idxi) {
    if (metric_type == faiss::METRIC_INNER_PRODUCT) {
      faiss::heap_heapify<HeapForIP>(topk, simi, idxi);
    } else {
      faiss::heap_heapify<HeapForL2>(topk, simi, idxi);
    }
  };

  auto reorder_result = [&](int topk, float *simi, idx_t *idxi) {
    if (metric_type == faiss::METRIC_INNER_PRODUCT) {
      faiss::heap_reorder<HeapForIP>(topk, simi, idxi);
    } else {
      faiss::heap_reorder<HeapForL2>(topk, simi, idxi);
    }
  };
#ifdef PERFORMANCE_TESTING
  double s_start = utils::getmillisecs();
#endif

  int ni_total = -1;
  if (condition->range_query_result &&
      condition->range_query_result->GetAllResult().size() >= 1) {
    ni_total = condition->range_query_result->GetAllResult()[0].Size();
  }

  if (condition->range_query_result &&
      condition->range_query_result->GetAllResult().size() == 1 &&
      condition->range_query_result->GetAllResult()[0].Size() < 50000) {
    const std::vector<int> docid_list = condition->range_query_result->ToDocs();

#ifdef DEBUG
    std::stringstream ss;
    ss << "doc id list=[";
    for (int i = 0; i < docid_list.size(); i++) {
      ss << docid_list[i] << ",";
      if (i > 1000) {
        break;
      }
    }
    ss << "]";
    LOG(INFO) << ss.str();
#endif

    std::vector<int> vid_list(docid_list.size() * MAX_VECTOR_NUM_PER_DOC);
    int *vid_list_data = vid_list.data();
    int *curr_ptr = vid_list_data;
    for (size_t i = 0; i < docid_list.size(); i++) {
      // vids_list[i] = this->raw_vec_->docid2vid_[docid_list[i]];
      if (bitmap::test(this->docids_bitmap_, docid_list[i])) {
        continue;
      }

      int *vids = this->raw_vec_->docid2vid_[docid_list[i]];
      if (vids) {
        memcpy((void *)(curr_ptr), (void *)(vids + 1), sizeof(int) * vids[0]);
        curr_ptr += vids[0];
      }
    }
    int vid_list_len = curr_ptr - vid_list_data;

#ifdef PERFORMANCE_TESTING
    double to_vid_end = utils::getmillisecs();
#endif

    std::vector<std::vector<const uint8_t *>> bucket_codes;
    std::vector<std::vector<long>> bucket_vids;
    int ret = ((RTInvertedLists *)this->invlists)
                  ->rt_invert_index_ptr_->RetrieveCodes(
                      vid_list_data, vid_list_len, bucket_codes, bucket_vids);
    if (ret != 0)
      throw std::runtime_error("retrieve codes by vid error");

#ifdef PERFORMANCE_TESTING
    double retrieve_code_end = utils::getmillisecs();
#endif

#pragma omp parallel reduction(+ : nlistv, ndis, nheap)
    {
      faiss::InvertedListScanner *scanner =
          get_InvertedListScanner(store_pairs);
      faiss::ScopeDeleter1<faiss::InvertedListScanner> del(scanner);

      if (metric_type == faiss::METRIC_INNER_PRODUCT) {
        ((GammaIndexScanner<faiss::METRIC_INNER_PRODUCT, false, HeapForIP, 2> *)
             scanner)
            ->set_search_condition(condition);
      } else {
        ((GammaIndexScanner<faiss::METRIC_L2, false, HeapForL2, 2> *)scanner)
            ->set_search_condition(condition);
      }
#pragma omp for
      for (int i = 0; i < n; i++) { // loop over queries
#ifdef PERFORMANCE_TESTING
        double query_start = utils::getmillisecs();
#endif

        const float *xi = x + i * d;
        scanner->set_query(x + i * d);

        float *simi = distances + i * k;
        idx_t *idxi = labels + i * k;

        float *recall_simi = recall_distances + i * recall_num;
        idx_t *recall_idxi = recall_labels + i * recall_num;

        init_result(k, simi, idxi);
        init_result(recall_num, recall_simi, recall_idxi);

        for (int ik = 0; ik < nprobe; ik++) {
          long key = keys[i * nprobe + ik];
          float coarse_dis_i = coarse_dis[i * nprobe + ik];
          size_t ncode = bucket_codes[key].size();
          if (ncode <= 0) {
            continue;
          }
          const uint8_t **codes = bucket_codes[key].data();
          const idx_t *vids =
              reinterpret_cast<idx_t *>(bucket_vids[key].data());

          scanner->set_list(key, coarse_dis_i);

          if (metric_type == faiss::METRIC_INNER_PRODUCT) {
            ((GammaIndexScanner<faiss::METRIC_INNER_PRODUCT, false, HeapForIP,
                                2> *)scanner)
                ->scan_codes(ncode, codes, vids, recall_simi, recall_idxi,
                             recall_num);
          } else {
            ((GammaIndexScanner<faiss::METRIC_L2, false, HeapForL2, 2> *)
                 scanner)
                ->scan_codes(ncode, codes, vids, recall_simi, recall_idxi,
                             recall_num);
            ;
          }
        }

#ifdef PERFORMANCE_TESTING
        double coarse_end = utils::getmillisecs();
#endif

        long *recall_ids_pq =
            utils::NewArray<long>(recall_num, "recall_ids_pq");
        faiss::ScopeDeleter<long> del(recall_ids_pq);
        memcpy((void *)recall_ids_pq, (void *)recall_idxi,
               recall_num * sizeof(long));

        // calculate inner product for selected possible vectors
        std::vector<const float *> vecs(recall_num);
        raw_vec_->Gets(recall_num, recall_ids_pq, vecs);
        for (int j = 0; j < recall_num; j++) {
          if (recall_ids_pq[j] == -1)
            continue;
          float dis = 0;
          if (metric_type == faiss::METRIC_INNER_PRODUCT) {
            dis = faiss::fvec_inner_product(xi, vecs[j], this->d);
          } else {
            dis = faiss::fvec_L2sqr(xi, vecs[j], this->d);
          }

          if (((condition->min_dist >= 0 && dis >= condition->min_dist) &&
               (condition->max_dist >= 0 && dis <= condition->max_dist)) ||
              (condition->min_dist == -1 && condition->max_dist == -1)) {
            if (metric_type == faiss::METRIC_INNER_PRODUCT) {
              if (HeapForIP::cmp(simi[0], dis)) {
                faiss::heap_pop<HeapForIP>(k, simi, idxi);
                long id = recall_ids_pq[j];
                faiss::heap_push<HeapForIP>(k, simi, idxi, dis, id);
              }
            } else {
              if (HeapForL2::cmp(simi[0], dis)) {
                faiss::heap_pop<HeapForL2>(k, simi, idxi);
                long id = recall_ids_pq[j];
                faiss::heap_push<HeapForL2>(k, simi, idxi, dis, id);
              }
            }
          }
        }
        // release vectors
        raw_vec_->Destroy(vecs);

#ifdef PERFORMANCE_TESTING
        double refine_end = utils::getmillisecs();
#endif

        if (condition->sort_by_docid) { // sort by doc id
          std::vector<std::pair<idx_t, float>> id_sim_pairs;
          for (int i = 0; i < k; i++) {
            id_sim_pairs.emplace_back(std::make_pair(idxi[i], simi[i]));
          }
          std::sort(id_sim_pairs.begin(), id_sim_pairs.end());
          for (int i = 0; i < k; i++) {
            idxi[i] = id_sim_pairs[i].first;
            simi[i] = id_sim_pairs[i].second;
          }
        } else { // sort by distance
          reorder_result(k, simi, idxi);
        }
        total[i] = ni_total;

#ifdef PERFORMANCE_TESTING
        double end = utils::getmillisecs();
        if (++search_count_ % 1000 == 0) {
          std::stringstream perf_ss;
          perf_ss << "ivfqp range filter, doc id list size="
                  << docid_list.size() << ", vid list len=" << vid_list_len
                  << "to docid cost=" << to_vid_end - s_start << "ms, "
                  << "retrieve code cost=" << retrieve_code_end - to_vid_end
                  << "ms, query[coarse cost=" << coarse_end - query_start
                  << "ms, "
                  << "refine cost=" << refine_end - coarse_end << "ms, "
                  << "reorder cost=" << end - refine_end << "ms, "
                  << "total cost=" << end - s_start << "ms] "
                  << "metric type=" << metric_type << ", "
                  << "nprobe=" << this->nprobe;
          LOG(INFO) << perf_ss.str();
        }
#endif
      }
    }
    return;
  }

#pragma omp parallel reduction(+ : nlistv, ndis, nheap)
  {
    faiss::InvertedListScanner *scanner = get_InvertedListScanner(store_pairs);
    faiss::ScopeDeleter1<faiss::InvertedListScanner> del(scanner);

    if (metric_type == faiss::METRIC_INNER_PRODUCT) {
      ((GammaIndexScanner<faiss::METRIC_INNER_PRODUCT, false, HeapForIP, 2> *)
           scanner)
          ->set_search_condition(condition);
    } else {
      ((GammaIndexScanner<faiss::METRIC_L2, false, HeapForL2, 2> *)scanner)
          ->set_search_condition(condition);
    }

    // single list scan using the current scanner (with query
    // set porperly) and storing results in simi and idxi
    auto scan_one_list = [&](long key, float coarse_dis_i, float *simi,
                             idx_t *idxi, int topk) -> size_t {
      if (key < 0) {
        // not enough centroids for multiprobe
        return 0;
      }

      size_t list_size = invlists->list_size(key);

      // don't waste time on empty lists
      if (list_size == 0) {
        return 0;
      }

      scanner->set_list(key, coarse_dis_i);

      nlistv++;

      faiss::InvertedLists::ScopedCodes scodes(invlists, key);
      const idx_t *ids = store_pairs ? nullptr : invlists->get_ids(key);

      nheap +=
          scanner->scan_codes(list_size, scodes.get(), ids, simi, idxi, topk);

      if (ids) {
        invlists->release_ids(key, ids);
      }

      return list_size;
    };

    if (condition->parallel_mode == 0) { // parallelize over queries
#pragma omp for
      for (int i = 0; i < n; i++) {
#ifdef PERFORMANCE_TESTING
        double query_start = utils::getmillisecs();
#endif

        // loop over queries
        const float *xi = x + i * d;
        scanner->set_query(x + i * d);

#ifdef PERFORMANCE_TESTING
        double set_query_end = utils::getmillisecs();
#endif

        float *simi = distances + i * k;
        idx_t *idxi = labels + i * k;

        float *recall_simi = recall_distances + i * recall_num;
        idx_t *recall_idxi = recall_labels + i * recall_num;

        init_result(k, simi, idxi);
        init_result(recall_num, recall_simi, recall_idxi);

        long nscan = 0;

        // loop over probes
        for (int ik = 0; ik < nprobe; ik++) {
          long list_size =
              scan_one_list(keys[i * nprobe + ik], coarse_dis[i * nprobe + ik],
                            recall_simi, recall_idxi, recall_num);

          nscan += list_size;

          if (max_codes && nscan >= max_codes)
            break;
        }
        total[i] = ni_total;

        ndis += nscan;

#ifdef PERFORMANCE_TESTING
        double coarse_end = utils::getmillisecs();
#endif

        long *recall_ids_pq =
            utils::NewArray<long>(recall_num, "recall_ids_pq");
        faiss::ScopeDeleter<long> del(recall_ids_pq);
        memcpy((void *)recall_ids_pq, (void *)recall_idxi,
               recall_num * sizeof(long));

        // calculate inner product for selected possible vectors
        std::vector<const float *> vecs(recall_num);
        raw_vec_->Gets(recall_num, recall_ids_pq, vecs);
        for (int j = 0; j < recall_num; j++) {
          if (recall_ids_pq[j] == -1)
            continue;
          float dis = 0;
          if (metric_type == faiss::METRIC_INNER_PRODUCT) {
            dis = faiss::fvec_inner_product(xi, vecs[j], this->d);
          } else {
            dis = faiss::fvec_L2sqr(xi, vecs[j], this->d);
          }

          if (((condition->min_dist >= 0 && dis >= condition->min_dist) &&
               (condition->max_dist >= 0 && dis <= condition->max_dist)) ||
              (condition->min_dist == -1 && condition->max_dist == -1)) {
            if (metric_type == faiss::METRIC_INNER_PRODUCT) {
              if (HeapForIP::cmp(simi[0], dis)) {
                faiss::heap_pop<HeapForIP>(k, simi, idxi);
                long id = recall_ids_pq[j];
                faiss::heap_push<HeapForIP>(k, simi, idxi, dis, id);
              }
            } else {
              if (HeapForL2::cmp(simi[0], dis)) {
                faiss::heap_pop<HeapForL2>(k, simi, idxi);
                long id = recall_ids_pq[j];
                faiss::heap_push<HeapForL2>(k, simi, idxi, dis, id);
              }
            }
          }
        }

        // release vectors
        raw_vec_->Destroy(vecs);

#ifdef PERFORMANCE_TESTING
        double refine_end = utils::getmillisecs();
#endif

        if (condition->sort_by_docid) { // sort by doc id
          std::vector<std::pair<long, float>> id_sim_pairs;
          for (int i = 0; i < k; i++) {
            id_sim_pairs.emplace_back(std::make_pair(idxi[i], simi[i]));
          }
          std::sort(id_sim_pairs.begin(), id_sim_pairs.end());
          for (int i = 0; i < k; i++) {
            idxi[i] = id_sim_pairs[i].first;
            simi[i] = id_sim_pairs[i].second;
          }
        } else { // sort by distance
          reorder_result(k, simi, idxi);
        }

#ifdef PERFORMANCE_TESTING
        double end = utils::getmillisecs();
        if (++search_count_ % 1000 == 0) {
          std::stringstream perf_ss;
          perf_ss << "ivfqp query parallel "
                  << "coarse cost=" << coarse_end - query_start << "ms, "
                  << "set query cost=" << set_query_end - query_start << "ms, "
                  << "refine cost=" << refine_end - coarse_end << "ms, "
                  << "reorder cost=" << end - refine_end << "ms, "
                  << "total cost=" << end - query_start << "ms,"
                  << "nscan=" << nscan << ", nheap=" << nheap
                  << ", nprobe=" << this->nprobe;
          LOG(INFO) << perf_ss.str();
        }
#endif

      }      // parallel for
    } else { // parallelize over inverted lists
      std::vector<idx_t> local_idx(recall_num);
      std::vector<float> local_dis(recall_num);

      for (int i = 0; i < n; i++) {
        const float *xi = x + i * d;
        scanner->set_query(xi);

        init_result(recall_num, local_dis.data(), local_idx.data());

#pragma omp for schedule(dynamic)
        for (int ik = 0; ik < nprobe; ik++) {
          ndis +=
              scan_one_list(keys[i * nprobe + ik], coarse_dis[i * nprobe + ik],
                            local_dis.data(), local_idx.data(), recall_num);

          // can't do the test on max_codes
        }

        total[i] = ni_total;

        // merge thread-local results

        float *simi = distances + i * k;
        idx_t *idxi = labels + i * k;

        float *recall_simi = recall_distances + i * recall_num;
        idx_t *recall_idxi = recall_labels + i * recall_num;

#pragma omp single
        {
          init_result(k, simi, idxi);
          init_result(recall_num, recall_simi, recall_idxi);
        }

#pragma omp barrier
#pragma omp critical
        {
          if (metric_type == faiss::METRIC_INNER_PRODUCT) {
            faiss::heap_addn<HeapForIP>(recall_num, recall_simi, recall_idxi,
                                        local_dis.data(), local_idx.data(),
                                        recall_num);
          } else {
            faiss::heap_addn<HeapForL2>(recall_num, recall_simi, recall_idxi,
                                        local_dis.data(), local_idx.data(),
                                        recall_num);
          }
        }
#pragma omp barrier
#pragma omp single
        {
#ifdef PERFORMANCE_TESTING
          double coarse_end = utils::getmillisecs();
// LOG(INFO) << "ivfpq perf coarse_end=" << coarse_end;
#endif

          long *recall_ids_pq =
              utils::NewArray<long>(recall_num, "recall_ids_pq");
          faiss::ScopeDeleter<long> del(recall_ids_pq);
          memcpy((void *)recall_ids_pq, (void *)recall_idxi,
                 recall_num * sizeof(long));

          // calculate inner product for selected possible vectors
          std::vector<const float *> vecs(recall_num);
          raw_vec_->Gets(recall_num, recall_ids_pq, vecs);
          double get_end = utils::getmillisecs();
          for (int j = 0; j < recall_num; j++) {
            if (recall_ids_pq[j] == -1)
              continue;
            float dis = 0;
            if (metric_type == faiss::METRIC_INNER_PRODUCT) {
              dis = faiss::fvec_inner_product(xi, vecs[j], this->d);
            } else {
              dis = faiss::fvec_L2sqr(xi, vecs[j], this->d);
            }

            if (((condition->min_dist >= 0 && dis >= condition->min_dist) &&
                 (condition->max_dist >= 0 && dis <= condition->max_dist)) ||
                (condition->min_dist == -1 && condition->max_dist == -1)) {
              if (metric_type == faiss::METRIC_INNER_PRODUCT) {
                if (HeapForIP::cmp(simi[0], dis)) {
                  faiss::heap_pop<HeapForIP>(k, simi, idxi);
                  long id = recall_ids_pq[j];
                  faiss::heap_push<HeapForIP>(k, simi, idxi, dis, id);
                }
              } else {
                if (HeapForL2::cmp(simi[0], dis)) {
                  faiss::heap_pop<HeapForL2>(k, simi, idxi);
                  long id = recall_ids_pq[j];
                  faiss::heap_push<HeapForL2>(k, simi, idxi, dis, id);
                }
              }
            }
          }

          // release vectors
          raw_vec_->Destroy(vecs);

#ifdef PERFORMANCE_TESTING
          double refine_end = utils::getmillisecs();
// LOG(INFO) << "ivfpq perf refine_end=" << refine_end;
#endif

          if (condition->sort_by_docid) {
            std::vector<std::pair<idx_t, float>> id_sim_pairs;
            for (int z = 0; z < k; z++) {
              id_sim_pairs.emplace_back(std::make_pair(idxi[z], simi[z]));
            }
            std::sort(id_sim_pairs.begin(), id_sim_pairs.end());
            for (int z = 0; z < k; z++) {
              idxi[z] = id_sim_pairs[z].first;
              simi[z] = id_sim_pairs[z].second;
            }
          } else {
            reorder_result(k, simi, idxi);
          }
#ifdef PERFORMANCE_TESTING
          double s_end = utils::getmillisecs();
          if (search_count_++ % 10000 == 0) {
            std::stringstream perf_ss;
            perf_ss << "ivfpq nprobe parallel: "
                    << "coarse cost=" << coarse_end - s_start << "ms, "
                    << "get cost=" << get_end - coarse_end << "ms, "
                    << "refine cost=" << refine_end - coarse_end << "ms, "
                    << "reorder cost=" << s_end - refine_end << "ms, "
                    << "total cost=" << s_end - s_start << "ms, "
                    << "metric type=" << metric_type << ", "
                    << "nprobe=" << this->nprobe
                    << ", recall_num=" << recall_num;
            LOG(INFO) << perf_ss.str();
          }
#endif
        }
      }
    }
  } // parallel
}

void GammaIVFPQIndex::SearchDirectly(int n, const float *x,
                                     const GammaSearchCondition *condition,
                                     float *distances, idx_t *labels,
                                     int *total) {
  int num_vectors = raw_vec_->GetVectorNum();
  const float *vectors = raw_vec_->GetVectorHeader(0, 0 + num_vectors);

  long k = condition->topn; // topK

  using HeapForIP = faiss::CMin<float, idx_t>;
  using HeapForL2 = faiss::CMax<float, idx_t>;

  size_t ndis = 0;

#pragma omp parallel reduction(+ : ndis)
  {
    // we must obtain the num of threads in *THE* parallel area.
    int num_threads = omp_get_num_threads();

    /*****************************************************
     * Depending on parallel_mode, there are two possible ways
     * to organize the search. Here we define local functions
     * that are in common between the two
     ******************************************************/

    auto init_result = [&](int k, float *simi, idx_t *idxi) {
      if (metric_type == faiss::METRIC_INNER_PRODUCT) {
        faiss::heap_heapify<HeapForIP>(k, simi, idxi);
      } else {
        faiss::heap_heapify<HeapForL2>(k, simi, idxi);
      }
    };

    auto reorder_result = [&](int k, float *simi, idx_t *idxi) {
      if (metric_type == faiss::METRIC_INNER_PRODUCT) {
        faiss::heap_reorder<HeapForIP>(k, simi, idxi);
      } else {
        faiss::heap_reorder<HeapForL2>(k, simi, idxi);
      }
    };

    auto sort_by_docid = [&](int k, float *simi, idx_t *idxi) {
      std::vector<std::pair<long, float>> id_sim_pairs;
      for (int i = 0; i < k; i++) {
        id_sim_pairs.emplace_back(std::make_pair(idxi[i], simi[i]));
      }
      std::sort(id_sim_pairs.begin(), id_sim_pairs.end());
      for (int i = 0; i < k; i++) {
        idxi[i] = id_sim_pairs[i].first;
        simi[i] = id_sim_pairs[i].second;
      }
    };

    auto search_impl = [&](const float *xi, const float *y, int ny, int offset,
                           float *simi, idx_t *idxi, int k) -> int {
      int total = 0;
      auto *nr = condition->range_query_result;
      bool ck_dis = (condition->min_dist >= 0 && condition->max_dist >= 0);
      auto d = this->d;

      if (metric_type == faiss::METRIC_INNER_PRODUCT) {
        for (int i = 0; i < ny; i++) {
          int vid = offset + i;
          auto docid = raw_vec_->vid2docid_[vid];

          if (bitmap::test(docids_bitmap_, docid) ||
              (nr && not nr->Has(docid))) {
            continue;
          }

          const float *yi = y + i * d;
          float dis = faiss::fvec_inner_product(xi, yi, d);

          if (ck_dis &&
              (dis < condition->min_dist || dis > condition->max_dist)) {
            continue;
          }

          if (HeapForIP::cmp(simi[0], dis)) {
            faiss::heap_pop<HeapForIP>(k, simi, idxi);
            faiss::heap_push<HeapForIP>(k, simi, idxi, dis, vid);
          }

          total++;
        }
      } else {
        for (int i = 0; i < ny; i++) {
          int vid = offset + i;
          auto docid = raw_vec_->vid2docid_[vid];

          if (bitmap::test(docids_bitmap_, docid) ||
              (nr && not nr->Has(docid))) {
            continue;
          }

          const float *yi = y + i * d;
          float dis = faiss::fvec_L2sqr(xi, yi, d);

          if (ck_dis &&
              (dis < condition->min_dist || dis > condition->max_dist)) {
            continue;
          }

          if (HeapForL2::cmp(simi[0], dis)) {
            faiss::heap_pop<HeapForL2>(k, simi, idxi);
            faiss::heap_push<HeapForL2>(k, simi, idxi, dis, vid);
          }

          total++;
        }
      }

      return total;
    };

    if (condition->parallel_mode == 0) { // parallelize over queries
#pragma omp for
      for (int i = 0; i < n; i++) {
        const float *xi = x + i * d;

        float *simi = distances + i * k;
        idx_t *idxi = labels + i * k;

        init_result(k, simi, idxi);

        total[i] += search_impl(xi, vectors, num_vectors, 0, simi, idxi, k);

        if (condition->sort_by_docid) {
          sort_by_docid(k, simi, idxi);
        } else { // sort by dist
          reorder_result(k, simi, idxi);
        }
      }
    } else { // parallelize over vectors

      std::vector<idx_t> local_idx(k);
      std::vector<float> local_dis(k);

      size_t num_vectors_per_thread = num_vectors / num_threads;

      for (int i = 0; i < n; i++) {
        const float *xi = x + i * d;

        init_result(k, local_dis.data(), local_idx.data());

#pragma omp for schedule(dynamic)
        for (int ik = 0; ik < num_threads; ik++) {
          const float *y = vectors + ik * num_vectors_per_thread * d;
          size_t ny = num_vectors_per_thread;

          if (ik == num_threads - 1) {
            ny += num_vectors % num_threads; // the rest
          }

          int offset = ik * num_vectors_per_thread;

          ndis += search_impl(xi, y, ny, offset, local_dis.data(),
                              local_idx.data(), k);
        }

        total[i] += ndis;

        // merge thread-local results
        float *simi = distances + i * k;
        idx_t *idxi = labels + i * k;

#pragma omp single
        init_result(k, simi, idxi);

#pragma omp barrier
#pragma omp critical
        {
          if (metric_type == faiss::METRIC_INNER_PRODUCT) {
            faiss::heap_addn<HeapForIP>(k, simi, idxi, local_dis.data(),
                                        local_idx.data(), k);
          } else {
            faiss::heap_addn<HeapForL2>(k, simi, idxi, local_dis.data(),
                                        local_idx.data(), k);
          }
        }
#pragma omp barrier
#pragma omp single
        {
          if (condition->sort_by_docid) {
            sort_by_docid(k, simi, idxi);
          } else {
            reorder_result(k, simi, idxi);
          }
        }
      }
    }
  } // parallel
  raw_vec_->Destroy(vectors, true);
}

int GammaIVFPQIndex::Search(const VectorQuery *query,
                            const GammaSearchCondition *condition,
                            VectorResult &result) {
  float *x = reinterpret_cast<float *>(query->value->value);
  int n = query->value->len / (d * sizeof(float));

  if (condition->metric_type == InnerProduct) {
    metric_type = faiss::METRIC_INNER_PRODUCT;
  } else {
    metric_type = faiss::METRIC_L2;
  }
  idx_t *idx = reinterpret_cast<idx_t *>(result.docids);
  if (condition->use_direct_search) {
    SearchDirectly(n, x, condition, result.dists, idx, result.total.data());
  } else {
    SearchIVFPQ(n, x, condition, result.dists, idx, result.total.data());
  }

  for (int i = 0; i < n; i++) {
    int pos = 0;

    std::map<int, int> docid2count;
    for (int j = 0; j < condition->topn; j++) {
      long *docid = result.docids + i * condition->topn + j;
      if (docid[0] == -1)
        continue;
      int vector_id = (int)docid[0];
      int real_docid = this->raw_vec_->vid2docid_[vector_id];
      if (docid2count.find(real_docid) == docid2count.end()) {
        int real_pos = i * condition->topn + pos;
        result.docids[real_pos] = real_docid;
        int ret = this->raw_vec_->GetSource(vector_id, result.sources[real_pos],
                                            result.source_lens[real_pos]);
        if (ret != 0) {
          result.sources[real_pos] = nullptr;
          result.source_lens[real_pos] = 0;
        }
        result.dists[real_pos] = result.dists[i * condition->topn + j];

        pos++;
        docid2count[real_docid] = 1;
      }
    }

    if (pos > 0) {
      result.idx[i] = 0; // init start id of seeking
    }

    for (; pos < condition->topn; pos++) {
      result.docids[i * condition->topn + pos] = -1;
      result.dists[i * condition->topn + pos] = -1;
    }
  }
  return 0;
}

RTInvertedLists::RTInvertedLists(realtime::RTInvertIndex *rt_invert_index_ptr,
                                 size_t nlist, size_t code_size)
    : InvertedLists(nlist, code_size),
      rt_invert_index_ptr_(rt_invert_index_ptr) {}

size_t RTInvertedLists::list_size(size_t list_no) const {
  if (!rt_invert_index_ptr_)
    return 0;
  long *ivt_list = nullptr;
  size_t list_size = 0;
  uint8_t *ivt_codes_list = nullptr;
  bool ret = rt_invert_index_ptr_->GetIvtList(list_no, ivt_list, list_size,
                                              ivt_codes_list);
  if (!ret)
    return 0;
  return list_size;
}

const uint8_t *RTInvertedLists::get_codes(size_t list_no) const {
  if (!rt_invert_index_ptr_)
    return nullptr;
  long *ivt_list = nullptr;
  size_t list_size = 0;
  uint8_t *ivt_codes_list = nullptr;
  bool ret = rt_invert_index_ptr_->GetIvtList(list_no, ivt_list, list_size,
                                              ivt_codes_list);
  if (!ret)
    return nullptr;
  return ivt_codes_list;
}

const idx_t *RTInvertedLists::get_ids(size_t list_no) const {
  if (!rt_invert_index_ptr_)
    return nullptr;
  long *ivt_list = nullptr;
  size_t list_size = 0;
  uint8_t *ivt_codes_list = nullptr;
  bool ret = rt_invert_index_ptr_->GetIvtList(list_no, ivt_list, list_size,
                                              ivt_codes_list);
  if (!ret)
    return nullptr;
  idx_t *ivt_lists = reinterpret_cast<idx_t *>(ivt_list);
  return ivt_lists;
}

size_t RTInvertedLists::add_entries(size_t list_no, size_t n_entry,
                                    const idx_t *ids, const uint8_t *code) {
  return 0;
}

void RTInvertedLists::resize(size_t list_no, size_t new_size) {}

void RTInvertedLists::update_entries(size_t list_no, size_t offset,
                                     size_t n_entry, const idx_t *ids_in,
                                     const uint8_t *codes_in) {}

} // namespace tig_gamma
