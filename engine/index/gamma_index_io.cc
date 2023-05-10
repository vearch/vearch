/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "gamma_index_io.h"

#include <numeric>

#include "search/error_code.h"
#include "util/log.h"

namespace tig_gamma {
void write_index_header(const faiss::Index *idx, faiss::IOWriter *f) {
  WRITE1(idx->d);
  WRITE1(idx->ntotal);
  faiss::Index::idx_t dummy = 1 << 20;
  WRITE1(dummy);
  WRITE1(dummy);
  WRITE1(idx->is_trained);
  WRITE1(idx->metric_type);
}

void write_direct_map(const faiss::DirectMap *dm, faiss::IOWriter *f) {
  char maintain_direct_map =
      (char)dm->type;  // for backwards compatibility with bool
  WRITE1(maintain_direct_map);
  WRITEVECTOR(dm->array);
  if (dm->type == faiss::DirectMap::Hashtable) {
    using idx_t = faiss::Index::idx_t;
    std::vector<std::pair<idx_t, idx_t>> v;
    const std::unordered_map<idx_t, idx_t> &map = dm->hashtable;
    v.resize(map.size());
    std::copy(map.begin(), map.end(), v.begin());
    WRITEVECTOR(v);
  }
}

void write_ivf_header(const faiss::IndexIVF *ivf, faiss::IOWriter *f) {
  write_index_header(ivf, f);
  WRITE1(ivf->nlist);
  WRITE1(ivf->nprobe);
  faiss::write_index(ivf->quantizer, f);
  write_direct_map(&ivf->direct_map, f);
}

void read_index_header(faiss::Index *idx, faiss::IOReader *f) {
  READ1(idx->d);
  READ1(idx->ntotal);
  faiss::Index::idx_t dummy;
  READ1(dummy);
  READ1(dummy);
  READ1(idx->is_trained);
  READ1(idx->metric_type);
  idx->verbose = false;
}

void read_direct_map(faiss::DirectMap *dm, faiss::IOReader *f) {
  char maintain_direct_map;
  READ1(maintain_direct_map);
  dm->type = (faiss::DirectMap::Type)maintain_direct_map;
  READVECTOR(dm->array);
  if (dm->type == faiss::DirectMap::Hashtable) {
    using idx_t = faiss::Index::idx_t;
    std::vector<std::pair<idx_t, idx_t>> v;
    READVECTOR(v);
    std::unordered_map<idx_t, idx_t> &map = dm->hashtable;
    map.reserve(v.size());
    for (auto it : v) {
      map[it.first] = it.second;
    }
  }
}

void read_ivf_header(faiss::IndexIVF *ivf, faiss::IOReader *f,
                     std::vector<std::vector<faiss::Index::idx_t>> *ids) {
  read_index_header(ivf, f);
  READ1(ivf->nlist);
  READ1(ivf->nprobe);
  ivf->quantizer = faiss::read_index(f);
  ivf->own_fields = true;
  if (ids) {  // used in legacy "Iv" formats
    ids->resize(ivf->nlist);
    for (size_t i = 0; i < ivf->nlist; i++) READVECTOR((*ids)[i]);
  }
  read_direct_map(&ivf->direct_map, f);
  // READ1(ivf->maintain_direct_map);
  // READVECTOR(ivf->direct_map);
}

void write_product_quantizer(const faiss::ProductQuantizer *pq,
                             faiss::IOWriter *f) {
  WRITE1(pq->d);
  WRITE1(pq->M);
  WRITE1(pq->nbits);
  WRITEVECTOR(pq->centroids);
}

void read_product_quantizer(faiss::ProductQuantizer *pq, faiss::IOReader *f) {
  READ1(pq->d);
  READ1(pq->M);
  READ1(pq->nbits);
  pq->set_derived_values();
  READVECTOR(pq->centroids);
}

int WriteInvertedLists(faiss::IOWriter *f,
                       realtime::RTInvertIndex *rt_invert_index) {
  realtime::RealTimeMemData *rt_data = rt_invert_index->cur_ptr_;
  // write header
  uint32_t h = faiss::fourcc("ilar");
  WRITE1(h);
  WRITE1(rt_data->buckets_num_);
  WRITE1(rt_data->code_bytes_per_vec_);
  uint32_t list_type = faiss::fourcc("full");
  WRITE1(list_type);

  std::vector<size_t> sizes;
  sizes.resize(rt_data->buckets_num_);
  for (size_t i = 0; i < rt_data->buckets_num_; ++i) {
    size_t size = rt_data->cur_invert_ptr_->retrieve_idx_pos_[i];
    sizes[i] = size;
  }
  // memcpy((void *)sizes.data(), rt_data->cur_invert_ptr_->retrieve_idx_pos_,
  //        sizeof(size_t) * rt_data->buckets_num_);
  WRITEVECTOR(sizes);

  for (size_t i = 0; i < rt_data->buckets_num_; i++) {
    if (sizes[i] > 0) {
      WRITEANDCHECK(rt_data->cur_invert_ptr_->codes_array_[i],
                    sizes[i] * rt_data->code_bytes_per_vec_);

      WRITEANDCHECK(rt_data->cur_invert_ptr_->idx_array_[i], sizes[i]);
    }
  }
  size_t ntotal = std::accumulate(sizes.data(), sizes.data() + sizes.size(), 0);
  LOG(INFO) << "ids_count=" << ntotal
            << ", buckets_num_=" << rt_data->buckets_num_;
  return 0;
}

int ReadInvertedLists(faiss::IOReader *f,
                      realtime::RTInvertIndex *rt_invert_index,
                      int &indexed_vec_count) {
  realtime::RealTimeMemData *rt_data = rt_invert_index->cur_ptr_;
  uint32_t h;
  size_t buckets_num = 0, code_bytes = 0;
  uint32_t list_type = 0;
  READ1(h);
  READ1(buckets_num);
  READ1(code_bytes);
  READ1(list_type);

  assert(h == faiss::fourcc("ilar"));
  assert(list_type == faiss::fourcc("full"));
  if (buckets_num != rt_data->buckets_num_ ||
      code_bytes != rt_data->code_bytes_per_vec_) {
    return FORMAT_ERR;
  }

  std::vector<size_t> sizes;
  READVECTOR(sizes);
  assert(sizes.size() == rt_data->buckets_num_);

  for (long bno = 0; (size_t)bno < rt_data->buckets_num_; ++bno) {
    if (sizes[bno] == 0) continue;

    indexed_vec_count += sizes[bno];
    if (rt_data->ExtendBucketIfNeed(bno, sizes[bno])) {
      LOG(ERROR) << "loading, extend bucket error";
      return INTERNAL_ERR;
    }
    uint8_t *codes = rt_data->cur_invert_ptr_->codes_array_[bno];
    long *ids = rt_data->cur_invert_ptr_->idx_array_[bno];
    READANDCHECK(codes, sizes[bno] * rt_data->code_bytes_per_vec_);
    READANDCHECK(ids, sizes[bno]);

    for (int pos = 0; pos < (int)sizes[bno]; pos++) {
      if (ids[pos] & realtime::kDelIdxMask) {
        rt_data->cur_invert_ptr_->deleted_nums_[bno]++;
        continue;
      }
      while ((size_t)ids[pos] >= rt_data->cur_invert_ptr_->nids_) {
        rt_data->cur_invert_ptr_->ExtendIDs();
      }
      rt_data->cur_invert_ptr_->vid_bucket_no_pos_[ids[pos]] = bno << 32 | pos;
      indexed_vec_count -= rt_data->cur_invert_ptr_->deleted_nums_[bno];
    }

    rt_data->cur_invert_ptr_->retrieve_idx_pos_[bno] = sizes[bno];
  }
  return 0;
}

void write_hnsw(const faiss::HNSW *hnsw, faiss::IOWriter *f) {
  WRITEVECTOR(hnsw->assign_probas);
  WRITEVECTOR(hnsw->cum_nneighbor_per_level);
  WRITEVECTOR(hnsw->levels);
  WRITEVECTOR(hnsw->offsets);
  WRITEVECTOR(hnsw->neighbors);

  WRITE1(hnsw->entry_point);
  WRITE1(hnsw->max_level);
  WRITE1(hnsw->efConstruction);
  WRITE1(hnsw->efSearch);
  WRITE1(hnsw->upper_beam);
}

void read_hnsw(faiss::HNSW *hnsw, faiss::IOReader *f) {
  READVECTOR(hnsw->assign_probas);
  READVECTOR(hnsw->cum_nneighbor_per_level);
  READVECTOR(hnsw->levels);
  READVECTOR(hnsw->offsets);
  READVECTOR(hnsw->neighbors);

  READ1(hnsw->entry_point);
  READ1(hnsw->max_level);
  READ1(hnsw->efConstruction);
  READ1(hnsw->efSearch);
  READ1(hnsw->upper_beam);
}

void write_opq(const faiss::VectorTransform *vt, faiss::IOWriter *f) {
  const faiss::LinearTransform *lt =
      dynamic_cast<const faiss::LinearTransform *>(vt);
  // generic LinearTransform (includes OPQ)
  uint32_t h = faiss::fourcc("LTra");
  WRITE1(h);

  WRITE1(lt->have_bias);
  WRITEVECTOR(lt->A);
  WRITEVECTOR(lt->b);

  // common fields
  WRITE1(vt->d_in);
  WRITE1(vt->d_out);
  WRITE1(vt->is_trained);
}

void read_opq(faiss::VectorTransform *vt, faiss::IOReader *f) {
  uint32_t h;
  READ1(h);
  faiss::LinearTransform *lt = nullptr;
  if (h == faiss::fourcc("LTra")) {
    lt = dynamic_cast<faiss::LinearTransform *>(vt);
  } else {
    return;
  }
  READ1(lt->have_bias);
  READVECTOR(lt->A);
  READVECTOR(lt->b);
  FAISS_THROW_IF_NOT(lt->A.size() >= (size_t)lt->d_in * lt->d_out);
  FAISS_THROW_IF_NOT(!lt->have_bias || lt->b.size() >= (size_t)lt->d_out);
  lt->set_is_orthonormal();

  READ1(vt->d_in);
  READ1(vt->d_out);
  READ1(vt->is_trained);
}

}  // namespace tig_gamma
