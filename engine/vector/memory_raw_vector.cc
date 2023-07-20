/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include <unistd.h>
#include "memory_raw_vector.h"
#include "common/error_code.h"

using std::string;
namespace tig_gamma {

MemoryRawVector::MemoryRawVector(VectorMetaInfo *meta_info,
                                 const std::string &root_path,
                                 const StoreParams &store_params,
                                 bitmap::BitmapManager *docids_bitmap)
    : RawVector(meta_info, root_path, docids_bitmap, store_params) {
  segments_ = nullptr;
  nsegments_ = 0;
  segment_size_ = store_params.segment_size;
  vector_byte_size_ = meta_info->DataSize() * meta_info->Dimension();
  current_segment_ = nullptr;
  curr_idx_in_seg_ = 0;
  allow_use_zfp = false;
}

MemoryRawVector::~MemoryRawVector() {
  for (int i = 0; i < nsegments_; i++) {
    CHECK_DELETE_ARRAY(segments_[i]);
  }
  CHECK_DELETE_ARRAY(segments_);
}

int MemoryRawVector::InitStore(std::string &vec_name) {
  // const std::string &name = meta_info_->Name();
  // string db_path = this->root_path_ + "/" + name;
  // if (rdb_.Open(db_path)) {
  //   LOG(ERROR) << "open rocks db error, path=" << db_path;
  //   return IO_ERR;
  // }
  segments_ = new uint8_t *[kMaxSegments];
  std::fill_n(segments_, kMaxSegments, nullptr);
  if (ExtendSegments()) return -2;
  
  LOG(INFO) << "init memory raw vector success! vector byte size="
            << vector_byte_size_ << ", path=" << root_path_ + "/" + meta_info_->Name();
  return SUCC;
}

int MemoryRawVector::AddToStore(uint8_t *v, int len) {
  ScopeVector svec;
  if (Compress(v, svec)) {
    return INTERNAL_ERR;
  }

  AddToMem((uint8_t *)svec.Get(), vector_byte_size_);
  // int total = meta_info_->Size();
  // rdb_.Put(total, (const char *)v, vector_byte_size_);
  return SUCC;
}

int MemoryRawVector::AddToMem(uint8_t *v, int len) {
  assert(len == vector_byte_size_);
  if (curr_idx_in_seg_ == segment_size_ && ExtendSegments()) return -2;
  memcpy((void *)(current_segment_ + curr_idx_in_seg_ * vector_byte_size_),
         (void *)v, vector_byte_size_);
  ++curr_idx_in_seg_;
  return 0;
}

int MemoryRawVector::ExtendSegments() {
  if (nsegments_ >= kMaxSegments) {
    LOG(ERROR) << this->desc_ << "segment number can't be > " << kMaxSegments;
    return LIMIT_ERR;
  }
  segments_[nsegments_] =
      new (std::nothrow) uint8_t[segment_size_ * vector_byte_size_];
  current_segment_ = segments_[nsegments_];
  if (current_segment_ == nullptr) {
    LOG(ERROR) << this->desc_
               << "malloc new segment failed, segment num=" << nsegments_
               << ", segment size=" << segment_size_;
    return ALLOC_ERR;
  }
  curr_idx_in_seg_ = 0;
  ++nsegments_;
  LOG(INFO) << "extend segment sucess! nsegments=" << nsegments_;
  return SUCC;
}

int MemoryRawVector::GetVectorHeader(int start, int n, ScopeVectors &vecs,
                                     std::vector<int> &lens) {
  if (start + n > (int)meta_info_->Size()) return -1;

  while (n) {
    uint8_t *cmprs_v = segments_[start / segment_size_] +
                       (size_t)start % segment_size_ * vector_byte_size_;
    int len = segment_size_ - start % segment_size_;
    if (len > n) len = n;

    uint8_t *vec = nullptr;
    bool deletable = false;
    if (Decompress(cmprs_v, len, vec, deletable)) {
      return INTERNAL_ERR;
    }

    vecs.Add(vec, deletable);
    lens.push_back(len);
    start += len;
    n -= len;
  }
  return SUCC;
}

int MemoryRawVector::UpdateToStore(int vid, uint8_t *v, int len) {
  ScopeVector svec;
  if (this->Compress(v, svec)) {
    return INTERNAL_ERR;
  }

  memcpy((void *)(segments_[vid / segment_size_] +
                  (size_t)vid % segment_size_ * vector_byte_size_),
         (void *)svec.Get(), vector_byte_size_);
  // rdb_.Put(vid, (const char *)svec.Get(), vector_byte_size_);
  return SUCC;
}

int MemoryRawVector::GetVector(long vid, const uint8_t *&vec,
                               bool &deletable) const {
  uint8_t *cmprs_v = segments_[vid / segment_size_] +
                     (size_t)vid % segment_size_ * vector_byte_size_;
  uint8_t *v = nullptr;
  if (Decompress(cmprs_v, 1, v, deletable)) {
    return INTERNAL_ERR;
  }
  vec = v;
  return SUCC;
}

uint8_t *MemoryRawVector::GetFromMem(long vid) const {
  uint8_t *cmprs_v = segments_[vid / segment_size_] +
                     (size_t)vid % segment_size_ * vector_byte_size_;
  return cmprs_v;
}

}  // namespace tig_gamma
