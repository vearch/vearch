/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "memory_raw_vector.h"

#include <unistd.h>

using std::string;
namespace vearch {

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
}

MemoryRawVector::~MemoryRawVector() {
  for (int i = 0; i < nsegments_; i++) {
    CHECK_DELETE_ARRAY(segments_[i]);
  }
  CHECK_DELETE_ARRAY(segments_);
}

int MemoryRawVector::InitStore(std::string &vec_name) {
  segments_ = new uint8_t *[kMaxSegments];
  std::fill_n(segments_, kMaxSegments, nullptr);
  if (ExtendSegments()) return -2;

  LOG(INFO) << "init memory raw vector success! vector byte size="
            << vector_byte_size_
            << ", path=" << root_path_ + "/" + meta_info_->Name();
  return 0;
}

int MemoryRawVector::AddToStore(uint8_t *v, int len) {
  AddToMem(v, vector_byte_size_);
  return 0;
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
    return -1;
  }
  segments_[nsegments_] =
      new (std::nothrow) uint8_t[segment_size_ * vector_byte_size_];
  current_segment_ = segments_[nsegments_];
  if (current_segment_ == nullptr) {
    LOG(ERROR) << this->desc_
               << "malloc new segment failed, segment num=" << nsegments_
               << ", segment size=" << segment_size_;
    return -1;
  }
  curr_idx_in_seg_ = 0;
  ++nsegments_;
  LOG(INFO) << "extend segment sucess! nsegments=" << nsegments_;
  return 0;
}

int MemoryRawVector::GetVectorHeader(int start, int n, ScopeVectors &vecs,
                                     std::vector<int> &lens) {
  if (start + n > (int)meta_info_->Size()) return -1;

  while (n) {
    uint8_t *vec = segments_[start / segment_size_] +
                   (size_t)start % segment_size_ * vector_byte_size_;
    int len = segment_size_ - start % segment_size_;
    if (len > n) len = n;

    bool deletable = false;
    vecs.Add(vec, deletable);
    lens.push_back(len);
    start += len;
    n -= len;
  }
  return 0;
}

int MemoryRawVector::UpdateToStore(int vid, uint8_t *v, int len) {
  memcpy((void *)(segments_[vid / segment_size_] +
                  (size_t)vid % segment_size_ * vector_byte_size_),
         (void *)v, vector_byte_size_);
  return 0;
}

int MemoryRawVector::GetVector(long vid, const uint8_t *&vec,
                               bool &deletable) const {
  vec = segments_[vid / segment_size_] +
        (size_t)vid % segment_size_ * vector_byte_size_;

  deletable = false;
  return 0;
}

uint8_t *MemoryRawVector::GetFromMem(long vid) const {
  return segments_[vid / segment_size_] +
         (size_t)vid % segment_size_ * vector_byte_size_;
}

}  // namespace vearch
