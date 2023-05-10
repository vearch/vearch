/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "memory_raw_vector.h"

#include <unistd.h>

#include "search/error_code.h"

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
  storage_mgr_ = nullptr;
  allow_use_zfp = false;
}

MemoryRawVector::~MemoryRawVector() {
  for (int i = 0; i < nsegments_; i++) {
    CHECK_DELETE_ARRAY(segments_[i]);
  }
  CHECK_DELETE_ARRAY(segments_);
  CHECK_DELETE(storage_mgr_);
}

int MemoryRawVector::InitStore(std::string &vec_name) {
  segments_ = new uint8_t *[kMaxSegments];
  std::fill_n(segments_, kMaxSegments, nullptr);
  if (ExtendSegments()) return -2;
  LOG(INFO) << "init success, segment_size=" << segment_size_;

  std::string vec_dir = root_path_ + "/" + meta_info_->Name();
  uint32_t var = std::numeric_limits<uint32_t>::max();
  uint32_t max_seg_size = var / vector_byte_size_;
  uint32_t seg_block_capacity = 2000000;
  if ((int)max_seg_size < store_params_.segment_size) {
    store_params_.segment_size = max_seg_size;
    seg_block_capacity = 4000000000 / (1000000000 / max_seg_size + 1) - 1;
    LOG(INFO) << "Because the vector length is too long, segment_size becomes "
              << max_seg_size << " and seg_block_capacity becomes "
              << seg_block_capacity;
  }
  StorageManagerOptions options;
  options.segment_size = store_params_.segment_size;
  options.fixed_value_bytes = vector_byte_size_;
  options.seg_block_capacity = seg_block_capacity;
  storage_mgr_ =
      new StorageManager(vec_dir, BlockType::VectorBlockType, options);

  int ret = storage_mgr_->Init(vec_name, 0);
  if (ret) {
    LOG(ERROR) << "init gamma db error, ret=" << ret;
    return ret;
  }

  LOG(INFO) << "init memory raw vector success! vector byte size="
            << vector_byte_size_ << ", path=" << vec_dir;
  return SUCC;
}

int MemoryRawVector::AddToStore(uint8_t *v, int len) {
  int ret = storage_mgr_->Add(v, len);

  ret = AddToMem(v, vector_byte_size_);
  return ret;
}

int MemoryRawVector::AddToMem(const uint8_t *v, int len) {
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

    bool deletable = false;
    vecs.Add(cmprs_v, deletable);
    lens.push_back(len);
    start += len;
    n -= len;
  }
  return SUCC;
}

int MemoryRawVector::UpdateToStore(int vid, uint8_t *v, int len) {
  memcpy((void *)(segments_[vid / segment_size_] +
                  (size_t)vid % segment_size_ * vector_byte_size_),
         (void *)v, vector_byte_size_);
  return storage_mgr_->Update(vid, v, len);
}

int MemoryRawVector::GetVector(long vid, const uint8_t *&vec,
                               bool &deletable) const {
  deletable = false;
  vec = segments_[vid / segment_size_] +
        (size_t)vid % segment_size_ * vector_byte_size_;
  return SUCC;
}

uint8_t *MemoryRawVector::GetFromMem(long vid) const {
  uint8_t *cmprs_v = segments_[vid / segment_size_] +
                     (size_t)vid % segment_size_ * vector_byte_size_;
  return cmprs_v;
}

}  // namespace tig_gamma
