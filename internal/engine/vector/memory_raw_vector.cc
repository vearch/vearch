/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "memory_raw_vector.h"

#include <unistd.h>
#include <functional>

using std::string;
namespace vearch {

MemoryRawVector::MemoryRawVector(VectorMetaInfo *meta_info,
                                 const StoreParams &store_params,
                                 bitmap::BitmapManager *docids_bitmap,
                                 StorageManager *storage_mgr, int cf_id)
    : RawVector(meta_info, docids_bitmap, store_params) {
  segments_ = nullptr;
  nsegments_ = 0;
  storage_mgr_ = storage_mgr;
  cf_id_ = cf_id;
  segment_size_ = store_params.segment_size;
  vector_byte_size_ = meta_info->DataSize() * meta_info->Dimension();
  curr_idx_in_seg_ = 0;
  compact_if_need_ = true;
  compact_ratio_ = 0.3;
}

MemoryRawVector::~MemoryRawVector() {
  for (int i = 0; i < nsegments_; i++) {
    CHECK_DELETE_ARRAY(segments_[i]);
  }
  CHECK_DELETE_ARRAY(segments_);
}

Status MemoryRawVector::Load(int64_t vec_num) {
  std::unique_ptr<rocksdb::Iterator> it = storage_mgr_->NewIterator(cf_id_);
  string start_key = utils::ToRowKey(0);
  it->Seek(rocksdb::Slice(start_key));
  string end_key = utils::ToRowKey(vec_num);

  int64_t n_load = 0;
  for (auto c = 0; c < vec_num; c++, it->Next()) {
    rocksdb::Slice current_key = it->key();
    if (current_key.compare(end_key) >= 0) {
        break;
    }
    if (!it->Valid()) {
      LOG(ERROR)  << desc_ << "memory vector load rocksdb iterator error, current_key=" << current_key.ToString()
                 << ", start_key=" << start_key << ", end_key=" << end_key << ", c=" << c;
      break;
    }
    rocksdb::Slice value = it->value();
    AddToMem(c, (uint8_t *)value.data_, VectorByteSize());
    n_load++;
  }

  MetaInfo()->size_ = vec_num;
  LOG(INFO)  << desc_ << "memory raw vector want to load [" << vec_num << "], real load ["
            << n_load << "]";

  return Status::OK();
}

int MemoryRawVector::GetDiskVecNum(int64_t &vec_num) {
  if (vec_num <= 0) return 0;
  int disk_vec_num = vec_num - 1;
  string key, value;
  for (int64_t i = disk_vec_num; i >= 0; --i) {
    key = utils::ToRowKey(i);
    Status s = storage_mgr_->Get(cf_id_, key, value);
    if (s.ok()) {
      vec_num = i + 1;
      LOG(INFO)  << desc_ << "in the disk rocksdb vec_num=" << vec_num;
      return 0;
    }
  }
  vec_num = 0;
  LOG(INFO)  << desc_ << "in the disk rocksdb vec_num=" << vec_num;
  return 0;
}

int MemoryRawVector::InitStore(std::string &vec_name) {
  segments_ = new (std::nothrow) uint8_t *[kMaxSegments];
  if (segments_ == nullptr) {
    LOG(ERROR)  << desc_ << "malloc new segment failed, segment size=" << segment_size_;
    return -1;
  }
  std::fill_n(segments_, kMaxSegments, nullptr);

  segment_deleted_nums_ = new (std::nothrow) std::atomic<uint32_t>[kMaxSegments];
  if (segment_deleted_nums_ == nullptr) {
    LOG(ERROR)  << desc_ << "malloc new segment failed, segment size=" << segment_size_;
    return -2;
  }
  std::fill_n(segment_deleted_nums_, kMaxSegments, 0);

  segment_nums_ = new (std::nothrow) std::atomic<uint32_t>[kMaxSegments];
  if (segment_nums_ == nullptr) {
    LOG(ERROR)  << desc_ << "malloc new segment failed, segment size=" << segment_size_;
    return -3;
  }
  std::fill_n(segment_nums_, kMaxSegments, 0);

  if (ExtendSegments()) return -4;

  LOG(INFO)  << desc_ << "init memory raw vector success! vector byte size="
            << vector_byte_size_ << ", " + meta_info_->Name();
  return 0;
}

int MemoryRawVector::AddToStore(uint8_t *v, int len) {
  AddToMem(meta_info_->Size(), v, vector_byte_size_);
  if (WithIO()) {
    storage_mgr_->Add(cf_id_, meta_info_->Size(), v, VectorByteSize());
  }
  return 0;
}

int MemoryRawVector::DeleteFromStore(int64_t vid) {
  if (WithIO()) {
    std::string key = utils::ToRowKey(vid);
    Status s = storage_mgr_->Delete(cf_id_, key);
    if (!s.ok()) {
      LOG(ERROR) << desc_ << "rocksdb delete error:" << s.ToString() << ", key=" << key;
      return -1;
    }
  }
  segment_deleted_nums_[vid / segment_size_] += 1;
  return 0;
}

int MemoryRawVector::AddToMem(int64_t vid, uint8_t *v, int len) {
  assert(len == vector_byte_size_);
  if ((vid / segment_size_ >= nsegments_) && ExtendSegments()) return -2;
  memcpy((void *)(segments_[vid / segment_size_] + (vid % segment_size_) * vector_byte_size_),
         (void *)v, vector_byte_size_);
  segment_nums_[vid / segment_size_] += 1;
  return 0;
}

int MemoryRawVector::ExtendSegments() {
  if (nsegments_ >= kMaxSegments) {
    LOG(ERROR) << this->desc_ << "segment number can't be > " << kMaxSegments;
    return -1;
  }
  segments_[nsegments_] =
      new (std::nothrow) uint8_t[segment_size_ * vector_byte_size_];
  if (segments_[nsegments_] == nullptr) {
    LOG(ERROR) << this->desc_
               << "malloc new segment failed, segment num=" << nsegments_
               << ", segment size=" << segment_size_;
    return -1;
  }
  curr_idx_in_seg_ = 0;
  ++nsegments_;
  LOG(DEBUG)  << desc_ << "extend segment success! nsegments=" << nsegments_;
  return 0;
}

int MemoryRawVector::GetVectorHeader(int64_t start, int n, ScopeVectors &vecs,
                                     std::vector<int> &lens) {
  if (start + n > meta_info_->Size()) return -1;

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

int MemoryRawVector::UpdateToStore(int64_t vid, uint8_t *v, int len) {
  if (vid >= meta_info_->Size() || vid < 0) {
    return -1;
  }
  if (docids_bitmap_->Test(vid)) {
    return -2;
  }
  memcpy((void *)(segments_[vid / segment_size_] +
                  (size_t)vid % segment_size_ * vector_byte_size_),
         (void *)v, vector_byte_size_);
  if (WithIO()) {
    storage_mgr_->Add(cf_id_, vid, v, VectorByteSize());
  }
  return 0;
}

int MemoryRawVector::GetVector(int64_t vid, const uint8_t *&vec,
                               bool &deletable) const {
  if (vid >= meta_info_->Size() || vid < 0) {
    return -1;
  }
  if (docids_bitmap_->Test(vid)) {
    vec = nullptr;
    deletable = false;
    return -2;
  }
  vec = segments_[vid / segment_size_] +
        (size_t)vid % segment_size_ * vector_byte_size_;

  deletable = false;
  return 0;
}

uint8_t *MemoryRawVector::GetFromMem(int64_t vid) const {
  return segments_[vid / segment_size_] +
         (size_t)vid % segment_size_ * vector_byte_size_;
}

bool MemoryRawVector::Compactable(int segment_no) {
  return (float)segment_deleted_nums_[segment_no] /
    segment_nums_[segment_no] >= compact_ratio_;
}

void FreeOldPtr(uint8_t *temp) { delete []temp; temp = nullptr; }

Status MemoryRawVector::Compact() {
  // only compact sealed segments
  for (int i = 0; i < nsegments_ - 1; i++) {
    if (Compactable(i)) {
      uint8_t *new_segment = new (std::nothrow) uint8_t[segment_size_ * vector_byte_size_];
      if (new_segment == nullptr) {
        LOG(ERROR)  << desc_ << "malloc new segment failed, segment size=" << segment_size_;
        return Status::ParamError(desc_ + "malloc new segment failed");
      }
      int new_idx = 0;
      for (uint32_t j = 0; j < segment_nums_[i]; j++) {
        if (!docids_bitmap_->Test(i * segment_size_ + j)) {
          memcpy(new_segment + j * vector_byte_size_,
                 segments_[i] + j * vector_byte_size_, vector_byte_size_);
          new_idx++;
        }
      }
      uint8_t *old_segment = segments_[i];
      segments_[i] = new_segment;
      uint32_t old_segment_num = segment_nums_[i];
      uint32_t old_segment_deleted_num = segment_deleted_nums_[i];
      segment_deleted_nums_[i] = 0;
      segment_nums_[i] = new_idx;

      LOG(INFO)  << desc_ << "compact segment=" << i << ", new_idx=" << new_idx
                << ", old_idx=" << old_segment_num << ", deleted_num="
                << old_segment_deleted_num;

      // delay free
      std::function<void(uint8_t *)> func_free =
        std::bind(&FreeOldPtr, std::placeholders::_1);
      utils::AsyncWait(10000, func_free, old_segment);
    } else {
      LOG(INFO)  << desc_ << "segment=" << i << " no need to compact, num=" << segment_nums_[i]
                << ", deleted_num=" << segment_deleted_nums_[i];
    }
  }
  return Status::OK();
}

}  // namespace vearch
