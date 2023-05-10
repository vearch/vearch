/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "realtime_mem_data.h"

#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "faiss/impl/io.h"
#include "search/error_code.h"
#include "util/bitmap.h"
#include "util/log.h"
#include "util/utils.h"

namespace tig_gamma {
namespace realtime {

RTInvertBucketData::RTInvertBucketData(RTInvertBucketData *other) {
  idx_array_ = other->idx_array_;
  retrieve_idx_pos_ = other->retrieve_idx_pos_;
  cur_bucket_keys_ = other->cur_bucket_keys_;
  bucket_extend_time_ = other->bucket_extend_time_;
  codes_array_ = other->codes_array_;
  vid_mgr_ = other->vid_mgr_;
  docids_bitmap_ = other->docids_bitmap_;
  vid_bucket_no_pos_ = other->vid_bucket_no_pos_;
  deleted_nums_ = other->deleted_nums_;
  compacted_num_ = other->compacted_num_;
  buckets_num_ = other->buckets_num_;
  nids_ = other->nids_;
}

RTInvertBucketData::RTInvertBucketData(VIDMgr *vid_mgr,
                                       bitmap::BitmapManager *docids_bitmap)
    : docids_bitmap_(docids_bitmap) {
  idx_array_ = nullptr;
  retrieve_idx_pos_ = nullptr;
  cur_bucket_keys_ = nullptr;
  bucket_extend_time_ = nullptr;
  codes_array_ = nullptr;
  vid_mgr_ = vid_mgr;
  vid_bucket_no_pos_ = nullptr;
  deleted_nums_ = nullptr;
  compacted_num_ = 0;
  buckets_num_ = 0;
  nids_ = 0;
}

RTInvertBucketData::~RTInvertBucketData() {}

bool RTInvertBucketData::Init(const size_t &buckets_num,
                              const size_t &bucket_keys,
                              const size_t &code_bytes_per_vec,
                              std::atomic<long> &total_mem_bytes) {
  idx_array_ = new (std::nothrow) long *[buckets_num];
  codes_array_ = new (std::nothrow) uint8_t *[buckets_num];
  cur_bucket_keys_ = new (std::nothrow) int[buckets_num];
  bucket_extend_time_ = new (std::nothrow) uint8_t[buckets_num];
  memset(bucket_extend_time_, 0, buckets_num * sizeof(uint8_t));
  deleted_nums_ = new (std::nothrow) std::atomic<int>[buckets_num];
  if (idx_array_ == nullptr || codes_array_ == nullptr ||
      cur_bucket_keys_ == nullptr || deleted_nums_ == nullptr)
    return false;
  for (size_t i = 0; i < buckets_num; i++) {
    idx_array_[i] = new (std::nothrow) long[bucket_keys];
    codes_array_[i] =
        new (std::nothrow) uint8_t[bucket_keys * code_bytes_per_vec];
    if (idx_array_[i] == nullptr || codes_array_[i] == nullptr) return false;
    cur_bucket_keys_[i] = bucket_keys;
    deleted_nums_[i] = 0;
  }
  nids_ = buckets_num * bucket_keys;
  vid_bucket_no_pos_ = new std::atomic<long>[nids_];
  for (size_t i = 0; i < nids_; i++) vid_bucket_no_pos_[i] = -1;

  total_mem_bytes += buckets_num * bucket_keys * sizeof(long);
  total_mem_bytes +=
      buckets_num * bucket_keys * code_bytes_per_vec * sizeof(uint8_t);
  total_mem_bytes += buckets_num * sizeof(int);

  retrieve_idx_pos_ = new (std::nothrow) size_t[buckets_num];
  if (retrieve_idx_pos_ == nullptr) return false;
  memset(retrieve_idx_pos_, 0, buckets_num * sizeof(retrieve_idx_pos_[0]));
  total_mem_bytes += buckets_num * sizeof(size_t);
  buckets_num_ = buckets_num;

  LOG(INFO) << "init success! total_mem_bytes=" << total_mem_bytes
            << ", current max size=" << nids_;
  return true;
}

void RTInvertBucketData::CompactOne(const size_t &bucket_no, long *&dst_idx,
                                    uint8_t *&dst_code, long *&src_idx,
                                    uint8_t *&src_code, int &pos,
                                    const size_t &code_bytes_per_vec) {
  if (!(*src_idx & kDelIdxMask) && not docids_bitmap_->Test(vid_mgr_->VID2DocID(
                                       *src_idx & kRecoverIdxMask))) {
    *dst_idx = *src_idx;
    memcpy((void *)dst_code, (void *)(src_code), (size_t)code_bytes_per_vec);
    vid_bucket_no_pos_[*dst_idx] = bucket_no << 32 | pos++;
    ++dst_idx;
    dst_code += code_bytes_per_vec;
  }
  ++src_idx;
  src_code += code_bytes_per_vec;
}

double RTInvertBucketData::ExtendCoefficient(uint8_t extend_time) {
  double result = 1.1 + PI / 2 - atan(extend_time);
  return result;
}

bool RTInvertBucketData::CompactBucket(const size_t &bucket_no,
                                       const size_t &code_bytes_per_vec) {
  int old_pos = retrieve_idx_pos_[bucket_no];
  long *old_idx_ptr = idx_array_[bucket_no];
  uint8_t *old_codes_ptr = codes_array_[bucket_no];

  int pos = 0;
  long *idx_array = (long *)malloc(sizeof(long) * cur_bucket_keys_[bucket_no]);
  uint8_t *codes_array =
      (uint8_t *)malloc(code_bytes_per_vec * cur_bucket_keys_[bucket_no]);
  long *idx_ptr = idx_array;
  uint8_t *codes_ptr = codes_array;

  for (int i = 0; i < old_pos; i++) {
    CompactOne(bucket_no, idx_ptr, codes_ptr, old_idx_ptr, old_codes_ptr, pos,
               code_bytes_per_vec);
  }

  idx_array_[bucket_no] = idx_array;
  codes_array_[bucket_no] = codes_array;
  int n = retrieve_idx_pos_[bucket_no] - pos;
  compacted_num_ += n;
  retrieve_idx_pos_[bucket_no] = pos;
  deleted_nums_[bucket_no] = 0;

#ifdef DEBUG
  LOG(INFO) << "compact bucket=" << bucket_no
            << " success! current codes num=" << pos << " compacted num=" << n
            << ", total compacted num=" << compacted_num_;
#endif
  return true;
}

bool RTInvertBucketData::ExtendBucketMem(const size_t &bucket_no,
                                         const int &increment,
                                         const size_t &code_bytes_per_vec,
                                         std::atomic<long> &total_mem_bytes) {
  int least = retrieve_idx_pos_[bucket_no] + increment;
  double coefficient = ExtendCoefficient(++bucket_extend_time_[bucket_no]);
  int extend_size = (int)(cur_bucket_keys_[bucket_no] * coefficient);
  while (extend_size < least) {
    coefficient = ExtendCoefficient(++bucket_extend_time_[bucket_no]);
    extend_size = (int)(extend_size * coefficient);
  }

  uint8_t *extend_code_bytes_array =
      new (std::nothrow) uint8_t[extend_size * code_bytes_per_vec];
  if (extend_code_bytes_array == nullptr) {
    LOG(ERROR) << "memory extend_code_bytes_array alloc error!";
    return false;
  }
  memcpy((void *)extend_code_bytes_array, (void *)codes_array_[bucket_no],
         sizeof(uint8_t) * retrieve_idx_pos_[bucket_no] * code_bytes_per_vec);
  codes_array_[bucket_no] = extend_code_bytes_array;
  total_mem_bytes += extend_size * code_bytes_per_vec * sizeof(uint8_t);

  long *extend_idx_array = new (std::nothrow) long[extend_size];
  if (extend_idx_array == nullptr) {
    LOG(ERROR) << "memory extend_idx_array alloc error!";
    return false;
  }
  memcpy((void *)extend_idx_array, (void *)idx_array_[bucket_no],
         sizeof(long) * retrieve_idx_pos_[bucket_no]);
  idx_array_[bucket_no] = extend_idx_array;
  total_mem_bytes += extend_size * sizeof(long);

  cur_bucket_keys_[bucket_no] = extend_size;

  return true;
}

void RTInvertBucketData::Delete(int vid) {
  long bucket_no_pos = vid_bucket_no_pos_[vid];
  if (bucket_no_pos == -1) return;  // do nothing
  int bucket_no = bucket_no_pos >> 32;
  // only increase bucket's deleted counter
  // int old_bucket_no = bucket_no_pos >> 32;
  // int old_pos = bucket_no_pos & 0xffffffff;
  // idx_array_[old_bucket_no][old_pos] |= kDelIdxMask;
  deleted_nums_[bucket_no]++;
}

void FreeOldBucketPos(std::atomic<long> *old_ptr) { delete[] old_ptr; }

void RTInvertBucketData::ExtendIDs() {
  std::atomic<long> *old_array = vid_bucket_no_pos_;
  std::atomic<long> *new_array = new std::atomic<long>[nids_ * 2];
#pragma omp parallel for
  for (size_t i = nids_; i < nids_ * 2; ++i) new_array[i] = -1;

  memcpy((void *)new_array, (void *)old_array,
         sizeof(std::atomic<long>) * nids_);

  vid_bucket_no_pos_ = new_array;
  nids_ *= 2;
  // delay free
  std::function<void(std::atomic<long> *)> func_free =
      std::bind(&FreeOldBucketPos, std::placeholders::_1);
  utils::AsyncWait(1000, func_free, old_array);
}

RealTimeMemData::RealTimeMemData(size_t buckets_num, VIDMgr *vid_mgr,
                                 bitmap::BitmapManager *docids_bitmap,
                                 size_t bucket_keys, size_t bucket_keys_limit,
                                 size_t code_bytes_per_vec)
    : buckets_num_(buckets_num),
      bucket_keys_(bucket_keys),
      bucket_keys_limit_(bucket_keys_limit),
      code_bytes_per_vec_(code_bytes_per_vec),
      vid_mgr_(vid_mgr),
      docids_bitmap_(docids_bitmap) {
  cur_invert_ptr_ = nullptr;
  extend_invert_ptr_ = nullptr;
  total_mem_bytes_ = 0;
}

RealTimeMemData::~RealTimeMemData() {
  if (cur_invert_ptr_) {
    for (size_t i = 0; i < buckets_num_; i++) {
      if (cur_invert_ptr_->idx_array_)
        CHECK_DELETE_ARRAY(cur_invert_ptr_->idx_array_[i]);
      if (cur_invert_ptr_->codes_array_)
        CHECK_DELETE_ARRAY(cur_invert_ptr_->codes_array_[i]);
    }
    CHECK_DELETE_ARRAY(cur_invert_ptr_->idx_array_);
    CHECK_DELETE_ARRAY(cur_invert_ptr_->retrieve_idx_pos_);
    CHECK_DELETE_ARRAY(cur_invert_ptr_->cur_bucket_keys_);
    CHECK_DELETE_ARRAY(cur_invert_ptr_->bucket_extend_time_);
    CHECK_DELETE_ARRAY(cur_invert_ptr_->codes_array_);
    CHECK_DELETE_ARRAY(cur_invert_ptr_->vid_bucket_no_pos_);
    CHECK_DELETE_ARRAY(cur_invert_ptr_->deleted_nums_);
  }
  CHECK_DELETE(cur_invert_ptr_);
  CHECK_DELETE(extend_invert_ptr_);
}

bool RealTimeMemData::Init() {
  CHECK_DELETE(cur_invert_ptr_);
  cur_invert_ptr_ =
      new (std::nothrow) RTInvertBucketData(vid_mgr_, docids_bitmap_);
  return cur_invert_ptr_ &&
         cur_invert_ptr_->Init(buckets_num_, bucket_keys_, code_bytes_per_vec_,
                               total_mem_bytes_);
}

bool RealTimeMemData::AddKeys(size_t list_no, size_t n, std::vector<long> &keys,
                              std::vector<uint8_t> &keys_codes) {
  if (ExtendBucketIfNeed(list_no, n)) return false;

  if (keys.size() * code_bytes_per_vec_ != keys_codes.size()) {
    LOG(ERROR) << "number of key and key codes not match!";
    return false;
  }

  int retrive_pos = cur_invert_ptr_->retrieve_idx_pos_[list_no];
  // copy new added idx to idx buffer

  if (nullptr == cur_invert_ptr_->idx_array_[list_no]) {
    LOG(ERROR) << "-------idx_array is nullptr!--------";
  }
  memcpy((void *)(cur_invert_ptr_->idx_array_[list_no] + retrive_pos),
         (void *)(keys.data()), sizeof(long) * keys.size());

  // copy new added codes to codes buffer
  memcpy((void *)(cur_invert_ptr_->codes_array_[list_no] +
                  retrive_pos * code_bytes_per_vec_),
         (void *)(keys_codes.data()), sizeof(uint8_t) * keys_codes.size());

  for (size_t i = 0; i < keys.size(); i++) {
    while ((size_t)keys[i] >= cur_invert_ptr_->nids_) {
      cur_invert_ptr_->ExtendIDs();
    }
    cur_invert_ptr_->vid_bucket_no_pos_[keys[i]] = list_no << 32 | retrive_pos;
    retrive_pos++;
    if (cur_invert_ptr_->docids_bitmap_->Test(
            cur_invert_ptr_->vid_mgr_->VID2DocID(keys[i]))) {
      cur_invert_ptr_->Delete(keys[i]);
    }
  }

  // atomic switch retriving pos of list_no
  cur_invert_ptr_->retrieve_idx_pos_[list_no] = retrive_pos;

  return true;
}

int RealTimeMemData::Update(int bucket_no, int vid,
                            std::vector<uint8_t> &codes) {
  if ((size_t)vid >= cur_invert_ptr_->nids_) return 0;
  long bucket_no_pos = cur_invert_ptr_->vid_bucket_no_pos_[vid];
  if (bucket_no_pos == -1) return 0;  // do nothing
  int old_bucket_no = bucket_no_pos >> 32;
  int old_pos = bucket_no_pos & 0xffffffff;
  assert(code_bytes_per_vec_ == codes.size());
  if (old_bucket_no == bucket_no) {
    uint8_t *codes_array = cur_invert_ptr_->codes_array_[old_bucket_no];
    memcpy(codes_array + old_pos * code_bytes_per_vec_, codes.data(),
           codes.size() * sizeof(uint8_t));
    return 0;
  }

  // mark deleted
  cur_invert_ptr_->idx_array_[old_bucket_no][old_pos] |= kDelIdxMask;
  cur_invert_ptr_->deleted_nums_[old_bucket_no]++;
  std::vector<long> keys;
  keys.push_back(vid);

  return AddKeys(bucket_no, 1, keys, codes);
}

int RealTimeMemData::Delete(int *vids, int n) {
  for (int i = 0; i < n; i++) {
    RTInvertBucketData *invert_ptr = cur_invert_ptr_;
    if ((int)invert_ptr->nids_ > vids[i]) invert_ptr->Delete(vids[i]);
  }
  return 0;
}

void RealTimeMemData::FreeOldData(long *idx, uint8_t *codes,
                                  RTInvertBucketData *invert, long size) {
  if (idx) {
    delete[] idx;
    idx = nullptr;
  }
  if (codes) {
    delete[] codes;
    codes = nullptr;
  }
  if (invert) {
    delete invert;
    invert = nullptr;
  }
  total_mem_bytes_ -= size;
}

int RealTimeMemData::CompactIfNeed() {
  long last_compacted_num = cur_invert_ptr_->compacted_num_;
  for (int i = 0; i < (int)buckets_num_; i++) {
    if (Compactable(i)) {
      if (!CompactBucket(i)) {
        LOG(ERROR) << "compact bucket=" << i << " error!";
        return -2;
      }
    }
  }
  if (cur_invert_ptr_->compacted_num_ > last_compacted_num) {
    LOG(INFO) << "Compaction happened, compacted num="
              << cur_invert_ptr_->compacted_num_ - last_compacted_num
              << ", last compacted num=" << last_compacted_num
              << ", current compacted num=" << cur_invert_ptr_->compacted_num_;
  }
  return 0;
}

bool RealTimeMemData::Compactable(int bucket_no) {
  return (float)cur_invert_ptr_->deleted_nums_[bucket_no] /
             cur_invert_ptr_->retrieve_idx_pos_[bucket_no] >=
         0.3f;
}

bool RealTimeMemData::CompactBucket(int bucket_no) {
  return AdjustBucketMem(bucket_no, 1);
}

int RealTimeMemData::ExtendBucketIfNeed(int bucket_no, size_t keys_size) {
  if (cur_invert_ptr_->retrieve_idx_pos_[bucket_no] + (int)keys_size <=
      (size_t)cur_invert_ptr_->cur_bucket_keys_[bucket_no]) {
    return 0;
  } else {  // can not add new keys any more
    if (cur_invert_ptr_->cur_bucket_keys_[bucket_no] * 2 >=
        (int)bucket_keys_limit_) {
      LOG(WARNING) << "exceed the max bucket keys [" << bucket_keys_limit_
                   << "], not extend memory any more!"
                   << " keys_size [" << keys_size << "] "
                   << "bucket_no [" << bucket_no << "]"
                   << " cur_invert_ptr_->cur_bucket_keys_[bucket_no] ["
                   << cur_invert_ptr_->cur_bucket_keys_[bucket_no] << "]";
      return -1;
    } else {
#if 0  // memory limit
        utils::MEM_PACK *p = utils::get_memoccupy();
        if (p->used_rate > 80) {
          LOG(WARNING)
              << "System memory used [" << p->used_rate
              << "]%, cannot add doc, keys_size [" << keys_size << "]"
              << "bucket_no [" << bucket_no << "]"
              << " cur_invert_ptr_->cur_bucket_keys_[bucket_no] ["
              << cur_invert_ptr_->cur_bucket_keys_[bucket_no] << "]";
          free(p);
          return false;
        } else {
          LOG(INFO) << "System memory used [" << p->used_rate << "]%";
          free(p);
        }
#endif
      if (!ExtendBucketMem(bucket_no, (int)keys_size)) {
        return -2;
      }
    }
  }
  return 0;
}

bool RealTimeMemData::ExtendBucketMem(const size_t &bucket_no, int increment) {
  return AdjustBucketMem(bucket_no, 0, increment);
}

bool RealTimeMemData::AdjustBucketMem(const size_t &bucket_no, int type,
                                      int increment) {
  extend_invert_ptr_ = new (std::nothrow) RTInvertBucketData(cur_invert_ptr_);
  if (!extend_invert_ptr_) {
    LOG(ERROR) << "memory extend_invert_ptr_ alloc error!";
    return false;
  }

  long *old_idx_array = cur_invert_ptr_->idx_array_[bucket_no];
  uint8_t *old_codes_array = cur_invert_ptr_->codes_array_[bucket_no];
  int old_keys = cur_invert_ptr_->cur_bucket_keys_[bucket_no];
  long free_size = old_keys * sizeof(long) +
                   old_keys * code_bytes_per_vec_ * sizeof(uint8_t);

  if (type == 0) {  // extend bucket
    // WARNING:
    // the above idx_array_ and codes_array_ pointer would be changed by
    // extendBucketMem()
    if (!extend_invert_ptr_->ExtendBucketMem(
            bucket_no, increment, code_bytes_per_vec_, total_mem_bytes_)) {
      LOG(ERROR) << "extendBucketMem error!";
      return false;
    }
  } else {  // compact bucket
    if (!extend_invert_ptr_->CompactBucket(bucket_no, code_bytes_per_vec_)) {
      LOG(ERROR) << "compact error!";
      return false;
    }
    free_size = 0;
  }

  RTInvertBucketData *old_invert_ptr = cur_invert_ptr_;
  cur_invert_ptr_ = extend_invert_ptr_;

  std::function<void(long *, uint8_t *, RTInvertBucketData *, long)> func_free =
      std::bind(&RealTimeMemData::FreeOldData, this, std::placeholders::_1,
                std::placeholders::_2, std::placeholders::_3,
                std::placeholders::_4);

  utils::AsyncWait(1000, func_free, old_idx_array, old_codes_array,
                   old_invert_ptr, free_size);

  old_idx_array = nullptr;
  old_codes_array = nullptr;
  old_invert_ptr = nullptr;
  extend_invert_ptr_ = nullptr;

  return true;
}

bool RealTimeMemData::GetIvtList(const size_t &bucket_no, long *&ivt_list,
                                 uint8_t *&ivt_codes_list) {
  ivt_list = cur_invert_ptr_->idx_array_[bucket_no];
  ivt_codes_list = (uint8_t *)(cur_invert_ptr_->codes_array_[bucket_no]);

  return true;
}

void RealTimeMemData::PrintBucketSize() {
  std::vector<std::pair<size_t, int>> buckets;

  for (size_t bucket_id = 0; bucket_id < buckets_num_; ++bucket_id) {
    int bucket_size = cur_invert_ptr_->retrieve_idx_pos_[bucket_id];
    buckets.push_back(std::make_pair(bucket_id, bucket_size));
  }

  std::sort(
      buckets.begin(), buckets.end(),
      [](const std::pair<size_t, int> &a, const std::pair<size_t, int> &b) {
        return (a.second > b.second);
      });

  std::stringstream ss;
  ss << "Bucket (id, size): ";
  for (const auto &bucket : buckets) {
    ss << "(" << bucket.first << ", " << bucket.second << ") ";
  }
  LOG(INFO) << ss.str();
}

void RealTimeMemData::RetrieveCodes(int bucket_no, int pos, int n,
                                    uint8_t *codes, long *vids) {
  memcpy((void *)vids, (void *)(cur_invert_ptr_->idx_array_[bucket_no] + pos),
         n * sizeof(long));

  uint8_t *src_codes =
      cur_invert_ptr_->codes_array_[bucket_no] + pos * code_bytes_per_vec_;
  memcpy((void *)codes, (void *)src_codes, n * code_bytes_per_vec_);
}

}  // namespace realtime

}  // namespace tig_gamma
