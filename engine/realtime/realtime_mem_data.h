/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <stdint.h>
#include <stdlib.h>

#include <atomic>
#include <string>
#include <vector>

#include "util/bitmap_manager.h"
#include "vector/raw_vector_common.h"

namespace tig_gamma {

namespace realtime {

#define PI 3.14159265

const static long kDelIdxMask = (long)1 << 63;     // 0x8000000000000000
const static long kRecoverIdxMask = ~kDelIdxMask;  // 0x7fffffffffffffff

struct RTInvertBucketData {
  RTInvertBucketData(RTInvertBucketData *other);
  RTInvertBucketData(VIDMgr *vid_mgr, bitmap::BitmapManager *docids_bitmap);

  bool Init(const size_t &buckets_num, const size_t &bucket_keys,
            const size_t &code_bytes_per_vec,
            std::atomic<long> &total_mem_bytes);
  ~RTInvertBucketData();

  bool ExtendBucketMem(const size_t &bucket_no, const int &increment,
                       const size_t &code_bytes_per_vec,
                       std::atomic<long> &total_mem_bytes);

  bool CompactBucket(const size_t &bucket_no, const size_t &code_bytes_per_vec);

  void Delete(int vid);
  void ExtendIDs();

 private:
  inline void CompactOne(const size_t &bucket_no, long *&dst_idx,
                         uint8_t *&dst_code, long *&src_idx, uint8_t *&src_code,
                         int &pos, const size_t &code_bytes_per_vec);

  double ExtendCoefficient(uint8_t extend_time);

 public:
  long **idx_array_;
  size_t *retrieve_idx_pos_;  // total nb of realtime added indexed vectors
  int *cur_bucket_keys_;
  uint8_t *bucket_extend_time_;
  uint8_t **codes_array_;
  // int *dump_latest_pos_;
  VIDMgr *vid_mgr_;
  bitmap::BitmapManager *docids_bitmap_;
  std::atomic<long> *vid_bucket_no_pos_;
  std::atomic<int> *deleted_nums_;
  long compacted_num_;
  size_t buckets_num_;
  size_t nids_;
};

struct RealTimeMemData {
 public:
  RealTimeMemData(size_t buckets_num, VIDMgr *vid_mgr,
                  bitmap::BitmapManager *docids_bitmap,
                  size_t bucket_keys = 500, size_t bucket_keys_limit = 1000000,
                  size_t code_bytes_per_vec = 512 * sizeof(float));
  ~RealTimeMemData();

  bool Init();

  bool AddKeys(size_t list_no, size_t n, std::vector<long> &keys,
               std::vector<uint8_t> &keys_codes);

  int Update(int bucket_no, int vid, std::vector<uint8_t> &codes);

  void FreeOldData(long *idx, uint8_t *codes, RTInvertBucketData *invert,
                   long size);
  int ExtendBucketIfNeed(int bucket_no, size_t keys_size);
  bool ExtendBucketMem(const size_t &bucket_no, int increment);
  bool AdjustBucketMem(const size_t &bucket_no, int type, int increment = 0);
  bool GetIvtList(const size_t &bucket_no, long *&ivt_list,
                  uint8_t *&ivt_codes_list);

  long GetTotalMemBytes() { return total_mem_bytes_; }

  // for unit test
  void RetrieveCodes(int bucket_no, int pos, int n, uint8_t *codes, long *vids);

  void PrintBucketSize();

  int CompactIfNeed();
  bool Compactable(int bucket_no);
  bool CompactBucket(int bucket_no);
  int Delete(int *vids, int n);

  RTInvertBucketData *cur_invert_ptr_;
  RTInvertBucketData *extend_invert_ptr_;

  size_t buckets_num_;  // count of buckets
  size_t bucket_keys_;  // max bucket keys
  size_t bucket_keys_limit_;

  size_t code_bytes_per_vec_;
  std::atomic<long> total_mem_bytes_;

  VIDMgr *vid_mgr_;
  bitmap::BitmapManager *docids_bitmap_;
};

}  // namespace realtime

}  // namespace tig_gamma
