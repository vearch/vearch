/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>

#include "raw_vector.h"

namespace vearch {

class MemoryRawVector : public RawVector {
 public:
  MemoryRawVector(VectorMetaInfo *meta_info, const StoreParams &store_params,
                  bitmap::BitmapManager *docids_bitmap,
                  StorageManager *storage_mgr, int cf_id);

  ~MemoryRawVector();

  int InitStore(std::string &vec_name) override;

  int AddToStore(uint8_t *v, int len) override;

  int DeleteFromStore(int64_t vid) override;

  int GetVectorHeader(int64_t start, int n, ScopeVectors &vecs,
                      std::vector<int> &lens) override;

  int UpdateToStore(int64_t vid, uint8_t *v, int len) override;

  uint8_t *GetFromMem(int64_t vid) const;

  int AddToMem(int64_t vid, uint8_t *v, int len);

  Status Load(int64_t vec_num) override;

  int GetDiskVecNum(int64_t &vec_num) override;

  Status Dump(int64_t start_vid, int64_t end_vid) override {
    return Status::OK();
  };

  Status Compact() override;

  bool Compactable(int segment_no);

 protected:
  int GetVector(int64_t vid, const uint8_t *&vec,
                bool &deleteable) const override;

 private:
  int ExtendSegments();

  uint8_t **segments_;
  int nsegments_;
  int segment_size_;
  int curr_idx_in_seg_;
  float compact_ratio_;
  std::atomic<uint32_t> *segment_deleted_nums_;
  std::atomic<uint32_t> *segment_nums_;
};

}  // namespace vearch
