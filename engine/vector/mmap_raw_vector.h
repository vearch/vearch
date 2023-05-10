/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>

#include "storage/storage_manager.h"
#include "vector/raw_vector.h"

namespace tig_gamma {

class MmapRawVectorIO;

class MmapRawVector : public RawVector {
 public:
  MmapRawVector(VectorMetaInfo *meta_info, const std::string &root_path,
                const StoreParams &store_params,
                bitmap::BitmapManager *docids_bitmap);
  ~MmapRawVector();
  int InitStore(std::string &vec_name) override;
  int AddToStore(uint8_t *v, int len) override;
  int GetVectorHeader(int start, int n, ScopeVectors &vecs,
                      std::vector<int> &lens) override;
  int UpdateToStore(int vid, uint8_t *v, int len) override;

  int AlterCacheSize(int cache_size) override;

  int GetCacheSize(int &cache_size) override;

 protected:
  int GetVector(long vid, const uint8_t *&vec, bool &deletable) const override;

 private:
  int Extend();
  std::string NextSegmentFilePath();

 private:
  friend MmapRawVectorIO;
};

}  // namespace tig_gamma
