/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "mmap_raw_vector.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <exception>

#include "search/error_code.h"
#include "util/log.h"
#include "util/utils.h"

namespace tig_gamma {

MmapRawVector::MmapRawVector(VectorMetaInfo *meta_info,
                             const std::string &root_path,
                             const StoreParams &store_params,
                             bitmap::BitmapManager *docids_bitmap)
    : RawVector(meta_info, root_path, docids_bitmap, store_params) {
  allow_use_zfp = false;    // Storage_mgr is compressed internally using ZFP.
  vector_byte_size_ = meta_info_->DataSize() * meta_info->Dimension();
  storage_mgr_ = nullptr;
}

MmapRawVector::~MmapRawVector() { CHECK_DELETE(storage_mgr_); }

int MmapRawVector::InitStore(std::string &vec_name) {
  std::string vec_dir = root_path_ + "/" + meta_info_->Name();
  int var = std::numeric_limits<int>::max();
  int max_seg_size = var / vector_byte_size_;
  int seg_block_capacity = 2000000;
  if (max_seg_size < store_params_.segment_size) {
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
#ifdef WITH_ZFP
  if (!store_params_.compress.IsEmpty()) {
    if (meta_info_->DataType() != VectorValueType::FLOAT) {
      LOG(ERROR) << "data type is not float, compress is unsupported";
      return PARAM_ERR;
    }
    if (storage_mgr_->UseCompress(CompressType::Zfp,
          meta_info_->Dimension()) == 0) {
      LOG(INFO) << "Storage_manager use zfp compress vector";
    } else {
      LOG(INFO) << "ZFP initialization failed, not use zfp";
    }
  }
#endif
  int ret = storage_mgr_->Init(vec_name, store_params_.cache_size);
  if (ret) {
    LOG(ERROR) << "init gamma db error, ret=" << ret;
    return ret;
  }

  LOG(INFO) << "init mmap raw vector success! vector byte size="
            << vector_byte_size_ << ", path=" << vec_dir;
  return 0;
}

int MmapRawVector::GetVectorHeader(int start, int n, ScopeVectors &vecs,
                                   std::vector<int> &lens) {
  int ret = storage_mgr_->GetHeaders(start, n, vecs.ptr_, lens);
  vecs.deletable_.resize(vecs.ptr_.size(), true);
  return ret;
}

int MmapRawVector::AddToStore(uint8_t *v, int len) {
  return storage_mgr_->Add(v, len);
}

int MmapRawVector::UpdateToStore(int vid, uint8_t *v, int len) {
  return storage_mgr_->Update(vid, v, len);
}

int MmapRawVector::AlterCacheSize(int cache_size) {
  if (storage_mgr_ == nullptr) return -1;
  storage_mgr_->AlterCacheSize(cache_size, 0);
  return 0;
}

int MmapRawVector::GetCacheSize(int &cache_size) {
  if (storage_mgr_ == nullptr) return -1;
  int str_cache_size = 0;
  storage_mgr_->GetCacheSize(cache_size, str_cache_size);
  return 0;
}

int MmapRawVector::GetVector(long vid, const uint8_t *&vec,
                             bool &deletable) const {
  deletable = true;
  return storage_mgr_->Get(vid, vec);
}

}  // namespace tig_gamma

