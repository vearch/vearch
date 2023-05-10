/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "raw_vector.h"

#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "util/log.h"
#include "util/utils.h"

using namespace std;

namespace tig_gamma {

RawVector::RawVector(VectorMetaInfo *meta_info, const string &root_path,
                     bitmap::BitmapManager *docids_bitmap,
                     const StoreParams &store_params)
    : VectorReader(meta_info),
      root_path_(root_path),
      total_mem_bytes_(0),
      store_params_(store_params),
      docids_bitmap_(docids_bitmap) {
  data_size_ = meta_info_->DataSize();
  vio_ = nullptr;
  str_mem_ptr_ = nullptr;
  vid_mgr_ = nullptr;
#ifdef WITH_ZFP
  zfp_compressor_ = nullptr;
#endif
  allow_use_zfp = true;
}

RawVector::~RawVector() {
  CHECK_DELETE_ARRAY(str_mem_ptr_);
  CHECK_DELETE(vid_mgr_);
#ifdef WITH_ZFP
  CHECK_DELETE(zfp_compressor_);
#endif
}

int RawVector::Init(std::string vec_name, bool has_source, bool multi_vids) {
  desc_ += "raw vector=" + meta_info_->Name() + ", ";
  if (has_source || multi_vids) {
    LOG(ERROR) << "source and multi-vids is unsupported now";
    return -1;
  }
  // source
  str_mem_ptr_ = nullptr;
  if (has_source) {
    uint64_t len = (uint64_t)kInitSize * 100;
    str_mem_ptr_ = new (std::nothrow) char[len];
    source_mem_pos_.resize(kInitSize + 1, 0);
    total_mem_bytes_ += len + kInitSize * sizeof(long);
  }
  has_source_ = has_source;

  // vid2docid
  vid_mgr_ = new VIDMgr(multi_vids);
  vid_mgr_->Init(kInitSize, total_mem_bytes_);

  vector_byte_size_ = meta_info_->Dimension() * data_size_;

#ifdef WITH_ZFP
  if (!store_params_.compress.IsEmpty() && allow_use_zfp) {
    if (meta_info_->DataType() != VectorValueType::FLOAT) {
      LOG(ERROR) << "data type is not float, compress is unsupported";
      return PARAM_ERR;
    }
    zfp_compressor_ = new ZFPCompressor;
    int ret =
        zfp_compressor_->Init(meta_info_->Dimension(), store_params_.compress);
    if (ret) return ret;
    vector_byte_size_ = zfp_compressor_->ZfpSize();
  }
#endif

  if (InitStore(vec_name)) return -2;

  LOG(INFO) << "raw vector init success! name=" << meta_info_->Name()
            << ", has source=" << has_source << ", multi_vids=" << multi_vids
            << ", vector_byte_size=" << vector_byte_size_
            << ", dimension=" << meta_info_->Dimension()
            << ", compress=" << store_params_.compress.ToStr();
  return 0;
}

int RawVector::GetVector(long vid, ScopeVector &vec) const {
  return GetVector(vid, vec.ptr_, vec.deletable_);
}

int RawVector::Gets(const std::vector<int64_t> &vids,
                    ScopeVectors &vecs) const {
  bool deletable;
  for (size_t i = 0; i < vids.size(); i++) {
    const uint8_t *vec = nullptr;
    deletable = false;
    GetVector(vids[i], vec, deletable);
    vecs.Add(vec, deletable);
  }
  return 0;
}

int RawVector::GetSource(int vid, char *&str, int &len) {
  if (vid < 0 || vid >= (int)meta_info_->Size()) return -1;
  if (!has_source_) {
    str = nullptr;
    len = 0;
    return 0;
  }
  len = source_mem_pos_[vid + 1] - source_mem_pos_[vid];
  str = str_mem_ptr_ + source_mem_pos_[vid];
  return 0;
}

int RawVector::Add(int docid, struct Field &field) {
  if (field.value.size() != (size_t)data_size_ * meta_info_->Dimension()) {
    LOG(ERROR) << "Doc [" << docid << "] len [" << field.value.size() << "]";
    return -1;
  }
  int ret = AddToStore((uint8_t *)field.value.c_str(), field.value.size());
  if (ret) {
    LOG(ERROR) << "add to store error, docid=" << docid << ", ret=" << ret;
    return -2;
  }

  // add to source
  if (has_source_) {
    size_t size = meta_info_->Size();
    int len = field.source.size();
    if (len > 0) {
      memcpy(str_mem_ptr_ + source_mem_pos_[size], field.source.c_str(),
             len * sizeof(char));
      source_mem_pos_[size + 1] = source_mem_pos_[size] + len;
    } else {
      source_mem_pos_[size + 1] = source_mem_pos_[size];
    }
  }
  return vid_mgr_->Add(meta_info_->size_++, docid);
}

int RawVector::Add(int docid, float *data) {
  int ret = AddToStore((uint8_t *)data, data_size_ * meta_info_->Dimension());
  if (ret) {
    LOG(ERROR) << "add to store error, docid=" << docid << ", ret=" << ret;
    return -2;
  }
  return vid_mgr_->Add(meta_info_->size_++, docid);
}

int RawVector::Update(int docid, struct Field &field) {
  if (vid_mgr_->MultiVids() || docid >= (int)meta_info_->Size()) {
    return -1;
  }

  int vid = docid;

  if (field.value.size() / data_size_ <= 0) {
    LOG(ERROR) << "Doc [" << docid << "] len " << field.value.size() << "]";
    return -1;
  }

  if (UpdateToStore(vid, (uint8_t *)field.value.c_str(), field.value.size())) {
    LOG(ERROR) << "update to store error, docid=" << docid;
    return -1;
  }

  // TODO: update source
  return 0;
}

int RawVector::Compress(uint8_t *v, ScopeVector &svec) {
#ifdef WITH_ZFP
  if (zfp_compressor_) {
    uint8_t *cmprs_v = nullptr;
    if (zfp_compressor_->Compress((float *)v, cmprs_v)) {
      return INTERNAL_ERR;
    }
    svec.Set(cmprs_v, true);
  } else
#endif
  {
    svec.Set(v, false);
  }
  return 0;
}

int RawVector::Decompress(uint8_t *cmprs_v, int n, uint8_t *&vec,
                          bool &deletable) const {
#ifdef WITH_ZFP
  if (zfp_compressor_) {
    float *v = nullptr;
    if (zfp_compressor_->Decompress(cmprs_v, n, v)) {
      return INTERNAL_ERR;
    }
    vec = (uint8_t *)v;
    deletable = true;
  } else
#endif
  {
    vec = cmprs_v;
    deletable = false;
  }
  return 0;
}

DumpConfig *RawVector::GetDumpConfig() {
  return dynamic_cast<DumpConfig *>(&store_params_);
}

int StoreParams::Parse(const char *str) {
  utils::JsonParser jp;
  if (jp.Parse(str)) {
    LOG(ERROR) << "parse store parameters error: " << str;
    return -1;
  }
  return Parse(jp);
}

int StoreParams::Parse(utils::JsonParser &jp) {
  double cache_size = 0;
  if (!jp.GetDouble("cache_size", cache_size)) {
    if (cache_size > MAX_CACHE_SIZE || cache_size < 0) {
      LOG(ERROR) << "invalid cache size=" << cache_size << "M"
                 << ", limit size=" << MAX_CACHE_SIZE << "M";
      return -1;
    }
    this->cache_size = cache_size;
  }

  if (!jp.GetInt("segment_size", segment_size)) {
    if (segment_size <= 0) {
      LOG(ERROR) << "invalid segment size=" << segment_size;
      return -1;
    }
  }

  if (jp.Contains("compress") && jp.GetObject("compress", compress)) {
    LOG(ERROR) << "parse compress error";
    return -1;
  }

  return 0;
}

int StoreParams::MergeRight(StoreParams &other) {
  cache_size = other.cache_size;
  segment_size = other.segment_size;
  // compress.MergeRight(other.compress);
  return 0;
}

}  // namespace tig_gamma
