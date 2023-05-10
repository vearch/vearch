/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "realtime/realtime_invert_index.h"
#include "util/log.h"
#include "util/utils.h"

namespace tig_gamma {
namespace realtime {

RTInvertIndex::RTInvertIndex(size_t nlist, size_t code_size, VIDMgr *vid_mgr,
                             bitmap::BitmapManager *docids_bitmap,
                             size_t bucket_keys, size_t bucket_keys_limit)
    : nlist_(nlist),
      code_size_(code_size),
      bucket_keys_(bucket_keys),
      bucket_keys_limit_(bucket_keys_limit),
      vid_mgr_(vid_mgr),
      docids_bitmap_(docids_bitmap) {
  cur_ptr_ = nullptr;
}

RTInvertIndex::~RTInvertIndex() {
  if (cur_ptr_) {
    delete cur_ptr_;
    cur_ptr_ = nullptr;
  }
}

bool RTInvertIndex::Init() {
  CHECK_DELETE(cur_ptr_);
  cur_ptr_ = new (std::nothrow)
      RealTimeMemData(nlist_, vid_mgr_, docids_bitmap_, bucket_keys_,
                      bucket_keys_limit_, code_size_);
  if (nullptr == cur_ptr_) return false;

  if (!cur_ptr_->Init()) return false;
  return true;
}

bool RTInvertIndex::AddKeys(std::map<int, std::vector<long>> &new_keys,
                            std::map<int, std::vector<uint8_t>> &new_codes) {
  std::map<int, std::vector<long>>::iterator new_keys_iter = new_keys.begin();

  for (; new_keys_iter != new_keys.end(); new_keys_iter++) {
    int bucket_no = new_keys_iter->first;
    if (new_codes.find(bucket_no) == new_codes.end()) {
      continue;
    }

    if (((new_keys_iter->second).size() * code_size_) !=
        new_codes[bucket_no].size()) {
      LOG(ERROR) << "the pairs of new_keys and new_codes are not suitable!";
      continue;
    }

    size_t keys_size = new_keys[bucket_no].size();
    if (!cur_ptr_->AddKeys((size_t)bucket_no, (size_t)keys_size,
                           new_keys[bucket_no], new_codes[bucket_no])) {
      LOG(ERROR) << "add keys error, bucket no=" << bucket_no
                 << ", key size=" << keys_size;
      return false;
    }
  }

  return true;
}

int RTInvertIndex::Update(int bucket_no, int vid, std::vector<uint8_t> &codes) {
  return cur_ptr_->Update(bucket_no, vid, codes);
}

bool RTInvertIndex::GetIvtList(const size_t &bucket_no, long *&ivt_list,
                               size_t &ivt_size, uint8_t *&ivt_codes_list) {
  ivt_size = cur_ptr_->cur_invert_ptr_->retrieve_idx_pos_[bucket_no];
  return cur_ptr_->GetIvtList(bucket_no, ivt_list, ivt_codes_list);
}

void RTInvertIndex::PrintBucketSize() { cur_ptr_->PrintBucketSize(); }

int RTInvertIndex::CompactIfNeed() { return cur_ptr_->CompactIfNeed(); }

int RTInvertIndex::Delete(int *vids, int n) {
  return cur_ptr_->Delete(vids, n);
}

RTInvertedLists::RTInvertedLists(realtime::RTInvertIndex *rt_invert_index_ptr,
                                 size_t nlist, size_t code_size)
    : InvertedLists(nlist, code_size),
      rt_invert_index_ptr_(rt_invert_index_ptr) {}

size_t RTInvertedLists::list_size(size_t list_no) const {
  if (!rt_invert_index_ptr_) return 0;
  long *ivt_list = nullptr;
  size_t list_size = 0;
  uint8_t *ivt_codes_list = nullptr;
  bool ret = rt_invert_index_ptr_->GetIvtList(list_no, ivt_list, list_size,
                                              ivt_codes_list);
  if (!ret) return 0;
  return list_size;
}

const uint8_t *RTInvertedLists::get_codes(size_t list_no) const {
  if (!rt_invert_index_ptr_) return nullptr;
  long *ivt_list = nullptr;
  size_t list_size = 0;
  uint8_t *ivt_codes_list = nullptr;
  bool ret = rt_invert_index_ptr_->GetIvtList(list_no, ivt_list, list_size,
                                              ivt_codes_list);
  if (!ret) return nullptr;
  return ivt_codes_list;
}

const idx_t *RTInvertedLists::get_ids(size_t list_no) const {
  if (!rt_invert_index_ptr_) return nullptr;
  long *ivt_list = nullptr;
  size_t list_size = 0;
  uint8_t *ivt_codes_list = nullptr;
  bool ret = rt_invert_index_ptr_->GetIvtList(list_no, ivt_list, list_size,
                                              ivt_codes_list);
  if (!ret) return nullptr;
  idx_t *ivt_lists = reinterpret_cast<idx_t *>(ivt_list);
  return ivt_lists;
}

size_t RTInvertedLists::add_entries(size_t list_no, size_t n_entry,
                                    const idx_t *ids, const uint8_t *code) {
  return 0;
}

void RTInvertedLists::resize(size_t list_no, size_t new_size) {}

void RTInvertedLists::update_entries(size_t list_no, size_t offset,
                                     size_t n_entry, const idx_t *ids_in,
                                     const uint8_t *codes_in) {}

}  // namespace realtime
}  // namespace tig_gamma
