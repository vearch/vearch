/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "realtime_invert_index.h"
#include "log.h"
#include "utils.h"

namespace tig_gamma {
namespace realtime {

RTInvertIndex::RTInvertIndex(faiss::Index *index, long max_vec_size,
                             size_t bucket_keys, size_t bucket_keys_limit)
    : _bucket_keys(bucket_keys),
      _bucket_keys_limit(bucket_keys_limit),
      _max_vec_size(max_vec_size) {
  _cur_ptr = nullptr;
  if (index) {
    _index_ivf = dynamic_cast<faiss::IndexIVF *>(index);
  } else {
    _index_ivf = nullptr;
  }
}

RTInvertIndex::~RTInvertIndex() {
  if (_cur_ptr) {
    delete _cur_ptr;
    _cur_ptr = nullptr;
  }
}

bool RTInvertIndex::Init() {
  if (nullptr == _index_ivf) return false;
  _cur_ptr = new (std::nothrow) RealTimeMemData(
      _index_ivf->nlist, _max_vec_size, _bucket_keys, _index_ivf->code_size);
  if (nullptr == _cur_ptr) return false;

  if (!_cur_ptr->Init()) return false;
  return true;
}

bool RTInvertIndex::AddKeys(std::map<int, std::vector<long>> &new_keys,
                            std::map<int, std::vector<uint8_t>> &new_codes) {
  // error, not index_ivf
  if (!_index_ivf) return false;

  std::map<int, std::vector<long>>::iterator new_keys_iter = new_keys.begin();

  for (; new_keys_iter != new_keys.end(); new_keys_iter++) {
    int bucket_no = new_keys_iter->first;
    if (new_codes.find(bucket_no) == new_codes.end()) {
      continue;
    }

    if (((new_keys_iter->second).size() * _index_ivf->code_size) !=
        new_codes[bucket_no].size()) {
      LOG(ERROR) << "the pairs of new_keys and new_codes are not suitable!";
      continue;
    }

    size_t keys_size = new_keys[bucket_no].size();
    if (_cur_ptr->_cur_invert_ptr->_retrieve_idx_pos[bucket_no] +
            (int)keys_size <=
        _cur_ptr->_cur_invert_ptr->_cur_bucket_keys[bucket_no]) {
      std::vector<long> &new_keys_vec = new_keys[bucket_no];
      std::vector<uint8_t> &new_codes_vec = new_codes[bucket_no];
      _cur_ptr->AddKeys((size_t)bucket_no, (size_t)keys_size, new_keys_vec,
                        new_codes_vec);
    } else {  // can not add new keys any more
      if (_cur_ptr->_cur_invert_ptr->_cur_bucket_keys[bucket_no] * 2 >=
          (int)_bucket_keys_limit) {
        LOG(WARNING)
            << "exceed the max bucket keys [" << _bucket_keys_limit
            << "], not extend memory any more!"
            << " keys_size [" << keys_size << "] "
            << "bucket_no [" << bucket_no << "]"
            << " _cur_ptr->_cur_invert_ptr->_cur_bucket_keys[bucket_no] ["
            << _cur_ptr->_cur_invert_ptr->_cur_bucket_keys[bucket_no] << "]";
        return false;
      } else {
#if 0  // memory limit
        utils::MEM_PACK *p = utils::get_memoccupy();
        if (p->used_rate > 80) {
          LOG(WARNING)
              << "System memory used [" << p->used_rate
              << "]%, cannot add doc, keys_size [" << keys_size << "]"
              << "bucket_no [" << bucket_no << "]"
              << " _cur_ptr->_cur_invert_ptr->_cur_bucket_keys[bucket_no] ["
              << _cur_ptr->_cur_invert_ptr->_cur_bucket_keys[bucket_no] << "]";
          free(p);
          return false;
        } else {
          LOG(INFO) << "System memory used [" << p->used_rate << "]%";
          free(p);
        }
#endif
        if (!_cur_ptr->ExtendBucketMem(bucket_no)) {
          return false;
        }

        _cur_ptr->AddKeys((size_t)bucket_no, (size_t)keys_size,
                          new_keys[bucket_no], new_codes[bucket_no]);
      }
    }
  }

  return true;
}

bool RTInvertIndex::GetIvtList(const size_t &bucket_no, long *&ivt_list,
                               size_t &ivt_size, uint8_t *&ivt_codes_list) {
  ivt_size = _cur_ptr->_cur_invert_ptr->_retrieve_idx_pos[bucket_no];
  return _cur_ptr->GetIvtList(bucket_no, ivt_list, ivt_codes_list);
}

int RTInvertIndex::RetrieveCodes(
    int *vids, size_t vid_size,
    std::vector<std::vector<const uint8_t *>> &bucket_codes,
    std::vector<std::vector<long>> &bucket_vids) {
  return _cur_ptr->RetrieveCodes(vids, vid_size, bucket_codes, bucket_vids);
}

int RTInvertIndex::RetrieveCodes(
    int **vids_list, size_t vids_list_size,
    std::vector<std::vector<const uint8_t *>> &bucket_codes,
    std::vector<std::vector<long>> &bucket_vids) {
  return _cur_ptr->RetrieveCodes(vids_list, vids_list_size, bucket_codes,
                                 bucket_vids);
}

int RTInvertIndex::Dump(const std::string &dir, const std::string &vec_name,
                        int max_vid) {
  return _cur_ptr->Dump(dir, vec_name, max_vid);
}

int RTInvertIndex::Load(const std::vector<std::string> &index_dirs,
                        const std::string &vec_name) {
  return _cur_ptr->Load(index_dirs, vec_name);
}

}  // namespace realtime
}  // namespace tig_gamma
