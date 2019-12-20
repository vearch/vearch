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
#include "log.h"
#include "utils.h"

namespace tig_gamma {
namespace realtime {

RTInvertBucketData::RTInvertBucketData(long **idx_array, int *retrieve_idx_pos,
                                       int *cur_bucket_keys,
                                       uint8_t **codes_array,
                                       int *dump_latest_pos)
    : _idx_array(idx_array),
      _retrieve_idx_pos(retrieve_idx_pos),
      _cur_bucket_keys(cur_bucket_keys),
      _codes_array(codes_array),
      _dump_latest_pos(dump_latest_pos) {}

RTInvertBucketData::RTInvertBucketData() {
  _idx_array = nullptr;
  _retrieve_idx_pos = nullptr;
  _cur_bucket_keys = nullptr;
  _codes_array = nullptr;
  _dump_latest_pos = nullptr;
}

RTInvertBucketData::~RTInvertBucketData() {}

bool RTInvertBucketData::Init(const size_t &buckets_num,
                              const size_t &bucket_keys,
                              const size_t &code_bytes_per_vec,
                              long &total_mem_bytes) {
  _idx_array = new (std::nothrow) long *[buckets_num];
  _codes_array = new (std::nothrow) uint8_t *[buckets_num];
  _cur_bucket_keys = new (std::nothrow) int[buckets_num];
  if (_idx_array == nullptr || _codes_array == nullptr) return false;
  for (size_t i = 0; i < buckets_num; i++) {
    _idx_array[i] = new (std::nothrow) long[bucket_keys];
    _codes_array[i] =
        new (std::nothrow) uint8_t[bucket_keys * code_bytes_per_vec];
    if (_idx_array[i] == nullptr || _codes_array[i] == nullptr) return false;
    _cur_bucket_keys[i] = bucket_keys;
  }

  total_mem_bytes += buckets_num * bucket_keys * sizeof(long);
  total_mem_bytes +=
      buckets_num * bucket_keys * code_bytes_per_vec * sizeof(uint8_t);
  total_mem_bytes += buckets_num * sizeof(int);

  _retrieve_idx_pos = new (std::nothrow) int[buckets_num];
  if (_retrieve_idx_pos == nullptr) return false;
  memset(_retrieve_idx_pos, 0, buckets_num * sizeof(int));
  _dump_latest_pos = new (std::nothrow) int[buckets_num];
  if (_dump_latest_pos == nullptr) return false;
  memset(_dump_latest_pos, 0, buckets_num * sizeof(int));

  total_mem_bytes += buckets_num * sizeof(int) * 2;
  LOG(INFO) << "===init total_mem_bytes is " << total_mem_bytes << "===";
  return true;
}

bool RTInvertBucketData::ExtendBucketMem(const size_t &bucket_no,
                                         const size_t &code_bytes_per_vec,
                                         long &total_mem_bytes) {
  int extend_size = _cur_bucket_keys[bucket_no] * 2;

  uint8_t *extend_code_bytes_array =
      new (std::nothrow) uint8_t[extend_size * code_bytes_per_vec];
  if (extend_code_bytes_array == nullptr) {
    LOG(ERROR) << "memory extend_code_bytes_array alloc error!";
    return false;
  }
  memcpy((void *)extend_code_bytes_array, (void *)_codes_array[bucket_no],
         sizeof(uint8_t) * _cur_bucket_keys[bucket_no] * code_bytes_per_vec);
  _codes_array[bucket_no] = extend_code_bytes_array;
  total_mem_bytes += extend_size * code_bytes_per_vec * sizeof(uint8_t);

  long *extend_idx_array = new (std::nothrow) long[extend_size];
  if (extend_idx_array == nullptr) {
    LOG(ERROR) << "memory extend_idx_array alloc error!";
    return false;
  }
  memcpy((void *)extend_idx_array, (void *)_idx_array[bucket_no],
         sizeof(long) * _cur_bucket_keys[bucket_no]);
  _idx_array[bucket_no] = extend_idx_array;
  total_mem_bytes += extend_size * sizeof(long);

  _cur_bucket_keys[bucket_no] = extend_size;
  return true;
}

bool RTInvertBucketData::ReleaseBucketMem(const size_t &bucket_no,
                                          const size_t &code_bytes_per_vec,
                                          long &total_mem_bytes) {
  if (_idx_array[bucket_no]) {
    delete[] _idx_array[bucket_no];
    _idx_array[bucket_no] = nullptr;
    total_mem_bytes -= _cur_bucket_keys[bucket_no] * sizeof(long);
  }
  if (_codes_array[bucket_no]) {
    delete[] _codes_array[bucket_no];
    _codes_array[bucket_no] = nullptr;
    total_mem_bytes -=
        _cur_bucket_keys[bucket_no] * code_bytes_per_vec * sizeof(uint8_t);
  }
  return true;
}

bool RTInvertBucketData::GetBucketMemInfo(const size_t &bucket_no,
                                          std::string &mem_info) {
  return false;
}

int RTInvertBucketData::GetCurDumpPos(const size_t &bucket_no, int max_vid,
                                      int &dump_start_pos, int &size) {
  int start_pos = _dump_latest_pos[bucket_no];
  int end_pos = _retrieve_idx_pos[bucket_no];
  if (end_pos == 0) {
    LOG(ERROR) << "bucket no=" << bucket_no << "has no data to dump";
    return -1;
  }
  if (start_pos > end_pos) {
    LOG(ERROR) << "the latest dumping pos exceed the max retrieval pos";
    return -1;
  }
  while ((long)max_vid < _idx_array[bucket_no][--end_pos])
    ;
  if (start_pos > end_pos) {
    return -2;
  }
  dump_start_pos = start_pos;
  size = end_pos - start_pos + 1;
  _dump_latest_pos[bucket_no] += size;
  return 0;
}

RealTimeMemData::RealTimeMemData(size_t buckets_num, long max_vec_size,
                                 size_t bucket_keys, size_t code_bytes_per_vec)
    : _buckets_num(buckets_num),
      _bucket_keys(bucket_keys),
      _code_bytes_per_vec(code_bytes_per_vec),
      _max_vec_size(max_vec_size) {
  _cur_invert_ptr = new (std::nothrow) RTInvertBucketData();
  _extend_invert_ptr = nullptr;
  _total_mem_bytes = 0;
}

RealTimeMemData::~RealTimeMemData() {
  if (_cur_invert_ptr) {
    for (size_t i = 0; i < _buckets_num; i++) {
      if (_cur_invert_ptr->_idx_array)
        CHECK_DELETE_ARRAY(_cur_invert_ptr->_idx_array[i]);
      if (_cur_invert_ptr->_codes_array)
        CHECK_DELETE_ARRAY(_cur_invert_ptr->_codes_array[i]);
    }
    CHECK_DELETE_ARRAY(_cur_invert_ptr->_idx_array);
    CHECK_DELETE_ARRAY(_cur_invert_ptr->_retrieve_idx_pos);
    CHECK_DELETE_ARRAY(_cur_invert_ptr->_cur_bucket_keys);
    CHECK_DELETE_ARRAY(_cur_invert_ptr->_codes_array);
    CHECK_DELETE_ARRAY(_cur_invert_ptr->_dump_latest_pos);
    delete _cur_invert_ptr;
    _cur_invert_ptr = nullptr;
  }
  if (_extend_invert_ptr) {
    delete _extend_invert_ptr;
    _extend_invert_ptr = nullptr;
  }
}

bool RealTimeMemData::Init() {
  // fprintf(stderr, "%u\n", _total_keys);
  // fprintf(stderr, "%u\n", _code_bytes_per_vec);
  // fprintf(stderr, "%u\n", _buckets_num);

  _vid_bucket_no_pos.resize(_max_vec_size, -1);

  return _cur_invert_ptr &&
         _cur_invert_ptr->Init(_buckets_num, _bucket_keys, _code_bytes_per_vec,
                               _total_mem_bytes);
}

bool RealTimeMemData::AddKeys(size_t list_no, size_t n, std::vector<long> &keys,
                              std::vector<uint8_t> &keys_codes) {
  if (keys.size() * _code_bytes_per_vec != keys_codes.size()) {
    LOG(ERROR) << "number of key and key codes not match!";
    return false;
  }
  int retrive_pos = _cur_invert_ptr->_retrieve_idx_pos[list_no];
  // copy new added idx to idx buffer

  if (nullptr == _cur_invert_ptr->_idx_array[list_no]) {
    LOG(ERROR) << "-------idx_array is nullptr!--------";
  }
  memcpy((void *)(_cur_invert_ptr->_idx_array[list_no] + retrive_pos),
         (void *)(keys.data()), sizeof(long) * keys.size());

  // copy new added codes to codes buffer
  memcpy((void *)(_cur_invert_ptr->_codes_array[list_no] +
                  retrive_pos * _code_bytes_per_vec),
         (void *)(keys_codes.data()), sizeof(uint8_t) * keys_codes.size());

  for (size_t i = 0; i < keys.size(); i++) {
    if (keys[i] >= _max_vec_size) {
      return false;
    }
    _vid_bucket_no_pos[keys[i]] = list_no << 32 | retrive_pos;
    retrive_pos++;
  }

  // atomic switch retriving pos of list_no
  _cur_invert_ptr->_retrieve_idx_pos[list_no] = retrive_pos;
  return true;
}

bool RealTimeMemData::ExtendBucketMem(const size_t &bucket_no) {
  _extend_invert_ptr = new (std::nothrow) RTInvertBucketData(
      _cur_invert_ptr->_idx_array, _cur_invert_ptr->_retrieve_idx_pos,
      _cur_invert_ptr->_cur_bucket_keys, _cur_invert_ptr->_codes_array,
      _cur_invert_ptr->_dump_latest_pos);
  if (!_extend_invert_ptr) {
    LOG(ERROR) << "memory _extend_invert_ptr alloc error!";
    return false;
  }

  long *old_idx_array = _cur_invert_ptr->_idx_array[bucket_no];
  uint8_t *old_codes_array = _cur_invert_ptr->_codes_array[bucket_no];
  int old_keys = _cur_invert_ptr->_cur_bucket_keys[bucket_no];

  // WARNING:
  // the above _idx_array and _codes_array pointer would be changed by
  // extendBucketMem()
  if (!_extend_invert_ptr->ExtendBucketMem(bucket_no, _code_bytes_per_vec,
                                           _total_mem_bytes)) {
    LOG(ERROR) << "extendBucketMem error!";
    return false;
  }

  RTInvertBucketData *old_invert_ptr = _cur_invert_ptr;
  _cur_invert_ptr = _extend_invert_ptr;

  sleep(1);

  if (old_idx_array) {
    delete old_idx_array;
    old_idx_array = nullptr;
    _total_mem_bytes -= old_keys * sizeof(long);
  }

  if (old_codes_array) {
    delete old_codes_array;
    old_codes_array = nullptr;
    _total_mem_bytes -= old_keys * _code_bytes_per_vec * sizeof(uint8_t);
  }

  delete old_invert_ptr;
  old_invert_ptr = nullptr;
  _extend_invert_ptr = nullptr;

  return true;
}

bool RealTimeMemData::GetIvtList(const size_t &bucket_no, long *&ivt_list,
                                 uint8_t *&ivt_codes_list) {
  ivt_list = _cur_invert_ptr->_idx_array[bucket_no];
  ivt_codes_list = (uint8_t *)(_cur_invert_ptr->_codes_array[bucket_no]);

  return true;
}

int RealTimeMemData::RetrieveCodes(
    int *vids, size_t vid_size,
    std::vector<std::vector<const uint8_t *>> &bucket_codes,
    std::vector<std::vector<long>> &bucket_vids) {
  bucket_codes.resize(_buckets_num);
  bucket_vids.resize(_buckets_num);
  for (size_t i = 0; i < _buckets_num; i++) {
    bucket_codes[i].reserve(vid_size / _buckets_num);
    bucket_vids[i].reserve(vid_size / _buckets_num);
  }

  for (size_t i = 0; i < vid_size; i++) {
    if (_vid_bucket_no_pos[vids[i]] != -1) {
      int bucket_no = _vid_bucket_no_pos[vids[i]] >> 32;
      int pos = _vid_bucket_no_pos[vids[i]] & 0xffffffff;
      bucket_codes[bucket_no].push_back(
          _cur_invert_ptr->_codes_array[bucket_no] + pos * _code_bytes_per_vec);
      bucket_vids[bucket_no].push_back(vids[i]);
    }
  }

  return 0;
}

int RealTimeMemData::RetrieveCodes(
    int **vids_list, size_t vids_list_size,
    std::vector<std::vector<const uint8_t *>> &bucket_codes,
    std::vector<std::vector<long>> &bucket_vids) {
  bucket_codes.resize(_buckets_num);
  bucket_vids.resize(_buckets_num);
  for (size_t i = 0; i < _buckets_num; i++) {
    bucket_codes[i].reserve(vids_list_size / _buckets_num);
    bucket_vids[i].reserve(vids_list_size / _buckets_num);
  }

  for (size_t i = 0; i < vids_list_size; i++) {
    for (int j = 1; j <= vids_list[i][0]; j++) {
      int vid = vids_list[i][j];
      if (_vid_bucket_no_pos[vid] != -1) {
        int bucket_no = _vid_bucket_no_pos[vid] >> 32;
        int pos = _vid_bucket_no_pos[vid] & 0xffffffff;
        bucket_codes[bucket_no].push_back(
            _cur_invert_ptr->_codes_array[bucket_no] +
            pos * _code_bytes_per_vec);
        bucket_vids[bucket_no].push_back(vid);
      }
    }
  }

  return 0;
}

int RealTimeMemData::Dump(const std::string &dir, const std::string &vec_name,
                          int max_vid) {
  int buckets[_buckets_num];
  long *ids[_buckets_num];
  uint8_t *codes[_buckets_num];
  LOG(INFO) << "dump max vector id=" << max_vid;

  int ids_count = 0;
  int real_dump_min_vid = INT_MAX, real_dump_max_vid = -1;
  for (size_t i = 0; i < _buckets_num; i++) {
    int start_pos = -1;
    int size = 0;
    if (_cur_invert_ptr->GetCurDumpPos(i, max_vid, start_pos, size) == 0) {
      ids[i] = _cur_invert_ptr->_idx_array[i] + start_pos;
      codes[i] =
          _cur_invert_ptr->_codes_array[i] + (start_pos * _code_bytes_per_vec);
      int bucket_min_vid = _cur_invert_ptr->_idx_array[i][start_pos];
      int bucket_max_vid = _cur_invert_ptr->_idx_array[i][start_pos + size - 1];
#ifdef DEBUG
      LOG(INFO) << "dump bucket no=" << i << ", min vid=" << bucket_min_vid
                << ", max vid=" << bucket_max_vid << ", size=" << size
                << ", dir=" << dir
                << ", _dump_latest_pos=" << _cur_invert_ptr->_dump_latest_pos[i]
                << ", vids=" << utils::join(ids[i], size, ',');
#endif
      if (real_dump_min_vid > bucket_min_vid) {
        real_dump_min_vid = bucket_min_vid;
      }
      if (real_dump_max_vid < bucket_max_vid) {
        real_dump_max_vid = bucket_max_vid;
      }
    }
    buckets[i] = size;
    ids_count += size;
  }

  if (ids_count > 0) {
    std::string dump_file = dir + "/" + vec_name + ".index";
    FILE *fp = fopen(dump_file.c_str(), "wb");

    fwrite((void *)&ids_count, sizeof(int), 1, fp);
    fwrite((void *)&real_dump_min_vid, sizeof(int), 1, fp);
    fwrite((void *)&real_dump_max_vid, sizeof(int), 1, fp);
    fwrite((void *)&_buckets_num, sizeof(int), 1, fp);
    fwrite((void *)buckets, sizeof(int), _buckets_num, fp);
    for (size_t i = 0; i < _buckets_num; i++) {
      fwrite((void *)ids[i], sizeof(long), buckets[i], fp);
      fwrite((void *)codes[i], sizeof(uint8_t),
             buckets[i] * _code_bytes_per_vec, fp);
    }
    fclose(fp);
    LOG(INFO) << "ids_count=" << ids_count
              << ", real_dump_min_vid=" << real_dump_min_vid
              << ", real_dump_max_vid=" << real_dump_max_vid
              << ", _buckets_num=" << _buckets_num
              << ", buckets=" << utils::join<int>(buckets, _buckets_num, ',');
  }
  return ids_count;
}

int RealTimeMemData::Load(const std::vector<std::string> &index_dirs,
                          const std::string &vec_name) {
  size_t indexes_num = index_dirs.size();
  int ids_count[indexes_num], min_vids[indexes_num], max_vids[indexes_num];
  int bucket_ids[indexes_num][_buckets_num];
  FILE *fp_array[indexes_num];

  int total_bucket_ids[_buckets_num], total_ids = 0;
  memset((void *)total_bucket_ids, 0, _buckets_num * sizeof(int));
  for (size_t i = 0; i < indexes_num; i++) {
    std::string index_file = index_dirs[i] + "/" + vec_name + ".index";
    if (access(index_file.c_str(), F_OK) != 0) {
      fp_array[i] = nullptr;
      continue;
    }
    fp_array[i] = fopen(index_file.c_str(), "rb");
    fread((void *)(ids_count + i), sizeof(int), 1, fp_array[i]);
    fread((void *)(min_vids + i), sizeof(int), 1, fp_array[i]);
    fread((void *)(max_vids + i), sizeof(int), 1, fp_array[i]);
    int buckets_num = 0;
    fread((void *)&buckets_num, sizeof(int), 1, fp_array[i]);
    if ((size_t)buckets_num != _buckets_num) {
      LOG(ERROR) << "buckets_num must be " << _buckets_num;
      continue;
    }
    if (ids_count[i] == 0 || min_vids[i] == INT_MAX || max_vids[i] == -1) {
      LOG(INFO) << " no data in the bucket " << i
                << " of real time index dumped";
      continue;
    }
    if (i > 0 && max_vids[i - 1] != -1 &&
        (min_vids[i] != (max_vids[i - 1] + 1))) {
      std::string last_index_file = index_dirs[i - 1] + "/vector.idx";
      LOG(ERROR) << "the file " << index_file
                 << " missing some vectors after the file " << last_index_file;
    }

    fread((void *)bucket_ids[i], sizeof(int), _buckets_num, fp_array[i]);
    for (size_t j = 0; j < _buckets_num; j++) {
      total_bucket_ids[j] += bucket_ids[i][j];
      total_ids += bucket_ids[i][j];
    }
  }
  long *load_bucket_ids[_buckets_num];
  uint8_t *load_bucket_codes[_buckets_num];
  _total_mem_bytes = _buckets_num * sizeof(int) * 3;
  for (size_t i = 0; i < _buckets_num; i++) {
    size_t total_keys = total_bucket_ids[i] * 2;
    // if (total_keys > _bucket_keys) {
    //   total_keys = _bucket_keys;
    // }
    load_bucket_ids[i] = new long[total_keys];
    _total_mem_bytes += total_keys * sizeof(long);
    load_bucket_codes[i] = new uint8_t[total_keys * _code_bytes_per_vec];
    _total_mem_bytes += total_keys * _code_bytes_per_vec * sizeof(uint8_t);
    _cur_invert_ptr->_cur_bucket_keys[i] = total_keys;
    _cur_invert_ptr->_retrieve_idx_pos[i] = total_bucket_ids[i];
  }

  int ids_load_offset_list[_buckets_num], codes_load_offset_list[_buckets_num];
  memset(ids_load_offset_list, 0, sizeof(ids_load_offset_list));
  memset(codes_load_offset_list, 0, sizeof(codes_load_offset_list));
  for (size_t i = 0; i < indexes_num; i++) {
    if (!fp_array[i]) {
      continue;
    }
    for (size_t j = 0; j < _buckets_num; j++) {
      fread((void *)(load_bucket_ids[j] + ids_load_offset_list[j]),
            sizeof(long), bucket_ids[i][j], fp_array[i]);
#ifdef DEBUG
      long min_vid = load_bucket_ids[j][ids_load_offset_list[j]];
      long max_vid =
          load_bucket_ids[j][ids_load_offset_list[j] + bucket_ids[i][j] - 1];
      LOG(INFO) << "index id=" << i << ", bucket no=" << j
                << ", min vid=" << min_vid << ", max vid=" << max_vid
                << ", size=" << bucket_ids[i][j];
#endif
      ids_load_offset_list[j] += bucket_ids[i][j];
      int codes_count = bucket_ids[i][j] * _code_bytes_per_vec;
      fread((void *)(load_bucket_codes[j] + codes_load_offset_list[j]),
            sizeof(uint8_t), codes_count, fp_array[i]);
      codes_load_offset_list[j] += codes_count;
    }
    fclose(fp_array[i]);
  }

  /* switch the ids and codes memory pointer */
  for (size_t i = 0; i < _buckets_num; i++) {
    delete[] _cur_invert_ptr->_idx_array[i];
    _cur_invert_ptr->_idx_array[i] = load_bucket_ids[i];
    delete[] _cur_invert_ptr->_codes_array[i];
    _cur_invert_ptr->_codes_array[i] = load_bucket_codes[i];
    _cur_invert_ptr->_dump_latest_pos[i] = total_bucket_ids[i];
#ifdef DEBUG
    LOG(INFO) << "bucket id=" << i
              << ", _dump_latest_pos=" << _cur_invert_ptr->_dump_latest_pos[i];
#endif
  }

  // create _vid_bucket_no_pos
  _vid_bucket_no_pos.resize(_max_vec_size, -1);
  int bucket_size = 0;
  long vid = -1;
  for (size_t bucket_id = 0; bucket_id < _buckets_num; bucket_id++) {
    bucket_size = _cur_invert_ptr->_retrieve_idx_pos[bucket_id];
    for (int retrive_pos = 0; retrive_pos < bucket_size; retrive_pos++) {
      vid = _cur_invert_ptr->_idx_array[bucket_id][retrive_pos];
      if (vid >= _max_vec_size || vid < 0) {
        LOG(INFO) << "invalid vid=" << vid
                  << ", max vector size=" << _max_vec_size;
        return -1;
      }
      _vid_bucket_no_pos[vid] = bucket_id << 32 | retrive_pos;
    }
  }
  return total_ids;
}

}  // namespace realtime

}  // namespace tig_gamma
