/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifndef _REALTIME_MEM_DATA_H_
#define _REALTIME_MEM_DATA_H_

#include <stdint.h>
#include <stdlib.h>
#include <string>
#include <vector>

namespace tig_gamma {

namespace realtime {

struct RTInvertBucketData {

  RTInvertBucketData(long **idx_array, int *retrieve_idx_pos,
                     int *cur_bucket_keys, uint8_t **codes_array,
                     int *dump_latest_pos);

  RTInvertBucketData();

  bool Init(const size_t &buckets_num, const size_t &bucket_keys,
            const size_t &code_bytes_per_vec, long &total_mem_bytes);
  ~RTInvertBucketData();

  bool ExtendBucketMem(const size_t &bucket_no,
                       const size_t &code_bytes_per_vec, long &total_mem_bytes);

  bool ReleaseBucketMem(const size_t &bucket_no,
                        const size_t &code_bytes_per_vec,
                        long &total_mem_bytes);

  bool GetBucketMemInfo(const size_t &bucket_no, std::string &mem_info);

  int GetCurDumpPos(const size_t &bucket_no, int max_vid, int &dump_start_pos,
                    int &size);

  long **_idx_array;
  int *_retrieve_idx_pos; // total nb of realtime added indexed vectors
  int *_cur_bucket_keys;
  uint8_t **_codes_array;
  int *_dump_latest_pos;
};

struct RealTimeMemData {
public:
  RealTimeMemData(size_t buckets_num, long max_vec_size,
                  size_t bucket_keys = 500,
                  size_t code_bytes_per_vec = 512 * sizeof(float));
  ~RealTimeMemData();

  bool Init();

  bool AddKeys(size_t list_no, size_t n, std::vector<long> &keys,
               std::vector<uint8_t> &keys_codes);

  bool ExtendBucketMem(const size_t &bucket_no);
  bool GetIvtList(const size_t &bucket_no, long *&ivt_list,
                  uint8_t *&ivt_codes_list);

  long GetTotalMemBytes() { return _total_mem_bytes; }

  int RetrieveCodes(int *vids, size_t vid_size,
                    std::vector<std::vector<const uint8_t *>> &bucket_codes,
                    std::vector<std::vector<long>> &bucket_vids);

  int RetrieveCodes(int **vids_list, size_t vids_list_size,
                    std::vector<std::vector<const uint8_t *>> &bucket_codes,
                    std::vector<std::vector<long>> &bucket_vids);

  int Dump(const std::string &dir, const std::string &vec_name, int max_vid);
  int Load(const std::vector<std::string> &index_dirs, const std::string &vec_name);

  RTInvertBucketData *_cur_invert_ptr;
  RTInvertBucketData *_extend_invert_ptr;

  size_t _buckets_num; // count of buckets
  size_t _bucket_keys; // max bucket keys

  size_t _code_bytes_per_vec;
  long _total_mem_bytes;

  long _max_vec_size;

  std::vector<long> _vid_bucket_no_pos;
};

} // namespace realtime

} // namespace tig_gamma

#endif
