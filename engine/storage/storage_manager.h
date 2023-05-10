/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <sstream>
#include <vector>

#include "async_writer.h"
#include "compress/compressor_zfp.h"
#include "compress/compressor_zstd.h"
#include "lru_cache.h"
#include "segment.h"

#define BEGIN_GRP_CAPACITY_OF_SEGMENT 20
#define GRP_GAP_OF_SEGMENT 50

namespace tig_gamma {

struct StorageManagerOptions {
  int segment_size;
  int fixed_value_bytes;
  uint32_t seg_block_capacity;

  StorageManagerOptions() {
    segment_size = -1;
    fixed_value_bytes = -1;
    seg_block_capacity = 0;
  }

  StorageManagerOptions(const StorageManagerOptions &options) {
    segment_size = options.segment_size;
    fixed_value_bytes = options.fixed_value_bytes;
    seg_block_capacity = options.seg_block_capacity;
  }

  bool IsValid() {
    if (segment_size == -1 || fixed_value_bytes == -1 ||
        seg_block_capacity == 0)
      return false;
    return true;
  }

  std::string ToStr() {
    std::stringstream ss;
    ss << "{segment_size=" << segment_size
       << ", fixed_value_bytes=" << fixed_value_bytes
       << ", seg_block_capacity=" << seg_block_capacity << "}";
    return ss.str();
  }
};

class StorageManager {
 public:
  StorageManager(const std::string &root_path, BlockType block_type,
                 const StorageManagerOptions &options);
  ~StorageManager();
  int Init(std::string name, int cache_size, int str_cache_size = 0);

  int Add(const uint8_t *value, int len);

  str_offset_t AddString(const char *value, str_len_t len, uint32_t &block_id,
                         in_block_pos_t &in_block_pos);

  int Update(int id, uint8_t *value, int len);

  str_offset_t UpdateString(int id, const char *value, str_len_t old_len,
                            str_len_t len, uint32_t &block_id,
                            in_block_pos_t &in_block_pos);

  // warning: vec can't be free
  int Get(int id, const uint8_t *&value);

  int GetString(int id, std::string &value, uint32_t block_id,
                in_block_pos_t in_block_pos, str_len_t len);

  int GetHeaders(int start_id, int n, std::vector<const uint8_t *> &values,
                 std::vector<int> &lens);

  // currently it must call truncate after loading to set size of gamma db
  int Truncate(size_t size);

  int Size() { return size_; }

  int UseCompress(CompressType type, int d = -1, double rate = -1);

  bool AlterCacheSize(int cache_size, int str_cache_size);

  void GetCacheSize(int &cache_size, int &str_cache_size);

  int Sync();

  Compressor *GetCompressor() { return compressor_; }

  const StorageManagerOptions &GetStorageManagerOptions() { return options_; }

 private:
  int Load();

  int Extend();

  std::string NextSegmentFilePath();

 private:
  std::string root_path_;
  std::string name_;
  size_t size_;  // The total number of doc.
  // tbb::concurrent_vector<Segment *> segments_;
  tig_gamma::ConcurrentVector<uint8_t, Segment *> segments_;
  disk_io::AsyncWriter *disk_io_;
  BlockType block_type_;
  StorageManagerOptions options_;
  CacheBase<uint32_t, ReadFunParameter *> *cache_;
  CacheBase<uint32_t, ReadFunParameter *> *str_cache_;
  Compressor *compressor_;
};

}  // namespace tig_gamma
