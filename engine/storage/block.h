/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <stdint.h>
#include <unistd.h>

#include <atomic>
#include <vector>

#include "async_writer.h"
#include "compress/compressor_zfp.h"
#include "compress/compressor_zstd.h"
#include "lru_cache.h"

#define MAX_BLOCK_SIZE 65536
#define MAX_STRING_LEN 65535

typedef uint32_t str_offset_t;
typedef uint16_t str_len_t;
typedef uint16_t in_block_pos_t;

namespace tig_gamma {

enum class BlockType : uint8_t {
  TableBlockType = 0,
  StringBlockType,
  VectorBlockType
};

class Block {
 public:
  Block(int fd, int per_block_size, int length, uint32_t header_size,
        uint32_t seg_id, std::string name, uint32_t seg_block_capacity,
        const std::atomic<uint32_t> *cur_size, int max_size);

  virtual ~Block();

  void Init(void *lru, Compressor *compressor = nullptr);

  int Write(const uint8_t *value, int n_bytes, uint32_t start,
            disk_io::AsyncWriter *disk_io, std::atomic<uint32_t> *cur_size);

  virtual int Read(uint8_t *value, uint32_t n_bytes, uint32_t start);

  virtual int Update(const uint8_t *value, uint32_t n_bytes, uint32_t start);

  void SegmentIsFull();  // Segment is full and all data is brushed to disk.

  int32_t GetCacheBlockId(uint32_t block_id);

  std::string &GetName() { return name_; }

  virtual void SetCache(void *cache);

 protected:
  // virtual int Compress() = 0;

  // virtual int Uncompress() = 0;

  virtual void InitSubclass() = 0;

  virtual int WriteContent(const uint8_t *value, uint32_t n_bytes,
                           uint32_t start, disk_io::AsyncWriter *disk_io,
                           std::atomic<uint32_t> *cur_size) = 0;

  virtual int GetReadFunParameter(ReadFunParameter &parameter, uint32_t len,
                                  uint32_t off) = 0;

  virtual int ReadContent(uint8_t *value, uint32_t n_bytes, uint32_t start) = 0;

 protected:
  CacheBase<uint32_t, ReadFunParameter *> *lru_cache_;

  int fd_;

  Compressor *compressor_;

  uint32_t per_block_size_;

  uint32_t item_length_;

  uint32_t header_size_;

  uint32_t seg_block_capacity_;

  uint32_t seg_id_;

  std::string name_;

  const std::atomic<uint32_t> *cur_size_;

  uint32_t max_size_;

  uint32_t last_bid_in_disk_;
};

}  // namespace tig_gamma
