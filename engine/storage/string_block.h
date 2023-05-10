/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>

#include "block.h"
#include "util/concurrent_vector.h"

#define BEGIN_GRP_OF_BLOCK_POS 50
#define GRP_GAP_OF_BLOCK_POS 1000

namespace tig_gamma {

class StringBlock : public Block {
 public:
  StringBlock(int fd, int per_block_size, int length, uint32_t header_size,
              uint32_t seg_id, std::string name, uint32_t seg_block_capacity);

  ~StringBlock();

  int GetReadFunParameter(ReadFunParameter &parameter, uint32_t len,
                          uint32_t off) {
    return 0;
  };

  int LoadIndex(const std::string &file_path);

  int CloseBlockPosFile();

  int WriteContent(const uint8_t *value, uint32_t n_bytes, uint32_t start,
                   disk_io::AsyncWriter *disk_io,
                   std::atomic<uint32_t> *cur_size) override;

  void InitStrBlock(void *lru);

  int ReadContent(uint8_t *value, uint32_t n_bytes, uint32_t start) override;

  int WriteString(const char *value, str_len_t n_bytes, str_offset_t start,
                  uint32_t &block_id, in_block_pos_t &in_block_pos);

  int UpdateString(const char *value, str_len_t n_bytes, uint32_t block_id,
                   in_block_pos_t in_block_pos);

  int Read(uint32_t block_id, in_block_pos_t in_block_pos, str_len_t n_bytes,
           std::string &str_out);

  static bool ReadString(uint32_t key, char *block, ReadFunParameter *param);

  void SetCache(void *cache) override;

 private:
  void InitSubclass(){};

  int AddBlockPos(uint32_t block_pos);

  CacheBase<uint32_t, ReadFunParameter *> *str_lru_cache_;

  std::string block_pos_file_path_;

  FILE *block_pos_fp_;

  tig_gamma::ConcurrentVector<uint16_t, uint32_t> block_pos_;
};

}  // namespace tig_gamma
