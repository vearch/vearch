/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "block.h"

namespace tig_gamma {

class TableBlock : public Block {
 public:
  TableBlock(int fd, int per_block_size, int length, uint32_t header_size,
             uint32_t seg_id, std::string name, uint32_t seg_block_capacity,
             const std::atomic<uint32_t> *cur_size, int max_size);

  static bool ReadBlock(uint32_t key, char *block, ReadFunParameter *param);

  int WriteContent(const uint8_t *value, uint32_t n_bytes, uint32_t start,
                   disk_io::AsyncWriter *disk_io,
                   std::atomic<uint32_t> *cur_size) override;

  int ReadContent(uint8_t *value, uint32_t n_bytes, uint32_t start) override;

 private:
  void InitSubclass(){};

  int GetReadFunParameter(ReadFunParameter &parameter, uint32_t len,
                          uint32_t off) override;
};

}  // namespace tig_gamma
