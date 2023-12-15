/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "vector_block.h"

#include <unistd.h>

namespace tig_gamma {

VectorBlock::VectorBlock(int fd, int per_block_size, int length,
                         uint32_t header_size, uint32_t seg_id,
                         std::string name, uint32_t seg_block_capacity,
                         const std::atomic<uint32_t> *cur_size, int max_size)
    : Block(fd, per_block_size, length, header_size, seg_id, name,
            seg_block_capacity, cur_size, max_size) {
  vec_item_len_ = item_length_;
}

int VectorBlock::GetReadFunParameter(ReadFunParameter &parameter, uint32_t len,
                                     uint32_t off) {
  parameter.fd = fd_;
  parameter.len = len;
  parameter.offset = off;
  parameter.offset += header_size_;
  return 0;
}

bool VectorBlock::ReadBlock(uint32_t key, char *block,
                            ReadFunParameter *param) {
  if (param->len > MAX_BLOCK_SIZE) {
    LOG(ERROR) << "vector ReadConten len is:" << param->len << " key:" << key;
    return false;
  }
  if (block == nullptr) {
    LOG(ERROR) << "ReadString block is nullptr.";
    return false;
  }
  pread(param->fd, block, param->len, param->offset);
  return true;
}

int VectorBlock::WriteContent(const uint8_t *value, uint32_t n_bytes,
                              uint32_t start, disk_io::AsyncWriter *disk_io,
                              std::atomic<uint32_t> *cur_size) {
  disk_io->Set(header_size_, vec_item_len_);
  struct disk_io::WriterStruct *write_struct =
      new struct disk_io::WriterStruct();
  write_struct->fd = fd_;
  write_struct->data = new uint8_t[vec_item_len_];
  memcpy(write_struct->data, value, vec_item_len_);
  write_struct->start = header_size_ + start;
  write_struct->len = vec_item_len_;
  write_struct->cur_size = cur_size;
  disk_io->AsyncWrite(write_struct);
  // disk_io->SyncWrite(write_struct);
  return 0;
}

int VectorBlock::ReadContent(uint8_t *value, uint32_t n_bytes, uint32_t start) {
  pread(fd_, value, n_bytes, header_size_ + start);
  return 0;
}

int VectorBlock::Read(uint8_t *value, uint32_t n_bytes, uint32_t start) {
  if (lru_cache_ == nullptr) {
    return ReadContent(value, n_bytes, start);
  }

  uint32_t read_num = 0;
  while (n_bytes) {
    uint32_t len = n_bytes;
    if (len > per_block_size_) len = per_block_size_;

    uint32_t block_id = start / per_block_size_;
    uint32_t block_pos = block_id * per_block_size_;
    uint32_t block_offset = start % per_block_size_;

    if (len > per_block_size_ - block_offset)
      len = per_block_size_ - block_offset;

    if (last_bid_in_disk_ <= block_id) {
      last_bid_in_disk_ = (*cur_size_) * vec_item_len_ / per_block_size_;
      if ((*cur_size_) == max_size_) SegmentIsFull();
    }

    bool is_pass_cache = false;
    if (last_bid_in_disk_ > block_id) {
      // std::shared_ptr<std::vector<uint8_t>> block;
      char *block = nullptr;
      uint32_t cache_bid = GetCacheBlockId(block_id);
      ReadFunParameter parameter;
      GetReadFunParameter(parameter, per_block_size_, block_pos);
      bool res = lru_cache_->SetOrGet(cache_bid, block, &parameter);

      if (not res || block == nullptr) {
        LOG(ERROR) << "Read block fails from disk_file, block_id["
                   << name_ + "_" << seg_id_ << "]";
      } else {
        memcpy(value + read_num, block + block_offset, len);
        is_pass_cache = true;
      }
    }

    if (is_pass_cache == false) {
      ReadContent(value + read_num, len, block_pos + block_offset);
    }

    start += len;
    read_num += len;
    n_bytes -= len;
  }
  return 0;
}

int VectorBlock::Update(const uint8_t *value, uint32_t n_bytes,
                        uint32_t start) {
  pwrite(fd_, value, n_bytes, header_size_ + start);

  if (lru_cache_ == nullptr) {
    return 0;
  }
  uint32_t update_len = 0;
  while (n_bytes) {
    uint32_t len = n_bytes;
    if (len > per_block_size_) len = per_block_size_;

    uint32_t block_id = start / per_block_size_;
    uint32_t block_offset = start % per_block_size_;

    if (len > per_block_size_ - block_offset)
      len = per_block_size_ - block_offset;

    uint32_t cache_block_id = GetCacheBlockId(block_id);
    lru_cache_->Update(cache_block_id, (const char *)value + update_len, len,
                       block_offset);

    start += len;
    n_bytes -= len;
    update_len += len;
  }
  return 0;
}

}  // namespace tig_gamma
