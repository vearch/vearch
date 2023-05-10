/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "string_block.h"

#include <unistd.h>

namespace tig_gamma {

StringBlock::StringBlock(int fd, int per_block_size, int length,
                         uint32_t header_size, uint32_t seg_id,
                         std::string name, uint32_t seg_block_capacity)
    : Block(fd, per_block_size, length, header_size, seg_id, name,
            seg_block_capacity, nullptr, -1) {}

StringBlock::~StringBlock() {
  if (block_pos_fp_ != nullptr) {
    fclose(block_pos_fp_);
    block_pos_fp_ = nullptr;
  }
}

void StringBlock::InitStrBlock(void *lru) {
  str_lru_cache_ = (CacheBase<uint32_t, ReadFunParameter *> *)lru;
  bool res = block_pos_.Init(std::string("StrBlock_") + std::to_string(seg_id_),
                             BEGIN_GRP_OF_BLOCK_POS, GRP_GAP_OF_BLOCK_POS);
  if (res == false) {
    LOG(ERROR) << "StrBlock[" << name_ + "_" << seg_id_
               << "]ConcurrentVector init failed.";
  }
}

int StringBlock::LoadIndex(const std::string &file_path) {
  FILE *file = fopen(file_path.c_str(), "rb");
  if (file != nullptr) {
    size_t read_num = 0;
    do {
      uint32_t pos;
      read_num = fread(&pos, sizeof(pos), 1, file);
      if (read_num == 0) {
        break;
      }
      block_pos_.PushBack(pos);
    } while (read_num != 0);

    fclose(file);
  }

  block_pos_fp_ = fopen(file_path.c_str(), "ab+");
  if (block_pos_fp_ == nullptr) {
    LOG(ERROR) << "StrBlock[" << name_ + "_" << seg_id_
               << "] open block pos file error, path=" << file_path;
    return -1;
  }
  block_pos_file_path_ = file_path;
  return 0;
}

int StringBlock::CloseBlockPosFile() {
  if (block_pos_fp_ != nullptr) {
    fclose(block_pos_fp_);
    block_pos_fp_ = nullptr;
    return 0;
  }
  return -1;
}

int StringBlock::WriteContent(const uint8_t *value, uint32_t n_bytes,
                              uint32_t start, disk_io::AsyncWriter *disk_io,
                              std::atomic<uint32_t> *cur_size) {
  return 0;
}

int StringBlock::ReadContent(uint8_t *value, uint32_t n_bytes, uint32_t start) {
  return 0;
}

int StringBlock::WriteString(const char *value, str_len_t n_bytes,
                             str_offset_t start, uint32_t &block_id,
                             in_block_pos_t &in_block_pos) {
  pwrite(fd_, value, n_bytes, start);

  if (block_pos_.Size() == 0) {
    AddBlockPos(0);
  }
  uint32_t block_pos = 0;
  block_pos_.GetLastData(block_pos);
  in_block_pos = start - block_pos;
  uint32_t block_len = (uint32_t)in_block_pos;
  block_len += (uint32_t)n_bytes;
  if (block_len >= per_block_size_) {
    AddBlockPos(start);
    in_block_pos = 0;
  }
  block_id = block_pos_.Size() - 1;

  return 0;
}

int StringBlock::UpdateString(const char *value, str_len_t n_bytes, uint32_t block_id,
                              in_block_pos_t in_block_pos) {
  uint32_t block_pos = 0;
  bool ret = block_pos_.GetData(block_id, block_pos);
  if (ret == false || in_block_pos + n_bytes > per_block_size_) {
    LOG(ERROR) << "update failed. block_pos_ size:" << block_pos_.Size()
               << " in_block_pos:" << in_block_pos << " n_bytes:" << n_bytes
               << " per_block_size:" << per_block_size_;
    return -1;
  }
  pwrite(fd_, value, n_bytes, block_pos + in_block_pos);
  if (str_lru_cache_) {
    uint32_t cache_bid = GetCacheBlockId(block_id);
    str_lru_cache_->Update(cache_bid, value, n_bytes, in_block_pos);
  }
  return 0;
}

int StringBlock::Read(uint32_t block_id, in_block_pos_t in_block_pos, str_len_t n_bytes,
                      std::string &str_out) {
  if (str_lru_cache_ == nullptr) {
    uint32_t block_pos = 0;
    block_pos_.GetData(block_id, block_pos);
    uint32_t off = block_pos + in_block_pos;
    char *tmp_buf = new char[n_bytes];
    pread(fd_, tmp_buf, n_bytes, off);
    str_out = std::string(tmp_buf, n_bytes);
    delete[] tmp_buf;
    return 0;
  }

  uint32_t block_pos = 0;
  block_pos_.GetData(block_id, block_pos);
  uint32_t block_num = block_pos_.Size();

  // The last block of each segment does not enter the cache.
  if (block_id + 1 >= block_num) {
    char *str = new char[n_bytes];
    pread(fd_, str, n_bytes, block_pos + in_block_pos);
    str_out = std::string(str, n_bytes);
    delete[] str;
  } else {
    char *block = nullptr;
    uint32_t cache_bid = GetCacheBlockId(block_id);
    ReadFunParameter parameter;
    parameter.fd = fd_;
    uint32_t front_b_pos = 0;
    uint32_t back_b_pos = 0;
    block_pos_.GetData(block_id + 1, back_b_pos);
    block_pos_.GetData(block_id, front_b_pos);

    parameter.len = back_b_pos - front_b_pos;
    parameter.offset = front_b_pos;
    bool res = str_lru_cache_->SetOrGet(cache_bid, block, &parameter);

    if (not res || block == nullptr) {
      LOG(ERROR) << "StrBlock[" << name_ + "_" << seg_id_
                 << "], Read block fails from disk_file, block_id[" << block_id
                 << "]";
      char *str = new char[n_bytes];
      pread(fd_, str, n_bytes, block_pos + in_block_pos);
      str_out = std::string(str, n_bytes);
      delete[] str;
      return 0;
    }
    str_out = std::string(block + in_block_pos, n_bytes);
  }
  // else {
  //   char *block = nullptr;
  //   uint32_t cache_bid = GetCacheBlockId(block_id);
  //   bool res = str_lru_cache_->Get2(cache_bid, block);
  //   if (block == nullptr) {
  //     LOG(ERROR) << "StringBlock Get block=nullptr:";
  //   }
  //   if (not res) {
  //     ReadFunParameter parameter;
  //     parameter.fd = fd_;              // TODO remove
  //     parameter.len = block_pos_.GetData(block_id + 1) -
  //     block_pos_.GetData(block_id); parameter.offset =
  //     block_pos_.GetData(block_id);

  //     ReadString(cache_bid, block, &parameter);
  //     str_lru_cache_->Set2(cache_bid, block);
  //     if (block == nullptr) {
  //       LOG(ERROR) << "stringblock cell = nullptr";
  //     }
  //   }

  //   str_out = std::string(block + in_block_pos, n_bytes);
  // }
  return 0;
}

bool StringBlock::ReadString(uint32_t key, char *block,
                             ReadFunParameter *param) {
  if (param->len > MAX_BLOCK_SIZE) {
    LOG(ERROR) << "ReadString len[" << param->len << "] fd[" << param->fd
               << "] offset[" << param->offset << "]";
    return false;
  }
  if (block == nullptr) {
    LOG(ERROR) << "ReadString block is nullptr.";
    return false;
  }
  pread(param->fd, block, param->len, param->offset);
  return true;
}

void StringBlock::SetCache(void *cache) {
  str_lru_cache_ = (CacheBase<uint32_t, ReadFunParameter *> *)cache;
}

int StringBlock::AddBlockPos(uint32_t block_pos) {
  block_pos_.PushBack(block_pos);
  bool is_close = false;
  if (block_pos_fp_ == nullptr) {
    block_pos_fp_ = fopen(block_pos_file_path_.c_str(), "ab+");
    if (block_pos_fp_ == nullptr) {
      LOG(ERROR) << "StrBlock[" << name_ + "_" << seg_id_
                 << "] open block pos file error, path="
                 << block_pos_file_path_;
      return -1;
    }
    is_close = true;
  }
  fwrite(&block_pos, sizeof(block_pos), 1, block_pos_fp_);
  fflush(block_pos_fp_);
  if (is_close) {
    CloseBlockPosFile();
  }
  return 0;
}

}  // namespace tig_gamma
