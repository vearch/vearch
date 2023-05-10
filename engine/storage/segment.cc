/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "segment.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "search/error_code.h"
#include "table_block.h"
#include "util/log.h"
#include "util/thread_util.h"
#include "util/utils.h"
#include "vector_block.h"

namespace tig_gamma {

namespace {

inline size_t CapacityOff() { return sizeof(uint8_t); }

inline size_t SizeOff() {
  uint32_t capacity;
  return CapacityOff() + sizeof(capacity);
}

inline size_t StrCapacityOff() {
  uint32_t size;
  return SizeOff() + sizeof(size);
}

inline size_t StrOffsetOff() {
  uint64_t str_capacity;
  return StrCapacityOff() + sizeof(str_capacity);
}

inline size_t StrBlocksSizeOff() {
  str_offset_t str_offset;
  return StrOffsetOff() + sizeof(str_offset);
}

inline size_t StrCompressedOff() {
  uint32_t str_blocks_size;
  return StrBlocksSizeOff() + sizeof(str_blocks_size);
}

inline size_t BCompressedOff() {
  str_offset_t str_compressed_size;
  return StrCompressedOff() + sizeof(str_compressed_size);
}

}  // namespace

Segment::Segment(const std::string &file_path, uint32_t seg_id, int max_size,
                 int vec_byte_size, uint32_t seg_block_capacity,
                 disk_io::AsyncWriter *disk_io, void *cache, void *str_cache)
    : file_path_(file_path),
      seg_id_(seg_id),
      max_size_(max_size),
      item_length_(vec_byte_size),
      seg_block_capacity_(seg_block_capacity),
      disk_io_(disk_io),
      cache_(cache),
      str_cache_(str_cache) {
  base_fd_ = -1;
  str_fd_ = -1;

  version_ = 0;
  uint32_t capacity;
  cur_size_ = 0;
  str_capacity_ = 0;
  str_offset_ = 0;
  uint32_t str_blocks_size;
  str_offset_t str_compressed_size;
  uint8_t b_compressed;

  seg_header_size_ = sizeof(version_) + sizeof(capacity) + sizeof(cur_size_) +
                     sizeof(str_capacity_) + sizeof(str_offset_) +
                     sizeof(str_blocks_size) + sizeof(str_compressed_size) +
                     sizeof(b_compressed);

  per_block_size_ = ((64 * 1024) / item_length_) * item_length_;  // block~=64k
  buffered_size_ = 0;
  str_blocks_ = nullptr;
  blocks_ = nullptr;
}

Segment::~Segment() {
  if (base_fd_ != -1) {
    close(base_fd_);
    base_fd_ = -1;
  }

  if (str_fd_ != -1) {
    close(str_fd_);
    str_fd_ = -1;
  }

  if (blocks_ != nullptr) {
    delete blocks_;
    blocks_ = nullptr;
  }

  if (str_blocks_ != nullptr) {
    delete str_blocks_;
    str_blocks_ = nullptr;
  }
}

uint8_t Segment::Version() {
  uint8_t version = 0;
  pread(base_fd_, &version, sizeof(version), 0);
  return version;
}

void Segment::SetVersion(uint8_t version) {
  pwrite(base_fd_, &version, sizeof(version), 0);
}

uint32_t Segment::BufferedSize() { return buffered_size_; }

void Segment::PersistentedSize() {
  uint32_t capacity;
  uint32_t size = 0;
  pread(base_fd_, &size, sizeof(size), sizeof(version_) + sizeof(capacity));
  cur_size_ = size;
  if (cur_size_ == max_size_) {
    blocks_->SegmentIsFull();
  }
}

uint32_t Segment::BaseOffset() {
  uint32_t size;
  pread(base_fd_, &size, sizeof(size), sizeof(version_) + sizeof(uint32_t));
  return size * item_length_;
}

void Segment::SetBaseSize(uint32_t size) {
  uint32_t capacity;
  if (cur_size_ > size) {
    cur_size_ = size;
  }
  if (buffered_size_ > size) {
    buffered_size_ = size;
  }
  pwrite(base_fd_, &size, sizeof(size), sizeof(version_) + sizeof(capacity));
}

uint64_t Segment::StrCapacity() {
  uint64_t str_capacity;
  pread(base_fd_, &str_capacity, sizeof(str_capacity), StrCapacityOff());
  return str_capacity;
}

void Segment::SetStrCapacity(uint64_t str_capacity) {
  pwrite(base_fd_, &str_capacity, sizeof(str_capacity), StrCapacityOff());
}

uint32_t Segment::StrBlocksSize() {
  uint32_t str_blocks_size;
  pread(base_fd_, &str_blocks_size, sizeof(str_blocks_size),
        StrBlocksSizeOff());
  return str_blocks_size;
}

void Segment::SetStrBlocksSize(uint32_t str_blocks_size) {
  pwrite(base_fd_, &str_blocks_size, sizeof(str_blocks_size),
         StrBlocksSizeOff());
}

str_offset_t Segment::StrOffset() {
  str_offset_t str_offset;
  pread(base_fd_, &str_offset, sizeof(str_offset), StrOffsetOff());
  return str_offset;
}

void Segment::SetStrOffset(str_offset_t str_size) {
  pwrite(base_fd_, &str_size, sizeof(str_size), StrOffsetOff());
}

uint8_t Segment::BCompressed() {
  uint8_t b_compressed;
  pread(base_fd_, &b_compressed, sizeof(b_compressed), BCompressedOff());
  return b_compressed;
}

void Segment::SetCompressed(uint8_t compressed) {
  pwrite(base_fd_, &compressed, sizeof(compressed), BCompressedOff());
}

str_offset_t Segment::StrCompressedSize() {
  str_offset_t str_compressed_size;
  pread(base_fd_, &str_compressed_size, sizeof(str_compressed_size),
        StrCompressedOff());
  return str_compressed_size;
}

void Segment::SetStrCompressedSize(str_offset_t str_compressed_size) {
  pwrite(base_fd_, &str_compressed_size, sizeof(str_compressed_size),
         StrCompressedOff());
}

int Segment::Init(std::string name, BlockType block_type,
                  Compressor *compressor) {
  OpenFile(block_type);
  uint32_t item_len = item_length_;
  if (block_type == BlockType::VectorBlockType && compressor) {
    item_len = compressor->GetCompressLen();
  }
  if (ftruncate(base_fd_, seg_header_size_ + item_len * max_size_)) {
    close(base_fd_);
    LOG(ERROR) << "truncate file error:" << strerror(errno);
    return IO_ERR;
  }

  if (str_fd_ != -1) {
    str_capacity_ = seg_header_size_ + max_size_ * 4;
    str_offset_ = 0;
    int ret = ftruncate(str_fd_, str_capacity_);
    if (ret != 0) {
      return -1;
    }
  }
  SetStrCapacity(str_capacity_);
  SetStrOffset(str_offset_);

  InitBlock(name, block_type, compressor);
  return 0;
}

int Segment::OpenFile(BlockType block_type) {
  base_fd_ = open(file_path_.c_str(), O_RDWR | O_CREAT, 0666);
  if (-1 == base_fd_) {
    LOG(ERROR) << "open vector file error, path=" << file_path_;
    return IO_ERR;
  }

  if (block_type == BlockType::TableBlockType) {
    str_fd_ = open((file_path_ + "_str").c_str(), O_RDWR | O_CREAT, 0666);
    if (-1 == str_fd_) {
      LOG(ERROR) << "open vector file error, path=" << (file_path_ + "_str");
      return -1;
    }
  }
  return 0;
}

int Segment::InitBlock(std::string name, BlockType block_type,
                       Compressor *compressor) {
  switch (block_type) {
    case BlockType::TableBlockType:
      blocks_ = new TableBlock(base_fd_, per_block_size_, item_length_,
                               seg_header_size_, seg_id_, name,
                               seg_block_capacity_, &cur_size_, max_size_);
      str_blocks_ = new StringBlock(str_fd_, per_block_size_, MAX_BLOCK_SIZE,
                                    seg_header_size_, seg_id_, name + "_str",
                                    seg_block_capacity_);
      break;
    case BlockType::VectorBlockType:
      blocks_ = new VectorBlock(base_fd_, per_block_size_, item_length_,
                                seg_header_size_, seg_id_, name,
                                seg_block_capacity_, &cur_size_, max_size_);
      break;
    default:
      LOG(ERROR) << "Unknow BlockType [" << static_cast<int>(block_type) << "]";
      break;
  }

  blocks_->Init(cache_, compressor);

  if (str_blocks_) {
    str_blocks_->InitStrBlock(str_cache_);
    str_blocks_->LoadIndex(file_path_ + "_str.idx");
    if (BufferedSize() == max_size_) {
      str_blocks_->CloseBlockPosFile();
    }
  }
  return 0;
}

// TODO: Load compressor
int Segment::Load(std::string name, BlockType block_type,
                  Compressor *compressor) {
  OpenFile(block_type);
  InitBlock(name, block_type, compressor);
  str_capacity_ = StrCapacity();
  str_offset_ = StrOffset();
  PersistentedSize();
  if (cur_size_ > max_size_) {
    cur_size_ = 0;
    LOG(ERROR) << "Segment[" << blocks_->GetName() + "_" << seg_id_
               << "], load size[" << cur_size_ << "] > max_size[" << max_size_
               << "]. File[" << file_path_ << "] error. cur_size_ change to 0.";
  }
  buffered_size_ = cur_size_;
  return cur_size_;
}

int Segment::Add(const uint8_t *data, int len) {
  size_t offset = (size_t)buffered_size_ * item_length_;
  blocks_->Write(data, len, offset, disk_io_, &cur_size_);
  ++buffered_size_;
  return 0;
}

str_offset_t Segment::AddString(const char *str, str_len_t len,
                                uint32_t &block_id,
                                in_block_pos_t &in_block_pos) {
  if (str_offset_ + len >= str_capacity_) {
    uint64_t extend_capacity = str_capacity_ * 1.3;
    while (str_offset_ + len >= extend_capacity) {
      extend_capacity *= 1.3;
    }

    int ret = 0;
    str_capacity_ = extend_capacity;
    SetStrCapacity(str_capacity_);

    ret = ftruncate(str_fd_, str_capacity_);
    if (ret != 0) {
      return -1;
    }
  }

  str_blocks_->WriteString(str, len, str_offset_, block_id, in_block_pos);

  str_offset_ += len;
  SetStrOffset(str_offset_);
  return str_offset_;
}

str_offset_t Segment::UpdateString(const char *str, str_len_t len, uint32_t block_id,
                                   in_block_pos_t in_block_pos) {
  return str_blocks_->UpdateString(str, len, block_id, in_block_pos);
}

int Segment::GetValues(uint8_t *value, int id, int n) {
  uint32_t start = (uint32_t)id * item_length_;
  uint32_t n_bytes = (uint32_t)n * item_length_;
  // TODO read from buffer queue
  int count = 0;
  while (id + n > (int)cur_size_) {
    // PersistentedSize();
    if (id + n <= (int)cur_size_) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    ++count;
    if (count % 512 == 0) {
      LOG(WARNING) << "Waited " << count * 512
                   << "ms because the data is not being brushed to disk."
                   << " segment[" << blocks_->GetName() + "_" << seg_id_
                   << "], cur_size[" << cur_size_ << "], GetValue(id=" << id
                   << ", n=" << n << ")";
    }
  }
  blocks_->Read(value, n_bytes, start);
  return 0;
}

std::string Segment::GetString(uint32_t block_id, in_block_pos_t in_block_pos,
                               str_len_t len) {
  std::string str;
  str_blocks_->Read(block_id, in_block_pos, len, str);
  return str;
}

bool Segment::IsFull() {
  if (BufferedSize() == max_size_) {
    if (str_blocks_) str_blocks_->CloseBlockPosFile();
    return true;
  } else {
    return false;
  }
}

int Segment::Update(int id, uint8_t *data, int len) {
  size_t offset = (size_t)id * item_length_;
  blocks_->Update(data, len, offset);
  return 0;
}

void Segment::SetCache(void *cache, void *str_cache) {
  blocks_->SetCache(cache);
  if (str_blocks_) {
    str_blocks_->SetCache(str_cache);
  }
}

}  // namespace tig_gamma
