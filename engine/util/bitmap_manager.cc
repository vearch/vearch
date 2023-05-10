/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "util/bitmap_manager.h"

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <functional>

#include "util/log.h"
#include "util/utils.h"

namespace bitmap {

BitmapManager::BitmapManager() {
  bitmap_ = nullptr;
  size_ = 0;
  fd_ = -1;
  fpath_ = "";
}

BitmapManager::~BitmapManager() {
  if (bitmap_) {
    delete[] bitmap_;
    bitmap_ = nullptr;
  }
  if (fd_ != -1) {
    close(fd_);
    fd_ = -1;
  }
}

int BitmapManager::Init(uint32_t bit_size, const std::string &fpath,
                        char *bitmap) {
  if (bit_size <= 0) {
    LOG(ERROR) << "bit_size <= 0";
    return -1;
  }
  this->size_ = bit_size;
  uint32_t bytes_count = (bit_size >> 3) + 1;

  if (bitmap) {
    bitmap_ = bitmap;
  } else {
    bitmap_ = new char[bytes_count];
    if (bitmap_ == nullptr) {
      LOG(ERROR) << "new char[" << bytes_count << "] error.";
      return -1;
    }
  }
  memset(bitmap_, 0, bytes_count);

  // open dump file
  int ret = 0;
  if (not fpath.empty() && fd_ == -1) {
    fpath_ = fpath;
    fd_ = open(fpath_.c_str(), O_RDWR | O_CREAT, 0666);
    if (-1 == fd_) {
      LOG(ERROR) << "open file error, path=" << fpath_;
      ret = -1;
    }
  }
  LOG(INFO) << "BitmapManager init successed. bytes_count=" << bytes_count
            << " bit_size=" << bit_size;
  return ret;
}

int BitmapManager::SetDumpFilePath(const std::string &fpath) {
  if (not fpath.empty()) {
    if (fd_ != -1) {
      LOG(ERROR) << "The file[" << fpath_ << "] is already open. close it.";
      close(fd_);
    }
    fpath_ = fpath;
    fd_ = open(fpath_.c_str(), O_RDWR | O_CREAT, 0666);
    if (-1 == fd_) {
      LOG(ERROR) << "open file error, path=" << fpath_;
      return -1;
    }
    LOG(INFO) << "open bitmap file[" << fpath << "] success.";
    return 0;
  }
  return -1;
}

int BitmapManager::Dump(uint32_t begin_bit_id, uint32_t bit_len) {
  if (bit_len == 0) bit_len = size_;

  if (begin_bit_id < 0 || bit_len < 0 || begin_bit_id + bit_len > size_) {
    LOG(ERROR) << "parameters error, begin_bit_id=" << begin_bit_id
               << " dump_bit_len=" << bit_len << " bit_size=" << size_;
    return -1;
  }

  uint32_t begin_bytes = begin_bit_id >> 3;
  uint32_t end_bytes = (begin_bit_id + bit_len - 1) >> 3;
  uint32_t dump_bytes = end_bytes - begin_bytes + 1;
  int ret = 0;

  if (fd_ != -1) {
    uint32_t written_bytes = 0;
    int i = 0;
    while (written_bytes < dump_bytes) {
      uint32_t bytes =
          pwrite(fd_, bitmap_ + begin_bytes + written_bytes,
                 dump_bytes - written_bytes, begin_bytes + written_bytes);
      written_bytes += bytes;
      if (++i >= 1000) {
        LOG(ERROR) << "dumped bitmap is not complate, written_bytes="
                   << written_bytes;
        ret = -1;
        break;
      }
    }
  } else {
    ret = -1;
  }
  return ret;
}

int BitmapManager::Load(uint32_t begin_bit_id, uint32_t bit_len) {
  if (bit_len == 0) bit_len = size_;

  if (begin_bit_id < 0 || bit_len < 0 || begin_bit_id + bit_len > size_) {
    LOG(ERROR) << "parameters error, begin_bit_id=" << begin_bit_id
               << " load_bit_len=" << bit_len << " size=" << size_;
    return -1;
  }

  uint32_t begin_bytes = begin_bit_id >> 3;
  uint32_t end_bytes = (begin_bit_id + bit_len - 1) >> 3;
  uint32_t load_bytes = end_bytes - begin_bytes + 1;
  int ret = 0;
  if (fd_ != -1) {
    uint32_t read_bytes = 0;
    int i = 0;
    while (read_bytes < load_bytes) {
      uint32_t bytes = pread(fd_, bitmap_ + begin_bytes + read_bytes,
                             load_bytes - read_bytes, begin_bytes + read_bytes);
      read_bytes += bytes;
      if (++i >= 1000) {
        LOG(ERROR) << "load bitmap is not complate, load_bytes=" << read_bytes;
        ret = -1;
        break;
      }
    }
  } else {
    ret = -1;
  }
  return ret;
}

uint32_t BitmapManager::FileBytesSize() {
  if (fd_ != -1) {
    uint32_t len = lseek(fd_, 0, SEEK_END);
    return len;
  }
  return 0;
}

int BitmapManager::Set(uint32_t bit_id) {
  if (bit_id >= 0 && bit_id < size_ && bitmap_ != nullptr) {
    bitmap_[bit_id >> 3] |= (0x1 << (bit_id & 0x7));
    return 0;
  }
  return -1;
}

int BitmapManager::Unset(uint32_t bit_id) {
  if (bit_id >= 0 && bit_id < size_ && bitmap_ != nullptr) {
    bitmap_[bit_id >> 3] &= ~(0x1 << (bit_id & 0x7));
    return 0;
  }
  return -1;
}

bool BitmapManager::Test(uint32_t bit_id) {
  if (bit_id >= 0 && bit_id < size_ && bitmap_ != nullptr) {
    return (bitmap_[bit_id >> 3] & (0x1 << (bit_id & 0x7)));
  }
  return false;
}

void BitmapManager::SetMaxID(uint32_t bit_id) {
  if (size_ > bit_id) return;

  uint32_t old_bytes_count = (size_ >> 3) + 1;
  size_ *= 2;
  uint32_t bytes_count = (size_ >> 3) + 1;
  char *bitmap = new char[bytes_count];
  if (bitmap == nullptr) {
    LOG(INFO) << "new char [" << bytes_count << "] error.";
    return;
  }
  memset(bitmap, 0, bytes_count);
  char *old = bitmap_;
  memcpy(bitmap, old, old_bytes_count);
  bitmap_ = bitmap;
  Dump();

  // delay free
  utils::AsyncWait(
      1000 * 100, [](char *bitmap) { delete[] bitmap; }, old);  // after 100s

  LOG(INFO) << "Current bitmap size [" << size_ << "]";
}

}  // namespace bitmap
