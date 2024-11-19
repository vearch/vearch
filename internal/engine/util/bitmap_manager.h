/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "util/status.h"

namespace bitmap {

class BitmapManager {
 public:
  BitmapManager();
  virtual ~BitmapManager();

  virtual int Init(int64_t bit_size, const std::string &fpath = "",
                   std::shared_ptr<char[]> bitmap = nullptr);

  virtual int SetDumpFilePath(const std::string &fpath);

  virtual int Dump(int64_t begin_bit_id = 0, int64_t bit_len = 0);

  virtual int Load(int64_t bit_len = 0);

  virtual int64_t FileBytesSize();

  bool IsLoad() { return is_load_; }

  virtual int Set(int64_t bit_id);

  virtual int Unset(int64_t bit_id);

  virtual bool Test(int64_t bit_id);

  virtual int64_t BitSize() { return size_; }

  std::shared_ptr<char[]> Bitmap() { return bitmap_; }

  virtual int64_t BytesSize() { return (size_ >> 3) + 1; }

  virtual int SetMaxID(int64_t bit_id);

  std::shared_ptr<char[]> bitmap_;
  int64_t size_;
  int fd_;
  std::string fpath_;
  bool is_load_;
};

constexpr uint32_t kBitmapSegmentBits = 1024 * 8;
constexpr uint32_t kBitmapSegmentBytes = 1024;
constexpr uint32_t kBitmapCacheSize = 10 * 1024 * 1024;
const std::string kBitmapSizeKey = "bitmap_size";

class RocksdbBitmapManager : public BitmapManager {
 public:
  RocksdbBitmapManager();
  virtual ~RocksdbBitmapManager();

  virtual int Init(int64_t bit_size, const std::string &fpath = "",
                   std::shared_ptr<char[]> bitmap = nullptr);

  virtual int SetDumpFilePath(const std::string &fpath);

  virtual int Dump(int64_t begin_bit_id = 0, int64_t bit_len = 0);

  virtual int Load(int64_t bit_len = 0);

  virtual int64_t FileBytesSize();

  virtual int Set(int64_t bit_id);

  virtual int Unset(int64_t bit_id);

  virtual bool Test(int64_t bit_id);

  virtual int SetMaxID(int64_t bit_id);

  rocksdb::DB *db_;
  bool should_load_;
};

}  // namespace bitmap
