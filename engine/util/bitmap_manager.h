/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>

namespace bitmap {

class BitmapManager {
 public:
  BitmapManager();
  ~BitmapManager();

  int Init(uint32_t bit_size, const std::string &fpath = "",
           char *bitmap = nullptr);

  int SetDumpFilePath(const std::string &fpath);

  int Dump(uint32_t begin_bit_id = 0, uint32_t bit_len = 0);

  int Load(uint32_t begin_bit_id = 0, uint32_t bit_len = 0);

  uint32_t FileBytesSize();

  int Set(uint32_t bit_id);

  int Unset(uint32_t bit_id);

  bool Test(uint32_t bit_id);

  uint32_t BitSize() { return size_; }

  char *Bitmap() { return bitmap_; }

  uint32_t BytesSize() { return (size_ >> 3) + 1; }

  void SetMaxID(uint32_t bit_id);

 private:
  char *bitmap_;
  uint32_t size_;
  int fd_;
  std::string fpath_;
};

}  // namespace bitmap
