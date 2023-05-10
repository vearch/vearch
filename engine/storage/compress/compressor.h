/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

namespace tig_gamma {

#define DEFAULT_RATE 16
enum class CompressType : uint8_t { NotCompress, Zfp, Zstd };

class Compressor {
 public:
  Compressor(CompressType type) { type_ = type; }

  virtual ~Compressor() {}

  virtual void Init(int d, double r = DEFAULT_RATE, int t = 0) = 0;

  virtual size_t GetCompressLen(int data_len = 0) = 0;

  virtual int GetRawLen() = 0;

  virtual size_t Compress(char* data, char* output, int data_len) = 0;

  virtual size_t Decompress(char* data, char* output, int data_len) = 0;

  virtual size_t CompressBatch(char* datum, char* output, int n,
                               int data_len) = 0;

  virtual size_t DecompressBatch(char* datum, char* output, int n,
                                 int data_len) = 0;

  CompressType GetCompressType() { return type_; }

 private:
  CompressType type_;
};

}  // namespace tig_gamma
