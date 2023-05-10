/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <zstd.h>

#include "compressor.h"
#include "util/log.h"

namespace tig_gamma {

class CompressorZSTD : public Compressor {
 public:
  CompressorZSTD(CompressType type) : Compressor(type) {
    LOG(INFO) << "CompressorZSTD construction!";
  }

  ~CompressorZSTD() { LOG(INFO) << "CompressorZSTD destroyed successfully!"; }

  void Init(int d, double r = DEFAULT_RATE, int t = 0) {}

  size_t GetCompressLen(int data_len = 0) {
    return ZSTD_compressBound(data_len);
  }

  size_t Compress(char* data, char* output, int data_len) {
    if (data == nullptr || output == nullptr || 0 == data_len) {
      LOG(ERROR) << "data is nullptr or output is nullptr or data_len is 0";
      return 0;
    }
    size_t dst_capacity = ZSTD_compressBound(data_len);

    auto len = ZSTD_compress(output, dst_capacity, data, data_len, 1);
    size_t ret = ZSTD_isError(len);
    if (ret != 0) {
      LOG(ERROR) << "ZSTD_compress error";
      delete[] output;
      output = nullptr;
      return 0;
    }
    // StatisticCompressRate((float)data_len / len);
    // PrintCompressRate(10000);
    return len;
  }

  size_t Decompress(char* data, char* output, int data_len) {
    if (data == nullptr || output == nullptr || 0 == data_len) {
      LOG(ERROR) << "data is NULL or output is NULL or data_len is 0";
      return 0;
    }
    auto de_capacity = ZSTD_getDecompressedSize(data, data_len);

    auto len = ZSTD_decompress(output, de_capacity, data, data_len);
    size_t ret = ZSTD_isError(len);

    if (ret != 0) {
      LOG(ERROR) << "ZSTD_decompress error";
      return 0;
    }
    return len;
  }

  size_t CompressBatch(char* datum, char* output, int n, int data_len) {
    return 0;
  }

  size_t DecompressBatch(char* datum, char* output, int n, int data_len) {
    return 0;
  }

  int GetRawLen() { return 0; }

 private:
  //    void StatisticCompressRate(float rate) {
  //     avg_cmprs_rate_ =
  //         (avg_cmprs_rate_ * compress_num_ + rate) / (compress_num_ + 1);
  //     ++compress_num_;
  //   }

  //   void PrintCompressRate(int interval) {
  //     if (interval > 0 && compress_num_ % interval == 0) {
  //       LOG(INFO) << "CompressorZSTD compress_num[" << compress_num_
  //                 << "], avg_cmprs_rate[" << avg_cmprs_rate_ << "]";
  //     }
  //   }

  //   uint64_t compress_num_ = 0;
  //   double avg_cmprs_rate_ = 0;
};

}  // namespace tig_gamma
