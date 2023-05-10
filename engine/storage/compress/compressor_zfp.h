/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#ifdef WITH_ZFP

#include <limits.h>
#include <math.h>
#include <omp.h>

#include "compressor.h"
#include "util/log.h"
#include "zfp.h"

namespace tig_gamma {

class CompressorZFP : public Compressor {
 public:
  CompressorZFP(CompressType type) : Compressor(type) {
    LOG(INFO) << "CompressorZFP construction!";
  }

  ~CompressorZFP() { LOG(INFO) << "CompressorZFP destroyed successfully!"; }

  void Init(int d, double r = DEFAULT_RATE, int t = 0) {
    dims = d;
    threads = t;
    int n = 4;
    int remain = d % 4 == 0 ? 24 : 16;
    int blocks = floor((dims + n - 1) / n);
    int bits = floor(n * r + 0.5);
    bits = bits > 9 ? bits : 9;
    rate = (double)bits / n;
    zfpsize = ((ZFP_HEADER_MAX_BITS + blocks * bits + stream_word_bits - 1) &
               ~(stream_word_bits - 1)) /
                  CHAR_BIT -
              remain;
    raw_len = n * d;
  }

  size_t GetCompressLen(int data_len = 0) { return zfpsize; }

  int GetRawLen() { return raw_len; }

  size_t Compress(char *data, char *output, int data_len) {
    zfp_field *field = zfp_field_1d(data, type, dims);
    zfp_stream *zfp = zfp_stream_open(NULL);
    zfp_stream_set_rate(zfp, rate, type, 1, 0);

    bitstream *b_stream;
    b_stream = stream_open(output, zfpsize);
    zfp_stream_set_bit_stream(zfp, b_stream);
    // zfp_stream_rewind(zfp);
    size_t size = (size_t)zfp_compress(zfp, field);
    zfp_field_free(field);
    zfp_stream_close(zfp);
    stream_close(b_stream);
    return size;
  }

  size_t CompressBatch(char *datum, char *output, int n, int data_len) {
    size_t flag = n * zfpsize;
    int size;

    if (!threads) threads = omp_get_max_threads();
    int chunks = (n + threads - 1) / threads;

#pragma omp parallel for num_threads(threads)
    for (int i = 0; i < threads; i++) {
      for (int j = 0; j < chunks; j++) {
        if (j + i * chunks > n - 1) break;
        size = Compress(datum + sizeof(float) * dims * (j + i * chunks),
                        output + zfpsize * (j + i * chunks), 0);
        if (size == 0) {
          flag = 0;
        }
      }
    }
    return flag;
  }

  size_t Decompress(char *data, char *output, int data_len) {
    zfp_field *field = zfp_field_1d(output, type, dims);
    zfp_stream *zfp = zfp_stream_open(NULL);
    zfp_stream_set_rate(zfp, rate, type, 1, 0);
    /* zfp_stream_set_execution(zfp, zfp_exec_omp); */
    /* zfp_stream_set_reversible(zfp); */
    bitstream *b_stream;
    zfp_field_set_pointer(field, output);
    b_stream = stream_open(data, zfpsize);
    zfp_stream_set_bit_stream(zfp, b_stream);
    // zfp_stream_rewind(zfp);
    size_t size = (size_t)zfp_decompress(zfp, field);
    zfp_field_free(field);
    zfp_stream_close(zfp);
    stream_close(b_stream);
    return size;
  }

  size_t DecompressBatch(char *datum, char *output, int n, int data_len) {
    size_t flag = n * zfpsize;
    int size;
    if (!threads) threads = omp_get_max_threads();
    int chunks = (n + threads - 1) / threads;

#pragma omp parallel for num_threads(threads)
    for (int i = 0; i < threads; i++) {
      for (int j = 0; j < chunks; j++) {
        if (j + i * chunks > n - 1) break;
        size = Decompress(datum + zfpsize * (j + i * chunks),
                          output + sizeof(float) * dims * (j + i * chunks), 0);
        if (size == 0) {
          flag = 0;
        }
      }
    }
    return flag;
  }

 private:
  int dims;     // the dims of 1D_array
  double rate;  // the rate of compress, default is 16
  int threads;
  size_t zfpsize;
  int raw_len;
  zfp_type type = zfp_type_float;
};

}  // namespace tig_gamma

#endif  // WITH_ZFP
