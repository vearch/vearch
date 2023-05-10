/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifdef WITH_ZFP
#include "gamma_zfp.h"

#include <limits.h>
#include <math.h>
#include <omp.h>

namespace tig_gamma {

/* GammaZFP::GammaZFP(int d){Init(d, DEFAULT_RATE);} */
/* GammaZFP::GammaZFP(int d, double r){Init(d, r);} */

void GammaZFP::Init(int d, double r, int t) {
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
}

size_t GammaZFP::CompressBatch(float* arrays, char* buffer, int n) {
  size_t flag = n * zfpsize;
  int size;

  if (!threads) threads = omp_get_max_threads();
  int chunks = (n + threads - 1) / threads;

#pragma omp parallel for num_threads(threads)
  for (int i = 0; i < threads; i++) {
    for (int j = 0; j < chunks; j++) {
      if (j + i * chunks > n - 1) break;
      size = Compress(arrays + dims * (j + i * chunks),
                      buffer + zfpsize * (j + i * chunks));
      if (size == 0) {
        flag = 0;
      }
    }
  }
  return flag;
}

int GammaZFP::Compress(float* input, char* output) {
  zfp_field* field = zfp_field_1d(input, type, dims);
  zfp_stream* zfp = zfp_stream_open(NULL);
  zfp_stream_set_rate(zfp, rate, type, 1, 0);

  bitstream* b_stream;
  b_stream = stream_open(output, zfpsize);
  zfp_stream_set_bit_stream(zfp, b_stream);
  // zfp_stream_rewind(zfp);
  int size = (int)zfp_compress(zfp, field);
  zfp_field_free(field);
  zfp_stream_close(zfp);
  stream_close(b_stream);
  return size;
}

size_t GammaZFP::DecompressBatch(char* compressed_data, float* output, int n) {
  size_t flag = n * zfpsize;
  int size;
  if (!threads) threads = omp_get_max_threads();
  int chunks = (n + threads - 1) / threads;

#pragma omp parallel for num_threads(threads)
  for (int i = 0; i < threads; i++) {
    for (int j = 0; j < chunks; j++) {
      if (j + i * chunks > n - 1) break;
      size = Decompress(compressed_data + zfpsize * (j + i * chunks),
                        output + dims * (j + i * chunks));
      if (size == 0) {
        flag = 0;
      }
    }
  }
  return flag;
}

int GammaZFP::Decompress(char* compressed_data, float* output) {
  zfp_field* field = zfp_field_1d(output, type, dims);
  zfp_stream* zfp = zfp_stream_open(NULL);
  zfp_stream_set_rate(zfp, rate, type, 1, 0);
  /* zfp_stream_set_execution(zfp, zfp_exec_omp); */
  /* zfp_stream_set_reversible(zfp); */
  bitstream* b_stream;
  zfp_field_set_pointer(field, output);
  b_stream = stream_open(compressed_data, zfpsize);
  zfp_stream_set_bit_stream(zfp, b_stream);
  // zfp_stream_rewind(zfp);
  int size = (int)zfp_decompress(zfp, field);
  zfp_field_free(field);
  zfp_stream_close(zfp);
  stream_close(b_stream);
  return size;
}

}  // namespace tig_gamma

#endif  // WITH_ZFP
