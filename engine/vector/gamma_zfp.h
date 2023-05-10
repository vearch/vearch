/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#ifdef WITH_ZFP

#include "zfp.h"
namespace tig_gamma {

#define DEFAULT_RATE 16

struct GammaZFP {
  GammaZFP(int d, double r) { Init(d, r, 0); }

  GammaZFP(int d, double r, int t) { Init(d, r, t); }

  GammaZFP(int d) { Init(d, DEFAULT_RATE, 0); }

  void Init(int d, double r, int t);

  ~GammaZFP() {}

  int dims;     // the dims of 1D_array
  double rate;  // the rate of compress, default is 16
  int threads;
  size_t zfpsize;
  zfp_type type = zfp_type_float;

  size_t GetSize() { return zfpsize; }

  size_t CompressBatch(float* arrays, char* buffer, int n);

  int Compress(float* input, char* output);

  size_t DecompressBatch(char* compressed_data, float* output, int n);

  int Decompress(char* compressed_data, float* output);
};

}  // namespace tig_gamma

#endif  // WITH_ZFP
