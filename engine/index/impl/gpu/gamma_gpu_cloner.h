/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 * The works below are modified based on faiss:
 * 1. Add gamma_index_cpu_to_gpu_multiple
 *
 * Modified works copyright 2019 The Gamma Authors.
 *
 * The modified codes are licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 *
 */

#pragma once

#include <faiss/Index.h>
#include <faiss/clone_index.h>
#include <faiss/gpu/GpuClonerOptions.h>
#include <faiss/gpu/GpuIndex.h>
#include <faiss/gpu/GpuIndicesOptions.h>
#include <faiss/gpu/StandardGpuResources.h>

#include <vector>

#include "index/impl/gamma_index_ivfpq.h"

namespace tig_gamma {
namespace gamma_gpu {

using faiss::Index;
using faiss::IndexIVF;
using faiss::gpu::GpuClonerOptions;
using faiss::gpu::GpuMultipleClonerOptions;
using faiss::gpu::GpuResources;
using faiss::gpu::StandardGpuResources;

/// Cloner specialized for GPU -> CPU
struct GammaToCPUCloner : faiss::Cloner {
  void merge_index(Index *dst, Index *src, bool successive_ids);
  Index *clone_Index(const Index *index) override;
};

/// Cloner specialized for CPU -> 1 GPU
struct GammaToGpuCloner : faiss::Cloner, GpuClonerOptions {
  StandardGpuResources *resources;
  int device;

  GammaToGpuCloner(StandardGpuResources *resources, int device,
                   const GpuClonerOptions &options);

  Index *clone_Index(const Index *index) override;
};

/// Cloner specialized for CPU -> multiple GPUs
struct GammaToGpuClonerMultiple : faiss::Cloner, GpuMultipleClonerOptions {
  std::vector<GammaToGpuCloner> sub_cloners;

  GammaToGpuClonerMultiple(std::vector<StandardGpuResources *> &resources,
                           std::vector<int> &devices,
                           const GpuMultipleClonerOptions &options);

  GammaToGpuClonerMultiple(const std::vector<GammaToGpuCloner> &sub_cloners,
                           const GpuMultipleClonerOptions &options);

  void copy_ivf_shard(const GammaIVFPQIndex *index_ivf, IndexIVF *idx2, long n,
                      long i);

  Index *clone_Index_to_shards(const GammaIVFPQIndex *index);

  /// main function
  Index *clone_Index(const GammaIVFPQIndex *index);

  /// main function
  Index *clone_Index(const faiss::Index *index) { return nullptr; };
};

/// converts any GPU index inside gpu_index to a CPU index
faiss::Index *gamma_index_gpu_to_cpu(const faiss::Index *gpu_index);

/// converts any CPU index that can be converted to GPU
faiss::Index *gamma_index_cpu_to_gpu(StandardGpuResources *resources,
                                     int device, const GammaIVFPQIndex *index,
                                     const GpuClonerOptions *options = nullptr);

faiss::Index *gamma_index_cpu_to_gpu_multiple(
    std::vector<StandardGpuResources *> &resources, std::vector<int> &devices,
    const GammaIVFPQIndex *index,
    const GpuMultipleClonerOptions *options = nullptr);

}  // namespace gamma_gpu
}  // namespace tig_gamma
