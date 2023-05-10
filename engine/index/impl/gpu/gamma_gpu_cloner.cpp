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

#include "gamma_gpu_cloner.h"

#include <faiss/IndexFlat.h>
#include <faiss/IndexIVF.h>
#include <faiss/IndexIVFFlat.h>
#include <faiss/IndexIVFPQ.h>
#include <faiss/IndexReplicas.h>
#include <faiss/IndexScalarQuantizer.h>
#include <faiss/MetaIndexes.h>
#include <faiss/gpu/GpuIndex.h>
#include <faiss/gpu/GpuIndexFlat.h>
#include <faiss/gpu/GpuIndexIVFFlat.h>
#include <faiss/gpu/GpuIndexIVFPQ.h>
#include <faiss/gpu/GpuIndexIVFScalarQuantizer.h>
#include <faiss/gpu/utils/DeviceUtils.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/index_io.h>

#include <typeinfo>

using faiss::IndexFlat;
using faiss::IndexIVFFlat;
using faiss::IndexIVFPQ;
using faiss::IndexIVFScalarQuantizer;

using faiss::gpu::GpuIndexFlat;
using faiss::gpu::GpuIndexIVFFlat;
using faiss::gpu::GpuIndexIVFPQ;
using faiss::gpu::GpuIndexIVFScalarQuantizer;

using faiss::IndexReplicas;
using faiss::IndexShards;

using faiss::MultiIndexQuantizer;
using faiss::gpu::GpuIndexFlatConfig;
using faiss::gpu::GpuIndexIVFFlatConfig;
using faiss::gpu::GpuIndexIVFScalarQuantizerConfig;

using faiss::IndexFlatL2;
using faiss::IndexSplitVectors;
using faiss::ProductQuantizer;

namespace tig_gamma {
namespace gamma_gpu {

/**********************************************************
 * Cloning to CPU
 **********************************************************/

void GammaToCPUCloner::merge_index(Index *dst, Index *src,
                                   bool successive_ids) {
  if (auto ifl = dynamic_cast<IndexFlat *>(dst)) {
    auto ifl2 = dynamic_cast<const IndexFlat *>(src);
    FAISS_ASSERT(ifl2);
    FAISS_ASSERT(successive_ids);
    ifl->add(ifl2->ntotal, ifl2->xb.data());
  } else if (auto ifl = dynamic_cast<IndexIVFFlat *>(dst)) {
    auto ifl2 = dynamic_cast<IndexIVFFlat *>(src);
    FAISS_ASSERT(ifl2);
    ifl->merge_from(*ifl2, successive_ids ? ifl->ntotal : 0);
  } else if (auto ifl = dynamic_cast<IndexIVFScalarQuantizer *>(dst)) {
    auto ifl2 = dynamic_cast<IndexIVFScalarQuantizer *>(src);
    FAISS_ASSERT(ifl2);
    ifl->merge_from(*ifl2, successive_ids ? ifl->ntotal : 0);
  } else if (auto ifl = dynamic_cast<IndexIVFPQ *>(dst)) {
    auto ifl2 = dynamic_cast<IndexIVFPQ *>(src);
    FAISS_ASSERT(ifl2);
    ifl->merge_from(*ifl2, successive_ids ? ifl->ntotal : 0);
  } else {
    FAISS_ASSERT(!"merging not implemented for this type of class");
  }
}

Index *GammaToCPUCloner::clone_Index(const Index *index) {
  if (auto ifl = dynamic_cast<const GpuIndexFlat *>(index)) {
    IndexFlat *res = new IndexFlat();
    ifl->copyTo(res);
    return res;
  } else if (auto ifl = dynamic_cast<const GpuIndexIVFFlat *>(index)) {
    IndexIVFFlat *res = new IndexIVFFlat();
    ifl->copyTo(res);
    return res;
  } else if (auto ifl =
                 dynamic_cast<const GpuIndexIVFScalarQuantizer *>(index)) {
    IndexIVFScalarQuantizer *res = new IndexIVFScalarQuantizer();
    ifl->copyTo(res);
    return res;
  } else if (auto ipq = dynamic_cast<const GpuIndexIVFPQ *>(index)) {
    IndexIVFPQ *res = new IndexIVFPQ();
    ipq->copyTo(res);
    return res;

    // for IndexShards and IndexReplicas we assume that the
    // objective is to make a single component out of them
    // (inverse op of GammaToGpuClonerMultiple)

  } else if (auto ish = dynamic_cast<const IndexShards *>(index)) {
    int nshard = ish->count();
    FAISS_ASSERT(nshard > 0);
    Index *res = clone_Index(ish->at(0));
    for (int i = 1; i < ish->count(); i++) {
      Index *res_i = clone_Index(ish->at(i));
      merge_index(res, res_i, ish->successive_ids);
      delete res_i;
    }
    return res;
  } else if (auto ipr = dynamic_cast<const IndexReplicas *>(index)) {
    // just clone one of the replicas
    FAISS_ASSERT(ipr->count() > 0);
    return clone_Index(ipr->at(0));
  } else {
    return Cloner::clone_Index(index);
  }
}

faiss::Index *gamma_index_gpu_to_cpu(const faiss::Index *gpu_index) {
  GammaToCPUCloner cl;
  return cl.clone_Index(gpu_index);
}

/**********************************************************
 * Cloning to 1 GPU
 **********************************************************/

GammaToGpuCloner::GammaToGpuCloner(StandardGpuResources *resources, int device,
                                   const GpuClonerOptions &options)
    : GpuClonerOptions(options), resources(resources), device(device) {}

Index *GammaToGpuCloner::clone_Index(const Index *index) {
  if (auto ipq = dynamic_cast<const faiss::IndexIVFPQ *>(index)) {
    if (verbose)
      printf(
          "  IndexIVFPQ size %ld -> GpuIndexIVFPQ "
          "indicesOptions=%d "
          "usePrecomputed=%d useFloat16=%d reserveVecs=%ld\n",
          ipq->ntotal, indicesOptions, usePrecomputed, useFloat16, reserveVecs);
    faiss::gpu::GpuIndexIVFPQConfig config;
    config.device = device;
    config.indicesOptions = indicesOptions;
    config.flatConfig.useFloat16 = useFloat16CoarseQuantizer;
    config.flatConfig.storeTransposed = storeTransposed;
    config.useFloat16LookupTables = useFloat16;
    config.usePrecomputedTables = usePrecomputed;

    GpuIndexIVFPQ *res = new GpuIndexIVFPQ(resources, ipq, config);

    if (reserveVecs > 0 && ipq->ntotal == 0) {
      res->reserveMemory(reserveVecs);
    }

    return res;
  } else {
    return Cloner::clone_Index(index);
  }
}

faiss::Index *gamma_index_cpu_to_gpu(StandardGpuResources *resources,
                                     int device, const faiss::Index *index,
                                     const GpuClonerOptions *options) {
  GpuClonerOptions defaults;
  GammaToGpuCloner cl(resources, device, options ? *options : defaults);
  return cl.clone_Index(index);
}

/**********************************************************
 * Cloning to multiple GPUs
 **********************************************************/

GammaToGpuClonerMultiple::GammaToGpuClonerMultiple(
    std::vector<StandardGpuResources *> &resources, std::vector<int> &devices,
    const GpuMultipleClonerOptions &options)
    : GpuMultipleClonerOptions(options) {
  FAISS_ASSERT(resources.size() == devices.size());
  for (size_t i = 0; i < resources.size(); i++) {
    sub_cloners.push_back(GammaToGpuCloner(resources[i], devices[i], options));
  }
}

GammaToGpuClonerMultiple::GammaToGpuClonerMultiple(
    const std::vector<GammaToGpuCloner> &sub_cloners,
    const GpuMultipleClonerOptions &options)
    : GpuMultipleClonerOptions(options), sub_cloners(sub_cloners) {}

void GammaToGpuClonerMultiple::copy_ivf_shard(const GammaIVFPQIndex *index_ivf,
                                              IndexIVF *idx2, long n, long i) {
  if (shard_type == 2) {
    long i0 = i * index_ivf->ntotal / n;
    long i1 = (i + 1) * index_ivf->ntotal / n;

    if (verbose) printf("IndexShards shard %ld indices %ld:%ld\n", i, i0, i1);
    index_ivf->copy_subset_to(*idx2, 2, i0, i1);
    FAISS_ASSERT(idx2->ntotal == i1 - i0);
  } else if (shard_type == 1) {
    if (verbose)
      printf("IndexShards shard %ld select modulo %ld = %ld\n", i, n, i);
    index_ivf->copy_subset_to(*idx2, 1, n, i);
  } else {
    FAISS_THROW_FMT("shard_type %d not implemented", shard_type);
  }
}

Index *GammaToGpuClonerMultiple::clone_Index_to_shards(
    const GammaIVFPQIndex *index) {
  long n = sub_cloners.size();

  auto index_ivfpq = index;
  std::vector<faiss::Index *> shards(n);

  for (long i = 0; i < n; i++) {
    // make a shallow copy
    if (reserveVecs) sub_cloners[i].reserveVecs = (reserveVecs + n - 1) / n;

    if (index_ivfpq) {
      faiss::IndexIVFPQ idx2(index_ivfpq->quantizer, index_ivfpq->d,
                             index_ivfpq->nlist, index_ivfpq->code_size,
                             index_ivfpq->pq.nbits);
      idx2.metric_type = index_ivfpq->metric_type;
      idx2.pq = index_ivfpq->pq;
      idx2.nprobe = index_ivfpq->nprobe;
      idx2.use_precomputed_table = 0;
      idx2.is_trained = index->is_trained;
      copy_ivf_shard(index_ivfpq, &idx2, n, i);
      shards[i] = sub_cloners[i].clone_Index(&idx2);
    }
  }

  bool successive_ids = false;
  faiss::IndexShards *res =
      new faiss::IndexShards(index->d, true, successive_ids);

  for (int i = 0; i < n; i++) {
    res->add_shard(shards[i]);
  }
  res->own_fields = true;
  // FAISS_ASSERT(index->indexed_vec_count_ == res->ntotal);
  return res;
}

Index *GammaToGpuClonerMultiple::clone_Index(const GammaIVFPQIndex *index) {
  long n = sub_cloners.size();
  if (n == 1) return sub_cloners[0].clone_Index(index);

  if (!shard) {
    IndexReplicas *res = new IndexReplicas();
    for (auto &sub_cloner : sub_cloners) {
      res->addIndex(sub_cloner.clone_Index(index));
    }
    res->own_fields = true;
    return res;
  } else {
    return clone_Index_to_shards(index);
  }
}

faiss::Index *gamma_index_cpu_to_gpu_multiple(
    std::vector<StandardGpuResources *> &resources, std::vector<int> &devices,
    const GammaIVFPQIndex *index, const GpuMultipleClonerOptions *options) {
  GpuMultipleClonerOptions defaults;
  GammaToGpuClonerMultiple cl(resources, devices,
                              options ? *options : defaults);
  return cl.clone_Index(index);
}

}  // namespace gamma_gpu
}  // namespace tig_gamma
