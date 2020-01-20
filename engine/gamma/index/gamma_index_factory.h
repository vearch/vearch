/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifndef GAMMA_INDEX_FACTORY_H_
#define GAMMA_INDEX_FACTORY_H_

#include "gamma_common_data.h"
#include "gamma_index_ivfpq.h"
#ifdef BUILD_GPU
#include "gamma_index_ivfpq_gpu.h"
#endif
#include "raw_vector.h"

#include "faiss/IndexFlat.h"

namespace tig_gamma {

class GammaIndexFactory {
 public:
  static GammaIndex *Create(RetrievalModel model, size_t dimension,
                            const char *docids_bitmap, RawVector *raw_vec,
                            IVFPQParameters *ivfpq_param) {
    if (docids_bitmap == nullptr) {
      LOG(ERROR) << "docids_bitmap is NULL!";
      return nullptr;
    }
    switch (model) {
      case IVFPQ: {
        if (dimension % ivfpq_param->nsubvector != 0) {
          dimension = (dimension / ivfpq_param->nsubvector + 1) *
                      ivfpq_param->nsubvector;
          LOG(INFO) << "Dimension [" << raw_vec->GetDimension()
                    << "] cannot divide by nsubvector ["
                    << ivfpq_param->nsubvector << "], adjusted to ["
                    << dimension << "]";
        }

        faiss::IndexFlatL2 *coarse_quantizer =
            new faiss::IndexFlatL2(dimension);

        return (GammaIndex *)new GammaIVFPQIndex(
            coarse_quantizer, dimension, ivfpq_param->ncentroids,
            ivfpq_param->nsubvector, ivfpq_param->nbits_per_idx, docids_bitmap,
            raw_vec, ivfpq_param->nprobe);
        break;
      }
#ifdef BUILD_GPU
      case GPU_IVFPQ: {
        gamma_gpu::GammaIVFPQGPUIndex *gpu_index =
            new gamma_gpu::GammaIVFPQGPUIndex(
                dimension, ivfpq_param->ncentroids, 32, 8, docids_bitmap,
                raw_vec, ivfpq_param->nprobe);
        return (GammaIndex *)gpu_index;
        break;
      }
#endif

      default: {
        throw std::invalid_argument("invalid raw feature type");
        break;
      }
    }

    return nullptr;
  }
};
}  // namespace tig_gamma

#endif  // GAMMA_INDEX_FACTORY_H_
