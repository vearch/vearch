/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifndef RAW_VECTOR_FACTORY_H_
#define RAW_VECTOR_FACTORY_H_

#include "memory_raw_vector.h"
#include "raw_vector.h"
#include <string>

namespace tig_gamma {

class RawVectorFactory {
public:
  static RawVector *Create(RawVectorType type, const std::string name,
                           int dimension, int max_doc_size) {
    switch (type) {
    case MemoryOnly:
      return (RawVector *)new MemoryRawVector(name, dimension, max_doc_size);
    default:
      throw std::invalid_argument("invalid raw feature type");
    }
  }
};
} // namespace tig_gamma

#endif // RAW_VECTOR_FACTORY_H_
