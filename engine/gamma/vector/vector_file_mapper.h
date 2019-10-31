/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifndef VECTOR_FILE_MAPPER_H_
#define VECTOR_FILE_MAPPER_H_
#include <string>
#include <sys/mman.h>

namespace tig_gamma {

class VectorFileMapper {
public:
  VectorFileMapper(std::string file_path, int offset, int max_vector_size,
                   int dimension);
  ~VectorFileMapper();
  int Init();
  const float *GetVector(int id);
  const float *GetVectors();
  int GetMappedNum() const { return mapped_num_; };

private:
  void *buf_;
  float *vectors_;
  std::string file_path_;
  int offset_;
  int max_vector_size_;
  int dimension_;
  size_t mapped_byte_size_;
  int mapped_num_;
};

} // namespace tig_gamma

#endif
