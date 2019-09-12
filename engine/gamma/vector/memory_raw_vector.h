/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifndef MEMORY_RAW_VECTOR_H_
#define MEMORY_RAW_VECTOR_H_

#include "raw_vector.h"

namespace tig_gamma {

class MemoryRawVector : public RawVector {
public:
  MemoryRawVector(const std::string name, int dimension, int max_doc_size);
  virtual ~MemoryRawVector();

  int Init() override;
  void Close() override;
  const float *GetVector(long vid) const override;
  int Add(int docid, Field *&field) override;
  const float *GetVectorHeader() override;
  int Gets(int k, long *ids_list,
           std::vector<const float *> &results) const override;
  int GetSource(int vid, char *&str, int &len) override;

  int Dump(const std::string &path, int dump_docid, int max_docid) override;
  int Load(const std::vector<std::string> &path) override;

private:
  float *vector_mem_; // vector memory
  char *str_mem_ptr_; // source memory

  std::vector<long> source_mem_pos_; // position of each source
};

} // namespace tig_gamma
#endif /* MEMORY_RAW_VECTOR_H_ */
