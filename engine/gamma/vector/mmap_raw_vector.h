/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifndef MEMORY_DISK_RAW_VECTOR_H_
#define MEMORY_DISK_RAW_VECTOR_H_

#include "raw_vector.h"
#include "vector_buffer_queue.h"
#include "vector_file_mapper.h"
#include <string>
#include <thread>

namespace tig_gamma {

class MmapRawVector : public RawVector, public AsyncFlusher {
public:
  MmapRawVector(const std::string &name, int dimension, int max_vector_size,
                const std::string &root_path, const StoreParams &store_params);
  ~MmapRawVector();
  int Init() override; // malloc memory and mmap file, if file is not existed,
                       // create it
  const float *GetVector(long vid) const override;
  int AddToStore(float *v, int len) override;
  const float *GetVectorHeader(int start, int end) override;
  void Destroy(std::vector<const float *> &results) override;
  void Destroy(const float *result, bool header = false) override;

protected:
  int FlushOnce() override;

private:
  VectorBufferQueue *vector_buffer_queue_;
  VectorFileMapper *vector_file_mapper_;
  int max_buffer_size_;
  int buffer_chunk_num_;
  int flush_batch_size_;
  int flush_write_retry_;
  int init_vector_num_;
  float *flush_batch_vectors_;
  std::string fet_file_path_;
  int fet_fd_;
  RawVectorIO *raw_vector_io_;
  StoreParams *store_params_;
};

} // namespace tig_gamma

#endif
