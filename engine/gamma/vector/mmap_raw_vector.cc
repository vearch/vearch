/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "mmap_raw_vector.h"
#include "log.h"
#include "utils.h"
#include <errno.h>
#include <exception>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

using namespace std;

namespace tig_gamma {

int MmapRawVector::FlushOnce() {
  int psize = vector_buffer_queue_->GetPopSize();
  int count = 0;
  while (count < psize) {
    int num =
        psize - count > flush_batch_size_ ? flush_batch_size_ : psize - count;
    vector_buffer_queue_->Pop(flush_batch_vectors_, dimension_, num, -1);
    ssize_t write_size = (ssize_t)num * vector_byte_size_;
    ssize_t ret = utils::write_n(fet_fd_, (char *)flush_batch_vectors_,
                                 write_size, flush_write_retry_);
    if (ret != write_size) {
      LOG(ERROR) << "write_n error:" << strerror(errno) << ", num=" << num;
      // TODO: truncate and seek file, or write the success number to file
      return -2;
    }
    raw_vector_io_->Dump(nflushed_ + count, num);
    count += num;
  }
  return psize;
}

MmapRawVector::MmapRawVector(const string &name, int dimension,
                             int max_vector_size, const string &root_path,
                             const StoreParams &store_params)
    : RawVector(name, dimension, max_vector_size, root_path),
      AsyncFlusher(name) {
  flush_batch_size_ = 1000;
  init_vector_num_ = 0;
  vector_byte_size_ = sizeof(float) * dimension;
  flush_write_retry_ = 10;
  buffer_chunk_num_ = 1024;
  fet_file_path_ = root_path + "/" + name + ".fet";
  fet_fd_ = -1;
  raw_vector_io_ = nullptr;
  store_params_ = new StoreParams(store_params);
}

MmapRawVector::~MmapRawVector() {
  if (vector_buffer_queue_ != nullptr) {
    delete vector_buffer_queue_;
  }

  if (vector_file_mapper_ != nullptr) {
    delete vector_file_mapper_;
  }

  if (flush_batch_vectors_ != nullptr) {
    delete[] flush_batch_vectors_;
  }
  if (raw_vector_io_)
    delete raw_vector_io_;
  if (fet_fd_ != -1)
    close(fet_fd_);
  if (store_params_)
    delete store_params_;
}

int MmapRawVector::Init() {
  max_buffer_size_ = (int)((long)store_params_->cache_size_ * 1024 * 1024 / vector_byte_size_);

  fet_fd_ = open(fet_file_path_.c_str(), O_WRONLY | O_APPEND | O_CREAT, 00664);
  if (fet_fd_ == -1) {
    LOG(ERROR) << "open file error:" << strerror(errno);
    return -1;
  }

  raw_vector_io_ = new RawVectorIO(this);
  if (raw_vector_io_->Init(true)) {
    LOG(ERROR) << "init raw vector io error";
    return -1;
  }

  vector_buffer_queue_ =
      new VectorBufferQueue(max_buffer_size_, dimension_, buffer_chunk_num_);
  vector_file_mapper_ =
      new VectorFileMapper(fet_file_path_, 0, max_vector_size_, dimension_);

  int ret = vector_buffer_queue_->Init(fet_file_path_);
  if (0 != ret) {
    LOG(ERROR) << "init vector buffer queue error, ret=" << ret;
    return -1;
  }
  total_mem_bytes_ += vector_buffer_queue_->GetTotalMemBytes();

  flush_batch_vectors_ = new float[(uint64_t)flush_batch_size_ * dimension_];
  total_mem_bytes_ += (uint64_t)flush_batch_size_ * dimension_ * sizeof(float);

  ret = vector_file_mapper_->Init();
  if (0 != ret) {
    LOG(ERROR) << "vector file mapper map error, ret=" << ret;
    return -1;
  }
  nflushed_ = vector_file_mapper_->GetMappedNum();
  ntotal_ = nflushed_;

  LOG(INFO) << "init success! vector byte size=" << vector_byte_size_
            << ", flush batch size=" << flush_batch_size_
            << ", dimension=" << dimension_;
  return 0;
}

int MmapRawVector::AddToStore(float *v, int len) {
  return vector_buffer_queue_->Push(v, len, -1);
}

const float *MmapRawVector::GetVectorHeader(int start, int end) {
  if (end > ntotal_)
    return nullptr;
  Until(end);
  return vector_file_mapper_->GetVectors() + (uint64_t)start * dimension_;
}

const float *MmapRawVector::GetVector(long vid) const {
  if (vid >= ntotal_ || vid < 0) {
    return nullptr;
  };

  float *vector = new float[dimension_];
  if (vector_buffer_queue_->GetVector(vid, vector, dimension_) == 0) {
    return vector;
  }
  const float *fea = vector_file_mapper_->GetVector(vid);
  memcpy((void *)vector, (void *)fea, vector_byte_size_);
  return vector;
}

void MmapRawVector::Destroy(std::vector<const float *> &results) {
  for (const float *p : results) {
    delete[] p;
  }
}

void MmapRawVector::Destroy(const float *result, bool header) {
  if (header)
    return;
  delete[] result;
}

} // namespace tig_gamma
