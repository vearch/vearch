/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "mmap_raw_vector.h"
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <exception>
#include "log.h"
#include "utils.h"

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
  buffer_chunk_num_ = kDefaultBufferChunkNum;
  fet_file_path_ = root_path + "/" + name + ".fet";
  fet_fd_ = -1;
  store_params_ = new StoreParams(store_params);
  stored_num_ = 0;
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
  if (fet_fd_ != -1) close(fet_fd_);
  if (store_params_) delete store_params_;
}

int MmapRawVector::Init() {
  max_buffer_size_ = (int)(store_params_->cache_size_ / vector_byte_size_);

  fet_fd_ = open(fet_file_path_.c_str(), O_WRONLY | O_APPEND | O_CREAT, 00664);
  if (fet_fd_ == -1) {
    LOG(ERROR) << "open file error:" << strerror(errno);
    return -1;
  }

  int remainder = max_buffer_size_ % buffer_chunk_num_;
  if (remainder > 0) {
    max_buffer_size_ += buffer_chunk_num_ - remainder;
  }
  vector_buffer_queue_ =
      new VectorBufferQueue(max_buffer_size_, dimension_, buffer_chunk_num_);
  vector_file_mapper_ =
      new VectorFileMapper(fet_file_path_, 0, max_vector_size_, dimension_);

  int ret = vector_buffer_queue_->Init();
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

  LOG(INFO) << "init success! vector byte size=" << vector_byte_size_
            << ", flush batch size=" << flush_batch_size_
            << ", dimension=" << dimension_ << ", ntotal=" << ntotal_;
  return 0;
}

int MmapRawVector::DumpVectors(int dump_vid, int n) {
  int dump_end = dump_vid + n;
  while (nflushed_ < dump_end) {
    LOG(INFO) << "raw vector=" << vector_name_
              << ", dump waiting! dump_end=" << dump_end
              << ", nflushed=" << nflushed_;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  return 0;
}

int MmapRawVector::LoadVectors(int vec_num) {
  StopFlushingIfNeed(this);
  long file_size = utils::get_file_size(fet_file_path_.c_str());
  if (file_size % vector_byte_size_ != 0) {
    LOG(ERROR) << "file_size % vector_byte_size_ != 0, path=" << fet_file_path_;
    return -1;
  }
  long disk_vector_num = file_size / vector_byte_size_;
  LOG(INFO) << "disk_vector_num=" << disk_vector_num << ", vec_num=" << vec_num;
  assert(disk_vector_num >= vec_num);
  if (disk_vector_num > vec_num) {
    // release file
    if (fet_fd_ != -1) close(fet_fd_);
    if (vector_file_mapper_) delete vector_file_mapper_;

    long trunc_size = (long)vec_num * vector_byte_size_;
    if (truncate(fet_file_path_.c_str(), trunc_size)) {
      LOG(ERROR) << "truncate feature file=" << fet_file_path_ << " to "
                 << trunc_size << ", error:" << strerror(errno);
      return -1;
    }
    fet_fd_ =
        open(fet_file_path_.c_str(), O_WRONLY | O_APPEND | O_CREAT, 00664);
    if (fet_fd_ == -1) {
      LOG(ERROR) << "open file error:" << strerror(errno);
      return -1;
    }
    vector_file_mapper_ =
        new VectorFileMapper(fet_file_path_, 0, max_vector_size_, dimension_);
    if (vector_file_mapper_->Init()) {
      LOG(ERROR) << "vector file mapper map error";
      return -1;
    }
    disk_vector_num = vec_num;
  }

  // read vectors from fet file to vector buffer queue
  if (vec_num > 0) {
    long offset = vec_num > max_buffer_size_
                      ? (long)(vec_num - max_buffer_size_) * vector_byte_size_
                      : 0;
    FILE *fet_fp = fopen(fet_file_path_.c_str(), "rb");
    if (fet_fp == NULL) {
      LOG(ERROR) << "open feature file error, file path=" << fet_file_path_;
      return 1;
    }
    if (0 != fseek(fet_fp, offset, SEEK_SET)) {
      LOG(ERROR) << "fseek feature file error, file path=" << fet_file_path_
                 << ", offset=" << offset;
      fclose(fet_fp);
      return 2;
    }
    int load_num = vec_num > max_buffer_size_ ? max_buffer_size_ : vec_num;
    stored_num_ = vec_num > max_buffer_size_ ? vec_num - max_buffer_size_ : 0;
    int batch = 1000;
    float *buffer = new float[batch * dimension_];
    int times = load_num / batch;
    for (int i = 0; i < times; i++) {
      fread(buffer, vector_byte_size_, batch, fet_fp);
      vector_buffer_queue_->Push(buffer, dimension_, batch, -1);
    }
    int remainder = load_num % batch;
    if (remainder > 0) {
      fread(buffer, vector_byte_size_, remainder, fet_fp);
      vector_buffer_queue_->Push(buffer, dimension_, remainder, -1);
    }
    delete[] buffer;
    fclose(fet_fp);

    vector_buffer_queue_->Erase();
  }

  nflushed_ = disk_vector_num;
  last_nflushed_ = nflushed_;

  LOG(INFO) << "load vectors success, nflushed=" << nflushed_;

  StartFlushingIfNeed(this);
  return 0;
}

int MmapRawVector::AddToStore(float *v, int len) {
  return vector_buffer_queue_->Push(v, len, -1);
}

int MmapRawVector::GetVectorHeader(int start, int end, ScopeVector &vec) {
  if (end > ntotal_) return 1;
  Until(end);
  vec.Set(vector_file_mapper_->GetVectors() + (uint64_t)start * dimension_,
          false);
  return 0;
}

int MmapRawVector::GetVector(long vid, const float *&vec,
                             bool &deletable) const {
  if (vid >= ntotal_ || vid < 0) {
    return 1;
  };

  // int stored_num = ntotal_ - vector_buffer_queue_->size();
  if (vid >= stored_num_) {
    float *vector = new float[dimension_];
    if (vector_buffer_queue_->GetVector(vid - stored_num_, vector,
                                        dimension_) == 0) {
      vec = vector;
      deletable = true;
      return 0;
    }
    delete[] vector;
  }
  const float *fea = vector_file_mapper_->GetVector(vid);
  vec = fea;
  deletable = false;
  return 0;
}

}  // namespace tig_gamma
