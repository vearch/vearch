/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "vector_buffer_queue.h"
#include "log.h"
#include "thread_util.h"
#include "utils.h"
#include <cassert>
#include <iostream>
#include <stdexcept>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

using namespace std;

VectorBufferQueue::VectorBufferQueue(int max_vector_size, int dimension,
                                     int chunk_num) {
  max_vector_size_ = max_vector_size;
  dimension_ = dimension;
  chunk_num_ = chunk_num;
  pop_index_ = 0;
  push_index_ = 0;
  total_mem_bytes_ = 0;
  stored_num_ = 0;
}

VectorBufferQueue::~VectorBufferQueue() {
  if (buffer_ != NULL) {
    free(buffer_);
    buffer_ = nullptr;
  }
  if (shared_mutexes_ != nullptr) {
    for (int i = 0; i < chunk_num_; i++) {
      int ret = pthread_rwlock_destroy(&shared_mutexes_[i]);
      if (0 != ret) {
        LOG(ERROR) << "destory read write lock error, ret=" << ret;
      }
    }
    delete[] shared_mutexes_;
    shared_mutexes_ = nullptr;
  }
}

int VectorBufferQueue::Init() {
  if (dimension_ <= 0) {
    return 1;
  }
  if (max_vector_size_ % chunk_num_ != 0) {
    LOG(ERROR) << "max_vector_size(" << max_vector_size_ << ") % chunk_num("
               << chunk_num_ << ") != 0";
    return 1;
  }
  chunk_size_ = max_vector_size_ / chunk_num_;

  vector_byte_size_ = sizeof(float) * dimension_;
  buffer_ = (float *)calloc(max_vector_size_, vector_byte_size_);
  if (buffer_ == NULL) {
    cerr << "malloc buffer failed" << endl;
    return 2;
  }
  total_mem_bytes_ += (long)max_vector_size_ * vector_byte_size_;

  shared_mutexes_ = new pthread_rwlock_t[chunk_num_];
  for (int i = 0; i < chunk_num_; i++) {
    int ret = pthread_rwlock_init(&shared_mutexes_[i], NULL);
    if (ret != 0) {
      LOG(ERROR) << "init read-write lock error, ret=" << ret << ", i=" << i;
      return 2;
    }
  }
  LOG(INFO) << "vector buffer queue init success! buffer byte size="
            << (long)max_vector_size_ * vector_byte_size_
            << ", buffer vector size=" << max_vector_size_
            << ", chunk number=" << chunk_num_
            << ", stored number=" << stored_num_;
  return 0;
}
int VectorBufferQueue::Push(const float *v, int dim, int timeout) {
  if (v == NULL || dim != dimension_)
    return 1;

  if (!WaitFor(timeout, 1, 1)) {
    return 3; // timeout
  }
  int chunk_id = push_index_ / chunk_size_ % chunk_num_;
  WriteThreadLock write_lock(shared_mutexes_[chunk_id]);

  memcpy((void *)(buffer_ + push_index_ % max_vector_size_ * dimension_),
         (void *)v, vector_byte_size_);
  push_index_++;
  return 0;
}

int VectorBufferQueue::Push(const float *v, int dim, int num, int timeout) {
  if (v == NULL || dim != dimension_ || num <= 0)
    return 1;

  if (!WaitFor(timeout, 1, num)) {
    return 3; // timeout
  }

  do {
    int offset = push_index_ % chunk_size_;
    int batch_size = offset + num > chunk_size_ ? chunk_size_ - offset : num;
    int chunk_id = push_index_ / chunk_size_ % chunk_num_;
    WriteThreadLock *write_lock =
        new WriteThreadLock(shared_mutexes_[chunk_id]);
    memcpy((void *)(buffer_ + push_index_ % max_vector_size_ * dimension_),
           (void *)v, (long)vector_byte_size_ * batch_size);
    push_index_ += batch_size;
    delete write_lock;
    num -= batch_size;
    v += (long)dimension_ * batch_size;
  } while (num > 0);
  return 0;
}

int VectorBufferQueue::Pop(float *v, int dim, int timeout) {
  if (v == nullptr || dim != dimension_)
    return 1;

  if (!WaitFor(timeout, 2, 1)) {
    return 3; // timeout
  }

  uint64_t offset = pop_index_ % max_vector_size_ * dimension_;
  memcpy((void *)v, (void *)(buffer_ + offset), vector_byte_size_);
  pop_index_++;
  return 0;
}
int VectorBufferQueue::Pop(float *v, int dim, int num, int timeout) {
  if (v == nullptr || dim != dimension_ || num <= 0 || num > max_vector_size_)
    return 1;

  if (!WaitFor(timeout, 2, num)) {
    return 3; // timeout
  }
  int offset = pop_index_ % max_vector_size_;
  // the remain vectors in last chunk are less than num
  int batch_size =
      max_vector_size_ - offset > num ? num : max_vector_size_ - offset;
  memcpy((void *)v, (void *)(buffer_ + (uint64_t)offset * dimension_),
         (long)vector_byte_size_ * batch_size);
  v += (long)batch_size * dimension_;
  batch_size = num - batch_size;
  // read from the first of buffer
  if (batch_size > 0) {
    memcpy((void *)v, (void *)buffer_, (long)vector_byte_size_ * batch_size);
  }
  pop_index_ += num;
  return 0;
}

int VectorBufferQueue::GetVector(int id, float *v, int dim) {
  if (v == nullptr || dim != dimension_)
    return 1;
  int id_in_queue = id;
  int chunk_id = id_in_queue / chunk_size_ % chunk_num_;
  ReadThreadLock read_lock(shared_mutexes_[chunk_id]);
  // the vector isn't in buffer
  if ((uint64_t)id_in_queue >= push_index_ ||
      push_index_ - id_in_queue > (uint64_t)max_vector_size_) {
    return 4;
  }
  long offset = (long)id_in_queue % max_vector_size_ * dimension_;
  memcpy((void *)v, (void *)(buffer_ + offset), vector_byte_size_);
  return 0;
}

int VectorBufferQueue::GetPopSize() const { return push_index_ - pop_index_; }
int VectorBufferQueue::Size() const {
  uint64_t idx = push_index_;
  return idx > (uint64_t)max_vector_size_ ? max_vector_size_ : idx;
}

bool VectorBufferQueue::WaitFor(int timeout, int type, int num) {
  int cost = 0;
  while (timeout == -1 || cost < timeout) {
    bool status = false;
    switch (type) {
    case 1: // if it can add num vector
      status =
          max_vector_size_ - (push_index_ - pop_index_) >= (std::uint64_t)num;
      break;
    case 2: // if it can poll num vector
      status = push_index_ - pop_index_ >= (std::uint64_t)num;
      break;
    default:
      throw std::invalid_argument("invalid type=" + std::to_string(type));
    }
    if (status)
      return true;
    usleep(100000); // wait 100ms
    cost += 100;
  }
  return false;
}
