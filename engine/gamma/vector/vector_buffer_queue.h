/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifndef VECTOR_BUFFER_QUEUER_H_
#define VECTOR_BUFFER_QUEUER_H_

#include <pthread.h>
#include <cstdint>
#include <string>

class VectorBufferQueue {
 public:
  /**
   * @param capacity the max memory buffer size, the smallest unit is million(M)
   * bytes, for example: capacity=100, it means 100M bytes
   * @param dimension the dimension for each vector, for example: dimemison=1024
   * @return
   */
  VectorBufferQueue(int max_vector_size, int dimension, int chunk_num);
  ~VectorBufferQueue();

  /**
   * malloc memory, init variables
   * @return 0 success; 1 parameter error; 2 malloc memory error;
   */
  int Init();

  /**
   * push one vector to queue
   * @param v the float array of vector
   * @param dim the dimension of vector, it must be equal to dimension of
   * constructor
   * @param timeout the timeout of waiting enough space to store this vector, -1
   * means waiting forever, the smallest unit is millisecond(ms), for example:
   * timeout=100, it means to waiting 100ms
   * @return 0 success; 1 parameter error; 3 timeout
   */
  int Push(const float *v, int dim, int timeout);

  /**
   * push multiple vector to queue
   * @param v the float array of all multiple vector
   * @param dim the dimension of each vector, it must be equal to dimension of
   * constructor
   * @num the number of vector
   * @param timeout the timeout of waiting enough space to store this vector, -1
   * means waiting forever, the smallest unit is millisecond(ms), for example:
   * timeout=100, it means to waiting 100ms
   * @return 0 success; 1 parameter error; 3 timeout
   */
  int Push(const float *v, int dim, int num, int timeout);  // batch push

  /**
   * pop one vector from queue
   * @param v the float array to store vector
   * @param dim the dimension of vector, it must be equal to dimension of
   * constructor
   * @param timeout the timeout of waiting enough vector to poll from the queue,
   * -1 means waiting forever, the smallest unit is millisecond(ms), for
   * example: timeout=100, it means to waiting 100ms
   * @return 0 success; 1 parameter error; 3 timeout
   */
  int Pop(float *v, int dim, int timeout);

  /**
   * pop multiple vector from queue
   * @param v the float array to store multiple vector
   * @param dim the dimension of each vector, it is equal to dimension of
   * constructor
   * @num the number of vector to poll
   * @param timeout the timeout of waiting enough vector to poll from the queue,
   * -1 means waiting forever, the smallest unit is millisecond(ms), for
   * example: timeout=100, it means to waiting 100ms
   * @return 0 success; 1 parameter error; 3 timeout
   */
  int Pop(float *v, int dim, int num, int timeout);  // batch pop

  int GetVector(int id, float *v, int dim);
  int Size() const;
  int GetPopSize() const;
  long GetTotalMemBytes() { return total_mem_bytes_; }
  void Erase() { pop_index_ = push_index_; }

 private:
  bool WaitFor(int timeout, int type, int num);

 private:
  float *buffer_;
  int max_vector_size_;
  int chunk_num_;
  int chunk_size_;
  int dimension_;
  std::uint64_t pop_index_;
  std::uint64_t push_index_;
  int vector_byte_size_;
  pthread_rwlock_t *shared_mutexes_;
  long total_mem_bytes_;
  int stored_num_;
};

#endif
