/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <stdint.h>
#include <tbb/concurrent_queue.h>

#include <atomic>

#include "concurrentqueue/blockingconcurrentqueue.h"

namespace tig_gamma {
namespace disk_io {

struct WriterStruct {
  WriterStruct() {}
  int fd;
  uint8_t *data;
  uint32_t start;
  uint32_t len;
  std::atomic<uint32_t> *cur_size;
};

// typedef moodycamel::BlockingConcurrentQueue<struct WriterStruct *>
// WriterQueue;
typedef tbb::concurrent_bounded_queue<struct WriterStruct *> WriterQueue;

class AsyncWriter {
 public:
  AsyncWriter();
  ~AsyncWriter();

  int Init();

  int AsyncWrite(struct WriterStruct *writer_struct);

  int SyncWrite(struct WriterStruct *writer_struct);

  int Sync();

  void Set(uint32_t header_size, int item_length) {
    header_size_ = header_size;
    item_length_ = item_length;
  }

 private:
  int WriterHandler();

  WriterQueue *writer_q_;

  bool running_;
  std::thread handler_thread_;

  uint32_t header_size_;
  int item_length_;
};

}  // namespace disk_io
}  // namespace tig_gamma
