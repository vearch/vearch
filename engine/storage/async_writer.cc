/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "async_writer.h"

#include <unistd.h>

#include "util/log.h"

namespace tig_gamma {
namespace disk_io {

AsyncWriter::AsyncWriter() {
  running_ = true;
  writer_q_ = nullptr;
  header_size_ = 0;
  item_length_ = 0;
}

AsyncWriter::~AsyncWriter() {
  Sync();
  running_ = false;
  handler_thread_.join();
  if (writer_q_) {
    delete writer_q_;
    writer_q_ = nullptr;
  }
}

int AsyncWriter::Init() {
  writer_q_ = new WriterQueue;
  if (writer_q_ == nullptr) {
    LOG(ERROR) << "AsyncWriter init failed.";
    return -1;
  }
  auto func_operate = std::bind(&AsyncWriter::WriterHandler, this);
  handler_thread_ = std::thread(func_operate);
  return 0;
}

static void UpdateSize(int fd, std::atomic<uint32_t> *cur_size, int num) {
  uint32_t size = *cur_size + num;
  pwrite(fd, &size, sizeof(size), sizeof(uint8_t) + sizeof(uint32_t));
  *cur_size = size;
}

int AsyncWriter::WriterHandler() {
  int bulk_size = 1000;
  size_t bulk_bytes = 1 * 1024 * 1024;  // TODO check overflow
  uint8_t *buffer = new uint8_t[bulk_bytes];

  while (running_) {
    struct WriterStruct *writer_structs[bulk_size];

    int size = 0;
    while (not writer_q_->empty() && size < bulk_size) {
      struct WriterStruct *pop_val = nullptr;
      bool ret = writer_q_->try_pop(pop_val);
      if (ret) writer_structs[size++] = pop_val;
    }

    if (size == 1) {
      pwrite(writer_structs[0]->fd, writer_structs[0]->data,
             writer_structs[0]->len, writer_structs[0]->start);
      UpdateSize(writer_structs[0]->fd, writer_structs[0]->cur_size,
                 writer_structs[0]->len / item_length_);
      delete[] writer_structs[0]->data;
      delete writer_structs[0];
    } else if (size > 1) {
      int prev_fd = writer_structs[0]->fd;
      uint32_t buffered_size = writer_structs[0]->len;
      uint32_t buffered_start = writer_structs[0]->start;
      std::atomic<uint32_t> *prev_cur_size = writer_structs[0]->cur_size;
      memcpy(buffer + 0, writer_structs[0]->data, buffered_size);
      delete[] writer_structs[0]->data;
      delete writer_structs[0];

      for (int i = 1; i < size; ++i) {
        int fd = writer_structs[i]->fd;
        uint8_t *data = writer_structs[i]->data;
        uint32_t len = writer_structs[i]->len;
        uint32_t start = writer_structs[i]->start;
        std::atomic<uint32_t> *cur_size = writer_structs[i]->cur_size;

        if (prev_fd != fd || buffered_size + len >= bulk_bytes) {
          // flush prev data
          pwrite(prev_fd, buffer, buffered_size, buffered_start);
          UpdateSize(prev_fd, prev_cur_size, buffered_size / item_length_);
          prev_fd = fd;
          buffered_start = start;
          prev_cur_size = cur_size;
          // TODO check buffered_size + len < bulk_bytes
          memcpy(buffer + 0, data, len);
          buffered_size = len;
        } else {
          memcpy(buffer + buffered_size, data, len);
          buffered_size += len;
        }

        delete[] data;
        delete writer_structs[i];
      }
      pwrite(prev_fd, buffer, buffered_size, buffered_start);
      UpdateSize(prev_fd, prev_cur_size, buffered_size / item_length_);
    } else {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
  }
  delete[] buffer;
  return 0;
}

int AsyncWriter::AsyncWrite(struct WriterStruct *writer_struct) {
  auto qu_size = writer_q_->size();
  while (qu_size > 10000) {
    LOG(INFO) << "AsyncWriter queue size[" << qu_size
              << "] > 10000, sleep 10ms";
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    qu_size = writer_q_->size();
  }
  writer_q_->push(writer_struct);
  return 0;
}

int AsyncWriter::SyncWrite(struct WriterStruct *writer_struct) {
  int fd = writer_struct->fd;
  uint8_t *data = writer_struct->data;
  uint32_t start = writer_struct->start;
  uint32_t len = writer_struct->len;
  std::atomic<uint32_t> *cur_size = writer_struct->cur_size;

  pwrite(fd, data, len, start);
  UpdateSize(fd, cur_size, len / item_length_);

  delete[] data;
  delete writer_struct;
  return 0;
}

int AsyncWriter::Sync() {
  while (writer_q_->size()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return 0;
}

}  // namespace disk_io
}  // namespace tig_gamma
