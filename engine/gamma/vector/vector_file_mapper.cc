/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "vector_file_mapper.h"
#include "log.h"
#include "utils.h"
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace tig_gamma {

VectorFileMapper::VectorFileMapper(std::string file_path, int offset,
                                   int max_vector_size, int dimension)
    : file_path_(file_path), offset_(offset), max_vector_size_(max_vector_size),
      dimension_(dimension) {
  mapped_byte_size_ = (size_t)max_vector_size * dimension * sizeof(float) + offset;
  buf_ = nullptr;
  vectors_ = nullptr;
}

VectorFileMapper::~VectorFileMapper() {
  if (buf_ != nullptr) {
    int ret = munmap(buf_, mapped_byte_size_);
    if (ret != 0) {
      LOG(ERROR) << "munmap error, ret=" << ret;
    }
    buf_ = nullptr;
    vectors_ = nullptr;
  }
}

int VectorFileMapper::Init() {
  int fd = open(file_path_.c_str(), O_RDONLY, 0);
  if (-1 == fd) {
    LOG(ERROR) << "open vector file error, path=" << file_path_;
    return -1;
  }
  buf_ = mmap(NULL, mapped_byte_size_, PROT_READ, MAP_SHARED, fd, 0);
  close(fd);
  if (buf_ == MAP_FAILED) {
    LOG(ERROR) << "mmap error:" << strerror(errno);
    return -1;
  }
  vectors_ = (float *)((char *)buf_ + offset_);

  long file_size = utils::get_file_size(file_path_.c_str());
  mapped_num_ = (file_size - offset_) / (sizeof(float) * dimension_);

  int ret = madvise(static_cast<void *>(buf_), mapped_byte_size_, MADV_RANDOM);
  if (ret != 0) {
    LOG(ERROR) << "madvise error: " << ret;
    return -1;
  }
  LOG(INFO) << "map success! max byte size=" << mapped_byte_size_
            << ", file path=" << file_path_ << ", offset=" << offset_
            << ", mapped vector number=" << mapped_num_;
  return 0;
}

const float *VectorFileMapper::GetVector(int id) {
  if (id < 0 || id >= max_vector_size_)
    return nullptr;
  return vectors_ + ((long)id) * dimension_;
}

const float *VectorFileMapper::GetVectors() { return vectors_; }

} // namespace tig_gamma
