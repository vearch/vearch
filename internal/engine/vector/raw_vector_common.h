/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string.h>

#include "util/log.h"
#include "util/utils.h"

const static int MAX_VECTOR_NUM_PER_DOC = 10;
const static int MAX_CACHE_SIZE = 1024 * 1024;  // M bytes, it is equal to 1T
const static int kMaxSegments = 10000;

class ScopeVector {
 public:
  explicit ScopeVector(uint8_t *ptr = nullptr) : ptr_(ptr) {}

  ~ScopeVector() {
    if (deletable_ && ptr_) delete[] ptr_;
  }

  const uint8_t *&Get() { return ptr_; }

  void Set(const uint8_t *ptr_in, bool deletable = true) {
    // ptr_ = const_cast<uint8_t *>(ptr_in);
    ptr_ = ptr_in;
    deletable_ = deletable;
  }

  bool &Deletable() { return deletable_; }

 public:
  const uint8_t *ptr_;
  bool deletable_;
};
