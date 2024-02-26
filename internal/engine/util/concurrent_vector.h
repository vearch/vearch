/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <string>

#include "util/log.h"

/**
 * It is thread-safe for single write and concurrent read.
 * Value is base type(int, char, ...).
 */

namespace tig_gamma {

template <typename Base, typename Value>
class ConcurrentVector {
 public:
  ConcurrentVector() {
    name_ = "";
    begin_grp_capacity_ = 0;
    grp_capacity_ = 0;
    grp_gap_ = 0;
    grp_num_ = 0;
    size_ = 0;
    grps_ = nullptr;
  }

  ~ConcurrentVector() {
    for (int i = 0; i < grp_num_; ++i) {
      if (grps_[i]) {
        delete[] grps_[i];
        grps_[i] = nullptr;
      }
    }
    grp_num_ = 0;
    if (grps_) {
      delete[] grps_;
      grps_ = nullptr;
    }
  }

  bool Init(std::string name, Base begin_grp_capacity, Base grp_gap) {
    name_ = name;
    begin_grp_capacity_ = begin_grp_capacity;
    grp_capacity_ = begin_grp_capacity;
    grp_gap_ = grp_gap;

    grps_ = new Value *[begin_grp_capacity_];
    if (grps_ == nullptr) {
      LOG(ERROR) << "ConcurrentVector[" << name_ << "], new Value*["
                 << begin_grp_capacity_ << "] fail.";
      return false;
    }
    return true;
  }

  bool PushBack(Value val) {
    if (size_ % grp_gap_ == 0) {
      if (grp_num_ >= grp_capacity_) {
        Base new_capacitye = grp_capacity_ + begin_grp_capacity_;
        Value **new_grps = new Value *[new_capacitye];
        Value **del_grps = grps_;
        if (new_grps == nullptr) {
          LOG(ERROR) << "ConcurrentVector[" << name_ << "], new Value*["
                     << new_capacitye << "] fail.";
          return false;
        }
        memset(new_grps + grp_capacity_, 0,
               sizeof(Value *) * begin_grp_capacity_);
        memcpy(new_grps, grps_, grp_capacity_ * sizeof(Value *));
        grps_ = new_grps;
        grp_capacity_ = new_capacitye;
        delete[] del_grps;
        LOG(INFO) << "ConcurrentVector[" << name_ << "] is full."
                  << "grp_capacity extend to " << grp_capacity_;
      }

      grps_[grp_num_] = new Value[grp_gap_];
      if (not grps_[grp_num_]) {
        LOG(ERROR) << "ConcurrentVector[" << name_ << "], new Value["
                   << grp_gap_ << "] fail.";
        return false;
      }
      memset(grps_[grp_num_], 0, grp_gap_ * sizeof(Value));
      grps_[grp_num_][0] = val;
      ++grp_num_;
      ++size_;
    } else {
      grps_[grp_num_ - 1][size_ % grp_gap_] = val;
      ++size_;
    }
    return true;
  }

  bool ResetData(uint32_t id, Value val) {
    if (id >= size_) {
      LOG(ERROR) << "ConcurrentVector[" << name_ << "], size[" << size_
                 << "], id[" << id << "] is out of bounds";
      return false;
    }
    Value *grp = grps_[id / grp_gap_];
    grp[id % grp_gap_] = val;
    return true;
  }

  Value GetData(uint32_t id) {
    if (id / grp_gap_ >= grp_num_) {
      LOG(ERROR) << "ConcurrentVector[" << name_ << "], id[" << id
                 << "] is out of bounds";
      return 0;
    }
    Value *grp = grps_[id / grp_gap_];
    return grp[id % grp_gap_];
  }

  bool GetData(uint32_t id, Value &value) {
    if (id >= size_) {
      LOG(ERROR) << "ConcurrentVector[" << name_ << "], id[" << id
                 << "] >= size[" << size_ << "]";
      return false;
    }
    Value *grp = grps_[id / grp_gap_];
    value = grp[id % grp_gap_];
    return true;
  }

  bool GetLastData(Value &value) {
    if (grp_num_ == 0) {
      LOG(WARNING) << "ConcurrentVector[" << name_
                   << "] is empty, GetLastData failed.";
      return false;
    }
    Value *grp = grps_[(size_ - 1) / grp_gap_];
    value = grp[(size_ - 1) % grp_gap_];
    return true;
  }

  bool Resize(uint32_t size) {
    uint32_t new_grp_num = size / grp_gap_ + (size % grp_gap_ ? 1 : 0);
    if (new_grp_num <= grp_num_) {
      uint32_t old_grp_num = grp_num_;
      size_ = size;
      grp_num_ = new_grp_num;
      if (size_ % grp_gap_) {
        memset(grps_[grp_num_ - 1] + (size_ % grp_gap_), 0,
               sizeof(Value *) * (grp_gap_ - size_ % grp_gap_));
      }
      for (uint32_t j = grp_num_; j < old_grp_num; ++j) {
        delete[] grps_[j];
        grps_[j] = nullptr;
      }
    } else {
      if (new_grp_num > grp_capacity_) {
        Base new_capacitye = new_grp_num / begin_grp_capacity_ +
                             (new_grp_num % begin_grp_capacity_ ? 1 : 0);
        Value **new_grps = new Value *[new_capacitye];
        Value **del_grps = grps_;
        if (new_grps == nullptr) {
          LOG(ERROR) << "ConcurrentVector[" << name_ << "], new Value*["
                     << new_capacitye << "] fail.";
          return false;
        }
        memset(new_grps, 0, sizeof(Value *) * new_capacitye);
        memcpy(new_grps, grps_, grp_capacity_ * sizeof(Value *));
        grps_ = new_grps;
        grp_capacity_ = new_capacitye;
        delete[] del_grps;
        LOG(INFO) << "ConcurrentVector[" << name_ << "] is full."
                  << "grp_capacity extend to " << grp_capacity_;
      }

      uint32_t extend_grp_num = new_grp_num - grp_num_;
      for (uint32_t i = 0; i < extend_grp_num; ++i) {
        grps_[grp_num_] = new Value[grp_gap_];
        if (not grps_[grp_num_]) {
          LOG(ERROR) << "ConcurrentVector[" << name_ << "], new Value["
                     << grp_gap_ << "] fail.";
          size_ = grp_num_ * grp_gap_;
          return false;
        }
        memset(grps_[grp_num_], 0, grp_gap_ * sizeof(Value));
        ++grp_num_;
      }
      size_ = size;
    }
    return true;
  }

  uint32_t Size() { return size_; }

 private:
  Base begin_grp_capacity_;
  std::atomic<Base> grp_capacity_;
  std::atomic<Base> grp_num_;
  std::atomic<uint32_t> size_;
  Base grp_gap_;
  Value **grps_;
  std::string name_;
};

}  // namespace tig_gamma
