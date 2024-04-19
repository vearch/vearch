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

class VIDMgr {
 public:
  VIDMgr(bool multi_vids) : multi_vids_(multi_vids) {}

  ~VIDMgr() {
    if (multi_vids_) {
      for (size_t i = 0; i < docid2vid_.size(); i++) {
        delete[] docid2vid_[i];
        docid2vid_[i] = nullptr;
      }
    }
  }

  int Init(int max_vector_size, long &total_mem_bytes) {
    if (multi_vids_) {
      vid2docid_.resize(max_vector_size, -1);
      total_mem_bytes += max_vector_size * sizeof(int);
      docid2vid_.resize(max_vector_size, nullptr);
      total_mem_bytes += max_vector_size * sizeof(docid2vid_[0]);
    }
    return 0;
  }

  int Add(int vid, int docid) {
    // add to vid2docid_ and docid2vid_
    if (multi_vids_) {
      vid2docid_[vid] = docid;
      if (docid2vid_[docid] == nullptr) {
        docid2vid_[docid] =
            utils::NewArray<int>(MAX_VECTOR_NUM_PER_DOC + 1, "init_vid_list");
        docid2vid_[docid][0] = 1;
        docid2vid_[docid][1] = vid;
      } else {
        int *vid_list = docid2vid_[docid];
        if (vid_list[0] >= MAX_VECTOR_NUM_PER_DOC) {
          return -1;
        }
        vid_list[vid_list[0]] = vid;
        ++vid_list[0];
      }
    }
    return 0;
  }

  int VID2DocID(int vid) {
    if (!multi_vids_) {
      return vid;
    }
    return vid2docid_[vid];
  }

  void DocID2VID(int docid, std::vector<int64_t> &vids) {
    if (!multi_vids_) {
      vids.resize(1);
      vids[0] = docid;
      return;
    }
    int *vid_list = docid2vid_[docid];
    int n_vids = vid_list[0];
    vids.resize(n_vids);
    for (int i = 0; i < n_vids; ++i) {
      vids[i] = *(vid_list + i + 1);
    }
    // memcpy((void *)vids.data(), (void *)(vid_list + 1), n_vids *
    // sizeof(int));
  }

  int GetFirstVID(int docid) {
    if (!multi_vids_) {
      return docid;
    }
    int *vid_list = docid2vid_[docid];
    int n_vids = vid_list[0];
    if (n_vids <= 0) {
      return -1;
    }
    return vid_list[1];
  }

  int GetLastVID(int docid) {
    if (!multi_vids_) {
      return docid;
    }
    int *vid_list = docid2vid_[docid];
    int n_vids = vid_list[0];
    if (n_vids <= 0) {
      return -1;
    }
    return vid_list[n_vids];
  }

  bool MultiVids() { return multi_vids_; }

  std::vector<int> &Vid2Docid() { return vid2docid_; }

  std::vector<int *> &Docid2Vid() { return docid2vid_; }

 private:
  std::vector<int> vid2docid_;    // vector id to doc id
  std::vector<int *> docid2vid_;  // doc id to vector id list
  bool multi_vids_;
};
