/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "memory_raw_vector.h"
#include "gamma_common_data.h"
#include "log.h"
#include <string.h>

using namespace std;

namespace tig_gamma {

MemoryRawVector::MemoryRawVector(const std::string name, int dimension,
                                 int max_doc_size)
    : RawVector(name, dimension, max_doc_size) {
  vector_mem_ = nullptr;
  str_mem_ptr_ = nullptr;
  total_mem_bytes_ = 0;
}

MemoryRawVector::~MemoryRawVector() { Close(); }

int MemoryRawVector::Init() {
  if (vector_mem_ == nullptr) {
    delete[] vector_mem_;
  }
  uint64_t len = (uint64_t)max_vector_size_ * dimension_;
  vector_mem_ = new (std::nothrow) float[len];
  if (vector_mem_ == nullptr) {
    LOG(ERROR) << "vector memory alloc err , max_doc_size_ ["
               << max_vector_size_ << "], dimension_ [" << dimension_ << "]";
    return 2; // internal error
  }
  total_mem_bytes_ += len * sizeof(float);

  len = (uint64_t)max_vector_size_ * 100;
  str_mem_ptr_ = new (std::nothrow) char[len];
  total_mem_bytes_ += len;

  vid2docid_.resize(max_vector_size_, -1);
  total_mem_bytes_ += max_vector_size_ * sizeof(int);

  source_mem_pos_.resize(max_vector_size_ + 1, 0);
  // source_mem_pos_.assign(max_vector_size_, 0);
  total_mem_bytes_ += max_vector_size_ * sizeof(long);

  docid2vid_.resize(max_vector_size_, nullptr);
  total_mem_bytes_ += max_vector_size_ * sizeof(docid2vid_[0]);

  return 0;
}

void MemoryRawVector::Close() {
  if (vector_mem_ != nullptr) {
    delete[] vector_mem_;
  }
  if (str_mem_ptr_) {
    delete[] str_mem_ptr_;
  }
  vid2docid_.clear();
  source_mem_pos_.clear();
  ntotal_ = 0;
  total_mem_bytes_ = 0;
  for (size_t i = 0; i < docid2vid_.size(); i++) {
    if (docid2vid_[i] != nullptr) {
      delete[] docid2vid_[i];
      docid2vid_[i] = nullptr;
    }
  }
  docid2vid_.clear();
}

const float *MemoryRawVector::GetVector(long vid) const {
  if (vid < 0 || vid >= ntotal_)
    return nullptr;
  return vector_mem_ + (uint64_t)vid * dimension_;
}

int MemoryRawVector::Add(int docid, Field *&field) {
  if (ntotal_ >= max_vector_size_) {
    return -1;
  }
  memcpy((void *)(vector_mem_ + (uint64_t)ntotal_ * dimension_),
         (void *)(field->value->value), dimension_ * sizeof(float));

  int len = field->source ? field->source->len : 0;
  if (len > 0) {
    memcpy(str_mem_ptr_ + source_mem_pos_[ntotal_], field->source->value,
           len * sizeof(char));
    source_mem_pos_[ntotal_ + 1] = source_mem_pos_[ntotal_] + len;
  } else {
    source_mem_pos_[ntotal_ + 1] = source_mem_pos_[ntotal_];
  }
  vid2docid_[ntotal_] = docid;
  if (docid2vid_[docid] == nullptr) {
    docid2vid_[docid] =
        utils::NewArray<int>(MAX_VECTOR_NUM_PER_DOC + 1, "init_vid_list");
    total_mem_bytes_ += (MAX_VECTOR_NUM_PER_DOC + 1) * sizeof(int);
    docid2vid_[docid][0] = 1;
    docid2vid_[docid][1] = ntotal_;
  } else {
    int *vid_list = docid2vid_[docid];
    if (vid_list[0] + 1 > MAX_VECTOR_NUM_PER_DOC) {
      return -1;
    }
    vid_list[vid_list[0]] = ntotal_;
    vid_list[0]++;
  }
  ntotal_++;
  return 0;
}

int MemoryRawVector::GetSource(int vid, char *&str, int &len) {
  if (vid < 0 || vid >= ntotal_)
    return -1;
  else {
    len = source_mem_pos_[vid + 1] - source_mem_pos_[vid];
    str = str_mem_ptr_ + source_mem_pos_[vid];
  }
  return 0;
}

const float *MemoryRawVector::GetVectorHeader() { return vector_mem_; }

int MemoryRawVector::Gets(int k, long *ids_list,
                          std::vector<const float *> &results) const {
  if (results.size() != (size_t)k) {
    return -1;
  }
  for (int i = 0; i < k; i++) {
    results[i] = GetVector(ids_list[i]);
  }
  return 0;
}

int MemoryRawVector::Dump(const string &path, int dump_docid, int max_docid) {
  string fet_file_path = path + "/" + vector_name_ + ".fet";
  string src_file_path = path + "/" + vector_name_ + ".src";

  FILE *fet_fp = fopen(fet_file_path.c_str(), "wb");
  FILE *src_fp = fopen(src_file_path.c_str(), "wb");
  if (fet_fp == nullptr) {
    LOG(ERROR) << "open feature file error, file path=" << fet_file_path;
    return -1;
  }
  if (src_fp == nullptr) {
    fclose(fet_fp);
    LOG(ERROR) << "open source file error, file path=" << src_file_path;
    return -1;
  }
  size_t nwrite = 0;
  int vid_begin = GetFirstVectorID(dump_docid);
  int vid_end = GetLastVectorID(max_docid);
  int total = vid_end - vid_begin + 1;

  // dump inc vid2docid to feature file
  fwrite((void *)&vid_begin, sizeof(vid_begin), 1, fet_fp);
  fwrite((void *)&total, sizeof(total), 1, fet_fp);
  nwrite = fwrite((void *)(vid2docid_.data() + vid_begin), sizeof(int), total,
                  fet_fp);
  assert((size_t)total == nwrite);

  // dump inc vector to feature file
  nwrite = fwrite((void *)(vector_mem_ + (uint64_t)vid_begin * dimension_),
                  sizeof(float) * dimension_, total, fet_fp);
  assert((size_t)total == nwrite);

  // dump inc source
  int src_total = total + 1;
  fwrite((void *)&src_total, sizeof(int), 1, src_fp);
  nwrite = fwrite((void *)(source_mem_pos_.data() + vid_begin), sizeof(long),
                  src_total, src_fp);
  assert((size_t)src_total == nwrite);
  long src_len = source_mem_pos_[vid_end + 1] - source_mem_pos_[vid_begin];
  nwrite = fwrite((void *)(str_mem_ptr_ + source_mem_pos_[vid_begin]),
                  sizeof(char), src_len, src_fp);
  assert((size_t)src_len == nwrite);

  fclose(fet_fp);
  fclose(src_fp);

  LOG(INFO) << "dump feature file path=" << fet_file_path
            << ", source file path=" << src_file_path
            << "begin vector id=" << vid_begin << ", total=" << total
            << ", dimension=" << dimension_ << ", source length=" << src_len;

  return 0;
}

int MemoryRawVector::Load(const std::vector<std::string> &dirs) {
  Close();
  if (0 != Init()) {
    LOG(INFO) << "init error";
    return -1;
  }

  for (size_t i = 0; i < dirs.size(); i++) {
    string fet_file_path = dirs[i] + "/" + vector_name_ + ".fet";
    FILE *fet_fp = fopen(fet_file_path.c_str(), "rb");
    if (fet_fp == NULL) {
      LOG(ERROR) << "open feature file error, file path=" << fet_file_path;
      return -1;
    }
    size_t fet_file_size = (size_t)utils::get_file_size(fet_file_path.c_str());
    size_t head_len = 0, read_n = 0;

    // load inc vid2docid from feature file
    int vid_begin = -1, vid_end = -1, total = 0;
    fread((void *)&vid_begin, sizeof(vid_begin), 1, fet_fp);
    assert(-1 != vid_begin);
    fread((void *)&total, sizeof(total), 1, fet_fp);
    fread((void *)(vid2docid_.data() + vid_begin), sizeof(int), total, fet_fp);
    head_len += sizeof(vid_begin) + sizeof(total) + total * sizeof(int);
    vid_end = vid_begin + total - 1;

    // load inc vector from feature file
    if ((fet_file_size - head_len) != sizeof(float) * dimension_ * total) {
      LOG(ERROR) << "invalid feature file size=" << fet_file_size
                 << ", total=" << total << ", head len=" << head_len;
      fclose(fet_fp);
      return -1;
    }
    read_n = fread((void *)(vector_mem_ + (uint64_t)ntotal_ * dimension_),
                   sizeof(float) * dimension_, total, fet_fp);
    assert((size_t)total == read_n);
    fclose(fet_fp);

    // load inc source
    string src_file_path = dirs[i] + "/" + vector_name_ + ".src";
    FILE *src_fp = fopen(src_file_path.c_str(), "rb");
    if (src_fp == nullptr) {
      LOG(ERROR) << "open source file error, file path=" << src_file_path;
      return -1;
    }
    // load soucre
    size_t src_file_size = (size_t)utils::get_file_size(src_file_path.c_str());
    int src_total = 0;
    head_len = 0;
    fread((void *)&src_total, sizeof(int), 1, src_fp);
    head_len += sizeof(int);
    assert(src_total == total + 1);
    read_n = fread((void *)(source_mem_pos_.data() + vid_begin), sizeof(long),
                   src_total, src_fp);
    assert((size_t)src_total == read_n);
    head_len += sizeof(long) * src_total;
    long src_len = source_mem_pos_[vid_end + 1] - source_mem_pos_[vid_begin];
    if (src_file_size - head_len != (size_t)src_len) {
      LOG(ERROR) << "invalid source file size=" << src_file_size
                 << ", source length=" << src_len
                 << ", head length=" << head_len << ", vid begin=" << vid_begin
                 << ", total=" << total;
      fclose(src_fp);
      return -1;
    }
    read_n = fread((void *)(str_mem_ptr_ + source_mem_pos_[vid_begin]),
                   sizeof(char), src_len, src_fp);
    assert((size_t)src_len == read_n);
    fclose(src_fp);

    ntotal_ = vid_end + 1;
  }

  // create docid2vid_ from vid2docid_
  for (int vid = 0; vid < ntotal_; vid++) {
    int docid = vid2docid_[vid];
    if (docid == -1) {
      continue;
    }
    if (docid2vid_[docid] == nullptr) {
      docid2vid_[docid] = utils::NewArray<int>(MAX_VECTOR_NUM_PER_DOC + 1,
                                               "load_init_vid_list");
      total_mem_bytes_ += (MAX_VECTOR_NUM_PER_DOC + 1) * sizeof(int);
      docid2vid_[docid][0] = 1;
      docid2vid_[docid][1] = vid;
    } else {
      int *vid_list = docid2vid_[docid];
      if (vid_list[0] + 1 > MAX_VECTOR_NUM_PER_DOC) {
        LOG(ERROR) << "vid list size=" << vid_list[0] + 1 << " > "
                   << MAX_VECTOR_NUM_PER_DOC << ", vid=" << vid;
        return -1;
      }
      vid_list[vid_list[0]] = vid;
      vid_list[0]++;
    }
  }

  LOG(INFO) << "memory raw vector load success! ntotal=" << ntotal_
            << ", dimension=" << dimension_
            << ", source data size=" << source_mem_pos_[ntotal_]
            << "directory=" << utils::join(dirs, ',');

  return 0;
}

} // namespace tig_gamma
