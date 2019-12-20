/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "raw_vector.h"
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "log.h"
#include "utils.h"

using namespace std;

namespace tig_gamma {

RawVectorIO::RawVectorIO(RawVector *raw_vector) {
  raw_vector_ = raw_vector;
  docid_fd_ = -1;
  src_fd_ = -1;
  src_pos_fd_ = -1;
}

RawVectorIO::~RawVectorIO() {
  if (docid_fd_ != -1) close(docid_fd_);
  if (src_fd_ != -1) close(src_fd_);
  if (src_pos_fd_ != -1) close(src_pos_fd_);
}

int RawVectorIO::Init() {
  string docid_file_path =
      raw_vector_->root_path_ + "/" + raw_vector_->vector_name_ + ".docid";
  string src_file_path =
      raw_vector_->root_path_ + "/" + raw_vector_->vector_name_ + ".src";
  string src_pos_file_path =
      raw_vector_->root_path_ + "/" + raw_vector_->vector_name_ + ".src.pos";
  docid_fd_ = open(docid_file_path.c_str(), O_RDWR | O_APPEND | O_CREAT, 00664);
  src_fd_ = open(src_file_path.c_str(), O_RDWR | O_APPEND | O_CREAT, 00664);
  src_pos_fd_ =
      open(src_pos_file_path.c_str(), O_RDWR | O_APPEND | O_CREAT, 00664);
  if (docid_fd_ == -1 || src_fd_ == -1 || src_pos_fd_ == -1) {
    LOG(ERROR) << "open file error:" << strerror(errno);
    return -1;
  }
  return 0;
}
int RawVectorIO::Dump(int start, int n) {
  char *str_mem_ptr = raw_vector_->str_mem_ptr_;
  long *source_mem_pos = raw_vector_->source_mem_pos_.data();
  int *vid2docid = raw_vector_->vid2docid_.data();

  // dump source
  write(src_fd_, (void *)(str_mem_ptr + source_mem_pos[start]),
        source_mem_pos[start + n] - source_mem_pos[start]);

  // dump source position
  if (start == 0) {
    write(src_pos_fd_, (void *)(source_mem_pos + start),
          (n + 1) * sizeof(long));
  } else {
    write(src_pos_fd_, (void *)(source_mem_pos + start + 1), n * sizeof(long));
  }

#ifdef DEBUG
  LOG(INFO) << "io dump,  start=" << start << ", n=" << n;
#endif
  write(docid_fd_, (void *)(vid2docid + start), n * sizeof(int));
  return 0;
}
int RawVectorIO::Load(int doc_num) {
  if (doc_num == 0) {
    if (ftruncate(docid_fd_, 0)) {
      LOG(ERROR) << "truncate docid file error:" << strerror(errno);
      return -1;
    }
    if (ftruncate(src_pos_fd_, 0)) {
      LOG(ERROR) << "truncate source position file error:" << strerror(errno);
      return -1;
    }
    if (ftruncate(src_fd_, 0)) {
      LOG(ERROR) << "truncate source file error:" << strerror(errno);
      return -1;
    }
    return 0;
  }
  string docid_file_path =
      raw_vector_->root_path_ + "/" + raw_vector_->vector_name_ + ".docid";
  long docid_file_size = utils::get_file_size(docid_file_path.c_str());
  if (docid_file_size <= 0 || docid_file_size % sizeof(int) != 0) {
    LOG(ERROR) << "invalid docid file size=" << docid_file_size;
    return -1;
  }
  int num = docid_file_size / sizeof(int);
  read(docid_fd_, (void *)raw_vector_->vid2docid_.data(), num * sizeof(int));
  // create docid2vid_ from vid2docid_
  int vid = 0;
  for (; vid < num; vid++) {
    int docid = raw_vector_->vid2docid_[vid];
    if (docid == -1) {
      continue;
    }
    if (docid >= doc_num) {
      break;
    }
    if (raw_vector_->docid2vid_[docid] == nullptr) {
      raw_vector_->docid2vid_[docid] = utils::NewArray<int>(
          MAX_VECTOR_NUM_PER_DOC + 1, "load_init_vid_list");
      raw_vector_->docid2vid_[docid][0] = 1;
      raw_vector_->docid2vid_[docid][1] = vid;
    } else {
      int *vid_list = raw_vector_->docid2vid_[docid];
      if (vid_list[0] + 1 > MAX_VECTOR_NUM_PER_DOC) {
        LOG(ERROR) << "vid list size=" << vid_list[0] + 1 << " > "
                   << MAX_VECTOR_NUM_PER_DOC << ", vid=" << vid
                   << ", docid=" << docid << ", vid list="
                   << utils::join(vid_list + 1, vid_list[0], ',');
        return -1;
      }
      vid_list[vid_list[0]] = vid;
      vid_list[0]++;
    }
  }
  int n = vid;
  // set [n, num) to be -1
  for (int i = n; i < num; i++) {
    raw_vector_->vid2docid_[i] = -1;
  }
  read(src_pos_fd_, (void *)raw_vector_->source_mem_pos_.data(),
       (n + 1) * sizeof(long));
  read(src_fd_, (void *)raw_vector_->str_mem_ptr_,
       raw_vector_->source_mem_pos_[n]);

  // truncate docid and str file to vid_num length
  if (ftruncate(docid_fd_, n * sizeof(int))) {
    LOG(ERROR) << "truncate docid file error:" << strerror(errno);
    return -1;
  }
  if (ftruncate(src_pos_fd_, (n + 1) * sizeof(long))) {
    LOG(ERROR) << "truncate source position file error:" << strerror(errno);
    return -1;
  }
  if (ftruncate(src_fd_, raw_vector_->source_mem_pos_[n])) {
    LOG(ERROR) << "truncate source file error:" << strerror(errno);
    return -1;
  }
  return n;
}

RawVector::RawVector(const string &name, int dimension, int max_vector_size,
                     const string &root_path)
    : vector_name_(name),
      dimension_(dimension),
      max_vector_size_(max_vector_size),
      root_path_(root_path),
      ntotal_(0),
      total_mem_bytes_(0) {
  uint64_t len = (uint64_t)max_vector_size_ * 100;
  str_mem_ptr_ = new (std::nothrow) char[len];
  total_mem_bytes_ += len;
  source_mem_pos_.resize(max_vector_size_ + 1, 0);
  total_mem_bytes_ += max_vector_size_ * sizeof(long);
  vid2docid_.resize(max_vector_size_, -1);
  total_mem_bytes_ += max_vector_size_ * sizeof(int);
  docid2vid_.resize(max_vector_size_, nullptr);
  total_mem_bytes_ += max_vector_size_ * sizeof(docid2vid_[0]);
  vector_byte_size_ = dimension * sizeof(float);
}

RawVector::~RawVector() {
  if (str_mem_ptr_) {
    delete[] str_mem_ptr_;
  }
  for (size_t i = 0; i < docid2vid_.size(); i++) {
    if (docid2vid_[i] != nullptr) {
      delete[] docid2vid_[i];
      docid2vid_[i] = nullptr;
    }
  }
}

int RawVector::GetVector(long vid, ScopeVector &vec) {
  return GetVector(vid, vec.ptr_, vec.deletable_);
}

int RawVector::Dump(const std::string &path, int dump_docid, int max_docid) {
  LOG(INFO) << "dump_docid=" << dump_docid << ", max_docid=" << max_docid;
  int start = GetFirstVectorID(dump_docid);
  int end = GetLastVectorID(max_docid);
  int n = end - start + 1;
  RawVectorIO *raw_vector_io = new RawVectorIO(this);
  if (raw_vector_io->Init()) return -1;
  raw_vector_io->Dump(start, n);
  delete raw_vector_io;
  return DumpVectors(start, n);
};

int RawVector::Load(const std::vector<std::string> &path, int doc_num) {
  RawVectorIO *raw_vector_io = new RawVectorIO(this);
  if (raw_vector_io->Init()) return -1;
  int num = raw_vector_io->Load(doc_num);
  delete raw_vector_io;
  assert(num >= 0);
  if (LoadVectors(num)) {
    LOG(ERROR) << "load vectors error";
    return -2;
  }
  ntotal_ = num;
  return 0;
}

int RawVector::Gets(int k, long *ids_list, ScopeVectors &vecs) const {
  const float *vec;
  bool deletable;
  for (int i = 0; i < k; i++) {
    vec = nullptr;
    deletable = false;
    GetVector(ids_list[i], vec, deletable);
    vecs.Set(i, vec, deletable);
  }
  return 0;
}

int RawVector::GetSource(int vid, char *&str, int &len) {
  if (vid < 0 || vid >= ntotal_) return -1;
  len = source_mem_pos_[vid + 1] - source_mem_pos_[vid];
  str = str_mem_ptr_ + source_mem_pos_[vid];
  return 0;
}

int RawVector::Add(int docid, Field *&field) {
  if (ntotal_ >= max_vector_size_) {
    return -1;
  }
  AddToStore((float *)field->value->value, field->value->len / sizeof(float));

  // add to source
  int len = field->source ? field->source->len : 0;
  if (len > 0) {
    memcpy(str_mem_ptr_ + source_mem_pos_[ntotal_], field->source->value,
           len * sizeof(char));
    source_mem_pos_[ntotal_ + 1] = source_mem_pos_[ntotal_] + len;
  } else {
    source_mem_pos_[ntotal_ + 1] = source_mem_pos_[ntotal_];
  }

  // add to vid2docid_ and docid2vid_
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
int RawVector::GetFirstVectorID(int docid) {
  int *vid_list = docid2vid_[docid];
  if (vid_list[0] <= 0) return -1;
  return vid_list[1];
}
int RawVector::GetLastVectorID(int docid) {
  int *vid_list = docid2vid_[docid];
  if (vid_list[0] <= 0) return -1;
  return vid_list[vid_list[0]];
}

AsyncFlusher::AsyncFlusher(string name) : name_(name) {
  stopped_ = false;
  last_nflushed_ = nflushed_ = 0;
  interval_ = 100;  // ms
  runner_ = nullptr;
}

AsyncFlusher::~AsyncFlusher() {
  if (runner_) delete runner_;
}

void AsyncFlusher::Start() {
  // TODO: check if it is stopped
  stopped_ = false;
  runner_ = new std::thread(Handler, this);
}

void AsyncFlusher::Stop() {
  stopped_ = true;
  if (runner_) {
    runner_->join();
    delete runner_;
    runner_ = nullptr;
  }
}

void AsyncFlusher::Handler(tig_gamma::AsyncFlusher *flusher) {
  LOG(INFO) << "flusher=" << flusher->name_ << " is started!";
  int ret = flusher->Flush();
  if (ret != 0) {
    LOG(ERROR) << "flusher=" << flusher->name_
               << " exit unexpectedly! ret=" << ret;
  } else {
    LOG(INFO) << "flusher=" << flusher->name_ << " exit successfully!";
  }
}

int AsyncFlusher::Flush() {
  while (!stopped_) {
    int ret = FlushOnce();
    if (ret < 0)
      return ret;
    else
      nflushed_ += ret;
    if (nflushed_ - last_nflushed_ > 100) {
      LOG(INFO) << "flushed number=" << nflushed_;
      last_nflushed_ = nflushed_;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(interval_));
  }
  return 0;
}
void AsyncFlusher::Until(int nexpect) {
  while (nflushed_ < nexpect) {
    LOG(INFO) << "flusher waiting......, expected num=" << nexpect
              << ", flushed num=" << nflushed_;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

void StartFlushingIfNeed(RawVector *vec) {
  AsyncFlusher *flusher = nullptr;
  if ((flusher = dynamic_cast<AsyncFlusher *>(vec))) {
    flusher->Start();
    LOG(INFO) << "start flushing, raw vector=" << vec->GetName();
  }
}

void StopFlushingIfNeed(RawVector *vec) {
  AsyncFlusher *flusher = nullptr;
  if ((flusher = dynamic_cast<AsyncFlusher *>(vec))) {
    flusher->Until(vec->GetVectorNum());
    flusher->Stop();
    LOG(INFO) << "stop flushing, raw vector=" << vec->GetName();
  }
}

int StoreParams::Parse(const char *str) {
  utils::JsonParser jp;
  if (jp.Parse(str)) {
    LOG(ERROR) << "parse store parameters error: " << str;
    return -1;
  }

  double cache_size = 0;
  if (!jp.GetDouble("cache_size", cache_size)) {
    if (cache_size > MAX_CACHE_SIZE || cache_size < 0) {
      LOG(ERROR) << "invalid cache size=" << cache_size << "M"
                 << ", limit size=" << MAX_CACHE_SIZE << "M";
      return -1;
    }
    cache_size_ = (long)cache_size * 1024 * 1024;
  }

  return 0;
}

}  // namespace tig_gamma
