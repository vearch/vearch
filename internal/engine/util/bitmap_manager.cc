/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "util/bitmap_manager.h"

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <functional>

#include "util/log.h"
#include "util/utils.h"

namespace bitmap {

static std::string ToRowKey(uint32_t bit_id) {
  // max length:6 because (2^32 -1) / 1024 / 8 = 524287
  std::string key;
  char data[7];
  snprintf(data, 7, "%06d", bit_id / kBitmapSegmentBits);
  key.assign(data, 7);
  return key;
}

BitmapManager::BitmapManager() {
  bitmap_ = nullptr;
  size_ = 0;
  fd_ = -1;
  fpath_ = "";
  is_load_ = false;
}

BitmapManager::~BitmapManager() {
  if (fd_ != -1) {
    close(fd_);
    fd_ = -1;
  }
}

int BitmapManager::Init(int64_t bit_size, const std::string &fpath,
                        std::shared_ptr<char[]> bitmap) {
  if (bit_size <= 0) {
    LOG(ERROR) << "bit_size <= 0";
    return -1;
  }
  this->size_ = bit_size;
  int64_t bytes_count = (bit_size >> 3) + 1;

  if (bitmap) {
    bitmap_ = bitmap;
  } else {
    bitmap_ = std::shared_ptr<char[]>(new char[bytes_count],
                                      [](char *p) -> void { delete[] p; });
    if (bitmap_ == nullptr) {
      LOG(ERROR) << "new char[" << bytes_count << "] error.";
      return -1;
    }
  }
  memset(bitmap_.get(), 0, bytes_count);

  // open dump file
  int ret = 0;
  if (not fpath.empty() && fd_ == -1) {
    fpath_ = fpath;
    fd_ = open(fpath_.c_str(), O_RDWR | O_CREAT, 0666);
    if (-1 == fd_) {
      LOG(ERROR) << "open file error, path=" << fpath_;
      ret = -1;
    }
  }
  LOG(INFO) << "BitmapManager init successed. bytes_count=" << bytes_count
            << " bit_size=" << bit_size;
  return ret;
}

int BitmapManager::SetDumpFilePath(const std::string &fpath) {
  if (not fpath.empty()) {
    if (fd_ != -1) {
      LOG(ERROR) << "The file[" << fpath_ << "] is already open. close it.";
      close(fd_);
    }
    fpath_ = fpath;
    fd_ = open(fpath_.c_str(), O_RDWR | O_CREAT, 0666);
    if (-1 == fd_) {
      LOG(ERROR) << "open file error, path=" << fpath_;
      return -1;
    }
    LOG(INFO) << "open bitmap file[" << fpath << "] success.";
    return 0;
  }
  return -1;
}

int BitmapManager::Dump(int64_t begin_bit_id, int64_t bit_len) {
  if (bit_len == 0) bit_len = size_;

  if (begin_bit_id < 0 || bit_len < 0 || begin_bit_id + bit_len > size_) {
    LOG(ERROR) << "parameters error, begin_bit_id=" << begin_bit_id
               << " dump_bit_len=" << bit_len << " bit_size=" << size_;
    return -1;
  }

  int64_t begin_bytes = begin_bit_id >> 3;
  int64_t end_bytes = (begin_bit_id + bit_len - 1) >> 3;
  int64_t dump_bytes = end_bytes - begin_bytes + 1;
  int ret = 0;

  if (fd_ != -1) {
    int64_t written_bytes = 0;
    int i = 0;
    while (written_bytes < dump_bytes) {
      int64_t bytes =
          pwrite(fd_, bitmap_.get() + begin_bytes + written_bytes,
                 dump_bytes - written_bytes, begin_bytes + written_bytes);
      written_bytes += bytes;
      if (++i >= 1000) {
        LOG(ERROR) << "dumped bitmap is not complate, written_bytes="
                   << written_bytes;
        ret = -1;
        break;
      }
    }
  } else {
    ret = -1;
  }
  return ret;
}

int BitmapManager::Load(int64_t bit_len) {
  if (bit_len == 0) bit_len = size_;

  if (bit_len < 0 || bit_len > size_) {
    LOG(ERROR) << "parameters error, load_bit_len=" << bit_len
               << " size=" << size_;
    return -1;
  }

  int64_t begin_bytes = 0;
  int64_t end_bytes = (bit_len - 1) >> 3;
  int64_t load_bytes = end_bytes - begin_bytes + 1;
  int ret = 0;
  if (fd_ != -1) {
    int64_t read_bytes = 0;
    int i = 0;
    while (read_bytes < load_bytes) {
      int64_t bytes = pread(fd_, bitmap_.get() + begin_bytes + read_bytes,
                            load_bytes - read_bytes, begin_bytes + read_bytes);
      read_bytes += bytes;
      if (++i >= 1000) {
        LOG(ERROR) << "load bitmap is not complate, load_bytes=" << read_bytes;
        ret = -1;
        break;
      }
    }
  } else {
    ret = -1;
  }
  return ret;
}

int64_t BitmapManager::FileBytesSize() {
  if (fd_ != -1) {
    int64_t len = lseek(fd_, 0, SEEK_END);
    return len;
  }
  return 0;
}

int BitmapManager::Set(int64_t bit_id) {
  if (bit_id >= 0 && bit_id < size_ && bitmap_ != nullptr) {
    bitmap_[bit_id >> 3] |= (0x1 << (bit_id & 0x7));
    return 0;
  }
  return -1;
}

int BitmapManager::Unset(int64_t bit_id) {
  if (bit_id >= 0 && bit_id < size_ && bitmap_ != nullptr) {
    bitmap_[bit_id >> 3] &= ~(0x1 << (bit_id & 0x7));
    return 0;
  }
  return -1;
}

bool BitmapManager::Test(int64_t bit_id) {
  if (bit_id >= 0 && bit_id < size_ && bitmap_ != nullptr) {
    return (bitmap_[bit_id >> 3] & (0x1 << (bit_id & 0x7)));
  }
  return false;
}

int BitmapManager::SetMaxID(int64_t bit_id) {
  if (size_ > bit_id) return 0;

  int64_t old_bytes_count = (size_ >> 3) + 1;
  size_ *= 2;
  int64_t bytes_count = (size_ >> 3) + 1;
  auto new_bitmap = std::shared_ptr<char[]>(
      new char[bytes_count], [](char *p) -> void { delete[] p; });
  if (new_bitmap == nullptr) {
    LOG(INFO) << "new char [" << bytes_count << "] error.";
    return -1;
  }
  memset(new_bitmap.get(), 0, bytes_count);
  if (bitmap_) {
    memcpy(new_bitmap.get(), bitmap_.get(), old_bytes_count);
  }
  bitmap_ = new_bitmap;
  Dump();

  LOG(INFO) << "Current bitmap size [" << size_ << "]";
  return 0;
}

RocksdbBitmapManager::RocksdbBitmapManager() {
  bitmap_ = nullptr;
  size_ = 0;
  fd_ = -1;
  fpath_ = "";
  db_ = nullptr;
  is_load_ = false;
}

RocksdbBitmapManager::~RocksdbBitmapManager() {
  if (db_ != nullptr) {
    db_->Close();
    delete db_;
    db_ = nullptr;
  }
}

int RocksdbBitmapManager::Init(int64_t bit_size, const std::string &fpath,
                               std::shared_ptr<char[]> bitmap) {
  if (bit_size <= 0) {
    LOG(ERROR) << "bit_size <= 0";
    return -1;
  }
  this->size_ = bit_size;

  if (fpath != "") {
    int ret = RocksdbBitmapManager::SetDumpFilePath(fpath);
    if (ret) {
      LOG(ERROR) << "RoskdDB BitmapManager init path err:" << ret;
      return ret;
    }
  } else {
    LOG(ERROR) << "RoskdDB BitmapManager init path should not be empty.";
    return -1;
  }

  // load bitmap size
  std::string value;
  rocksdb::Status s =
      db_->Get(rocksdb::ReadOptions(), rocksdb::Slice(kBitmapSizeKey), &value);
  if (s.ok()) {
    size_ = atol(value.c_str());
    LOG(INFO) << "RoskdDB set dump file path successed, load size_=" << size_;
  } else {
    // dump bitmap size
    std::string value = std::to_string(size_);
    rocksdb::Status s =
        db_->Put(rocksdb::WriteOptions(), rocksdb::Slice(kBitmapSizeKey),
                 rocksdb::Slice(value));
    if (!s.ok()) {
      LOG(ERROR) << "rocksdb set bitmap size error:" << s.ToString()
                 << ", key=" << kBitmapSizeKey << ", value=" << value;
      return s.code();
    }
  }

  uint32_t bytes_count = ((size_ / kBitmapSegmentBits) + 1) * kBitmapSegmentBytes;
  if (bitmap) {
    bitmap_ = bitmap;
  } else {
    bitmap_ = std::shared_ptr<char[]>(new char[bytes_count],
                                      [](char *p) -> void { delete[] p; });
    if (bitmap_ == nullptr) {
      LOG(ERROR) << "new char[" << bytes_count << "] error.";
      return -1;
    }
  }
  memset(bitmap_.get(), 0, bytes_count);

  LOG(INFO) << "RoskdDB BitmapManager init successed. bytes_count="
            << bytes_count << " bit_size=" << bit_size;
  return 0;
}

int RocksdbBitmapManager::SetDumpFilePath(const std::string &fpath) {
  if (db_ == nullptr && fpath != "") {
    rocksdb::BlockBasedTableOptions table_options;
    std::shared_ptr<rocksdb::Cache> cache =
        rocksdb::NewLRUCache(kBitmapCacheSize);
    table_options.block_cache = cache;
    rocksdb::Options options;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    options.IncreaseParallelism();
    // options.OptimizeLevelStyleCompaction();
    // create the DB if it's not already present
    options.create_if_missing = true;

    if (!utils::isFolderExist(fpath.c_str())) {
      if (mkdir(fpath.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH)) {
        std::string msg = "mkdir " + fpath + " error";
        LOG(ERROR) << msg;
        return -1;
      }
    } else {
      is_load_ = true;
    }

    // open DB
    rocksdb::Status s = rocksdb::DB::Open(options, fpath, &db_);
    if (!s.ok()) {
      LOG(ERROR) << "open rocksdb error: " << s.ToString();
      return -2;
    }
    return 0;
  }
  return -1;
}

int RocksdbBitmapManager::Dump(int64_t begin_bit_id, int64_t bit_len) {
  return 0;
}

int RocksdbBitmapManager::Load(int64_t bit_len) {
  if (bit_len == 0) bit_len = size_;

  if (bit_len < 0 || bit_len > size_) {
    LOG(ERROR) << "parameters error, load_bit_len=" << bit_len
               << " size=" << size_;
    return -1;
  }
  int64_t load_num = 0;
  for (int64_t i = 0; i < bit_len; i += kBitmapSegmentBits) {
    std::string key, value;
    key = ToRowKey(i);
    rocksdb::Status s =
        db_->Get(rocksdb::ReadOptions(), rocksdb::Slice(key), &value);
    if (s.ok()) {
      memcpy((void *)(bitmap_.get() +
                      i / kBitmapSegmentBits * kBitmapSegmentBytes),
             value.c_str(), kBitmapSegmentBytes);
      load_num += 1;
    }
  }
  LOG(INFO) << "RoskdDB BitmapManager load successed. size_=" << size_
            << ", load_num=" << load_num;
  return 0;
}

int64_t RocksdbBitmapManager::FileBytesSize() { return 0; }

int RocksdbBitmapManager::Set(int64_t bit_id) {
  if (bit_id >= 0 && bit_id < size_ && bitmap_ != nullptr) {
    bitmap_[bit_id >> 3] |= (0x1 << (bit_id & 0x7));

    std::string key, value;
    key = ToRowKey(bit_id);

    rocksdb::Status s = db_->Put(
        rocksdb::WriteOptions(), rocksdb::Slice(key),
        rocksdb::Slice(
            (const char *)(bitmap_.get() +
                           bit_id / kBitmapSegmentBits * kBitmapSegmentBytes),
            kBitmapSegmentBytes));
    if (!s.ok()) {
      LOG(ERROR) << "rocksdb set bitmap error:" << s.ToString()
                 << ", key=" << key << ", value=" << value;
      // reset
      bitmap_[bit_id >> 3] &= ~(0x1 << (bit_id & 0x7));
      return s.code();
    }
    return 0;
  }
  return -1;
}

int RocksdbBitmapManager::Unset(int64_t bit_id) {
  if (bit_id >= 0 && bit_id < size_ && bitmap_ != nullptr) {
    bitmap_[bit_id >> 3] &= ~(0x1 << (bit_id & 0x7));

    std::string key, value;
    key = ToRowKey(bit_id);

    rocksdb::Status s = db_->Put(
        rocksdb::WriteOptions(), rocksdb::Slice(key),
        rocksdb::Slice(
            (const char *)(bitmap_.get() +
                           bit_id / kBitmapSegmentBits * kBitmapSegmentBytes),
            kBitmapSegmentBytes));
    if (!s.ok()) {
      LOG(ERROR) << "rocksdb unset bitmap error:" << s.ToString()
                 << ", key=" << key << ", value=" << value;
      // reset
      bitmap_[bit_id >> 3] |= (0x1 << (bit_id & 0x7));
      return s.code();
    }
    return 0;
  }
  return -1;
}

bool RocksdbBitmapManager::Test(int64_t bit_id) {
  if (bit_id >= 0 && bit_id < size_ && bitmap_ != nullptr) {
    auto temp = bitmap_;
    return (temp[bit_id >> 3] & (0x1 << (bit_id & 0x7)));
  }
  return false;
}

int RocksdbBitmapManager::SetMaxID(int64_t bit_id) {
  if (size_ > bit_id) return 0;

  size_t new_size = size_ * 2;
  std::string value = std::to_string(new_size);
  rocksdb::WriteOptions write_options;
  write_options.sync = true;
  rocksdb::Status s =
      db_->Put(write_options, rocksdb::Slice(kBitmapSizeKey),
               rocksdb::Slice(value));
  if (!s.ok()) {
    LOG(ERROR) << "rocksdb set bitmap size error:" << s.ToString()
               << ", key=" << kBitmapSizeKey << ", value=" << value;
    return s.code();
  }

  int64_t old_bytes_count = ((size_ / kBitmapSegmentBits) + 1) * kBitmapSegmentBytes;
  size_ = new_size;
  int64_t bytes_count = ((size_ / kBitmapSegmentBits) + 1) * kBitmapSegmentBytes;

  auto new_bitmap = std::shared_ptr<char[]>(
      new char[bytes_count], [](char *p) -> void { delete[] p; });
  if (new_bitmap == nullptr) {
    LOG(INFO) << "new char [" << bytes_count << "] error.";
    return -1;
  }
  memset(new_bitmap.get(), 0, bytes_count);
  if (bitmap_) {
    memcpy(new_bitmap.get(), bitmap_.get(), old_bytes_count);
  }
  bitmap_ = new_bitmap;

  LOG(INFO) << "Current bitmap size [" << size_ << "]";

  return 0;
}

}  // namespace bitmap