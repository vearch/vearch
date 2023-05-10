/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "storage_manager.h"

#include "search/error_code.h"
#include "table_block.h"
#include "util/log.h"
#include "util/utils.h"
#include "vector_block.h"

namespace tig_gamma {

StorageManager::StorageManager(const std::string &root_path,
                               BlockType block_type,
                               const StorageManagerOptions &options)
    : root_path_(root_path), block_type_(block_type), options_(options) {
  size_ = 0;
  cache_ = nullptr;
  str_cache_ = nullptr;
  compressor_ = nullptr;
  disk_io_ = nullptr;
}

StorageManager::~StorageManager() {
  for (size_t i = 0; i < segments_.Size(); i++) {
    Segment *seg = nullptr;
    segments_.GetData(i, seg);
    CHECK_DELETE(seg);
  }
  CHECK_DELETE(disk_io_);
  CHECK_DELETE(str_cache_);
  CHECK_DELETE(cache_);
  CHECK_DELETE(compressor_);
}

std::string StorageManager::NextSegmentFilePath() {
  char buf[7];
  snprintf(buf, 7, "%06d", (int)segments_.Size());
  std::string file_path = root_path_ + "/" + buf;
  return file_path;
}

int StorageManager::UseCompress(CompressType type, int d, double rate) {
  if (type == CompressType::Zfp) {
#ifdef WITH_ZFP
    if (d > 0) {
      compressor_ = new CompressorZFP(type);
      compressor_->Init(d);
    }
#endif
  }
  return (compressor_ ? 0 : -1);
}

bool StorageManager::AlterCacheSize(int cache_size, int str_cache_size) {
  CacheBase<uint32_t, ReadFunParameter *>  *del_cache = nullptr;
  CacheBase<uint32_t, ReadFunParameter *>  *del_str_cache = nullptr;
  uint32_t per_block_size = ((64 * 1024) / options_.fixed_value_bytes) *
                            options_.fixed_value_bytes;  // block~=64k
  auto cache_fun = &TableBlock::ReadBlock;
  if (block_type_ == BlockType::VectorBlockType) {
    cache_fun = &VectorBlock::ReadBlock;
  }

  if (cache_size > 0) {  // cache_size unit: M
    if (cache_ == nullptr || cache_->GetCacheType() == CacheType::SimpleCache) {
      del_cache = cache_;
      cache_ = new LRUCache<uint32_t, ReadFunParameter *>
                        (name_, (size_t)cache_size, per_block_size, cache_fun);
      cache_->Init();
    } else {
      cache_->AlterCacheSize((size_t)cache_size);
    }
  } else if (cache_size < 0) {     // use all memory
    if (cache_ == nullptr || cache_->GetCacheType() == CacheType::LRUCacheType) {
      del_cache = cache_;
      cache_ = new SimpleCache<uint32_t, ReadFunParameter *>(
                  name_, per_block_size, cache_fun, options_.seg_block_capacity);
      cache_->Init();
    }
  } else {
    del_cache = cache_;
    cache_ = nullptr;
  }

  if (block_type_ == BlockType::TableBlockType) {
    if (str_cache_size > 0) {
      if (str_cache_ == nullptr || str_cache_->GetCacheType() == CacheType::SimpleCache) {
        del_str_cache = str_cache_;
        str_cache_ = new LRUCache<uint32_t, ReadFunParameter *>(name_, 
                        (size_t)str_cache_size, MAX_BLOCK_SIZE, &StringBlock::ReadString);
        str_cache_->Init();
      } else {
        str_cache_->AlterCacheSize((size_t)str_cache_size);
      }
    } else if (str_cache_size < 0) {    // use all memory
      if (str_cache_ == nullptr || str_cache_->GetCacheType() == CacheType::LRUCacheType) {
        del_str_cache = str_cache_;
        str_cache_ = new SimpleCache<uint32_t, ReadFunParameter *>(
                            name_, MAX_BLOCK_SIZE, &StringBlock::ReadString,
                            options_.seg_block_capacity);
        str_cache_->Init();
      }
    } else {
      del_str_cache = str_cache_;
      str_cache_ = nullptr;
    }
  }

  for(size_t i = 0; i < segments_.Size(); ++i) {
    Segment *segment = segments_.GetData(i);
    segment->SetCache((void *)cache_, (void *)str_cache_);
  }

  // delay free
  utils::AsyncWait(1000 * 100,
      [](CacheBase<uint32_t, ReadFunParameter *>  *cache,
         CacheBase<uint32_t, ReadFunParameter *>  *str_cache) { 
            CHECK_DELETE(cache); CHECK_DELETE(str_cache); },
      del_cache,del_str_cache);  // after 100s

  return true;
}

void StorageManager::GetCacheSize(int &cache_size,
                                  int &str_cache_size) {
  cache_size = 0;
  str_cache_size = 0;
  if (cache_ != nullptr) {
    int64_t max_size = cache_->GetMaxSize();
    cache_size = (max_size < 0 ? max_size : (int)(max_size * 64 / 1024));
  }
  if (str_cache_ != nullptr) {
    int64_t max_size = cache_->GetMaxSize();
    str_cache_size = (max_size < 0 ? max_size : (int)(max_size * 64 / 1024));
  }
}

int StorageManager::Init(std::string name, int cache_size, int str_cache_size) {
  name_ = name;
  segments_.Init(name + "_ConcurrentVector", BEGIN_GRP_CAPACITY_OF_SEGMENT,
                 GRP_GAP_OF_SEGMENT);

  LOG(INFO) << "Storage[" << name_ << "]. lrucache cache_size[" << cache_size
            << "M], string lrucache cache_size[" << str_cache_size << "M]";
  auto fun = &TableBlock::ReadBlock;
  if (block_type_ == BlockType::VectorBlockType) {
    fun = &VectorBlock::ReadBlock;
  }
  if (options_.fixed_value_bytes > MAX_BLOCK_SIZE) {
    LOG(ERROR) << "fixed_value_bytes[" << options_.fixed_value_bytes
               << "] > 64K. it exceeds the length of the block.";
  }
  uint32_t per_block_size = ((64 * 1024) / options_.fixed_value_bytes) *
                            options_.fixed_value_bytes;  // block~=64k
  if (cache_size > 0) {
    cache_ = new LRUCache<uint32_t, ReadFunParameter *>(name, cache_size,
                                                        per_block_size, fun);
    cache_->Init();
  } else if (cache_size < 0) {
    cache_ = new SimpleCache<uint32_t, ReadFunParameter *>(name,
                    per_block_size, fun, options_.seg_block_capacity);
    cache_->Init();
  }
  if (str_cache_size > 0) {
    str_cache_ = new LRUCache<uint32_t, ReadFunParameter *>(
        name + "_str", str_cache_size, MAX_BLOCK_SIZE,
        &StringBlock::ReadString);
    str_cache_->Init();
  } else if (str_cache_size < 0) {
    str_cache_ = new SimpleCache<uint32_t, ReadFunParameter *>(
        name + "_str", MAX_BLOCK_SIZE, &StringBlock::ReadString,
        options_.seg_block_capacity);
    str_cache_->Init();
  }

  disk_io_ = new disk_io::AsyncWriter();
  if (disk_io_ == nullptr) {
    LOG(ERROR) << "new AsyncWriter failed.";
    return SYSTEM_ERR;
  }
  disk_io_->Init();
  if (!options_.IsValid()) {
    LOG(ERROR) << "invalid options=" << options_.ToStr();
    return PARAM_ERR;
  }
  if (utils::make_dir(root_path_.c_str())) {
    LOG(ERROR) << "mkdir error, path=" << root_path_;
    return IO_ERR;
  }

  Load();
  // init the first segment
  if (segments_.Size() == 0 && Extend()) {
    return INTERNAL_ERR;
  }
  LOG(INFO) << "init storage[" << name_
            << "] success! options=" << options_.ToStr()
            << ", segment num=" << segments_.Size();
  return 0;
}

int StorageManager::Load() {
  // load existed segments
  while (utils::file_exist(NextSegmentFilePath())) {
    Segment *segment = new Segment(
        NextSegmentFilePath(), segments_.Size(), options_.segment_size,
        options_.fixed_value_bytes, options_.seg_block_capacity, disk_io_,
        (void *)cache_, (void *)str_cache_);
    int ret = segment->Load(name_, block_type_, compressor_);
    if (ret < 0) {
      LOG(ERROR) << "Storage[" << name_
                 << "] extend file segment error, ret=" << ret;
      return ret;
    }
    size_ += ret;
    segments_.PushBack(segment);
  }

  LOG(INFO) << "load storage[" << name_
            << "] success! options=" << options_.ToStr()
            << ", segment num=" << segments_.Size();
  return size_;
}

int StorageManager::Extend() {
  uint32_t seg_id = (uint32_t)segments_.Size();
  Segment *segment =
      new Segment(NextSegmentFilePath(), seg_id, options_.segment_size,
                  options_.fixed_value_bytes, options_.seg_block_capacity,
                  disk_io_, (void *)cache_, (void *)str_cache_);
  int ret = segment->Init(name_, block_type_, compressor_);
  if (ret) {
    LOG(ERROR) << "Storage[" << name_
               << "] extend file segment error, ret=" << ret;
    return ret;
  }
  segments_.PushBack(segment);
  return 0;
}

int StorageManager::Add(const uint8_t *value, int len) {
  if (len != options_.fixed_value_bytes) {
    LOG(ERROR) << "Add len error [" << len << "] != options_.fixed_value_bytes["
               << options_.fixed_value_bytes << "]";
    return PARAM_ERR;
  }

  Segment *segment = nullptr;
  segments_.GetLastData(segment);
  int ret = segment->Add(value, len);
  if (ret) {
    LOG(ERROR) << "Storage[" << name_ << "] segment add error[" << ret << "]";
    return ret;
  }

  if (segment->IsFull() && Extend()) {
    LOG(ERROR) << "Storage[" << name_ << "] extend error";
    return INTERNAL_ERR;
  }
  ++size_;
  return 0;
}

str_offset_t StorageManager::AddString(const char *value, str_len_t len,
                                       uint32_t &block_id,
                                       in_block_pos_t &in_block_pos) {
  Segment *segment = nullptr;
  segments_.GetLastData(segment);
  str_offset_t ret = segment->AddString(value, len, block_id, in_block_pos);
  return ret;
}

int StorageManager::GetHeaders(int start_id, int n,
                               std::vector<const uint8_t *> &values,
                               std::vector<int> &lens) {
  if ((size_t)start_id + n > size_) {
    LOG(ERROR) << "Storage[" << name_ << "], start_id [" << start_id
               << "] + n [" << n << "] > size_ [" << size_ << "]";
    return PARAM_ERR;
  }
  while (n) {
    int id = start_id % options_.segment_size;
    int len = options_.segment_size - id;
    if (len > n) len = n;
    Segment *segment = nullptr;
    segments_.GetData(start_id / options_.segment_size, segment);
    if (segment == nullptr) {
      LOG(ERROR) << "Storage[" << name_ << "], segments_size["
                 << segments_.Size() << "], seg_id["
                 << start_id / options_.segment_size
                 << "] cannot be used. GetHeaders(" << start_id << "," << n
                 << ")";
      return -1;
    }

    uint8_t *value = new uint8_t[len * options_.fixed_value_bytes];
    segment->GetValues(value, id, len);
    // std::stringstream ss;
    // for (int i = 0; i < 100; ++i) {
    //   float a;
    //   memcpy(&a, value + i * 4, 4);
    //   ss << a << " ";
    // }
    // std::string aa = ss.str();
    lens.push_back(len);
    values.push_back(value);
    start_id += len;
    n -= len;
  }
  return 0;
}

int StorageManager::Update(int id, uint8_t *value, int len) {
  if ((size_t)id >= size_ || id < 0 || len != options_.fixed_value_bytes) {
    LOG(ERROR) << "Storage[" << name_ << "], id [" << id << "] >= size_ ["
               << size_ << "]";
    return PARAM_ERR;
  }
  Segment *segment = nullptr;
  segments_.GetData(id / options_.segment_size, segment);
  if (segment == nullptr) {
    LOG(ERROR) << "Storage[" << name_ << "], segments_size[" << segments_.Size()
               << "], seg_id[" << id / options_.segment_size
               << "] cannot be used. Update(" << id << ") failed.";
    return -1;
  }
  return segment->Update(id % options_.segment_size, value, len);
}

str_offset_t StorageManager::UpdateString(int id, const char *value,
                                          str_len_t old_len, str_len_t len,
                                          uint32_t &block_id,
                                          in_block_pos_t &in_block_pos) {
  if ((size_t)id >= size_ || id < 0) {
    LOG(ERROR) << "Storage[" << name_ << "], id [" << id << "] >= size_ ["
               << size_ << "]";
    return PARAM_ERR;
  }
  int seg_id = id / options_.segment_size;
  Segment *segment = nullptr;
  segments_.GetData(seg_id, segment);
  if (segment == nullptr) {
    LOG(ERROR) << "Storage[" << name_ << "], segments_size[" << segments_.Size()
               << "], seg_id[" << seg_id << "] cannot be used. UpdateString("
               << id << ") failed.";
    return -1;
  }
  int ret = -1;
  if (len <= old_len) {
    ret = segment->UpdateString(value, len, block_id, in_block_pos);
  }
  if (ret == -1) {
    ret = segment->AddString(value, len, block_id, in_block_pos);
  }

  return ret;
}

int StorageManager::Get(int id, const uint8_t *&value) {
  if ((size_t)id >= size_ || id < 0) {
    LOG(WARNING) << "Storage[" << name_ << "], id [" << id << "] >= size_ ["
                 << size_ << "]";
    return PARAM_ERR;
  }

  int seg_id = id / options_.segment_size;
  Segment *segment = nullptr;
  segments_.GetData(seg_id, segment);
  if (segment == nullptr) {
    LOG(ERROR) << "Storage[" << name_ << "], segments_size[" << segments_.Size()
               << "], seg_id[" << seg_id << "] cannot be used. Get(" << id
               << ")";
    return -1;
  }

  uint8_t *value2 = new uint8_t[options_.fixed_value_bytes];
  int ret = segment->GetValues(value2, id % options_.segment_size, 1);
  value = value2;
  return ret;
}

int StorageManager::GetString(int id, std::string &value, uint32_t block_id,
                              in_block_pos_t in_block_pos, str_len_t len) {
  if ((size_t)id >= size_ || id < 0) {
    LOG(ERROR) << "Storage[" << name_ << "], id [" << id << "] >= size_ ["
               << size_ << "]";
    return PARAM_ERR;
  }
  int seg_id = id / options_.segment_size;
  Segment *segment = nullptr;
  segments_.GetData(seg_id, segment);
  if (segment == nullptr) {
    LOG(ERROR) << "Storage[" << name_ << "], segments_size[" << segments_.Size()
               << "], seg_id[" << seg_id << "] cannot be used. GetString(" << id
               << ") failed.";
    return -1;
  }

  value = segment->GetString(block_id, in_block_pos, len);
  return 0;
}

int StorageManager::Truncate(size_t size) {
  if (size > size_) {
    LOG(ERROR) << "Storage_mgr[" << name_ << "] size[" << size_
               << "] < truncate size[" << size << "]";
  }
  size_t seg_num = size / options_.segment_size;
  size_t offset = size % options_.segment_size;
  if (offset > 0) ++seg_num;
  if (seg_num > segments_.Size()) {
    LOG(ERROR) << "Storage[" << name_ << "] only has " << segments_.Size()
               << " segments, but expect " << seg_num
               << ", trucate size=" << size;
    return PARAM_ERR;
  }

  if (segments_.Size() > seg_num) {
    for (size_t i = seg_num; i < segments_.Size(); ++i) {
      Segment *seg = nullptr;
      segments_.GetData(i, seg);
      if (seg) delete seg;
      segments_.ResetData(i, nullptr);
    }
  }

  segments_.Resize(seg_num);
  if (offset > 0) {
    Segment *seg = nullptr;
    segments_.GetLastData(seg);
    seg->SetBaseSize((uint32_t)offset);
  }
  size_ = size;

  if (seg_num == 0 && Extend()) {
    return INTERNAL_ERR;
  }

  Segment *segment = nullptr;
  segments_.GetLastData(segment);
  if (segment && segment->IsFull() && Extend()) {
    LOG(ERROR) << "Storage[" << name_ << "] extend error";
    return INTERNAL_ERR;
  }
  LOG(INFO) << "Storage[" << name_ << "] truncate to size=" << size
            << ", current segment num=" << segments_.Size()
            << ", last offset=" << offset;
  return 0;
}

int StorageManager::Sync() { return disk_io_->Sync(); }

}  // namespace tig_gamma
