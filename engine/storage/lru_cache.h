/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <time.h>
#include <unistd.h>
// #include <malloc.h>

#include <atomic>
#include <list>
#include <memory>
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>

#include "util/log.h"
#include "util/utils.h"

struct ReadFunParameter {
  int fd;
  uint32_t len;
  uint32_t offset;
};

#define THRESHOLD_OF_SWAP 250
#define THRESHOLD_TYPE uint8_t
#define MAP_GROUP_NUM 100

enum class CacheType : std::uint8_t { LRUCacheType, SimpleCache };

template <typename Value>
class CacheList {
 public:
  struct Node {
    Value val;
    Node *prev;
    Node *next;
  };
  CacheList() {
    head_ = nullptr;
    tail_ = nullptr;
  }
  ~CacheList() {
    while (head_) {
      Node *del = head_;
      head_ = del->next;
      delete del;
    }
    tail_ = nullptr;
  }

  void Init() {
    head_ = new Node;
    tail_ = head_;
    head_->next = nullptr;
    head_->prev = nullptr;
  }

  void Erase(void *n) {
    if (!n) {
      return;
    }
    Node *del = (Node *)n;
    del->prev->next = del->next;
    if (del->next) {
      del->next->prev = del->prev;
    } else {
      tail_ = del->prev;
    }
    delete del;
  }

  void MoveToTail(void *n) {
    Node *node = (Node *)n;
    if (!node || node->prev == nullptr || node->next == nullptr ||
        node == tail_) {
      return;
    }
    node->prev->next = node->next;
    node->next->prev = node->prev;
    tail_->next = node;
    node->prev = tail_;
    node->next = nullptr;
    tail_ = node;
  }

  void *Insert(Value value) {
    tail_->next = new Node;
    tail_->next->prev = tail_;
    tail_ = tail_->next;
    tail_->val = value;
    tail_->next = nullptr;

    return (void *)tail_;
  }

  bool Pop(Value &value) {
    if (head_ != tail_) {
      Node *del = head_->next;
      value = del->val;
      head_->next = del->next;
      if (del->next) {
        del->next->prev = head_;
      } else {
        tail_ = head_;
      }
      delete del;
      return true;
    }
    return false;
  }

 private:
  Node *head_;
  Node *tail_;
};

template <typename Value>
class CacheQueue {
 public:
  struct Node {
    Value val;
    Node *next;
  };

  CacheQueue() {
    head_ = nullptr;
    tail_ = nullptr;
    size_ = 0;
  }

  ~CacheQueue() {
    while (head_) {
      Node *del = head_;
      head_ = del->next;
      delete del;
      del = nullptr;
    }
    tail_ = nullptr;
    size_ = 0;
  }

  void Init() {
    head_ = new Node;
    tail_ = head_;
    head_->next = nullptr;
  }

  void Push(Value value) {
    tail_->next = new Node;
    tail_ = tail_->next;
    tail_->val = value;
    tail_->next = nullptr;
    ++size_;
  }

  bool Pop(Value &value) {
    if (head_ != tail_) {
      Node *del = head_->next;
      value = del->val;
      head_->next = del->next;
      if (del->next == nullptr) {
        tail_ = head_;
      }
      delete del;
      --size_;
      return true;
    }
    return false;
  }

  uint32_t size_;

 private:
  Node *head_;
  Node *tail_;
};

class MemoryPool {
 public:
  MemoryPool() {}

  ~MemoryPool() { ClearQueue(); }

  void Init(uint32_t max_cell_num, uint32_t cell_size) {
    // que_.Init();
    cell_size_ = cell_size;  // uint: byte
    max_cell_num_ = max_cell_num;
    LOG(INFO) << "MemoryPool info, cell_size_=" << cell_size_
              << ",max_cell_num_=" << max_cell_num_
              << ",use_cell_num_=" << use_cell_num_;
  }

  char *GetBuffer() {
    char *val = nullptr;
    if (que_.size() > 0) {
      val = que_.front();
      que_.pop();
    } else {
      val = new char[cell_size_];
      if (val == nullptr) {
        LOG(ERROR) << "lrucache GetBuffer failed, value size[" << cell_size_
                   << "]";
        return nullptr;
      }
    }
    ++use_cell_num_;
    return val;
  }

  void ReclaimBuffer(char *val) {
    // que_.Push(cell);
    que_.push(val);
    --use_cell_num_;
    if (use_cell_num_ + que_.size() > max_cell_num_) {
      char *del = nullptr;
      del = que_.front();
      que_.pop();
      if (del == nullptr) {
        LOG(ERROR) << "lrucache MemPool que_.front() is nullptr";
      } else {
        delete[] del;
        del = nullptr;
      }
    }
  }

  uint32_t SetMaxBufferNum(uint32_t max_cell_num) {
    // if (que_.size_ + use_cell_num_ <= max_cell_num) {
    if (que_.size() + use_cell_num_ <= max_cell_num) {
      max_cell_num_ = max_cell_num;
      return max_cell_num_;
    }

    uint32_t del_num = 0;
    if (use_cell_num_ > max_cell_num) {
      // del_num = que_.size_;
      del_num = que_.size();
    } else {
      // del_num = que_.size_ + use_cell_num_ - max_cell_num;
      del_num = que_.size() + use_cell_num_ - max_cell_num;
    }

    for (uint32_t i = 0; i < del_num; ++i) {
      char *del = nullptr;
      // que_.Pop(del);
      del = que_.front();
      que_.pop();
      delete[] del;
      del = nullptr;
    }
    max_cell_num_ -= del_num;
    return max_cell_num;
  }

  uint32_t UseBufferNum() { return use_cell_num_; }

 private:
  void ClearQueue() {
    while (que_.size() > 0) {
      char *del = nullptr;
      del = que_.front();
      que_.pop();
      delete[] del;
      del = nullptr;
    }
  }

  uint32_t cell_size_ = 0;
  uint32_t max_cell_num_ = 0;
  uint32_t use_cell_num_ = 0;

  // CacheQueue<void *> que_;
  std::queue<char *> que_;
};

template <typename Key, typename FuncToken,
          typename HashFunction = std::hash<Key>>
class CacheBase {
 public:
  using LoadFunc = bool (*)(Key, char *, FuncToken);
 protected:
  std::string name_;
  LoadFunc load_func_;
  size_t cell_size_;
  CacheType cache_type_;
  std::mutex mtx_;

 public:
  virtual ~CacheBase() {}

  virtual int Init() = 0;

  virtual bool Get(Key key, char *&value) { return true; }

  virtual void Set(Key key, char *value) {}

  virtual bool SetOrGet(Key key, char *&value, FuncToken token) = 0;

  virtual void Update(Key key, const char *buffer, int len, int begin_pos) = 0;

  virtual void AlterCacheSize(size_t cache_size) {}

  virtual int64_t GetMaxSize() { return 0; }

  virtual size_t Count() { return 0; }

  virtual size_t CacheMemBytes() { return 0; }

  virtual size_t GetHits() { return 0; }

  virtual size_t GetSetHits() { return 0; }

  virtual size_t GetMisses() { return 0; }

  virtual std::string &GetName() { return name_; }

  virtual CacheType GetCacheType() { return cache_type_; }

  LoadFunc GetLoadFun() { return load_func_; }

};

template <typename Key, typename FuncToken,
          typename HashFunction = std::hash<Key>>
class LRUCache final : public CacheBase<Key, FuncToken, HashFunction> {
 public:
  using LoadFunc = bool (*)(Key, char *, FuncToken);
  using QueueIterator = typename std::list<Key>::iterator;
  struct Cell {
    char *value = nullptr;
    QueueIterator que_ite;
    THRESHOLD_TYPE hits;
    // ~Cell() {
    //   if (value) {
    //     delete[] value;
    //     value = nullptr;
    //   }
    // }
  };

  struct InsertInfo {
    std::mutex mtx_;
    bool is_clean_ = false;
    bool is_product_ = false;
    Cell cell_;
  };

 private:
  size_t max_size_;
  MemoryPool mem_pool_;
  std::unordered_map<Key, std::shared_ptr<InsertInfo>, HashFunction>
      insert_infos_;
  size_t max_overflow_ = 0;
  size_t last_show_log_ = 1;
  std::atomic<size_t> cur_size_{0};
  std::atomic<size_t> hits_{0};
  std::atomic<size_t> misses_{0};
  std::atomic<size_t> set_hits_{0};
  size_t evict_num_ = 0;
  std::unordered_map<Key, Cell, HashFunction> cells_;

  // CacheList<Key> queue_;
  std::list<Key> queue_;
  // pthread_rwlock_t rw_lock_;

 public:
  LRUCache(std::string name, size_t cache_size, size_t cell_size,
           LoadFunc func) {
    this->name_ = name;
    this->cell_size_ = cell_size;
    this->cache_type_ = CacheType::LRUCacheType;
    max_size_ = (cache_size * 1024 * 1024) / cell_size;
    max_overflow_ = max_size_ / 20;
    if (max_overflow_ > 1000) {
      max_overflow_ = 1000;
    }
    max_size_ -= max_overflow_;
    this->load_func_ = func;
    LOG(INFO) << "LruCache[" << this->name_ << "] open! Max_size[" << max_size_
              << "], max_overflow[" << max_overflow_ << "]";
  }

  virtual ~LRUCache() {
    // pthread_rwlock_destroy(&rw_lock_);
    {
      std::lock_guard<std::mutex> lock(this->mtx_);
      Clean();
    }
    LOG(INFO) << "LruCache[" << this->name_ << "] destroyed successfully!";
  }

  int Init() {
    // queue_.Init();
    mem_pool_.Init((uint32_t)max_size_ + max_overflow_ + 500,
                   (uint32_t)(this->cell_size_));
    // int ret = pthread_rwlock_init(&rw_lock_, nullptr);
    // if (ret != 0) {
    //   LOG(ERROR) << "LruCache[" << name_
    //              << "] init read-write lock error, ret=" << ret;
    //   return 2;
    // }
    return 0;
  }

  bool Get(Key key, char *&value) {
    bool res;
    {
      std::lock_guard<std::mutex> lock(this->mtx_);
      // pthread_rwlock_rdlock(&rw_lock_);
      res = GetImpl(key, value);
      // pthread_rwlock_unlock(&rw_lock_);
    }

    if (res)
      ++hits_;
    else
      ++misses_;
    if (hits_ % 10000000 == 0 && hits_ != last_show_log_) {
      LOG(INFO) << "LruCache[" << this->name_ << "] cur_size[" << cur_size_
                << "] cells_size[" << cells_.size() << "] hits[" << hits_
                << "] set_hits[" << set_hits_ << "] misses[" << misses_
                << "] evict_num_[" << evict_num_ << "]";
      last_show_log_ = hits_;
    }
    return res;
  }

  void Set(Key key, char *value) {
    std::lock_guard<std::mutex> lock(this->mtx_);
    // pthread_rwlock_wrlock(&rw_lock_);
    SetImpl(key, value);
    // pthread_rwlock_unlock(&rw_lock_);
  }

  bool SetOrGet(Key key, char *&value, FuncToken token) {
    std::shared_ptr<InsertInfo> ptr_insert;
    {
      std::lock_guard<std::mutex> cache_lck(this->mtx_);
      // pthread_rwlock_wrlock(&rw_lock_);
      bool res = GetImpl(key, value);
      if (res) {
        // pthread_rwlock_unlock(&rw_lock_);
        ++hits_;
        return true;
      }

      auto &insert_info = insert_infos_[key];
      if (!insert_info) {
        insert_info = std::make_shared<InsertInfo>();
        insert_info->cell_.value = mem_pool_.GetBuffer();
      }
      ptr_insert = insert_info;
      // pthread_rwlock_unlock(&rw_lock_);
    }

    ++misses_;
    InsertInfo *insert = ptr_insert.get();
    std::lock_guard<std::mutex> insert_lck(insert->mtx_);

    if (insert->is_product_) {
      ++set_hits_;
      value = insert->cell_.value;
      return true;
    }
    bool res = this->load_func_(key, insert->cell_.value, token);
    if (res) {
      value = insert->cell_.value;
      insert->is_product_ = true;
    }

    std::lock_guard<std::mutex> cache_lck(this->mtx_);
    // pthread_rwlock_wrlock(&rw_lock_);
    auto ite = insert_infos_.find(key);
    if (res && ite != insert_infos_.end() && ite->second.get() == insert) {
      SetImpl(key, insert->cell_.value);
    } else {
      mem_pool_.ReclaimBuffer(insert->cell_.value);
      value = nullptr;
    }

    if (!ptr_insert->is_clean_) {
      insert->is_clean_ = true;
      insert_infos_.erase(key);
    }
    // pthread_rwlock_unlock(&rw_lock_);
    return res;
  }

  bool Get2(Key key, char *&value) {
    bool res;
    {
      std::lock_guard<std::mutex> lock(this->mtx_);
      res = GetImpl(key, value);
      if (res == false) {
        value = mem_pool_.GetBuffer();
        if (value == nullptr) {
          LOG(ERROR) << "lrucache[" << this->name_
                     << "] mem_pool GetBuffer error, buffer is nullptr:";
        }
      }
    }

    if (res)
      ++hits_;
    else
      ++misses_;
    if (hits_ % 10000000 == 0 && hits_ != last_show_log_) {
      LOG(INFO) << "LruCache[" << this->name_ << "] cur_size[" << cur_size_
                << "] cells_size[" << cells_.size() << "] hits[" << hits_
                << "] set_hits[" << set_hits_ << "] misses[" << misses_
                << "] evict_num_[" << evict_num_ << "]";
      last_show_log_ = hits_;
    }
    return res;
  }

  void Set2(Key key, char *buffer) {
    std::lock_guard<std::mutex> lock(this->mtx_);
    if (buffer == nullptr) {
      LOG(ERROR) << "lrucache[" << this->name_ << "] Set2 buffer is nullptr";
    }
    SetImpl(key, buffer);
  }

  void Evict(Key key) {
    std::lock_guard<std::mutex> lock(this->mtx_);
    // pthread_rwlock_wrlock(&rw_lock_);
    auto ite = cells_.find(key);
    if (ite == cells_.end()) {
      // pthread_rwlock_unlock(&rw_lock_);
      return;
    }
    Cell &del = ite->second;
    char *buffer = del.value;
    auto que_ite = del.que_ite;
    mem_pool_.ReclaimBuffer(buffer);
    queue_.erase(que_ite);
    cells_.erase(ite);
    --cur_size_;
    ++evict_num_;
    // pthread_rwlock_unlock(&rw_lock_);
  }

  void Update(Key key, const char *buffer, int len, int begin_pos) {
    std::shared_ptr<InsertInfo> ptr_insert;
    {
      std::lock_guard<std::mutex> lock(this->mtx_);
      auto insert_ite = insert_infos_.find(key);
      if (insert_ite == insert_infos_.end()) {
        auto ite = cells_.find(key);
        if (ite != cells_.end()) {
          Cell &cell = ite->second;
          memcpy(cell.value + begin_pos, buffer, len);
        }
        return;
      } else {
        ptr_insert = insert_ite->second;
      }
    }

    std::lock_guard<std::mutex> insert_lck(ptr_insert->mtx_);
    std::lock_guard<std::mutex> cache_lck(this->mtx_);
    auto ite = cells_.find(key);
    if (ite != cells_.end()) {
      Cell &cell = ite->second;
      memcpy(cell.value + begin_pos, buffer, len);
    }
  }

  void AlterCacheSize(size_t cache_size) {
    max_size_ = (cache_size * 1024 * 1024) / this->cell_size_;
    max_overflow_ = max_size_ / 20;
    if (max_overflow_ > 1000) {
      max_overflow_ = 1000;
    }
    max_size_ = max_size_ - max_overflow_;
    // pthread_rwlock_wrlock(&rw_lock_);
    std::lock_guard<std::mutex> lock(this->mtx_);
    EvictOverflow();
    mem_pool_.SetMaxBufferNum((uint32_t)max_size_ + 500);
    // pthread_rwlock_unlock(&rw_lock_);
    LOG(INFO) << "LruCache[" << this->name_ << "] Max_size[" << max_size_
              << "], max_overflow[" << max_overflow_ << "]";
  }

  int64_t GetMaxSize() { return (int64_t)max_size_ + (int64_t)max_overflow_; }

  size_t Count() const { return cur_size_; }

  size_t GetHits() { return hits_; }

  size_t GetSetHits() { return set_hits_; }

  size_t GetMisses() { return misses_; }

 private:
  bool GetImpl(const Key &key, char *&value) {
    auto ite = cells_.find(key);
    if (ite == cells_.end()) {
      return false;
    }
    Cell &cell = ite->second;
    value = cell.value;

    if (cell.hits >= THRESHOLD_OF_SWAP) {
      // pthread_rwlock_unlock(&rw_lock_);
      // pthread_rwlock_wrlock(&rw_lock_);
      // queue_.MoveToTail(cell->queue_ite);
      queue_.splice(queue_.end(), queue_, cell.que_ite);
      cell.hits = 0;
    } else {
      ++cell.hits;
    }
    return true;
  }

  void SetImpl(const Key &key, char *value) {
    auto res =
        cells_.emplace(std::piecewise_construct, std::forward_as_tuple(key),
                       std::forward_as_tuple());
    Cell &cell = res.first->second;
    bool inserted = res.second;

    if (inserted) {
      cell.value = value;
      // cell->queue_ite = queue_.Insert(key);
      cell.que_ite = queue_.insert(queue_.end(), key);
      cell.hits = 0;
      ++cur_size_;
      EvictOverflow();
    } else {
      if (cell.hits >= THRESHOLD_OF_SWAP) {
        // queue_.MoveToTail(cell->queue_ite);
        queue_.splice(queue_.end(), queue_, cell.que_ite);
        cell.hits = 0;
      } else {
        ++cell.hits;
      }
      mem_pool_.ReclaimBuffer(cell.value);
      cell.value = value;
    }
  }

  void Clean() {
    while (queue_.size() > 0) {
      auto key = queue_.front();
      auto ite = cells_.find(key);
      if (ite == cells_.end()) {
        LOG(ERROR) << "LruCache[" << this->name_ << "], cur_size[" << cur_size_
                    << "], cells_.size()[" << cells_.size() << "]."
                    << "Queue and map is inconsistent.";
      } else {
        if (ite->second.value) { delete[] ite->second.value; }
        cells_.erase(ite);
      }
      queue_.pop_front();
      --cur_size_;
    }
    if (cells_.size() > 0) {
      LOG(ERROR) << "cells_ size()[" << cells_.size() << "] != queue_.size()";
      for (auto iter = cells_.begin(); iter != cells_.end(); iter++) {
        if (iter->second.value) { delete[] iter->second.value; }
        cells_.erase(iter);
      }
    }
  }

  void EvictOverflow() {
    if (cur_size_ >= max_size_ + max_overflow_) {
      int evict_num = cur_size_ - max_size_;
      cur_size_ -= evict_num;

      int fail_pop_num = 0;
      Key key;
      for (int i = 0; i < evict_num; ++i) {
        // if (!queue_.Pop(key)) {
        if (queue_.empty()) {
          ++fail_pop_num;
          LOG(ERROR) << "Lrucache[" << this->name_ << "] queue_ is empty.";
          continue;
        }
        key = queue_.front();
        auto ite = cells_.find(key);
        if (ite == cells_.end()) {
          LOG(ERROR) << "LruCache[" << this->name_ << "], cur_size[" << cur_size_
                     << "], cells_.size()[" << cells_.size() << "]."
                     << "Queue and map is inconsistent.";
          continue;
          // abort();
        }
        mem_pool_.ReclaimBuffer((ite->second).value);
        cells_.erase(ite);
        queue_.pop_front();
      }
      cur_size_ += fail_pop_num;
      // malloc_trim(0);
    }
  }
};

template <typename Key, typename FuncToken,
          typename HashFunction = std::hash<Key>>
class SimpleCache final : public CacheBase<Key, FuncToken, HashFunction> {
 public:
  using LoadFunc = bool (*)(Key, char *, FuncToken);

  SimpleCache(std::string name, size_t cell_size, LoadFunc func,
              size_t segment_gap, size_t init_segment_capacity = 0) {
    this->name_ = name;
    this->cell_size_ = cell_size;
    this->load_func_ = func;
    this->cache_type_ = CacheType::SimpleCache;
    segment_gap_ = segment_gap;
    if (init_segment_capacity > 0) {
      init_segment_capacity_ = init_segment_capacity;
    }
  }

  ~SimpleCache() {
    for (int j = 0; j < segment_num_; ++j) {
      char **segment = segments_[j];
      if (segment) {
        for (int i = 0; i < segments_capacity_[j]; ++i) {
          if (segment[i]) {
            delete[] segment[i];
            segment[i] = nullptr;
          }
        }
        delete[] segment;
        segment = nullptr;
        segments_capacity_[j] = 0;
      }
    }
  }

  int Init() {
    segments_.resize(segment_num_, nullptr);
    segments_capacity_.resize(segment_num_, 0);
    char **segment =  new char*[init_segment_capacity_];
    if (not segment) {
      LOG(ERROR) << "SimpleCache[" << this->name_ << "] new char*[" 
                 << init_segment_capacity_ << "] fail.";
      return -1;
    }
    memset(segment, 0, sizeof(char *) * init_segment_capacity_);
    segments_[0] = segment;
    segments_capacity_[0] = init_segment_capacity_;
    return 0;
  }
  
  bool Get(Key key, char *&value) {
    if (key < 0) return false;
    size_t seg_id = key / segment_gap_;
    size_t id_in_seg = key % segment_gap_;
    char **segment = segments_[seg_id];
    if (segment && id_in_seg < (size_t)segments_capacity_[seg_id]
          && segment[id_in_seg]) {
      value = segment[id_in_seg];
      return true;
    }
    return false;
  }

  bool SetOrGet(Key key, char *&value, FuncToken token) {
    if (key < 0) return false;
    size_t seg_id = key / segment_gap_;
    size_t id_in_seg = key % segment_gap_;
    char **segment = segments_[seg_id];
    if (segment && id_in_seg < (size_t)segments_capacity_[seg_id]
          && segment[id_in_seg]) {
      value = segment[id_in_seg];
      return true;
    }
    
    char *cell = new char[this->cell_size_];
    if (not cell) {
      LOG(ERROR) << "SimpleCache[" << this->name_ << "] new char[" 
                << this->cell_size_ << "] fail.";
      return false;
    }
    bool res = this->load_func_(key, cell, token);
    if (res == false) {
      LOG(INFO) << "SimpleCache[" << this->name_ << "] load_func_ fail.";
      delete[] cell;
      cell = nullptr;
      return false;
    }
    
    std::lock_guard<std::mutex> cache_lck(this->mtx_);
    res = SetImpl(key, cell);
    if(res) value = segments_[seg_id][id_in_seg];
    return res;
  }

  void Update(Key key, const char *buffer, int len, int begin_pos) {
    if (key < 0) return;
    std::lock_guard<std::mutex> cache_lck(this->mtx_);
    int seg_id = key / segment_gap_;
    size_t id_in_seg = key % segment_gap_;
    char **segment = segments_[seg_id];
    if (segment == nullptr || id_in_seg >= (size_t)segments_capacity_[seg_id] ||
        segment[id_in_seg] == nullptr) { return; }

    memcpy(segment[id_in_seg] + begin_pos, buffer, len);
  }

  size_t CacheMemBytes() {
    size_t total_mem = cells_num_ * this->cell_size_;
    total_mem += (segments_.size() * (sizeof(char **) + sizeof(int)));
    for (int i = 0; i < segment_num_; ++i) {
      if (segments_capacity_[i] > 0) {
        total_mem += (segments_capacity_[i] * sizeof(char *));
      }
    }
    return total_mem;
  }

  int64_t GetMaxSize() { return -1; }

 private:
  bool SetImpl(Key key, char *value) {
    size_t seg_id = key / segment_gap_;
    size_t id_in_seg = key % segment_gap_;
    if (segments_[seg_id] == nullptr) {
      char **segment =  new char*[init_segment_capacity_];
      if (not segment) {
        LOG(ERROR) << "SimpleCache[" << this->name_ << "] new char*[" 
                   << init_segment_capacity_ << "] fail.";
        return -1;
      }
      memset(segment, 0, sizeof(char *) * init_segment_capacity_);
      segments_[seg_id] = segment;
      segments_capacity_[seg_id] = init_segment_capacity_;
    }

    if ((size_t)segments_capacity_[seg_id] <= id_in_seg) {
      size_t capacity = id_in_seg * 1.5;

      char **new_segment = new char*[capacity];
      if (new_segment == nullptr) {
        LOG(INFO) << "SimpleCache[" << this->name_ << "] new char*[" << capacity
                  << "] fail.";
        delete[] value;
        value = nullptr;
        return false; 
      }
      memset(new_segment, 0, sizeof(char *) * capacity);
      memcpy(new_segment, segments_[seg_id], sizeof(char *) * segments_capacity_[seg_id]);
      char **del = segments_[seg_id];
      segments_[seg_id] = new_segment;
      segments_capacity_[seg_id] = capacity;
      // delay free
      utils::AsyncWait(1000 * 100, [](char **del_buf) { delete[] del_buf; },
                       del);  // after 100s
    }

    if (segments_[seg_id][id_in_seg]) {
      memcpy(segments_[seg_id][id_in_seg], value, this->cell_size_);
      delete[] value;
      value = nullptr;
    } else {
      segments_[seg_id][id_in_seg] = value;
      ++cells_num_;
    }
    return true;
  }

  std::vector<char **> segments_;
  std::vector<int> segments_capacity_;
  size_t segment_gap_;
  size_t init_segment_capacity_ = 512;
  uint16_t segment_num_ = 400;
  size_t cells_num_ = 0;
};
