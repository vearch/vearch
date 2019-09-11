/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifndef SRC_SEARCHER_INDEX_NUMERIC_SKIPLIST_INDEX_H_
#define SRC_SEARCHER_INDEX_NUMERIC_SKIPLIST_INDEX_H_

#include <cassert>
#include <cstddef>
#include <cstdint>

#include <algorithm>
#include <atomic>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

#include "random.h"

namespace tig_gamma {
namespace NI {

// A compromise must be made between performance and memory.
// When the dataset is dense, there will be less memory waste, with high
// performance. otherwise, ...
const int kDupNodeSize = 512;

//======================================================================
// A Simple SkipList Index Implement
//======================================================================
template <typename T> struct DupNode {
  explicit DupNode(const T _1st) : next(nullptr) {
    std::fill_n(ids, kDupNodeSize, -1);
    ids[0] = _1st;
  }

  T ids[kDupNodeSize];
  DupNode *next;
};

template <typename T> struct DupList {
  DupList() : head(nullptr), size_(0) {}

  DupNode<T> *head;

  bool Add(T x) {
    int pos = size_ % kDupNodeSize + 1;
    if (pos == kDupNodeSize) {
      return false; // require to Insert() a new node
    }

    Tail()->ids[pos] = x;
    size_++;
    return true;
  }

  void Insert(DupNode<T> *x) {
    // while searching, only Tail() will be accessible,
    // so its *next* pointer can be safely modified.
    Tail()->next = x;
    SetTail(x);
    size_++;
  }

  void SetTail(DupNode<T> *x) { tail_.store(x, std::memory_order_release); }
  DupNode<T> *Tail() { return tail_.load(std::memory_order_acquire); }

  int Size() const { return size_; }

private:
  std::atomic<DupNode<T> *> tail_;

  // only used to locate the insertion slot in the rt.Insert() thread.
  std::atomic<int> size_;
};

template <typename K, typename V> struct Node {
  Node(int level, K k, V v) : level(level), key(k), value(v) {}

  int level; // used for OutputNode

  K key;
  V value;

  Node *Next(int n) {
    assert(n >= 0);
    return (next_[n].load(std::memory_order_acquire));
  }
  void SetNext(int n, Node *x) {
    assert(n >= 0);
    next_[n].store(x, std::memory_order_release);
  }
  Node *NoBarrier_Next(int n) {
    assert(n >= 0);
    return (next_[n].load(std::memory_order_relaxed));
  }
  void NoBarrier_SetNext(int n, Node *x) {
    assert(n >= 0);
    next_[n].store(x, std::memory_order_relaxed);
  }

  void SetDup(DupList<V> *x) { dup_.store(x, std::memory_order_release); }
  DupList<V> *Dup() { return (dup_.load(std::memory_order_acquire)); }

  void NoBarrier_SetDup(DupList<V> *x) {
    dup_.store(x, std::memory_order_relaxed);
  }

private:
  std::atomic<DupList<V> *> dup_; // store duplicates if any

  // array of length equal to the node level + 1. next_[0] is lowest level link.
  std::atomic<Node *> next_[1];
};

template <typename K, typename V> class SkipList {
public:
  SkipList()
      : allow_dup_(false), head_(nullptr), tail_(nullptr), random_(0x12345678),
        alloc_bytes_(0) {
    K tail_key = std::numeric_limits<K>::max();
    CreateList(tail_key);
  }

  ~SkipList() { DestroyList(); }

  Node<K, V> *Search(const K key) const;

  Node<K, V> *FindGreaterOrEqual(const K key) const;
  Node<K, V> *FindLessOrEqual(const K key) const;

  bool Insert(K key, V value);
  bool Remove(K key, V &value);

  int Size() const {
    return (size_.load(std::memory_order_relaxed)); //
  }
  int Level() const {
    return (level_.load(std::memory_order_relaxed)); //
  }

  // the first node that contains a value
  Node<K, V> *First() const { return head_->Next(0); }
  // the last node that contains a value, the one previous to tail_
  Node<K, V> *Last() const { return head_->Next(kMaxHeight_); }

  void Reset() {
    DestroyList();

    K tail_key = std::numeric_limits<K>::max();
    CreateList(tail_key);
  }

  void SetAllowDup(bool value) { allow_dup_ = value; }

  void Output(const std::string &tag) {
    std::cout << "Num of nodes: " << Size() << ", level: " << Level()
              << ", memory usage: " << MemoryUsage() << " bytes.\n";
    std::cout << "---------- only list the first 10 nodes -----------\n";
    OutputAllNodes(tag, 10);
  }

  size_t MemoryUsage() const { return alloc_bytes_; }

private:
  void CreateList(K tail_key);
  void DestroyList();

  Node<K, V> *CreateNode(int level, K key, V value);
  Node<K, V> *CreateNode(int level) {
    return CreateNode(level, 0, 0); // k & v always 0, it's ok
  }

  int RandomLevel();

  DupList<V> *CreateDupList(V value);
  DupNode<V> *CreateDupNode(V value);

private:
  // No copying allowed
  SkipList(const SkipList &);
  void operator=(const SkipList &);

private:
  // TODO use Comparator
  // Comparator _compare;
  // bool Equal(const K &a, const K &b) const { return (_compare(a, b) == 0); }
  // bool LessThan(const K &a, const K &b) const { return (_compare(a, b) < 0);
  // }

private:
  static const int kMaxHeight_ = 16;

  bool allow_dup_; // Whether a duplicate key insertion is allowed

  Node<K, V> *head_;
  Node<K, V> *tail_;

  std::atomic<int> level_; // Height of the entrie list
  std::atomic<size_t> size_;

  utils::Random random_;

private:
  // TODO use memory allocator
  std::vector<char *> mems_;
  size_t alloc_bytes_;

  char *Allocate(size_t bytes) {
    char *mem = static_cast<char *>(malloc(bytes));
    assert(mem != nullptr);
    mems_.emplace_back(mem);
    alloc_bytes_ += bytes;
    return mem;
  }

private:
  void OutputAllNodes(const std::string &tag, int num = -1);
  void OutputNode(Node<K, V> *node);
};

template <typename K, typename V> void SkipList<K, V>::CreateList(K tail_key) {
  tail_ = CreateNode(0);

  tail_->key = tail_key;
  tail_->SetNext(0, nullptr);

  head_ = CreateNode(kMaxHeight_);

  for (int i = 0; i < kMaxHeight_ + 1; ++i) {
    head_->SetNext(i, tail_);
  }

  level_ = 0;
  size_ = 0;
}

template <typename K, typename V>
Node<K, V> *SkipList<K, V>::CreateNode(int level, K key, V value) {
  char *mem = Allocate(sizeof(Node<K, V>) +
                       (1 + level) * sizeof(std::atomic<Node<K, V> *>));
  return new (mem) Node<K, V>(level, key, value);
}

template <typename K, typename V>
DupList<V> *SkipList<K, V>::CreateDupList(V value) {
  char *mem = Allocate(sizeof(DupList<V>));
  DupList<V> *lst = new (mem) DupList<V>();

  // set head & tail of dup list
  lst->head = CreateDupNode(value);
  lst->SetTail(lst->head);
  return lst;
}

template <typename K, typename V>
DupNode<V> *SkipList<K, V>::CreateDupNode(V value) {
  char *mem = Allocate(sizeof(DupNode<V>));
  return new (mem) DupNode<V>(value);
}

template <typename K, typename V> void SkipList<K, V>::DestroyList() {
  for (auto &x : mems_) {
    free(x);
  }
  mems_.clear();
}

template <typename K, typename V> int SkipList<K, V>::RandomLevel() {
  int level = static_cast<int>(random_.Uniform(kMaxHeight_));
  return (level == 0) ? 1 : level;
}

template <typename K, typename V>
Node<K, V> *SkipList<K, V>::Search(const K key) const {
  Node<K, V> *node = head_;
  for (int i = Level(); i >= 0; --i) {
    Node<K, V> *next = node->Next(i);

    while (next->key < key) {
      node = next;
      next = node->Next(i);
    }
  }

  node = node->Next(0);
  return ((node->key == key) ? node : nullptr);
}

template <typename K, typename V>
Node<K, V> *SkipList<K, V>::FindLessOrEqual(const K key) const {
  Node<K, V> *node = head_;
  for (int i = Level(); i >= 0; --i) {
    Node<K, V> *next = node->Next(i);

    while (next->key < key) {
      node = next;
      next = node->Next(i);
    }
  }

  Node<K, V> *prev = node; // the last node less than *key*
  node = node->Next(0);

  return ((node->key == key) ? node : prev);
}

template <typename K, typename V>
Node<K, V> *SkipList<K, V>::FindGreaterOrEqual(const K key) const {
  Node<K, V> *node = head_;
  for (int i = Level(); i >= 0; --i) {
    Node<K, V> *next = node->Next(i);

    while (next->key < key) {
      node = next;
      next = node->Next(i);
    }
  }

  node = node->Next(0);
  return node;
}

template <typename K, typename V> bool SkipList<K, V>::Insert(K key, V value) {
  Node<K, V> *update[kMaxHeight_];

  Node<K, V> *node = head_;

  for (int i = Level(); i >= 0; --i) {
    Node<K, V> *next = node->Next(i);

    while (next->key < key) {
      node = next;
      next = node->Next(i);
    }

    update[i] = node;
  }

  // for the first insertion, node->Next(0) is the tail_
  node = node->Next(0);

  // return false if key already exists
  if (node->key == key) {
    if (not allow_dup_)
      return false;

    auto dup = node->Dup();
    if (dup) {
      if (not dup->Add(value)) {
        auto x = CreateDupNode(value);
        dup->Insert(x);
      }
    } else {
      auto x = CreateDupList(value);
      node->SetDup(x);
    }

    return true;
  }

  int nodeLevel = RandomLevel();

  if (nodeLevel > Level()) {
    nodeLevel = ++level_;
    update[nodeLevel] = head_;
  }

  // create a new node
  Node<K, V> *x = CreateNode(nodeLevel, key, value);

  // initialize to a node without duplicate values
  x->NoBarrier_SetDup(nullptr);

  // adjust the pointer to next
  for (int i = nodeLevel; i >= 0; --i) {
    node = update[i];

    x->NoBarrier_SetNext(i, node->NoBarrier_Next(i));
    node->SetNext(i, x);
  }

  // update the last pointer while inserting the last valid node
  // TODO no barrier ???
  if (x->Next(0) == tail_) {
    head_->SetNext(kMaxHeight_, x);
  }

  ++size_;

#ifdef DEBUG
  OutputAllNodes(__func__);
#endif

  return true;
}

template <typename K, typename V> bool SkipList<K, V>::Remove(K key, V &value) {
  Node<K, V> *update[kMaxHeight_];
  Node<K, V> *node = head_;
  for (int i = Level(); i >= 0; --i) {
    Node<K, V> *next = node->Next(i);

    while (next->key < key) {
      node = next;
      next = node->Next(i);
    }

    update[i] = node;
  }

  node = node->Next(0);

  // return false if node doesn't exist
  if (node->key != key) {
    return false;
  }

  value = node->value;

  for (int i = 0; i <= Level(); ++i) {
    if (update[i]->Next(i) != node) {
      break;
    }
    update[i]->SetNext(i, node->Next(i));
  }

  node->~Node();

  while (level_ > 0 && head_->Next(level_) == tail_) {
    --level_;
  }

  --size_;

#ifdef DEBUG
  OutputAllNodes(__func__);
#endif

  return true;
}

template <typename K, typename V>
void SkipList<K, V>::OutputAllNodes(const std::string &tag, int num) {
  std::cout << tag << " => ";
  Node<K, V> *tmp = head_;

  Node<K, V> *next = tmp->Next(0);
  while (num-- && next != tail_) {
    tmp = next;
    next = tmp->Next(0);

    OutputNode(tmp);
    std::cout << "\t-------------------------------------\n";
  }

  std::cout << "\n";
}

template <typename K, typename V>
void SkipList<K, V>::OutputNode(Node<K, V> *node) {
  if (node == nullptr) {
    return;
  }

  std::cout << "\tnode->key:" << node->key << ",node->value:" << node->value
            << "\n";

  for (int i = 0; i <= node->level; ++i) {
    Node<K, V> *tmp = node->Next(i);

    std::cout << "\t\tforward[" << i << "]:"
              << "key:" << tmp->key << ",value:" << tmp->value << "\n";
  }
}

} // namespace NI
} // namespace tig_gamma

#endif // SRC_SEARCHER_INDEX_NUMERIC_SKIPLIST_INDEX_H_
