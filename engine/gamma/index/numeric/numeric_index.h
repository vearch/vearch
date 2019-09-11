/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifndef SRC_SEARCHER_INDEX_NUMERIC_NUMERIC_INDEX_H_
#define SRC_SEARCHER_INDEX_NUMERIC_NUMERIC_INDEX_H_

#include <string.h> // for memcpy

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <typeinfo>

#include "log.h"
#include <algorithm>
#include <functional>
#include <iostream>
#include <iterator>
#include <limits>
#include <map>
#include <numeric>
#include <sstream>
#include <string>
#include <vector>

#include "numeric_struct.h"
#include "skiplist_index.h"

#include "timer.h" // for Timer

namespace tig_gamma {
namespace NI {

//---------------------------------------------------------------
// Numeric Index Implement: externally *invisible*
//---------------------------------------------------------------
namespace {

typedef int64_t Long; // make cpplint happy

// used for dump & load
template <typename T> const char *TypeName() { return typeid(T).name(); }

template <> const char *TypeName<int>() { return "I"; }
template <> const char *TypeName<Long>() { return "L"; }
template <> const char *TypeName<float>() { return "F"; }
template <> const char *TypeName<double>() { return "D"; }

template <typename T> bool lexical_cast(const std::string &input, T &output) {
  std::istringstream is;
  is.str(input);
  is >> output;
  return !is.fail() && !is.rdbuf()->in_avail();
}

struct Block {
  Long offset; // offset in docValues
  int size;

  int min_doc;
  int max_doc;
};

template <typename T> struct BlockSkipListIndex {
  BlockSkipListIndex() : size(0) {}

  std::vector<int> docIDs; // sorted by docValues
  Long size;

  T min_value;
  T max_value;

  std::vector<Block> blocks;
  SkipList<T, int> index;
};

template <typename T> class NumericIndex : public Index {
public:
  explicit NumericIndex(const std::string &field);

  int Search(const std::string &lowerValue, const std::string &upperValue,
             RangeQueryResultV1 &result) const override {
    T l, u;
    bool rv = lexical_cast(lowerValue, l) && lexical_cast(upperValue, u);
    if (rv) {
      return Search(l, u, result);
    }
    return -1;
  }

  void Add(const std::string &bytes, int docID) override {
    T value;
    memcpy(&value, bytes.data(), sizeof(T));
    Add(value, docID);
  }

  void Add(T value, int docID) {
    // only rt
    rt_idx_.Insert(value, docID);
  }

  int Build(const int num_docs) override;
  void Output(const std::string &tag) override;

  int Dump(const IndexIO &out) override;
  int Load(const IndexIO &in) override;

  // use callback instead of _raw
  void Set(std::function<T(const int)> callback) {
    get_value_ = std::move(callback);
  }

  size_t MemoryUsage() const override {
    size_t bytes = bsl_idx_.size * sizeof(int) +
                   bsl_idx_.blocks.size() * sizeof(Block) +
                   bsl_idx_.index.MemoryUsage();
    bytes += rt_idx_.MemoryUsage();
    return bytes;
  }

private:
  int Search(const T lowerValue, const T upperValue,
             RangeQueryResultV1 &result) const;

  int Search(const BlockSkipListIndex<T> *index, const T lowerValue,
             const T upperValue, RangeQueryResultV1 &result) const;

  int Search(const SkipList<T, int> *rt_idx, const T lowerValue,
             const T upperValue, RangeQueryResultV1 &result) const;

  // similar to std::lower_bound & std::upper_bound
  Long LowerBound(std::function<T(int)> get_value, Long first, Long last,
                  const T &value) const;
  Long UpperBound(std::function<T(int)> get_value, Long first, Long last,
                  const T &value) const;

private:
  // used to get the actual specific field data from the outer
  std::function<T(const int)> get_value_;
  Long size_;

  static const int kBlockSize_ = 1024;

  // TODO dynamically merging
  BlockSkipListIndex<T> bsl_idx_;
  SkipList<T, int> rt_idx_;
};

template <typename T>
NumericIndex<T>::NumericIndex(const std::string &field) : size_(0) {
  field_.name = field;
  const char *type = TypeName<T>();

  // it's enough to compare the first char only
  switch (type[0]) {
  case 'I':
    field_.type = IndexFieldType::INT;
    break;
  case 'L':
    field_.type = IndexFieldType::LONG;
    break;
  case 'F':
    field_.type = IndexFieldType::FLOAT;
    break;
  case 'D':
    field_.type = IndexFieldType::DOUBLE;
    break;
  default:
    field_.type = IndexFieldType::UNKNOWN;
    break;
  }

  rt_idx_.SetAllowDup(true);
}

template <typename T> int NumericIndex<T>::Build(const int num_docs) {
  if (num_docs < 1) {
    return -1;
  }
  size_ = num_docs;

  bsl_idx_.size = size_;

  bsl_idx_.docIDs.resize(size_);
  std::iota(bsl_idx_.docIDs.begin(), bsl_idx_.docIDs.end(), 0);

  // sort _docIDs by _raw, use get_value_(i) instead of _raw[i]
  std::sort(bsl_idx_.docIDs.begin(), bsl_idx_.docIDs.end(),
            [&](int i, int j) { return get_value_(i) < get_value_(j); });

  std::function<T(int)> get_value = [&](int n) -> T {
    int docid = bsl_idx_.docIDs[n]; // for performance, no validity checking
    return get_value_(docid);
  };

  bsl_idx_.min_value = get_value(0);
  bsl_idx_.max_value = get_value(size_ - 1);

  std::cout << "min_value=" << bsl_idx_.min_value
            << ", max_value=" << bsl_idx_.max_value << "\n";

  // split to blocks
  bsl_idx_.blocks.reserve(1 + size_ / kBlockSize_);
  int nBlock = 0;

  T curr_blk_start = get_value(0);

  Long i = 0;
  Block blk;

  while (i < size_) {
    bsl_idx_.index.Insert(curr_blk_start, nBlock++);

    blk.offset = i;
    i += kBlockSize_;

    // the last block
    if (i >= size_) {
      blk.size = size_ - blk.offset;
      bsl_idx_.blocks.emplace_back(blk);
      break;
    }

    // keep the same values in the same block
    T prev_blk_end = get_value(i - 1);
    curr_blk_start = get_value(i);

    while (curr_blk_start == prev_blk_end) {
      i += 1;
      if (i >= size_) {
        break;
      }
      curr_blk_start = get_value(i);
    }

    blk.size = i - blk.offset;
    bsl_idx_.blocks.emplace_back(blk);
  }

  bsl_idx_.blocks.shrink_to_fit();

  // calculate min_doc & max_doc in each block
  // [begin, end)
  for (auto &blk : bsl_idx_.blocks) {
    auto begin = bsl_idx_.docIDs.cbegin() + blk.offset;
    auto end = begin + blk.size;

    blk.min_doc = *(std::min_element(begin, end));
    blk.max_doc = *(std::max_element(begin, end));

#ifdef DEBUG
    std::cout << "min_doc=" << blk.min_doc << ", max_doc=" << blk.max_doc
              << "\n";
#endif
  }

  std::cout << "Num of blocks: " << bsl_idx_.blocks.size()
            << ", capacity: " << bsl_idx_.blocks.capacity() << "\n";
  return 0;
}

template <typename T> void NumericIndex<T>::Output(const std::string &tag) {
  bsl_idx_.index.Output(tag);

  std::cout << "Num of blocks: " << bsl_idx_.blocks.size() << "\n";
  std::cout << "-------- only list the first 10 blocks ---------\n";
  int count = 0;
  for (auto &blk : bsl_idx_.blocks) {
    std::cout << "\toffset:" << blk.offset << ", size:" << blk.size << "\n";
    if (count++ > 10) {
      break;
    }
  }

  size_t bytes = rt_idx_.MemoryUsage();
  float MB = 1.0 * bytes / 1048576;
  std::cout << tag << " -> rt size: " << rt_idx_.Size()
            << ", memory usage: " << bytes << " bytes (" << MB << " MB).\n";
}

template <typename T> int NumericIndex<T>::Dump(const IndexIO &out) {
  // TODO
  // for now, dynamically Build() after the external load is complete
  return 0;
}

template <typename T> int NumericIndex<T>::Load(const IndexIO &in) {
  // TODO
  // for now, dynamically Build() after the external load is complete
  return 0;
}

template <typename T>
Long NumericIndex<T>::LowerBound(std::function<T(int)> get_value, Long first,
                                 Long last, const T &value) const {
  // optimize
  if (get_value(first) >= value) {
    return first;
  } else if (get_value(last - 1) < value) {
    return last;
  }

  Long step(0), it(0);
  Long count = last - first;

  while (count > 0) {
    step = count / 2;
    it = first + step;
    if (get_value(it) < value) {
      first = ++it;
      count -= step + 1;
    } else {
      count = step;
    }
  }

  return first;
}

template <typename T>
Long NumericIndex<T>::UpperBound(std::function<T(int)> get_value, Long first,
                                 Long last, const T &value) const {
  // optimize
  if (get_value(first) > value) {
    return first;
  } else if (get_value(last - 1) <= value) {
    return last;
  }

  Long step(0), it(0);
  Long count = last - first;

  while (count > 0) {
    step = count / 2;
    it = first + step;
    if (!(value < get_value(it))) {
      first = ++it;
      count -= step + 1;
    } else {
      count = step;
    }
  }

  return first;
}

static void SetBitmap(const std::vector<int> &docIDs, Long begin, Long end,
                      RangeQueryResultV1 &result) {
  if (docIDs.empty())
    return;

  // utils::Timer t;
  // t.Start("SetBitmap");

  int min_doc = result.Min();

  // for (auto& docID : docIDs) {
  //     int pos = docID - min_doc;
  //     result.Set(pos);
  // }

  // BLOCK: optimized code of the above
  if (end > begin) {
#define SET_bitmap                                                             \
  do {                                                                         \
    int pos = docIDs[i] - min_doc;                                             \
    result.Set(pos);                                                           \
    i++;                                                                       \
  } while (0)

    // Duff's device, count must be greater than 0
    register int count = static_cast<int>(end - begin);
    register int n = (count + 7) / 8;
    register int i = begin;

    switch (count % 8) {
    case 0:
      do {
        SET_bitmap;
      case 7:
        SET_bitmap;
      case 6:
        SET_bitmap;
      case 5:
        SET_bitmap;
      case 4:
        SET_bitmap;
      case 3:
        SET_bitmap;
      case 2:
        SET_bitmap;
      case 1:
        SET_bitmap;
      } while (--n > 0);
    }
#undef SET_bitmap
  }

  // t.Stop();
  // t.Output();
}

template <typename T>
int NumericIndex<T>::Search(const T lowerValue, const T upperValue,
                            RangeQueryResultV1 &result) const {
  result.Clear();

  if (lowerValue > upperValue) {
    return 0; // no result
  }

  T min_value;
  T max_value;

  int rt_size = rt_idx_.Size();
  if (bsl_idx_.size < 1 && rt_size < 1) {
    return -1; // all result
  }

  if (bsl_idx_.size > 0 && rt_size > 0) {
    min_value = std::min(bsl_idx_.min_value, rt_idx_.First()->key);
    max_value = std::max(bsl_idx_.max_value, rt_idx_.Last()->key);
  } else if (bsl_idx_.size > 0) {
    min_value = bsl_idx_.min_value;
    max_value = bsl_idx_.max_value;
  } else if (rt_size > 0) {
    min_value = rt_idx_.First()->key;
    max_value = rt_idx_.Last()->key;
  }

  if (lowerValue > max_value || upperValue < min_value) {
    return 0; // no result
  }
  if (lowerValue <= min_value && max_value <= upperValue) {
    return -1; // all result
  }

  // WARNING: ensure a search request always retriving from the same time index
  int count = 0;

  if ((result.Flags() & 0x1) && (bsl_idx_.size > 0)) {
    count += Search(&bsl_idx_, lowerValue, upperValue, result);
  }
  if ((result.Flags() & 0x2) && (rt_size > 0)) {
    count += Search(&rt_idx_, lowerValue, upperValue, result);
  }

  return count;
}

template <typename T>
int NumericIndex<T>::Search(const SkipList<T, int> *rt_idx, const T lowerValue,
                            const T upperValue,
                            RangeQueryResultV1 &result) const {
  // std::cout << "[INFO] search rt-skiplist index.\n";
  assert(rt_idx != nullptr);

  if (rt_idx->Size() < 1) {
    return -1;
  }

  T min_value = rt_idx->First()->key;
  T max_value = rt_idx->Last()->key;

  Node<T, int> *left = nullptr;
  Node<T, int> *right = nullptr;

  if (lowerValue < min_value /*&& upperValue <= max_value*/) {
    left = rt_idx->First();
    right = rt_idx->FindLessOrEqual(upperValue);
  } else if (/*lowerValue >= min_value &&*/ upperValue > max_value) {
    left = rt_idx->FindGreaterOrEqual(lowerValue);
    right = rt_idx->Last();
  } else { // min_value <= lowerValue && upperValue <= max_value
    left = rt_idx->FindGreaterOrEqual(lowerValue);
    right = rt_idx->FindLessOrEqual(upperValue);
  }

  assert(left != nullptr && right != nullptr);

  Node<T, int> *end = right->Next(0); // open interval

  // TODO change to dynamic growth bitmap ?
  std::vector<int> docIDs; // unordered

  // I want to build a smaller bitmap ...
  int min_doc = std::numeric_limits<int>::max();
  int max_doc = 0;

  // utils::Timer t;
  // t.Start("visit");

  // docid list of different values (docid is disordered, docid is always
  // smaller than docid with the same value)
  for (; left != end; left = left->Next(0)) {
    docIDs.emplace_back(left->value);

    if (left->value < min_doc) {
      min_doc = left->value;
    }

    if (max_doc < left->value) {
      max_doc = left->value;
    }

    // docid list with the same value (docid is in order, and the docid of the
    // last node with the same value is the largest).
    // WARNING: note thread safe of the node access
    auto dup = left->Dup();
    if (dup) {
      auto node = dup->head;
      for (auto tail = dup->Tail(); node != tail; node = node->next) {
        docIDs.insert(docIDs.end(), std::begin(node->ids), std::end(node->ids));
      }

      for (auto id : node->ids) {
        if (id < 0)
          break;

        docIDs.emplace_back(id);
      }

      if (max_doc < docIDs.back()) {
        max_doc = docIDs.back();
      }
    }
  }

  // t.Stop();
  // t.Output();

  if (docIDs.empty()) {
    return 0; // no result
  }

  // re-build docID's bitmap
  result.SetRange(min_doc, max_doc);
  result.Resize();

  SetBitmap(docIDs, 0, docIDs.size(), result);

  return static_cast<int>(docIDs.size());
}

template <typename T>
int NumericIndex<T>::Search(const BlockSkipListIndex<T> *bsl_idx,
                            const T lowerValue, const T upperValue,
                            RangeQueryResultV1 &result) const {
  // std::cout << "[INFO] search block-skiplist index.\n";
  assert(bsl_idx != nullptr);

  if (bsl_idx->size < 1) {
    return -1;
  }

  T min_value = bsl_idx->min_value;
  T max_value = bsl_idx->max_value;

  // if (lowerValue > max_value || upperValue < min_value) {
  //  return 0; // no result
  //}
  // if (lowerValue <= min_value && max_value <= upperValue) {
  //  return -1; // all result
  //}

  std::function<T(int)> get_value = [&](int n) -> T {
    int docid = bsl_idx->docIDs[n]; // for performance, no validity checking
    return get_value_(docid);
  };

  // calculate [begin, end)
  Long begin = -1, end = -1;
  // [lv, uv] used to optimize bitmap size
  int lv = -1, uv = -1;

  Node<T, int> *node(nullptr);
  Long first = -1, last = -1;

  if (lowerValue < min_value /*&& upperValue <= max_value*/) {
    lv = 0;
    begin = 0;

    node = bsl_idx->index.FindLessOrEqual(upperValue);

    uv = node->value; // last block
    first = bsl_idx->blocks[uv].offset;
    last = first + bsl_idx->blocks[uv].size;

    end = UpperBound(get_value, first, last, upperValue);

  } else if (/*lowerValue >= min_value &&*/ upperValue > max_value) {
    uv = bsl_idx->blocks.size() - 1;
    end = bsl_idx->size;

    node = bsl_idx->index.FindLessOrEqual(lowerValue);

    lv = node->value; // 1st block
    first = bsl_idx->blocks[lv].offset;
    last = first + bsl_idx->blocks[lv].size;

    begin = LowerBound(get_value, first, last, lowerValue);

  } else { // min_value <= lowerValue && upperValue <= max_value
    node = bsl_idx->index.FindLessOrEqual(lowerValue);

    lv = node->value; // 1st block
    first = bsl_idx->blocks[lv].offset;
    last = first + bsl_idx->blocks[lv].size;

    // Lowerbound returns the first iterator that is not less than (that is,
    // greater than or equal to) lowervalue
    begin = LowerBound(get_value, first, last, lowerValue);

    node = bsl_idx->index.FindLessOrEqual(upperValue);

    uv = node->value; // last block
    first = bsl_idx->blocks[uv].offset;
    last = first + bsl_idx->blocks[uv].size;

    // Upperbound returns the first iterator larger than uppervalue
    end = UpperBound(get_value, first, last, upperValue);
  }

  if (end == begin) {
    return 0; // no result
  }

  // assign DocIDs
  // docIDs.insert(docIDs.end(), bsl_idx->docIDs.cbegin() + begin,
  //              bsl_idx->docIDs.cbegin() + end);

  // I want to build a smaller bitmap ...
  int min_doc = bsl_idx->size - 1;
  int max_doc = 0;

  for (auto i = lv; i <= uv; i++) {
    auto &b = bsl_idx->blocks[i];
    min_doc = std::min(min_doc, b.min_doc);
    max_doc = std::max(max_doc, b.max_doc);
  }

  // build docID's bitmap
  result.SetRange(min_doc, max_doc);
  result.Resize();

  SetBitmap(bsl_idx->docIDs, begin, end, result);

  return (end - begin);
}

} // anonymous namespace

//---------------------------------------------------------------
// Index API: externally visible
//---------------------------------------------------------------
struct FilterInfo {
  std::string field;
  std::string lower_value;
  std::string upper_value;
  int is_union;
};

class Indexes {
public:
  Indexes() {}

  ~Indexes() {
    for (auto &one : indexes_) {
      delete one.second;
    }
  }

  // search & do intersection *** immediately ***
  int Search(const std::vector<FilterInfo> &filters,
             RangeQueryResultV1 &result) const;

  // search & do intersection *** lazily ***
  int Search(const std::vector<FilterInfo> &filters,
             RangeQueryResult &out) const;

  int Search(const std::string &field, const std::string &lowerValue,
             const std::string &upperValue, RangeQueryResultV1 &result) const {
    Index *index = GetIndex(field);
    if (nullptr == index) {
      return -1;
    }
    return index->Search(lowerValue, upperValue, result);
  }

  int Search(const std::string &field, const std::string &value,
             RangeQueryResultV1 &result) const {
    return Search(field, value, value, result);
  }

  // Indexing all *numeric* fields
  template <typename T>
  int Indexing(const std::string &field, int n_docs,
               std::function<T(const int)> cb) {
    if (!GetIndex(field)) {
      auto idx = new (std::nothrow) NumericIndex<T>(field);
      if (idx) {
        idx->Set(cb); // set callback before build
        if (idx->Build(n_docs) < 0) {
          delete idx;
        } else {
          indexes_.insert({field, idx});
          return 0;
        }
      }
    }
    return -1;
  }

  // Two-phases indexing: 1, Add field one by one 2, Indexing all fields
  template <typename T>
  int Add(const std::string &field, std::function<T(const int)> cb) {
    if (!GetIndex(field)) {
      auto idx = new (std::nothrow) NumericIndex<T>(field);
      if (idx) {
        idx->Set(cb); // set callback before build
        indexes_.insert({field, idx});
        return 0;
      }
    }
    return -1;
  }

  int Indexing(const int n_docs) {
    assert(n_docs > 0);
    for (auto &one : indexes_) {
      // std::cout << "building " << one.first << "\n";
      if (one.second->Build(n_docs) < 0) {
        return -1;
      }
    }
    return 0;
  }

  void Add(int docID, const std::string &field, const std::string &value) {
    Index *index = GetIndex(field);
    if (index) {
      index->Add(value, docID);
    }
  }

  size_t MemoryUsage() const {
    size_t bytes = 0;
    for (auto &one : indexes_) {
      bytes += one.second->MemoryUsage();
    }
    return bytes;
  }

  void Output() const {
    for (auto &one : indexes_) {
      one.second->Output(one.first);
    }
  }

  int Dump(const std::string &file) {
    return 0; // not implemented
  }
  int Load(const std::string &file) {
    return 0; // not implemented
  }

private:
  Index *GetIndex(const std::string &field) const {
    auto iter = indexes_.find(field);
    return iter != indexes_.end() ? iter->second : nullptr;
  }

  int Intersect(const RangeQueryResultV1 *results, int j, int k,
                RangeQueryResultV1 &out) const;

private:
  static const int kLazyThreshold_ = 10000;

  std::map<std::string, Index *> indexes_;
};

// specialization for string
template <>
int Indexes::Add<std::string>(const std::string &field,
                              std::function<std::string(const int)> cb);

} // namespace NI
} // namespace tig_gamma

#endif // SRC_SEARCHER_INDEX_NUMERIC_NUMERIC_INDEX_H_
