/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifndef SRC_SEARCHER_INDEX_NUMERIC_TAG_INDEX_H_
#define SRC_SEARCHER_INDEX_NUMERIC_TAG_INDEX_H_

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <ctime>

#include <algorithm>
#include <functional>
#include <map>
#include <string>
#include <vector>

#include "numeric_struct.h"
#include "skiplist_index.h"
#include "utils.h"

#include <cuckoohash_map.hh>

namespace tig_gamma {
namespace NI {

class TagIndex : public Index {
public:
  explicit TagIndex(const std::string &field) : tag_id_(0) {
    field_.name = field;
    field_.type = IndexFieldType::STRING; // may be used in future

    rt_idx_.SetAllowDup(true);
    tag2id_.reserve(2048); // hardcode

    curr_doc_id_ = -1; // used while Build()
  }

  int Search(const std::string &tags, const std::string & /*nouse*/,
             RangeQueryResultV1 &result) const override;

  void Add(const std::string &tags, int docID) override {
    if (not tags.empty()) {
      auto items = utils::Split(tags, kDelim_);
      for (auto &item : items) {
        Add(GetOrIncrTagID(item), docID);
      }
    }
    curr_doc_id_ = docID;
  }

  void Add(int value, int docID) {
    // only rt
    rt_idx_.Insert(value, docID);
  }

  int Build(const int num_docs) override;
  void Output(const std::string &tag) override;

  int Dump(const IndexIO &out) override;
  int Load(const IndexIO &in) override;

  size_t MemoryUsage() const override {
    size_t bytes = rt_idx_.MemoryUsage();
    return bytes;
  }

  void Set(std::function<std::string(const int)> callback) {
    get_value_ = std::move(callback);
  }

private:
  int Intersect(const std::string &, RangeQueryResultV1 &) const;

  int Union(const std::string &, RangeQueryResultV1 &) const;

  int GetTagID(const std::string &tag) const {
    int tag_id = -1;
    return (tag2id_.find(tag, tag_id)) ? tag_id : -1;
  }

  // Get tag id if tag exists, otherwise generate a new one
  int GetOrIncrTagID(const std::string &tag) {
    int tag_id = GetTagID(tag);
    if (tag_id < 0) {
      tag_id = tag_id_++;
      tag2id_.insert(tag, tag_id);
    }
    return tag_id;
  }

private:
  static const char kDelim_ = '';

  int tag_id_; // next tag id
  cuckoohash_map<std::string, int> tag2id_;

  std::function<std::string(const int)> get_value_;
  int curr_doc_id_;

  SkipList<int, int> rt_idx_;
};

} // namespace NI
} // namespace tig_gamma

#endif // SRC_SEARCHER_INDEX_NUMERIC_TAG_INDEX_H_
