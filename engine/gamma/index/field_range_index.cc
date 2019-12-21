/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "field_range_index.h"
#include <string.h>
#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <functional>
#include <iostream>
#include <iterator>
#include <limits>
#include <numeric>
#include <sstream>
#include <typeinfo>
#include "log.h"
#include "threadskv10h.h"
#include "utils.h"

using std::string;

namespace tig_gamma {

class Node {
 public:
  Node(int size) {
    num_ = 0;
    next_ = nullptr;
    size_ = size;
    value_ = (int *)malloc(size_ * sizeof(int));
  }

  ~Node() {
    if (next_ != nullptr) {
      delete next_;
      next_ = nullptr;
    }
    if (value_ != nullptr) {
      free(value_);
      value_ = nullptr;
    }
  }

  Node *Add(int val) {
    if (num_ < size_) {
      value_[num_] = val;
      ++num_;
      return this;
    } else {
      next_ = new Node(size_ * 2);
      next_->Add(val);
      return next_;
    }
  }

  int Num() { return num_; }

  int *Value() { return value_; }

  Node *Next() { return next_; }

 private:
  int size_;
  int num_;
  int *value_;
  Node *next_;
};

class NodeList {
 public:
  NodeList() {
    size_ = 0;
    head_ = nullptr;
    tail_ = nullptr;
    min_ = std::numeric_limits<int>::max();
    max_ = -1;
  }

  ~NodeList() {
    if (head_ != nullptr) {
      delete head_;
      head_ = nullptr;
    }
  }

  int Add(int val) {
    min_ = min_ < val ? min_ : val;
    max_ = max_ > val ? max_ : val;
    if (head_ == nullptr) {
      head_ = new Node(512);
      tail_ = head_;
    }
    tail_ = tail_->Add(val);
    ++size_;
    return 0;
  }

  Node *Head() { return head_; }

  int Min() { return min_; }
  int Max() { return max_; }

  int Size() { return size_; }

 private:
  int min_;
  int max_;

  int size_;
  Node *head_;
  Node *tail_;
};

class FieldRangeIndex {
 public:
  FieldRangeIndex(int idx, enum DataType field_type);
  ~FieldRangeIndex();

  int Add(unsigned char *key, uint key_len, int value);

  int Search(const string &low, const string &high, RangeQueryResult &result);

  int Search(const string &tags, RangeQueryResult &result);

  static const uint mainleafxtra_ = 0;
  static const uint maxleaves_ = 1000000;
  static const uint poolsize_ = 500;
  static const uint leafxtra_ = 0;
  static const uint mainpool_ = 500;
  static const uint mainbits_ = 16;
  static const uint bits_ = 16;
  static const char *kDelim_;

 private:
  BtMgr *main_mgr_;
  BtMgr *cache_mgr_;
  bool is_numeric_;
};

const char *FieldRangeIndex::kDelim_ = "\001";

FieldRangeIndex::FieldRangeIndex(int idx, enum DataType field_type) {
  string cache_file = string("cache_") + std::to_string(idx) + ".dis";
  string main_file = string("main_") + std::to_string(idx) + ".dis";

  remove(cache_file.c_str());
  remove(main_file.c_str());

  cache_mgr_ = bt_mgr(const_cast<char *>(cache_file.c_str()), bits_, leafxtra_,
                      poolsize_);
  cache_mgr_->maxleaves = maxleaves_;
  main_mgr_ = bt_mgr(const_cast<char *>(main_file.c_str()), mainbits_,
                     mainleafxtra_, mainpool_);
  main_mgr_->maxleaves = maxleaves_;

  if (field_type == DataType::STRING) {
    is_numeric_ = false;
  } else {
    is_numeric_ = true;
  }
}

FieldRangeIndex::~FieldRangeIndex() {
  BtDb *bt = bt_open(cache_mgr_, main_mgr_);

  if (bt_startkey(bt, nullptr, 0) == 0) {
    while (bt_nextkey(bt)) {
      if (bt->phase == 1) {
        NodeList **list = (NodeList **)bt->mainval->value;
        delete *list;
      }
    }
  }

  bt_unlockpage(BtLockRead, bt->cacheset->latch, __LINE__);
  bt_unpinlatch(bt->cacheset->latch);

  bt_unlockpage(BtLockRead, bt->mainset->latch, __LINE__);
  bt_unpinlatch(bt->mainset->latch);
  bt_close(bt);

  if (cache_mgr_) {
    bt_mgrclose(cache_mgr_);
    cache_mgr_ = nullptr;
  }
  if (main_mgr_) {
    bt_mgrclose(main_mgr_);
    main_mgr_ = nullptr;
  }
}

static int ReverseEndian(const unsigned char *in, unsigned char *out,
                         uint len) {
  for (uint i = 0; i < len; ++i) {
    out[i] = in[len - i - 1];
  }

  unsigned char N = 0x80;
  out[0] += N;
  return 0;
}

int FieldRangeIndex::Add(unsigned char *key, uint key_len, int value) {
  BtDb *bt = bt_open(cache_mgr_, main_mgr_);
  unsigned char key2[key_len];

  std::function<void(unsigned char *, uint)> InsertToBt =
      [&](unsigned char *key_to_add, uint key_len) {
        NodeList **p_list = new NodeList *;
        int ret = bt_findkey(bt, key_to_add, key_len, (unsigned char *)p_list,
                             sizeof(NodeList *));

        if (ret < 0) {
          *p_list = new NodeList;
          BTERR bterr = bt_insertkey(bt->main, key_to_add, key_len, 0,
                                     static_cast<void *>(p_list),
                                     sizeof(NodeList *), Unique);
          if (bterr) {
            LOG(ERROR) << "Error " << bt->mgr->err;
          }
        }
        (*p_list)->Add(value);
        delete p_list;
      };

  if (is_numeric_) {
    ReverseEndian(key, key2, key_len);
    InsertToBt(key2, key_len);
  } else {
    char key_s[key_len + 1];
    memcpy(key_s, key, key_len);
    key_s[key_len] = 0;

    char *p, *k;
    k = strtok_r(key_s, kDelim_, &p);
    while (k != nullptr) {
      InsertToBt(reinterpret_cast<unsigned char *>(k), strlen(k));
      k = strtok_r(NULL, kDelim_, &p);
    }
  }

  bt_close(bt);

  return 0;
}

int FieldRangeIndex::Search(const string &lower, const string &upper,
                            RangeQueryResult &result) {
  if (!is_numeric_) {
    return Search(lower, result);
  }

  BtDb *bt = bt_open(cache_mgr_, main_mgr_);
  unsigned char key_l[lower.length()];
  unsigned char key_u[upper.length()];
  ReverseEndian(reinterpret_cast<const unsigned char *>(lower.data()), key_l,
                lower.length());
  ReverseEndian(reinterpret_cast<const unsigned char *>(upper.data()), key_u,
                upper.length());

  std::vector<NodeList *> lists;
  lists.reserve(1000000);

  int min_doc = std::numeric_limits<int>::max();
  int max_doc = 0;

  int idx = 0;
  NodeList **p_list = new NodeList *;
  if (bt_startkey(bt, key_l, lower.length()) == 0) {
    while (bt_nextkey(bt)) {
      if (bt->phase == 1) {
        if (keycmp(bt->mainkey, key_u, upper.length()) > 0) {
          break;
        }
        memcpy(p_list, bt->mainval->value, sizeof(NodeList *));
        lists.push_back(*p_list);

        min_doc = std::min(min_doc, (*p_list)->Min());
        max_doc = std::max(max_doc, (*p_list)->Max());
      }
    }
  }
  delete p_list;

  bt_unlockpage(BtLockRead, bt->cacheset->latch, __LINE__);
  bt_unpinlatch(bt->cacheset->latch);

  bt_unlockpage(BtLockRead, bt->mainset->latch, __LINE__);
  bt_unpinlatch(bt->mainset->latch);
  bt_close(bt);

  if (max_doc - min_doc + 1 <= 0) {
    return 0;
  }

  result.SetRange(min_doc, max_doc);
  result.Resize();

  idx = lists.size();
#pragma omp parallel for
  for (int i = 0; i < idx; ++i) {
    NodeList *list = lists[i];
    Node *node = list->Head();
    while (node != nullptr) {
      int node_num = node->Num();
      int *values = node->Value();
      for (int j = 0; j < node_num; ++j) {
        if (values[j] > max_doc) {
          continue;
        }
        result.Set(values[j] - min_doc);
      }
      node = node->Next();
    }
  }

  return max_doc - min_doc + 1;
}

int FieldRangeIndex::Search(const string &tags, RangeQueryResult &result) {
  std::vector<string> items = utils::split(tags, kDelim_);

  RangeQueryResult results_union[items.size()];

  NodeList **p_list = new NodeList *;
  for (size_t i = 0; i < items.size(); ++i) {
    string item = items[i];
    const unsigned char *key_tag =
        reinterpret_cast<const unsigned char *>(item.data());

    int min_doc = std::numeric_limits<int>::max();
    int max_doc = 0;

    BtDb *bt = bt_open(cache_mgr_, main_mgr_);
    int ret =
        bt_findkey(bt, const_cast<unsigned char *>(key_tag), item.length(),
                   (unsigned char *)p_list, sizeof(NodeList *));
    bt_close(bt);

    if (ret < 0) {
      return 0;
    }
    min_doc = std::min(min_doc, (*p_list)->Min());
    max_doc = std::max(max_doc, (*p_list)->Max());

    if (max_doc - min_doc + 1 <= 0) {
      return 0;
    }

    results_union[i].SetRange(min_doc, max_doc);
    results_union[i].Resize();

    Node *node = (*p_list)->Head();
    while (node != nullptr) {
      int node_num = node->Num();
      int *values = node->Value();
      for (int j = 0; j < node_num; ++j) {
        if (values[j] > max_doc) {
          continue;
        }
        results_union[i].Set(values[j] - min_doc);
      }
      node = node->Next();
    }
  }
  delete p_list;

  int min_doc = std::numeric_limits<int>::max();
  int max_doc = 0;
  for (size_t i = 0; i < items.size(); ++i) {
    min_doc = std::min(min_doc, results_union[i].Min());
    max_doc = std::max(max_doc, results_union[i].Max());
  }

  int retval = max_doc - min_doc + 1;
  if (retval <= 0) {
    return 0;
  }
  result.SetRange(min_doc, max_doc);
  result.Resize();
  for (size_t i = 0; i < items.size(); ++i) {
    int doc = results_union[i].Next();
    while (doc >= 0) {
      result.Set(doc - min_doc);
      doc = results_union[i].Next();
    }
  }

  return retval;
}

MultiFieldsRangeIndex::MultiFieldsRangeIndex(Profile *profile) {
  profile_ = profile;
  fields_.resize(profile->FieldsNum());
  std::fill(fields_.begin(), fields_.end(), nullptr);
}

MultiFieldsRangeIndex::~MultiFieldsRangeIndex() {
  for (size_t i = 0; i < fields_.size(); i++) {
    if (fields_[i]) {
      delete fields_[i];
      fields_[i] = nullptr;
    }
  }
}

int MultiFieldsRangeIndex::Add(int docid, int field) {
  FieldRangeIndex *index = fields_[field];
  if (index == nullptr) {
    return 0;
  }

  unsigned char *key;
  int key_len = 0;
  profile_->GetFieldRawValue(docid, field, &key, key_len);
  index->Add(key, key_len, docid);

  return 0;
}

int MultiFieldsRangeIndex::Search(const std::vector<FilterInfo> &filters,
                                  MultiRangeQueryResults &out) {
  out.Clear();
  int fsize = filters.size();

  if (1 == fsize) {
    auto &_ = filters[0];
    if (_.is_union) {
      out.SetFlags(out.Flags() | 0x4);
    }
    RangeQueryResult tmp(out.Flags());
    FieldRangeIndex *index = fields_[_.field];
    if (index == nullptr || _.field < 0) {
      return -1;
    }

    int retval = index->Search(_.lower_value, _.upper_value, tmp);
    if (retval > 0) {
      out.Add(tmp);
    }
    return retval;
  }

  RangeQueryResult results[fsize];
  int valuable_result = -1;
  // record the shortest docid list
  int shortest_idx = -1, shortest = std::numeric_limits<int>::max();

  for (int i = 0; i < fsize; ++i) {
    auto &filter = filters[i];

    FieldRangeIndex *index = fields_[filter.field];
    if (index == nullptr || filter.field < 0) {
      continue;
    }

    int flags = out.Flags();
    if (filter.is_union) {
      flags |= 0x4;
    }

    results[valuable_result + 1].SetFlags(flags);

    int retval = index->Search(filter.lower_value, filter.upper_value,
                               results[valuable_result + 1]);
    if (retval < 0) {
      ;
    } else if (retval == 0) {
      return 0;  // no intersection
    } else {
      valuable_result += 1;

      if (shortest > retval) {
        shortest = retval;
        shortest_idx = valuable_result;
      }
    }
  }

  if (valuable_result < 0) {
    return -1;  // universal set
  }

  // t.Stop();
  // t.Output();

  // When the shortest doc chain is long,
  // instead of calculating the intersection immediately, a lazy
  // mechanism is made.
  if (shortest > kLazyThreshold_) {
    for (int i = 0; i <= valuable_result; ++i) {
      out.Add(results[i]);
    }
    return 1;  // it's hard to count the return docs
  }

  RangeQueryResult tmp(out.Flags());
  int count = Intersect(results, valuable_result, shortest_idx, tmp);
  if (count > 0) {
    out.Add(tmp);
  }

  return count;
}

int MultiFieldsRangeIndex::Intersect(const RangeQueryResult *results, int j,
                                     int k, RangeQueryResult &out) const {
  assert(results != nullptr && j >= 0);

  // t.Start("Intersect");
  // I want to build a smaller bitmap ...
  int min_doc = results[0].Min();
  int max_doc = results[0].Max();

  for (int i = 1; i <= j; i++) {
    auto &r = results[i];

    // the maximum of the minimum(s)
    if (r.Min() > min_doc) {
      min_doc = r.Min();
    }
    // the minimum of the maximum(s)
    if (r.Max() < max_doc) {
      max_doc = r.Max();
    }
  }

  if (max_doc - min_doc + 1 <= 0) {
    return 0;
  }
  out.SetRange(min_doc, max_doc);
  out.Resize();

  // calculate the intersection with the shortest doc chain.
  int count = 0;
  int docID = results[k].Next();
  while (docID >= 0) {
    int i = 0;
    while (i <= j && results[i].Has(docID)) {
      i++;
    }
    if (i > j) {
      int pos = docID - min_doc;
      out.Set(pos);
      count++;
    }
    docID = results[k].Next();
  }

  // t.Stop();
  // t.Output();

  return count;
}

int MultiFieldsRangeIndex::AddField(int field, enum DataType field_type) {
  FieldRangeIndex *index = new FieldRangeIndex(field, field_type);
  fields_[field] = index;
  return 0;
}
}  // namespace tig_gamma
