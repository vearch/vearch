/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "tag_index.h"

#include "log.h"
#include "timer.h" // for Timer

using std::string;
using std::vector;

namespace tig_gamma {
namespace NI {

int TagIndex::Build(const int num_docs) {
  if (num_docs < 1) {
    return -1;
  }

  auto next_doc_id = curr_doc_id_ + 1;
  for (auto docid = next_doc_id; docid < num_docs; docid++) {
    string value = get_value_(docid);
    if (value.empty()) {
      continue;
    }
    Add(value, docid);
  }

  return 0;
}

void TagIndex::Output(const string &tag) {
  rt_idx_.Output(tag);

  size_t bytes = rt_idx_.MemoryUsage();
  float MB = 1.0 * bytes / 1048576;
  std::cout << tag << " -> rt size: " << rt_idx_.Size()
            << ", memory usage: " << bytes << " bytes (" << MB << " MB).\n";
}

int TagIndex::Dump(const IndexIO &out) {
  // TODO
  // for now, dynamically Build() after the external load is complete
  return 0;
}

int TagIndex::Load(const IndexIO &in) {
  // TODO
  // for now, dynamically Build() after the external load is complete
  return 0;
}

int TagIndex::Search(const string &tags, const string & /* nouse */,
                     RangeQueryResultV1 &result) const {
  result.Clear();

  // utils::Timer t;
  // t.Start("visit");

  int count = -1;

  if (result.Flags() & 0x4) {
    count = Union(tags, result);
  } else {
    count = Intersect(tags, result);
  }

  // t.Stop();
  // t.Output();

  // vector<int> docs = result.ToDocs();
  // std::copy(std::begin(docs), std::end(docs),
  //          std::ostream_iterator<int>(std::cout, ","));

  return count;
}

static void CollectDocIDs(Node<int, int> *inv_list, vector<int> &docIDs,
                          int &min_doc, int &max_doc) {
  assert(inv_list != nullptr);

  docIDs.emplace_back(inv_list->value);

  if (inv_list->value < min_doc) {
    min_doc = inv_list->value;
  }

  if (max_doc < inv_list->value) {
    max_doc = inv_list->value;
  }

  // docid list with the same value (docid is in order)
  // WARNING: note thread safe of the node access
  auto dup = inv_list->Dup();
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

static void SetBitmap(const std::vector<int> &docIDs,
                      RangeQueryResultV1 &result) {
  size_t num_docs = docIDs.size();
  size_t i = 0;

#define SET_ONE                                                                \
  do {                                                                         \
    int pos = docIDs[i] - result.Min();                                        \
    result.Set(pos);                                                           \
    i++;                                                                       \
  } while (0)

  size_t loops = num_docs / 8;
  for (size_t loop = 0; loop < loops; loop++) {
    SET_ONE; // 1
    SET_ONE; // 2
    SET_ONE; // 3
    SET_ONE; // 4
    SET_ONE; // 5
    SET_ONE; // 6
    SET_ONE; // 7
    SET_ONE; // 8
  }

  switch (num_docs % 8) {
  case 7:
    SET_ONE;
  case 6:
    SET_ONE;
  case 5:
    SET_ONE;
  case 4:
    SET_ONE;
  case 3:
    SET_ONE;
  case 2:
    SET_ONE;
  case 1:
    SET_ONE;
  }

  assert(i == num_docs);
#undef SET_ONE
}

int TagIndex::Union(const string &tags, RangeQueryResultV1 &result) const {
  if (tags.empty() || rt_idx_.Size() < 1) {
    return -1;
  }

  vector<Node<int, int> *> inv_lists;

  // BLOCK: Get inv lists
  {
    vector<string> items = utils::Split(tags, kDelim_);
    for (auto item : items) {
      int tag_id = GetTagID(item);
      if (tag_id < 0) {
        continue; // skip
      }

      auto *inv_list = rt_idx_.Search(tag_id);
      if (inv_list == nullptr) {
        continue;
      }

      inv_lists.push_back(inv_list);
    }
  }

  if (inv_lists.empty()) {
    return 0; // no result
  }

  int inv_list_size = 0; // this's just a estimated value

  for (size_t i = 0; i < inv_lists.size(); i++) {
    auto dup = inv_lists[i]->Dup();

    inv_list_size += 1;
    if (dup) {
      inv_list_size += dup->Size();
    }
  }

  // TODO optimization
  // actually, it's not necessary to collect the docIDs here.
  vector<int> docIDs; // unordered
  docIDs.reserve(inv_list_size);

  // I want to build a smaller bitmap ...
  int min_doc = std::numeric_limits<int>::max();
  int max_doc = 0;

  for (auto *inv_list : inv_lists) {
    CollectDocIDs(inv_list, docIDs, min_doc, max_doc);
  }

  if (docIDs.empty()) {
    return 0; // no result
  }

  // build docID's bitmap
  result.SetRange(min_doc, max_doc);
  result.Resize();

  SetBitmap(docIDs, result);

  return static_cast<int>(docIDs.size());
}

struct InvListInfo {
  InvListInfo()
      : inv_list(nullptr), list_size(0), head(nullptr), tail(nullptr) {}

  Node<int, int> *inv_list;
  int list_size;

  DupNode<int> *head;
  DupNode<int> *tail;
};

static void CollectDocIDs(const InvListInfo &li, int min_doc, int max_doc,
                          vector<int> &docIDs) {
  assert(li.inv_list != nullptr);

  auto docid = li.inv_list->value;
  if (docid >= min_doc && docid <= max_doc) {
    docIDs.emplace_back(docid);
  }

  // if the 1st docid is GTE max_doc, then nothing to do
  if (docid < max_doc && li.head) {
    bool calc_last_node = true;
    auto node = li.head;
    for (auto tail = li.tail; node != tail; node = node->next) {
      auto the_1st = node->ids[0];
      if (the_1st > max_doc) {
        calc_last_node = false;
        break;
      }

      auto the_last = node->ids[kDupNodeSize - 1];
      if (the_last < min_doc) {
        continue;
      }

      if (the_1st >= min_doc && the_last <= max_doc) {
        docIDs.insert(docIDs.end(), std::begin(node->ids), std::end(node->ids));
      } else {
        for (auto docid : node->ids) {
          if (docid > max_doc)
            break;
          if (docid >= min_doc) {
            docIDs.emplace_back(docid);
          }
        }
      }
    }

    if (calc_last_node) {
      for (auto docid : node->ids) {
        if (docid < 0 || docid > max_doc)
          break;
        if (docid >= min_doc) {
          docIDs.emplace_back(docid);
        }
      }
    }
  }
}

static vector<int> CalcIntersect(const InvListInfo &li, int min_doc,
                                 int max_doc, const vector<int> &k_docIDs) {
  assert(li.inv_list != nullptr);

  size_t num_docs = k_docIDs.size();
  size_t i = 0;

  vector<int> result;

  auto docid = li.inv_list->value;
  if (docid >= min_doc && docid <= max_doc) {
    while (i < num_docs) {
      if (k_docIDs[i] < docid) {
        i++;
      } else {
        if (k_docIDs[i] == docid) {
          result.emplace_back(docid);
          i++;
        }
        break;
      }
    }

    if (i >= num_docs) {
      return result;
    }
  }

  // if the 1st docid is GTE max_doc, then nothing to do
  if (docid < max_doc && li.head) {
    bool calc_last_node = true;
    auto node = li.head;
    for (auto tail = li.tail; node != tail; node = node->next) {
      auto the_1st = node->ids[0];
      if (the_1st > max_doc) {
        calc_last_node = false;
        break;
      }

      auto the_last = node->ids[kDupNodeSize - 1];
      if (the_last < min_doc) {
        continue;
      }

      int j = 0;
      while (i < num_docs && j < kDupNodeSize) {
        auto docid = node->ids[j];
        if (docid > max_doc)
          break;
        if (k_docIDs[i] < docid) {
          i++;
        } else if (docid < k_docIDs[i]) {
          j++;
        } else {
          result.emplace_back(docid);
          i++;
          j++;
        }
      }

      if (i >= num_docs) {
        calc_last_node = false;
        break;
      }
    }

    if (calc_last_node) {
      int j = 0;
      while (i < num_docs && j < kDupNodeSize) {
        auto docid = node->ids[j];
        if (docid < 0 || docid > max_doc)
          break;
        if (k_docIDs[i] < docid) {
          i++;
        } else if (docid < k_docIDs[i]) {
          j++;
        } else {
          result.emplace_back(docid);
          i++;
          j++;
        }
      }
    }
  }

  return result;
}

int TagIndex::Intersect(const string &tags, RangeQueryResultV1 &result) const {
  if (tags.empty() || rt_idx_.Size() < 1) {
    return -1;
  }

  vector<Node<int, int> *> inv_lists;

  // BLOCK: Get inv lists
  {
    vector<string> items = utils::Split(tags, kDelim_);
    for (auto item : items) {
      int tag_id = GetTagID(item);
      if (tag_id < 0) {
        return 0; // no result
      }

      auto *inv_list = rt_idx_.Search(tag_id);
      if (inv_list == nullptr) {
        return 0; // no result
      }

      inv_lists.push_back(inv_list);
    }
  }

  if (inv_lists.empty()) {
    return 0; // no result
  }

  size_t num_lists = inv_lists.size();

  vector<InvListInfo> lis(num_lists);

  // used to build a smaller bitmap
  int min_doc = std::numeric_limits<int>::max();
  int max_doc = 0;

  // shortest inv list & its size
  size_t k = 0;
  int k_size = std::numeric_limits<int>::max();

  // BLOCK: calc min_doc & max_doc, k & k_size, inv list info
  {
    vector<int> min_docs(num_lists), max_docs(num_lists);

    for (size_t i = 0; i < num_lists; i++) {
      lis[i].inv_list = inv_lists[i];

      int docid = lis[i].inv_list->value;
      lis[i].list_size = 1;

      min_docs[i] = max_docs[i] = docid;

      auto dup = lis[i].inv_list->Dup();
      if (dup) {
        lis[i].list_size += dup->Size();

        lis[i].head = dup->head;
        lis[i].tail =
            dup->Tail(); // for thread-safe, Tail() can only fetch once

        for (auto docid : lis[i].tail->ids) {
          if (docid < 0)
            break;
          max_docs[i] = docid;
        }
      }

      if (lis[i].list_size < k_size) {
        k = i;
        k_size = lis[i].list_size;
      }
    }

    min_doc = *(std::max_element(
        min_docs.begin(), min_docs.end())); // the maximum of the minimum(s)
    max_doc = *(std::min_element(
        max_docs.begin(), max_docs.end())); // the minimum of the maximum(s)
  }

  // collect all inv lists
  //  vector<vector<int>> all_docIDs(num_lists);
  //
  //  for (size_t i = 0; i < num_lists; i++) {
  //    all_docIDs[i].reserve(lis[i].list_size / 10);
  //
  //    CollectDocIDs(lis[i], min_doc, max_doc, all_docIDs[i]);
  //
  //    if (all_docIDs[i].empty()) {
  //      return 0; // no result
  //    }
  //  }

  vector<int> k_docIDs;
  k_docIDs.reserve(k_size / 10);

  CollectDocIDs(lis[k], min_doc, max_doc, k_docIDs);
  if (k_docIDs.empty()) {
    return 0; // no result
  }

  for (size_t i = 0; i < num_lists; i++) {
    if (i == k)
      continue;

    vector<int> docIDs = CalcIntersect(lis[i], min_doc, max_doc, k_docIDs);
    if (docIDs.empty()) {
      return 0; // no result
    }

    k_docIDs.swap(docIDs); // visit next inv list
  }

  // build docID's bitmap
  result.SetRange(min_doc, max_doc);
  result.Resize();

  SetBitmap(k_docIDs, result);

  return static_cast<int>(k_docIDs.size());
}

} // namespace NI
} // namespace tig_gamma
