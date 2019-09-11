/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "numeric_index.h"
#include "tag_index.h"

namespace tig_gamma {
namespace NI {

// specialization for string
template <>
int Indexes::Add<std::string>(const std::string &field,
                              std::function<std::string(const int)> cb) {
  if (!GetIndex(field)) {
    auto idx = new (std::nothrow) TagIndex(field);
    if (idx) {
      idx->Set(cb);
      indexes_.insert({field, idx});
      return 0;
    }
  }
  return -1;
}

int Indexes::Search(const std::vector<FilterInfo> &filters,
                    RangeQueryResultV1 &out) const {
  out.Clear();
  int fsize = filters.size();

  if (1 == fsize) {
    auto &_ = filters[0];
    if (_.is_union) {
      out.SetFlags(out.Flags() | 0x4);
    }
    return Search(_.field, _.lower_value, _.upper_value, out);
  }

  // utils::Timer t;
  // t.Start("Search");

  RangeQueryResultV1 results[fsize];
  int j = -1;
  // record the shortest docid list
  int k = -1, k_size = std::numeric_limits<int>::max();

  for (int i = 0; i < fsize; i++) {
    auto &_ = filters[i];

    int flags = out.Flags();
    if (_.is_union) {
      flags |= 0x4;
    }

    results[j + 1].SetFlags(flags);
    int retval = Search(_.field, _.lower_value, _.upper_value, results[j + 1]);
    if (retval < 0) {
      ;
    } else if (retval == 0) {
      return 0; // no intersection
    } else {
      j += 1;

      if (k_size > retval) {
        k_size = retval;
        k = j;
      }
    }
  }

  if (j < 0) {
    return -1; // universal set
  }

  // t.Stop();
  // t.Output();

  return Intersect(results, j, k, out);
}

int Indexes::Search(const std::vector<FilterInfo> &filters,
                    RangeQueryResult &out) const {
  out.Clear();
  int fsize = filters.size();

  if (1 == fsize) {
    auto &_ = filters[0];
    if (_.is_union) {
      out.SetFlags(out.Flags() | 0x4);
    }
    RangeQueryResultV1 tmp(out.Flags());
    int retval = Search(_.field, _.lower_value, _.upper_value, tmp);
    if (retval > 0) {
      out.Add(tmp);
    }
    return retval;
  }

  // utils::Timer t;
  // t.Start("Search");

  RangeQueryResultV1 results[fsize];
  int j = -1;
  // record the shortest docid list
  int k = -1, k_size = std::numeric_limits<int>::max();

  for (int i = 0; i < fsize; i++) {
    auto &_ = filters[i];

    int flags = out.Flags();
    if (_.is_union) {
      flags |= 0x4;
    }

    results[j + 1].SetFlags(flags);
    int retval = Search(_.field, _.lower_value, _.upper_value, results[j + 1]);
    if (retval < 0) {
      ;
    } else if (retval == 0) {
      return 0; // no intersection
    } else {
      j += 1;

      if (k_size > retval) {
        k_size = retval;
        k = j;
      }
    }
  }

  if (j < 0) {
    return -1; // universal set
  }

  // t.Stop();
  // t.Output();

  // When the shortest doc chain is long,
  // instead of calculating the intersection immediately, a lazy
  // mechanism is made.
  if (k_size > kLazyThreshold_) {
    for (int i = 0; i <= j; i++) {
      out.Add(results[i]);
    }
    return 1; // it's hard to count the return docs
  }

  RangeQueryResultV1 tmp(out.Flags());
  int count = Intersect(results, j, k, tmp);
  if (count > 0) {
    out.Add(tmp);
  }

  return count;
}

int Indexes::Intersect(const RangeQueryResultV1 *results, int j, int k,
                       RangeQueryResultV1 &out) const {
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

#if 0  // too slow
  out.SetRange(min_doc, max_doc);
  out.Resize(true);

  // calculate the intersection
  for (int i = 0; i <= j; i++) {
    auto &r = results[i];

    int x = min_doc - r.Min();
    int y = r.Max() - max_doc;

    std::transform(r.Ref().begin() + x, r.Ref().end() - y, // operand 1
                   out.Ref().begin(),                      // operand 2
                   out.Ref().begin(), std::bit_and<bool>());
  }

  return out.Size();
#endif // too slow

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

} // namespace NI
} // namespace tig_gamma
