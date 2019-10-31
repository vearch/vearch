/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifndef FIELD_RANGE_INDEX_H_
#define FIELD_RANGE_INDEX_H_

#include <map>
#include <string>
#include <vector>
#include "gamma_api.h"
#include "range_query_result.h"

namespace tig_gamma {

typedef struct {
  std::string field;
  std::string lower_value;
  std::string upper_value;
  int is_union;
} FilterInfo;

class FieldRangeIndex;
class MultiFieldsRangeIndex {
 public:
  MultiFieldsRangeIndex();

  int Add(const std::string &field, unsigned char *key, uint key_len,
          int value);

  int AddField(const std::string &field, enum DataType field_type);

  int Search(const std::vector<FilterInfo> &filters,
             MultiRangeQueryResults &out);

 private:
  int Intersect(const RangeQueryResult *results, int j, int k,
                RangeQueryResult &out) const;
  std::map<std::string, FieldRangeIndex *> fields_;
  static const int kLazyThreshold_ = 10000;
};

}  // namespace tig_gamma

#endif