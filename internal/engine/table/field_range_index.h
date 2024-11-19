/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <map>
#include <string>
#include <vector>

#include "range_query_result.h"
#include "table.h"

namespace vearch {

enum class FilterOperator : uint8_t { And = 0, Or, Not };

typedef struct {
  int field;
  std::string lower_value;
  std::string upper_value;
  bool include_lower;
  bool include_upper;
  FilterOperator is_union;
} FilterInfo;

class FieldRangeIndex;
class MultiFieldsRangeIndex {
 public:
  MultiFieldsRangeIndex(std::string &path, Table *table);
  ~MultiFieldsRangeIndex();

  int Add(int64_t docid, int field);

  int Delete(int64_t docid, int field);

  int AddField(int field, enum DataType field_type, std::string &name);

  int64_t Search(const std::vector<FilterInfo> &origin_filters,
                 MultiRangeQueryResults *out);

  // for debug
  long MemorySize(long &dense, long &sparse);

 private:
  int64_t Intersect(std::vector<RangeQueryResult> &results,
                    int64_t shortest_idx, RangeQueryResult *out);

  int AddDoc(int64_t docid, int field);

  int DeleteDoc(int64_t docid, int field, std::string &key);
  std::string path_;
  Table *table_;
  std::vector<FieldRangeIndex *> fields_;
  pthread_rwlock_t *field_rw_locks_;
};

}  // namespace vearch
