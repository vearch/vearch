/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <cstring>
#include <sstream>
#include <string>
#include "c_api/api_data/table.h"

namespace vearch {

/*
  ScalarIndexType:
    SCALAR: Base type (deprecated, treated as INVERTED for compatibility)
    INVERTED:  RocksDB (field, value, docid) -> null   (one-to-one mapping)
    INVERTED_LIST: (field, value) -> [docids] (one-to-many mapping) (future)
    BITMAP: Roaring bitmap-based index
    COMPOSITE: Composite index
*/
const std::string NULL_INDEX_TYPE_STRING = "NULL";
const std::string SCALAR_INDEX_TYPE_STRING = "SCALAR";
const std::string INVERTED_INDEX_TYPE_STRING = "INVERTED";
const std::string INVERTED_LIST_INDEX_TYPE_STRING = "INVERTED_LIST";
const std::string BITMAP_INDEX_TYPE_STRING = "BITMAP";
const std::string COMPOSITE_INDEX_TYPE_STRING = "COMPOSITE";
constexpr const char* kStringArrayValueDelimiter = "\001";

enum class ScalarIndexType : uint8_t {
  Null = 0,
  Index = 1,
  Scalar = 2,
  Inverted = 3,
  InvertedList = 4,
  Bitmap = 5,
  Composite = 6
};

enum class FilterOperator : uint8_t { And = 0, Or, Not };

// Filter mode when matched against composite index prefix
enum class CompositeFilterMode { Equal, NotEqual, In, NotIn, Range };

// Composite filter execution strategies.
// SCAN: full-iteration fallback when filters cannot form a valid composite
// key prefix. Iterates every entry of the composite index and applies the
// filters per entry.
enum class CompositeStrategy { NONE, EQUAL, RANGE, IN, NOT_IN, NOT_EQUAL, SCAN };

std::string ToString(FilterOperator filter_operator);

typedef struct FilterInfo {
  int field;
  std::string lower_value;
  std::string upper_value;
  bool include_lower;
  bool include_upper;
  FilterOperator filter_operator;

  std::string ToString(enum DataType data_type) const;
} FilterInfo;

}  // namespace vearch
