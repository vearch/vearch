/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

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

enum class FilterOperator : uint8_t { And = 0, Or, Not };

// Filter mode when matched against composite index prefix
enum class CompositeFilterMode { Equal, NotEqual, In, NotIn, Range };

// Composite filter execution strategies
enum class CompositeStrategy { NONE, EQUAL, RANGE, IN, NOT_IN, NOT_EQUAL };

enum class ScalarIndexType : uint8_t {
  Null = 0,
  Index = 1,
  Scalar = 2,
  Inverted = 3,
  InvertedList = 4,
  Bitmap = 5,
  Composite = 6
};

std::string ScalarIndexTypeToString(enum ScalarIndexType scalar_index_type);

ScalarIndexType ScalarIndexTypeFromString(const std::string &index_type_string);

bool IsScalarIndexType(const std::string &index_type_string);

std::string DataTypeToString(enum DataType data_type);

std::string ToRowKey(int32_t key);

/**
 * Convert floating point byte sequence to sortable string (float/double)
 */
std::string FloatingToSortableStr(const std::string &bytes);

/**
 * Convert signed integer byte sequence to sortable string (int32)
 */
std::string Int32ToSortableStr(const std::string &bytes);

/**
 * Convert numeric bytes to sortable string by data type
 */
std::string NumericToSortableStr(enum DataType type,
                                        const std::string &bytes);

std::string BinaryToString(std::string value, enum DataType data_type);

/**
 * Adjust the boundary value for exclusive range queries.
 * Operates on the raw string value (e.g., "10", "3.14") before NumericToSortableStr.
 * For exclusive lower bound (> x): shifts the value up slightly (integer +1, float +epsilon)
 * For exclusive upper bound (< x): shifts the value down slightly (integer -1, float -epsilon)
 * Inplace: modifies the string directly.
 */
void AdjustDataTypeBoundary(std::string &value, enum DataType type, int delta);

namespace detail {

constexpr float kFloatEpsilon = 1.0e-6f;
constexpr double kDoubleEpsilon = 1e-15;

template <typename Type>
void AdjustBoundary(std::string &boundary, int delta);

}  // namespace detail

std::string MinSortableValue(enum DataType type);
std::string MaxSortableValue(enum DataType type);

/**
 * Get the sortable value length for a data type.
 * - INT, FLOAT: 4 bytes
 * - LONG, DATE, DOUBLE: 8 bytes
 * - STRING: variable (stored with 3-byte length prefix)
 */
size_t GetSortableLen(enum DataType type);

/**
 * Decode a sortable binary value back to raw string.
 * - INT: 4-byte big-endian -> decimal string
 * - LONG, DATE: 8-byte big-endian -> decimal string
 * - FLOAT: 4-byte IEEE-754 sortable -> float string
 * - DOUBLE: 8-byte IEEE-754 sortable -> double string
 * - STRING: [len: 3B][content] -> raw string
 */
std::string DecodeSortableValue(enum DataType type, const std::string& sortable_value);

/**
 * Advance a binary prefix to the next key boundary.
 * Increments the last byte that is not 0xFF; used as exclusive upper bound.
 * Returns empty string if all bytes are 0xFF (no upper bound).
 */
std::string AdvancePrefix(const std::string& prefix);

}  // namespace vearch