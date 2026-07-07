/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "scalar_index_types.h"

#include <cstring>
#include <sstream>

#include "scalar_index_utils.h"

namespace vearch {

std::string ToString(FilterOperator filter_operator) {
  switch (filter_operator) {
    case FilterOperator::And:
      return "And";
    case FilterOperator::Or:
      return "Or";
    case FilterOperator::Not:
      return "Not";
    default:
      return "Unknown";
  }
}

std::string FilterInfo::ToString(enum DataType data_type) const {
  std::stringstream ss;
  ss << "field=" << field << ", include_lower=" << include_lower << ", include_upper=" << include_upper
     << ", filter_operator=" << vearch::ToString(filter_operator)
     << ", data_type=" << DataTypeToString(data_type);
  if (data_type == DataType::STRING || data_type == DataType::STRINGARRAY) {
    ss << ", lower_value=" << lower_value << ", upper_value=" << upper_value;
  } else if (data_type == DataType::INT) {
    int lower_value_int;
    memcpy(&lower_value_int, lower_value.c_str(), sizeof(int));
    int upper_value_int;
    memcpy(&upper_value_int, upper_value.c_str(), sizeof(int));
    ss << ", lower_value=" << lower_value_int << ", upper_value=" << upper_value_int;
  } else if (data_type == DataType::LONG || data_type == DataType::DATE) {
    long lower_value_long;
    memcpy(&lower_value_long, lower_value.c_str(), sizeof(long));
    long upper_value_long;
    memcpy(&upper_value_long, upper_value.c_str(), sizeof(long));
    ss << ", lower_value=" << lower_value_long << ", upper_value=" << upper_value_long;
  } else if (data_type == DataType::FLOAT) {
    float lower_value_float;
    memcpy(&lower_value_float, lower_value.c_str(), sizeof(float));
    float upper_value_float;
    memcpy(&upper_value_float, upper_value.c_str(), sizeof(float));
    ss << ", lower_value=" << lower_value_float << ", upper_value=" << upper_value_float;
  } else if (data_type == DataType::DOUBLE) {
    double lower_value_double;
    memcpy(&lower_value_double, lower_value.c_str(), sizeof(double));
    double upper_value_double;
    memcpy(&upper_value_double, upper_value.c_str(), sizeof(double));
    ss << ", lower_value=" << lower_value_double << ", upper_value=" << upper_value_double;
  }
  return ss.str();
}

}  // namespace vearch
