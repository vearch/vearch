/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include <cmath>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "scalar_index_utils.h"
#include "util/log.h"
#include "util/utils.h"

namespace vearch {

std::string ScalarIndexTypeToString(enum ScalarIndexType scalar_index_type) {
  switch (scalar_index_type) {
    case ScalarIndexType::Scalar:
        return SCALAR_INDEX_TYPE_STRING;
    case ScalarIndexType::Inverted:
        return INVERTED_INDEX_TYPE_STRING;
    case ScalarIndexType::InvertedList:
        return INVERTED_LIST_INDEX_TYPE_STRING;
    case ScalarIndexType::Bitmap:
        return BITMAP_INDEX_TYPE_STRING;
    case ScalarIndexType::Composite:
        return COMPOSITE_INDEX_TYPE_STRING;
    default:
        return NULL_INDEX_TYPE_STRING;
  }
}

ScalarIndexType ScalarIndexTypeFromString(const std::string &index_type_string) {
  if (index_type_string == SCALAR_INDEX_TYPE_STRING) {
    return ScalarIndexType::Scalar;
  } else if (index_type_string == INVERTED_INDEX_TYPE_STRING) {
    return ScalarIndexType::Inverted;
  } else if (index_type_string == INVERTED_LIST_INDEX_TYPE_STRING) {
    return ScalarIndexType::InvertedList;
  } else if (index_type_string == COMPOSITE_INDEX_TYPE_STRING) {
    return ScalarIndexType::Composite;
  } else if (index_type_string == BITMAP_INDEX_TYPE_STRING) {
    return ScalarIndexType::Bitmap;
  } else {
    LOG(ERROR) << "Invalid index type string: " << index_type_string;
    return ScalarIndexType::Null;
  }
  return ScalarIndexType::Null;
}

bool IsScalarIndexType(const std::string &index_type_string) {
  return index_type_string == SCALAR_INDEX_TYPE_STRING ||
         index_type_string == INVERTED_INDEX_TYPE_STRING ||
         index_type_string == BITMAP_INDEX_TYPE_STRING ||
         index_type_string == COMPOSITE_INDEX_TYPE_STRING;
}

std::string DataTypeToString(enum DataType data_type) {
  switch (data_type) {
    case DataType::STRING:
        return "STRING";
    case DataType::STRINGARRAY:
        return "STRINGARRAY";
    case DataType::INT:
        return "INT";
    case DataType::LONG:
        return "LONG";
    case DataType::FLOAT:
        return "FLOAT";
    case DataType::DOUBLE:
        return "DOUBLE";
    case DataType::DATE:
        return "DATE";
    default:
        return "UNKNOWN";
  }
}

std::string ToRowKey(int32_t key) {
    int32_t be32_value = htobe32(key);  // convert to big-endian
    return std::string(reinterpret_cast<const char *>(&be32_value),
                        sizeof(be32_value));
}

/**
 * Convert floating point byte sequence to sortable string (float/double)
 */
std::string FloatingToSortableStr(const std::string &bytes) {
    if (bytes.size() == sizeof(float)) {
        union {
        float f;
        uint32_t i;
        } u;
        memcpy(&u.f, bytes.data(), sizeof(float));

        // IEEE-754 floating point processing
        if (u.i & 0x80000000) {  // negative number
        u.i = ~u.i;            // invert all bits
        } else {                  // positive number
        u.i ^= 0x80000000;     // invert only the sign bit
        }

        // convert to big-endian
        uint32_t be = htobe32(u.i);
        return std::string(reinterpret_cast<char *>(&be), sizeof(be));

    } else if (bytes.size() == sizeof(double)) {
        union {
        double d;
        uint64_t i;
        } u;
        memcpy(&u.d, bytes.data(), sizeof(double));

        // IEEE-754 floating point processing
        if (u.i & 0x8000000000000000ULL) {  // negative number
        u.i = ~u.i;                        // invert all bits
        } else {                             // positive number
        u.i ^= 0x8000000000000000ULL;      // invert only the sign bit
        }

        // convert to big-endian
        uint64_t be = htobe64(u.i);
        return std::string(reinterpret_cast<char *>(&be), sizeof(be));

    } else {
        LOG(ERROR) << "Invalid floating point bytes length: " << bytes.size();
        return bytes;
    }
}

/**
 * Convert signed integer byte sequence to sortable string (int32)
 */
std::string Int32ToSortableStr(const std::string &bytes) {
    if (bytes.size() != sizeof(int32_t)) {
        LOG(ERROR) << "Invalid int32 bytes length: " << bytes.size();
        return bytes;
    }
    int32_t v;
    memcpy(&v, bytes.data(), sizeof(v));
    // Flip sign bit to get lexicographically sortable order
    uint32_t u = static_cast<uint32_t>(v) ^ 0x80000000u;
    uint32_t be = htobe32(u);
    return std::string(reinterpret_cast<char *>(&be), sizeof(be));
}

/**
 * Convert signed integer byte sequence to sortable string (int64 / date)
 */
std::string Int64ToSortableStr(const std::string &bytes) {
    if (bytes.size() != sizeof(int64_t)) {
        LOG(ERROR) << "Invalid int64 bytes length: " << bytes.size();
        return bytes;
    }
    int64_t v;
    memcpy(&v, bytes.data(), sizeof(v));
    // Flip sign bit to get lexicographically sortable order
    uint64_t u = static_cast<uint64_t>(v) ^ 0x8000000000000000ULL;
    uint64_t be = htobe64(u);
    return std::string(reinterpret_cast<char *>(&be), sizeof(be));
}

/**
 * Convert numeric bytes to sortable string by data type
 */
std::string NumericToSortableStr(enum DataType type,
                                        const std::string &bytes) {
    switch (type) {
        case DataType::INT:
            return Int32ToSortableStr(bytes);
        case DataType::LONG:
        case DataType::DATE:
            return Int64ToSortableStr(bytes);
        case DataType::FLOAT:
        case DataType::DOUBLE:
            return FloatingToSortableStr(bytes);
        default:
            // For other types, keep original
            return bytes;
    }
}

/**
 * Convert sortable string back to numeric string representation.
 */
std::string SortableStrToNumeric(enum DataType type,
                                        const std::string &sortable_str) {
    switch (type) {
        case DataType::INT: {
            if (sortable_str.size() != sizeof(int32_t)) {
                LOG(ERROR) << "Invalid sortable string length for INT: "
                           << sortable_str.size();
                return sortable_str;
            }
            uint32_t u = be32toh(*reinterpret_cast<const uint32_t *>(sortable_str.data()));
            u ^= 0x80000000u;  // undo sign flip
            int32_t v = static_cast<int32_t>(u);
            return std::to_string(v);
        }
        case DataType::LONG:
        case DataType::DATE: {
            if (sortable_str.size() != sizeof(int64_t)) {
                LOG(ERROR) << "Invalid sortable string length for LONG/DATE: "
                           << sortable_str.size();
                return sortable_str;
            }
            uint64_t u = be64toh(*reinterpret_cast<const uint64_t *>(sortable_str.data()));
            u ^= 0x8000000000000000ULL;  // undo sign flip
            int64_t v = static_cast<int64_t>(u);
            return std::to_string(v);
        }
        case DataType::FLOAT: {
            if (sortable_str.size() != sizeof(float)) {
                LOG(ERROR) << "Invalid sortable string length for FLOAT: "
                           << sortable_str.size();
                return sortable_str;
            }
            uint32_t u = be32toh(*reinterpret_cast<const uint32_t *>(sortable_str.data()));
            // undo floating-point transform:
            //   sortable = (was_neg ? ~bits : bits ^ sign_bit)
            //   if high bit of sortable is set (was_neg), it was negative
            //     original_bits = ~sortable ^ sign_bit
            //   else it was positive
            //     original_bits = sortable ^ sign_bit
            uint32_t original;
            if (u & 0x80000000) {
                original = ~u;         // undo bitwise-not
            } else {
                original = u ^ 0x80000000u;  // undo XOR with sign bit
            }
            float v;
            memcpy(&v, &original, sizeof(v));
            return std::to_string(v);
        }
        case DataType::DOUBLE: {
            if (sortable_str.size() != sizeof(double)) {
                LOG(ERROR) << "Invalid sortable string length for DOUBLE: "
                           << sortable_str.size();
                return sortable_str;
            }
            uint64_t u = be64toh(*reinterpret_cast<const uint64_t *>(sortable_str.data()));
            // undo floating-point transform (same logic as FLOAT)
            uint64_t original;
            if (u & 0x8000000000000000ULL) {
                original = ~u;
            } else {
                original = u ^ 0x8000000000000000ULL;
            }
            double v;
            memcpy(&v, &original, sizeof(v));
            return std::to_string(v);
        }
        default:
            return sortable_str;
    }
}

std::string BinaryToString(std::string value, enum DataType data_type) {
    std::stringstream ss;
    ss << "data_type=" << DataTypeToString(data_type);
    switch (data_type)
    {
    case DataType::INT:
      int value_int;
      memcpy(&value_int, value.c_str(), sizeof(int));
      ss << ", value=" << value_int;
      break;
    case DataType::FLOAT:
      float value_float;
      memcpy(&value_float, value.c_str(), sizeof(float));
      ss << ", value=" << value_float;
      break;
    case DataType::DOUBLE:
      double value_double;
      memcpy(&value_double, value.c_str(), sizeof(double));
      ss << ", value=" << value_double;
      break;
    case DataType::LONG:
    case DataType::DATE:
      long value_long;
      memcpy(&value_long, value.c_str(), sizeof(long));
      ss << ", value=" << value_long;
      break;
    default:
      ss << ", value=" << value;
      break;
    }
    return ss.str();
  }

constexpr float kFloatEpsilon = 1.0e-6f;
constexpr double kDoubleEpsilon = 1e-15;

namespace detail {

template <typename Type>
void AdjustBoundary(std::string &boundary, int offset) {
	static_assert(std::is_fundamental<Type>::value, "Type must be fundamental.");

	if (boundary.size() >= sizeof(Type)) {
		Type b;
		std::vector<char> vec(sizeof(b));
		memcpy(&b, boundary.data(), sizeof(b));

		if constexpr (std::is_same_v<Type, float>) {
			if (std::abs(b) < kFloatEpsilon) {
				b = (offset > 0) ? kFloatEpsilon : -kFloatEpsilon;
			} else {
				b += offset * kFloatEpsilon;
			}
		} else if constexpr (std::is_same_v<Type, double>) {
			if (std::abs(b) < kDoubleEpsilon) {
				b = (offset > 0) ? kDoubleEpsilon : -kDoubleEpsilon;
			} else {
				b += offset * kDoubleEpsilon;
			}
		} else {
			b += offset;
		}

		memcpy(vec.data(), &b, sizeof(b));
		boundary = std::string(vec.begin(), vec.end());
	}
}

}  // namespace detail

void AdjustDataTypeBoundary(std::string &value, enum DataType data_type, int delta) {
	switch (data_type) {
		case DataType::INT:
			detail::AdjustBoundary<int>(value, delta);
			break;
		case DataType::DATE:
		case DataType::LONG:
			detail::AdjustBoundary<long>(value, delta);
			break;
		case DataType::FLOAT:
			detail::AdjustBoundary<float>(value, delta);
			break;
		case DataType::DOUBLE:
			detail::AdjustBoundary<double>(value, delta);
			break;
		default:
			break;
	}
}

std::string MinSortableValue(enum DataType type) {
  switch (type) {
    case DataType::INT:
    case DataType::FLOAT:
      return std::string(sizeof(uint32_t), '\x00');
    case DataType::LONG:
    case DataType::DATE:
    case DataType::DOUBLE:
      return std::string(sizeof(uint64_t), '\x00');
    default:
      return "";
  }
}

std::string MaxSortableValue(enum DataType type) {
  switch (type) {
    case DataType::INT:
    case DataType::FLOAT:
      return std::string(sizeof(uint32_t), '\xFF');
    case DataType::LONG:
    case DataType::DATE:
    case DataType::DOUBLE:
      return std::string(sizeof(uint64_t), '\xFF');
    default:
      return "";
  }
}

std::string AdvancePrefix(const std::string& prefix) {
  std::string result = prefix;
  for (int i = static_cast<int>(result.size()) - 1; i >= 0; --i) {
    if (static_cast<unsigned char>(result[i]) < 0xFF) {
      result[i] = static_cast<char>(static_cast<unsigned char>(result[i]) + 1);
      result.resize(i + 1);
      return result;
    }
  }
  return "";
}

size_t GetSortableLen(enum DataType type) {
  switch (type) {
    case DataType::INT:
    case DataType::FLOAT:
      return 4;
    case DataType::LONG:
    case DataType::DATE:
    case DataType::DOUBLE:
      return 8;
    case DataType::STRING:
    case DataType::STRINGARRAY:
      return 0;  // Variable length, must be decoded from prefix
    default:
      return 0;
  }
}

std::string DecodeSortableValue(enum DataType type, const std::string& sortable_value) {
  if (sortable_value.empty()) {
    return "";
  }

  switch (type) {
    case DataType::INT: {
      if (sortable_value.size() < 4) return "";
      int32_t val = 0;
      for (size_t i = 0; i < 4; ++i) {
        val = (val << 8) | static_cast<unsigned char>(sortable_value[i]);
      }
      return std::to_string(val);
    }

    case DataType::LONG:
    case DataType::DATE: {
      if (sortable_value.size() < 8) return "";
      int64_t val = 0;
      for (size_t i = 0; i < 8; ++i) {
        val = (val << 8) | static_cast<unsigned char>(sortable_value[i]);
      }
      return std::to_string(val);
    }

    case DataType::FLOAT: {
      if (sortable_value.size() < 4) return "";
      uint32_t uval = 0;
      for (size_t i = 0; i < 4; ++i) {
        uval = (uval << 8) | static_cast<unsigned char>(sortable_value[i]);
      }
      // Reverse the sortable transformation
      if (uval & 0x80000000) {
        uval = ~uval;
      } else {
        uval ^= 0x80000000;
      }
      float fval;
      memcpy(&fval, &uval, sizeof(fval));
      return std::to_string(fval);
    }

    case DataType::DOUBLE: {
      if (sortable_value.size() < 8) return "";
      uint64_t uval = 0;
      for (size_t i = 0; i < 8; ++i) {
        uval = (uval << 8) | static_cast<unsigned char>(sortable_value[i]);
      }
      // Reverse the sortable transformation for double
      if (uval & 0x8000000000000000ULL) {
        uval = ~uval;
      } else {
        uval ^= 0x8000000000000000ULL;
      }
      double dval;
      memcpy(&dval, &uval, sizeof(dval));
      return std::to_string(dval);
    }

    case DataType::STRING:
    case DataType::STRINGARRAY: {
      // STRING format: [len: 3 bytes big-endian][content]
      if (sortable_value.size() < 3) return "";
      uint32_t len = (static_cast<unsigned char>(sortable_value[0]) << 16) |
                     (static_cast<unsigned char>(sortable_value[1]) << 8) |
                     static_cast<unsigned char>(sortable_value[2]);
      if (sortable_value.size() < 3 + len) return "";
      return sortable_value.substr(3, len);
    }

    default:
      return "";
  }
}

}  // namespace vearch
