/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "field_range_index.h"

#include <roaring/roaring.h>
#include <string.h>

#include "util/log.h"
#include "util/utils.h"

namespace vearch {

static const char *kDelim = "\001";

static std::string ToRowKey(int32_t key) {
  int32_t be32_value = htobe32(key);  // convert to big-endian
  return std::string(reinterpret_cast<const char *>(&be32_value),
                     sizeof(be32_value));
}

static std::string GenLogicalKey(int field, const std::string &index_value,
                                 int64_t docid) {
  return ToRowKey(field) + "_" + index_value + "_" + utils::ToRowKey64(docid);
}

class FieldRangeIndex {
 public:
  FieldRangeIndex(int field_idx, enum DataType field_type) {
    if (field_type == DataType::STRING || field_type == DataType::STRINGARRAY) {
      is_numeric_ = false;
    } else {
      is_numeric_ = true;
    }
    data_type_ = field_type;
  }

  bool IsNumeric() { return is_numeric_; }

  enum DataType DataType() { return data_type_; }

 private:
  bool is_numeric_;
  enum DataType data_type_;
};

int MultiFieldsRangeIndex::Init() {
  int cf_id = storage_mgr_->CreateColumnFamily("scalar");
  cf_id_ = cf_id;
  return 0;
}

/**
 * Convert floating point byte sequence to sortable string
 * Automatically determines whether it is float or double based on input byte
 * length
 *
 * @param bytes Byte sequence of the floating point number
 * @return Sortable string
 */
static std::string FloatingToSortableStr(const std::string &bytes) {
  if (bytes.size() == sizeof(float)) {
    union {
      float f;
      uint32_t i;
    } u;
    memcpy(&u.f, bytes.data(), sizeof(float));

    // IEEE-754 floating point processing
    if (u.i & 0x80000000) {  // negative number
      u.i = ~u.i;            // invert all bits
    } else {                 // positive number
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
      u.i = ~u.i;                       // invert all bits
    } else {                            // positive number
      u.i ^= 0x8000000000000000ULL;     // invert only the sign bit
    }

    // convert to big-endian
    uint64_t be = htobe64(u.i);
    return std::string(reinterpret_cast<char *>(&be), sizeof(be));

  } else {
    LOG(ERROR) << "Invalid floating point bytes length: " << bytes.size();
    return bytes;
  }
}

MultiFieldsRangeIndex::MultiFieldsRangeIndex(Table *table,
                                             StorageManager *storage_mgr)
    : table_(table), fields_(table->FieldsNum()), storage_mgr_(storage_mgr) {
  std::fill(fields_.begin(), fields_.end(), nullptr);
}

MultiFieldsRangeIndex::~MultiFieldsRangeIndex() {}

int MultiFieldsRangeIndex::Delete(int64_t docid, int field) {
  if (fields_[field] == nullptr) {
    return 0;
  }
  std::string value;
  int ret = table_->GetFieldRawValue(docid, field, value);
  if (ret != 0) {
    return ret;
  }

  ret = DeleteDoc(docid, field, value);

  return ret;
}

int MultiFieldsRangeIndex::AddDoc(int64_t docid, int field) {
  if (fields_[field] == nullptr) {
    return 0;
  }

  std::string key;
  int ret = table_->GetFieldRawValue(docid, field, key);
  if (ret != 0) {
    LOG(ERROR) << "get doc " << docid << " failed";
    return ret;
  }
  std::string key_str;

  if (fields_[field]->IsNumeric()) {
    std::string key2_str = FloatingToSortableStr(key);
    key_str = GenLogicalKey(field, key2_str, docid);
  } else {
    key_str = GenLogicalKey(field, key, docid);
  }

  auto &db = storage_mgr_->GetDB();
  rocksdb::ColumnFamilyHandle *cf_handler =
      storage_mgr_->GetColumnFamilyHandle(cf_id_);
  std::string null = "";
  rocksdb::Status s = db->Put(rocksdb::WriteOptions(), cf_handler,
                              rocksdb::Slice(key_str), null);
  if (!s.ok()) {
    std::stringstream msg;
    msg << "rocksdb put error:" << s.ToString() << ", key=" << key_str;
    LOG(ERROR) << msg.str();
    return -1;
  }

  return 0;
}

int MultiFieldsRangeIndex::DeleteDoc(int64_t docid, int field,
                                     std::string &key) {
  if (fields_[field] == nullptr) {
    return 0;
  }
  auto &db = storage_mgr_->GetDB();
  rocksdb::ColumnFamilyHandle *cf_handler =
      storage_mgr_->GetColumnFamilyHandle(cf_id_);

  std::string key_str;

  if (fields_[field]->IsNumeric()) {
    std::string key2_str = FloatingToSortableStr(key);
    key_str = GenLogicalKey(field, key2_str, docid);
  } else {
    key_str = GenLogicalKey(field, key, docid);
  }

  rocksdb::Status s =
      db->Delete(rocksdb::WriteOptions(), cf_handler, rocksdb::Slice(key_str));
  if (!s.ok()) {
    std::stringstream msg;
    msg << "rocksdb delete error:" << s.ToString() << ", key=" << key;
    LOG(ERROR) << msg.str();
    return -1;
  }

  return 0;
}

static constexpr float kFloatEpsilon = 1.0e-6f;
static constexpr double kDoubleEpsilon = 1.0e-15;

template <typename Type>
static void AdjustBoundary(std::string &boundary, int offset) {
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

int64_t MultiFieldsRangeIndex::Search(
    FilterOperator query_filter_operator,
    const std::vector<FilterInfo> &origin_filters,
    MultiRangeQueryResults *out) {
  out->Clear();

  std::vector<FilterInfo> filters;

  for (const auto &filter : origin_filters) {
    if ((size_t)filter.field >= fields_.size()) {
      LOG(ERROR) << "field index is out of range, field=" << filter.field;
      return -1;
    }

    if ((filter.is_union == FilterOperator::And) &&
        fields_[filter.field]->DataType() == DataType::STRING) {
      // type is string and operator is "and", split this filter
      std::vector<std::string> items = utils::split(filter.lower_value, kDelim);
      for (std::string &item : items) {
        FilterInfo f = filter;
        f.lower_value = item;
        filters.push_back(f);
      }
      continue;
    }
    filters.push_back(filter);
  }

  auto fsize = filters.size();

  RangeQueryResult result;
  RangeQueryResult result_not_in;
  bool first_result = true;
  bool first_result_not_in = true;
  bool have_not_in = false;
  result_not_in.SetNotIn(true);
  std::atomic<int64_t> retval{0};

  for (size_t i = 0; i < fsize; ++i) {
    RangeQueryResult result_tmp;
    RangeQueryResult result_not_in_tmp;
    result_not_in_tmp.SetNotIn(true);

    auto &filter = filters[i];
    if (filter.is_union == FilterOperator::Not) {
      have_not_in = true;
    }

    if (not filter.include_lower) {
      if (fields_[filter.field]->DataType() == DataType::INT) {
        AdjustBoundary<int>(filter.lower_value, 1);
      } else if (fields_[filter.field]->DataType() == DataType::LONG) {
        AdjustBoundary<long>(filter.lower_value, 1);
      } else if (fields_[filter.field]->DataType() == DataType::FLOAT) {
        AdjustBoundary<float>(filter.lower_value, 1);
      } else if (fields_[filter.field]->DataType() == DataType::DOUBLE) {
        AdjustBoundary<double>(filter.lower_value, 1);
      }
    }

    if (not filter.include_upper) {
      if (fields_[filter.field]->DataType() == DataType::INT) {
        AdjustBoundary<int>(filter.upper_value, -1);
      } else if (fields_[filter.field]->DataType() == DataType::LONG) {
        AdjustBoundary<long>(filter.upper_value, -1);
      } else if (fields_[filter.field]->DataType() == DataType::FLOAT) {
        AdjustBoundary<float>(filter.upper_value, -1);
      } else if (fields_[filter.field]->DataType() == DataType::DOUBLE) {
        AdjustBoundary<double>(filter.upper_value, -1);
      }
    }

    auto &db = storage_mgr_->GetDB();
    rocksdb::ColumnFamilyHandle *cf_handler =
        storage_mgr_->GetColumnFamilyHandle(cf_id_);
    std::string value;
    rocksdb::ReadOptions read_options;

    if (fields_[filter.field]->IsNumeric()) {
      std::unique_ptr<rocksdb::Iterator> it(
          db->NewIterator(read_options, cf_handler));

      std::string lower_key, upper_key;
      lower_key = ToRowKey(filter.field) + "_" +
                  FloatingToSortableStr(filter.lower_value) + "_";
      upper_key = ToRowKey(filter.field) + "_" +
                  FloatingToSortableStr(filter.upper_value) + "_";

      size_t prefix_len = lower_key.length();
      for (it->Seek(lower_key); it->Valid(); it->Next()) {
        std::string key = it->key().ToString();
        key = key.substr(0, prefix_len);
        if (key > upper_key) {
          break;
        }
        key = it->key().ToString();
        int64_t docid = utils::FromRowKey64(key.substr(key.length() - 8, 8));
        if (filter.is_union == FilterOperator::Not) {
          result_not_in_tmp.Add(docid);
        } else {
          result_tmp.Add(docid);
        }
        retval++;
      }
    } else {
      std::vector<std::string> items;
      if (fields_[filter.field]->DataType() == DataType::STRING) {
        items = utils::split(filter.lower_value, kDelim);
      } else {
        items.push_back(filter.lower_value);
      }

      for (size_t i = 0; i < items.size(); i++) {
        std::string item = items[i];
        std::unique_ptr<rocksdb::Iterator> it(
            db->NewIterator(read_options, cf_handler));

        std::string prefix = ToRowKey(filter.field) + "_" + item + "_";
        size_t prefix_len = prefix.length();

        for (it->Seek(prefix);
             it->Valid() && it->key().starts_with(prefix) &&
             it->key().size() == prefix_len + 8;  // 8 is the length of docid
             it->Next()) {
          std::string key = it->key().ToString();
          int64_t docid = utils::FromRowKey64(key.substr(key.length() - 8, 8));
          if (filter.is_union == FilterOperator::Not) {
            result_not_in_tmp.Add(docid);
          } else {
            result_tmp.Add(docid);
          }
          retval++;
        }
      }
    }

    if (filter.is_union != FilterOperator::Not) {
      if (first_result) {
        result = std::move(result_tmp);
        first_result = false;
      } else {
        if (query_filter_operator == FilterOperator::And) {
          result.Intersection(result_tmp);
        } else if (query_filter_operator == FilterOperator::Or) {
          result.Union(result_tmp);
        }
      }
    } else {
      if (first_result_not_in) {
        result_not_in = std::move(result_not_in_tmp);
        first_result_not_in = false;
      } else {
        if (query_filter_operator == FilterOperator::And) {
          result_not_in.Union(result_not_in_tmp);
        } else if (query_filter_operator == FilterOperator::Or) {
          result_not_in.Intersection(result_not_in_tmp);
        }
      }
    }
  }

  if (have_not_in) {
    RangeQueryResult all_result;
    all_result.AddRange(0, storage_mgr_->Size());
    all_result.IntersectionWithNotIn(result_not_in);

    if (query_filter_operator == FilterOperator::And && first_result) {
      result = std::move(all_result);
    } else if (query_filter_operator == FilterOperator::And) {
      result.IntersectionWithNotIn(result_not_in);
    } else if (query_filter_operator == FilterOperator::Or) {
      result.Union(all_result);
    }
    retval = result.Cardinality();
  }

  out->Add(std::move(result));
  return retval;
}

int64_t MultiFieldsRangeIndex::Query(
    FilterOperator query_filter_operator,
    const std::vector<FilterInfo> &origin_filters,
    std::vector<uint64_t> &docids, size_t topn) {
  MultiRangeQueryResults range_query_result;
  int64_t retval =
      Search(query_filter_operator, origin_filters, &range_query_result);
  if (retval <= 0) {
    return retval;
  }

  docids = range_query_result.GetDocIDs(topn);
  return retval;
}

int MultiFieldsRangeIndex::AddField(int field, enum DataType field_type,
                                    std::string &field_name) {
  fields_[field] = std::make_unique<FieldRangeIndex>(field, field_type);
  return 0;
}

}  // namespace vearch
