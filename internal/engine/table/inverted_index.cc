/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "inverted_index.h"
#include "scalar_index_utils.h"

#include <sstream>

#include "util/log.h"
#include "util/utils.h"

namespace vearch {

// ============================================================================
// InvertedIndex implementation
// ============================================================================

InvertedIndex::InvertedIndex(Table* table, StorageManager* storage_mgr, int cf_id, DataType data_type, int field_id)
    : ScalarIndex(table, storage_mgr, cf_id, data_type, field_id) {}

std::string InvertedIndex::GenKeyPrefix(const std::string& index_value) const {
  return ToRowKey(field_id_) + "_" + index_value + "_";
}

std::string InvertedIndex::GenKey(const std::string& index_value, int64_t docid) const {
  return GenKeyPrefix(index_value) + utils::ToRowKey64(docid);
}

int InvertedIndex::AddDoc(int64_t docid) {
  std::string key;
  int ret = table_->GetFieldRawValue(docid, field_id_, key);
  if (ret != 0) {
    LOG(ERROR) << "get doc " << docid << " failed";
    return ret;
  }

  auto &db = storage_mgr_->GetDB();
  rocksdb::ColumnFamilyHandle *cf_handler =
      storage_mgr_->GetColumnFamilyHandle(cf_id_);

  std::string key_str;
  std::string null = "";

  if (IsNumeric()) {
    std::string sortable_str = NumericToSortableStr(data_type_, key);
    key_str = GenKey(sortable_str, docid);
  } else if (data_type_ == DataType::STRINGARRAY) {
    std::vector<std::string> keys = utils::split(key, kStringArrayValueDelimiter);
    for (const auto &k : keys) {
      key_str = GenKey(k, docid);
      rocksdb::Status s = db->Put(rocksdb::WriteOptions(), cf_handler,
                                  rocksdb::Slice(key_str), null);
      if (!s.ok()) {
        std::stringstream msg;
        msg << "rocksdb put error:" << s.ToString() << ", key=" << key_str;
        LOG(ERROR) << msg.str();
        return -1;
      }
    }
    return 0;
  } else {
    key_str = GenKey(key, docid);
  }

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

int InvertedIndex::DeleteDoc(int64_t docid) {
  std::string key;
  int ret = table_->GetFieldRawValue(docid, field_id_, key);
  if (ret != 0) {
    return ret;
  }

  auto &db = storage_mgr_->GetDB();
  rocksdb::ColumnFamilyHandle *cf_handler =
      storage_mgr_->GetColumnFamilyHandle(cf_id_);

  std::string key_str;

  if (IsNumeric()) {
    std::string sortable_str = NumericToSortableStr(data_type_, key);
    key_str = GenKey(sortable_str, docid);
  } else if (data_type_ == DataType::STRINGARRAY) {
    std::vector<std::string> keys = utils::split(key, kStringArrayValueDelimiter);
    for (const auto &k : keys) {
      key_str = GenKey(k, docid);
      rocksdb::Status s = db->Delete(rocksdb::WriteOptions(), cf_handler,
                                     rocksdb::Slice(key_str));
      if (!s.ok()) {
        std::stringstream msg;
        msg << "rocksdb delete error:" << s.ToString() << ", key=" << key_str;
        LOG(ERROR) << msg.str();
        return -1;
      }
    }
    return 0;
  } else {
    key_str = GenKey(key, docid);
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

void InvertedIndex::ScanRange(const std::string& lower_key, const std::string& upper_key, 
                             ScalarIndexResult& result, int offset, int limit) {
  if (storage_mgr_ == nullptr) {
    LOG(ERROR) << "Storage manager not set";
    return;
  }

  auto &db = storage_mgr_->GetDB();
  rocksdb::ColumnFamilyHandle *cf_handler =
      storage_mgr_->GetColumnFamilyHandle(cf_id_);
  rocksdb::ReadOptions read_options;

  std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(read_options, cf_handler));
  
  size_t prefix_len = lower_key.length();
  for (it->Seek(lower_key); it->Valid(); it->Next()) {
    std::string key = it->key().ToString();
    std::string key_prefix = key.substr(0, prefix_len);
    if (key_prefix > upper_key) {
      break;
    }
    if (offset > 0) {
      offset--;
      continue;
    }
    if (limit > 0 && result.Cardinality() >= limit) {
      break;
    }

    int64_t docid = utils::FromRowKey64(key.substr(key.length() - 8, 8));
    result.Add(docid);
  }
}

ScalarIndexResult InvertedIndex::In(const std::vector<std::string>& values, int offset, int limit) {
  ScalarIndexResult result;
  
  if (storage_mgr_ == nullptr) {
    return result;
  }

  auto &db = storage_mgr_->GetDB();
  rocksdb::ColumnFamilyHandle *cf_handler =
      storage_mgr_->GetColumnFamilyHandle(cf_id_);
  rocksdb::ReadOptions read_options;

  for (const auto &value : values) {
    std::string sortable_str = NumericToSortableStr(data_type_, value);
    std::string prefix = GenKeyPrefix(sortable_str);
    std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(read_options, cf_handler));
    for (it->Seek(prefix);
         it->Valid() && it->key().starts_with(prefix);
         it->Next()) {
      if (offset > 0) {
        offset--;
        continue;
      }
      if (limit > 0 && result.Cardinality() >= limit) {
        break;
      }
      std::string key = it->key().ToString();
      int64_t docid = utils::FromRowKey64(key.substr(key.length() - 8, 8));
      result.Add(docid);
    }
  }

  return result;
}

ScalarIndexResult InvertedIndex::NotIn(const std::vector<std::string>& values, int offset, int limit) {
  ScalarIndexResult result;

  if (storage_mgr_ == nullptr) {
    return result;
  }

  int64_t total_size = storage_mgr_->Size();
  ScalarIndexResult matched = In(values, 0, 0);

  result.AddRange(0, total_size);
  result.IntersectionWithNotIn(matched);

  if (offset > 0 || limit > 0) {
    ScalarIndexResult offset_limited(result, offset, limit);
    return offset_limited;
  }
  return result;
}

ScalarIndexResult InvertedIndex::Range(const std::string& lower_value, bool lb_inclusive,
                                    const std::string& upper_value, bool ub_inclusive, int offset, int limit) {
  std::string adjusted_lower = lower_value;
  std::string adjusted_upper = upper_value;

  if (!lb_inclusive) {
    AdjustDataTypeBoundary(adjusted_lower, data_type_, 1);
  }

  if (!ub_inclusive) {
    AdjustDataTypeBoundary(adjusted_upper, data_type_, -1);
  }

  std::string lower_key, upper_key;
  lower_key = GenKeyPrefix(NumericToSortableStr(data_type_, adjusted_lower));
  upper_key = GenKeyPrefix(NumericToSortableStr(data_type_, adjusted_upper));

  ScalarIndexResult result;
  ScanRange(lower_key, upper_key, result, offset, limit);
  return result;
}

ScalarIndexResult InvertedIndex::Equal(const std::string& value, int offset, int limit) {
  return In({value}, offset, limit);
}

ScalarIndexResult InvertedIndex::NotEqual(const std::string& value, int offset, int limit) {
  return NotIn({value}, offset, limit);
}

ScalarIndexResult InvertedIndex::LessThan(const std::string& value, int offset, int limit) {
  std::string lower_key, upper_key;
  if (IsNumeric()) {
    std::string adjusted = value;
    AdjustDataTypeBoundary(adjusted, data_type_, -1);
    lower_key = GenKeyPrefix(MinSortableValue(data_type_));
    upper_key = GenKeyPrefix(NumericToSortableStr(data_type_, adjusted));
  } else {
    lower_key = GenKeyPrefix("");
    upper_key = GenKeyPrefix(value);
  }
  ScalarIndexResult result;
  ScanRange(lower_key, upper_key, result, offset, limit);
  return result;
}

ScalarIndexResult InvertedIndex::LessEqual(const std::string& value, int offset, int limit) {
  std::string lower_key, upper_key;
  if (IsNumeric()) {
    lower_key = GenKeyPrefix(MinSortableValue(data_type_));
    upper_key = GenKeyPrefix(NumericToSortableStr(data_type_, value));
  } else {
    lower_key = GenKeyPrefix("");
    upper_key = GenKeyPrefix(value);
  }
  ScalarIndexResult result;
  ScanRange(lower_key, upper_key, result, offset, limit);
  return result;
}

ScalarIndexResult InvertedIndex::GreaterThan(const std::string& value, int offset, int limit) {
  std::string lower_key, upper_key;
  if (IsNumeric()) {
    std::string adjusted = value;
    AdjustDataTypeBoundary(adjusted, data_type_, 1);
    lower_key = GenKeyPrefix(NumericToSortableStr(data_type_, adjusted));
    upper_key = GenKeyPrefix(MaxSortableValue(data_type_));
  } else {
    lower_key = GenKeyPrefix(value);
    upper_key = GenKeyPrefix("");
  }
  ScalarIndexResult result;
  ScanRange(lower_key, upper_key, result, offset, limit);
  return result;
}

ScalarIndexResult InvertedIndex::GreaterEqual(const std::string& value, int offset, int limit) {
  std::string lower_key, upper_key;
  if (IsNumeric()) {
    lower_key = GenKeyPrefix(NumericToSortableStr(data_type_, value));
    upper_key = GenKeyPrefix(MaxSortableValue(data_type_));
  } else {
    lower_key = GenKeyPrefix(value);
    upper_key = GenKeyPrefix("");
  }
  ScalarIndexResult result;
  ScanRange(lower_key, upper_key, result, offset, limit);
  return result;
}

}  // namespace vearch
