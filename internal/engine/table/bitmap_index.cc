/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "bitmap_index.h"
#include "scalar_index_utils.h"

#include "util/log.h"
#include "util/utils.h"

namespace vearch {

// ============================================================================
// BitmapIndex implementation
// ============================================================================

BitmapIndex::BitmapIndex(Table* table, StorageManager* storage_mgr, int cf_id, DataType data_type, int field_id)
    : ScalarIndex(table, storage_mgr, cf_id, data_type, field_id) {}

BitmapIndex::~BitmapIndex() {}

int BitmapIndex::Init() {
  Clear();

  if (storage_mgr_ == nullptr) {
    LOG(ERROR) << "Storage manager not set";
    return -1;
  }

  int64_t max_doc_id = storage_mgr_->Size();
  for (int64_t docid = 0; docid < max_doc_id; ++docid) {
    AddDoc(docid);
  }

  LOG(INFO) << "BitmapIndex::Init completed: " << data_.size()
            << " unique values, " << max_doc_id << " total docs";
  return 0;
}

int BitmapIndex::AddDoc(int64_t docid) {
  std::string value;
  int ret = table_->GetFieldRawValue(docid, field_id_, value);
  if (ret != 0) {
    LOG(ERROR) << "get doc " << docid << " field " << field_id_
               << " failed";
    return ret;
  }

  auto add_one = [&](const std::string& v) {
    std::string value_key = IsNumeric() ? NumericToSortableStr(data_type_, v) : v;
    auto it = data_.find(value_key);
    if (it == data_.end()) {
      roaring::Roaring64Map bitmap;
      bitmap.add(static_cast<uint64_t>(docid));
      data_[value_key] = bitmap;
    } else {
      it->second.add(static_cast<uint64_t>(docid));
    }
  };

  if (data_type_ == DataType::STRINGARRAY) {
    std::vector<std::string> values = utils::split(value, kStringArrayValueDelimiter);
    for (const auto& v : values) {
      add_one(v);
    }
  } else {
    add_one(value);
  }

  return 0;
}

int BitmapIndex::DeleteDoc(int64_t docid) {
  std::string value;
  int ret = table_->GetFieldRawValue(docid, field_id_, value);
  if (ret != 0) {
    return ret;
  }

  auto remove_one = [&](const std::string& v) {
    std::string value_key = IsNumeric() ? NumericToSortableStr(data_type_, v) : v;
    auto it = data_.find(value_key);
    if (it != data_.end()) {
      it->second.remove(static_cast<uint64_t>(docid));
    }
  };

  if (data_type_ == DataType::STRINGARRAY) {
    std::vector<std::string> values = utils::split(value, kStringArrayValueDelimiter);
    for (const auto& v : values) {
      remove_one(v);
    }
  } else {
    remove_one(value);
  }

  return 0;
}

ScalarIndexResult BitmapIndex::BitmapToResult(const roaring::Roaring64Map& bitmap) {
  ScalarIndexResult result(bitmap);
  return result;
}

ScalarIndexResult BitmapIndex::BitmapToResultWithOffsetLimit(const roaring::Roaring64Map& bitmap, int offset, int limit) {
  ScalarIndexResult result(bitmap, offset, limit);
  return result;
}

ScalarIndexResult BitmapIndex::In(const std::vector<std::string> &values, int offset, int limit) {
  roaring::Roaring64Map result;
  for (const auto &value : values) {
    std::string value_key = IsNumeric() ? NumericToSortableStr(data_type_, value) : value;
    auto it = data_.find(value_key);
    if (it != data_.end()) {
      result |= it->second;
    }
  }
  return BitmapToResultWithOffsetLimit(result, offset, limit);
}

ScalarIndexResult BitmapIndex::NotIn(const std::vector<std::string> &values, int offset, int limit) {
  roaring::Roaring64Map in_values;
  int64_t total_size = storage_mgr_->Size();
  for (const auto &value : values) {
    std::string value_key = IsNumeric() ? NumericToSortableStr(data_type_, value) : value;
    auto it = data_.find(value_key);
    if (it != data_.end()) {
      in_values |= it->second;
    }
  }

  roaring::Roaring64Map not_in;
  roaring::Roaring64Map all_values;
  all_values.addRange(0, total_size);
  not_in = all_values - in_values;
  return BitmapToResultWithOffsetLimit(not_in, offset, limit);
}

ScalarIndexResult BitmapIndex::Range(const std::string &lower_value,
                              bool lb_inclusive,
                              const std::string &upper_value,
                              bool ub_inclusive, int offset, int limit) {
  roaring::Roaring64Map result;

  std::string lower = lower_value;
  std::string upper = upper_value;
  if (IsNumeric()) {
    if (!lb_inclusive) {
      AdjustDataTypeBoundary(lower, data_type_, 1);
    }
    if (!ub_inclusive) {
      AdjustDataTypeBoundary(upper, data_type_, -1);
    }
    lower = NumericToSortableStr(data_type_, lower);
    upper = NumericToSortableStr(data_type_, upper);
  }

  for (const auto &kv : data_) {
    const std::string &key = kv.first;
    if (key >= lower && key <= upper) {
      result |= kv.second;
    }
  }

  return BitmapToResultWithOffsetLimit(result, offset, limit);
}

ScalarIndexResult BitmapIndex::Equal(const std::string &value, int offset, int limit) {
  std::string value_key = IsNumeric() ? NumericToSortableStr(data_type_, value) : value;
  auto it = data_.find(value_key);
  if (it != data_.end()) {
    return BitmapToResultWithOffsetLimit(it->second, offset, limit);
  }
  return ScalarIndexResult();
}

ScalarIndexResult BitmapIndex::NotEqual(const std::string &value, int offset, int limit) {
  std::string value_key = IsNumeric() ? NumericToSortableStr(data_type_, value) : value;
  int64_t total_size = storage_mgr_->Size();
  auto it = data_.find(value_key);
  roaring::Roaring64Map eq;
  if (it != data_.end()) {
    eq = it->second;
  }
  roaring::Roaring64Map all_values;
  all_values.addRange(0, total_size);
  roaring::Roaring64Map not_equal = all_values - eq;
  return BitmapToResultWithOffsetLimit(not_equal, offset, limit);
}

ScalarIndexResult BitmapIndex::LessThan(const std::string &value, int offset, int limit) {
  roaring::Roaring64Map result;
  std::string upper = value;
  if (IsNumeric()) {
    AdjustDataTypeBoundary(upper, data_type_, -1);
    upper = NumericToSortableStr(data_type_, upper);
  }

  for (const auto &kv : data_) {
    if (kv.first < upper) {
      result |= kv.second;
    }
  }
  return BitmapToResultWithOffsetLimit(result, offset, limit);
}

ScalarIndexResult BitmapIndex::LessEqual(const std::string &value, int offset, int limit) {
  roaring::Roaring64Map result;
  std::string upper = IsNumeric() ? NumericToSortableStr(data_type_, value) : value;

  for (const auto &kv : data_) {
    if (kv.first <= upper) {
      result |= kv.second;
    }
  }
  return BitmapToResultWithOffsetLimit(result, offset, limit);
}

ScalarIndexResult BitmapIndex::GreaterThan(const std::string &value, int offset, int limit) {
  roaring::Roaring64Map result;
  std::string lower = value;
  if (IsNumeric()) {
    AdjustDataTypeBoundary(lower, data_type_, 1);
    lower = NumericToSortableStr(data_type_, lower);
  }

  for (const auto &kv : data_) {
    if (kv.first > lower) {
      result |= kv.second;
    }
  }
  return BitmapToResultWithOffsetLimit(result, offset, limit);
}

ScalarIndexResult BitmapIndex::GreaterEqual(const std::string &value, int offset, int limit) {
  roaring::Roaring64Map result;
  std::string lower = IsNumeric() ? NumericToSortableStr(data_type_, value) : value;

  for (const auto &kv : data_) {
    if (kv.first >= lower) {
      result |= kv.second;
    }
  }
  return BitmapToResultWithOffsetLimit(result, offset, limit);
}

const roaring::Roaring64Map *BitmapIndex::GetBitmapForValue(
    const std::string &value) const {
  std::string value_key = IsNumeric() ? NumericToSortableStr(data_type_, value) : value;
  auto it = data_.find(value_key);
  if (it != data_.end()) {
    return &it->second;
  }
  return nullptr;
}

size_t BitmapIndex::GetIndexDataSize() {
  size_t size = 0;
  for (const auto &kv : data_) {
    size += kv.second.getSizeInBytes();
  }
  return size;
}

void BitmapIndex::Clear() {
  data_.clear();
}

}  // namespace vearch
