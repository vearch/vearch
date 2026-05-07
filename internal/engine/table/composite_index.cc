/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "composite_index.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <sstream>
#include <unordered_set>
#include <unordered_map>
#include <iomanip>

#include "inverted_index.h"
#include "scalar_index_utils.h"
#include "util/log.h"
#include "util/utils.h"

namespace vearch {

namespace {

// Composite key binary format constants
constexpr size_t kFieldIdBytes = sizeof(int);
constexpr size_t kDocidBytes = sizeof(int64_t);
constexpr size_t kStringLenBytes = 3;

// Build composite header (binary format).
// Format: [fid₁: 4B][fid₂: 4B]...[fidₙ: 4B]_
std::string BuildCompositeHeader(const std::vector<int>& field_ids) {
  std::string header;
  header.reserve(field_ids.size() * kFieldIdBytes);
  for (int fid : field_ids) {
    header += ToRowKey(fid);
  }
  header += "_";
  return header;
}

// Encode a field value into sortable binary format.
std::string EncodeSortableValue(enum DataType type, const std::string& raw_value) {
  if (type != DataType::STRING && type != DataType::STRINGARRAY) {
    return NumericToSortableStr(type, raw_value);
  }
  // STRING: [len: 3B be][content]
  std::string encoded;
  uint32_t len = static_cast<uint32_t>(raw_value.size());
  encoded.reserve(kStringLenBytes + len);
  for (int i = kStringLenBytes - 1; i >= 0; --i) {
    encoded.push_back(static_cast<char>((len >> (i * 8)) & 0xFF));
  }
  encoded += raw_value;
  return encoded;
}

// Encode a string value into sortable binary format.
std::string StringToSortableStr(enum DataType type, const std::string& raw_value) {
  if (type != DataType::STRING && type != DataType::STRINGARRAY) {
    return raw_value;
  }
  // STRING: [len: 3B be][content]
  std::string encoded;
  uint32_t len = static_cast<uint32_t>(raw_value.size());
  encoded.reserve(kStringLenBytes + len);
  for (int i = kStringLenBytes - 1; i >= 0; --i) {
    encoded.push_back(static_cast<char>((len >> (i * 8)) & 0xFF));
  }
  encoded += raw_value;
  return encoded;
}

// Build composite prefix for range seeks (binary format).
// Format: [header][val₁][val₂]...[valₙ]
std::string GenCompositeKeyPrefix(const std::vector<std::string>& raw_values,
                                 const std::vector<int>& field_ids,
                                 const std::vector<enum DataType>& data_types) {
  std::string key = BuildCompositeHeader(field_ids);
  for (size_t i = 0; i < raw_values.size(); ++i) {
    key += EncodeSortableValue(data_types[i], raw_values[i]);
  }
  return key;
}

// Build composite RocksDB key (binary format).
std::string GenCompositeFullKey(const std::vector<std::string>& raw_values,
                                  const std::vector<int>& field_ids,
                                  const std::vector<enum DataType>& data_types,
                                  int64_t docid) {
  std::string prefix = GenCompositeKeyPrefix(raw_values, field_ids, data_types);
  std::string key = prefix + utils::ToRowKey64(docid);
  return key;
}

// Decode docid from the tail of a binary composite key.
int64_t DecodeDocidFromBinaryKey(const std::string& key) {
  if (key.size() < kDocidBytes) return -1;
  return utils::FromRowKey64(key.substr(key.size() - kDocidBytes));
}

}  // namespace

namespace detail {

// Split STRINGARRAY value into individual elements.
std::vector<std::string> ExpandStringArrayValue(enum DataType type,
                                                 const std::string& raw_value) {
  std::vector<std::string> result;
  if (type == DataType::STRINGARRAY) {
    result = utils::split(raw_value, kStringArrayValueDelimiter);
  } else {
    result.push_back(raw_value);
  }
  return result;
}

}  // namespace detail

// ============================================================================
// CompositeIndex implementation
// ============================================================================

CompositeIndex::CompositeIndex(Table* table,
                               StorageManager* storage_mgr,
                               int cf_id,
                               std::vector<enum DataType> data_types,
                               std::vector<int> field_ids)
    : InvertedIndex(table, storage_mgr, cf_id, data_types[0], field_ids[0]),
      field_ids_(std::move(field_ids)),
      data_types_(std::move(data_types)) {
        header_key_ = BuildCompositeHeader(field_ids_);
      }

rocksdb::ColumnFamilyHandle* CompositeIndex::CfHandler() const {
  return storage_mgr_->GetColumnFamilyHandle(cf_id_);
}

std::string CompositeIndex::GenKeyPrefix(const std::string& index_value) const {
  std::string encoded_value = StringToSortableStr(data_types_[0], index_value);
  return header_key_ + encoded_value;
}

std::string CompositeIndex::GenKey(const std::string& index_value,
                                   int64_t docid) const {
  return GenKeyPrefix(index_value) + utils::ToRowKey64(docid);
}

void CompositeIndex::GenCompositeKey(int64_t docid,
                                       std::vector<std::string>* composite_keys) {
  std::vector<std::string> raw_vals;
  int ret = table_->GetFieldRawValues(docid, field_ids_, raw_vals);
  if (ret != 0) {
    LOG(ERROR) << "get doc " << docid << " fields failed";
    composite_keys->clear();
    return;
  }

  // Expand STRINGARRAY fields into multiple combinations
  std::vector<std::vector<std::string>> expanded_fields;
  for (size_t i = 0; i < field_ids_.size(); ++i) {
    expanded_fields.push_back(
        detail::ExpandStringArrayValue(data_types_[i], raw_vals[i]));
  }

  // Generate all combinations using Cartesian product
  size_t num_fields = field_ids_.size();
  std::vector<size_t> indices(num_fields, 0);

  while (true) {
    std::vector<std::string> raw_vals;
    for (size_t i = 0; i < num_fields; ++i) {
      raw_vals.push_back(expanded_fields[i][indices[i]]);
    }
    composite_keys->push_back(
        GenCompositeFullKey(raw_vals, field_ids_, data_types_, docid));

    // Advance indices like multi-digit counter
    size_t carry = 1;
    for (size_t i = num_fields - 1; i >= 1; --i) {
      indices[i] += carry;
      if (indices[i] >= expanded_fields[i].size()) {
        indices[i] = 0;
        carry = 1;
      } else {
        carry = 0;
        break;
      }
    }

    if (carry == 1) {
      indices[0]++;
      if (indices[0] >= expanded_fields[0].size()) {
        break;
      }
    }
  }
}

int CompositeIndex::AddDoc(int64_t docid) {
  std::vector<std::string> composite_keys;
  GenCompositeKey(docid, &composite_keys);
  if (composite_keys.empty()) {
    return -1;
  }

  auto& db = storage_mgr_->GetDB();
  rocksdb::ColumnFamilyHandle* cf_handler = CfHandler();
  rocksdb::WriteBatch batch;

  for (const auto& composite_key : composite_keys) {
    batch.Put(cf_handler, rocksdb::Slice(composite_key), rocksdb::Slice());
  }

  rocksdb::Status s = db->Write(rocksdb::WriteOptions(), &batch);
  if (!s.ok()) {
    LOG(ERROR) << "rocksdb write batch error: " << s.ToString();
    return -1;
  }
  return 0;
}

int CompositeIndex::DeleteDoc(int64_t docid) {
  std::vector<std::string> composite_keys;
  GenCompositeKey(docid, &composite_keys);
  if (composite_keys.empty()) {
    return -1;
  }

  auto& db = storage_mgr_->GetDB();
  rocksdb::ColumnFamilyHandle* cf_handler = CfHandler();
  rocksdb::WriteBatch batch;

  for (const auto& composite_key : composite_keys) {
    batch.Delete(cf_handler, rocksdb::Slice(composite_key));
  }

  rocksdb::Status s = db->Write(rocksdb::WriteOptions(), &batch);
  if (!s.ok()) {
    LOG(ERROR) << "rocksdb write batch delete error: " << s.ToString();
    return -1;
  }
  return 0;
}

bool isPrefixUnordered(const std::vector<int>& A, const std::vector<int>& B) {
  if (B.size() > A.size()) return false;
  if (B.empty()) return false;
  
  std::unordered_map<int, int> freq;
  freq.reserve(B.size() * 2);
  
  for (int x : B) {
    if (freq.find(x) == freq.end()) {
      freq[x] = 1;
    } else {
      freq[x]++;
    }
  }
  
  for (size_t i = 0; i < B.size(); ++i) {
      int x = A[i];
      auto it = freq.find(x);
      if (it == freq.end()) return false;
      
      if (--(it->second) == 0) {
          freq.erase(it);
      }
  }

  return freq.empty();
}


bool CompositeIndex::CanUseFilterMode(const std::vector<int>& field_ids,
  const std::vector<CompositeFilterMode>& modes,
  CompositeStrategy& strategy) {
  if (!isPrefixUnordered(field_ids_, field_ids)) {
    return false;
  }
  int range_count = 0;
  int equal_count = 0;
  int not_equal_count = 0;
  int not_in_count = 0;
  int in_count = 0;
  strategy = CompositeStrategy::NONE;
  for (size_t i = 0; i < modes.size(); i++) {
    if ((modes[i] == CompositeFilterMode::NotEqual 
        || modes[i] == CompositeFilterMode::NotIn) 
        && (i > 0 || field_ids.size() != 1)) {
      return false;
    }
    if (modes[i] == CompositeFilterMode::Equal) {
      equal_count++;
    }
    if (modes[i] == CompositeFilterMode::NotEqual) {
      not_equal_count++;
    }
    if (modes[i] == CompositeFilterMode::NotIn) {
      not_in_count++;
    }
    if (modes[i] == CompositeFilterMode::In) {
      in_count++;
    }
    if (modes[i] == CompositeFilterMode::Range) {
      if (i != field_ids.size() - 1) {
        return false;
      }
      range_count++;
    }
  }
  if (range_count > 1 || not_equal_count > 1 || not_in_count > 1) {
    return false;
  }

  if (range_count == 1 && not_equal_count == 0 && not_in_count == 0 && in_count == 0) {
    strategy = CompositeStrategy::RANGE;
    return true;
  }
  if (not_equal_count == 1 && equal_count == 0 && range_count == 0 && not_in_count == 0 && in_count == 0) {
    strategy = CompositeStrategy::NOT_EQUAL;
    return true;
  }
  if (not_in_count == 1 && equal_count == 0 && range_count == 0 && not_equal_count == 0 && in_count == 0) {
    strategy = CompositeStrategy::NOT_IN;
    return true;
  }
  if (equal_count > 0 && not_equal_count == 0 && range_count == 0 && not_in_count == 0 && in_count == 0) {
    strategy = CompositeStrategy::EQUAL;
    return true;
  }
  if (in_count > 0) {
    strategy = CompositeStrategy::IN;
    return true;
  }
  return false;
}

// ============================================================================
// Query methods with leftmost prefix matching
// ============================================================================

ScalarIndexResult CompositeIndex::Equal(const std::vector<std::string>& prefix_values,
                                        int offset, int limit) {
  if (prefix_values.size() == 0 || prefix_values.size() > field_ids_.size()) {
    return ScalarIndexResult();
  }

  if (storage_mgr_ == nullptr) {
    return ScalarIndexResult();
  }

  auto& db = storage_mgr_->GetDB();
  rocksdb::ColumnFamilyHandle* cf_handler = CfHandler();
  rocksdb::ReadOptions read_options;

  std::string prefix_key = GenCompositeKeyPrefix(prefix_values, field_ids_, data_types_);

  ScalarIndexResult result;
  int count = 0;
  int skipped = 0;

  std::unique_ptr<rocksdb::Iterator> iter(db->NewIterator(read_options, cf_handler));
  for (iter->Seek(prefix_key);
       iter->Valid() && iter->key().starts_with(prefix_key);
       iter->Next()) {
    if (skipped < offset) {
      skipped++;
      continue;
    }
    int64_t docid = DecodeDocidFromBinaryKey(iter->key().ToString());
    if (docid >= 0) {
      result.Add(docid);
      count++;
      if (limit > 0 && count >= limit) {
        break;
      }
    }
  }

  return result;
}

std::string toHexString(const std::string& binary) {
  std::ostringstream oss;
  for (unsigned char c : binary) {
      oss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(c);
  }
  return oss.str();
}

ScalarIndexResult CompositeIndex::Range(const std::vector<std::string>& prefix_values,
                                       const std::string& lower_value,
                                       const std::string& upper_value,
                                       bool include_lower,
                                       bool include_upper,
                                       int offset, int limit) {
  if (prefix_values.size() > field_ids_.size()) {
    return ScalarIndexResult();
  }

  if (storage_mgr_ == nullptr) {
    return ScalarIndexResult();
  }

  auto& db = storage_mgr_->GetDB();
  rocksdb::ColumnFamilyHandle* cf_handler = CfHandler();
  rocksdb::ReadOptions read_options;

  std::string lower_key = BuildCompositeHeader(field_ids_);
  std::string upper_key = BuildCompositeHeader(field_ids_);

  size_t prefix_len = prefix_values.size();
  for (size_t i = 0; i < field_ids_.size(); ++i) {
    std::string encoded_val;
    if (i < prefix_len) {
      // Prefix fields: exact value
      encoded_val = EncodeSortableValue(data_types_[i], prefix_values[i]);
    } else if (i == prefix_len) {
      // Range field: boundary value
      if (lower_value.empty()) {
        encoded_val = MinSortableValue(data_types_[i]);
      } else {
        std::string adjusted_lower = lower_value;
        if (!include_lower) {
          AdjustDataTypeBoundary(adjusted_lower, data_types_[i], 1);
        }
        encoded_val = EncodeSortableValue(data_types_[i], adjusted_lower);
      }
    }
    lower_key += encoded_val;
  }

  for (size_t i = 0; i < field_ids_.size(); ++i) {
    std::string encoded_val;
    if (i < prefix_len) {
      // Prefix fields: exact value
      encoded_val = EncodeSortableValue(data_types_[i], prefix_values[i]);
    } else if (i == prefix_len) {
      // Range field: boundary value
      if (upper_value.empty()) {
        encoded_val = MaxSortableValue(data_types_[i]);
      } else {
        std::string adjusted_upper = upper_value;
        if (!include_upper) {
          AdjustDataTypeBoundary(adjusted_upper, data_types_[i], -1);
        }
        encoded_val = EncodeSortableValue(data_types_[i], adjusted_upper);
      }
    }
    upper_key += encoded_val;
  }

  ScalarIndexResult result;
  size_t prefix_length = lower_key.length();
  std::unique_ptr<rocksdb::Iterator> iter(db->NewIterator(read_options, cf_handler));
  for (iter->Seek(lower_key);
       iter->Valid();
       iter->Next()) {
    std::string key = iter->key().ToString();
    std::string key_prefix = key.substr(0, prefix_length);
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
    int64_t docid = DecodeDocidFromBinaryKey(key);
    if (docid >= 0) {
      result.Add(docid);
    }
  }

  return result;
}

ScalarIndexResult CompositeIndex::In(const std::vector<std::string>& prefix_values,
                                         const std::vector<std::vector<std::string>>& field_values,
                                         int offset, int limit) {
  if (prefix_values.size() > field_ids_.size()) {
    return ScalarIndexResult();
  }

  for (const auto& values : field_values) {
    if (values.empty()) {
      return ScalarIndexResult();
    }
  }

  std::vector<std::vector<std::string>> all_field_values;
  all_field_values.reserve(field_ids_.size());
  for (size_t i = 0; i < prefix_values.size(); ++i) {
    all_field_values.push_back({prefix_values[i]});
  }
  for (const auto& values : field_values) {
    all_field_values.push_back(values);
  }

  std::vector<std::vector<std::string>> all_combinations;
  std::vector<size_t> indices(field_ids_.size(), 0);

  while (true) {
    std::vector<std::string> combo;
    combo.reserve(field_ids_.size());
    for (size_t i = 0; i < field_ids_.size(); ++i) {
      combo.push_back(all_field_values[i][indices[i]]);
    }
    all_combinations.push_back(std::move(combo));

    size_t carry = 1;
    for (int i = field_ids_.size() - 1; i >= 0; --i) {
      indices[i]++;
      if (indices[i] < all_field_values[i].size()) {
        carry = 0;
        break;
      }
      indices[i] = 0;
    }
    if (carry == 1) break;
  }

  ScalarIndexResult combined_result;
  for (const auto& values : all_combinations) {
    std::string prefix_key = GenCompositeKeyPrefix(values, field_ids_, data_types_);
    auto& db = storage_mgr_->GetDB();
    rocksdb::ColumnFamilyHandle* cf_handler = CfHandler();
    rocksdb::ReadOptions read_options;

    std::unique_ptr<rocksdb::Iterator> iter(db->NewIterator(read_options, cf_handler));
    for (iter->Seek(prefix_key);
         iter->Valid() && iter->key().starts_with(prefix_key);
         iter->Next()) {
      int64_t docid = DecodeDocidFromBinaryKey(iter->key().ToString());
      if (docid >= 0) {
        combined_result.Add(docid);
      }
    }
  }

  if (offset > 0 || limit > 0) {
    return ScalarIndexResult(combined_result, offset, limit);
  }
  return combined_result;
}

} // namespace vearch
