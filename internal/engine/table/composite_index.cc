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

int CompositeIndex::DropAll() {
  auto& db = storage_mgr_->GetDB();
  rocksdb::ColumnFamilyHandle* cf_handler = CfHandler();
  if (db == nullptr || cf_handler == nullptr) {
    LOG(ERROR) << "DropAll: db or cf handler is null";
    return -1;
  }

  // header_key_ ends with '_' and is the common prefix of every key this
  // composite index writes. The exclusive upper bound must be the prefix with
  // its last byte incremented (AdvancePrefix), NOT header_key_ + 0xFF: a
  // sortable-encoded field value can begin with 0xFF (e.g. an INT near INT_MAX),
  // so "header_key_ + 0xFF" would sort below such a key and DeleteRange would
  // skip it, leaving stale keys behind.
  std::string begin_key = header_key_;
  std::string end_key = AdvancePrefix(header_key_);
  if (end_key.empty()) {
    // header_key_ ends with '_' (0x5F), so this never happens; guard anyway.
    LOG(ERROR) << "CompositeIndex::DropAll: prefix is all 0xFF, cannot bound";
    return -1;
  }

  rocksdb::Status s = db->DeleteRange(rocksdb::WriteOptions(), cf_handler,
                                      rocksdb::Slice(begin_key),
                                      rocksdb::Slice(end_key));
  if (!s.ok()) {
    LOG(ERROR) << "DropAll DeleteRange failed: " << s.ToString();
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

bool CompositeIndex::CanUseScan(const std::vector<int>& query_field_ids,
                                const std::vector<CompositeFilterMode>& modes) const {
  if (query_field_ids.empty() || query_field_ids.size() != modes.size()) {
    return false;
  }
  std::unordered_set<int> idx_set(field_ids_.begin(), field_ids_.end());
  for (int f : query_field_ids) {
    if (idx_set.find(f) == idx_set.end()) return false;
  }
  // All modes in the enum are point-decidable; reject nothing here, but keep
  // the gate so future None / wildcard modes are explicitly excluded.
  for (auto m : modes) {
    switch (m) {
      case CompositeFilterMode::Equal:
      case CompositeFilterMode::NotEqual:
      case CompositeFilterMode::In:
      case CompositeFilterMode::NotIn:
      case CompositeFilterMode::Range:
        break;
      default:
        return false;
    }
  }
  return true;
}

namespace {

// Pull the i-th field value out of a composite key body (the bytes between
// header and trailing docid). Returns false on malformed key. On success
// `cursor` advances past the consumed bytes and `out` holds the raw value:
//   numeric: sortable bytes (caller compares against sortable filter bounds)
//   string : raw content (length prefix stripped)
bool ExtractField(const std::string& body, size_t& cursor,
                  enum DataType dtype, std::string& out) {
  switch (dtype) {
    case DataType::INT:
    case DataType::FLOAT: {
      constexpr size_t kBytes = sizeof(int32_t);
      if (cursor + kBytes > body.size()) return false;
      out.assign(body, cursor, kBytes);
      cursor += kBytes;
      return true;
    }
    case DataType::LONG:
    case DataType::DATE:
    case DataType::DOUBLE: {
      constexpr size_t kBytes = sizeof(int64_t);
      if (cursor + kBytes > body.size()) return false;
      out.assign(body, cursor, kBytes);
      cursor += kBytes;
      return true;
    }
    case DataType::STRING:
    case DataType::STRINGARRAY: {
      if (cursor + kStringLenBytes > body.size()) return false;
      uint32_t len = 0;
      for (size_t i = 0; i < kStringLenBytes; ++i) {
        len = (len << 8) | static_cast<uint8_t>(body[cursor + i]);
      }
      cursor += kStringLenBytes;
      if (cursor + len > body.size()) return false;
      out.assign(body, cursor, len);
      cursor += len;
      return true;
    }
    default:
      return false;
  }
}

// Apply one filter to one entry's raw field value. For numeric fields the
// raw value is sortable bytes; we compare directly against pre-encoded
// filter bounds (caller is responsible for encoding lower/upper the same
// way). For string fields, raw is the literal content.
bool MatchOne(const std::string& raw,
              enum DataType dtype,
              const FilterInfo& filter,
              const std::string& enc_lower,
              const std::string& enc_upper,
              const std::vector<std::string>* string_in_items) {
  if (dtype == DataType::STRING || dtype == DataType::STRINGARRAY) {
    if (string_in_items == nullptr) return false;
    bool found = false;
    for (const auto& item : *string_in_items) {
      if (raw == item) { found = true; break; }
    }
    if (filter.filter_operator == FilterOperator::Not) return !found;
    return found;
  }
  // Numeric: raw is already sortable, so memcmp gives correct order.
  // Equal: raw == enc_lower (enc_lower == enc_upper, both inclusive)
  // NotEqual: raw != enc_lower
  // Range: lower <= raw <= upper, with inclusive flags
  bool eq_bounds = (enc_lower == enc_upper);
  if (eq_bounds && filter.include_lower && filter.include_upper) {
    bool eq = (raw == enc_lower);
    if (filter.filter_operator == FilterOperator::Not) return !eq;
    return eq;
  }
  if (!enc_lower.empty()) {
    if (filter.include_lower) {
      if (raw < enc_lower) return false;
    } else {
      if (raw <= enc_lower) return false;
    }
  }
  if (!enc_upper.empty()) {
    if (filter.include_upper) {
      if (raw > enc_upper) return false;
    } else {
      if (raw >= enc_upper) return false;
    }
  }
  return true;
}

// ============================================================================
// Scan: internal helpers
// ============================================================================

// Precomputed view of a FilterInfo against this composite index: which
// position in field_ids_ it constrains, its data type, and any sortable-
// encoded bounds / split items that the per-entry evaluator will want.
struct PreparedFilter {
  int idx_pos;             // position in field_ids_
  enum DataType dtype;
  const FilterInfo* fi;
  std::string enc_lower;   // sortable-encoded lower bound (numeric)
  std::string enc_upper;   // sortable-encoded upper bound (numeric)
  std::vector<std::string> string_items;  // split for IN/NotIn
};

// One contiguous RocksDB iterator range that the segmented scan visits.
struct ScanSegment {
  std::string seek_start;    // iter->Seek target
  std::string prefix_match;  // require key.starts_with(prefix_match)
  // Optional upper-bound short-circuit: applies to the raw bytes
  // extracted at position upper_pos. Empty enc_upper disables.
  int upper_pos = -1;
  std::string enc_upper;
  bool include_upper = true;
};

constexpr size_t kInSegmentThreshold = 16;

// Resolve every filter against the composite layout, dropping filters whose
// field isn't part of this index. Numeric bounds are sortable-encoded once
// here so the hot loop only has to memcmp.
std::vector<PreparedFilter> PrepareFilters(
    const std::vector<FilterInfo>& filters,
    const std::vector<int>& field_ids,
    const std::vector<enum DataType>& data_types) {
  std::vector<PreparedFilter> prepared;
  prepared.reserve(filters.size());
  for (const auto& f : filters) {
    auto it = std::find(field_ids.begin(), field_ids.end(), f.field);
    if (it == field_ids.end()) {
      LOG(WARNING) << "Scan: filter field " << f.field
                   << " not in composite, skip filter";
      continue;
    }
    PreparedFilter pf;
    pf.idx_pos = static_cast<int>(it - field_ids.begin());
    pf.dtype = data_types[pf.idx_pos];
    pf.fi = &f;
    if (pf.dtype == DataType::STRING || pf.dtype == DataType::STRINGARRAY) {
      pf.string_items = utils::split(f.lower_value, kStringArrayValueDelimiter);
    } else {
      if (!f.lower_value.empty()) {
        pf.enc_lower = EncodeSortableValue(pf.dtype, f.lower_value);
      }
      if (!f.upper_value.empty()) {
        pf.enc_upper = EncodeSortableValue(pf.dtype, f.upper_value);
      }
    }
    prepared.push_back(std::move(pf));
  }
  return prepared;
}

// True iff any field in this composite is STRINGARRAY. When true, multiple
// RocksDB entries can share one docid (cartesian expansion), so the scan
// must aggregate per-docid before evaluating NOT IN / IN semantics.
bool HasStringArray(const std::vector<enum DataType>& data_types) {
  for (auto dt : data_types) {
    if (dt == DataType::STRINGARRAY) return true;
  }
  return false;
}

// Build the longest leading-Equal seek prefix: walk field_ids from position
// 0 and append encoded values for each consecutive position whose filter
// pins the field to a single value. The first position NOT covered is
// returned in `out_next_pos`.
//
// Anything that breaks the chain (no filter, negation, range, multi-value
// IN, ambiguous STRINGARRAY case, or OR semantics across filters) stops
// the prefix at that point; remaining fields still get evaluated via the
// existing per-entry / per-docid aggregation path.
//
// STRINGARRAY rule: each doc with an N-element array writes N RocksDB
// entries (cartesian-expanded over other STRINGARRAY fields too). A prefix
// on a STRINGARRAY position would skip the doc's other-element rows. That
// is safe iff:
//   1. the field is constrained by exactly one filter in this query, AND
//   2. that filter is positive single-value (IN with one item or "=").
// Otherwise the per-docid aggregated view would be missing values that
// live in skipped segments, leading to wrong results. Multi-value IN can
// be safely handled via multi-segment sub-scans below; that path is taken
// when the prefix chain ends, not here.
std::string BuildEqualPrefix(const std::string& header_key,
                             const std::vector<int>& field_ids,
                             const std::vector<PreparedFilter>& prepared,
                             FilterOperator inner_op,
                             size_t* out_next_pos) {
  std::string equal_prefix = header_key;
  size_t next_pos = 0;
  if (inner_op != FilterOperator::And) {
    *out_next_pos = next_pos;
    return equal_prefix;
  }

  std::unordered_map<int, int> pos_filter_count;
  std::unordered_map<int, const PreparedFilter*> by_pos;
  for (const auto& pf : prepared) {
    pos_filter_count[pf.idx_pos]++;
    by_pos.emplace(pf.idx_pos, &pf);
  }

  for (size_t pos = 0; pos < field_ids.size(); ++pos) {
    auto it = by_pos.find(static_cast<int>(pos));
    if (it == by_pos.end()) break;
    const PreparedFilter* pf = it->second;
    if (pf->fi->filter_operator == FilterOperator::Not) break;
    if (pf->dtype == DataType::STRINGARRAY) {
      if (pos_filter_count[pf->idx_pos] != 1) break;
      if (pf->string_items.size() != 1) break;
      equal_prefix += EncodeSortableValue(pf->dtype, pf->string_items[0]);
    } else if (pf->dtype == DataType::STRING) {
      if (pf->string_items.size() != 1) break;
      equal_prefix += EncodeSortableValue(pf->dtype, pf->string_items[0]);
    } else {
      if (pf->enc_lower.empty() || pf->enc_lower != pf->enc_upper) break;
      if (!pf->fi->include_lower || !pf->fi->include_upper) break;
      equal_prefix += pf->enc_lower;
    }
    next_pos = pos + 1;
  }
  *out_next_pos = next_pos;
  return equal_prefix;
}

// Derive scan segments from equal_prefix + the next field's filter, if one
// exists and is amenable to further narrowing:
//   * IN with <= kInSegmentThreshold values  -> N segments, one per value
//   * Range with a lower bound               -> 1 segment, start at lower,
//                                               early-break when this field
//                                               exceeds the encoded upper.
//   * Anything else                          -> 1 segment == equal_prefix
std::vector<ScanSegment> BuildScanSegments(
    const std::string& equal_prefix,
    size_t equal_prefix_pos,
    const std::vector<int>& field_ids,
    const std::vector<PreparedFilter>& prepared,
    FilterOperator inner_op) {
  std::vector<ScanSegment> segments;

  if (inner_op == FilterOperator::And &&
      equal_prefix_pos < field_ids.size()) {
    const PreparedFilter* next_pf = nullptr;
    for (const auto& pf : prepared) {
      if (pf.idx_pos == static_cast<int>(equal_prefix_pos)) {
        next_pf = &pf;
        break;
      }
    }
    if (next_pf != nullptr &&
        next_pf->fi->filter_operator != FilterOperator::Not &&
        next_pf->dtype != DataType::STRINGARRAY) {
      bool is_string_in =
          (next_pf->dtype == DataType::STRING) &&
          next_pf->string_items.size() >= 2 &&
          next_pf->string_items.size() <= kInSegmentThreshold;
      // Numeric Range path: has a lower bound; upper may be empty.
      bool is_numeric_range =
          (next_pf->dtype != DataType::STRING) &&
          !next_pf->enc_lower.empty();
      if (is_string_in) {
        segments.reserve(next_pf->string_items.size());
        for (const auto& item : next_pf->string_items) {
          ScanSegment s;
          s.seek_start = equal_prefix + EncodeSortableValue(next_pf->dtype, item);
          s.prefix_match = s.seek_start;
          segments.push_back(std::move(s));
        }
      } else if (is_numeric_range) {
        ScanSegment s;
        s.seek_start = equal_prefix + next_pf->enc_lower;
        s.prefix_match = equal_prefix;
        s.upper_pos = static_cast<int>(equal_prefix_pos);
        s.enc_upper = next_pf->enc_upper;  // may be empty (open upper)
        s.include_upper = next_pf->fi->include_upper;
        segments.push_back(std::move(s));
      }
    }
  }

  if (segments.empty()) {
    ScanSegment s;
    s.seek_start = equal_prefix;
    s.prefix_match = equal_prefix;
    segments.push_back(std::move(s));
  }
  return segments;
}

// Decode each composite field's raw value from a RocksDB key body. Returns
// false on malformed key (caller should skip it).
bool DecodeKeyFields(const std::string& key,
                     size_t header_len,
                     const std::vector<enum DataType>& data_types,
                     std::vector<std::string>* raw_vals) {
  if (key.size() < header_len + sizeof(int64_t)) return false;
  size_t body_end = key.size() - sizeof(int64_t);
  std::string body(key.data() + header_len, body_end - header_len);
  size_t cursor = 0;
  raw_vals->assign(data_types.size(), {});
  for (size_t i = 0; i < data_types.size(); ++i) {
    if (!ExtractField(body, cursor, data_types[i], (*raw_vals)[i])) {
      return false;
    }
  }
  return true;
}

// Per-docid filter evaluation. For STRINGARRAY-bearing composites the
// caller has aggregated every observed value for the field; semantics:
//   - IN  (Or):  match iff ANY observed value is in the IN list.
//   - NotIn(Not): match iff NO observed value is in the list.
//   - Range/Equal/NotEqual (numeric): same any/none rules.
bool EvalFilterForDocid(const PreparedFilter& pf,
                        const std::vector<std::string>& values) {
  if (values.empty()) return false;
  if (pf.dtype == DataType::STRING || pf.dtype == DataType::STRINGARRAY) {
    bool any_in_list = false;
    for (const auto& v : values) {
      for (const auto& item : pf.string_items) {
        if (v == item) { any_in_list = true; break; }
      }
      if (any_in_list) break;
    }
    if (pf.fi->filter_operator == FilterOperator::Not) return !any_in_list;
    return any_in_list;
  }
  bool eq_bounds = (pf.enc_lower == pf.enc_upper);
  if (eq_bounds && pf.fi->include_lower && pf.fi->include_upper) {
    bool any_eq = false;
    for (const auto& v : values) {
      if (v == pf.enc_lower) { any_eq = true; break; }
    }
    if (pf.fi->filter_operator == FilterOperator::Not) return !any_eq;
    return any_eq;
  }
  for (const auto& v : values) {
    if (!pf.enc_lower.empty()) {
      if (pf.fi->include_lower) { if (v < pf.enc_lower) continue; }
      else { if (v <= pf.enc_lower) continue; }
    }
    if (!pf.enc_upper.empty()) {
      if (pf.fi->include_upper) { if (v > pf.enc_upper) continue; }
      else { if (v >= pf.enc_upper) continue; }
    }
    return true;
  }
  return false;
}

}  // namespace

ScalarIndexResult CompositeIndex::Scan(const std::vector<FilterInfo>& filters,
                                       FilterOperator inner_op,
                                       int offset, int limit) {
  ScalarIndexResult result;

  auto prepared = PrepareFilters(filters, field_ids_, data_types_);
  if (prepared.empty()) return result;

  const bool has_stringarray = HasStringArray(data_types_);

  size_t equal_prefix_pos = 0;
  std::string equal_prefix = BuildEqualPrefix(
      header_key_, field_ids_, prepared, inner_op, &equal_prefix_pos);

  auto segments = BuildScanSegments(
      equal_prefix, equal_prefix_pos, field_ids_, prepared, inner_op);

  // Field positions referenced by prepared filters; only these are aggregated
  // in the STRINGARRAY path.
  std::unordered_set<int> needed_positions;
  for (const auto& pf : prepared) needed_positions.insert(pf.idx_pos);

  auto& db = storage_mgr_->GetDB();
  rocksdb::ColumnFamilyHandle* cf_handler = CfHandler();
  rocksdb::ReadOptions read_options;
  std::unique_ptr<rocksdb::Iterator> iter(db->NewIterator(read_options, cf_handler));

  const size_t header_len = header_key_.size();
  size_t matched = 0;
  int skipped = 0;

  // STRINGARRAY aggregation buffer: docid -> per-position observed values.
  std::unordered_map<int64_t, std::vector<std::vector<std::string>>> agg;
  std::unordered_set<int64_t> emitted;

  auto try_emit = [&](int64_t docid) -> bool {
    if (emitted.count(docid)) return false;
    emitted.insert(docid);
    if (skipped < offset) { ++skipped; return false; }
    result.Add(docid);
    ++matched;
    return true;
  };

  auto flush_docid = [&](int64_t docid) -> bool {
    auto it = agg.find(docid);
    if (it == agg.end()) return false;
    const auto& per_pos = it->second;
    bool hit = (inner_op == FilterOperator::And);
    for (const auto& pf : prepared) {
      bool m = EvalFilterForDocid(pf, per_pos[pf.idx_pos]);
      if (inner_op == FilterOperator::And) {
        if (!m) { hit = false; break; }
      } else {
        if (m) { hit = true; break; }
        hit = false;
      }
    }
    agg.erase(it);
    if (!hit) return false;
    return try_emit(docid);
  };

  bool reached_limit = false;
  std::vector<std::string> raw_vals;
  for (const auto& seg : segments) {
    if (reached_limit) break;
    for (iter->Seek(seg.seek_start);
         iter->Valid() && iter->key().starts_with(seg.prefix_match);
         iter->Next()) {
      std::string key = iter->key().ToString();
      if (!DecodeKeyFields(key, header_len, data_types_, &raw_vals)) continue;

      // Range upper-bound short-circuit: keys within the same prefix_match
      // are sorted by this field's sortable encoding, so once we move past
      // enc_upper we can stop the segment entirely.
      if (seg.upper_pos >= 0 && !seg.enc_upper.empty()) {
        const std::string& v = raw_vals[seg.upper_pos];
        bool past = seg.include_upper ? (v > seg.enc_upper)
                                       : (v >= seg.enc_upper);
        if (past) break;
      }

      int64_t docid = DecodeDocidFromBinaryKey(key);
      if (docid < 0) continue;

      if (!has_stringarray) {
        // Fast path: one entry per docid, evaluate inline against the key.
        bool hit = (inner_op == FilterOperator::And);
        for (const auto& pf : prepared) {
          bool m = MatchOne(raw_vals[pf.idx_pos], pf.dtype, *pf.fi,
                            pf.enc_lower, pf.enc_upper,
                            (pf.dtype == DataType::STRING ||
                             pf.dtype == DataType::STRINGARRAY)
                                ? &pf.string_items : nullptr);
          if (inner_op == FilterOperator::And) {
            if (!m) { hit = false; break; }
          } else {
            if (m) { hit = true; break; }
            hit = false;
          }
        }
        if (!hit) continue;
        if (!try_emit(docid)) continue;
        if (limit > 0 && static_cast<int>(matched) >= limit) {
          reached_limit = true;
          break;
        }
        continue;
      }

      // STRINGARRAY path: accumulate; defer evaluation to the post-loop flush
      // because a later entry for the same docid can contribute a value that
      // flips a NOT IN result.
      auto& per_pos = agg[docid];
      if (per_pos.empty()) per_pos.assign(field_ids_.size(), {});
      for (int pos : needed_positions) {
        auto& bucket = per_pos[pos];
        const std::string& v = raw_vals[pos];
        bool dup = false;
        for (const auto& existing : bucket) {
          if (existing == v) { dup = true; break; }
        }
        if (!dup) bucket.push_back(v);
      }
    }
  }

  if (has_stringarray) {
    std::vector<int64_t> docids;
    docids.reserve(agg.size());
    for (auto& kv : agg) docids.push_back(kv.first);
    std::sort(docids.begin(), docids.end());
    for (int64_t docid : docids) {
      flush_docid(docid);
      if (limit > 0 && static_cast<int>(matched) >= limit) break;
    }
  }

  return result;
}

} // namespace vearch
