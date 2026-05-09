/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "scalar_index_manager.h"

#include <string.h>

#include <algorithm>
#include <cstdint>
#include <sstream>
#include <unordered_set>

#include "util/log.h"
#include "util/utils.h"
#include "scalar_index_utils.h"

namespace vearch {

ScalarIndexManager::ScalarIndexManager(Table *table,
                                             StorageManager *storage_mgr)
    : table_(table), storage_mgr_(storage_mgr), cf_id_(0) {}

ScalarIndexManager::~ScalarIndexManager() = default;

int ScalarIndexManager::Init(std::string space_name, std::vector<struct IndexInfo> indexes) {
  if (table_ == nullptr || storage_mgr_ == nullptr) {
    LOG(ERROR) << "init range index failed: table or storage manager is null";
    return -1;
  }
  cf_id_ = storage_mgr_->CreateColumnFamily("scalar");
  space_name_ = space_name;
  int ret = AddIndexes(indexes);
  if (ret < 0) {
    LOG(ERROR) << "add indexes error, ret=" << ret;
    return ret;
  }
  return 0;
}

int ScalarIndexManager::AddIndexes(std::vector<struct IndexInfo> &indexes) {
  int retvals = 0;
  std::map<std::string, enum DataType> attr_type;
  retvals = table_->GetAttrType(attr_type);

  std::map<std::string, bool> attr_index;
  retvals = table_->GetAttrIsIndex(attr_index);

  std::map<std::string, ScalarIndexType> attr_index_type;
  table_->GetAttrIndexType(attr_index_type);

  if (!indexes.empty()) {
    for (const auto &idx : indexes) {
      if (!IsScalarIndexType(idx.type)) {
        continue;
      }
      if (idx.type == COMPOSITE_INDEX_TYPE_STRING && idx.field_names.size() >= 2) {
        std::vector<int> composite_field_ids;
        std::vector<enum DataType> composite_field_types;
        for (const auto &fname : idx.field_names) {
          int fid = table_->GetAttrIdx(fname);
          if (fid < 0) {
            LOG(ERROR) << space_name_ << " composite index field [" << fname << "] not found in table";
            continue;
          }
          composite_field_ids.push_back(fid);
          composite_field_types.push_back(attr_type[fname]);
        }
        if (composite_field_ids.size() >= 2) {
          LOG(INFO) << space_name_ << " add composite index [" << idx.name
                    << "] for " << composite_field_ids.size() << " fields";
          AddCompositeIndex(composite_field_ids, composite_field_types);
        } else {
          LOG(ERROR) << space_name_ << " composite index requires at least 2 fields";
          continue;
        }
      } else {
        int field_idx = table_->GetAttrIdx(idx.field_name);
        if (field_idx < 0) {
          LOG(ERROR) << space_name_ << " single field index [" << idx.field_name << "] not found in table";
          continue;
        }
        ScalarIndexType st = ScalarIndexType::Scalar;
        if (idx.type == BITMAP_INDEX_TYPE_STRING) {
          st = ScalarIndexType::Bitmap;
        } else if (idx.type == INVERTED_INDEX_TYPE_STRING) {
          st = ScalarIndexType::Inverted;
        }
        LOG(INFO) << space_name_ << " add scalar index for field [" << idx.field_name << "], index_type=" << static_cast<int>(st);
        AddIndex(field_idx, attr_type[idx.field_name], idx.field_name, st);
      }
    }
  } else {
    std::map<int, std::string> field_map_by_id = table_->FieldMapById();
    for (const auto &it : field_map_by_id) {
      const std::string &field_name = it.second;
      const auto &attr_index_it = attr_index.find(field_name);
      if (attr_index_it == attr_index.end()) {
        LOG(ERROR) << space_name_ << " cannot find field [" << field_name << "]";
        continue;
      }
      if (!attr_index_it->second) {
        continue;
      }

      int field_idx = table_->GetAttrIdx(field_name);
      ScalarIndexType index_type = ScalarIndexType::Null;
      const auto &ait_it = attr_index_type.find(field_name);
      if (ait_it != attr_index_type.end()) {
        index_type = ait_it->second;
      }
      if (index_type == ScalarIndexType::Null) {
        continue;
      }

      LOG(INFO) << space_name_ << " add scalar index for field [" << field_name
                << "], index_type=" << ScalarIndexTypeToString(index_type);
      AddIndex(field_idx, attr_type[field_name], field_name, index_type);
    }
  }

  return retvals;
}


int ScalarIndexManager::AddDoc(int64_t docid) {
  for (const auto &it : field_indexes_) {
    it.second->AddDoc(docid);
  }
  for (const auto &it : composite_indexes_) {
    it.second->AddDoc(docid);
  }
  return 0;
}


int ScalarIndexManager::AddDoc(int64_t docid, int field) {
  auto it = field_indexes_.find(field);
  if (it != field_indexes_.end()) {
    it->second->AddDoc(docid);
  }
  UpdateCompositeIndexes(docid, field, true);
  return 0;
}

int ScalarIndexManager::DeleteDoc(int64_t docid) {
  for (const auto &it : field_indexes_) {
    it.second->DeleteDoc(docid);
  }
  for (const auto &it : composite_indexes_) {
    it.second->DeleteDoc(docid);
  }
  return 0;
}

int ScalarIndexManager::DeleteDoc(int64_t docid, int field) {
  auto it = field_indexes_.find(field);
  if (it != field_indexes_.end()) {
    it->second->DeleteDoc(docid);
  }
  UpdateCompositeIndexes(docid, field, false);
  return 0;
}

int ScalarIndexManager::AddIndex(int field, DataType data_type,
                                   const std::string &field_name,
                                   ScalarIndexType index_type) {
  field_index_types_[field] = index_type;

  if (index_type == ScalarIndexType::Bitmap) {
    field_indexes_[field] = std::make_shared<BitmapIndex>(table_, storage_mgr_, cf_id_, data_type, field);
    LOG(INFO) << "Added bitmap index for field [" << field_name << "]";
  } else if (index_type == ScalarIndexType::Inverted || index_type == ScalarIndexType::Scalar || index_type == ScalarIndexType::Index) {
    // Index and Scalar both create InvertedIndex for backward compatibility
    field_indexes_[field] = std::make_shared<InvertedIndex>(table_, storage_mgr_, cf_id_, data_type, field);
    if (index_type == ScalarIndexType::Scalar) {
      LOG(INFO) << "Added inverted index (SCALAR type) for field [" << field_name << "]";
    } else if (index_type == ScalarIndexType::Index) {
      LOG(INFO) << "Added inverted index (INDEX type) for field [" << field_name << "]";
    } else {
      LOG(INFO) << "Added inverted index for field [" << field_name << "]";
    }
  } else {
    LOG(ERROR) << "Invalid index type: " << static_cast<int>(index_type);
    return -1;
  }
  return 0;
}

int ScalarIndexManager::RemoveIndex(int field) {
  field_indexes_.erase(field);
  field_index_types_.erase(field);
  return 0;
}

int ScalarIndexManager::AddCompositeIndex(
    const std::vector<int>& field_ids,
    const std::vector<enum DataType>& data_types) {
  if (field_ids.empty() || field_ids.size() != data_types.size()) {
    LOG(ERROR) << "Invalid composite index: field count mismatch";
    return -1;
  }
  if (field_ids.size() < 2) {
    LOG(ERROR) << "Composite index requires at least 2 fields";
    return -1;
  }

  auto composite = std::make_shared<CompositeIndex>(
      table_, storage_mgr_, cf_id_, data_types, field_ids);

  composite_indexes_[composite->GetHeaderKey()] = composite;
  std::string field_ids_str;
  for (size_t i = 0; i < field_ids.size(); ++i) {
    if (i > 0) field_ids_str += ", ";
    field_ids_str += std::to_string(field_ids[i]);
  }
  LOG(INFO) << "Added composite index for fields [" << field_ids_str
            << "], cf=" << cf_id_ << ")";
  return 0;
}

ScalarIndex* ScalarIndexManager::GetCompositeIndex(
    const std::string& header_key) {
  auto it = composite_indexes_.find(header_key);
  if (it != composite_indexes_.end()) {
    return it->second.get();
  }
  return nullptr;
}

int ScalarIndexManager::UpdateCompositeIndexes(int64_t docid, int field_id,
                                                  bool is_add) {
  for (const auto& kv : composite_indexes_) {
    const auto& fids = kv.second->GetFieldIds();
    for (int fid : fids) {
      if (fid == field_id) {
        if (is_add) {
          kv.second->AddDoc(docid);
        } else {
          kv.second->DeleteDoc(docid);
        }
        break;
      }
    }
  }
  return 0;
}

int ScalarIndexManager::RebuildBitmapIndex(int field_id) {
  auto it = field_indexes_.find(field_id);
  if (it == field_indexes_.end()) {
    LOG(ERROR) << "Field " << field_id << " does not have index";
    return -1;
  }

  auto* bitmap_idx = dynamic_cast<BitmapIndex*>(it->second.get());
  if (bitmap_idx == nullptr) {
    LOG(ERROR) << "Field " << field_id << " is not a bitmap index";
    return -1;
  }

  LOG(INFO) << "Rebuilding bitmap index for field " << field_id
            << " from storage";

  int ret = bitmap_idx->Init();
  if (ret != 0) {
    LOG(ERROR) << "BitmapIndex::Load failed for field " << field_id
               << ", ret=" << ret;
    return ret;
  }

  LOG(INFO) << "Bitmap index for field " << field_id << " rebuilt successfully";
  return 0;
}

int ScalarIndexManager::RebuildAllBitmapIndexes() {
  for (const auto& kv : field_indexes_) {
    auto* bitmap_idx = dynamic_cast<BitmapIndex*>(kv.second.get());
    if (bitmap_idx != nullptr) {
      int ret = RebuildBitmapIndex(kv.first);
      if (ret != 0) {
        LOG(ERROR) << "Rebuild field " << kv.first << " error, ret=" << ret;
        return ret;
      }
    }
  }
  LOG(INFO) << "Rebuilt all scalar bitmap indexes";
  return 0;
}

int ScalarIndexManager::Filter(ScalarIndex* scalar_idx, const FilterInfo &filter, ScalarIndexResult &result, int offset, int limit) {
  enum DataType data_type;
  int ret = table_->GetFieldTypeById(filter.field, data_type);
  if (ret != 0) {
    LOG(ERROR) << "Failed to get field type, field=" << filter.field << ", ret=" << ret;
    return -1;
  }
  if (scalar_idx->IsNumeric()) {
    if (filter.lower_value.empty() && filter.upper_value.empty()) {
      return 0;
    }
    if (filter.lower_value == filter.upper_value) {
      if (filter.is_union == FilterOperator::Not) {
        result = scalar_idx->NotEqual(filter.lower_value, offset, limit);
      } else {
        result = scalar_idx->Equal(filter.lower_value, offset, limit);
      }
    } else if (filter.lower_value.empty()) {
      if (filter.include_upper) {
        result = scalar_idx->LessEqual(filter.upper_value, offset, limit);
      } else {
        result = scalar_idx->LessThan(filter.upper_value, offset, limit);
      }
    } else if (filter.upper_value.empty()) {
      if (filter.include_lower) {
        result = scalar_idx->GreaterEqual(filter.lower_value, offset, limit);
      } else {
        result = scalar_idx->GreaterThan(filter.lower_value, offset, limit);
      }
    } else {
      result = scalar_idx->Range(filter.lower_value,
                                    filter.include_lower,
                                    filter.upper_value,
                                    filter.include_upper,
                                    offset,
                                    limit);
    }
  } else {
    // now for string or stringArray, only use lower_value
    if (filter.lower_value.empty()) {
      return 0;
    }
    std::vector<std::string> items;
    items = utils::split(filter.lower_value, kStringArrayValueDelimiter);
    if (filter.is_union == FilterOperator::Not) {
      result = scalar_idx->NotIn(items, offset, limit);
    } else {
      result = scalar_idx->In(items, offset, limit);
    }
  }
  return 0;
}

// Check if a field belongs to any composite index
static bool FieldInCompositeIndex(int field_id, const std::map<std::string, std::shared_ptr<CompositeIndex>>& composite_indexes) {
  for (const auto& kv : composite_indexes) {
    if (kv.second->IsIndexField(field_id)) {
      return true;
    }
  }
  return false;
}

// get composite index
static ScalarIndex* GetCompositeIndexByFieldId(int field_id, const std::map<std::string, std::shared_ptr<CompositeIndex>>& composite_indexes) {
  for (const auto& kv : composite_indexes) {
    if (kv.second->IsIndexField(field_id) && kv.second->GetFieldId() == field_id) {
      return kv.second.get();
    }
  }
  return nullptr;
}

// Determine the filter mode for composite index matching.
// Supports:
//   - Equal (=): lower_value == upper_value, both inclusive
//   - In (IN): for STRING/STRINGARRAY fields, is_union==Or with lower_value containing values
//   - Range (>=, <=, >, <): for numeric fields (INT, LONG, FLOAT, DOUBLE, DATE)
static bool GetFilterMode(const FilterInfo& filter, Table* table, CompositeFilterMode& out_mode) {
  DataType dtype;
  int ret = table->GetFieldTypeById(filter.field, dtype);
  if (ret != 0) {
    LOG(ERROR) << "Failed to get field type, field=" << filter.field << ", ret=" << ret;
    return false;
  }

  // STRING/STRINGARRAY: IN or NotIn query mode
  if (dtype == DataType::STRING || dtype == DataType::STRINGARRAY) {
    // IN query: is_union == Or, lower_value contains the values
    if (filter.is_union == FilterOperator::Or && !filter.lower_value.empty()) {
      out_mode = CompositeFilterMode::In;
      return true;
    }
    // NotIn query: is_union == Not, lower_value contains the excluded values
    if (filter.is_union == FilterOperator::Not && !filter.lower_value.empty()) {
      out_mode = CompositeFilterMode::NotIn;
      return true;
    }
    // Not supported for STRING
    LOG(WARNING) << "GetFilterMode: unsupported STRING filter mode, is_union="
                 << static_cast<int>(filter.is_union)
                 << ", lower_value.empty=" << filter.lower_value.empty();
    return false;
  }

  // Numeric fields
  // NotEqual: single-value Not query (lower_value == upper_value, both inclusive, is_union == Not)
  if (filter.is_union == FilterOperator::Not &&
      filter.lower_value == filter.upper_value &&
      filter.include_lower && filter.include_upper) {
    out_mode = CompositeFilterMode::NotEqual;
    return true;
  }
  bool is_range = (filter.lower_value != filter.upper_value) ||
                  !filter.include_lower || !filter.include_upper ||
                  filter.is_union == FilterOperator::Not;
  if (is_range) {
    out_mode = CompositeFilterMode::Range;
    return true;
  }
  // Equal for numeric: bounds equal and both inclusive
  out_mode = CompositeFilterMode::Equal;
  return true;
}

int ScalarIndexManager::CompositeFilter(CompositeIndex* composite_idx, const std::vector<FilterInfo>& filters,
  CompositeStrategy strategy, ScalarIndexResult& result) {
  switch (strategy) {
    case CompositeStrategy::EQUAL:
      ExecuteEqualCase(composite_idx, filters, result);
      break;
    case CompositeStrategy::RANGE:
      ExecuteRangeCase(composite_idx, filters, result);
      break;
    case CompositeStrategy::IN:
      ExecuteInCase(composite_idx, filters, result);
      break;
    case CompositeStrategy::NOT_IN:
      ExecuteNotInCase(composite_idx, filters[0], result);
      break;
    case CompositeStrategy::NOT_EQUAL:
      ExecuteNotEqualCase(composite_idx, filters[0], result);
      break;
    default:
      LOG(WARNING) << "Unknown composite strategy, skipping";
  }
  return result.Cardinality();
}

bool CanUseCompositeFilter(CompositeIndex* composite, Table* table,
  const std::vector<FilterInfo>& filters, CompositeStrategy& strategy) {
  std::vector<int> field_ids;
  std::vector<CompositeFilterMode> modes;
  for (const auto& filter : filters) {
    field_ids.push_back(filter.field);
    CompositeFilterMode mode;
    if (!GetFilterMode(filter, table, mode)) {
      LOG(ERROR) << "Failed to get filter mode, field=" << filter.field;
      return false;
    }
    modes.push_back(mode);
  }
  return composite->CanUseFilterMode(field_ids, modes, strategy);
}

int ScalarIndexManager::OrganizeFiltersToIndex(
  const std::vector<FilterInfo>& filters,
  std::vector<FilterIndexPair>& filter_index_pairs,
  FilterOperator query_filter_operator) {
  if (filters.empty()) {
    return 0;
  }
  std::vector<int> field_ids;
  std::set<int> wanted_execute_fields;
  std::map<CompositeIndex*, std::vector<FilterInfo>> composite_filters;
  for (const auto& filter : filters) {
    field_ids.push_back(filter.field);
    wanted_execute_fields.insert(filter.field);
  }
  for (auto composite_index : composite_indexes_) {
    for (size_t i = 0; i < field_ids.size(); i++) {
      int field_id = field_ids[i];
      if (composite_index.second->IsIndexField(field_id)) {
        if (composite_filters.find(composite_index.second.get()) == composite_filters.end()) {
          composite_filters[composite_index.second.get()] = std::vector<FilterInfo>();
          composite_filters[composite_index.second.get()].push_back(filters[i]);
        } else {
          composite_filters[composite_index.second.get()].push_back(filters[i]);
        }
      }
    }
  }
  // Sort filters according to CompositeIndex field order (not user query order).
  for (auto& kv : composite_filters) {
    CompositeIndex* idx = kv.first;
    std::vector<int> idx_field_order = idx->GetFieldIds();
    std::map<int, std::vector<FilterInfo>> field_to_filters;
    for (const auto& f : kv.second) {
      field_to_filters[f.field].push_back(f);
    }
    kv.second.clear();
    for (int fid : idx_field_order) {
      auto it = field_to_filters.find(fid);
      if (it != field_to_filters.end()) {
        for (const auto& f : it->second) {
          kv.second.push_back(f);
        }
      }
    }
  }
  std::map<CompositeIndex*, std::vector<FilterInfo>> usable_composite;
  std::map<CompositeIndex*, CompositeStrategy> strategies;
  std::set<int> composite_covered_fields;
  for (const auto& kv : composite_filters) {
    CompositeStrategy strategy = CompositeStrategy::NONE;
    if (CanUseCompositeFilter(kv.first, table_, kv.second, strategy)) {
      usable_composite[kv.first] = kv.second;
      strategies[kv.first] = strategy;
      for (const auto& f : kv.second) {
        composite_covered_fields.insert(f.field);
      }
    }
  }

  std::vector<std::pair<CompositeIndex*, std::vector<FilterInfo>>> sorted_composite;
  for (const auto& kv : usable_composite) {
    sorted_composite.push_back(kv);
  }
  std::sort(sorted_composite.begin(), sorted_composite.end(),
      [](const auto& a, const auto& b) {
        return a.first->NumFields() > b.first->NumFields();
      });

  std::set<int> covered_fields;
  bool has_composite_filter = false;
  for (const auto& kv : sorted_composite) {
    bool already_covered = true;
    for (int fid : kv.first->GetFieldIds()) {
      if (covered_fields.find(fid) == covered_fields.end()) {
        already_covered = false;
        break;
      }
    }
    if (already_covered) continue;

    for (int fid : kv.first->GetFieldIds()) {
      covered_fields.insert(fid);
      composite_covered_fields.insert(fid);
    }

    FilterIndexPair pair;
    pair.composite_index = kv.first;
    pair.is_composite = true;
    pair.filters = kv.second;
    pair.strategy = strategies.at(kv.first);
    filter_index_pairs.push_back(std::move(pair));
    has_composite_filter = true;
  }
  // Remaining filters not covered by any composite index: fall through to scalar index.
  // Iterate directly over filters rather than field_ids to handle the case where
  // the same field appears multiple times with different filter conditions.
  for (const auto& f : filters) {
    if (composite_covered_fields.find(f.field) != composite_covered_fields.end()) continue;
    if (auto index = GetFieldIndex(f.field)) {
      FilterIndexPair pair;
      pair.index = index;
      pair.filters = {f};
      pair.is_composite = false;
      filter_index_pairs.push_back(std::move(pair));
    }
  }
  if (filter_index_pairs.empty()) {
    return -1;
  }
  if (has_composite_filter && query_filter_operator == FilterOperator::Or) {
    return -1;
  }
  std::set<int> can_execute_fields;
  for (const auto& filter_index_pair : filter_index_pairs) {
    for (const auto& filter : filter_index_pair.filters) {
      can_execute_fields.insert(filter.field);
    }
  }
  if (can_execute_fields.size() != wanted_execute_fields.size()) {
    return -1;
  }
  for (auto field_id : wanted_execute_fields) {
    if (can_execute_fields.find(field_id) == can_execute_fields.end()) {
      return -1;
    }
  }
  return 0;
}

int64_t ScalarIndexManager::Search(
    FilterOperator query_filter_operator,
    std::vector<FilterInfo> &origin_filters,
    ScalarIndexResults *out) {
  out->Clear();

  if (origin_filters.empty()) {
    return 0;
  }

  for (const auto &filter : origin_filters) {
    DataType dtype;
    int ret = table_->GetFieldTypeById(filter.field, dtype);
    if (ret != 0) {
      LOG(ERROR) << "Failed to get field type, field=" << filter.field;
      return -1;
    }
    auto index = GetFieldIndex(filter.field);
    if (index == nullptr &&
      !FieldInCompositeIndex(filter.field, composite_indexes_)) {
      return 0;
    }
  }
  std::vector<FilterIndexPair> filter_index_pairs;
  int ret = OrganizeFiltersToIndex(origin_filters, filter_index_pairs, query_filter_operator);
  if (ret != 0) {
    LOG(ERROR) << "Failed to organize filters to index, ret=" << ret;
    return 0;
  }
  for (const auto& filter_index_pair : filter_index_pairs) {
    for (const auto& filter : filter_index_pair.filters) {
      DataType dtype;
      int ret = table_->GetFieldTypeById(filter.field, dtype);
      if (ret != 0) {
        LOG(ERROR) << "Failed to get field type, field=" << filter.field;
        return -1;
      }
    }
  }

  ScalarIndexResult result;
  bool first_result = true;
  for (const auto& filter_index_pair : filter_index_pairs) {
    ScalarIndexResult result_tmp;
    if (filter_index_pair.is_composite) {
      CompositeFilter(filter_index_pair.composite_index, filter_index_pair.filters, filter_index_pair.strategy, result_tmp);
    } else {
      Filter(filter_index_pair.index, filter_index_pair.filters[0], result_tmp);
    }
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
  }
  int64_t card = result.Cardinality();
  out->Add(std::move(result));
  return card;
}

int64_t ScalarIndexManager::Query(
    FilterOperator query_filter_operator,
    std::vector<FilterInfo> &origin_filters,
    std::vector<uint64_t> &docids, size_t topn, size_t offset) {
  docids.clear();
  docids.reserve(topn);

  if (origin_filters.empty()) {
    return 0;
  }

  for (const auto &filter : origin_filters) {
    DataType dtype;
    int ret = table_->GetFieldTypeById(filter.field, dtype);
    if (ret != 0) {
      LOG(ERROR) << "Failed to get field type, field=" << filter.field;
      return -1;
    }
    auto index = GetFieldIndex(filter.field);
    if (index == nullptr &&
        !FieldInCompositeIndex(filter.field, composite_indexes_)) {
      return 0;
    }
  }

  // Single field filter can return early if get enough result
  if (origin_filters.size() == 1) {
    const auto& filter = origin_filters[0];
    if (filter.lower_value.empty() && filter.upper_value.empty()) {
      return 0;
    }
    ScalarIndexResult result;
    auto index = GetFieldIndex(filter.field);
    if (index == nullptr) {
      index = GetCompositeIndexByFieldId(filter.field, composite_indexes_);
      if (index == nullptr) {
        return 0;
      }
    }
    Filter(index, filter, result, offset, topn);
    docids = result.GetDocIDs(topn);
    return static_cast<int64_t>(docids.size());
  }

  ScalarIndexResults scalar_index_results;
  int64_t retval = Search(query_filter_operator, origin_filters, &scalar_index_results);
  if (retval <= 0) {
    return retval;
  }

  docids = scalar_index_results.GetDocIDs(topn + offset);
  if (offset >= docids.size()) {
    docids.clear();
  } else {
    docids.erase(docids.begin(), docids.begin() + offset);
  }
  return static_cast<int64_t>(docids.size());
}

// ============================================================================
// Execute strategy implementations
// ============================================================================

void ScalarIndexManager::ExecuteEqualCase(
    CompositeIndex* composite_idx,
    const std::vector<FilterInfo>& match_filters,
    ScalarIndexResult& result) {
  std::vector<std::string> prefix_values;
  for (size_t j = 0; j < match_filters.size(); j++) {
    prefix_values.push_back(match_filters[j].lower_value);
  }
  result = composite_idx->Equal(prefix_values, 0, 0);
}

void ScalarIndexManager::ExecuteRangeCase(
    CompositeIndex* composite_idx,
    const std::vector<FilterInfo>& match_filters,
    ScalarIndexResult& result) {
  std::vector<std::string> prefix_values;
  std::string lower_value, upper_value;
  bool include_lower = true, include_upper = true;
  for (size_t j = 0; j < match_filters.size() - 1; j++) {
    prefix_values.push_back(match_filters[j].lower_value);
  }

  lower_value = match_filters[match_filters.size() - 1].lower_value;
  upper_value = match_filters[match_filters.size() - 1].upper_value;
  include_lower = match_filters[match_filters.size() - 1].include_lower;
  include_upper = match_filters[match_filters.size() - 1].include_upper;

  result = composite_idx->Range(prefix_values,
                               lower_value, upper_value,
                               include_lower, include_upper, 0, 0);
}

void ScalarIndexManager::ExecuteNotInCase(
    CompositeIndex* composite_idx,
    const FilterInfo& filter,
    ScalarIndexResult& result) {
  std::vector<std::string> items =
      utils::split(filter.lower_value, kStringArrayValueDelimiter);
  result = composite_idx->NotIn(items, 0, 0);
}

void ScalarIndexManager::ExecuteNotEqualCase(
    CompositeIndex* composite_idx,
    const FilterInfo& filter,
    ScalarIndexResult& result) {
  result = composite_idx->NotEqual(filter.lower_value, 0, 0);
}

void ScalarIndexManager::ExecuteInCase(
    CompositeIndex* composite_idx,
    const std::vector<FilterInfo>& match_filters,
    ScalarIndexResult& result) {
  int range_idx = -1;
  std::vector<std::vector<std::string>> field_values(match_filters.size());
  for (size_t j = 0; j < match_filters.size(); j++) {
    CompositeFilterMode mode;
    if (!GetFilterMode(match_filters[j], table_, mode)) {
      LOG(ERROR) << "Failed to get filter mode, field=" << match_filters[j].field;
      return;
    }
    if (mode == CompositeFilterMode::In) {
      field_values[j] = utils::split(match_filters[j].lower_value, kStringArrayValueDelimiter);
    } else if (mode == CompositeFilterMode::Equal) {
      field_values[j] = {match_filters[j].lower_value};
    }
  }

  // Step 2: find the Range field (if any) within [0..match_filters.size()-1]
  std::string range_lower, range_upper;
  bool range_inc_lower = true, range_inc_upper = true;
  CompositeFilterMode mode;
  if (!GetFilterMode(match_filters[match_filters.size() - 1], table_, mode)) {
    LOG(ERROR) << "Failed to get filter mode, field=" << match_filters[match_filters.size() - 1].field;
    return;
  }
  if (mode == CompositeFilterMode::Range) {
    range_idx = match_filters.size() - 1;
    range_lower = match_filters[range_idx].lower_value;
    range_upper = match_filters[range_idx].upper_value;
    range_inc_lower = match_filters[range_idx].include_lower;
    range_inc_upper = match_filters[range_idx].include_upper;
  }

  // Step 3: collect non-Range field indices that have filters (IN or Equal).
  // Fields without filters are wildcards — NOT part of the Cartesian product.
  // They are handled by Range() or Equal()
  std::vector<int> non_range_idx;
  int match_count = match_filters.size();
  for (int j = 0; j < match_count; j++) {
    if (j == range_idx) continue;
    if (!field_values[j].empty()) non_range_idx.push_back(j);
  }
  std::vector<size_t> counters(non_range_idx.size(), 0);

  ScalarIndexResult combined;
  while (true) {
    std::vector<std::string> non_range_values;
    non_range_values.reserve(non_range_idx.size());
    for (size_t t = 0; t < non_range_idx.size(); t++) {
      int fi = non_range_idx[t];
      if (field_values[fi].empty()) {
        // No filter for this field: skip it (not part of the Cartesian product)
        continue;
      }
      non_range_values.push_back(field_values[fi][counters[t]]);
    }

    if (range_idx >= 0) {
      // Cartesian + Range: build prefix/suffix for Range()
      std::vector<std::string> range_prefix;
      std::vector<std::string> range_suffix;
      for (size_t t = 0; t < non_range_idx.size(); t++) {
        int fi = non_range_idx[t];
        if (field_values[fi].empty()) continue;
        if (fi < range_idx) range_prefix.push_back(non_range_values[t]);
        else range_suffix.push_back(non_range_values[t]);
      }
      if (!range_suffix.empty()) {
        return;
      }
      ScalarIndexResult r = composite_idx->Range(range_prefix,
                                            range_lower, range_upper,
                                            range_inc_lower, range_inc_upper, 0, 0);
      combined.Union(r);
    } else {
      ScalarIndexResult r = composite_idx->Equal(non_range_values, 0, 0);
      combined.Union(r);
    }

    // Advance lexicographic counter
    size_t carry = 1;
    for (int t = static_cast<int>(non_range_idx.size()) - 1; t >= 0; t--) {
      int fi = non_range_idx[t];
      if (field_values[fi].empty()) { counters[t] = 0; continue; }
      counters[t]++;
      if (counters[t] < field_values[fi].size()) { carry = 0; break; }
      counters[t] = 0;
    }
    if (carry == 1) break;
  }

  result = std::move(combined);
}

}  // namespace vearch
