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
      if (filter.filter_operator == FilterOperator::Not) {
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
    if (filter.filter_operator == FilterOperator::Not) {
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
//   - In (IN): for STRING/STRINGARRAY fields, filter_operator==Or with lower_value containing values
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
    // IN query: filter_operator == Or, lower_value contains the values
    if (filter.filter_operator == FilterOperator::Or && !filter.lower_value.empty()) {
      out_mode = CompositeFilterMode::In;
      return true;
    }
    // NotIn query: filter_operator == Not, lower_value contains the excluded values
    if (filter.filter_operator == FilterOperator::Not && !filter.lower_value.empty()) {
      out_mode = CompositeFilterMode::NotIn;
      return true;
    }
    // Not supported for STRING
    LOG(WARNING) << "GetFilterMode: unsupported STRING filter mode, filter_operator="
                 << static_cast<int>(filter.filter_operator)
                 << ", lower_value.empty=" << filter.lower_value.empty();
    return false;
  }

  // Numeric fields
  // NotEqual: single-value Not query (lower_value == upper_value, both inclusive, filter_operator == Not)
  if (filter.filter_operator == FilterOperator::Not &&
      filter.lower_value == filter.upper_value &&
      filter.include_lower && filter.include_upper) {
    out_mode = CompositeFilterMode::NotEqual;
    return true;
  }
  bool is_range = (filter.lower_value != filter.upper_value) ||
                  !filter.include_lower || !filter.include_upper ||
                  filter.filter_operator == FilterOperator::Not;
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
    case CompositeStrategy::SCAN:
      // Default to AND. OR-bucket SCANs go through ExecuteScanCase
      // directly from the Search loop.
      ExecuteScanCase(composite_idx, filters, FilterOperator::And, result);
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
  std::set<int> seen;
  for (const auto& filter : filters) {
    if (!seen.insert(filter.field).second) {
      return false;
    }
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

namespace {

// Plan chosen for a single composite index in the AND branch.
struct ChosenPlan {
  std::vector<FilterInfo> filters;
  CompositeStrategy strategy;
};

// Build per-composite filter views for the AND branch:
//   prefix_out: continuous prefix from the composite's fid order, used by the
//     legacy EQUAL / RANGE / IN strategies (requires CanUseCompositeFilter).
//   all_out:   every query filter whose field belongs to this composite,
//     regardless of order or duplicates; used as the SCAN fallback input.
static void BuildCompositeViews(
    const std::vector<FilterInfo>& filters,
    const std::map<std::string, std::shared_ptr<CompositeIndex>>& composite_indexes,
    std::map<CompositeIndex*, std::vector<FilterInfo>>& prefix_out,
    std::map<CompositeIndex*, std::vector<FilterInfo>>& all_out) {
  for (auto& composite_index : composite_indexes) {
    CompositeIndex* idx = composite_index.second.get();
    const std::vector<int>& idx_fields = idx->GetFieldIds();

    std::vector<FilterInfo> prefix_filters;
    for (size_t i = 0; i < idx_fields.size(); ++i) {
      int cf = idx_fields[i];
      std::vector<FilterInfo> field_filters;
      for (size_t j = 0; j < filters.size(); ++j) {
        if (filters[j].field == cf) {
          field_filters.push_back(filters[j]);
        }
      }
      if (field_filters.empty()) {
        break;
      }
      prefix_filters.insert(prefix_filters.end(), field_filters.begin(), field_filters.end());
    }
    if (!prefix_filters.empty()) {
      prefix_out[idx] = std::move(prefix_filters);
    }

    std::vector<FilterInfo> all_filters;
    for (const auto& f : filters) {
      if (idx->IsIndexField(f.field)) {
        all_filters.push_back(f);
      }
    }
    if (!all_filters.empty()) {
      all_out[idx] = std::move(all_filters);
    }
  }
}

// For each composite index that owns some query field, pick a plan: prefix
// strategy when it covers every candidate field, else SCAN over all candidate
// filters. Composites that cannot serve either path are skipped.
static void ChoosePerCompositeStrategy(
    const std::map<CompositeIndex*, std::vector<FilterInfo>>& prefix_filters_for_idx,
    const std::map<CompositeIndex*, std::vector<FilterInfo>>& all_filters_for_idx,
    Table* table,
    std::map<CompositeIndex*, ChosenPlan>& chosen) {
  for (const auto& kv : all_filters_for_idx) {
    CompositeIndex* idx = kv.first;
    const std::vector<FilterInfo>& all_f = kv.second;

    std::set<int> all_fields;
    for (const auto& f : all_f) all_fields.insert(f.field);

    auto pf_it = prefix_filters_for_idx.find(idx);
    if (pf_it != prefix_filters_for_idx.end()) {
      CompositeStrategy strategy = CompositeStrategy::NONE;
      std::set<int> prefix_fields;
      for (const auto& f : pf_it->second) prefix_fields.insert(f.field);

      bool prefix_covers_all = (prefix_fields == all_fields);
      if (prefix_covers_all &&
          CanUseCompositeFilter(idx, table, pf_it->second, strategy)) {
        chosen[idx] = {pf_it->second, strategy};
        continue;
      }
    }

    // Fallback to SCAN.
    std::vector<int> qf_ids;
    std::vector<CompositeFilterMode> modes;
    bool ok = true;
    for (const auto& f : all_f) {
      CompositeFilterMode m;
      if (!GetFilterMode(f, table, m)) { ok = false; break; }
      qf_ids.push_back(f.field);
      modes.push_back(m);
    }
    if (ok && idx->CanUseScan(qf_ids, modes)) {
      chosen[idx] = {all_f, CompositeStrategy::SCAN};
    }
  }
}

}  // namespace

// OR branch: prefix strategies can't be combined safely across filter buckets
// when some bucket would Union with a *partial* result. Build per-filter pairs:
// scalar where available, else SCAN-bucket on a composite that owns the field.
// Fail when a field has no index at all.
int ScalarIndexManager::OrganizeFiltersForOr(
    const std::vector<FilterInfo>& filters,
    std::vector<FilterIndexPair>& filter_index_pairs) {
  std::map<CompositeIndex*, std::vector<FilterInfo>> or_scan_buckets;
  for (const auto& f : filters) {
    if (auto sidx = GetFieldIndex(f.field)) {
      FilterIndexPair pair;
      pair.index = sidx;
      pair.filters = {f};
      pair.is_composite = false;
      filter_index_pairs.push_back(std::move(pair));
      continue;
    }
    // No scalar index: find a composite that owns this field.
    CompositeIndex* host = nullptr;
    for (auto& kv : composite_indexes_) {
      if (kv.second->IsIndexField(f.field)) {
        host = kv.second.get();
        break;
      }
    }
    if (host == nullptr) {
      // Field has no index at all -> OR cannot be satisfied via indexes.
      return -1;
    }
    or_scan_buckets[host].push_back(f);
  }
  for (auto& kv : or_scan_buckets) {
    // Verify SCAN is applicable for this bucket.
    std::vector<int> field_ids;
    std::vector<CompositeFilterMode> modes;
    bool ok = true;
    for (const auto& f : kv.second) {
      CompositeFilterMode m;
      if (!GetFilterMode(f, table_, m)) { ok = false; break; }
      field_ids.push_back(f.field);
      modes.push_back(m);
    }
    if (!ok || !kv.first->CanUseScan(field_ids, modes)) {
      return -1;
    }
    FilterIndexPair pair;
    pair.composite_index = kv.first;
    pair.is_composite = true;
    pair.filters = std::move(kv.second);
    pair.strategy = CompositeStrategy::SCAN;
    pair.inner_op = FilterOperator::Or;
    filter_index_pairs.push_back(std::move(pair));
  }
  if (filter_index_pairs.empty()) return -1;
  return 0;
}

int ScalarIndexManager::OrganizeFiltersToIndex(
  const std::vector<FilterInfo>& filters,
  std::vector<FilterIndexPair>& filter_index_pairs,
  FilterOperator query_filter_operator) {
  if (filters.empty()) {
    return 0;
  }
  std::set<int> wanted_execute_fields;
  for (const auto& filter : filters) {
    wanted_execute_fields.insert(filter.field);
  }

  if (query_filter_operator == FilterOperator::Or) {
    return OrganizeFiltersForOr(filters, filter_index_pairs);
  }

  // ----- AND branch -----
  std::map<CompositeIndex*, std::vector<FilterInfo>> prefix_filters_for_idx;
  std::map<CompositeIndex*, std::vector<FilterInfo>> all_filters_for_idx;
  BuildCompositeViews(filters, composite_indexes_,
                      prefix_filters_for_idx, all_filters_for_idx);

  std::map<CompositeIndex*, ChosenPlan> chosen;
  ChoosePerCompositeStrategy(prefix_filters_for_idx, all_filters_for_idx,
                             table_, chosen);

  // Order: prefix strategies first, then SCANs. Within each group prefer
  // composites covering more fields, so we don't double-scan the same field.
  std::vector<std::pair<CompositeIndex*, ChosenPlan>> sorted_composite(
      chosen.begin(), chosen.end());
  std::sort(sorted_composite.begin(), sorted_composite.end(),
      [](const auto& a, const auto& b) {
        bool a_scan = a.second.strategy == CompositeStrategy::SCAN;
        bool b_scan = b.second.strategy == CompositeStrategy::SCAN;
        if (a_scan != b_scan) return !a_scan;  // prefix first
        return a.first->NumFields() > b.first->NumFields();
      });

  std::set<int> covered_fields;
  for (const auto& kv : sorted_composite) {
    bool already_covered = true;
    for (const auto& f : kv.second.filters) {
      if (covered_fields.find(f.field) == covered_fields.end()) {
        already_covered = false;
        break;
      }
    }
    if (already_covered) continue;

    for (const auto& f : kv.second.filters) {
      covered_fields.insert(f.field);
    }

    FilterIndexPair pair;
    pair.composite_index = kv.first;
    pair.is_composite = true;
    pair.filters = kv.second.filters;
    pair.strategy = kv.second.strategy;
    pair.inner_op = FilterOperator::And;
    filter_index_pairs.push_back(std::move(pair));
  }

  // Remaining filters not covered by any composite index: fall through to scalar index.
  for (const auto& f : filters) {
    if (covered_fields.find(f.field) != covered_fields.end()) continue;
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
      if (filter_index_pair.strategy == CompositeStrategy::SCAN) {
        ExecuteScanCase(filter_index_pair.composite_index,
                        filter_index_pair.filters,
                        filter_index_pair.inner_op,
                        result_tmp);
      } else {
        CompositeFilter(filter_index_pair.composite_index, filter_index_pair.filters, filter_index_pair.strategy, result_tmp);
      }
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

  // Single field filter can return early if get enough result.
  // Only takes the fast path when a scalar index exists, or the field is the
  // first column of a composite index (so a single prefix-seek is valid).
  // Otherwise fall through to Search, which can use composite SCAN fallback.
  if (origin_filters.size() == 1) {
    const auto& filter = origin_filters[0];
    if (filter.lower_value.empty() && filter.upper_value.empty()) {
      return 0;
    }
    ScalarIndex* index = GetFieldIndex(filter.field);
    if (index == nullptr) {
      index = GetCompositeIndexByFieldId(filter.field, composite_indexes_);
    }
    if (index != nullptr) {
      ScalarIndexResult result;
      Filter(index, filter, result, offset, topn);
      docids = result.GetDocIDs(topn);
      return static_cast<int64_t>(docids.size());
    }
    // Fall through to Search; it can route the filter through composite SCAN.
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

void ScalarIndexManager::ExecuteScanCase(
    CompositeIndex* composite_idx,
    const std::vector<FilterInfo>& match_filters,
    FilterOperator inner_op,
    ScalarIndexResult& result) {
  result = composite_idx->Scan(match_filters, inner_op, 0, 0);
}

}  // namespace vearch
