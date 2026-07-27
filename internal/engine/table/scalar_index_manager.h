/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>

#include "scalar_index.h"
#include "inverted_index.h"
#include "bitmap_index.h"
#include "composite_index.h"
#include "index/index_state.h"
#include "scalar_index_result.h"
#include "storage/storage_manager.h"
#include "table.h"
#include "util/bitmap_manager.h"

namespace vearch {

enum class ResultStatus : int64_t { ZERO = 0, INTERNAL_ERR = -1, KILLED = -2 };

struct FilterIndexPair {
  ScalarIndex* index;
  CompositeIndex* composite_index;
  std::vector<FilterInfo> filters;
  bool is_composite;
  CompositeStrategy strategy;
  // For SCAN strategy only: how filters inside this bucket combine.
  // AND -> entry must satisfy every filter to be emitted.
  // OR  -> entry emitted if any filter matches.
  // Ignored when strategy != SCAN.
  FilterOperator inner_op = FilterOperator::And;
};

// ============================================================================
// ScalarIndexManager - Manages all scalar indexes for a table
// ============================================================================
class ScalarIndexManager {
 public:
  ScalarIndexManager(Table *table, StorageManager *storage_mgr);
  ~ScalarIndexManager();

  int Init(std::string space_name, std::vector<struct IndexInfo> indexes);

  int AddIndexes(std::vector<struct IndexInfo> &indexes);

  int AddDoc(int64_t docid);

  //update work as add and delete maybe only update one field
  int AddDoc(int64_t docid, int field);

  int DeleteDoc(int64_t docid);

  //update work as add and delete maybe only update one field
  int DeleteDoc(int64_t docid, int field);

  int AddIndex(int field, DataType data_type, const std::string &field_name,
               ScalarIndexType index_type, const std::string &name = "");

  // Insert a fully-built (or empty, to-be-backfilled) index object atomically.
  // Routes by runtime type:
  //   - dynamic_cast<CompositeIndex*> succeeds  → composite_indexes_ +
  //     index_name_to_composite_key_ + composite_index_state_
  //   - otherwise (single-field ScalarIndex)    → field_indexes_ +
  //     index_name_to_field_ + field_index_state_
  // `state` records the initial build state; publish-then-backfill callers pass
  // BUILDING and later flip to READY via SetIndexStateByName.
  int InsertIndex(std::shared_ptr<ScalarIndex> index,
                  const std::string &name = "",
                  IndexState state = IndexState::READY);

  // Flip the build state of an already-published index by its user-defined
  // name. Idempotent no-op if the name is unknown. Takes scalar_indexes_mutex_
  // in unique mode.
  void SetIndexStateByName(const std::string &name, IndexState state);

  // Snapshot of every registered index's build state (name → "BUILDING" /
  // "READY" / "FAILED"), for EngineStatus. Takes a shared lock.
  std::map<std::string, std::string> GetAllIndexStates() const;

  // Persisted BUILDING-marker helpers (crash recovery). marker_key builds the
  // reserved-prefix RocksDB key; the marker lives in the scalar cf and is
  // written before backfill, deleted after READY. EnumerateBuildingMarkers
  // scans the cf and returns the index names still marked BUILDING (i.e. a
  // backfill that never completed before a crash).
  std::string BuildingMarkerKey(const std::string &name) const;
  void WriteBuildingMarker(const std::string &name);
  void DeleteBuildingMarker(const std::string &name);
  std::vector<std::string> EnumerateBuildingMarkers() const;

  // Fetch the published index object registered under `name` (single-field or
  // composite), or nullptr if unknown. Used by crash recovery in Engine::Load
  // to re-backfill an index left in BUILDING. Takes a shared lock.
  std::shared_ptr<ScalarIndex> GetIndexByName(const std::string &name) const;

  // Remove an index by its user-defined name. Routes to either a single-field
  // index or a composite index. Drops persisted RocksDB entries via
  // ScalarIndex::DropAll() before erasing the in-memory object. Idempotent:
  // returns 0 if name not found.
  int RemoveIndex(const std::string &name);

  // Build a just-published (BUILDING) scalar/composite index and flip it READY,
  // entirely under scalar_indexes_mutex_'s unique lock. The lock excludes the
  // applier's AddDoc/DeleteDoc for the duration, so the build sees a stable
  // snapshot and no concurrent value change can leave a stale key: DropAll
  // clears anything already written, then the index is scanned from current
  // values over [0, max_docid) (max_docid read inside the lock is the live
  // frontier since the applier is blocked), then flipped READY in the same
  // critical section. `index` is the object registered under `name`;
  // docids_bitmap skips deleted docids. Note this blocks writes for the length
  // of one full scan — dynamic index add is not lock-free by design.
  void BuildFieldIndexUnderLock(const std::string &name,
                                const std::shared_ptr<ScalarIndex> &index,
                                const std::atomic<int64_t> &max_docid,
                                bitmap::BitmapManager *docids_bitmap);

  int OrganizeFiltersToIndex(const std::vector<FilterInfo>& filters,
    std::vector<FilterIndexPair>& filter_index_pairs, FilterOperator query_filter_operator);

  int Filter(ScalarIndex* scalar_idx, const FilterInfo &filter, ScalarIndexResult &result, int offset = 0, int limit = 0);

  /**
   * Try to use composite index for filtering.
   *
   */
  int CompositeFilter(CompositeIndex* composite_idx, const std::vector<FilterInfo>& filters,
    CompositeStrategy strategy, ScalarIndexResult& result);

  int64_t Search(FilterOperator query_filter_operator,
                 std::vector<FilterInfo> &origin_filters,
                 ScalarIndexResults *out);

  int64_t Query(FilterOperator query_filter_operator,
                std::vector<FilterInfo> &origin_filters,
                std::vector<uint64_t> &docids, size_t topn, size_t offset);

  // Rebuild bitmap index from storage for a specific field
  int RebuildBitmapIndex(int field_id);

  // Rebuild all bitmap fields from storage
  int RebuildAllBitmapIndexes();
  // Get field index by field id
  ScalarIndex* GetFieldIndex(int field);

  /**
   * Add a composite (multi-column) index. Registered by its first field ID.
   * @param field_ids   ordered list of field IDs (must have size >= 2)
   * @param field_types data types corresponding to each field_id
   * @return 0 on success
   */
  int AddCompositeIndex(const std::vector<int>& field_ids,
                        const std::vector<enum DataType>& data_types,
                        const std::string &name = "");

  /**
   * Get composite index by its header key.
   */
  ScalarIndex* GetCompositeIndex(const std::string& key);

  // Returns true if a single-field or composite index has been registered
  // under the given user-defined name.
  bool HasIndexName(const std::string &name) const;

 private:
  // Caller MUST hold scalar_indexes_mutex_ in shared mode. Implementation of Search /
  // Query without locking — used to avoid recursive shared_lock acquisition,
  // which is undefined behavior on std::shared_mutex.
  int64_t SearchLocked(FilterOperator query_filter_operator,
                       std::vector<FilterInfo> &origin_filters,
                       ScalarIndexResults *out);

  int64_t QueryLocked(FilterOperator query_filter_operator,
                      std::vector<FilterInfo> &origin_filters,
                      std::vector<uint64_t> &docids, size_t topn,
                      size_t offset);

  // Caller MUST hold scalar_indexes_mutex_ (shared or unique). Returns true iff
  // the single-field index registered for `field_id` is READY. A missing entry
  // is treated as READY: statically-created indexes never register a state, so
  // absence means "not a dynamically-tracked index" → usable as before.
  bool FieldIndexReadyLocked(int field_id) const;

  // Same contract for a composite index identified by its header_key.
  bool CompositeIndexReadyLocked(const std::string &header_key) const;

  // READY-aware variants of the file-local composite lookups. Caller holds
  // scalar_indexes_mutex_. Only consider composites whose state is READY.
  bool FieldInCompositeIndexReadyLocked(int field_id) const;
  ScalarIndex *GetCompositeIndexByFieldIdReady(int field_id) const;

  // Caller MUST hold scalar_indexes_mutex_ in unique mode.
  int RebuildBitmapIndexLocked(int field_id);

  // Check if a field belongs to any composite index and update them.
  int UpdateCompositeIndexes(int64_t docid, int field_id, bool is_add);

  // OR-branch dispatch for OrganizeFiltersToIndex. Builds per-filter pairs:
  // scalar where available, else SCAN-bucket on a composite that owns the
  // field. Returns -1 when any field has no index that can serve it.
  int OrganizeFiltersForOr(const std::vector<FilterInfo>& filters,
                           std::vector<FilterIndexPair>& filter_index_pairs);

  /**
   * Execute EQUAL strategy: all composite fields have single-value filters (Eq mode).
   * Uses composite->Equal() for a single RocksDB seek.
   */
  void ExecuteEqualCase(CompositeIndex* composite_idx,
                        const std::vector<FilterInfo>& match_filters,
                        ScalarIndexResult& result);

  /**
   * Execute RANGE strategy: full composite match with at least one Range filter.
   * Uses composite->Range() with bounds on all fields; suffix fields use min/max.
   */
  void ExecuteRangeCase(CompositeIndex* composite_idx,
                        const std::vector<FilterInfo>& match_filters,
                        ScalarIndexResult& result);

  /**
   * Execute IN strategy: prefix + suffix Cartesian product of Eq/IN values.
   * Generates all combinations of field values and issues composite->In() or composite->Equal().
   */
  void ExecuteInCase(CompositeIndex* composite_idx,
                     const std::vector<FilterInfo>& match_filters,
                     ScalarIndexResult& result);

  /**
   * Execute NOT_IN strategy: NotIn on first field, other fields should be absent.
   */
  void ExecuteNotInCase(CompositeIndex* composite_idx,
                        const FilterInfo& filter,
                        ScalarIndexResult& result);

  /**
   * Execute NOT_EQUAL strategy: NotEqual on first field, other fields should be absent.
   */
  void ExecuteNotEqualCase(CompositeIndex* composite_idx,
                           const FilterInfo& filter,
                           ScalarIndexResult& result);

  /**
   * Execute SCAN strategy: full-iteration fallback when filters cannot form
   * a valid composite key prefix. Delegates to CompositeIndex::Scan.
   */
  void ExecuteScanCase(CompositeIndex* composite_idx,
                       const std::vector<FilterInfo>& match_filters,
                       FilterOperator inner_op,
                       ScalarIndexResult& result);

 public:
  Table *table_;
  StorageManager *storage_mgr_;
  int cf_id_;
  std::string space_name_;

 private:
  // CONCURRENCY CONTRACT — READ BEFORE CHANGING LOCKS HERE.
  // Document writes (AddDoc/DeleteDoc) come ONLY from the single raft-apply
  // thread (Engine::AddOrUpdate/Update/Delete -> innerApply, serialized). The
  // locking below relies on that: writers take scalar_indexes_mutex_ only in
  // SHARED mode (they never contend with each other), so shared is enough to
  // keep the map stable while a write traverses it. If writes ever become
  // concurrent (parallel apply), shared no longer serializes writer-vs-writer
  // and this whole scheme must be redesigned (writers would need mutual
  // exclusion on the index objects, not just build_write_mutex_ vs build).
  //
  // Serializes bulk writes to index OBJECT contents (the applier's
  // AddDoc/DeleteDoc vs a dynamic BuildFieldIndexUnderLock / RemoveIndex) so a
  // build cannot interleave with a value change and leave a stale key. Held
  // OUTSIDE scalar_indexes_mutex_: the lock order is ALWAYS
  //   build_write_mutex_ -> scalar_indexes_mutex_
  // (never the reverse). Queries take only scalar_indexes_mutex_ (shared) and
  // never build_write_mutex_, so a build — which holds build_write_mutex_ but
  // only a SHARED scalar_indexes_mutex_ during its scan — does not block reads.
  std::mutex build_write_mutex_;

  // Guards the index-collection MAP structure (the maps below) and the build
  // states. shared = traverse/lookup/read-state (reads AND the single-threaded
  // applier writes); unique = add/remove a map entry or flip a build state.
  mutable std::shared_mutex scalar_indexes_mutex_;
  std::map<int, std::shared_ptr<ScalarIndex>> field_indexes_;
  std::map<std::string, std::shared_ptr<CompositeIndex>> composite_indexes_;
  std::map<std::string, int> index_name_to_field_;
  std::map<std::string, std::string> index_name_to_composite_key_;
  // Build state keyed to match the query path's view: single-field indexes by
  // field_id, composites by header_key. A field/composite absent from these
  // maps is treated as READY (see FieldIndexReadyLocked). All guarded by
  // scalar_indexes_mutex_, mutated only alongside the maps above.
  std::map<int, IndexState> field_index_state_;
  std::map<std::string, IndexState> composite_index_state_;
};

}  // namespace vearch
