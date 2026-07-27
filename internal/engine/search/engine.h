/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "c_api/api_data/doc.h"
#include "c_api/api_data/request.h"
#include "c_api/api_data/response.h"
#include "c_api/api_data/table.h"
#include "table/scalar_index_manager.h"
#include "table/table.h"
#include "util/bitmap_manager.h"
#include "vector/vector_manager.h"

namespace vearch {

enum IndexStatus { UNINDEXED = 0, INDEXING, INDEXED };

// Indexing state for thread-safe operations
enum class IndexingState : int {
  IDLE = 0,      // Not indexing
  STARTING = 1,  // Starting indexing process
  RUNNING = 2,   // Actively indexing
  STOPPING = 3   // Stopping indexing process
};

// A queued schema-mutation task processed serially by the maintenance worker.
enum class IndexTaskType { ADD, REMOVE };
struct IndexTask {
  IndexTaskType type;
  std::string index_name;
  std::vector<std::string> field_names;  // ADD only
  std::string index_type;                // ADD only
  std::string index_param;               // ADD only
};

class Engine {
 public:
  static Engine *GetInstance(const std::string &index_root_path,
                             const std::string &space_name = "");

  ~Engine();

  Status Setup();

  Status Search(Request &request, Response &response_results);

  Status Query(QueryRequest &request, Response &response_results);

  Status CreateTable(TableInfo &table);

  int AddOrUpdate(Doc &doc);

  int Update(int doc_id,
             std::unordered_map<std::string, struct Field> &fields_table,
             std::unordered_map<std::string, struct Field> &fields_vec);

  int Delete(std::string &key);

  int GetDoc(const std::string &key, Doc &doc);

  int GetDoc(int docid, Doc &doc, bool next = false);

  Status CheckDoc(std::unordered_map<std::string, struct Field> &fields_table,
                  std::unordered_map<std::string, struct Field> &fields_vec);

  /**
   * blocking to build index
   * @return 0 if exited
   */
  int BuildIndex();

  int RebuildIndex(int drop_before_rebuild, int limit_cpu, int describe);

  std::string EngineStatus();
  std::string GetMemoryInfo();

  IndexStatus GetIndexStatus() { return index_status_; }

  // Wait for index building to complete (with optional timeout)
  bool WaitForIndexingComplete(int timeout_ms = -1);

  int Dump();
  int Load();
  int LoadIdFromTable();
  int LoadFromFaiss();

  Status Backup(int command);

  /**
   * @brief add an index identified by a user-defined name. The kind of index
   *        is determined by inspecting field_names: a single vector field
   *        creates a vector index, a single scalar field creates a scalar
   *        index, and 2+ scalar fields create a composite scalar index.
   *
   * @param index_name  user-defined index name (used as the deletion handle)
   * @param field_names ordered list of fields the index is built on
   * @param indexType   index type string
   * @param indexParam  index parameters (JSON)
   */
  Status AddFieldIndex(const std::string &index_name,
                       const std::vector<std::string> &field_names,
                       const std::string &indexType,
                       const std::string &indexParam);

  /**
   * @brief remove an index by its user-defined name. Routes to vector,
   *        scalar, or composite removal based on internal bookkeeping.
   *
   * @param index_name  user-defined index name to remove
   */
  Status RemoveFieldIndex(const std::string &index_name);

  int GetDocsNum();

  int GetTrainingThreshold() { return training_threshold_; }
  void SetIsDirty(bool is_dirty) { is_dirty_ = is_dirty; }
  int GetMaxDocid() { return max_docid_.load(); }
  void SetMaxDocid(int max_docid) { max_docid_.store(max_docid); }

  Table *GetTable() { return table_; }

  VectorManager *GetVectorManager() { return vec_manager_; }

  bitmap::BitmapManager *GetBitmap() { return docids_bitmap_; }

  int GetConfig(std::string &conf_str);

  int SetConfig(std::string conf_str);

  const std::string SpaceName() { return space_name_; }

  void Close();

 private:
  Engine(const std::string &index_root_path, const std::string &space_name);

  int CreateTableFromLocal(std::string &table_name);

  int Indexing();

  // Safely stop a running indexing thread (compare_exchange RUNNING->STOPPING,
  // wait for completion) and join it. `reason` is interpolated into the log
  // messages, e.g. "rebuild", "field index addition", "field removal".
  void StopIndexingThread(const std::string &reason);

  int AddNumIndexFields();

  int64_t ScalarIndexQuery(Request &request, SearchCondition *condition,
                      Response &response_results,
                      ScalarIndexResults *scalar_index_result);

  void BackupThread(int command);

  // Executes one queued ADD task on the maintenance worker thread. Dispatches
  // by index shape to AddVectorFieldIndex / AddCompositeFieldIndex /
  // AddScalarFieldIndex.
  void AddFieldIndexTask(const std::string &index_name,
                         const std::vector<std::string> &field_names,
                         const std::string &indexType,
                         const std::string &indexParam);

  // Branch implementations dispatched by AddFieldIndexTask, one per index
  // shape. Each publishes its index and logs its own outcome. Scalar/composite
  // use publish-then-backfill (empty index published BUILDING, backfilled
  // off-lock, flipped READY). All run on the single maintenance worker, so no
  // two schema mutations overlap.
  void AddVectorFieldIndex(const std::string &index_name,
                           const std::string &field_name,
                           const std::string &indexType,
                           const std::string &indexParam);

  void AddCompositeFieldIndex(const std::string &index_name,
                              const std::vector<std::string> &field_names);

  void AddScalarFieldIndex(const std::string &index_name,
                           const std::string &field_name,
                           const std::string &indexType);

  // Backfill an already-published (BUILDING) scalar/composite index object over
  // docids [0, snapshot_max] inclusive, then flip it to READY and drop the
  // persisted BUILDING marker. Shared by the dynamic add path and the
  // crash-recovery path in Load(). `index` must already be in the manager map.
  // Returns 0 on success.
  int BackfillScalarIndex(const std::string &index_name,
                          const std::shared_ptr<ScalarIndex> &index,
                          int64_t snapshot_max);

  void RemoveFieldIndexTask(const std::string &index_name);

  // Maintenance worker: single FIFO thread that serializes every schema
  // mutation (add+backfill, remove+DropAll) for this Engine, replacing the
  // former fire-and-forget add/remove threads. Serialization here — not the
  // raft applier — is what guarantees no two index mutations race.
  void IndexMaintenanceLoop();
  void EnsureIndexMaintThread();
  void EnqueueIndexTask(IndexTask task);

 private:
  std::string index_root_path_;
  std::string dump_path_;
  std::string space_name_;
  StorageManager *storage_mgr_;

  ScalarIndexManager *scalar_index_manager_;

  bitmap::BitmapManager *docids_bitmap_;
  Table *table_;
  VectorManager *vec_manager_;

  // Next docid to assign. Atomic because the maintenance worker reads it as the
  // backfill snapshot concurrently with the raft-apply write thread's
  // increment, and search threads read it during queries. seq_cst ordering
  // guarantees a snapshot read taken after publishing an index observes every
  // increment that preceded the publish (see BackfillScalarIndex).
  std::atomic<int64_t> max_docid_{0};
  int training_threshold_;
  int slow_search_time_;
  // all indexes: scalar index managed by scalar index manager and vector index managed by vector manager
  std::vector<struct IndexInfo> indexes_;

  std::atomic<int>
      delete_num_;  // Index building state management with atomic operations
  std::atomic<IndexingState> indexing_state_{IndexingState::IDLE};

  // Synchronization for index building operations
  std::mutex indexing_mutex_;
  std::condition_variable indexing_cv_;

  enum IndexStatus index_status_;

  const std::string date_time_format_;
  std::string last_dump_dir_;  // it should be delete after next dump
  std::atomic<int> backup_status_;
  std::thread backup_thread_;
  std::thread indexing_thread_;

  // Maintenance worker: single thread draining index_task_queue_ in FIFO order.
  std::thread index_maint_thread_;
  std::mutex index_task_mutex_;
  std::condition_variable index_task_cv_;
  std::deque<IndexTask> index_task_queue_;
  bool index_maint_stop_ = false;
  bool index_maint_started_ = false;

  bool created_table_;

  bool is_dirty_;

  int refresh_interval_;

#ifdef PERFORMANCE_TESTING
  std::atomic<uint64_t> search_num_;
#endif
};

class RequestConcurrentController {
 public:
  static RequestConcurrentController &GetInstance() {
    static RequestConcurrentController intance;
    return intance;
  }

  ~RequestConcurrentController() = default;

  bool Acquire(int req_num);

  void Release(int req_num);

 private:
  RequestConcurrentController();

  RequestConcurrentController(const RequestConcurrentController &) = delete;

  RequestConcurrentController &operator=(const RequestConcurrentController &) =
      delete;

  int GetMaxThread();

  int GetSystemInfo(const char *cmd);

 private:
  int cur_concurrent_num_;
  int concurrent_threshold_;
  int max_threads_;
};

}  // namespace vearch
