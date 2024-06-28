/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <condition_variable>
#include <string>

#include "c_api/api_data/doc.h"
#include "c_api/api_data/request.h"
#include "c_api/api_data/response.h"
#include "c_api/api_data/table.h"
#include "table/field_range_index.h"
#include "table/table.h"
#include "util/bitmap_manager.h"
#include "vector/vector_manager.h"

namespace vearch {

enum IndexStatus { UNINDEXED = 0, INDEXING, INDEXED };

class Engine {
 public:
  static Engine *GetInstance(const std::string &index_root_path,
                             const std::string &space_name = "");

  ~Engine();

  Status Setup();

  Status Search(Request &request, Response &response_results);

  Status CreateTable(TableInfo &table);

  int AddOrUpdate(Doc &doc);

  int Update(int doc_id,
             std::unordered_map<std::string, struct Field> &fields_table,
             std::unordered_map<std::string, struct Field> &fields_vec);

  int Delete(std::string &key);

  int GetDoc(const std::string &key, Doc &doc);

  int GetDoc(int docid, Doc &doc, bool next = false);

  /**
   * blocking to build index
   * @return 0 if exited
   */
  int BuildIndex();

  int RebuildIndex(int drop_before_rebuild, int limit_cpu, int describe);

  std::string EngineStatus();
  std::string GetMemoryInfo();

  IndexStatus GetIndexStatus() { return index_status_; }

  int Dump();
  int Load();
  int LoadFromFaiss();

  Status Backup(int command);

  int GetDocsNum();

  int GetBRunning() { return b_running_; }
  int GetTrainingThreshold() { return training_threshold_; }
  void SetIsDirty(bool is_dirty) { is_dirty_ = is_dirty; }
  int GetMaxDocid() { return max_docid_; }
  void SetMaxDocid(int max_docid) { max_docid_ = max_docid; }

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

  int AddNumIndexFields();

  int MultiRangeQuery(Request &request, SearchCondition *condition,
                      Response &response_results,
                      MultiRangeQueryResults *range_query_result);

  void BacupThread(int command);

 private:
  std::string index_root_path_;
  std::string dump_path_;
  std::string space_name_;
  StorageManager *storage_mgr_;

  MultiFieldsRangeIndex *field_range_index_;

  bitmap::BitmapManager *docids_bitmap_;
  Table *table_;
  VectorManager *vec_manager_;

  int max_docid_;
  int training_threshold_;

  std::atomic<int> delete_num_;

  int b_running_;  // 0 not run, not 0 running

  std::condition_variable running_cv_;

  enum IndexStatus index_status_;

  const std::string date_time_format_;
  std::string last_dump_dir_;  // it should be delete after next dump
  std::atomic<int> backup_status_;
  std::thread backup_thread_;

  bool created_table_;

  bool is_dirty_;

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
