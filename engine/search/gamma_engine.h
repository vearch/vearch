/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <condition_variable>
#include <string>

#include "c_api/api_data/gamma_batch_result.h"
#include "c_api/api_data/gamma_doc.h"
#include "c_api/api_data/gamma_docs.h"
#include "c_api/api_data/gamma_engine_status.h"
#include "c_api/api_data/gamma_memory_info.h"
#include "c_api/api_data/gamma_request.h"
#include "c_api/api_data/gamma_response.h"
#include "c_api/api_data/gamma_table.h"
#include "io/async_flush.h"
#include "table/field_range_index.h"
#include "table/table.h"
#include "util/bitmap_manager.h"
#include "vector/vector_manager.h"

namespace tig_gamma {

enum IndexStatus { UNINDEXED = 0, INDEXING, INDEXED };

class GammaEngine {
 public:
  static GammaEngine *GetInstance(const std::string &index_root_path,
                                  const std::string &space_name = "");

  ~GammaEngine();

  int Setup();

  int Search(Request &request, Response &response_results);

  int CreateTable(TableInfo &table);

  int AddOrUpdate(Doc &doc);

  int AddOrUpdateDocs(Docs &docs, BatchResult &result);

  int Update(int doc_id, std::vector<struct Field> &fields_table,
             std::vector<struct Field> &fields_vec);

  /**
   * Delete doc
   * @param key
   * @return 0 if successed
   */
  int Delete(std::string &key);

  int GetDoc(const std::string &key, Doc &doc);

  int GetDoc(int docid, Doc &doc);

  /**
   * blocking to build index
   * @return 0 if exited
   */
  int BuildIndex();

  int RebuildIndex(int drop_before_rebuild, int limit_cpu, int describe);

  void GetIndexStatus(EngineStatus &engine_status);
  void GetMemoryInfo(MemoryInfo &memory_info);

  IndexStatus GetIndexStatus() { return index_status_; }

  int Dump();

  int Load();

  int LoadFromFaiss();

  int GetDocsNum();

  int GetBRunning() { return b_running_; }
  int GetIndexingSize() { return indexing_size_; }
  void SetIsDirty(bool is_dirty) { is_dirty_ = is_dirty; }
  int GetMaxDocid() { return max_docid_; }
  void SetMaxDocid(int max_docid) { max_docid_ = max_docid; }

  Table *GetTable() { return table_; }

  VectorManager *GetVectorManager() { return vec_manager_; }

  bitmap::BitmapManager *GetBitmap() { return docids_bitmap_; }

  int SetBatchDocsNum(int i) {
    batch_docs_.resize(i);
    return 0;
  }

  int BatchDocsPrepare(char *doc_str, int idx) {
    if (idx >= (int)batch_docs_.size()) {
      LOG(ERROR) << "idx [" << idx << "] > batch_docs size ["
                 << batch_docs_.size() << "]";
      return -1;
    }
    batch_docs_[idx] = doc_str;
    return 0;
  }

  char **BatchDocsStr() { return batch_docs_.data(); }

  int GetConfig(Config &config);

  int SetConfig(Config &config);

  const std::string SpaceName() { return space_name_; }

 private:
  GammaEngine(const std::string &index_root_path,
              const std::string &space_name);

  int CreateTableFromLocal(std::string &table_name);

  int Indexing();

  int AddNumIndexFields();

  int MultiRangeQuery(Request &request, GammaSearchCondition *condition,
                      Response &response_results,
                      MultiRangeQueryResults *range_query_result);

 private:
  std::string index_root_path_;
  std::string dump_path_;
  std::string space_name_;

  MultiFieldsRangeIndex *field_range_index_;

  bitmap::BitmapManager *docids_bitmap_;
  Table *table_;
  VectorManager *vec_manager_;

  int max_docid_;
  int indexing_size_;

  std::atomic<int> delete_num_;

  int b_running_;  // 0 not run, not 0 running

  std::condition_variable running_cv_;

  enum IndexStatus index_status_;

  const std::string date_time_format_;
  std::string last_dump_dir_;  // it should be delete after next dump

  bool created_table_;

  bool b_loading_;

  bool is_dirty_;

  std::vector<char *> batch_docs_;

#ifdef PERFORMANCE_TESTING
  std::atomic<uint64_t> search_num_;
#endif

  AsyncFlushExecutor *af_exector_;
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

}  // namespace tig_gamma
