
/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <map>
#include <string>

#include "c_api/api_data/gamma_config.h"
#include "common/gamma_common_data.h"
#include "index/retrieval_model.h"
#include "util/bitmap_manager.h"
#include "util/log.h"
#include "vector/raw_vector.h"

namespace tig_gamma {

class VectorManager {
 public:
  VectorManager(const VectorStorageType &store_type,
                bitmap::BitmapManager *docids_bitmap,
                const std::string &root_path);
  ~VectorManager();

  int CreateVectorTable(TableInfo &table, utils::JsonParser *jp);

  int AddToStore(int docid, std::vector<struct Field> &fields);

  int Update(int docid, std::vector<struct Field> &fields);

  int Indexing();

  int AddRTVecsToIndex(bool &index_is_dirty);

  // int Add(int docid, const std::vector<Field *> &field_vecs);
  int Search(GammaQuery &query, GammaResult *results);
  
  int GetVector(const std::vector<std::pair<std::string, int>> &fields_ids,
                std::vector<std::string> &vec, bool is_bytearray = false);

  int GetDocVector(int docid, std::string &field_name,
                   std::vector<uint8_t> &vec);

  void GetTotalMemBytes(long &index_total_mem_bytes,
                        long &vector_total_mem_bytes);

  int Dump(const std::string &path, int dump_docid, int max_docid);
  int Load(const std::vector<std::string> &path, int &doc_num);

  bool Contains(std::string &field_name);

  void VectorNames(std::vector<std::string> &names) {
    for (const auto &it : raw_vectors_) {
      names.push_back(it.first);
    }
  }

  std::map<std::string, RetrievalModel *> &VectorIndexes() {
    return vector_indexes_;
  }

  int Delete(int docid);

  std::map<std::string, RawVector *> RawVectors() { return raw_vectors_; }
  std::map<std::string, RetrievalModel *> RetrievalModels() {
    return vector_indexes_;
  }

  int MinIndexedNum();

  int AlterCacheSize(struct CacheInfo &cache_info);

  int GetAllCacheSize(Config &conf);

  bitmap::BitmapManager *Bitmap() { return docids_bitmap_; };

 private:
  void Close();  // release all resource
  inline std::string IndexName(const std::string &field_name,
                               const std::string &retrieval_type) {
    return field_name + "_" + retrieval_type;
  }

 private:
  VectorStorageType default_store_type_;
  bitmap::BitmapManager *docids_bitmap_;
  bool table_created_;
  std::string root_path_;

  std::map<std::string, RawVector *> raw_vectors_;
  std::map<std::string, RetrievalModel *> vector_indexes_;
  std::vector<std::string> retrieval_types_;
};

}  // namespace tig_gamma
