/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifndef VECTOR_MANAGER_H_
#define VECTOR_MANAGER_H_

#include "log.h"
#include <map>
#include <string>

#include "gamma_api.h"
#include "gamma_common_data.h"
#include "gamma_index.h"
#include "raw_vector.h"

namespace tig_gamma {

class VectorManager {
public:
  VectorManager(const RetrievalModel &model, const RawVectorType &store_type,
                const char *docids_bitmap, int max_doc_size);
  ~VectorManager();

  int CreateVectorTable(VectorInfo **vectors_info, int vectors_num,
                        IVFPQParameters *ivfpq_param);

  int AddToStore(int docid, std::vector<Field *> &fields);

  int Indexing();

  int AddRTVecsToIndex();

  // int Add(int docid, const std::vector<Field *> &field_vecs);
  int Search(const GammaQuery &query, GammaResult *results);

  long GetTotalMemBytes() {
    long index_total_mem_bytes = 0;
    for (auto iter = vector_indexes_.begin(); iter != vector_indexes_.end();
         iter++) {
      index_total_mem_bytes += iter->second->GetTotalMemBytes();
    }

    long vector_total_mem_bytes = 0;
    for (auto iter = raw_vectors_.begin(); iter != raw_vectors_.end(); ++iter) {
      vector_total_mem_bytes += iter->second->GetTotalMemBytes();
    }

    return index_total_mem_bytes + vector_total_mem_bytes;
  }

  int Dump(const std::string &path, int dump_docid, int max_docid);
  int Load(const std::vector<std::string> &path);

private:
  void Close(); // release all resource

private:
  RetrievalModel default_model_;
  RawVectorType default_store_type_;
  const char *docids_bitmap_;
  int max_doc_size_;
  bool table_created_;
  IVFPQParameters *ivfpq_param_;
  VectorInfo **vectors_info_;
  int vectors_num_;

  std::map<std::string, RawVector *> raw_vectors_;
  std::map<std::string, GammaIndex *> vector_indexes_;
};

} // namespace tig_gamma

#endif
