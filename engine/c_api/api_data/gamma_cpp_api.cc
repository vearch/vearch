/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */
#include <fcntl.h>
#include <sys/stat.h>

#include <chrono>
#include <iostream>
#include <sstream>
#include <vector>

#include "gamma_cpp_api.h"
#include "gamma_batch_result.h"
#include "gamma_config.h"
#include "gamma_doc.h"
#include "gamma_engine_status.h"
#include "gamma_response.h"
#include "gamma_table.h"
#include "search/gamma_engine.h"
#include "util/log.h"
#include "util/utils.h"
#include "index/impl/gamma_index_ivfpq.h"
#include "index/impl/gamma_index_ivfpqfs.h"
#include "index/impl/gamma_index_ivfflat.h"
#include "index/impl/scann/gamma_index_vearch.h"
#include "search/gamma_engine.h"
#include "vector/vector_manager.h"
#include "vector/raw_vector.h"
#include "common/gamma_common_data.h"

int CPPSearch(void *engine, tig_gamma::Request *request, tig_gamma::Response *response) {
  int ret = static_cast<tig_gamma::GammaEngine *>(engine)->Search(*request, *response);
  if(ret)
    return ret;
  response->PackResults(request->Fields());
  return 0;
}

int CPPSearch2(void *engine, tig_gamma::VectorResult *result) {
  int ret = 0;

  tig_gamma::GammaQuery gamma_query;
  PerfTool perf_tool;
  gamma_query.condition = new tig_gamma::GammaSearchCondition(&perf_tool);

  auto vec_manager = static_cast<tig_gamma::GammaEngine *>(engine)->GetVectorManager();

  std::map<std::string, RetrievalModel *>::iterator iter =
        vec_manager->RetrievalModels().begin();

  RetrievalModel *index = iter->second;
  tig_gamma::RawVector *raw_vec = dynamic_cast<tig_gamma::RawVector *>(iter->second->vector_);

  if (result->n <= 0) {
    LOG(ERROR) << "Search n shouldn't less than 0!";
    return -1;
  }
  gamma_query.condition->Init(std::numeric_limits<float>::min(), std::numeric_limits<float>::max(),
                          vec_manager->Bitmap(), raw_vec);

  uint8_t *x =
        reinterpret_cast<uint8_t *>(result->query);
  ret = index->Search(gamma_query.condition, result->n, x, result->topn,
                                result->dists,
                                result->docids);
  if (ret)
    LOG(ERROR) << "index search error with ret=" << ret;

#ifdef PERFORMANCE_TESTING
  LOG(INFO) << gamma_query.condition->GetPerfTool().OutputPerf().str();
#endif
  return ret; 
}

int CPPAddOrUpdateDoc(void *engine, tig_gamma::Doc *doc) {
    return static_cast<tig_gamma::GammaEngine *>(engine)->AddOrUpdate(*doc);        
}

int CPPAddOrUpdateDocs(void *engine, tig_gamma::Docs *docs, tig_gamma::BatchResult *results) {
    return static_cast<tig_gamma::GammaEngine *>(engine)->AddOrUpdateDocs(*docs, *results);
}

static int AddToStore(tig_gamma::VectorManager *vec_manager, int docid, float *data, long i) {
  int ret = 0;
  auto raw_vectors = vec_manager->RawVectors();
  for (const auto &iter : raw_vectors) {
    const std::string &name = iter.first;
    ret = raw_vectors[name]->Add(docid, data + i * iter.second->MetaInfo()->Dimension());
    if (ret != 0) break;
  }
  return ret;
}

int CPPAddOrUpdateDocs2(void *engine, tig_gamma::Docs *docs, float *data, tig_gamma::BatchResult *results) {
#ifdef PERFORMANCE_TESTING
  double start = utils::getmillisecs();
#endif
  auto gamma_engine = static_cast<tig_gamma::GammaEngine *>(engine);
  auto vec_manager = gamma_engine->GetVectorManager();
  auto table = gamma_engine->GetTable();
  auto bitmap = gamma_engine->GetBitmap();
  int b_running = gamma_engine->GetBRunning();
  int indexing_size = gamma_engine->GetIndexingSize();
  tig_gamma::IndexStatus index_status = gamma_engine->GetIndexStatus();
 
  std::vector<tig_gamma::Doc> &doc_vec = docs->GetDocs();
  std::set<std::string> remove_dupliacte;
  int batch_size = 0, start_id = 0;

  auto batchAdd = [&](int start_id, int batch_size) {
    if (batch_size <= 0) return;

    int ret =
        table->BatchAdd(start_id, batch_size, gamma_engine->GetMaxDocid(), doc_vec, *results);
    if (ret != 0) {
      LOG(ERROR) << "Add to table error";
      return;
    }

    for (int i = start_id; i < start_id + batch_size; ++i) {
      ret = AddToStore(vec_manager, gamma_engine->GetMaxDocid() + i - start_id, data, i);
      if (ret != 0) {
        std::string msg = "Add to vector manager error";
        results->SetResult(i, -1, msg);
        LOG(ERROR) << msg;
        continue;
      }
    }

    gamma_engine->SetMaxDocid(batch_size + gamma_engine->GetMaxDocid());
    bitmap->SetMaxID(gamma_engine->GetMaxDocid());
  };

  for (size_t i = 0; i < doc_vec.size(); ++i) {
    tig_gamma::Doc &doc = doc_vec[i];
    std::string &key = doc.Key();
    auto ite = remove_dupliacte.find(key);
    if (ite == remove_dupliacte.end()) remove_dupliacte.insert(key);
    // add fields into table
    int docid = -1;
    table->GetDocIDByKey(key, docid);
    if (docid == -1 && ite == remove_dupliacte.end()) {
      ++batch_size;
      continue;
    } else {
      batchAdd(start_id, batch_size);
      batch_size = 0;
      start_id = i + 1;
      std::vector<tig_gamma::Field> &fields_table = doc.TableFields();
      std::vector<tig_gamma::Field> &fields_vec = doc.VectorFields();
      if (ite != remove_dupliacte.end()) table->GetDocIDByKey(key, docid);
      if (fields_table.size() > 0 || fields_vec.size() > 0) {
        LOG(ERROR) << "don't support update now, key=" << key << ", docid=" << docid;
        continue;
      }
    }
  }

  batchAdd(start_id, batch_size);
  if (not b_running and index_status == tig_gamma::IndexStatus::UNINDEXED) {
    if (gamma_engine->GetMaxDocid() >= indexing_size) {
      LOG(INFO) << "Begin indexing.";
      gamma_engine->BuildIndex();
    }
  }
#ifdef PERFORMANCE_TESTING
  double end = utils::getmillisecs();
  if (gamma_engine->GetMaxDocid() % 10000 == 0) {
    LOG(INFO) << "Doc_num[" << gamma_engine->GetMaxDocid() << "], BatchAdd[" << batch_size
              << "] total cost [" << end - start << "]ms";
  }
#endif
  gamma_engine->SetIsDirty(true);
  return 0;
}

void CPPSetNprobe(void *engine, int nprobe, std::string index_type) {
  auto retrieval_model = static_cast<tig_gamma::GammaEngine *>(engine)->GetVectorManager()->RetrievalModels().begin()->second;
  if (index_type == "IVFPQ") {
    tig_gamma::GammaIVFPQIndex *index = dynamic_cast<tig_gamma::GammaIVFPQIndex *>(retrieval_model);
    if(index) {
      index->nprobe = nprobe;
    }
  } else if (index_type == "IVFFLAT") {
    tig_gamma::GammaIndexIVFFlat *index = dynamic_cast<tig_gamma::GammaIndexIVFFlat *>(retrieval_model);
    if(index) {
      index->nprobe = nprobe;
    }
  } else if (index_type == "IVFPQFastScan") {
    tig_gamma::GammaIVFPQFastScanIndex *index = dynamic_cast<tig_gamma::GammaIVFPQFastScanIndex *>(retrieval_model);
    if(index) {
      index->nprobe = nprobe;
    }
  } else if (index_type == "VEARCH") {
#ifdef USE_SCANN
#ifdef PYTHON_SDK
    tig_gamma::GammaVearchIndex *index = dynamic_cast<tig_gamma::GammaVearchIndex *>(retrieval_model);
    if(index) {
      index->nprobe = nprobe;
    }
#endif
#endif
  }
}

void CPPSetRerank(void *engine, int rerank, std::string index_type) {
  auto retrieval_model = static_cast<tig_gamma::GammaEngine *>(engine)->GetVectorManager()->RetrievalModels().begin()->second;
  if (index_type == "IVFPQ") {
    tig_gamma::GammaIVFPQIndex *index = dynamic_cast<tig_gamma::GammaIVFPQIndex *>(retrieval_model);
    if(index) {
      index->rerank_ = rerank;
    }
  } else if (index_type == "IVFPQ_RELAYOUT") {
#ifdef OPT_IVFPQ_RELAYOUT
    tig_gamma::GammaIndexIVFPQRelayout *index = dynamic_cast<tig_gamma::GammaIndexIVFPQRelayout *>(retrieval_model);
    if(index) {
      index->rerank_ = rerank;
    }  
#endif
  } else if (index_type == "IVFPQFastScan") {
    tig_gamma::GammaIVFPQFastScanIndex *index = dynamic_cast<tig_gamma::GammaIVFPQFastScanIndex *>(retrieval_model);
    if(index) {
      index->rerank_ = rerank;
    }
  } else if (index_type == "VEARCH") {
#ifdef USE_SCANN
#ifdef PYTHON_SDK
    tig_gamma::GammaVearchIndex *index = dynamic_cast<tig_gamma::GammaVearchIndex *>(retrieval_model);
    if(index) {
      index->rerank_ = rerank;
    }
#endif
#endif
  }
}


