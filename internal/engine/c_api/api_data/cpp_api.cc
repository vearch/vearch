/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */
#include "cpp_api.h"

#include <fcntl.h>
#include <sys/stat.h>

#include <chrono>
#include <iostream>
#include <sstream>
#include <vector>

#include "batch_result.h"
#include "common/gamma_common_data.h"
#include "config.h"
#include "doc.h"
#include "engine_status.h"
#include "index/impl/gamma_index_ivfflat.h"
#include "index/impl/gamma_index_ivfpq.h"
#include "index/impl/gamma_index_ivfpqfs.h"
#include "index/impl/scann/gamma_index_vearch.h"
#include "response.h"
#include "search/gamma_engine.h"
#include "table.h"
#include "util/log.h"
#include "util/utils.h"
#include "vector/raw_vector.h"
#include "vector/vector_manager.h"

int CPPSearch(void *engine, tig_gamma::Request *request,
              tig_gamma::Response *response) {
  int ret = static_cast<tig_gamma::GammaEngine *>(engine)->Search(*request,
                                                                  *response);
  if (ret) return ret;
  response->PackResults(request->Fields());
  return 0;
}

int CPPSearch2(void *engine, tig_gamma::VectorResult *result) {
  int ret = 0;

  tig_gamma::GammaQuery gamma_query;
  PerfTool perf_tool;
  gamma_query.condition = new tig_gamma::GammaSearchCondition(&perf_tool);

  auto vec_manager =
      static_cast<tig_gamma::GammaEngine *>(engine)->GetVectorManager();

  std::map<std::string, RetrievalModel *>::iterator iter =
      vec_manager->RetrievalModels().begin();

  RetrievalModel *index = iter->second;
  tig_gamma::RawVector *raw_vec =
      dynamic_cast<tig_gamma::RawVector *>(iter->second->vector_);

  if (result->n <= 0) {
    LOG(ERROR) << "Search n shouldn't less than 0!";
    return -1;
  }
  gamma_query.condition->Init(std::numeric_limits<float>::min(),
                              std::numeric_limits<float>::max(),
                              vec_manager->Bitmap(), raw_vec);

  uint8_t *x = reinterpret_cast<uint8_t *>(result->query);
  ret = index->Search(gamma_query.condition, result->n, x, result->topn,
                      result->dists, result->docids);
  if (ret) LOG(ERROR) << "index search error with ret=" << ret;

#ifdef PERFORMANCE_TESTING
  LOG(INFO) << gamma_query.condition->GetPerfTool().OutputPerf().str();
#endif
  return ret;
}

int CPPAddOrUpdateDoc(void *engine, tig_gamma::Doc *doc) {
  return static_cast<tig_gamma::GammaEngine *>(engine)->AddOrUpdate(*doc);
}

int CPPAddOrUpdateDocs(void *engine, tig_gamma::Docs *docs,
                       tig_gamma::BatchResult *results) {
  return static_cast<tig_gamma::GammaEngine *>(engine)->AddOrUpdateDocs(
      *docs, *results);
}

void CPPSetNprobe(void *engine, int nprobe, std::string index_type) {
  auto retrieval_model = static_cast<tig_gamma::GammaEngine *>(engine)
                             ->GetVectorManager()
                             ->RetrievalModels()
                             .begin()
                             ->second;
  if (index_type == "IVFPQ") {
    tig_gamma::GammaIVFPQIndex *index =
        dynamic_cast<tig_gamma::GammaIVFPQIndex *>(retrieval_model);
    if (index) {
      index->nprobe = nprobe;
    }
  } else if (index_type == "IVFFLAT") {
    tig_gamma::GammaIndexIVFFlat *index =
        dynamic_cast<tig_gamma::GammaIndexIVFFlat *>(retrieval_model);
    if (index) {
      index->nprobe = nprobe;
    }
  } else if (index_type == "IVFPQFastScan") {
    tig_gamma::GammaIVFPQFastScanIndex *index =
        dynamic_cast<tig_gamma::GammaIVFPQFastScanIndex *>(retrieval_model);
    if (index) {
      index->nprobe = nprobe;
    }
  } else if (index_type == "VEARCH") {
#ifdef USE_SCANN
#ifdef PYTHON_SDK
    tig_gamma::GammaVearchIndex *index =
        dynamic_cast<tig_gamma::GammaVearchIndex *>(retrieval_model);
    if (index) {
      index->nprobe = nprobe;
    }
#endif
#endif
  }
}

void CPPSetRerank(void *engine, int rerank, std::string index_type) {
  auto retrieval_model = static_cast<tig_gamma::GammaEngine *>(engine)
                             ->GetVectorManager()
                             ->RetrievalModels()
                             .begin()
                             ->second;
  if (index_type == "IVFPQ") {
    tig_gamma::GammaIVFPQIndex *index =
        dynamic_cast<tig_gamma::GammaIVFPQIndex *>(retrieval_model);
    if (index) {
      index->rerank_ = rerank;
    }
  } else if (index_type == "IVFPQ_RELAYOUT") {
#ifdef OPT_IVFPQ_RELAYOUT
    tig_gamma::GammaIndexIVFPQRelayout *index =
        dynamic_cast<tig_gamma::GammaIndexIVFPQRelayout *>(retrieval_model);
    if (index) {
      index->rerank_ = rerank;
    }
#endif
  } else if (index_type == "IVFPQFastScan") {
    tig_gamma::GammaIVFPQFastScanIndex *index =
        dynamic_cast<tig_gamma::GammaIVFPQFastScanIndex *>(retrieval_model);
    if (index) {
      index->rerank_ = rerank;
    }
  } else if (index_type == "VEARCH") {
#ifdef USE_SCANN
#ifdef PYTHON_SDK
    tig_gamma::GammaVearchIndex *index =
        dynamic_cast<tig_gamma::GammaVearchIndex *>(retrieval_model);
    if (index) {
      index->rerank_ = rerank;
    }
#endif
#endif
  }
}
