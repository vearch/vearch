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

#include "common/gamma_common_data.h"
#include "config.h"
#include "doc.h"
#include "engine_status.h"
#include "index/impl/gamma_index_ivfflat.h"
#include "index/impl/gamma_index_ivfpq.h"
#include "index/impl/gamma_index_ivfpqfs.h"
#include "index/impl/scann/gamma_index_vearch.h"
#include "response.h"
#include "search/engine.h"
#include "table.h"
#include "util/log.h"
#include "util/utils.h"
#include "vector/raw_vector.h"
#include "vector/vector_manager.h"

int CPPSearch(void *engine, vearch::Request *request,
              vearch::Response *response) {
  vearch::Status status =
      static_cast<vearch::Engine *>(engine)->Search(*request, *response);
  if (status.code()) return status.code();
  response->PackResults(request->Fields());
  return 0;
}

int CPPSearch2(void *engine, vearch::VectorResult *result) {
  int ret = 0;

  vearch::GammaQuery gamma_query;
  PerfTool perf_tool;
  gamma_query.condition = new vearch::SearchCondition(&perf_tool);

  auto vec_manager = static_cast<vearch::Engine *>(engine)->GetVectorManager();

  std::map<std::string, IndexModel *>::iterator iter =
      vec_manager->IndexModels().begin();

  IndexModel *index = iter->second;
  vearch::RawVector *raw_vec =
      dynamic_cast<vearch::RawVector *>(iter->second->vector_);

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
  if(gamma_query.condition->GetPerfTool()) {
    LOG(INFO) << gamma_query.condition->GetPerfTool()->OutputPerf().str();
  }
#endif
  return ret;
}

int CPPAddOrUpdateDoc(void *engine, vearch::Doc *doc) {
  return static_cast<vearch::Engine *>(engine)->AddOrUpdate(*doc);
}

void CPPSetNprobe(void *engine, int nprobe, std::string index_type) {
  auto index_model = static_cast<vearch::Engine *>(engine)
                         ->GetVectorManager()
                         ->IndexModels()
                         .begin()
                         ->second;
  if (index_type == "IVFPQ") {
    vearch::GammaIVFPQIndex *index =
        dynamic_cast<vearch::GammaIVFPQIndex *>(index_model);
    if (index) {
      index->nprobe = nprobe;
    }
  } else if (index_type == "IVFFLAT") {
    vearch::GammaIndexIVFFlat *index =
        dynamic_cast<vearch::GammaIndexIVFFlat *>(index_model);
    if (index) {
      index->nprobe = nprobe;
    }
  } else if (index_type == "IVFPQFastScan") {
    vearch::GammaIVFPQFastScanIndex *index =
        dynamic_cast<vearch::GammaIVFPQFastScanIndex *>(index_model);
    if (index) {
      index->nprobe = nprobe;
    }
  } else if (index_type == "VEARCH") {
#ifdef USE_SCANN
#ifdef PYTHON_SDK
    vearch::GammaVearchIndex *index =
        dynamic_cast<vearch::GammaVearchIndex *>(index_model);
    if (index) {
      index->nprobe = nprobe;
    }
#endif
#endif
  }
}

void CPPSetRerank(void *engine, int rerank, std::string index_type) {
  auto index_model = static_cast<vearch::Engine *>(engine)
                         ->GetVectorManager()
                         ->IndexModels()
                         .begin()
                         ->second;
  if (index_type == "IVFPQ") {
    vearch::GammaIVFPQIndex *index =
        dynamic_cast<vearch::GammaIVFPQIndex *>(index_model);
    if (index) {
      index->rerank_ = rerank;
    }
  } else if (index_type == "IVFPQ_RELAYOUT") {
#ifdef OPT_IVFPQ_RELAYOUT
    vearch::GammaIndexIVFPQRelayout *index =
        dynamic_cast<vearch::GammaIndexIVFPQRelayout *>(index_model);
    if (index) {
      index->rerank_ = rerank;
    }
#endif
  } else if (index_type == "IVFPQFastScan") {
    vearch::GammaIVFPQFastScanIndex *index =
        dynamic_cast<vearch::GammaIVFPQFastScanIndex *>(index_model);
    if (index) {
      index->rerank_ = rerank;
    }
  } else if (index_type == "VEARCH") {
#ifdef USE_SCANN
#ifdef PYTHON_SDK
    vearch::GammaVearchIndex *index =
        dynamic_cast<vearch::GammaVearchIndex *>(index_model);
    if (index) {
      index->rerank_ = rerank;
    }
#endif
#endif
  }
}
