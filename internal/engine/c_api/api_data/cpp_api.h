/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once
#include <string>

#include "batch_result.h"
#include "common/common_query_data.h"
#include "doc.h"
#include "docs.h"
#include "request.h"
#include "response.h"

// Here are some corresponding C++ interfaces in c_api/gamma_api.h

int CPPSearch(void *engine, tig_gamma::Request *request,
              tig_gamma::Response *response);

int CPPSearch2(void *engine, tig_gamma::VectorResult *result);

int CPPAddOrUpdateDoc(void *engine, tig_gamma::Doc *doc);

int CPPAddOrUpdateDocs(void *engine, tig_gamma::Docs *docs,
                       tig_gamma::BatchResult *results);

void CPPSetNprobe(void *engine, int nprobe, std::string index_type);

void CPPSetRerank(void *engine, int rerank, std::string index_type);
