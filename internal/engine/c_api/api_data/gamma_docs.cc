/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "gamma_docs.h"
#include "util/utils.h"
#include "util/log.h"

namespace tig_gamma {

int Docs::Serialize(char **out, int *out_len) {
  return 0;
}

int Docs::Serialize(char ***out, int *out_len) {
  *out = (char **)malloc(doc_vec_.size() * sizeof(char *));

#pragma omp parallel for
  for (size_t i = 0; i < doc_vec_.size(); ++i) {
    Doc &doc = doc_vec_[i];
    int len = 0;
    doc.Serialize(&(*out)[i], &len);
  }
  *out_len = doc_vec_.size();
  return 0;
}

void Docs::Deserialize(const char *data, int len) {
}

void Docs::Deserialize(char **data, int len) {
  size_t docs_num = len;
  doc_vec_.resize(docs_num);

#pragma omp parallel for
  for (size_t i = 0; i < docs_num; ++i) {
    Doc d;
    d.SetEngine(engine_);
    d.Deserialize(data[i], 0);
    doc_vec_[i] = std::move(d);
  }
}

void Docs::Reserve(int size) { doc_vec_.reserve(size); }

void Docs::AddDoc(const Doc &doc) { doc_vec_.push_back(doc); }

void Docs::AddDoc(Doc &&doc) { doc_vec_.emplace_back(std::forward<Doc>(doc)); }

std::vector<Doc> &Docs::GetDocs() { return doc_vec_; }

}  // namespace tig_gamma