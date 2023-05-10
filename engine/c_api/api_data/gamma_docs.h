/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "gamma_doc.h"

namespace tig_gamma {

class GammaEngine;

class Docs : public RawData {
 public:
  Docs() { engine_ = nullptr; }

  virtual int Serialize(char **out, int *out_len);

  int Serialize(char ***out, int *out_len);

  virtual void Deserialize(const char *data, int len);

  void Deserialize(char **data, int len);

  void AddDoc(const Doc &doc);

  void AddDoc(Doc &&doc);

  void Reserve(int size);

  std::vector<Doc> &GetDocs();

  void SetEngine(GammaEngine *engine) { engine_ = engine; }

 private:
  std::vector<Doc> doc_vec_;

  GammaEngine *engine_;
};

}  // namespace tig_gamma
