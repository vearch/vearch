/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifndef GAMMA_INDEX_H_
#define GAMMA_INDEX_H_

#include <vector>
#include "gamma_common_data.h"
#include "raw_vector.h"

namespace tig_gamma {

struct VectorResult {
  VectorResult() {
    n = 0;
    topn = 0;
    dists = nullptr;
    docids = nullptr;
    sources = nullptr;
    source_lens = nullptr;
    total.resize(n);
    idx.resize(n);
    idx.assign(n, 0);
    total.assign(n, 0);
  }

  ~VectorResult() {
    if (dists) {
      delete dists;
      dists = nullptr;
    }

    if (docids) {
      delete docids;
      docids = nullptr;
    }

    if (sources) {
      delete sources;
      sources = nullptr;
    }

    if (source_lens) {
      delete source_lens;
      source_lens = nullptr;
    }
  }

  bool init(int a, int b) {
    n = a;
    topn = b;
    dists = new float[n * topn];
    if (!dists) {
      // LOG(ERROR) << "dists in VectorResult malloc error!";
      return false;
    }
    docids = new long[n * topn];
    if (!docids) {
      // LOG(ERROR) << "docids in VectorResult malloc error!";
      return false;
    }

    sources = new char *[n * topn];
    if (!sources) {
      // LOG(ERROR) << "sources in VectorResult malloc error!";
      return false;
    }

    source_lens = new int[n * topn];

    if (!source_lens) {
      // LOG(ERROR) << "source_lens in VectorResult malloc error!";
      return false;
    }

    total.resize(n, 0);
    idx.resize(n, -1);

    return true;
  }

  int seek(const int &req_no, const int &docid, float &score, char *&source,
           int &len) {
    int ret = -1;
    int base_idx = req_no * topn;
    int &start_idx = idx[req_no];
    if (start_idx == -1) return -1;
    for (int i = base_idx + start_idx; i < base_idx + topn; i++) {
      if (docids[i] >= docid) {
        ret = docids[i];
        score = dists[i];
        source = sources[i];
        len = source_lens[i];

        start_idx = i - base_idx;
        break;
      } else {
        continue;
      }
    }
    if (ret == -1) start_idx = -1;
    return ret;
  }

  int n;
  int topn;
  float *dists;
  long *docids;
  char **sources;
  int *source_lens;
  std::vector<int> total;
  std::vector<int> idx;
};

struct GammaIndex {
  GammaIndex(size_t dimension, const char *docids_bitmap, RawVector *raw_vec)
      : d_(dimension), docids_bitmap_(docids_bitmap), raw_vec_(raw_vec) {}

  virtual ~GammaIndex() {}

  virtual int Indexing() = 0;

  virtual int AddRTVecsToIndex() = 0;
  virtual bool Add(int n, const float *vec) = 0;

  /** assign the vectors, then call search_preassign */
  virtual int Search(const VectorQuery *query,
                     const GammaSearchCondition *condition,
                     VectorResult &result) = 0;

  virtual long GetTotalMemBytes() = 0;

  virtual int Dump(const std::string &dir, int max_vid) = 0;
  virtual int Load(const std::vector<std::string> &index_dirs) = 0;

  int d_;

  const char *docids_bitmap_;
  RawVector *raw_vec_;
};

}  // namespace tig_gamma

#endif
