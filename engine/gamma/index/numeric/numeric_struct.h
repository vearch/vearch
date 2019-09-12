/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifndef SRC_SEARCHER_INDEX_NUMERIC_NUMERIC_STRUCT_H_
#define SRC_SEARCHER_INDEX_NUMERIC_NUMERIC_STRUCT_H_

#include "range_query_result.h"
#include <cstdint>
#include <string>

namespace tig_gamma {
namespace NI {

enum class IndexFieldType : uint8_t {
  UNKNOWN = 0,

  INT,
  LONG,
  FLOAT,
  DOUBLE,

  STRING,
};

struct IndexField {
  std::string name;
  IndexFieldType type;
};

struct IndexIO {
  int Write() { return -1; }
  int Read() { return -1; }

  FILE *_fp;
};

struct Index {
  virtual ~Index() {}

  virtual int Search(const std::string &, const std::string &,
                     RangeQueryResultV1 &) const {
    return -1;
  }
  virtual void Add(const std::string &, int) {}
  virtual int Build(const int /*num_docs*/) { return -1; }
  virtual size_t MemoryUsage() const { return 0; }
  virtual void Output(const std::string &) {}
  virtual int Dump(const IndexIO &) { return -1; }
  virtual int Load(const IndexIO &) { return -1; }

  IndexField field_;
};

} // namespace NI
} // namespace tig_gamma

#endif // SRC_SEARCHER_INDEX_NUMERIC_NUMERIC_STRUCT_H_
