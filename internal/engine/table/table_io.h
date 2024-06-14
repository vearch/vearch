/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "c_api/api_data/table.h"

namespace vearch {

class TableSchemaIO {
 public:
  TableSchemaIO(std::string &file_path);

  ~TableSchemaIO();

  int Write(TableInfo &table);

  int Read(std::string &name, TableInfo &table);

  std::string file_path;
};

}  // namespace vearch
