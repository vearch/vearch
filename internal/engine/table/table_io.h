/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "c_api/api_data/table.h"
#include "util/utils.h"

namespace vearch {

class TableSchemaIO {
 public:
  TableSchemaIO(std::string &file_path);

  ~TableSchemaIO();

  int Write(TableInfo &table);

  void WriteIndexingSize(TableInfo &table);

  void WriteFieldInfos(TableInfo &table);

  void WriteVectorInfos(TableInfo &table);

  void WriteIndexType(TableInfo &table);

  void WriteIndexParams(TableInfo &table);

  int Read(std::string &name, TableInfo &table);

  void ReadTrainingThreshold(TableInfo &table);

  void ReadFieldInfos(TableInfo &table);

  void ReadVectorInfos(TableInfo &table);

  void ReadIndexType(TableInfo &table);

  void ReadIndexParams(TableInfo &table);

  utils::FileIO *fio;
};

}  // namespace vearch
