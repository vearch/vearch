/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "table_io.h"

#include "util/log.h"

namespace vearch {

static const char *kPlaceHolder = "NULL";

static void FWriteByteArray(utils::FileIO *fio, std::string &ba) {
  int len = ba.length();
  fio->Write((void *)&len, sizeof(len), 1);
  fio->Write((void *)ba.c_str(), len, 1);
}

static void FReadByteArray(utils::FileIO *fio, std::string &ba) {
  int len = 0;
  fio->Read((void *)&len, sizeof(len), 1);
  char *data = new char[len];
  fio->Read((void *)data, sizeof(char), len);

  ba = std::string(data, len);
  delete[] data;
}

TableSchemaIO::TableSchemaIO(std::string &file_path) {
  fio = new utils::FileIO(file_path);
}

TableSchemaIO::~TableSchemaIO() {
  if (fio) {
    delete fio;
    fio = nullptr;
  }
}

int TableSchemaIO::Write(TableInfo &table) {
  if (!fio->IsOpen() && fio->Open("wb")) {
    LOG(INFO) << "open error, file path=" << fio->Path();
    return -1;
  }
  WriteIndexingSize(table);
  WriteFieldInfos(table);
  WriteVectorInfos(table);
  WriteIndexType(table);
  WriteIndexParams(table);
  return 0;
}

void TableSchemaIO::WriteIndexingSize(TableInfo &table) {
  int training_threshold = table.TrainingThreshold();
  fio->Write((void *)&training_threshold, sizeof(int), 1);
}

void TableSchemaIO::WriteFieldInfos(TableInfo &table) {
  std::vector<struct FieldInfo> &fields = table.Fields();
  int fields_num = fields.size();

  fio->Write((void *)&fields_num, sizeof(int), 1);
  for (int i = 0; i < fields_num; ++i) {
    struct FieldInfo &fi = fields[i];
    FWriteByteArray(fio, fi.name);
    fio->Write((void *)&fi.data_type, sizeof(fi.data_type), 1);
    fio->Write((void *)&fi.is_index, sizeof(fi.is_index), 1);
  }
}

void TableSchemaIO::WriteVectorInfos(TableInfo &table) {
  std::vector<struct VectorInfo> &vectors = table.VectorInfos();
  int vectors_num = vectors.size();

  fio->Write((void *)&vectors_num, sizeof(int), 1);
  for (int i = 0; i < vectors_num; ++i) {
    struct VectorInfo &vi = vectors[i];
    FWriteByteArray(fio, vi.name);
    fio->Write((void *)&vi.data_type, sizeof(vi.data_type), 1);
    fio->Write((void *)&vi.is_index, sizeof(vi.is_index), 1);
    fio->Write((void *)&vi.dimension, sizeof(vi.dimension), 1);
    FWriteByteArray(fio, vi.store_type);
    if (vi.store_param != "") {
      FWriteByteArray(fio, vi.store_param);
    } else {
      std::string ba = kPlaceHolder;
      FWriteByteArray(fio, ba);
    }
  }
}

void TableSchemaIO::WriteIndexType(TableInfo &table) {
  FWriteByteArray(fio, table.IndexType());
}

void TableSchemaIO::WriteIndexParams(TableInfo &table) {
  FWriteByteArray(fio, table.IndexParams());
}

int TableSchemaIO::Read(std::string &name, TableInfo &table) {
  if (!fio->IsOpen() && fio->Open("rb")) {
    LOG(INFO) << "open error, file path=" << fio->Path();
    return -1;
  }
  table.SetName(name);
  ReadTrainingThreshold(table);
  ReadFieldInfos(table);
  ReadVectorInfos(table);
  ReadIndexType(table);
  ReadIndexParams(table);
  return 0;
}

void TableSchemaIO::ReadTrainingThreshold(TableInfo &table) {
  int training_threshold = 0;
  fio->Read((void *)&training_threshold, sizeof(int), 1);
  table.SetTrainingThreshold(training_threshold);
}

void TableSchemaIO::ReadFieldInfos(TableInfo &table) {
  int fields_num = 0;
  fio->Read((void *)&fields_num, sizeof(int), 1);

  for (int i = 0; i < fields_num; ++i) {
    struct FieldInfo fi;
    FReadByteArray(fio, fi.name);
    fio->Read((void *)&fi.data_type, sizeof(fi.data_type), 1);
    fio->Read((void *)&fi.is_index, sizeof(fi.is_index), 1);
    table.AddField(fi);
  }
}

void TableSchemaIO::ReadVectorInfos(TableInfo &table) {
  int vectors_num = 0;

  fio->Read((void *)&vectors_num, sizeof(int), 1);
  for (int i = 0; i < vectors_num; ++i) {
    struct VectorInfo vi;
    FReadByteArray(fio, vi.name);
    fio->Read((void *)&vi.data_type, sizeof(vi.data_type), 1);
    fio->Read((void *)&vi.is_index, sizeof(vi.is_index), 1);
    fio->Read((void *)&vi.dimension, sizeof(vi.dimension), 1);
    FReadByteArray(fio, vi.store_type);
    FReadByteArray(fio, vi.store_param);
    if (vi.store_param == kPlaceHolder) {
      vi.store_param = "";
    }
    table.AddVectorInfo(vi);
  }
}

void TableSchemaIO::ReadIndexType(TableInfo &table) {
  FReadByteArray(fio, table.IndexType());
}

void TableSchemaIO::ReadIndexParams(TableInfo &table) {
  FReadByteArray(fio, table.IndexParams());
}

}  // namespace vearch
