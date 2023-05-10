/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "gamma_engine_status.h"

namespace tig_gamma {

EngineStatus::EngineStatus() { engine_status_ = nullptr; }

int EngineStatus::Serialize(char **out, int *out_len) {
  flatbuffers::FlatBufferBuilder builder;
  auto table = gamma_api::CreateEngineStatus(
      builder, index_status_, table_mem_bytes_, index_mem_bytes_,
      vector_mem_bytes_, field_range_mem_bytes_, bitmap_mem_bytes_, doc_num_,
      max_docid_, min_indexed_num_);
  builder.Finish(table);
  *out_len = builder.GetSize();
  *out = (char *)malloc(*out_len * sizeof(char));
  memcpy(*out, (char *)builder.GetBufferPointer(), *out_len);
  return 0;
}

void EngineStatus::Deserialize(const char *data, int len) {
  engine_status_ =
      const_cast<gamma_api::EngineStatus *>(gamma_api::GetEngineStatus(data));

  index_status_ = engine_status_->index_status();
  table_mem_bytes_ = engine_status_->table_mem();
  index_mem_bytes_ = engine_status_->index_mem();
  vector_mem_bytes_ = engine_status_->vector_mem();
  field_range_mem_bytes_ = engine_status_->field_range_mem();
  bitmap_mem_bytes_ = engine_status_->bitmap_mem();

  doc_num_ = engine_status_->doc_num();
  max_docid_ = engine_status_->max_docid();
  min_indexed_num_ = engine_status_->min_indexed_num();
}

int EngineStatus::IndexStatus() { return index_status_; }

void EngineStatus::SetIndexStatus(int index_status) {
  index_status_ = index_status;
}

long EngineStatus::TableMem() { return table_mem_bytes_; }

void EngineStatus::SetTableMem(long table_mem) { table_mem_bytes_ = table_mem; }

long EngineStatus::IndexMem() { return index_mem_bytes_; }

void EngineStatus::SetIndexMem(long index_mem_bytes) {
  index_mem_bytes_ = index_mem_bytes;
}

long EngineStatus::VectorMem() { return vector_mem_bytes_; }

void EngineStatus::SetVectorMem(long vector_mem_bytes) {
  vector_mem_bytes_ = vector_mem_bytes;
}

long EngineStatus::FieldRangeMem() { return field_range_mem_bytes_; }

void EngineStatus::SetFieldRangeMem(long field_range_mem_bytes) {
  field_range_mem_bytes_ = field_range_mem_bytes;
}

long EngineStatus::BitmapMem() { return bitmap_mem_bytes_; }

void EngineStatus::SetBitmapMem(long bitmap_mem_bytes) {
  bitmap_mem_bytes_ = bitmap_mem_bytes;
}

int EngineStatus::DocNum() { return doc_num_; }

void EngineStatus::SetDocNum(int doc_num) { doc_num_ = doc_num; }

int EngineStatus::MaxDocID() { return max_docid_; }

void EngineStatus::SetMaxDocID(int docid) { max_docid_ = docid; }

}  // namespace tig_gamma
