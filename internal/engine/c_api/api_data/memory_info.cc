/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "memory_info.h"

namespace vearch {

MemoryInfo::MemoryInfo() { memory_info_ = nullptr; }

int MemoryInfo::Serialize(char **out, int *out_len) {
  flatbuffers::FlatBufferBuilder builder;
  auto table = gamma_api::CreateMemoryInfo(
      builder, table_mem_bytes_, index_mem_bytes_, vector_mem_bytes_,
      field_range_mem_bytes_, bitmap_mem_bytes_);
  builder.Finish(table);
  *out_len = builder.GetSize();
  *out = (char *)malloc(*out_len * sizeof(char));
  memcpy(*out, (char *)builder.GetBufferPointer(), *out_len);
  return 0;
}

void MemoryInfo::Deserialize(const char *data, int len) {
  memory_info_ =
      const_cast<gamma_api::MemoryInfo *>(gamma_api::GetMemoryInfo(data));

  table_mem_bytes_ = memory_info_->table_mem();
  index_mem_bytes_ = memory_info_->index_mem();
  vector_mem_bytes_ = memory_info_->vector_mem();
  field_range_mem_bytes_ = memory_info_->field_range_mem();
  bitmap_mem_bytes_ = memory_info_->bitmap_mem();
}

long MemoryInfo::TableMem() { return table_mem_bytes_; }

void MemoryInfo::SetTableMem(long table_mem) { table_mem_bytes_ = table_mem; }

long MemoryInfo::IndexMem() { return index_mem_bytes_; }

void MemoryInfo::SetIndexMem(long index_mem_bytes) {
  index_mem_bytes_ = index_mem_bytes;
}

long MemoryInfo::VectorMem() { return vector_mem_bytes_; }

void MemoryInfo::SetVectorMem(long vector_mem_bytes) {
  vector_mem_bytes_ = vector_mem_bytes;
}

long MemoryInfo::FieldRangeMem() { return field_range_mem_bytes_; }

void MemoryInfo::SetFieldRangeMem(long field_range_mem_bytes) {
  field_range_mem_bytes_ = field_range_mem_bytes;
}

long MemoryInfo::BitmapMem() { return bitmap_mem_bytes_; }

void MemoryInfo::SetBitmapMem(long bitmap_mem_bytes) {
  bitmap_mem_bytes_ = bitmap_mem_bytes;
}

}  // namespace vearch
