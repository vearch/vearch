/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "idl/fbs-gen/c/memory_info_generated.h"
#include "raw_data.h"

namespace vearch {

class MemoryInfo : public RawData {
 public:
  MemoryInfo();

  virtual int Serialize(char **out, int *out_len);

  virtual void Deserialize(const char *data, int len);

  long TableMem();

  void SetTableMem(long table_mem_bytes);

  long IndexMem();

  void SetIndexMem(long index_mem_bytes);

  long VectorMem();

  void SetVectorMem(long vector_mem_bytes);

  long FieldRangeMem();

  void SetFieldRangeMem(long field_range_mem_bytes);

  long BitmapMem();

  void SetBitmapMem(long bitmap_mem_bytes);

 private:
  gamma_api::MemoryInfo *memory_info_;

  long table_mem_bytes_;
  long index_mem_bytes_;
  long vector_mem_bytes_;
  long field_range_mem_bytes_;
  long bitmap_mem_bytes_;
};

}  // namespace vearch
