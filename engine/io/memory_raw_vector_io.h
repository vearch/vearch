#ifdef WITH_ROCKSDB

#pragma once

#include <string>
#include "async_flush.h"
#include "vector/memory_raw_vector.h"
#include "vector/rocksdb_wrapper.h"
#include "raw_vector_io.h"

namespace tig_gamma {

struct MemoryRawVectorIO : public RawVectorIO, public AsyncFlusher {
  MemoryRawVector *raw_vector;
  RocksDBWrapper rdb;

  MemoryRawVectorIO(MemoryRawVector *raw_vector_)
      : AsyncFlusher(raw_vector_->MetaInfo()->Name()),
        raw_vector(raw_vector_) {}
  ~MemoryRawVectorIO() {}
  int Init() override;
  int Dump(int start_vid, int end_vid) override;
  int GetDiskVecNum(int &vec_num) override;
  int Load(int vec_num) override;
  int Update(int vid) override;

  int FlushOnce() override;

  int Put(int vid);
};

}  // namespace tig_gamma

#endif // WITH_ROCKSDB
