#pragma once

#include <string>
#include <atomic>
#include "raw_vector.h"
#include "memory_raw_vector.h"

namespace vearch {

class MemoryBufferRawVector : public MemoryRawVector {
 public:
  MemoryBufferRawVector(VectorMetaInfo *meta_info, const StoreParams &store_params,
                        bitmap::BitmapManager *docids_bitmap, StorageManager *storage_mgr,
                        int cf_id);
  ~MemoryBufferRawVector();

  int InitStore(std::string &vec_name) override;

  int SetStartSegmentId(int indexed_count);

  int GetStartSegmentId();

  int GetNsegments();

  int GetIndexedCount();

  int GetSegmentSize() { return segment_size_; }

  int UpdateToStore(int64_t vid, uint8_t *v, int len) override;

  // Load data into segments from a given range
  Status Load(int64_t vec_indexed_count, int64_t vec_size, int64_t disk_vec_num);

  void IncrementSegmentRef(int segment_id);
  void DecrementSegmentRef(int segment_id);

private:
  void ReleaseExpiredSegments(int start_segment_id_);
  int ExtendSegments() override;

  std::atomic<int> start_segment_id_;          // Valid segment start ID
  std::atomic<int> indexed_count_;          
  std::atomic<int> *segment_ref_counts_;       // Reference counts for each segment, -1 as delete
  std::atomic<int> last_expired_segment_id_;   // Last expired segment ID
  int add_count_;
};

}  // namespace vearch
