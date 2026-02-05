#include "memory_buffer_raw_vector.h"
#include "common/gamma_common_data.h"
#include <string>
#include "util/utils.h"

namespace vearch {

  MemoryBufferRawVector::MemoryBufferRawVector(VectorMetaInfo *meta_info, const StoreParams &store_params,
                           bitmap::BitmapManager *docids_bitmap, StorageManager *storage_mgr,
                           int cf_id)
    : MemoryRawVector(meta_info, store_params, docids_bitmap, storage_mgr, cf_id), 
    start_segment_id_(0), indexed_count_(0), last_expired_segment_id_(0), add_count_(0) {
    segment_size_ = DEFAULT_MEMORY_BUFFER_SEGMENT_SIZE;
  }

  MemoryBufferRawVector::~MemoryBufferRawVector() {
  for (int i = 0; i < nsegments_; ++i) {
    delete[] segments_[i]; // Free each segment
  }
  delete[] segments_; // Free the array of segment pointers
  delete[] segment_ref_counts_;
}

int MemoryBufferRawVector::InitStore(std::string &vec_name) {
  // vector buffer no need to dump to disk
  meta_info_->with_io_ = false;
  segments_ = new (std::nothrow) uint8_t *[kMaxMemoryBufferSegments];
  if (segments_ == nullptr) {
    LOG(ERROR) << desc_
               << "malloc new segment failed, segment size=" << segment_size_;
    return -1;
  }
  std::fill_n(segments_, kMaxMemoryBufferSegments, nullptr);

  segment_nums_ = new (std::nothrow) std::atomic<uint32_t>[kMaxMemoryBufferSegments];
  if (segment_nums_ == nullptr) {
    LOG(ERROR) << desc_
               << "malloc new segment failed, segment size=" << segment_size_;
    return -2;
  }
  std::fill_n(segment_nums_, kMaxMemoryBufferSegments, 0);

  segment_ref_counts_ = new std::atomic<int>[kMaxMemoryBufferSegments]();
  if (segment_ref_counts_ == nullptr) {
    LOG(ERROR) << desc_
               << "malloc new segment ref counts failed, segment size=" << segment_size_;
    return -3;
  }

  LOG(INFO) << desc_ << "init memory buffer raw vector success! vector byte size="
            << vector_byte_size_ << ", vector name=" + meta_info_->Name() << ", vector size="
            << meta_info_->Size();
  return 0;
}

int MemoryBufferRawVector::SetStartSegmentId(int indexed_count) {
  if (indexed_count < indexed_count_ || indexed_count > MetaInfo()->Size()) {
    LOG(WARNING) << desc_ << "SetStartSegmentId indexed_count=" << indexed_count
                 << " greater than current vector size=" << MetaInfo()->Size()
                 << ", indexed_count_=" << indexed_count_
                 << ", start_segment_id_=" << start_segment_id_
                 << ", nsegments_=" << nsegments_;
    return -1;
  }

  add_count_ += indexed_count - indexed_count_;
  indexed_count_ = indexed_count;
  start_segment_id_ = indexed_count / segment_size_;

  if (last_expired_segment_id_ <= start_segment_id_ - 1 && segment_ref_counts_[last_expired_segment_id_].load() >= 0) {
    ReleaseExpiredSegments(start_segment_id_ - 1);
  }
  if (start_segment_id_ > 0) {
    last_expired_segment_id_ = start_segment_id_ - 1;
  }

  if (add_count_ >= ADD_COUNT_THRESHOLD) {
    LOG(DEBUG) << desc_ << "indexed_count_=" << indexed_count_
              << ", start_segment_id_=" << start_segment_id_
              << ", last_expired_segment_id_=" << last_expired_segment_id_
              << ", nsegments_=" << nsegments_
              << ", indexed_count_=" << indexed_count_;
    add_count_ = 0;
  }

  return 0;
}

int MemoryBufferRawVector::GetStartSegmentId() {
  return start_segment_id_;
}

int MemoryBufferRawVector::GetNsegments() {
  return nsegments_;
}

int MemoryBufferRawVector::GetIndexedCount() {
  return indexed_count_;
}

void MemoryBufferRawVector::IncrementSegmentRef(int segment_id) {
  if (segment_id >= 0 && segment_id < nsegments_) {
    segment_ref_counts_[segment_id].fetch_add(1, std::memory_order_relaxed);
  }
}

void MemoryBufferRawVector::DecrementSegmentRef(int segment_id) {
  if (segment_id >= 0 && segment_id < nsegments_) {
    segment_ref_counts_[segment_id].fetch_sub(1, std::memory_order_relaxed);
  }
}

int MemoryBufferRawVector::UpdateToStore(int64_t vid, uint8_t *v, int len) {
  // no need to update
  if (vid < indexed_count_) {
    return 0;
  }
  if (docids_bitmap_->Test(vid)) {
    return 0;
  }
  if (vid >= meta_info_->Size() || vid < 0) {
    return -1;
  }
  if (segment_ref_counts_[vid / segment_size_].load() >= 0) {
    memcpy((void *)(segments_[vid / segment_size_] +
      (size_t)vid % segment_size_ * vector_byte_size_),
      (void *)v, vector_byte_size_);
  }

  return 0;
}

int MemoryBufferRawVector::ExtendSegments() {
  if (nsegments_ >= kMaxMemoryBufferSegments) {
    LOG(ERROR) << this->desc_ << "segment number can't be > " << kMaxMemoryBufferSegments;
    return -1;
  }
  segments_[nsegments_] =
      new (std::nothrow) uint8_t[segment_size_ * vector_byte_size_];
  if (segments_[nsegments_] == nullptr) {
    LOG(ERROR) << this->desc_
               << "malloc new segment failed, segment num=" << nsegments_
               << ", segment size=" << segment_size_;
    return -1;
  }
  ++nsegments_;
  LOG(DEBUG) << desc_ << "extend segment success! nsegments=" << nsegments_;
  return 0;
}

Status MemoryBufferRawVector::Load(int64_t vec_indexed_count, int64_t vec_size, int64_t disk_vec_num) {
  if (vec_indexed_count > vec_size || vec_indexed_count < 0) {
    LOG(ERROR) << desc_ << " vec_indexed_count=" << vec_indexed_count << ", vec_size=" << vec_size;
    return Status::ParamError(desc_ + " invalid load range");
  }

  // initialize the start segment and parameters
  if (vec_indexed_count > 0) {
    indexed_count_ = vec_indexed_count;
    start_segment_id_ = vec_indexed_count / segment_size_;
    // nsegment should be start_segment_id_ + 1, because start_segment_id_ is the index of the segment, not the number of the segment
    // but ExtendSegments will increment nsegments_ by 1, so we need to set nsegments_ to start_segment_id_
    nsegments_ = start_segment_id_;
    int ret = ExtendSegments();
    if (ret != 0) {
      LOG(ERROR) << desc_ << "extend segments failed, vec_indexed_count=" << vec_indexed_count
                  << ", nsegments_=" << nsegments_
                  << ", segment_size_=" << segment_size_;
      return Status::IOError(desc_ + " extend segments failed");
    }
  }

  std::unique_ptr<rocksdb::Iterator> it = storage_mgr_->NewIterator(cf_id_);
  std::string start_key = utils::ToRowKey(vec_indexed_count);
  it->Seek(rocksdb::Slice(start_key));
  std::string end_key = utils::ToRowKey(vec_size);

  int64_t n_load = 0;
  for (; it->Valid() && it->key().compare(end_key) < 0; it->Next()) {
    rocksdb::Slice current_key = it->key();
    int64_t vid = utils::FromRowKey(current_key.ToString());
    if (vid < 0) {
      LOG(ERROR) << desc_ << "parse row key failed, key=" << current_key.ToString();
      continue;
    }
    rocksdb::Slice value = it->value();
    AddToMem(vid, (uint8_t *)value.data_, VectorByteSize());
    n_load++;
  }

  MetaInfo()->size_ = vec_size;
  LOG(INFO) << desc_ << "memory raw vector want to load from [" << vec_indexed_count
            << "] to [" << vec_size
            << "] real load [" << n_load << "]"
            << ", disk_vec_num=" << disk_vec_num
            << ", start_segment_id_=" << start_segment_id_
            << ", indexed_count_=" << indexed_count_
            << ", nsegment_=" << nsegments_;

  return Status::OK();
}

void FreeOldBucketPos(uint8_t *old_ptr) { delete[] old_ptr; old_ptr = nullptr; }

void MemoryBufferRawVector::ReleaseExpiredSegments(int end_segment_id) {
  if (end_segment_id >= nsegments_ || last_expired_segment_id_ < 0 || last_expired_segment_id_ >= nsegments_) {
    LOG(WARNING) << desc_
                 << "release expired segments invalid, end_segment_id="
                 << end_segment_id << ", nsegments_=" << nsegments_
                 << ", last_expired_segment_id_="
                 << last_expired_segment_id_;
    return;
  }
  for (int segment_no = last_expired_segment_id_; segment_no <= end_segment_id; segment_no++) {
    if (segment_ref_counts_[segment_no].load() == 0 && segments_[segment_no] != nullptr) {
      // delay free
      std::function<void(uint8_t *)> func_free =
          std::bind(&FreeOldBucketPos, std::placeholders::_1);
      utils::AsyncWait(1800000, func_free, segments_[segment_no]);
      // set for deleted
      segment_ref_counts_[segment_no].fetch_sub(1, std::memory_order_relaxed);
    }
  }
  LOG(DEBUG) << desc_ << "release expired segments from last_expired_segment_id_ "
            << last_expired_segment_id_ << ", to end_segment_id "
            << end_segment_id << ", nsegments_=" << nsegments_;
}

}  // namespace vearch
