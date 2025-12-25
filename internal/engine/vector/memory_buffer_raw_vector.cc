#include "memory_buffer_raw_vector.h"
#include "common/gamma_common_data.h"
#include <string>
#include "util/utils.h"

namespace vearch {

  MemoryBufferRawVector::MemoryBufferRawVector(VectorMetaInfo *meta_info, const StoreParams &store_params,
                           bitmap::BitmapManager *docids_bitmap, StorageManager *storage_mgr,
                           int cf_id)
    : MemoryRawVector(meta_info, store_params, docids_bitmap, storage_mgr, cf_id), 
    start_segment_id_(0), indexed_count_(0), last_expired_segment_id_(0) {
    segment_size_ = defautMemoryBufferSegmentSize;
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
            << vector_byte_size_ << ", " + meta_info_->Name();
  return 0;
}

int MemoryBufferRawVector::SetStartSegmentId(int vid) {
  if (vid < indexed_count_) {
    LOG(WARNING) << desc_ << "SetStartSegmentId vid=" << vid
                 << " less than current indexed_count_=" << indexed_count_;
    return -1;
  }
  if (vid >= MetaInfo()->Size()) {
    LOG(WARNING) << desc_ << "SetStartSegmentId vid=" << vid
                 << " greater than current vector size=" << MetaInfo()->Size();
    return -2;
  }
  indexed_count_ = vid;
  start_segment_id_ = vid / segment_size_;
  if (last_expired_segment_id_ <= start_segment_id_ - 1) {
    ReleaseExpiredSegments(start_segment_id_);
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

Status MemoryBufferRawVector::Load(int64_t start_vid, int64_t end_vid, int64_t &disk_vec_num) {
  if (start_vid >= end_vid) {
    LOG(ERROR) << desc_ << " start_id=" << start_vid << ", end_id=" << end_vid;
    return Status::ParamError(desc_ + " invalid load range");
  }

  // load from start_vid so should set start segment id
  SetStartSegmentId(start_vid);
  // set segment number according to start_vid
  nsegments_ = start_segment_id_;

  std::unique_ptr<rocksdb::Iterator> it = storage_mgr_->NewIterator(cf_id_);
  std::string start_key = utils::ToRowKey(start_vid);
  it->Seek(rocksdb::Slice(start_key));
  std::string end_key = utils::ToRowKey(end_vid);

  int64_t n_load = 0;
  for (auto c = start_vid; c < end_vid; c++, it->Next()) {
    rocksdb::Slice current_key = it->key();
    if (current_key.compare(end_key) >= 0) {
      break;
    }
    if (!it->Valid()) {
      LOG(ERROR) << desc_
                 << "memory vector load rocksdb iterator error, current_key="
                 << current_key.ToString() << ", start_key=" << start_key
                 << ", end_key=" << end_key << ", c=" << c;
      break;
    }
    rocksdb::Slice value = it->value();
    AddToMem(c, (uint8_t *)value.data_, VectorByteSize());
    n_load++;
  }

  MetaInfo()->size_ = end_vid;
  disk_vec_num = n_load;
  LOG(INFO) << desc_ << "memory raw vector want to load from [" << start_vid
            << "] to [" << end_vid
            << "real load [" << n_load << "]";

  return Status::OK();
}

void FreeOldBucketPos(uint8_t *old_ptr) { delete[] old_ptr; old_ptr = nullptr; }

void MemoryBufferRawVector::ReleaseExpiredSegments(int start_segment_id) {
  for (int segment_no = last_expired_segment_id_; segment_no < start_segment_id; segment_no++) {
    if (segments_[segment_no] != nullptr && segment_ref_counts_[segment_no].load() == 0) {
      // delay free
      std::function<void(uint8_t *)> func_free =
          std::bind(&FreeOldBucketPos, std::placeholders::_1);
      utils::AsyncWait(1800000, func_free, segments_[segment_no]);
    }
  }
  last_expired_segment_id_ = start_segment_id - 1;
}

}  // namespace vearch
