#pragma once

#include <memory>
#include <mutex>
#include <atomic>
#include <string>

#include "index/index_model.h"
#include "vector/raw_vector.h"
#include "diskann_static_params.h"

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

// DiskANN headers
#include <diskann/pq_flash_index.h>
#include <diskann/aligned_file_reader.h>
#include <diskann/linux_aligned_file_reader.h>

namespace vearch {

class GammaIndexDiskANNStatic : public IndexModel {
 public:
  GammaIndexDiskANNStatic();
  ~GammaIndexDiskANNStatic() override;

  Status Init(const std::string &model_parameters,
              int training_threshold) override;

  RetrievalParameters *Parse(const std::string &parameters) override;

  int Indexing() override;

  // support_increment_ is false: VectorManager does not invoke Add/Update/Delete
  // on this index during document writes (see SupportIncrement() in
  // vector_manager.cc). If still called with real work, Add returns false and
  // Update/Delete return -1; rebuild index to refresh on-disk state.
  bool Add(int n, const uint8_t *vec) override;

  int Update(const std::vector<int64_t> &ids,
             const std::vector<const uint8_t *> &vecs) override;

  int Delete(const std::vector<int64_t> &ids) override;

  int Search(RetrievalContext *retrieval_context, int n, const uint8_t *x,
             int k, float *distances, int64_t *ids) override;

  long GetTotalMemBytes() override;

  Status Dump(const std::string &dir) override;
  Status Load(const std::string &dir, int64_t &load_num) override;

  void Describe() override;

 private:
  Status BuildDiskIndex();
  Status LoadDiskIndex();
  Status WarmupCache();
  std::string GetIndexPrefix();
  diskann::Metric ToDiskANNMetric(DistanceComputeType type);

 private:
  std::shared_ptr<AlignedFileReader> reader_;
  std::unique_ptr<diskann::PQFlashIndex<float>> flash_index_;
  DiskANNStaticModelParams params_;
  int dimension_ = 0;

  std::atomic<bool> disk_index_ready_{false};
  int64_t indexed_vec_count_ = 0;

  std::string index_dir_;
  std::mutex build_mutex_;
};

}  // namespace vearch