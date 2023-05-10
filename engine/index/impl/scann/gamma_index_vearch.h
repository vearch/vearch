#pragma once

#include "index/retrieval_model.h"
#include <condition_variable>
#include <atomic>
#include <memory>

#ifdef USE_SCANN

namespace tig_gamma {

using namespace moodycamel;
struct VearchModelParams;

class SearchTask {
 public:
  SearchTask(int n, const float *x, int k, int leaves)
      : x_(x) {
    n_ = n;
    k_ = k;
    leaves_ = leaves;
    done_status_ = 0;
  }

  void Notify() {
    cv_.notify_one();
  }

  int WaitForDone() {
    std::unique_lock<std::mutex> lck(mtx_);
    if (cv_.wait_for(lck, std::chrono::seconds(1)) == std::cv_status::timeout) {
      done_status_ = -2;
      return done_status_;
    }
    return done_status_;
  }

  int n_;
  const float *x_;
  int k_;
  int leaves_;
  std::vector<std::vector<std::pair<uint32_t, float>>> results;
  int done_status_;       // -1 search error, -2 timeout, 0 success

 private:
  std::condition_variable cv_;
  std::mutex mtx_;
};

class VearchRetrievalParameters : public RetrievalParameters {
 public:
  VearchRetrievalParameters() : RetrievalParameters() {
    recall_num_ = 100;
    nprobe_ = 80;
  }

  VearchRetrievalParameters(bool parallel_on_queries, int recall_num, int nprobe,
                            enum DistanceComputeType type, bool ivf_flat) {
    recall_num_ = recall_num;
    nprobe_ = nprobe;
  }

  VearchRetrievalParameters(enum DistanceComputeType type) {
    recall_num_ = 100;
    nprobe_ = 50;
  }

  virtual ~VearchRetrievalParameters() {}

  int RecallNum() { return recall_num_; }

  void SetRecallNum(int recall_num) { recall_num_ = recall_num; }

  int Nprobe() { return nprobe_; }

  void SetNprobe(int nprobe) { nprobe_ = nprobe; }

  int recall_num_;
  int nprobe_;
};

class GammaVearchIndex : public RetrievalModel {

 public:
  GammaVearchIndex(VectorReader *vec, const std::string &model_parameters);

  GammaVearchIndex();

  virtual ~GammaVearchIndex();

  int Init(const std::string &model_parameters, int indexing_size) override;

  RetrievalParameters *Parse(const std::string &parameters) override;

  int Indexing() override;

  bool Add(int n, const uint8_t *vec) override;

  int Update(const std::vector<int64_t> &ids,
             const std::vector<const uint8_t *> &vecs) override;

  int Delete(const std::vector<int64_t> &ids) override;

  int Search(RetrievalContext *retrieval_context, int n, const uint8_t *x,
             int k, float *distances, int64_t *ids) override;

  long GetTotalMemBytes() override;

  int Dump(const std::string &dir) override;
  int Load(const std::string &index_dir) override;

  void ThreadRun(int timeout);
  int CreateThreads(int thread_num);

#ifdef PYTHON_SDK
  int nprobe = 1;
  int rerank_ = 0;
#endif  // PYTHON_SDK

  int leaves_ = 512;                      // nprobe

  int pre_reorder_nn_ = 100;              // rank_num

  int final_nn_ = 100;                    // topk

  void *vearch_index_ = nullptr;

  bool is_trained_ = false;

  int d_ = 512;

  int delete_num_ = 0;

  DistanceComputeType metric_type_ = DistanceComputeType::INNER_PRODUCT;

  std::vector<std::thread> threads_;
  bool is_run_ = true;
  BlockingConcurrentQueue<std::shared_ptr<SearchTask>> task_queue_;

#ifdef PERFORMANCE_TESTING
  std::atomic<uint64_t> search_count_{0};
  int add_count_ = 0;
#endif

  VearchModelParams *model_param_ = nullptr;
};

}

#endif

