
#ifdef USE_SCANN


#include "common/gamma_common_data.h"
#include "gamma_index_vearch.h"
#include "vector/raw_vector.h"
#include "scann_api.h"
#include "util/log.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <iostream>

namespace tig_gamma {

REGISTER_MODEL(VEARCH, GammaVearchIndex);

struct VearchModelParams {
  int ncentroids;       // coarse cluster center number
  int nsubvector;       // number of sub cluster center
  std::string metric_type;
  double noise_shaping_threshold;
  bool reordering;
  int n_thread;

  VearchModelParams() {
    ncentroids = 2048;
    nsubvector = 64;
    metric_type = "DotProductDistance";
    noise_shaping_threshold = 0.2;
    reordering = false;
    n_thread = 0;
  }

  int Parse(const char *str) {
    utils::JsonParser jp;
    if (jp.Parse(str)) {
      LOG(ERROR) << "parse vearch retrieval parameters error: " << str;
      return -1;
    }

    int ncentroids;
    int nsubvector;

    // -1 as default
    if (!jp.GetInt("ncentroids", ncentroids)) {
      if (ncentroids < -1) {
        LOG(ERROR) << "invalid ncentroids =" << ncentroids;
        return -1;
      }
      if (ncentroids > 0) this->ncentroids = ncentroids;
    } else {
      LOG(ERROR) << "cannot get ncentroids for vearch, set it when create space";
      return -1;
    }

    if (!jp.GetInt("nsubvector", nsubvector)) {
      if (nsubvector < -1) {
        LOG(ERROR) << "invalid nsubvector =" << nsubvector;
        return -1;
      }
      if (nsubvector > 0) this->nsubvector = nsubvector;
    } else {
      LOG(ERROR) << "cannot get nsubvector for vearch, set it when create space";
      return -1;
    }

    if (!jp.GetDouble("ns_threshold", noise_shaping_threshold)) {
      LOG(INFO) << "vearch ns_threshold: " << noise_shaping_threshold;
    }

    std::string metric_type;
    bool use_reordering;

    if (!jp.GetString("metric_type", metric_type)) {
      if (strcasecmp("L2", metric_type.c_str()) &&
          strcasecmp("InnerProduct", metric_type.c_str())) {
        LOG(ERROR) << "invalid metric_type = " << metric_type;
        return -1;
      }
      if (!strcasecmp("L2", metric_type.c_str()))
        this->metric_type = "SquaredL2Distance";
      else
        this->metric_type = "DotProductDistance";
    }

    if (!jp.GetBool("reordering", use_reordering)) {
      if (use_reordering == true) {
        LOG(INFO) << "The raw vectors are stored inside vearch. Using vearch reordering.";
        reordering = true;
      }
    }

    if (!jp.GetInt("thread_num", n_thread)) {
      if (n_thread > 1) {
        LOG(INFO) << "Use threadPool, thread_num[" << n_thread << "]";
      }
    }

    if (!Validate()) return -1;
    return 0;
  }

  bool Validate() {
    if (ncentroids <= 0 || nsubvector <= 0) return false;
    return true;
  }

  std::string ToString() {
    std::stringstream ss;
    ss << "ncentroids =" << ncentroids << ", ";
    ss << "nsubvector =" << nsubvector << ", ";
    ss << "metric_type =" << metric_type << ", ";
    ss << "ns_threshold =" << noise_shaping_threshold << ", ";
    ss << "vearch_reordering =" << reordering << ", ";

    return ss.str();
  }

  std::string GetConfig(int dimension, int indexing_size) {
    int num_children = ncentroids;
    int sample_size = indexing_size;
    int num_blocks = nsubvector;
    int num_dims_per_block = dimension / num_blocks;
    std::string partitioning_type = "GENERIC";
    std::string max_spill_centers = "1";
    std::string use_residual_quantization = "False";
    std::string use_global_topn = "False";
    std::string str_noise_shaping_threshold = std::to_string(noise_shaping_threshold);
    if (noise_shaping_threshold < 0) {
      str_noise_shaping_threshold = "nan";
    }
    if (metric_type == "DotProductDistance") {
      partitioning_type = "SPHERICAL";
      use_residual_quantization = "True";
      use_global_topn = "True";
    } else if (metric_type == "SquaredL2Distance") {
      str_noise_shaping_threshold = "nan";
    }

    std::string projection =
"    projection {\n\
        input_dim: " + std::to_string(dimension) + "\n\
        projection_type: CHUNK\n\
        num_blocks: " + std::to_string(num_blocks) + "\n\
        num_dims_per_block: " + std::to_string(num_dims_per_block) + "\n\
    }\n";
    int residual_dims = dimension % num_blocks;
    if (residual_dims > 0) {
      projection =
"    projection {\n\
        input_dim: " + std::to_string(dimension) + "\n\
        projection_type: VARIABLE_CHUNK\n\
        variable_blocks {\n\
            num_blocks: " + std::to_string(num_blocks) + "\n\
            num_dims_per_block: " + std::to_string(num_dims_per_block) + "\n\
        }\n\
        variable_blocks {\n\
            num_blocks: 1\n\
            num_dims_per_block: " + std::to_string(residual_dims) + "\n\
        }\n\
    }\n";
    }

    std::string str_config = "num_neighbors: 100\n\
distance_measure {\n\
    distance_measure: \"" + metric_type + "\"\n\
}\n\
partitioning {\n\
    num_children: " + std::to_string(num_children) + "\n\
    min_cluster_size: 50\n\
    max_clustering_iterations: 12\n\
    single_machine_center_initialization: RANDOM_INITIALIZATION\n\
    partitioning_distance {\n\
        distance_measure: \"SquaredL2Distance\"\n\
    }\n\
    query_spilling {\n\
        spilling_type: FIXED_NUMBER_OF_CENTERS\n\
        max_spill_centers: " + max_spill_centers + "\n\
    }\n\
    expected_sample_size: " + std::to_string(sample_size) + "\n\
    query_tokenization_distance_override {\n\
        distance_measure: \"" + metric_type + "\"\n\
    }\n\
    partitioning_type: " + partitioning_type + "\n\
    query_tokenization_type: FIXED_POINT_INT8\n\
}\n\
hash {\n\
    asymmetric_hash {\n\
        lookup_type: INT8_LUT16\n\
        use_residual_quantization: " + use_residual_quantization + "\n\
        use_global_topn: " + use_global_topn + "\n\
        quantization_distance {\n\
            distance_measure: \"SquaredL2Distance\"\n\
        }\n\
        num_clusters_per_block: 16\n" +
        projection + "\
        noise_shaping_threshold: " + str_noise_shaping_threshold + "\n\
        expected_sample_size: " + std::to_string(sample_size) + "\n\
        min_cluster_size: 100\n\
        max_clustering_iterations: 10\n\
    }\n\
}\n";
    if (reordering) {
      std::string reorder_config =
"exact_reordering {\n\
    approx_num_neighbors: 100\n\
    fixed_point {\n\
        enabled: False\n\
    }\n\
}";
      str_config += reorder_config;
    }
    return str_config;
  }

  int ToJson(utils::JsonParser &jp) { return 0; }
};


GammaVearchIndex::GammaVearchIndex(VectorReader *vec, const std::string &model_parameters) {
  indexed_count_ = 0;
}

GammaVearchIndex::GammaVearchIndex() {
  indexed_count_ = 0;
}

GammaVearchIndex::~GammaVearchIndex() {
  is_run_ = false;
  if (threads_.size() > 0) {
    for (size_t i = 0; i < threads_.size(); ++i) {
      threads_[i].join();
    }
    LOG(INFO) << "GammaIndex threadPool join, thread num:" << threads_.size();
  }
  if (vearch_index_) {
    ScannClose(vearch_index_);
  }
  if (model_param_) {
    delete model_param_;
    model_param_ = nullptr;
  }
}

void GammaVearchIndex::ThreadRun(int timeout) {
  std::shared_ptr<SearchTask> task;
  while (is_run_) {
    if (task_queue_.wait_dequeue_timed(task, timeout) > 0) {
      if (task->done_status_ != 0) continue;
      task->done_status_ = ScannSearch(vearch_index_, task->x_, task->n_, task->k_,
                                       task->k_, task->leaves_, task->results);
      task->Notify();
    }
  }
}

int GammaVearchIndex::CreateThreads(int thread_num) {
  if (thread_num <= 0) return 0;
  threads_.resize(thread_num);
  for (int i = 0; i < thread_num; ++i) {
    auto func_search = std::bind(&GammaVearchIndex::ThreadRun, this, 4 * (i + 1));
    threads_[i] = std::thread(func_search);
  }
  LOG(INFO) << "Create search thread, num:" << thread_num;
  return 0;
}

int GammaVearchIndex::Init(const std::string &model_parameters, int indexing_size) {
  indexing_size_ = indexing_size;
  if (model_param_) { delete model_param_; }
  model_param_ = new VearchModelParams();
  if (model_param_->Parse(model_parameters.c_str()) != 0) {
    return -1;
  }
  d_ = vector_->MetaInfo()->Dimension();
  LOG(INFO) << model_param_->ToString();
  std::string str_config = model_param_->GetConfig(d_, indexing_size_);
#ifndef PYTHON_SDK
  LOG(INFO) << str_config;
#endif // PYTHON_SDK

  vearch_index_ = ScannInit(str_config.c_str(), str_config.length());
  if (vearch_index_ == nullptr) {
    LOG(ERROR) << "VearchIndex Init failure.";
    return -1;
  } else {
    LOG(INFO) << "VearchIndex Init success.";
    return 0;
  }
}

RetrievalParameters *GammaVearchIndex::Parse(const std::string &parameters) {
if (parameters == "") {
    return new VearchRetrievalParameters(metric_type_);
  }

  utils::JsonParser jp;
  if (jp.Parse(parameters.c_str())) {
    LOG(ERROR) << "parse retrieval parameters error: " << parameters;
    return nullptr;
  }

  std::string metric_type;
  VearchRetrievalParameters *retrieval_params = new VearchRetrievalParameters();
  if (!jp.GetString("metric_type", metric_type)) {
    if (strcasecmp("L2", metric_type.c_str()) &&
        strcasecmp("InnerProduct", metric_type.c_str())) {
      LOG(ERROR) << "invalid metric_type = " << metric_type
                 << ", so use default value.";
    }
    if (!strcasecmp("L2", metric_type.c_str())) {
      retrieval_params->SetDistanceComputeType(DistanceComputeType::L2);
    } else {
      retrieval_params->SetDistanceComputeType(
          DistanceComputeType::INNER_PRODUCT);
    }
  } else {
    retrieval_params->SetDistanceComputeType(metric_type_);
  }

  int recall_num;
  int nprobe;

  if (!jp.GetInt("recall_num", recall_num)) {
    if (recall_num > 0) {
      retrieval_params->SetRecallNum(recall_num);
    }
  }

  if (!jp.GetInt("nprobe", nprobe)) {
    if (nprobe > 0) {
      retrieval_params->SetNprobe(nprobe);
    }
  }
  return retrieval_params;
}

int GammaVearchIndex::Indexing() {
  if (is_trained_) { return -1; }
  RawVector *raw_vec = dynamic_cast<RawVector *>(vector_);

  ScopeVectors headers;
  std::vector<int> lens;
  raw_vec->GetVectorHeader(0, indexing_size_, headers, lens);

  // merge vectors
  int raw_d = raw_vec->MetaInfo()->Dimension();
  const char *train_raw_vec = nullptr;
  int bytes_num = raw_d * indexing_size_ * sizeof(float);
  utils::ScopeDeleter<uint8_t> del_train_raw_vec;
  if (lens.size() == 1) {
    train_raw_vec = (const char *)headers.Get(0);
  } else {
    char *buf = new char[bytes_num];
    train_raw_vec = (const char *)buf;
    del_train_raw_vec.set((const uint8_t *)train_raw_vec);
    size_t offset = 0;
    for (size_t i = 0; i < headers.Size(); ++i) {
      memcpy((void *)(buf + offset), (void *)headers.Get(i),
             sizeof(float) * raw_d * lens[i]);
      offset += sizeof(float) * raw_d * lens[i];
    }
  }
  
  int ret = ScannTraining(vearch_index_, train_raw_vec, bytes_num,
                          raw_d, model_param_->n_thread);

  indexed_count_ = indexing_size_;
  is_trained_ = true;
  CreateThreads(model_param_->n_thread);
  LOG(INFO) << "vearch index trained successful ! ! !";
  return ret;
}

bool GammaVearchIndex::Add(int n, const uint8_t *vec) {
  if (is_trained_ == false) { return false; }
#ifdef PERFORMANCE_TESTING
  double t0 = utils::getmillisecs();
#endif
  const char *v = (const char *)vec;
  if (ScannAddIndex(vearch_index_, v, n * d_ * sizeof(float)) == 0) {
#ifdef PERFORMANCE_TESTING
    add_count_ += n;
    if (add_count_ >= 10000) {
      double t1 = utils::getmillisecs();
      LOG(INFO) << "vearch add time [" << (t1 - t0) / n << "]ms, count "
                << indexed_count_ + n;
      add_count_ = 0;
    }
#endif
    return true;
  } else {
    LOG(INFO) << "vearch add index failure";
    return false;
  }
}

int GammaVearchIndex::Update(const std::vector<int64_t> &ids,
                             const std::vector<const uint8_t *> &vecs) {
  if (is_trained_ == false) { return -1; }
  LOG(WARNING) << "vearch not support update.";
  return 0;
}

int GammaVearchIndex::Delete(const std::vector<int64_t> &ids) {
  if (is_trained_ == false) { return -1; }
  ++delete_num_;
  return 0;
}

int GammaVearchIndex::Search(RetrievalContext *retrieval_context, int n, const uint8_t *x,
                             int k, float *distances, int64_t *ids) {
  if (is_trained_ == false) return -1;

  int ret = -1;
#ifdef PYTHON_SDK
  int leaves = nprobe;
  const float *q = (const float *)x;
  if (rerank_ <= k) rerank_ = k;
  std::vector<std::vector<std::pair<uint32_t, float>>> results;
#else // else PYTHON_SDK
  VearchRetrievalParameters *retrieval_params =
      dynamic_cast<VearchRetrievalParameters *>(retrieval_context->RetrievalParams());
  int leaves = leaves_;
  if (retrieval_params && retrieval_params->nprobe_ > 0 &&
      retrieval_params->nprobe_ < model_param_->ncentroids) {
    leaves = retrieval_params->nprobe_;
  }
  
  const float *q = (const float *)x;
  int top = k;
  GammaSearchCondition *condition =
      dynamic_cast<GammaSearchCondition *>(retrieval_context);
  if (condition->range_query_result) { top *= 1.5; }
#endif  // PYTHON_SDK

#ifdef PERFORMANCE_TESTING
  retrieval_context->GetPerfTool().Perf("search prepare");
#endif

#ifdef PYTHON_SDK
  ret = ScannSearch(vearch_index_, q, n, k, rerank_, leaves, results);
  for (int i = 0; i < n; ++i) {
    int res_size = results[i].size();
    for (int j = 0; j < res_size; ++j) {
      float score = fabs(results[i][j].second);
      distances[i * k + j] = score;
      ids[i * k + j] = (int64_t)results[i][j].first;
    }
  }
#else // else PYTHON_SDK
  if (model_param_->n_thread > 0) {
    if (task_queue_.size_approx() > 3000) {
      LOG(INFO) << "task_queue.size_approx() > 3000. abandon";
      return -1;
    }
    std::shared_ptr<SearchTask> task = std::make_shared<SearchTask>(n, q, top, leaves);
    task_queue_.enqueue(task);
    ret = task->WaitForDone();
    if (ret != 0) {
      LOG(INFO) << "search wait timeout";
      return ret;
    }
    for (int i = 0; i < n; ++i) {
      int res_size = task->results[i].size();
      int count = 0;
      for (int j = 0; j < res_size; ++j) {
        if (condition->IsValid(task->results[i][j].first)) {
          float score = fabs(task->results[i][j].second);
          if (count >= k || score < condition->min_score ||
              score > condition->max_score) { break; }
          distances[i * k + count] = score;
          ids[i * k + count] = (int64_t)(task->results[i][j].first);
          ++count;
        }
      }
    }
  } else {
    std::vector<std::vector<std::pair<uint32_t, float>>> results;
    ret = ScannSearch(vearch_index_, q, n, top, top, leaves, results);
    for (int i = 0; i < n; ++i) {
      int res_size = results[i].size();
      int count = 0;
      for (int j = 0; j < res_size; ++j) {
        if (condition->IsValid(results[i][j].first)) {
          float score = fabs(results[i][j].second);
          if (count >= k || score < condition->min_score ||
              score > condition->max_score) { break; }
          distances[i * k + count] = score;
          ids[i * k + count] = (int64_t)(results[i][j].first);
          ++count;
        }
      }
    }
  }
#endif  // PYTHON_SDK
#ifdef PERFORMANCE_TESTING
  retrieval_context->GetPerfTool().Perf("search");
#endif
  return ret;
}

long GammaVearchIndex::GetTotalMemBytes() {

  return 0;
}

int GammaVearchIndex::Dump(const std::string &dir) {

  return 0;
}

int GammaVearchIndex::Load(const std::string &index_dir) {

  return 0;
}

}
#endif

