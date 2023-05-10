#include "scann/scann_ops/cc/scann.h"
#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <vector>
#include <unistd.h>
#include <numeric>
#include <stdio.h>
#include <chrono>
#include <iomanip>
#include "google/protobuf/text_format.h"
#include "scann/partitioning/kmeans_tree_partitioner_utils.h"
#include "scann/tree_x_hybrid/tree_ah_hybrid_residual.h"

using namespace research_scann;

size_t topk = 10, leaves = 50;
bool is_add = true, is_asyn_search = false, reordering = false;
std::string metric_type = "DotProductDistance"; // SquaredL2Distance  DotProductDistance
int num_children = 2048, num_blocks = 512, sample_size = 10 * 10000;


std::string ReadConfig(std::string path) {
  int fd = open(path.c_str(), O_RDONLY);
  if (fd <= 0) {
    std::cout << "ReadConfig() open[" << path << "] error\n";
    return "";
  }
  int len = lseek(fd, 0, SEEK_END);
  std::cout << "config byte len:" << len << "\n";
  char *conf = new char[len];
  pread(fd, conf, len, 0);
  close(fd);
  std::string config(conf, len);
  delete[] conf;
  return std::move(config);
}

std::string GetConfig(std::string metric_type, int dimension, int num_children,
                      int num_blocks, int sample_size, bool reordering = false) {
  int num_dims_per_block = dimension / num_blocks;
  std::string partitioning_type = "GENERIC";
  std::string use_residual_quantization = "False";
  std::string noise_shaping_threshold = "0.2";
  if (metric_type == "DotProductDistance") {
    partitioning_type = "SPHERICAL";
    use_residual_quantization = "True";
    noise_shaping_threshold = "nan";
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
        max_spill_centers: 80\n\
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
        use_global_topn: True\n\
        quantization_distance {\n\
            distance_measure: \"SquaredL2Distance\"\n\
        }\n\
        num_clusters_per_block: 16\n" + 
        projection + "\
        noise_shaping_threshold: " + noise_shaping_threshold + "\n\
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

bool GetFeature(std::string path, size_t d, std::vector<float> &data, size_t vec_size = 0) {
  int fd = open(path.c_str(), O_RDONLY);
  if (fd <= 0) {
    std::cout << "GetData() open[" << path << "] error\n";
    return false;
  }
  size_t len = lseek(fd, 0, SEEK_END);
  if (vec_size != 0) {
    size_t tmp_len = vec_size * d * sizeof(float);
    len = tmp_len > len? len : tmp_len;
  }
  std::cout << "data byte len:" << len << "\n";
  data.resize(len / sizeof(float));
  len = (len / sizeof(float)) * sizeof(float);
  size_t read_len = 0;
  char *p = (char *)data.data();
  while(read_len < len) {
    size_t once_read_len = pread(fd, p + read_len, len - read_len, read_len);
    read_len += once_read_len;
    std::cout << "complet len:" << read_len << std::endl;
  }
  std::cout << "vector_num:" << vec_size << "\n";
  close(fd);
  return true;
}

bool GetGroundTruth(std::string path, size_t topk, std::vector<std::vector<int>> &ground_truth) {
  int fd = open(path.c_str(), O_RDONLY);
  if (fd <= 0) {
    std::cout << "GetData() open[" << path << "] error\n";
    return false;
  }
  size_t len = lseek(fd, 0, SEEK_END);
  size_t count = len / (sizeof(int) * topk);
  ground_truth.resize(count);
  for (size_t i = 0; i < count; ++i) {
    ground_truth[i].resize(topk);
    pread(fd, ground_truth[i].data(), sizeof(int) * topk, sizeof(int) * topk * i);
  }
  std::cout << "complet read GroundTruth\n";
  return true;
}

bool GetGroundScore(std::string path, size_t topk, std::vector<std::vector<float>> &ground_score) {
  int fd = open(path.c_str(), O_RDONLY);
  if (fd <= 0) {
    std::cout << "GetData() open[" << path << "] error\n";
    return false;
  }
  size_t len = lseek(fd, 0, SEEK_END);
  size_t count = len / (sizeof(float) * topk);
  ground_score.resize(count);
  for (size_t i = 0; i < count; ++i) {
    ground_score[i].resize(topk);
    pread(fd, ground_score[i].data(), sizeof(float) * topk, sizeof(float) * topk * i);
  }
  std::cout << "complet read GroundScore\n";
  return true;
}

template <typename T>
Status ParseTextProto(T* proto, const std::string& proto_str) {
  ::google::protobuf::TextFormat::ParseFromString(proto_str, proto);
  return OkStatus();
}

double CountDuration(std::chrono::steady_clock::time_point &start_time) {
  auto end_time = std::chrono::steady_clock::now();
  auto time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
  double duration = time_span.count();
  return duration;
}

float Evaluate(std::vector<std::vector<std::pair<DatapointIndex, float>>> &res,
               std::vector<std::vector<int>> &ground_truth,
               std::vector<std::vector<float>> &ground_score) {
  int corret_count = 0;
  int nq = res.size();
  int topk = res[0].size();
  int error_num = 0;
  for (size_t i = 0; i < nq; ++i) {
    std::stringstream ss;
    std::stringstream ss_gt;
    ss << " search res " << i << ":\t"; ss_gt << "groundtruth " << i << ":\t";
    for (size_t j = 0; j < topk; ++j) {
      ss << res[i][j].first << " " << std::setprecision(2) << res[i][j].second << "\t";
      ss_gt << ground_truth[i][j] << " " << std::setprecision(2) << ground_score[i][j] << "=\t";
      if (ground_truth[i][0] == res[i][j].first) { corret_count++; break; }
    }
    ss << "\n"; ss_gt << "\n";
    // std::cout << ss_gt.str();std::cout << ss.str();
    if (res[i][0].second + 1 > 0.05) {
      // std::cout << ss.str();
      error_num++;
    }
  }
  float recall = (float)corret_count / (float)nq;
  std::cout << "recall_" << topk << ":" << recall << " error_num:" << error_num <<"\n";
  return recall;
}


int AddIndexImpl(research_scann::ScannInterface *scann, std::vector<float> *add_dataset, uint32_t dim) {
  uint32_t gap = 1000;
  uint32_t points_num = add_dataset->size() / dim;
  for (int i = 0; i < (points_num + gap - 1) / gap; ++i) {
    uint32_t end_vid = (i + 1) * gap < points_num ? (i + 1) * gap : points_num;
    std::vector<float> add_ds(add_dataset->begin() + i * gap * dim, add_dataset->begin() + end_vid  * dim);
    scann->AddBatched(add_ds, add_ds.size() / dim);
    // LOG(INFO) << "======Indexing======= all add point num:" << points_num << " complet_num:" << (i + 1) * gap;
    // sleep(1);
  }
  return 0;
}

int SearchingImpl(research_scann::ScannInterface *scann, std::vector<float> *queries,
                  uint32_t dim, std::vector<std::vector<int>> *ground_truth,
                  std::vector<std::vector<float>> *ground_score, uint32_t time) {
  for (int i = 0; i < time; ++i) {
    LOG(INFO) << "=========begin batch searching========= time:" << time << " id:" << i;
    auto queries_dataset = DenseDataset<float>(*queries, queries->size() / dim);
    std::vector<std::vector<std::pair<DatapointIndex, float>>> res(queries_dataset.size());
    Status starus = scann->SearchBatchedParallel(queries_dataset, MakeMutableSpan(res), topk, topk, leaves);
    Evaluate(res, *ground_truth, *ground_score);
    LOG(INFO) << "===========complet batch search==========";
  }
  LOG(INFO) << "SearchingImpl funtion leave";
  return 0;
}

void RunAddIndexThread(research_scann::ScannInterface *scann, std::vector<float> *add_dataset, uint32_t dim) {
  auto func_indexing = std::bind(&AddIndexImpl, scann, add_dataset, dim);
  std::thread t(func_indexing);
  t.detach();
  LOG(INFO) << "AddThread start run...";
}

void RunSearchThread(std::vector<std::thread> &threads, research_scann::ScannInterface *scann,
                     std::vector<float> *queries, uint32_t dim,
                     std::vector<std::vector<int>> *ground_truth, 
                     std::vector<std::vector<float>> *ground_score, uint32_t time) {
  for (int i = 0; i < threads.size(); ++i) {
    threads[i] = std::thread(SearchingImpl, scann, queries, dim, ground_truth, ground_score, time);
  }
}

int main(int argc, char** argv) {
  std::string conf_path, dataset_path, queries_path, gt_path, gs_path;
  size_t dim, nb, nq = 10000;
  if (argc != 8) {
    std::cout << "./main conf_path dataset_path queries_path gt_path gs_path dim nb\n";
    return 0;
  }
  else {
    conf_path = argv[1];
    dataset_path = argv[2];
    queries_path = argv[3];
    gt_path = argv[4];
    gs_path = argv[5];
    dim = atoi(argv[6]);
    nb = atoi(argv[7]);
  }

  std::vector<float> vec_dataset;
  GetFeature(dataset_path, dim, vec_dataset, nb);
  std::vector<std::vector<int>> ground_truth;
  int gt_topk = 100;
  GetGroundTruth(gt_path, gt_topk, ground_truth);
  std::vector<std::vector<float>> ground_score;
  GetGroundScore(gs_path, gt_topk, ground_score);
  std::vector<float> queries;
  GetFeature(queries_path, dim, queries, nq);

  research_scann::DatapointIndex n_points = nb;
  if (is_add) { n_points = nb / 2; }
  absl::Span<const float> span_dataset(vec_dataset.data(), dim * n_points);
  const std::string config = GetConfig(metric_type, dim, num_children, num_blocks, sample_size, reordering);

  auto begin_time = std::chrono::steady_clock::now();
  research_scann::ScannInterface scann_;
  int training_threads = 0;
  scann_.Initialize(span_dataset, n_points, config, training_threads);
  std::cout << "train time:" << CountDuration(begin_time) << "s\n";

  std::vector<std::thread> threads;
  if (is_asyn_search) {
    threads.resize(3);
    uint32_t impl_time = 50;
    RunSearchThread(threads, &scann_, &queries, dim, &ground_truth, &ground_score, impl_time);
  }

  if (is_add) {
    begin_time = std::chrono::steady_clock::now();
    std::vector<float> add_dataset(vec_dataset.begin() + 1 * dim * n_points,
                                   vec_dataset.begin() + 2 * dim * n_points);
    AddIndexImpl(&scann_, &add_dataset, dim);
    std::cout << "add" << n_points << " time:" << CountDuration(begin_time) << "s\n";
  }
  
  if (is_asyn_search) { for (auto &t : threads) { t.join(); } }

  {
    std::cout << "begin BatchedParalled search\n";
    auto queries_dataset = DenseDataset<float>(queries, nq);
    std::vector<std::vector<std::pair<DatapointIndex, float>>> res(nq);
    auto begin_time = std::chrono::steady_clock::now();
    Status starus = scann_.SearchBatchedParallel(queries_dataset, MakeMutableSpan(res), topk, topk * 10, leaves);
    std::cout << "SearchBatch" << nq << " time " << CountDuration(begin_time) << "s\n";
    Evaluate(res, ground_truth, ground_score);
  }
  
  {
    std::cout << "begin search\n";
    std::vector<std::vector<std::pair<DatapointIndex, float>>> res(nq);
    auto begin_time = std::chrono::steady_clock::now();
    for (int i = 0; i < res.size(); ++i) {
      DatapointPtr qurey_datapoint(nullptr, queries.data() + i * dim, dim, dim);
      scann_.Search(qurey_datapoint, &res[i], topk, topk, leaves);
      if (i % 1000 == 0) std::cout << "complete search num:" << i << "\n";
    }
    Evaluate(res, ground_truth, ground_score);
    std::cout << "Search" << nq << " time " << CountDuration(begin_time) << "s\n";
  }

  return 0;
}
