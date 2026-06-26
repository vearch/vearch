/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Modified works copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */


#include "gamma_index_diskann_static.h"
#include "diskann_static_raw_data_writer.h"
#include "index/reflector.h"
#include "util/utils.h"

#include <diskann/disk_utils.h>
#include <diskann/ann_exception.h>
#include <omp.h>
#include <algorithm>
#include <sstream>
#include <atomic>
#include <cstring>
#include <cstdio>
#include <memory>
#include <vector>

namespace vearch {

REGISTER_INDEX(DISKANN_STATIC, GammaIndexDiskANNStatic);
namespace {
constexpr const char *DiskANNStaticMetaFile = "diskann_static_meta.bin";

struct DiskANNAlignedFloatDeleter {
  void operator()(float *p) const noexcept {
    if (p != nullptr) {
      diskann::aligned_free(p);
    }
  }
};

using AlignedQueryBuffer =
    std::unique_ptr<float, DiskANNAlignedFloatDeleter>;
}  // namespace

GammaIndexDiskANNStatic::GammaIndexDiskANNStatic() {
}

GammaIndexDiskANNStatic::~GammaIndexDiskANNStatic() {
  if (flash_index_) {
    flash_index_.reset();
  }
  if (reader_) {
    reader_->close();
    reader_.reset();
  }
}

Status GammaIndexDiskANNStatic::Init(const std::string &model_parameters,
                                   int training_threshold) {
  RawVector *raw_vec = dynamic_cast<RawVector *>(vector_);
  if (raw_vec == nullptr) {
    return Status::ParamError("DISKANN_STATIC requires RawVector");
  }
  training_threshold_ = std::numeric_limits<int>::max();
  support_increment_ = false;
  dimension_ = vector_->MetaInfo()->Dimension();

  if (model_parameters != "") {
    Status status = params_.ParseFrom(model_parameters.c_str());
    if (!status.ok()) return status;
  }



  if (!raw_vec->storage_mgr_) {
    return Status::ParamError(
        "DISKANN_STATIC requires non-null storage_mgr_ for persistent index path");
  }
  const std::string &data_path = raw_vec->storage_mgr_->GetRootPath();
  std::string::size_type pos = data_path.rfind('/');
  std::string root_path =
      (pos != std::string::npos) ? data_path.substr(0, pos) : data_path;
  std::string diskann_parent = root_path + "/diskann_static";
  utils::make_dir(diskann_parent.c_str());
  index_dir_ = diskann_parent + "/" + vector_->MetaInfo()->Name();
  utils::make_dir(index_dir_.c_str());

  LOG(INFO) << "GammaIndexDiskANNStatic Init: dim=" << dimension_
            << ", R=" << params_.R << ", L=" << params_.L
            << ", search_dram=" << params_.search_dram_budget_gb << "GB"
            << ", build_dram=" << params_.build_dram_budget_gb << "GB"
            << ", beam_width=" << params_.beam_width
            << ", cache_nodes=" << params_.num_nodes_to_cache;

  return Status::OK();
}

RetrievalParameters *GammaIndexDiskANNStatic::Parse(
    const std::string &parameters) {
  if (parameters.empty()) {
    auto retrieval_params = std::make_unique<DiskANNStaticRetrievalParameters>();
    retrieval_params->SetDistanceComputeType(params_.metric_type);
    retrieval_params->beam_width = params_.beam_width;
    return retrieval_params.release();
  }

  utils::JsonParser jp;
  if (jp.Parse(parameters.c_str())) {
    LOG(ERROR) << "Parse retrieval parameters error: " << parameters;
    return nullptr;
  }

  DistanceComputeType type = params_.metric_type;
  std::string metric_type;
  if (!jp.GetString("metric_type", metric_type)) {
    if (!strcasecmp("L2", metric_type.c_str()))
      type = DistanceComputeType::L2;
    else if (!strcasecmp("InnerProduct", metric_type.c_str()))
      type = DistanceComputeType::INNER_PRODUCT;
    else if (!strcasecmp("Cosine", metric_type.c_str()))
      type = DistanceComputeType::Cosine;
  }

  DiskANNStaticRetrievalParameters default_retrieval_params;

  int l_search = default_retrieval_params.l_search;
  jp.GetInt("l_search", l_search);

  int beam_width = params_.beam_width;
  jp.GetInt("beam_width", beam_width);

  int use_reorder = 0;
  jp.GetInt("use_reorder_data", use_reorder);

  auto retrieval_params = std::make_unique<DiskANNStaticRetrievalParameters>(
      type, l_search > 0 ? l_search : default_retrieval_params.l_search,
      beam_width > 0 ? beam_width : default_retrieval_params.beam_width,
      use_reorder != 0);
  return retrieval_params.release();
}

diskann::Metric GammaIndexDiskANNStatic::ToDiskANNMetric(
    DistanceComputeType type) {
  switch (type) {
    case DistanceComputeType::INNER_PRODUCT:
      return diskann::Metric::INNER_PRODUCT;
    case DistanceComputeType::Cosine:
      return diskann::Metric::COSINE;
    default:
      return diskann::Metric::L2;
  }
}

std::string GammaIndexDiskANNStatic::GetIndexPrefix() {
  return index_dir_ + "/" + vector_->MetaInfo()->Name();
}

int GammaIndexDiskANNStatic::Indexing() {
  std::lock_guard<std::mutex> lock(build_mutex_);

  LOG(INFO) << "GammaIndexDiskANNStatic Indexing";
  Status status = BuildDiskIndex();
  if (!status.ok()) {
    LOG(ERROR) << "BuildDiskIndex failed: " << status.ToString();
    return -1;
  }

  status = LoadDiskIndex();
  if (!status.ok()) {
    LOG(ERROR) << "LoadDiskIndex failed: " << status.ToString();
    return -1;
  }

  status = WarmupCache();
  if (!status.ok()) {
    LOG(WARNING) << "WarmupCache failed: " << status.ToString();
  }

  disk_index_ready_ = true;
  indexed_count_ = indexed_vec_count_;
  LOG(INFO) << "DiskANN static index built successfully, "
            << indexed_vec_count_ << " vectors indexed";
  return 0;
}

// remove diskann intermediate files for diskann build
// this function is used to resolve bug of diskann build
static void RemoveDiskANNIntermediateFiles(const std::string &prefix) {
  const std::vector<std::string> suffixes = {
      "_pq_pivots.bin",
      "_pq_compressed.bin",
      "_mem.index",
      "_mem.index.data",
      "_mem.index.tags",
      "_disk.index",
      "_disk.index_medoids.bin",
      "_disk.index_centroids.bin",
      "_disk.index_pq_pivots.bin",
      "_disk.index_pq_compressed.bin",
      "_disk.index_max_base_norm.bin",
      "_prepped_base.bin",
      "_sample_data.bin",
      "_sample_ids.bin",
      "_raw_data.bin",
  };
  int removed = 0;
  for (const auto &suf : suffixes) {
    std::string path = prefix + suf;
    if (std::remove(path.c_str()) == 0) {
      removed++;
    }
  }
  if (removed > 0) {
    LOG(INFO) << "Cleaned " << removed
              << " stale DiskANN intermediate files at prefix: " << prefix;
  }
}

Status GammaIndexDiskANNStatic::BuildDiskIndex() {
  RawVector *raw_vec = dynamic_cast<RawVector *>(vector_);
  if (raw_vec == nullptr) {
    return Status::ParamError("vector_ is not RawVector");
  }

  int64_t num_vecs = raw_vec->GetVectorNum();
  if (num_vecs == 0) {
    return Status::ParamError("No vectors to index");
  }

  std::string index_prefix = GetIndexPrefix();
  RemoveDiskANNIntermediateFiles(index_prefix);

  std::string data_file = index_prefix + "_raw_data.bin";
  Status status = DiskANNRawDataWriter::WriteToFile(
      raw_vec, num_vecs, dimension_, data_file);
  LOG(INFO) << "BuildDiskIndex: WriteToFile status: " << status.ToString();
  if (!status.ok()) return status;

  LOG(INFO) << "Writer " << num_vecs << " vectors to " << data_file;

  std::string index_build_params =
      std::to_string(params_.R) + " " +
      std::to_string(params_.L) + " " +
      std::to_string(params_.search_dram_budget_gb) + " " +
      std::to_string(params_.build_dram_budget_gb) + " " +
      std::to_string(params_.num_threads) + " " +
      std::to_string(params_.disk_pq_bytes) + " " +
      std::to_string(params_.append_reorder_data ? 1 : 0) + " " +
      "0 0";

  diskann::Metric metric = ToDiskANNMetric(params_.metric_type);

  LOG(INFO) << "Building disk index with params: " << index_build_params;

  int ret = -1;
  std::string build_errmsg;
  try {
    ret = diskann::build_disk_index<float>(
        data_file.c_str(),
        index_prefix.c_str(),
        index_build_params.c_str(),
        metric,
        params_.use_opq);
  } catch (const diskann::ANNException &e) {
    build_errmsg = std::string("DiskANN build_disk_index threw ANNException: ") +
                   e.what();
  } catch (const std::bad_alloc &e) {
    build_errmsg = std::string("DiskANN build_disk_index out of memory: ") +
                   e.what() +
                   ". Try increasing build_dram_budget_gb (current=" +
                   std::to_string(params_.build_dram_budget_gb) + ").";
  } catch (const std::exception &e) {
    build_errmsg = std::string("DiskANN build_disk_index exception: ") + e.what();
  }

  if (!build_errmsg.empty()) {
    std::remove(data_file.c_str());
    RemoveDiskANNIntermediateFiles(index_prefix);
    return Status::IOError(build_errmsg);
  }

  if (ret != 0) {
    std::remove(data_file.c_str());
    RemoveDiskANNIntermediateFiles(index_prefix);
    return Status::IOError("build_disk_index failed with code " +
                           std::to_string(ret));
  }

  indexed_vec_count_ = num_vecs;
  std::remove(data_file.c_str());

  LOG(INFO) << "Disk index built, files at prefix: " << index_prefix;
  return Status::OK();
}

Status GammaIndexDiskANNStatic::LoadDiskIndex() {
  if (flash_index_) {
    flash_index_.reset();
  }
  if (reader_) {
    reader_->close();
    reader_.reset();
  }

  reader_.reset(new LinuxAlignedFileReader());

  std::string load_errmsg;
  try {
    diskann::Metric metric = ToDiskANNMetric(params_.metric_type);
    flash_index_ = std::make_unique<diskann::PQFlashIndex<float>>(
        reader_, metric);

    std::string index_prefix = GetIndexPrefix();
    int ret = flash_index_->load(params_.num_threads, index_prefix.c_str());
    if (ret != 0) {
      load_errmsg = "PQFlashIndex load failed, ret=" + std::to_string(ret);
    }
  } catch (const diskann::ANNException &e) {
    load_errmsg = std::string("PQFlashIndex load threw ANNException: ") + e.what();
  } catch (const std::bad_alloc &e) {
    load_errmsg = std::string("PQFlashIndex load out of memory: ") + e.what();
  } catch (const std::exception &e) {
    load_errmsg = std::string("PQFlashIndex load threw exception: ") + e.what();
  }

  if (!load_errmsg.empty()) {
    flash_index_.reset();
    return Status::IOError(load_errmsg);
  }

  LOG(INFO) << "PQFlashIndex loaded: " << flash_index_->get_num_points()
            << " points, dim=" << flash_index_->get_data_dim();
  return Status::OK();
}

Status GammaIndexDiskANNStatic::WarmupCache() {
  if (!flash_index_) {
    return Status::ParamError("flash_index_ is null");
  }

  uint32_t num_to_cache = params_.num_nodes_to_cache;
  if (num_to_cache == 0) return Status::OK();

  std::vector<uint32_t> node_list;
  try {
    flash_index_->cache_bfs_levels(num_to_cache, node_list);
    flash_index_->load_cache_list(node_list);
  } catch (const diskann::ANNException &e) {
    return Status::IOError(
        std::string("DiskANN cache warmup ANNException: ") + e.what());
  } catch (const std::bad_alloc &e) {
    return Status::IOError(
        std::string("DiskANN cache warmup out of memory: ") + e.what());
  } catch (const std::exception &e) {
    return Status::IOError(
        std::string("DiskANN cache warmup exception: ") + e.what());
  }

  LOG(INFO) << "Cached " << node_list.size() << " nodes for warm search";
  return Status::OK();
}

bool GammaIndexDiskANNStatic::Add(int n, const uint8_t *vec) {
  (void)vec;
  // DISKANN_STATIC is built only in Indexing(); support_increment_ is false.
  // VectorManager skips Add/Update/Delete on this index when !SupportIncrement()
  // (see vector_manager.cc). Normal document paths must not call Add here.
  return false;
}

int GammaIndexDiskANNStatic::Update(const std::vector<int64_t> &ids,
                                  const std::vector<const uint8_t *> &vecs) {
  (void)vecs;
  // Same contract as Add: not used on the document write path; on-disk index
  // is not updated incrementally.
  return -1;
}

int GammaIndexDiskANNStatic::Delete(const std::vector<int64_t> &ids) {
  // Same contract as Add: not used on the document write path; deletions are
  // filtered at search time. 
  return -1;
}

int GammaIndexDiskANNStatic::Search(RetrievalContext *retrieval_context,
                                  int n, const uint8_t *x, int k,
                                  float *distances, int64_t *labels) {
  if (retrieval_context == nullptr || x == nullptr || distances == nullptr ||
      labels == nullptr) {
    LOG(ERROR) << "DiskANN static Search got invalid null input";
    //InvalidArgument
    return -1;
  }

  const int nk = n * k;
  std::fill(distances, distances + nk, std::numeric_limits<float>::max());
  std::fill(labels, labels + nk, static_cast<int64_t>(-1));
  if (!disk_index_ready_ || !flash_index_) {
    return -1;
  }

  RetrievalParameters *base_params = retrieval_context->RetrievalParams();
  DiskANNStaticRetrievalParameters *params =
      dynamic_cast<DiskANNStaticRetrievalParameters *>(base_params);
  if (base_params != nullptr && params == nullptr) {
    LOG(WARNING) << "DiskANN static Search got unexpected retrieval params type, "
                    "fallback to model defaults";
  }

  DiskANNStaticRetrievalParameters default_retrieval_params;
  uint64_t l_search = params ? params->l_search : default_retrieval_params.l_search;
  uint32_t beam_width = params ? params->beam_width : params_.beam_width;
  bool use_reorder =
      params ? params->use_reorder_data : params_.append_reorder_data;

  const float *queries = reinterpret_cast<const float *>(x);
  size_t aligned_dim = ROUND_UP(dimension_, 8);

  float *raw_aligned = nullptr;
  try {
    diskann::alloc_aligned((void **)&raw_aligned,
                           aligned_dim * sizeof(float), 256);
  } catch (const std::exception &e) {
    LOG(ERROR) << "DiskANN alloc_aligned failed: " << e.what();
    return -2;
  }
  if (raw_aligned == nullptr) {
    LOG(ERROR) << "DiskANN alloc_aligned returned null";
    return -2;
  }
  AlignedQueryBuffer aligned_query(raw_aligned);

  int search_k = std::min(static_cast<int>(l_search), k * 3);

  try {
    for (int i = 0; i < n; i++) {
      memset(aligned_query.get(), 0, aligned_dim * sizeof(float));
      memcpy(aligned_query.get(), queries + i * dimension_,
             dimension_ * sizeof(float));

      std::vector<uint64_t> candidate_ids(search_k);
      std::vector<float> candidate_distances(search_k);

      flash_index_->cached_beam_search(
          aligned_query.get(), search_k, l_search,
          candidate_ids.data(), candidate_distances.data(),
          beam_width, use_reorder);

      int pos = 0;
      for (int j = 0; j < search_k && pos < k; j++) {
        int64_t id = static_cast<int64_t>(candidate_ids[j]);
        if (id < 0 || id >= indexed_vec_count_) continue;
        if (!retrieval_context->IsValid(id)) continue;
        if (!retrieval_context->IsSimilarScoreValid(candidate_distances[j])) continue;
        distances[i * k + pos] = candidate_distances[j];
        labels[i * k + pos] = id;
        pos++;
      }
    }
  } catch (const std::bad_alloc &e) {
    LOG(ERROR) << "DiskANN search out of memory: " << e.what();
    return -2;
  } catch (const diskann::ANNException &e) {
    LOG(ERROR) << "DiskANN cached_beam_search ANNException: " << e.what();
    return -1;
  } catch (const std::exception &e) {
    LOG(ERROR) << "DiskANN search exception: " << e.what();
    return -1;
  }
  return 0;
}

long GammaIndexDiskANNStatic::GetTotalMemBytes() {
  long mem = 0;
  if (flash_index_ && disk_index_ready_) {
    mem += indexed_vec_count_ * 64;
    mem += params_.num_nodes_to_cache * dimension_ * sizeof(float);
    mem += params_.num_nodes_to_cache * params_.R * sizeof(uint32_t);
  }
  return mem;
}

Status GammaIndexDiskANNStatic::Dump(const std::string &dir) {
  if (!disk_index_ready_) {
    std::string msg = "DiskANN static index not built, dump failed";
    LOG(ERROR) << msg;
    return Status::IndexNotTrained(msg);
  }

  std::string index_name = vector_->MetaInfo()->AbsoluteName();
  std::string dump_dir = dir + "/" + index_name;
  utils::make_dir(dump_dir.c_str());

  std::string meta_file = dump_dir + "/" + DiskANNStaticMetaFile;
  std::ofstream fout(meta_file, std::ios::binary);
  if (!fout.is_open()) {
    return Status::IOError("Cannot create meta file: " + meta_file);
  }
  fout.write(reinterpret_cast<const char *>(&indexed_vec_count_),
             sizeof(int64_t));

  uint32_t path_len = index_dir_.size();
  fout.write(reinterpret_cast<const char *>(&path_len), sizeof(uint32_t));
  fout.write(index_dir_.c_str(), path_len);
  fout.close();

  LOG(INFO) << "DiskANN static index metadata dumped to " << meta_file;
  return Status::OK();
}

Status GammaIndexDiskANNStatic::Load(const std::string &dir,
                                   int64_t &load_num) {
  std::string index_name = vector_->MetaInfo()->AbsoluteName();
  std::string load_dir = dir + "/" + index_name;
  std::string meta_file = load_dir + "/" + DiskANNStaticMetaFile;

  std::ifstream fin(meta_file, std::ios::binary);
  if (!fin.is_open()) {
    LOG(INFO) << "No DiskANN static meta file found, skip load";
    load_num = 0;
    return Status::OK();
  }

  fin.read(reinterpret_cast<char *>(&indexed_vec_count_), sizeof(int64_t));

  uint32_t path_len;
  fin.read(reinterpret_cast<char *>(&path_len), sizeof(uint32_t));
  index_dir_.resize(path_len);
  fin.read(&index_dir_[0], path_len);
  fin.close();

  Status status = LoadDiskIndex();
  if (!status.ok()) return status;

  status = WarmupCache();
  if (!status.ok()) {
    LOG(WARNING) << "Cache warmup failed: " << status.ToString();
  }

  disk_index_ready_ = true;
  indexed_count_ = indexed_vec_count_;
  load_num = indexed_vec_count_;
  LOG(INFO) << "DiskANN static index loaded: " << indexed_vec_count_
            << " vectors";
  return Status::OK();
}

void GammaIndexDiskANNStatic::Describe() {
  RawVector *raw_vec = dynamic_cast<RawVector *>(vector_);
  int64_t total = raw_vec ? raw_vec->MetaInfo()->Size() : 0;
  LOG(INFO) << "DiskANN static Index: "
            << "ready=" << disk_index_ready_
            << ", disk_indexed=" << indexed_vec_count_
            << ", total_stored=" << total
            << ", pending=" << (total - indexed_vec_count_)
            << ", dim=" << dimension_
            << ", R=" << params_.R
            << ", beam_width=" << params_.beam_width
            << ", cache_nodes=" << params_.num_nodes_to_cache;
}

}  // namespace vearch