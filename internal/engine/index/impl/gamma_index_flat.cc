/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This faiss source code is licensed under the MIT license.
 * https://github.com/facebookresearch/faiss/blob/master/LICENSE
 *
 *
 * The works below are modified based on faiss:
 * 1. Add the numeric field and bitmap filters in the process of searching
 *
 * Modified works copyright 2019 The Gamma Authors.
 *
 * The modified codes are licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 *
 */

#include "gamma_index_flat.h"

#include <type_traits>

#include "omp.h"
#include "common/gamma_common_data.h"
#include "vector/memory_raw_vector.h"
#include "vector/memory_buffer_raw_vector.h"

using idx_t = faiss::idx_t;

namespace vearch {

namespace {

using HeapForIP = faiss::CMin<float, idx_t>;
using HeapForL2 = faiss::CMax<float, idx_t>;

// Carries a heap comparator type into a generic lambda so the whole scan flow
// is instantiated once per metric with the metric branch resolved up front.
template <class T>
struct HeapTag {
  using type = T;
};

// Per-Search invariants threaded through the scan/score helpers. All fields
// are constant for the duration of one Search() call; varying inputs (xi,
// simi/idxi output buffers, the per-call scan range or candidate bitmap)
// stay as explicit arguments. The metric is not stored here: it is carried as
// the Heap template parameter so distance selection happens at compile time.
struct FlatScanCtx {
  RetrievalContext *retrieval_context;
  RawVector *raw_vec;
  int d;
  int k;
  int batch_size;
  const std::string &request_id;
  int partition_id;
};

// Score one batch of (already-fetched) vectors against xi and push the
// surviving ones into the top-k heap. Templated on the heap comparator; the
// metric follows from the comparator (CMin -> inner product, CMax -> L2), so
// the distance function is selected at compile time with no per-vector branch.
template <class Heap>
inline void ComputeScoreBatch(const FlatScanCtx &ctx, const float *xi,
                              const std::vector<int64_t> &vid_buf,
                              ScopeVectors &scope_vecs, float *simi,
                              idx_t *idxi) {
  for (size_t j = 0; j < vid_buf.size(); ++j) {
    const int64_t vid = vid_buf[j];
    const float *yi = reinterpret_cast<const float *>(scope_vecs.Get(j));
    if (yi == nullptr) continue;

    float dis;
    if constexpr (std::is_same_v<Heap, HeapForIP>) {
      dis = faiss::fvec_inner_product(xi, yi, ctx.d);
    } else {
      dis = faiss::fvec_L2sqr(xi, yi, ctx.d);
    }
    if (!ctx.retrieval_context->IsSimilarScoreValid(dis)) continue;

    if (Heap::cmp(simi[0], dis)) {
      faiss::heap_pop<Heap>(ctx.k, simi, idxi);
      faiss::heap_push<Heap>(ctx.k, simi, idxi, dis, vid);
    }
  }
}

// Scan a contiguous VID range [start_vid, start_vid+nsearch); filter each VID
// through retrieval_context->IsValid before fetching. Used when no candidate
// bitmap is available (or filter-first is disabled).
template <class Heap>
void FlatScanRange(const FlatScanCtx &ctx, const float *xi, int start_vid,
                   int nsearch, float *simi, idx_t *idxi) {
  const int end_vid = start_vid + nsearch;
  std::vector<int64_t> vid_buf;
  vid_buf.reserve(ctx.batch_size);

  // Amortize is_killed() across multiple chunks: O(1)/chunk -> O(1)/16 chunks.
  constexpr int kKillCheckChunks = 16;
  int chunks_since_kill_check = 0;

  for (int chunk_start = start_vid; chunk_start < end_vid;
       chunk_start += ctx.batch_size) {
    if (chunks_since_kill_check++ >= kKillCheckChunks) {
      chunks_since_kill_check = 0;
      if (RequestContext::is_killed(ctx.request_id, ctx.partition_id)) {
        break;
      }
    }
    const int chunk_end = std::min(chunk_start + ctx.batch_size, end_vid);

    vid_buf.clear();
    for (int vid = chunk_start; vid < chunk_end; ++vid) {
      if (ctx.retrieval_context->IsValid(vid)) {
        vid_buf.push_back(vid);
      }
    }
    if (vid_buf.empty()) continue;

    ScopeVectors scope_vecs;
    int rc = ctx.raw_vec->Gets(vid_buf, scope_vecs);
    if (rc != 0) {
      LOG(ERROR) << "Gets failed, rc=" << rc << ", chunk=[" << chunk_start
                 << "," << chunk_end << ")";
      return;
    }

    ComputeScoreBatch<Heap>(ctx, xi, vid_buf, scope_vecs, simi, idxi);
  }
}

// Scan only the candidate bitmap (already clipped to this partition's VID
// range). Candidates already satisfy the scalar predicate, so we only check
// soft-delete via IsDeleted (skipping the redundant scalar Has() inside
// IsValid).
template <class Heap>
void FlatScanBitmap(const FlatScanCtx &ctx, const float *xi,
                    const roaring::Roaring64Map &cand, float *simi,
                    idx_t *idxi) {
  const uint64_t total = cand.cardinality();
  std::vector<int64_t> vid_buf;
  vid_buf.reserve(ctx.batch_size);

  constexpr int kKillCheckChunks = 16;
  int chunks_since_kill_check = 0;

  // Each call runs on one query (potentially under OMP); the iterator is
  // value-typed and thread-local, so the shared const bitmap is safe to read
  // concurrently.
  auto it = cand.begin();
  const auto end = cand.end();
  uint64_t consumed = 0;
  while (consumed < total) {
    if (chunks_since_kill_check++ >= kKillCheckChunks) {
      chunks_since_kill_check = 0;
      if (RequestContext::is_killed(ctx.request_id, ctx.partition_id)) break;
    }

    vid_buf.clear();
    for (int filled = 0; filled < ctx.batch_size && it != end;
         ++it, ++consumed) {
      const int64_t vid = static_cast<int64_t>(*it);
      if (!ctx.retrieval_context->IsDeleted(vid)) {
        vid_buf.push_back(vid);
        ++filled;
      }
    }
    if (vid_buf.empty()) continue;

    ScopeVectors scope_vecs;
    int rc = ctx.raw_vec->Gets(vid_buf, scope_vecs);
    if (rc != 0) {
      LOG(ERROR) << "Gets failed (scan_bitmap), rc=" << rc
                 << ", consumed=" << consumed << "/" << total;
      return;
    }

    ComputeScoreBatch<Heap>(ctx, xi, vid_buf, scope_vecs, simi, idxi);
  }
}

}  // namespace

REGISTER_INDEX(FLAT, GammaFLATIndex);

struct FLATModelParams {
  DistanceComputeType metric_type;

  FLATModelParams() { metric_type = DistanceComputeType::INNER_PRODUCT; }

  Status Parse(const char *str) {
    utils::JsonParser jp;
    if (jp.Parse(str)) {
      std::string msg =
          std::string("parse FLAT retrieval parameters error: ") + str;
      LOG(ERROR) << msg;
      return Status::ParamError(msg);
    }
    std::string metric_type;

    if (!jp.GetString("metric_type", metric_type)) {
      if (strcasecmp("L2", metric_type.c_str()) &&
          strcasecmp("InnerProduct", metric_type.c_str())) {
        std::string msg = std::string("invalid metric_type = ") + metric_type;
        LOG(ERROR) << msg;
        return Status::ParamError(msg);
      }
      if (!strcasecmp("L2", metric_type.c_str()))
        this->metric_type = DistanceComputeType::L2;
      else
        this->metric_type = DistanceComputeType::INNER_PRODUCT;
    }
    return Status::OK();
  }

  std::string ToString() {
    std::stringstream ss;
    ss << "metric_type =" << (int)metric_type;
    return ss.str();
  }
};

GammaFLATIndex::GammaFLATIndex() {}

GammaFLATIndex::~GammaFLATIndex() {}

Status GammaFLATIndex::Init(const std::string &model_parameters,
                            int training_threshold) {
  training_threshold_ = 0;
  FLATModelParams flat_param;
  if (model_parameters != "") {
    Status status = flat_param.Parse(model_parameters.c_str());
    if (!status.ok()) return status;
  }
  metric_type_ = flat_param.metric_type;
  LOG(INFO) << "FLAT Init OK, " << flat_param.ToString();
  return Status::OK();
}

RetrievalParameters *GammaFLATIndex::Parse(const std::string &parameters) {
  if (parameters == "") {
    return new FlatRetrievalParameters(metric_type_);
  }

  utils::JsonParser jp;
  if (jp.Parse(parameters.c_str())) {
    LOG(ERROR) << "parse retrieval parameters error: " << parameters;
    return nullptr;
  }

  std::string metric_type;
  DistanceComputeType type = metric_type_;
  if (!jp.GetString("metric_type", metric_type)) {
    if (strcasecmp("L2", metric_type.c_str()) &&
        strcasecmp("InnerProduct", metric_type.c_str())) {
      LOG(ERROR) << "invalid metric_type = " << metric_type
                 << ", so use default value.";
    }

    if (!strcasecmp("L2", metric_type.c_str())) {
      type = DistanceComputeType::L2;
    } else {
      type = DistanceComputeType::INNER_PRODUCT;
    }
  }

  int parallel_on_queries = 1;
  jp.GetInt("parallel_on_queries", parallel_on_queries);

  FlatRetrievalParameters *retrieval_params = new FlatRetrievalParameters(
      parallel_on_queries == 0 ? false : true, type);

  int disable_filter_first = 0;
  if (jp.GetInt("disable_filter_first", disable_filter_first) == 0) {
    retrieval_params->SetDisableFilterFirst(disable_filter_first != 0);
  }

  int fetch_batch_size = 0;
  if (jp.GetInt("fetch_batch_size", fetch_batch_size) == 0) {
    if (fetch_batch_size < 1 || fetch_batch_size > 1024) {
      LOG(WARNING) << "invalid fetch_batch_size=" << fetch_batch_size
                   << ", fallback to default 64";
    } else {
      retrieval_params->SetFetchBatchSize(fetch_batch_size);
    }
  }

  return retrieval_params;
}

int GammaFLATIndex::Indexing() { return 0; }

bool GammaFLATIndex::Add(int n, const uint8_t *vec) { return true; }

bool GammaFLATIndex::AcquireBufferScanRange(MemoryBufferRawVector *memory_buf,
                                            int total_vectors,
                                            int &start_offset_vid,
                                            int &num_vectors,
                                            int &start_segment_id,
                                            int &end_segment_id) {
  start_segment_id = memory_buf->GetStartSegmentId();
  end_segment_id = memory_buf->GetNsegments();
  start_offset_vid = memory_buf->GetIndexedCount();
  if (start_offset_vid >= total_vectors) {
    return false;
  }
  if (start_offset_vid < 0) {
    start_offset_vid = 0;
  }
  for (int i = start_segment_id; i < end_segment_id; i++) {
    memory_buf->IncrementSegmentRef(i);
  }
  num_vectors = total_vectors - start_offset_vid;
  return true;
}

void GammaFLATIndex::DecrementSegmentRefs(MemoryBufferRawVector *memory_buf,
                                          int start_segment_id,
                                          int end_segment_id) {
  if (memory_buf == nullptr) return;
  for (int i = start_segment_id; i < end_segment_id; i++) {
    memory_buf->DecrementSegmentRef(i);
  }
}

void GammaFLATIndex::PlanBitmapScan(
    RetrievalContext *retrieval_context,
    FlatRetrievalParameters *retrieval_params, int n, int start_offset_vid,
    int num_vectors, roaring::Roaring64Map &candidates,
    bool &scan_bitmap) {
  scan_bitmap = false;

  SearchCondition *condition =
      dynamic_cast<SearchCondition *>(retrieval_context);
  ScalarIndexResults *scalar_results =
      condition ? condition->ScalarIndexResult() : nullptr;
  const bool eligible = (scalar_results != nullptr) &&
                        (!retrieval_params->DisableFilterFirst()) &&
                        (scalar_results->Cardinality() > 0);
  if (!eligible) return;

  const roaring::Roaring64Map *src = scalar_results->GetCandidateBitmap();
  if (src == nullptr) return;

  const uint64_t lower = static_cast<uint64_t>(start_offset_vid);
  const uint64_t upper =
      static_cast<uint64_t>(start_offset_vid) + (uint64_t)num_vectors;
  roaring::Roaring64Map range_mask;
  range_mask.addRange(lower, upper);
  candidates = *src;
  candidates &= range_mask;

  const int64_t cand_card = candidates.cardinality();
  if (cand_card == 0) return;

  // When n == 1 and the candidate set is large, skip the
  // bitmap scan and let the dispatch take the "parallelize over vectors" path
  // so single-query searches (especially on Memory FLAT) regain inner-loop CPU
  // parallelism. The threshold matches a single fetch batch: below it, OMP
  // startup cost outweighs the per-vid IsValid work that the bitmap scan skips.
  constexpr int64_t kScanBitmapSingleQueryMax = 1024;
  const bool single_query_bailout =
      (n == 1) && (cand_card > kScanBitmapSingleQueryMax);
  scan_bitmap = !single_query_bailout;
}

int GammaFLATIndex::Search(RetrievalContext *retrieval_context, int n,
                           const uint8_t *x, int k, float *distances,
                           int64_t *labels) {
  FlatRetrievalParameters *retrieval_params =
      dynamic_cast<FlatRetrievalParameters *>(
          retrieval_context->RetrievalParams());
  utils::ScopeDeleter1<FlatRetrievalParameters> del_params;
  if (retrieval_params == nullptr) {
    retrieval_params = new FlatRetrievalParameters(true, metric_type_);
    del_params.set(retrieval_params);
  }

  auto raw_vec = dynamic_cast<RawVector *>(vector_);
  if (raw_vec == nullptr) {
    LOG(ERROR) << "raw vector is null";
    return -1;
  }

  const float *xq = reinterpret_cast<const float *>(x);
  if (xq == nullptr) {
    LOG(ERROR) << "search feature is null";
    return -1;
  }

  int num_vectors = vector_->MetaInfo()->Size();

  int start_offset_vid = 0;
  auto memory_buf = dynamic_cast<MemoryBufferRawVector *>(raw_vec);
  int start_segment_id = 0, end_segment_id = 0;
  if (memory_buf != nullptr &&
      !AcquireBufferScanRange(memory_buf, num_vectors, start_offset_vid,
                              num_vectors, start_segment_id, end_segment_id)) {
    return 0;
  }

  int d = vector_->MetaInfo()->Dimension();

  faiss::MetricType metric_type;
  if (retrieval_params->GetDistanceComputeType() ==
      DistanceComputeType::INNER_PRODUCT) {
    metric_type = faiss::METRIC_INNER_PRODUCT;
  } else {
    metric_type = faiss::METRIC_L2;
  }

  if (RequestContext::is_killed()) {
    DecrementSegmentRefs(memory_buf, start_segment_id, end_segment_id);
    return -2;
  }
  std::string request_id = RequestContext::get_current_request()->RequestId();
  int partition_id = RequestContext::get_partition_id();

  roaring::Roaring64Map candidates;
  bool scan_bitmap = false;
  PlanBitmapScan(retrieval_context, retrieval_params, n, start_offset_vid,
                 num_vectors, candidates, scan_bitmap);

  {
    // we must obtain the num of threads in *THE* parallel area.
    int num_threads = omp_get_max_threads();
    const FlatScanCtx ctx{
        retrieval_context, raw_vec, d, k, retrieval_params->FetchBatchSize(),
        request_id,        partition_id};

    bool parallel_on_queries =
        (retrieval_params->ParallelOnQueries() == true) && (n > 1);

    int threads_num = n < omp_get_max_threads() ? n : omp_get_max_threads();

    // Two orthogonal axes decide the scan:
    //   - scan source: scan_bitmap -> candidate bitmap, else full VID range.
    //   - parallel axis: over queries (one query per thread) vs over vectors
    //     (one VID sub-range per thread, merged under a critical section).
    // They are not fully independent: a candidate bitmap cannot be split into
    // contiguous VID sub-ranges, so scan_bitmap is pinned to the over-queries
    // axis. Over-vectors is therefore the only path that yields parallelism
    // when n == 1 -- which is exactly why PlanBitmapScan clears
    // scan_bitmap for large single queries (so they land here, not on a
    // single-threaded over-queries loop).
    const bool over_queries = scan_bitmap || parallel_on_queries;

    // Instantiated once per metric (Heap = CMin for IP, CMax for L2) so every
    // heap op and distance call below resolves at compile time.
    auto run_search = [&](auto heap_tag) {
      using Heap = typename decltype(heap_tag)::type;
      if (over_queries) {  // parallelize over queries
#pragma omp parallel for schedule(dynamic) num_threads(threads_num)
        for (int i = 0; i < n; i++) {
          if (RequestContext::is_killed(request_id, partition_id)) continue;
          const float *xi = xq + i * d;
          float *simi = distances + i * k;
          idx_t *idxi = (idx_t *)labels + i * k;

          faiss::heap_heapify<Heap>(k, simi, idxi);

          if (scan_bitmap) {
            FlatScanBitmap<Heap>(ctx, xi, candidates, simi, idxi);
          } else {
            FlatScanRange<Heap>(ctx, xi, start_offset_vid, num_vectors, simi,
                                idxi);
          }

          faiss::heap_reorder<Heap>(k, simi, idxi);
        }
      } else {  // parallelize over vectors

        size_t num_vectors_per_thread = num_vectors / num_threads;

        for (int i = 0; i < n; i++) {
          const float *xi = xq + i * d;

          // merge thread-local results
          float *simi = distances + i * k;
          idx_t *idxi = (idx_t *)labels + i * k;
          faiss::heap_heapify<Heap>(k, simi, idxi);

#pragma omp parallel for schedule(dynamic)
          for (int ik = 0; ik < num_threads; ik++) {
            if (!RequestContext::is_killed(request_id, partition_id)) {
              std::vector<idx_t> local_idx(k);
              std::vector<float> local_dis(k);
              faiss::heap_heapify<Heap>(k, local_dis.data(), local_idx.data());

              size_t ny = num_vectors_per_thread;

              if (ik == num_threads - 1) {
                ny += num_vectors % num_threads;  // the rest
              }

              int offset = start_offset_vid + ik * num_vectors_per_thread;

              FlatScanRange<Heap>(ctx, xi, offset, ny, local_dis.data(),
                                  local_idx.data());

#pragma omp critical
              {
                faiss::heap_addn<Heap>(k, simi, idxi, local_dis.data(),
                                       local_idx.data(), k);
              }
            }
          }

          if (RequestContext::is_killed(request_id, partition_id)) {
            break;
          }
          faiss::heap_reorder<Heap>(k, simi, idxi);
        }
      }
    };

    if (metric_type == faiss::METRIC_INNER_PRODUCT) {
      run_search(HeapTag<HeapForIP>{});
    } else {
      run_search(HeapTag<HeapForL2>{});
    }
  }  // parallel

  if (memory_buf != nullptr) {
    DecrementSegmentRefs(memory_buf, start_segment_id, end_segment_id);
  }

  if (RequestContext::is_killed(request_id, partition_id)) {
    return -2;
  }
#ifdef PERFORMANCE_TESTING
  if (retrieval_context->GetPerfTool()) {
    std::string compute_msg = "flat compute ";
    compute_msg += std::to_string(n);
    retrieval_context->GetPerfTool()->Perf(compute_msg);
  }
#endif  // PERFORMANCE_TESTING
  return 0;
}

long GammaFLATIndex::GetTotalMemBytes() { return 0; }

int GammaFLATIndex::Update(const std::vector<idx_t> &ids,
                           const std::vector<const uint8_t *> &vecs) {
  return 0;
}

Status GammaFLATIndex::Dump(const std::string &dir) { return Status::OK(); }

Status GammaFLATIndex::Load(const std::string &index_dir, int64_t &load_num) {
  load_num = 0;
  return Status::OK();
}

}  // namespace vearch
