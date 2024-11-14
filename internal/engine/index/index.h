#pragma once

#include <faiss/Index.h>

#ifdef OPT_IVFPQ_RELAYOUT
#include "index/impl/relayout/gamma_index_ivfpq_relayout.h"
#include "index/impl/relayout/x86/x86_gamma_index_ivfflat.h"
#else
#include "index/impl/gamma_index_ivfflat.h"
#include "index/impl/gamma_index_ivfpq.h"
#endif

#include "index/index_model.h"

#ifdef USE_SCANN
#include "index/impl/scann/gamma_index_vearch.h"
#endif  // USE_SCANN

#include "util/bitmap_manager.h"
#include "vector/raw_vector.h"

namespace vearch {

using idx_t = faiss::Index::idx_t;

/**
 * faiss_like index
 *
 */
class Index {
 public:
  /**
   * @brief Construct a new Gamma Faisslike Index object
   *
   * @param index_type : IVFFLAT, IVFPQ
   * @param d : dimension
   * @param metric : support METRIC_INNER_PRODUCT, METRIC_L2
   */
  Index();

  virtual ~Index();

  /**
   * @brief init index by json string
   *
   * @param index_param : example "{\"nprobe\" : 10, \"ncentroids\" : 256
   * ,\"nsubvector\" : 64}"
   * @return int
   */
  virtual Status init(const std::string &index_param) { return Status::OK(); };

  virtual Status init() { return Status::OK(); };

  virtual void train(idx_t n, const float *x){};

  virtual void add(idx_t n, const float *x){};

  virtual void search(idx_t n, const float *x, idx_t k, float *distances,
                      idx_t *labels){};

  virtual Status dump(const std::string &dir) { return Status::OK(); };

  virtual Status load(const std::string &dir, int64_t &load_num) {
    load_num = 0;
    return Status::OK();
  };

 protected:
  bitmap::BitmapManager *docids_bitmap_;
  RawVector *raw_vector_;
  std::string index_param;
  std::string vec_name;
};

#ifdef OPT_IVFPQ_RELAYOUT
class IndexIVFFlat : public x86GammaIndexIVFFlat, public Index {
#else
class IndexIVFFlat : public GammaIndexIVFFlat, public Index {
#endif
 public:
  IndexIVFFlat(faiss::Index *quantizer, size_t d, size_t nlist,
               faiss::MetricType metric = faiss::METRIC_L2);

  virtual ~IndexIVFFlat();

  Status init(const std::string &index_param) override;

  Status init() override;

  void train(idx_t n, const float *x) override;

  void add(idx_t n, const float *x) override;

  void search(idx_t n, const float *x, idx_t k, float *distances,
              idx_t *labels) override;

  Status dump(const std::string &dir) override;

  Status load(const std::string &dir, int64_t &load_num) override;
};

#ifdef OPT_IVFPQ_RELAYOUT
class IndexIVFPQ : public GammaIndexIVFPQRelayout, public Index {
#else
class IndexIVFPQ : public GammaIVFPQIndex, public Index {
#endif
 public:
  IndexIVFPQ(faiss::Index *quantizer, size_t d, size_t nlist, size_t M,
             size_t nbits_per_idx, faiss::MetricType metric = faiss::METRIC_L2);

  virtual ~IndexIVFPQ();

  Status init(const std::string &index_param) override;

  Status init() override;

  void train(idx_t n, const float *x) override;

  void add(idx_t n, const float *x) override;

  void search(idx_t n, const float *x, idx_t k, float *distances,
              idx_t *labels) override;

  Status dump(const std::string &dir) override;

  Status load(const std::string &dir, int64_t &load_num) override;
};

#ifdef USE_SCANN
class IndexScann : public GammaVearchIndex, public Index {
 public:
  IndexScann(size_t d, size_t nlist, size_t M,
             faiss::MetricType metric = faiss::METRIC_L2);

  virtual ~IndexScann();

  int init(const std::string &index_param);

  int init();

  void train(idx_t n, const float *x) override;

  void add(idx_t n, const float *x) override;

  void search(idx_t n, const float *x, idx_t k, float *distances,
              idx_t *labels);

  int dump(const std::string &dir);

  int load(const std::string &dir);
};
#endif  // USE_SCANN

Index *index_factory(int d, const char *description_in,
                     faiss::MetricType metric);

}  // namespace vearch
