#include "scann/scann_ops/cc/scann.h"
#include "scann/scann_ops/cc/scann_api.h"

using namespace research_scann;

void *ScannInit(const char *config_str, int len) {
  std::string config(config_str, len);
  research_scann::ScannInterface *scann = new research_scann::ScannInterface(config_str);
  return (void *)scann;
}

void ScannClose(void *scann) {
  research_scann::ScannInterface *s = static_cast<research_scann::ScannInterface *>(scann);
  delete s;
}

int ScannTraining(void *scann, const char *dataset, int len, int dim,
                  int training_threads) {
  research_scann::ScannInterface *s = 
        static_cast<research_scann::ScannInterface *>(scann);
  const float *vecs = (const float *)dataset;
  std::string config = s->str_config();
  uint32_t points = len / (sizeof(float) * dim);
  absl::Span<const float> span_dataset(vecs, points * dim);
  s->Initialize(span_dataset, points, config, training_threads);
  return 0;
}

int ScannAddIndex(void *scann, const char *dataset, int len) {
  research_scann::ScannInterface *s = 
        static_cast<research_scann::ScannInterface *>(scann);
  int dim = (int)s->dimensionality();
  int points_num = len / (sizeof(float) * dim);
  std::vector<float> add_ds(points_num * dim);
  memcpy(add_ds.data(), dataset, points_num * dim * sizeof(float));
  s->AddBatched(add_ds, points_num);
  return 0;
}

int ScannSearch(void *scann, const float *queries, int points_num,
                int final_nn, int pre_reorder_nn, int leaves,
                std::vector<std::vector<std::pair<uint32_t, float>>> &res) {
  research_scann::ScannInterface *s =
        static_cast<research_scann::ScannInterface *>(scann);
  int dim = (int)s->dimensionality();
  res.resize(points_num);
  if (points_num > 1) {
    std::vector<float> vec_queries(points_num * dim);
    memcpy(vec_queries.data(), queries, points_num * dim * sizeof(float));

    research_scann::DenseDataset<float> queries_dataset(vec_queries, points_num);
    s->SearchBatchedParallel(queries_dataset, MakeMutableSpan(res), final_nn,
                             pre_reorder_nn, leaves);
  } else {
    DatapointPtr qurey_datapoint(nullptr, queries, dim, dim);
    s->Search(qurey_datapoint, &res[0], final_nn, pre_reorder_nn, leaves);
  }
  return 0;
}
