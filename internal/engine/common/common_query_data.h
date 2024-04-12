#pragma once

#include <cassert>
#include <functional>
#include <string>
#include <vector>
#include "util/log.h"
#include "util/status.h"
#include "cjson/cJSON.h"

namespace vearch {

struct TermFilter {
  std::string field;
  std::string value;
  int is_union;
};

struct RangeFilter {
  std::string field;
  std::string lower_value;
  std::string upper_value;
  bool include_lower;
  bool include_upper;
};

struct VectorQuery {
  std::string name;
  std::string value;
  double min_score;
  double max_score;
  std::string index_type;
};

struct VectorResult {
  VectorResult() {
    n = 0;
    topn = 0;
    dists = nullptr;
    docids = nullptr;
    total.resize(n);
    idx.resize(n);
    idx.assign(n, 0);
    total.assign(n, 0);
  }

  ~VectorResult() {
    if (dists) {
      delete[] dists;
      dists = nullptr;
    }

    if (docids) {
      delete[] docids;
      docids = nullptr;
    }
  }

  void init(int a, int b) {
    n = a;
    topn = b;
    dists = new float[n * topn];
    docids = new int64_t[n * topn];
    total.resize(n, 0);
    idx.resize(n, -1);
    std::fill_n(dists, n * topn, 0.0);
    std::fill_n(docids, n * topn, -1);
  }

  bool init(int a, int b, float *distances, int64_t *labels) {
    n = a;
    topn = b;
    dists = distances;
    docids = labels;
    std::fill_n(dists, n * topn, 0.0);
    std::fill_n(docids, n * topn, -1);

    return true;
  }

  int seek(const int &req_no, const int &docid, float &score) {
    int ret = -1;
    int base_idx = req_no * topn;
    int &start_idx = idx[req_no];
    if (start_idx == -1) return -1;
    for (int i = base_idx + start_idx; i < base_idx + topn; i++) {
      if (docids[i] >= docid) {
        ret = docids[i];
        score = dists[i];

        start_idx = i - base_idx;
        break;
      } else {
        continue;
      }
    }
    if (ret == -1) start_idx = -1;
    return ret;
  }

  void sort_by_docid() {
    std::function<int(int64_t *, float *, int, int)> paritition =
        [&](int64_t *docids, float *dists, int low, int high) {
          long pivot = docids[low];
          float dist = dists[low];

          while (low < high) {
            while (low < high && docids[high] >= pivot) {
              --high;
            }
            docids[low] = docids[high];
            dists[low] = dists[high];
            while (low < high && docids[low] <= pivot) {
              ++low;
            }
            docids[high] = docids[low];
            dists[high] = dists[low];
          }
          docids[low] = pivot;
          dists[low] = dist;
          return low;
        };

    std::function<void(int64_t *, float *, int, int)> quick_sort_by_docid =
        [&](int64_t *docids, float *dists, int low, int high) {
          if (low < high) {
            int pivot = paritition(docids, dists, low, high);
            quick_sort_by_docid(docids, dists, low, pivot - 1);
            quick_sort_by_docid(docids, dists, pivot + 1, high);
          }
        };

    for (int i = 0; i < n; ++i) {
      quick_sort_by_docid(docids + i * topn, dists + i * topn, 0, topn - 1);
    }
  }

  int n;
  int topn;
  float *dists;
  float *query;
  int64_t *docids;
  std::vector<int> total;
  std::vector<int> idx;
};

struct VectorDocField {
  std::string name;
  double score;
};

struct VectorDoc {
  VectorDoc() {
    docid = -1;
    score = 0.0f;
  }

  ~VectorDoc() {
    if (fields) {
      delete[] fields;
      fields = nullptr;
    }
  }

  void init(std::string *vec_names, int vec_num) {
    fields = new (std::nothrow) VectorDocField[vec_num];
    assert(fields != nullptr);
    for (int i = 0; i < vec_num; i++) {
      fields[i].name = vec_names[i];
    }
    fields_len = vec_num;
  }

  int docid;
  double score;
  struct VectorDocField *fields;
  int fields_len;
};

struct GammaResult {
  GammaResult() {
    topn = 0;
    total = 0;
    results_count = 0;
    docs = nullptr;
  }
  ~GammaResult() {
    if (docs) {
      for (int i = 0; i < topn; i++) {
        if (docs[i]) {
          delete docs[i];
          docs[i] = nullptr;
        }
      }
      delete[] docs;
      docs = nullptr;
    }
  }

  void init(int n, std::string *vec_names, int vec_num) {
    topn = n;
    docs = new (std::nothrow) VectorDoc *[topn];
    assert(docs != nullptr);

    for (int i = 0; i < n; i++) {
      docs[i] = new VectorDoc();
      docs[i]->init(vec_names, vec_num);
    }
  }

  int topn;
  int total;
  int results_count;

  VectorDoc **docs;
};

const std::string WeightedRanker_str = "WeightedRanker";

struct Ranker {
  Ranker(std::string str) : raw_str(str) {}

  virtual ~Ranker() {}

  virtual Status Parse() = 0;

  std::string ToString() {
    std::stringstream ss;
    ss << "ranker type =" << type << ", ";
    ss << "params: " << params;
    return ss.str();
  }
  std::string raw_str;
  std::string type;
  std::string params;
};

struct WeightedRanker : public Ranker {
  WeightedRanker(std::string ranker_parmas, int ranker_weight_num):
    Ranker(ranker_parmas), weights_num(ranker_weight_num), weights(ranker_weight_num, 1.0 / ranker_weight_num) {
  }

  virtual ~WeightedRanker() {}

  Status Parse() {
    Status status;
    std::string msg = "weighted ranker params err: " + std::string(raw_str);
    cJSON* jsonroot = cJSON_Parse(raw_str.c_str());
    if (jsonroot == NULL) {
      status = Status::InvalidArgument(msg);
      LOG(ERROR) << msg;
      return status;
    }
    cJSON * ctype = cJSON_GetObjectItemCaseSensitive(jsonroot, "type");
    if (ctype == NULL) {
      status = Status::InvalidArgument(msg);
      LOG(ERROR) << msg;
      return status;
    } else {
      type = ctype->valuestring;
    }
    cJSON * arr = cJSON_GetObjectItem(jsonroot, "params");
    if (arr == NULL) {
      status = Status::InvalidArgument(msg);
      LOG(ERROR) << msg;
      return status;
    }
    if (cJSON_IsArray(arr)) {
      int len = cJSON_GetArraySize(arr);
      if (len != weights_num) {
        msg = "weighted ranker params: " + std::string(raw_str) + ", length don't equal to " + std::to_string(weights_num);
        status = Status::InvalidArgument(msg);
        LOG(ERROR) << msg;
        return status;
      }
      for (int i = 0; i < len; i++) {
        cJSON * ArrNumEle = cJSON_GetArrayItem(arr, i);
        if(NULL == ArrNumEle) {
          continue;
        }
        weights[i] = ArrNumEle->valuedouble;
      }
    } else {
      status = Status::InvalidArgument(msg);
      LOG(ERROR) << msg;
      return status;
    }
    return Status::OK();
  }

  std::string ToString() {
    std::stringstream ss;
    ss << "ranker type =" << type << ", ";
    ss << "params: " << params << ", ";
    ss << "weights_num: " << weights_num << ",";
    for(int i = 0; i <  weights_num; i++)
      ss << "weight[" << i << "]=" << weights[i] << ",";
    return ss.str();
  }

  int weights_num;
  std::vector<double> weights;
};

}  // namespace vearch
