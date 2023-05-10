#pragma once

#include <string>
#include <vector>
#include <functional>

namespace tig_gamma {

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
  double boost;
  int has_boost;
  std::string retrieval_type;
};

struct VectorResult {
  VectorResult() {
    n = 0;
    topn = 0;
    dists = nullptr;
    docids = nullptr;
    sources = nullptr;
    source_lens = nullptr;
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

    if (sources) {
      delete[] sources;
      sources = nullptr;
    }

    if (source_lens) {
      delete[] source_lens;
      source_lens = nullptr;
    }
  }

  bool init(int a, int b) {
    n = a;
    topn = b;
    dists = new float[n * topn];
    docids = new int64_t[n * topn];
    sources = new char *[n * topn];
    source_lens = new int[n * topn];
    total.resize(n, 0);
    idx.resize(n, -1);
    std::fill_n(dists, n * topn, 0.0);
    std::fill_n(docids, n * topn, -1);

    return true;
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

  int seek(const int &req_no, const int &docid, float &score, char *&source,
           int &len) {
    int ret = -1;
    int base_idx = req_no * topn;
    int &start_idx = idx[req_no];
    if (start_idx == -1) return -1;
    for (int i = base_idx + start_idx; i < base_idx + topn; i++) {
      if (docids[i] >= docid) {
        ret = docids[i];
        score = dists[i];
        source = sources[i];
        len = source_lens[i];

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
    std::function<int(int64_t *, float *, char **, int *, int, int)>
        paritition = [&](int64_t *docids, float *dists, char **sources,
                         int *source_lens, int low, int high) {
          long pivot = docids[low];
          float dist = dists[low];
          char *source = sources[low];
          int source_len = source_lens[low];

          while (low < high) {
            while (low < high && docids[high] >= pivot) {
              --high;
            }
            docids[low] = docids[high];
            dists[low] = dists[high];
            sources[low] = sources[high];
            source_lens[low] = source_lens[high];
            while (low < high && docids[low] <= pivot) {
              ++low;
            }
            docids[high] = docids[low];
            dists[high] = dists[low];
            sources[high] = sources[low];
            source_lens[high] = source_lens[low];
          }
          docids[low] = pivot;
          dists[low] = dist;
          sources[low] = source;
          source_lens[low] = source_len;
          return low;
        };

    std::function<void(int64_t *, float *, char **, int *, int, int)>
        quick_sort_by_docid = [&](int64_t *docids, float *dists, char **sources,
                                  int *source_lens, int low, int high) {
          if (low < high) {
            int pivot =
                paritition(docids, dists, sources, source_lens, low, high);
            quick_sort_by_docid(docids, dists, sources, source_lens, low,
                                pivot - 1);
            quick_sort_by_docid(docids, dists, sources, source_lens, pivot + 1,
                                high);
          }
        };

    for (int i = 0; i < n; ++i) {
      quick_sort_by_docid(docids + i * topn, dists + i * topn,
                          sources + i * topn, source_lens + i * topn, 0,
                          topn - 1);
    }
  }

  int n;
  int topn;
  float *dists;
  float *query;
  int64_t *docids;
  char **sources;
  int *source_lens;
  std::vector<int> total;
  std::vector<int> idx;
};

struct VectorDocField {
  std::string name;
  double score;
  char *source;
  int source_len;
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

  bool init(std::string *vec_names, int vec_num) {
    if (vec_num <= 0) {
      fields = nullptr;
      fields_len = 0;
      return true;
    }
    fields = new (std::nothrow) VectorDocField[vec_num];
    if (fields == nullptr) {
      return false;
    }
    for (int i = 0; i < vec_num; i++) {
      fields[i].name = vec_names[i];
    }
    fields_len = vec_num;
    return true;
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

  bool init(int n, std::string *vec_names, int vec_num) {
    topn = n;
    docs = new (std::nothrow) VectorDoc *[topn];
    if (!docs) {
      // LOG(ERROR) << "docs in CommonDocs init error!";
      return false;
    }
    for (int i = 0; i < n; i++) {
      docs[i] = new VectorDoc();
      if (!docs[i]->init(vec_names, vec_num)) {
        return false;
      }
    }
    return true;
  }

  int topn;
  int total;
  int results_count;

  VectorDoc **docs;
};

}
