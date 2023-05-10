/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include <fcntl.h>
#include <gtest/gtest.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <chrono>
#include <cmath>
#include <fstream>
#include <functional>
#include <future>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "c_api/api_data/gamma_config.h"
#include "c_api/api_data/gamma_docs.h"
#include "c_api/api_data/gamma_engine_status.h"
#include "c_api/api_data/gamma_request.h"
#include "c_api/api_data/gamma_batch_result.h"
#include "c_api/api_data/gamma_response.h"
#include "c_api/api_data/gamma_table.h"
#include "c_api/gamma_api.h"
#include "faiss/IndexFlat.h"
#include "faiss/IndexIVFPQ.h"
#include "faiss/index_io.h"
#include "faiss/utils/utils.h"
#include "gtest/gtest.h"
#include "util/bitmap.h"
#include "util/log.h"
#include "util/utils.h"

using std::string;

namespace {

string kIVFPQParam = R"(
    {
        "nprobe" : 10,
        "metric_type" : "L2",
        "ncentroids" : 256,
        "relayout_group_size" : 4,
        "nsubvector" : 64
    }
)";

string kIVFPQOPQParam = R"(
    {
        "nprobe" : 10,
        "metric_type" : "L2",
        "ncentroids" : 256,
        "relayout_group_size" : 4,
        "nsubvector" : 64,
        "opq" : {
            "nsubvector" : 64
        }
    }
)";

string kIVFHNSWPQParam = R"(
    {
        "nprobe" : 10,
        "metric_type" : "L2",
        "ncentroids" : 256,
        "relayout_group_size" : 4,
        "nsubvector" : 64,
        "hnsw" : {
            "nlinks" : 32,
            "efConstruction" : 160,
            "efSearch" : 64
        }
    }
)";

string kIVFHNSWOPQParam = R"(
    {
        "nprobe" : 10,
        "metric_type" : "L2",
        "ncentroids" : 256,
        "relayout_group_size" : 4,
        "nsubvector" : 64,
        "hnsw" : {
            "nlinks" : 32,
            "efConstruction" : 160,
            "efSearch" : 64
        },
        "opq" : {
            "nsubvector" : 64
        }
    }
)";

string kHNSWParam = R"(
    {
        "nlinks" : 32, 
        "metric_type" : "InnerProduct", 
        "efSearch" : 64,
        "efConstruction" : 160
    }
)";

string kFLATParam = R"(
    {
        "metric_type" : "InnerProduct"
    }
)";

string kIVFBINARYParam = R"(
    {
        "nprobe" : 10,
        "metric_type" : "L2",
        "ncentroids" : 256,
        "nsubvector" : 64
    }
)";

string kSCANNParam = R"(
    {
        "nprobe" : 10,
        "metric_type" : "InnerProduct",
        "ncentroids" : 256,
        "nsubvector" : 256
    }
)";

string kSCANNWithThreadPoolParam = R"(
    {
        "nprobe" : 10,
        "metric_type" : "InnerProduct",
        "ncentroids" : 256,
        "nsubvector" : 256,
        "thread_num" : 8
    }
)";

struct Options {
  Options() {
    nprobe = 10;
    doc_id = 0;
    d = 512;
    topK = 100;
    filter = false;
    print_doc = true;
    search_thread_num = 1;
    max_doc_size = 10000 * 100;
    add_doc_num = 10000 * 10;
    search_num = 100;
    indexing_size = 10000 * 1;
    fields_vec = {"_id", "img", "field1", "field2", "field3"};
    fields_type = {tig_gamma::DataType::STRING, tig_gamma::DataType::STRING,
                   tig_gamma::DataType::STRING, tig_gamma::DataType::INT,
                   tig_gamma::DataType::INT};
    vector_name = "abc";
    path = "files";
    log_dir = "log";
    model_id = "model";
    store_type = "MMap";
    // store_type = "RocksDB";
    profiles.resize(max_doc_size * fields_vec.size());
    engine = nullptr;
    add_type = 0;
    b_load = false;
  }

  int nprobe;
  size_t doc_id;
  size_t doc_id2;
  size_t d;
  int topK;
  size_t max_doc_size;
  size_t add_doc_num;
  size_t search_num;
  int indexing_size;
  bool filter;
  bool print_doc;
  int search_thread_num;
  std::vector<string> fields_vec;
  std::vector<tig_gamma::DataType> fields_type;
  string path;
  string log_dir;
  string vector_name;
  string model_id;
  string retrieval_type;
  string retrieval_param;
  string store_type;
  int add_type;  // 0 single add, 1 batch add

  std::vector<string> profiles;
  float *feature;

  string profile_file;
  string feature_file;
  char *docids_bitmap_;
  void *engine;
  bool b_load;
};

void printDoc(struct tig_gamma::ResultItem &result_item, std::string &msg,
              struct Options &opt) {
  msg += string("score [") + std::to_string(result_item.score) + "], ";
  for (size_t i = 0; i < result_item.names.size(); ++i) {
    std::string &name = result_item.names[i];
    tig_gamma::DataType data_type = tig_gamma::DataType::INT;
    for (size_t j = 0; j < opt.fields_vec.size(); ++j) {
      if (name == opt.vector_name) {
        data_type = tig_gamma::DataType::VECTOR;
        break;
      }
      if (name == opt.fields_vec[j]) {
        data_type = opt.fields_type[j];
        break;
      }
    }
    if (name == "float") {
      data_type = tig_gamma::DataType::FLOAT;
    }

    msg += "field name [" + name + "], type [" +
           std::to_string(static_cast<int>(data_type)) + "], value [";
    std::string &value = result_item.values[i];
    if (data_type == tig_gamma::DataType::INT) {
      msg += std::to_string(*((int *)value.data()));
    } else if (data_type == tig_gamma::DataType::LONG) {
      msg += std::to_string(*((long *)value.data()));
    } else if (data_type == tig_gamma::DataType::FLOAT) {
      msg += std::to_string(*((float *)value.data()));
    } else if (data_type == tig_gamma::DataType::DOUBLE) {
      msg += std::to_string(*((double *)value.data()));
    } else if (data_type == tig_gamma::DataType::STRING) {
      msg += value;
    } else if (data_type == tig_gamma::DataType::VECTOR) {
      std::string str_vec;
      int d = -1;
      memcpy((void *)&d, value.data(), sizeof(int));

      d /= sizeof(float);
      int cur = sizeof(int);

      const float *feature =
          reinterpret_cast<const float *>(value.data() + cur);

      cur += d * sizeof(float);
      int len = value.length();
      char source[len - cur];

      memcpy(source, value.data() + cur, len - cur);

      for (int i = 0; i < d; ++i) {
        str_vec += std::to_string(feature[i]) + ",";
      }
      str_vec.pop_back();

      std::string source_str = std::string(source, len - cur);
      msg += str_vec + "], source [" + source_str + "]";
    }
    msg += "], ";
  }
}

float *fvecs_read(const char *fname, size_t *d_out, size_t *n_out) {
  FILE *f = fopen(fname, "r");
  if (!f) {
    fprintf(stderr, "could not open %s\n", fname);
    perror("");
    abort();
  }
  int d;
  fread(&d, 1, sizeof(int), f);
  LOG(INFO) << "d = " << d;
  assert((d > 0 && d < 1000000) || !"unreasonable dimension");
  fseek(f, 0, SEEK_SET);
  struct stat st;
  fstat(fileno(f), &st);
  size_t sz = st.st_size;
  assert(sz % ((d + 1) * 4) == 0 || !"weird file size");
  size_t n = sz / ((d + 1) * 4);

  *d_out = d;
  *n_out = n;
  float *x = new float[n * (d + 1)];
  size_t nr = fread(x, sizeof(float), n * (d + 1), f);
  assert(nr == n * (d + 1) || !"could not read whole file");

  // shift array to remove row headers
  for (size_t i = 0; i < n; i++)
    memmove(x + i * d, x + 1 + i * (d + 1), d * sizeof(*x));

  fclose(f);
  return x;
}

float *fdat_read(const char *fname, size_t d, size_t *n_out) {
  FILE *f = fopen(fname, "r");
  if (!f) {
    fprintf(stderr, "could not open %s\n", fname);
    perror("");
    abort();
  }
  struct stat st;
  fstat(fileno(f), &st);
  size_t sz = st.st_size;
  assert(sz % (d * 4) == 0 || !"weird file size");
  size_t n = sz / (d * 4);

  *n_out = n;
  float *x = new float[n * d];
  size_t nr = fread(x, sizeof(float), n * d, f);
  assert(nr == n * d || !"could not read whole file");

  fclose(f);
  return x;
}

int CreateFile(std::string &path, bool truncate = true) {
  int flags = O_WRONLY | O_CREAT;
  if (truncate) {
    flags |= O_TRUNC;
  }
  int fd = open(path.c_str(), flags, 00777);
  assert(fd != -1);
  close(fd);
  return 0;
}

string GetCurrentCaseName() {
  const ::testing::TestInfo *const test_info =
      ::testing::UnitTest::GetInstance()->current_test_info();
  return string(test_info->test_case_name()) + "_" + test_info->name();
}

// void Sleep(long milliseconds) {
//  std::this_thread::sleep_for(std::chrono::milliseconds(milliseconds));
// }

struct RandomGenerator {
  RandomGenerator() {
    std::srand(std::time(nullptr));
    srand48(std::time(nullptr));
  }
  int Rand(int n, int offset = 0) { return std::rand() % n + offset; }
  double RandDouble(double offset = 0.0f) { return drand48() + offset; }
};

float random_float(float min, float max, unsigned int seed = 0) {
  static std::default_random_engine e(seed);
  static std::uniform_real_distribution<float> u(min, max);
  return u(e);
}

int TestMigrateDataImpl(struct Options &opt) {
  char *str_doc = nullptr;
  int n_migrage_doc = 0;
  int str_len = 0, is_del = 0;
  int fail_num = 0;
  BeginMigrate(opt.engine);
  while (fail_num < 100) {
    if (0 == GetMigrageDoc(opt.engine, &str_doc, &str_len, &is_del)) {
      tig_gamma::Doc doc;
      doc.SetEngine((tig_gamma::GammaEngine *)opt.engine);
      doc.Deserialize(str_doc, str_len);
      free(str_doc);
      str_doc = nullptr;
      if (++n_migrage_doc % 5000 == 0) {
        LOG(INFO) << "_id:" << doc.Key() << " is_delete:" << is_del
                  << " n_migrage_doc:" << n_migrage_doc;
      }
    } else {
      ++fail_num;
      sleep(0.1);
    }
  }
  LOG(INFO) << "Migrate complete, migrate doc num:" << n_migrage_doc;
  TerminateMigrate(opt.engine);
  return 0;
}

int TestMigrateThread(struct Options &opt) {
  auto func_Migrate_doc = std::bind(&TestMigrateDataImpl, opt);
  std::thread t(func_Migrate_doc);
  t.detach();
  return 0;
}

int AddDocToEngine(struct Options &opt, int doc_num, int interval = 0) {
  double cost = 0;
  for (int i = 0; i < doc_num; ++i) {
    tig_gamma::Doc doc;

    string url;
    for (size_t j = 0; j < opt.fields_vec.size(); ++j) {
      tig_gamma::Field field;
      field.name = opt.fields_vec[j];
      field.datatype = opt.fields_type[j];
      int len = 0;

      string &data =
          opt.profiles[(uint64_t)opt.doc_id * opt.fields_vec.size() + j];
      if (opt.fields_type[j] == tig_gamma::DataType::INT) {
        len = sizeof(int);
        int v = atoi(data.c_str());
        field.value = std::string((char *)(&v), len);
      } else if (opt.fields_type[j] == tig_gamma::DataType::LONG) {
        len = sizeof(long);
        long v = atol(data.c_str());
        field.value = std::string((char *)(&v), len);
      } else {
        // field.value = data + "\001all";
        field.value = data;
        url = data;
      }

      field.source = url;

      doc.AddField(std::move(field));
    }
    {
      tig_gamma::Field field;
      field.name = "float";
      field.datatype = tig_gamma::DataType::FLOAT;

      float f = random_float(0, 100);
      field.value = std::string((char *)(&f), sizeof(f));
      field.source = "url";
      doc.AddField(std::move(field));
    }

    tig_gamma::Field field;
    field.name = opt.vector_name;
    field.datatype = tig_gamma::DataType::VECTOR;
    field.source = url;
    int len = opt.d * sizeof(float);
    if (opt.retrieval_type == "BINARYIVF") {
      len = opt.d * sizeof(char) / 8;
    }

    field.value =
        std::string((char *)(opt.feature + (uint64_t)opt.doc_id * opt.d), len);

    doc.AddField(std::move(field));

    char *doc_str = nullptr;
    int doc_len = 0;
    doc.Serialize(&doc_str, &doc_len);
    double start = utils::getmillisecs();
    AddOrUpdateDoc(opt.engine, doc_str, doc_len);
    double end = utils::getmillisecs();
    cost += end - start;
    free(doc_str);
    ++opt.doc_id;
    if (i == (doc_num / 10) * 9) {
      TestMigrateThread(opt);
    }
    if (interval > 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(interval));
    }
  }
  LOG(INFO) << "AddDocToEngine cost [" << cost << "] ms";
  return 0;
}

int BatchAddDocToEngine(struct Options &opt, int doc_num, int interval = 0) {
  int batch_num = 10000;
  double cost = 0;
  int ret = 0;
  for (int i = 0; i < doc_num; i += batch_num) {
    tig_gamma::Docs docs;
    docs.Reserve(batch_num);
    for (int k = 0; k < batch_num; ++k) {
      tig_gamma::Doc doc;

      string url;
      for (size_t j = 0; j < opt.fields_vec.size(); ++j) {
        tig_gamma::Field field;
        field.name = opt.fields_vec[j];
        field.datatype = opt.fields_type[j];
        int len = 0;

        string &data =
            opt.profiles[(uint64_t)opt.doc_id * opt.fields_vec.size() + j];
        if (opt.fields_type[j] == tig_gamma::DataType::INT) {
          len = sizeof(int);
          int v = atoi(data.c_str());
          field.value = std::string((char *)(&v), len);
        } else if (opt.fields_type[j] == tig_gamma::DataType::LONG) {
          len = sizeof(long);
          long v = atol(data.c_str());
          field.value = std::string((char *)(&v), len);
        } else {
          // field.value = data + "\001all";
          field.value = data;
          url = data;
        }

        field.source = url;

        doc.AddField(std::move(field));
      }

      {
        tig_gamma::Field field;
        field.name = "float";
        field.datatype = tig_gamma::DataType::FLOAT;

        float f = random_float(0, 100);
        field.value = std::string((char *)(&f), sizeof(f));
        field.source = "url";
        doc.AddField(std::move(field));
      }

      tig_gamma::Field field;
      field.name = opt.vector_name;
      field.datatype = tig_gamma::DataType::VECTOR;
      field.source = url;
      int len = opt.d * sizeof(float);
      if (opt.retrieval_type == "BINARYIVF") {
        len = opt.d * sizeof(char) / 8;
      }

      field.value = std::string(
          (char *)(opt.feature + (uint64_t)opt.doc_id * opt.d), len);

      doc.AddField(std::move(field));
      docs.AddDoc(std::move(doc));
      ++opt.doc_id;
    }
    char **doc_str = nullptr;
    int doc_len = 0;

    docs.Serialize(&doc_str, &doc_len);
    double start = utils::getmillisecs();
    char *result_str = nullptr;
    int result_len = 0;
    AddOrUpdateDocs(opt.engine, doc_str, doc_len, &result_str, &result_len);
    tig_gamma::BatchResult result;
    result.Deserialize(result_str, 0);
    free(result_str);
    double end = utils::getmillisecs();
    cost += end - start;
    for (int i = 0; i < doc_len; ++i) {
      free(doc_str[i]);
    }
    free(doc_str);
    if (interval > 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(interval));
    }
  }
  LOG(INFO) << "BatchAddDocToEngine cost [" << cost << "] ms";
  return ret;
}

int SearchThread(struct Options &opt, size_t num) {
  size_t idx = 0;
  double time = 0;
  int failed_count = 0;
  int req_num = 1;
  string error;
  while (idx < num) {
    double start = utils::getmillisecs();
    struct tig_gamma::VectorQuery vector_query;
    vector_query.name = opt.vector_name;

    int len = opt.d * sizeof(float) * req_num;
    if (opt.retrieval_type == "BINARYIVF") {
      len = opt.d * sizeof(char) / 8 * req_num;
    }
    char *value = reinterpret_cast<char *>(opt.feature + (uint64_t)idx * opt.d);

    vector_query.value = std::string(value, len);

    vector_query.min_score = 0;
    vector_query.max_score = 10000;
    vector_query.boost = 0.1;
    vector_query.has_boost = 0;

    tig_gamma::Request request;
    request.SetTopN(10);
    request.AddVectorQuery(vector_query);
    request.SetReqNum(req_num);
    request.SetBruteForceSearch(0);
    request.SetHasRank(true);
    std::string retrieval_params =
        "{\"metric_type\" : \"InnerProduct\", \"recall_num\" : "
        "10, \"nprobe\" : 10, \"ivf_flat\" : 0}";
    request.SetRetrievalParams(retrieval_params);
    // request.SetOnlineLogLevel("");
    request.SetMultiVectorRank(0);
    request.SetL2Sqrt(false);

    if (opt.filter) {
      // string c1_lower = opt.profiles[idx * (opt.fields_vec.size()) + 4];
      // string c1_upper = opt.profiles[idx * (opt.fields_vec.size()) + 4];
      int low = 0;
      // long upper = 99999999999;
      int upper = 999999;
      string c1_lower = string((char *)&low, sizeof(low));
      string c1_upper = string((char *)&upper, sizeof(upper));

      // {
      //   struct tig_gamma::RangeFilter range_filter;
      //   range_filter.field = "field2";
      //   range_filter.lower_value = string((char *)&low, sizeof(low));
      //   range_filter.upper_value = string((char *)&upper, sizeof(upper));
      //   range_filter.include_lower = false;
      //   range_filter.include_upper = true;
      //   request.AddRangeFilter(range_filter);
      // }

      {
        struct tig_gamma::RangeFilter range_filter;
        range_filter.field = "float";
        float low = 0;
        float upper = 0.9;
        range_filter.lower_value = string((char *)&low, sizeof(low));
        range_filter.upper_value = string((char *)&upper, sizeof(upper));
        range_filter.include_lower = false;
        range_filter.include_upper = true;
        request.AddRangeFilter(range_filter);
      }

      tig_gamma::TermFilter term_filter;
      term_filter.field = "field1";
      term_filter.value = "1315\00115248";
      term_filter.is_union = 1;

      request.AddTermFilter(term_filter);

      std::string field_name = "field1";
      request.AddField(field_name);

      std::string id = "_id";
      request.AddField(id);

      std::string ff = "float";
      request.AddField(ff);
    }

    char *request_str, *response_str;
    int request_len, response_len;

    request.Serialize(&request_str, &request_len);
    int ret = Search(opt.engine, request_str, request_len, &response_str,
                     &response_len);

    if (ret != 0) {
      LOG(ERROR) << "Search error [" << ret << "]";
    }
    free(request_str);

    tig_gamma::Response response;
    response.Deserialize(response_str, response_len);

    free(response_str);

    if (opt.print_doc) {
      std::vector<struct tig_gamma::SearchResult> &results = response.Results();
      for (size_t i = 0; i < results.size(); ++i) {
        int ii = idx + i;
        string msg = std::to_string(ii) + ", ";
        struct tig_gamma::SearchResult &result = results[i];

        std::vector<struct tig_gamma::ResultItem> &result_items =
            result.result_items;
        if (result_items.size() <= 0) {
          LOG(ERROR) << "search no result, id=" << ii;
          continue;
        }
        msg += string("total [") + std::to_string(result.total) + "], ";
        msg += string("result_num [") + std::to_string(result_items.size()) +
               "], ";
        for (size_t j = 0; j < result_items.size(); ++j) {
          struct tig_gamma::ResultItem &result_item = result_items[j];
          printDoc(result_item, msg, opt);
          msg += "\n";
        }
        LOG(INFO) << msg;
        if (abs(result_items[0].score - 1.0) < 0.001) {
          if (ii % 100 == 0) {
            LOG(INFO) << msg;
          }
        } else {
          if (!bitmap::test(opt.docids_bitmap_, ii)) {
            LOG(ERROR) << msg;
            error += std::to_string(ii) + ",";
            bitmap::set(opt.docids_bitmap_, ii);
            failed_count++;
          }
        }
      }
    }
    double elap = utils::getmillisecs() - start;
    time += elap;
    if (idx % 10000 == 0) {
      LOG(INFO) << "search time [" << time / 10000 << "]ms";
      time = 0;
    }
    idx += req_num;
    if (idx >= opt.doc_id) {
      idx = 0;
      break;
    }
  }
  LOG(ERROR) << error;
  return failed_count;
}

int GetVector(struct Options &opt) {
  tig_gamma::Request request;
  request.SetTopN(10);
  request.SetReqNum(1);
  request.SetBruteForceSearch(0);
  // request.SetOnlineLogLevel("");
  request.SetMultiVectorRank(0);
  request.SetL2Sqrt(false);

  tig_gamma::TermFilter term_filter;
  term_filter.field = opt.vector_name;
  term_filter.value = "1.jpg";
  term_filter.is_union = 0;

  request.AddTermFilter(term_filter);

  char *request_str, *response_str;
  int request_len, response_len;

  request.Serialize(&request_str, &request_len);
  int ret = Search(opt.engine, request_str, request_len, &response_str,
                   &response_len);

  free(request_str);

  tig_gamma::Response response;
  response.Deserialize(response_str, response_len);

  free(response_str);

  std::vector<struct tig_gamma::SearchResult> &results = response.Results();

  for (size_t i = 0; i < results.size(); ++i) {
    std::string msg = std::to_string(i) + ", ";
    struct tig_gamma::SearchResult &result = results[i];

    std::vector<struct tig_gamma::ResultItem> &result_items =
        result.result_items;
    if (result_items.size() <= 0) {
      continue;
    }
    msg += string("total [") + std::to_string(result.total) + "], ";
    msg += string("result_num [") + std::to_string(result_items.size()) + "], ";
    for (size_t j = 0; j < result_items.size(); ++j) {
      struct tig_gamma::ResultItem &result_item = result_items[j];
      printDoc(result_item, msg, opt);
      msg += "\n";
    }
    LOG(INFO) << msg;
  }
  return ret;
}

void ReadScalarFile(struct Options &opt) {
  size_t idx = 0;
  std::ifstream fin;
  fin.open(opt.profile_file.c_str());
  std::string str;
  while (idx < opt.add_doc_num) {
    std::getline(fin, str);
    if (str == "") break;
    auto profile = std::move(utils::split(str, "\t"));
    size_t i = 0;
    for (const auto &p : profile) {
      opt.profiles[idx * opt.fields_vec.size() + i] = p;
      ++i;
      if (i > opt.fields_vec.size() - 1) {
        break;
      }
    }

    ++idx;
  }
  fin.close();
}

void UpdateThread(struct Options &opt) {
  ReadScalarFile(opt);
  auto DocAddField = [&](tig_gamma::Doc &doc, std::string name,
                         std::string source, std::string val,
                         tig_gamma::DataType data_type) {
    tig_gamma::Field field;
    field.name = name;
    field.source = source;
    field.datatype = data_type;
    field.value = val;
    doc.AddField(field);
  };

  auto DocInfoToString = [&](tig_gamma::Doc &doc, std::string &res_str) {
    auto fields = doc.TableFields();
    std::stringstream ss;
    for (auto &f : fields) {
      if (f.datatype == tig_gamma::DataType::INT) {
        int val = *(int *)(f.value.c_str());
        ss << val << ", ";
      } else {
        auto val = f.value;
        ss << val << ", ";
      }
    }
    res_str = ss.str();
  };

  for (size_t i = 0; i < opt.add_doc_num; i += 1000) {
    int doc_id = i;
    std::string _id;
    tig_gamma::Doc doc;

    for (size_t j = 0; j < opt.fields_vec.size(); ++j) {
      tig_gamma::DataType data_type = opt.fields_type[j];
      std::string &name = opt.fields_vec[j];
      std::string &data =
          opt.profiles[(uint64_t)doc_id * opt.fields_vec.size() + j];
      if (name == "_id") {
        _id = data;
      }
      std::string value;
      if (opt.fields_type[j] == tig_gamma::DataType::INT) {
        char *value_str = static_cast<char *>(malloc(sizeof(int)));
        int v = atoi("88");
        memcpy(value_str, &v, sizeof(int));
        value = std::string(value_str, sizeof(int));
        free(value_str);
      } else if (opt.fields_type[j] == tig_gamma::DataType::LONG) {
        char *value_str = static_cast<char *>(malloc(sizeof(long)));
        long v = atol(data.c_str());
        memcpy(value_str, &v, sizeof(long));
        value = std::string(value_str, sizeof(long));
        free(value_str);
      } else {
        if (name != "_id") {
          value = "00000";
        } else {
          value = data;
        }
      }
      DocAddField(doc, name, "abc", value, data_type);
    }
    {
      float val = 0;
      std::string data((char *)&val, sizeof(val));
      DocAddField(doc, "float", "abc", data, tig_gamma::DataType::FLOAT);
      data = std::string((char *)(opt.feature + (uint64_t)doc_id * opt.d),
                         opt.d * sizeof(float));
      DocAddField(doc, opt.vector_name, "abc", data,
                  tig_gamma::DataType::VECTOR);
    }

    {
      char *str_doc = nullptr;
      int str_len = 0;
      GetDocByID(opt.engine, _id.c_str(), _id.size(), &str_doc, &str_len);
      tig_gamma::Doc old_doc;
      old_doc.SetEngine((tig_gamma::GammaEngine *)opt.engine);
      old_doc.Deserialize(str_doc, str_len);
      std::string get_res;
      DocInfoToString(old_doc, get_res);
      LOG(INFO) << "old doc info:" << get_res;
    }

    char *doc_str = nullptr;
    int len = 0;
    doc.Serialize(&doc_str, &len);
    AddOrUpdateDoc(opt.engine, doc_str, len);

    char *str_doc = nullptr;
    int str_len = 0;
    GetDocByID(opt.engine, _id.c_str(), _id.size(), &str_doc, &str_len);
    tig_gamma::Doc get_doc;
    get_doc.SetEngine((tig_gamma::GammaEngine *)opt.engine);
    get_doc.Deserialize(str_doc, str_len);
    std::string get_res;
    DocInfoToString(get_doc, get_res);
    LOG(INFO) << "get_res: " << get_res;
    free(str_doc);
  }
}

void InitEngine(struct Options &opt) {
#ifdef PERFORMANCE_TESTING
  int fd = open(opt.feature_file.c_str(), O_RDONLY, 0);
  size_t mmap_size = opt.add_doc_num * sizeof(float) * opt.d;
  opt.feature =
      static_cast<float *>(mmap(NULL, mmap_size, PROT_READ, MAP_SHARED, fd, 0));
  close(fd);
#else
  opt.feature = fvecs_read(opt.feature_file.c_str(), &opt.d, &opt.add_doc_num);
#endif

  std::cout << "n [" << opt.add_doc_num << "]" << std::endl;

  opt.add_doc_num =
      opt.add_doc_num > opt.max_doc_size ? opt.max_doc_size : opt.add_doc_num;

  int bitmap_bytes_size = 0;
  int ret =
      bitmap::create(opt.docids_bitmap_, bitmap_bytes_size, opt.max_doc_size);
  ASSERT_EQ(ret, 0);

  ASSERT_NE(opt.docids_bitmap_, nullptr);
  tig_gamma::Config config;
  config.SetPath(opt.path);
  config.SetLogDir(opt.log_dir);

  char *config_str = nullptr;
  int len = 0;
  config.Serialize(&config_str, &len);
  opt.engine = Init(config_str, len);
  free(config_str);
  config_str = nullptr;

  ASSERT_NE(opt.engine, nullptr);
}

int Create(struct Options &opt) {
  tig_gamma::TableInfo table;
  table.SetName(opt.vector_name);
  table.SetRetrievalType(opt.retrieval_type);
  table.SetRetrievalParam(opt.retrieval_param);
  table.SetIndexingSize(opt.indexing_size);

  for (size_t i = 0; i < opt.fields_vec.size(); ++i) {
    struct tig_gamma::FieldInfo field_info;
    field_info.name = opt.fields_vec[i];

    char is_index = 0;
    if (opt.filter && (i == 0 || i == 2 || i == 3 || i == 4)) {
      is_index = 1;
    }
    field_info.is_index = is_index;
    field_info.data_type = opt.fields_type[i];
    table.AddField(field_info);
  }

  {
    struct tig_gamma::FieldInfo field_info;
    field_info.name = "float";

    field_info.is_index = 1;
    field_info.data_type = tig_gamma::DataType::FLOAT;
    table.AddField(field_info);
  }
  struct tig_gamma::VectorInfo vector_info;
  vector_info.name = opt.vector_name;
  vector_info.data_type = tig_gamma::DataType::FLOAT;
  vector_info.is_index = true;
  vector_info.dimension = opt.d;
  vector_info.model_id = opt.model_id;
  vector_info.store_type = opt.store_type;
  vector_info.store_param = "{\"cache_size\": 16, \"compress\": {\"rate\":16}}";
  // vector_info.store_param = "{\"cache_size\": 16}";
  vector_info.has_source = false;

  table.AddVectorInfo(vector_info);

  char *table_str = nullptr;
  int len = 0;
  table.Serialize(&table_str, &len);

  int ret = CreateTable(opt.engine, table_str, len);

  free(table_str);

  return ret;
}

int Add(struct Options &opt) {
  ReadScalarFile(opt);
  int ret = 0;
  if (opt.add_type == 0) {
    ret = AddDocToEngine(opt, opt.add_doc_num);
  } else {
    ret = BatchAddDocToEngine(opt, opt.add_doc_num);
  }
  return ret;
}

int BuildEngineIndex(struct Options &opt) {
  // UpdateThread(opt.engine);
  // BuildIndex(opt.engine);
  int n_index_status = -1;
  do {
    char *status = nullptr;
    int len = 0;
    GetEngineStatus(opt.engine, &status, &len);
    tig_gamma::EngineStatus engine_status;
    engine_status.Deserialize(status, len);
    free(status);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    n_index_status = engine_status.IndexStatus();
  } while (n_index_status != 2);

  char *doc_str = nullptr;
  int doc_str_len = 0;
  string docid = "1";
  GetDocByID(opt.engine, docid.c_str(), docid.size(), &doc_str, &doc_str_len);
  tig_gamma::Doc doc;
  doc.SetEngine((tig_gamma::GammaEngine *)opt.engine);
  doc.Deserialize(doc_str, doc_str_len);
  LOG(INFO) << "Doc fields [" << doc.TableFields().size() << "]";
  free(doc_str);
  DeleteDoc(opt.engine, docid.c_str(), docid.size());
  GetDocByID(opt.engine, docid.c_str(), docid.size(), &doc_str, &doc_str_len);
  free(doc_str);

  LOG(INFO) << "Indexed!";
  GetVector(opt);
  // opt.doc_id = 0;
  opt.doc_id2 = 1;
  return 0;
}

int Search(struct Options &opt) {
  std::thread t_searchs[opt.search_thread_num];

  std::function<int()> func_search =
      std::bind(SearchThread, opt, opt.search_num);
  std::future<int> search_futures[opt.search_thread_num];
  std::packaged_task<int()> tasks[opt.search_thread_num];

  double start = utils::getmillisecs();
  for (int i = 0; i < opt.search_thread_num; ++i) {
    tasks[i] = std::packaged_task<int()>(func_search);
    search_futures[i] = tasks[i].get_future();
    t_searchs[i] = std::thread(std::move(tasks[i]));
  }

  std::this_thread::sleep_for(std::chrono::seconds(2));

  // std::function<int(void *)> add_func =
  //     std::bind(AddDocToEngine, std::placeholders::_1, 1 * 1, 1);
  // std::thread add_thread(add_func, opt.engine);

  // get search results
  for (int i = 0; i < opt.search_thread_num; ++i) {
    search_futures[i].wait();
    int error_num = search_futures[i].get();
    if (error_num != 0) {
      LOG(ERROR) << "error_num [" << error_num << "]";
    }
    t_searchs[i].join();
  }

  double end = utils::getmillisecs();
  LOG(INFO) << "Search cost [" << end - start << "] ms";
  // add_thread.join();

  return 0;
}

int AlterCacheSizeTest(struct Options &opt) {
  tig_gamma::Config conf;
  conf.AddCacheInfo("table", 1024);
  conf.AddCacheInfo("string", 2048);
  conf.AddCacheInfo(opt.vector_name, 4096);
  char *buf = nullptr;
  int len = 0;
  conf.Serialize(&buf, &len);
  int ret = SetConfig(opt.engine, buf, len);
  if (buf) { free(buf); }
  return ret;
}

int GetCacheSizeTest(struct Options &opt) {
  tig_gamma::Config config;
  char *buf = nullptr;
  int len = 0;
  GetConfig(opt.engine, &buf, &len);
  config.Deserialize(buf, len);
  for (auto &cache_info : config.CacheInfos()) {
    LOG(INFO) << "TestGetCacheSize() field_name:" << cache_info.field_name
              << ", cache_size:" << cache_info.cache_size;
  }
  if (buf) { free(buf); }
  return 0;
}

int LoadEngine(struct Options &opt) {
  int bitmap_bytes_size = 0;
  int ret =
      bitmap::create(opt.docids_bitmap_, bitmap_bytes_size, opt.max_doc_size);
  if (ret != 0) {
    LOG(ERROR) << "Create bitmap failed!";
  }
  tig_gamma::Config config;
  config.SetPath(opt.path);

  char *config_str;
  int config_len;
  config.Serialize(&config_str, &config_len);

  opt.engine = Init(config_str, config_len);
  if (config_str) { free(config_str); }

  ret = Load(opt.engine);
  return ret;
}

int CloseEngine(struct Options &opt) {
  Close(opt.engine);
  opt.engine = nullptr;
  delete opt.docids_bitmap_;
#ifdef PERFORMANCE_TESTING
  munmap(opt.feature, opt.add_doc_num * sizeof(float) * opt.d);
#else
  delete opt.feature;
#endif
  return 0;
}

int TestIndexes(struct Options &opt) {
  int ret = 0;
  if (opt.b_load) {
    InitEngine(opt);
    LoadEngine(opt);
    opt.doc_id = 20000;
  } else {
    utils::remove_dir(opt.path.c_str());
    InitEngine(opt);
    ret = Create(opt);
    EXPECT_EQ(ret, 0);
    if (ret != 0) {
      return ret;
    }
    Add(opt);
  }
  BuildEngineIndex(opt);
  // Add();
  Search(opt);

    GetCacheSizeTest(opt);
    AlterCacheSizeTest(opt);
    GetCacheSizeTest(opt);

  //   UpdateThread(opt);
  if (not opt.b_load) {
    EXPECT_EQ(Dump(opt.engine), 0);
  }
  // LoadEngine();
  // BuildEngineIndex();
  // Search();
  CloseEngine(opt);
  return ret;
}

}  // namespace
