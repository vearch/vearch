/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "test.h"
#include <cmath>
#include <fcntl.h>
#include <fstream>
#include <functional>
#include <future>
#include <sys/mman.h>

/**
 * To run this demo, please download the ANN_SIFT10K dataset from
 *
 *   ftp://ftp.irisa.fr/local/texmex/corpus/siftsmall.tar.gz
 *
 * and unzip it.
 **/

struct Options {
  Options() {
    nprobe = 10;
    doc_id = 0;
    d = 512;
    max_doc_size = 10000 * 20;
    add_doc_num = 10000 * 10;
    search_num = 10000 * 1;
    fields_vec = {"sku", "_id", "cid1", "cid2", "cid3"};
    fields_type = {STRING, STRING, INT, INT, INT};
    vector_name = "abc";
    path = "files";
    log_dir = "log";
    model_id = "model";
    retrieval_type = "IVFPQ";
    store_type = "MemoryOnly";
    profiles.resize(max_doc_size * fields_vec.size());
    engine = nullptr;
  }

  int nprobe;
  size_t doc_id;
  size_t d;
  size_t max_doc_size;
  size_t add_doc_num;
  size_t search_num;
  std::vector<string> fields_vec;
  std::vector<enum DataType> fields_type;
  string path;
  string log_dir;
  string vector_name;
  string model_id;
  string retrieval_type;
  string store_type;

  std::vector<string> profiles;
  float *feature;

  string profile_file;
  string feature_file;
  char *docids_bitmap_;
  void *engine;
};

static struct Options opt;

int AddDocToEngine(void *engine, int doc_num, int interval = 0) {
  for (int i = 0; i < doc_num; ++i) {
    double start = utils::getmillisecs();
    Field **fields = MakeFields(opt.fields_vec.size() + 1);

    string url;
    for (size_t j = 0; j < opt.fields_vec.size(); ++j) {
      enum DataType data_type = opt.fields_type[j];
      ByteArray *name = StringToByteArray(opt.fields_vec[j]);
      ByteArray *value;

      string &data =
          opt.profiles[(uint64_t)opt.doc_id * opt.fields_vec.size() + j];
      if (opt.fields_type[j] == INT) {
        value = static_cast<ByteArray *>(malloc(sizeof(ByteArray)));
        value->value = static_cast<char *>(malloc(sizeof(int)));
        value->len = sizeof(int);
        int v = atoi(data.c_str());
        memcpy(value->value, &v, value->len);
      } else if (opt.fields_type[j] == LONG) {
        value = static_cast<ByteArray *>(malloc(sizeof(ByteArray)));
        value->value = static_cast<char *>(malloc(sizeof(long)));
        value->len = sizeof(long);
        long v = atol(data.c_str());
        memcpy(value->value, &v, value->len);
      } else {
        value = StringToByteArray(data);
        url = data;
      }
      ByteArray *source = StringToByteArray(url);
      Field *field = MakeField(name, value, source, data_type);
      SetField(fields, j, field);
    }

    ByteArray *value =
        FloatToByteArray(opt.feature + (uint64_t)opt.doc_id * opt.d, opt.d);
    ByteArray *name = StringToByteArray(opt.vector_name);
    ByteArray *source = StringToByteArray(url);
    Field *field = MakeField(name, value, source, VECTOR);
    SetField(fields, opt.fields_vec.size(), field);

    Doc *doc = MakeDoc(fields, opt.fields_vec.size() + 1);
    AddOrUpdateDoc(engine, doc);
    DestroyDoc(doc);
    ++opt.doc_id;
    double elap = utils::getmillisecs() - start;
    if (i % 10000 == 0) {
      LOG(INFO) << "AddDoc use [" << elap << "]ms";
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(interval));
  }
  return 0;
}

int SearchThread(void *engine, size_t num) {
  size_t idx = 0;
  double time = 0;
  int failed_count = 0;
  int req_num = 1000;
  string error;
  while (idx < num) {
    double start = utils::getmillisecs();
    VectorQuery **vector_querys = MakeVectorQuerys(1);
    ByteArray *value =
        FloatToByteArray(opt.feature + (uint64_t)idx * opt.d, opt.d * req_num);
    VectorQuery *vector_query = MakeVectorQuery(
        StringToByteArray(opt.vector_name), value, 0, 10000, 0.1, 0);
    SetVectorQuery(vector_querys, 0, vector_query);

    // string c1_lower = opt.profiles[idx * (opt.fields_vec.size()) + 4];
    // string c1_upper = opt.profiles[idx * (opt.fields_vec.size()) + 4];
    // LOG(INFO) << "idx=" << idx << ", cid3=" << c1_lower;
    // string cid = "cid3";
    // RangeFilter **range_filters = MakeRangeFilters(1);
    // RangeFilter *range_filter =
    //     MakeRangeFilter(StringToByteArray(cid), StringToByteArray(c1_lower),
    //                     StringToByteArray(c1_upper), false, true);
    // SetRangeFilter(range_filters, 0, range_filter);
    // Request *request = MakeRequest(10, vector_querys, 1, nullptr, 0,
    //                                range_filters, 1, nullptr, 0, req_num, 0,
    //                                nullptr);
    Request *request = MakeRequest(10, vector_querys, 1, nullptr, 0, nullptr, 0,
                                   nullptr, 0, req_num, 0, nullptr);

    Response *response = Search(engine, request);
    for (int i = 0; i < response->req_num; ++i) {
      int ii = idx + i;
      string msg = std::to_string(ii) + ", ";
      SearchResult *results = GetSearchResult(response, i);
      if (results->result_num <= 0) {
        continue;
      }
      msg += string("total [") + std::to_string(results->total) + "], ";
      msg +=
          string("result_num [") + std::to_string(results->result_num) + "], ";
      for (int j = 0; j < results->result_num; ++j) {
        ResultItem *result_item = GetResultItem(results, j);
        msg += string("score [") + std::to_string(result_item->score) + "], ";
        printDoc(result_item->doc, msg);
        msg += "\n";
      }
      if (abs(GetResultItem(results, 0)->score - 1.0) < 0.001) {
        if (ii % 100000 == 0) {
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

    DestroyRequest(request);
    DestroyResponse(response);
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

void UpdateThread(void *engine) {
  int doc_id = 1;
  Field **fields = MakeFields(opt.fields_vec.size() + 1);

  for (size_t j = 0; j < opt.fields_vec.size(); ++j) {
    enum DataType data_type = opt.fields_type[j];
    ByteArray *name = StringToByteArray(opt.fields_vec[j]);
    ByteArray *value;

    string &data = opt.profiles[(uint64_t)doc_id * opt.fields_vec.size() + j];
    if (opt.fields_type[j] == INT) {
      value = static_cast<ByteArray *>(malloc(sizeof(ByteArray)));
      value->value = static_cast<char *>(malloc(sizeof(int)));
      value->len = sizeof(int);
      int v = atoi("88");
      memcpy(value->value, &v, value->len);
    } else if (opt.fields_type[j] == LONG) {
      value = static_cast<ByteArray *>(malloc(sizeof(ByteArray)));
      value->value = static_cast<char *>(malloc(sizeof(long)));
      value->len = sizeof(long);
      long v = atol(data.c_str());
      memcpy(value->value, &v, value->len);
    } else {
      value = StringToByteArray(data);
    }
    ByteArray *source = StringToByteArray(string("abc"));
    Field *field = MakeField(name, value, source, data_type);
    SetField(fields, j, field);
  }

  ByteArray *value =
      FloatToByteArray(opt.feature + (uint64_t)doc_id * opt.d, opt.d);
  ByteArray *name = StringToByteArray(opt.vector_name);
  ByteArray *source = StringToByteArray(string("abc"));
  Field *field = MakeField(name, value, source, VECTOR);
  SetField(fields, opt.fields_vec.size(), field);

  Doc *doc = MakeDoc(fields, opt.fields_vec.size() + 1);
  UpdateDoc(engine, doc);
  DestroyDoc(doc);
}

int Init() {
  opt.feature = fvecs_read(opt.feature_file.c_str(), &opt.d, &opt.add_doc_num);
  std::cout << "n [" << opt.add_doc_num << "]" << std::endl;

  opt.add_doc_num =
      opt.add_doc_num > opt.max_doc_size ? opt.max_doc_size : opt.add_doc_num;

  int bitmap_bytes_size = 0;
  int ret =
      bitmap::create(opt.docids_bitmap_, bitmap_bytes_size, opt.max_doc_size);
  if (ret != 0) {
    LOG(ERROR) << "Create bitmap failed!";
  }
  assert(opt.docids_bitmap_ != nullptr);
  Config *config = MakeConfig(StringToByteArray(opt.path), opt.max_doc_size);
  SetLogDictionary(StringToByteArray(opt.log_dir));
  opt.engine = Init(config);
  DestroyConfig(config);
  assert(opt.engine != nullptr);
  return 0;
}

int CreateTable() {
  ByteArray *table_name = MakeByteArray("test", 4);
  FieldInfo **field_infos = MakeFieldInfos(opt.fields_vec.size());

  for (size_t i = 0; i < opt.fields_vec.size(); ++i) {
    FieldInfo *field_info = MakeFieldInfo(StringToByteArray(opt.fields_vec[i]),
                                          opt.fields_type[i], 1);
    SetFieldInfo(field_infos, i, field_info);
  }

  VectorInfo **vectors_info = MakeVectorInfos(1);
  VectorInfo *vector_info = MakeVectorInfo(
      StringToByteArray(opt.vector_name), FLOAT, opt.d,
      StringToByteArray(opt.model_id), StringToByteArray(opt.retrieval_type),
      StringToByteArray(opt.store_type));
  SetVectorInfo(vectors_info, 0, vector_info);

  Table *table = MakeTable(table_name, field_infos, opt.fields_vec.size(),
                           vectors_info, 1, kIVFPQParam);
  enum ResponseCode ret = CreateTable(opt.engine, table);
  DestroyTable(table);
  return ret;
}

int Add() {
  size_t idx = 0;

  std::ifstream fin;
  fin.open(opt.profile_file.c_str());
  std::string str;
  while (idx < opt.add_doc_num) {
    std::getline(fin, str);
    if (str == "")
      break;
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

  // int fd = open(opt.feature_file.c_str(), O_RDONLY, 0);
  // opt.feature =
  //     static_cast<float *>(mmap(NULL, opt.max_doc_size * sizeof(float) *
  //     opt.d,
  //                               PROT_READ, MAP_SHARED, fd, 0));
  // close(fd);

  int ret = AddDocToEngine(opt.engine, opt.add_doc_num);
  return ret;
}

int BuildEngineIndex() {
  std::thread t(BuildIndex, opt.engine);
  t.detach();

  while (GetIndexStatus(opt.engine) != INDEXED) {
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }

  // string docid = "1.jpg";
  // ByteArray *value = StringToByteArray(docid);
  // Doc *doc = GetDocByID(opt.engine, value);
  // DelDoc(opt.engine, value);
  // doc = GetDocByID(opt.engine, value);

  LOG(INFO) << "Indexed!";
  return 0;
}

int Search() {
  int search_thread_num = 1;
  std::thread t_searchs[search_thread_num];

  std::function<int()> func_search =
      std::bind(SearchThread, opt.engine, opt.search_num);
  std::future<int> search_futures[search_thread_num];
  std::packaged_task<int()> tasks[search_thread_num];

  for (int i = 0; i < search_thread_num; ++i) {
    tasks[i] = std::packaged_task<int()>(func_search);
    search_futures[i] = tasks[i].get_future();
    t_searchs[i] = std::thread(std::move(tasks[i]));
  }

  std::this_thread::sleep_for(std::chrono::seconds(2));

  // std::function<int(void *)> add_func =
  //     std::bind(AddDocToEngine, std::placeholders::_1, 1 * 1, 1);
  // std::thread add_thread(add_func, opt.engine);

  // get search results
  for (int i = 0; i < search_thread_num; ++i) {
    search_futures[i].wait();
    int error_num = search_futures[i].get();
    if (error_num != 0) {
      LOG(ERROR) << "error_num [" << error_num << "]";
    }
    t_searchs[i].join();
  }

  // add_thread.join();
  return 0;
}

int DumpEngine() {
  int ret = Dump(opt.engine);

  // ret = AddDocToEngine(opt.engine, opt.add_doc_num);

  // std::this_thread::sleep_for(std::chrono::seconds(10));

  // ret = Dump(opt.engine);

  Close(opt.engine);
  opt.engine = nullptr;
  delete opt.docids_bitmap_;
  return ret;
}

int LoadEngine() {
  int bitmap_bytes_size = 0;
  int ret =
      bitmap::create(opt.docids_bitmap_, bitmap_bytes_size, opt.max_doc_size);
  if (ret != 0) {
    LOG(ERROR) << "Create bitmap failed!";
  }
  Config *config = MakeConfig(StringToByteArray(opt.path), opt.max_doc_size);
  opt.engine = Init(config);
  DestroyConfig(config);

  ret = Load(opt.engine);
  return ret;
}

int BuildIndexAfterLoad() {
  std::thread t(BuildIndex, opt.engine);
  t.detach();
  while (GetIndexStatus(opt.engine) != INDEXED) {
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }

  LOG(INFO) << "Indexed finished after load!";
  return 0;
}

int SearchThreadAfterLoad() {
  int search_thread_num = 1;
  std::thread t_searchs[search_thread_num];

  std::function<int()> func_search =
      std::bind(SearchThread, opt.engine, opt.search_num);
  std::future<int> search_futures[search_thread_num];
  std::packaged_task<int()> tasks[search_thread_num];

  for (int i = 0; i < search_thread_num; ++i) {
    tasks[i] = std::packaged_task<int()>(func_search);
    search_futures[i] = tasks[i].get_future();
    t_searchs[i] = std::thread(std::move(tasks[i]));
  }

  std::this_thread::sleep_for(std::chrono::seconds(2));

  std::function<int(void *)> add_func =
      std::bind(AddDocToEngine, std::placeholders::_1, 10000 * 1, 1);
  // std::thread add_thread(add_func, opt.engine);

  // get search results
  for (int i = 0; i < search_thread_num; ++i) {
    search_futures[i].wait();
    int error_num = search_futures[i].get();
    if (error_num != 0) {
      LOG(ERROR) << "error_num [" << error_num << "]";
    }
    t_searchs[i].join();
  }

  // add_thread.join();
  return 0;
}

int DumpAfterLoad() {
  int ret = Dump(opt.engine);
  return ret;
}

int CloseEngine() {
  Close(opt.engine);
  opt.engine = nullptr;
  delete opt.docids_bitmap_;
  delete opt.feature;
  // munmap(opt.feature, opt.add_doc_num * sizeof(float) * opt.d);
  return 0;
}

int main(int argc, char **argv) {
  setvbuf(stdout, (char *)NULL, _IONBF, 0);
  if (argc != 3) {
    std::cout << "Usage: [Program] [profile_file] [vectors_file]\n";
    return 1;
  }
  opt.profile_file = argv[1];
  opt.feature_file = argv[2];
  std::cout << opt.profile_file.c_str() << " " << opt.feature_file.c_str()
            << std::endl;
  Init();
  CreateTable();
  Add();
  BuildEngineIndex();
  Search();
  DumpEngine();
  LoadEngine();
  // BuildIndexAfterLoad();
  // SearchThreadAfterLoad();
  // DumpAfterLoad();
  CloseEngine();

  return 0;
}
