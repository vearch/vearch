/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/sysinfo.h>
#include <unistd.h>
#include <cmath>
#include <fstream>
#include <functional>
#include <future>
#include <iostream>
#include <string>
#include <vector>
#include "util/log.h"
#include "util/utils.h"

/**
 * To run this demo, please download the ANN_SIFT10K dataset from
 *
 *   ftp://ftp.irisa.fr/local/texmex/corpus/siftsmall.tar.gz
 *
 * and unzip it.
 **/

using namespace std;

namespace {
inline ByteArray *StringToByteArray(const std::string &str) {
  ByteArray *ba = static_cast<ByteArray *>(malloc(sizeof(ByteArray)));
  ba->len = str.length();
  ba->value = static_cast<char *>(malloc((str.length()) * sizeof(char)));
  memcpy(ba->value, str.data(), str.length());
  return ba;
}

inline ByteArray *FloatToByteArray(const float *feature, int dimension) {
  ByteArray *ba = static_cast<ByteArray *>(malloc(sizeof(ByteArray)));
  ba->len = dimension * sizeof(float);
  ba->value = static_cast<char *>(malloc(ba->len));
  memcpy((void *)ba->value, (void *)feature, ba->len);
  return ba;
}

template <typename T>
inline ByteArray *ToByteArray(const T &d) {
  ByteArray *ba = static_cast<ByteArray *>(malloc(sizeof(ByteArray)));
  ba->len = sizeof(T);
  ba->value = static_cast<char *>(malloc(ba->len));
  memcpy(ba->value, &d, ba->len);
  return ba;
}

}  // namespace

namespace test {

#define JPMUSTGETSTRING(name, value)                \
  {                                                 \
    if (jp.GetString(name, value)) {                \
      LOG(ERROR) << "json get error name=" << name; \
      return -1;                                    \
    }                                               \
  }

#define JPGETSTRING(name, value)                      \
  {                                                   \
    if (jp.Contains(name)) {                          \
      if (jp.GetString(name, value)) {                \
        LOG(ERROR) << "json get error name=" << name; \
        return -1;                                    \
      }                                               \
    }                                                 \
  }

#define JPGETINT(name, value)                         \
  {                                                   \
    if (jp.Contains(name)) {                          \
      double v = 0.0f;                                \
      if (jp.GetDouble(name, v)) {                    \
        LOG(ERROR) << "json get error name=" << name; \
        return -1;                                    \
      }                                               \
      value = (int)v;                                 \
    }                                                 \
  }

struct GammaPerfConf {
  GammaPerfConf() {
    nprobe = 20;
    ncentroids = 256;
    nsubvector = 64;
    dimension = 512;
    max_doc_size = 10000 * 200;
    nadd = 10000 * 10;
    nsearch = 10000 * 1;
    fields_vec = {"_id", "field1", "field2"};
    fields_type = {LONG, STRING, STRING};
    root_path = "files";
    vector_name = "abc";
    model_id = "model";
    retrieval_type = "IVFPQ";
    store_type = "Mmap";
    store_param = "";
    has_rank = 1;
    fixed_search_threads = -1;
  }

  int Parse(string &conf_path) {
    long file_size = utils::get_file_size(conf_path.c_str());
    std::unique_ptr<char[]> data(new char[file_size]);
    utils::FileIO conf_file(conf_path);
    conf_file.Open("r");
    conf_file.Read(data.get(), 1, file_size);

    utils::JsonParser jp;
    if (jp.Parse(data.get())) {
      LOG(ERROR) << "json parse error: " << conf_path << ", len=" << file_size
                 << ", content=" << data.get();
      return -1;
    }

    JPMUSTGETSTRING("add_profile_file", add_profile_file);
    JPMUSTGETSTRING("add_feature_file", add_feature_file);
    JPMUSTGETSTRING("search_feature_file", search_feature_file);
    JPGETSTRING("store_type", store_type);
    JPGETSTRING("store_param", store_param);

    JPGETINT("nprobe", nprobe);
    JPGETINT("ncentroids", ncentroids);
    JPGETINT("nsubvector", nsubvector);
    JPGETINT("dimension", dimension);
    JPGETINT("max_doc_size", max_doc_size);
    JPGETINT("nadd", nadd);
    JPGETINT("nsearch", nsearch);
    JPGETINT("has_rank", has_rank);
    JPGETINT("fixed_search_threads", fixed_search_threads);

    if (add_profile_file == "" || add_feature_file == "" ||
        search_feature_file == "") {
      LOG(ERROR) << "add or search file empty";
      return -1;
    }
    LOG(INFO) << "add_profile_file=" << add_profile_file
              << ", add_feature_file=" << add_feature_file
              << ", search_feature_file=" << search_feature_file
              << ", nprobe=" << nprobe << ", dimension=" << dimension
              << ", max_doc_size=" << max_doc_size << ", nadd=" << nadd
              << ", nsearch=" << nsearch << ", store_type=" << store_type
              << ", store_param=" << store_param << ", has_rank=" << has_rank
              << ", fixed_search_threads=" << fixed_search_threads;
    return 0;
  }

  int nprobe;
  int ncentroids;
  int nsubvector;
  size_t dimension;
  int max_doc_size;
  int nadd;
  int nsearch;
  std::vector<string> fields_vec;
  std::vector<enum DataType> fields_type;
  string root_path;
  string vector_name;
  string model_id;
  string retrieval_type;
  string store_type;
  string store_param;
  int has_rank;
  int fixed_search_threads;

  string add_profile_file;
  string add_feature_file;
  string search_feature_file;
};

struct Stat {
  vector<float> costs;
  int nfail;
  int nsucc;

  // timestamp
  double start;
  double end;

  // compute results
  float tp90;
  float tp99;
  float avg_latency;
  float qps;

  Stat() {
    nfail = 0;
    nsucc = 0;
    start = std::numeric_limits<float>::max();
    end = 0.0f;
    tp90 = 0.0f;
    tp99 = 0.0f;
    avg_latency = 0.0f;
    qps = 0.0f;
  }

  Stat(int n) : Stat() { costs.reserve(n); }

  void Start() { start = utils::getmillisecs(); }

  void End() { end = utils::getmillisecs(); }

  void Merge(Stat &other) {
    costs.insert(costs.end(), other.costs.begin(), other.costs.end());
    nfail += other.nfail;
    nsucc += other.nsucc;
    // merge start and end
    if (other.start < start) start = other.start;
    if (other.end > end) end = other.end;
  }

  void Analyze() {
    std::sort(costs.begin(), costs.end());
    int i90 = (int)(costs.size() * 0.9);
    int i99 = (int)(costs.size() * 0.99);
    tp90 = costs[i90];
    tp99 = costs[i99];

    float total = std::accumulate(costs.begin(), costs.end(), 0.0f);
    avg_latency = total / costs.size();

    float secs = (float)(end - start) / 1000;
    if (secs < 1) secs = 1;
    qps = costs.size() / secs;
  }

  string ToString() {
    stringstream ss;
    ss << "qps=" << qps << ", average latency=" << avg_latency
       << "ms, tp99=" << tp99 << "ms, tp90=" << tp90
       << "ms, total cost=" << end - start << "ms"
       << ", start=" << (long)start << "ms, end=" << (long)end << "ms"
       << ", success number=" << nsucc << ", fail number=" << nfail;
    return ss.str();
  }
};

static int nums[] = {1, 5, 10, 20, 30, 50, 70, 100};

class GammaPerfTest {
 private:
  GammaPerfConf *conf_;
  void *engine_;
  float *search_vectors_;
  int *thread_nums_;
  int max_steps_;
  Stat *stats_;
  int run_steps_;

 public:
  GammaPerfTest(GammaPerfConf *conf) {
    conf_ = conf;
    thread_nums_ = nums;
    max_steps_ = sizeof(nums) / sizeof(nums[0]);
    stats_ = new Stat[max_steps_];
    run_steps_ = 0;
  }
  ~GammaPerfTest() {
    if (engine_) Close(engine_);
    if (search_vectors_) delete[] search_vectors_;
    if (stats_) delete[] stats_;
  }
  int Init() {
    utils::remove_dir(conf_->root_path.c_str());
    utils::make_dir(conf_->root_path.c_str());

    Config *config =
        MakeConfig(StringToByteArray(conf_->root_path), conf_->max_doc_size);
    engine_ = ::Init(config);
    DestroyConfig(config);
    assert(engine_ != nullptr);

    CreateTable();

    AddDoc();

    BuildIndex();

    search_vectors_ = new float[(long)conf_->nsearch * conf_->dimension];
    utils::FileIO fet_file(conf_->search_feature_file);
    if (fet_file.Open("rb")) {
      LOG(ERROR) << "open error:" << conf_->search_feature_file;
      return -1;
    }
    size_t size = (size_t)conf_->nsearch * conf_->dimension * sizeof(float);
    if (size != fet_file.Read(search_vectors_, 1, size)) {
      LOG(ERROR) << "read search vectors error, size=" << size;
      return -1;
    }

    return 0;
  }

  int Run() {
    if (conf_->fixed_search_threads > 0) {
      thread_nums_[0] = conf_->fixed_search_threads;
      max_steps_ = 1;
    }

    for (; run_steps_ < max_steps_; run_steps_++) {
      LOG(INFO) << "-----------begin to run nthread="
                << thread_nums_[run_steps_] << "-----------";
      RunOnce(thread_nums_[run_steps_], stats_[run_steps_]);
      stats_[run_steps_].Analyze();
      if (run_steps_ > 0 &&
          stats_[run_steps_].qps - stats_[run_steps_ - 1].qps < 5) {
        LOG(INFO) << "current qps=" << stats_[run_steps_].qps
                  << ", pre qps=" << stats_[run_steps_ - 1].qps
                  << ", the difference is not obvious, so stop increasing "
                     "thread number";
        run_steps_++;
        break;
      }
      LOG(INFO) << "-----------end nthread=" << thread_nums_[run_steps_] << ", "
                << stats_[run_steps_].ToString();
    }
    return 0;
  }

  void GetResult(string &result) {
    stringstream ss;
    ss << "------------------Performance----------------\n";
    for (int i = 0; i < run_steps_; i++) {
      ss << "nthread=" << thread_nums_[i] << ", " << stats_[i].ToString()
         << "\n";
    }
    ss << "--------------------------------------------\n";
    result = ss.str();
  }

  int RunOnce(int nthread, Stat &stat) {
    std::thread **threads = new std::thread *[nthread];
    Stat *stats = new Stat[nthread];
    for (int i = 0; i < nthread; i++) {
      threads[i] =
          new std::thread(GammaPerfTest::SearchRunner, this, &stats[i]);
    }
    for (int i = 0; i < nthread; i++) {
      threads[i]->join();
    }
    for (int i = 0; i < nthread; i++) {
      delete threads[i];
      // LOG(INFO) << "nthreads=" << nthread << ", costs="
      //           << utils::join(stats[i].costs.data(), stats[i].costs.size(),
      //                          ',');
      stat.Merge(stats[i]);
    }
    delete[] threads;
    delete[] stats;
    return 0;
  }

  static void SearchRunner(GammaPerfTest *t, Stat *stat) {
    GammaPerfConf *conf = t->conf_;
    int dimension = conf->dimension;
    stat->Start();
    for (int id = 0; id < conf->nsearch; id++) {
      float *vector = t->search_vectors_ + (long)id * dimension;
      VectorQuery **vector_querys = MakeVectorQuerys(1);
      ByteArray *value = FloatToByteArray(vector, dimension);
      VectorQuery *vector_query = MakeVectorQuery(
          StringToByteArray(conf->vector_name), value, 0, 10000, 0.1, 0);
      SetVectorQuery(vector_querys, 0, vector_query);

      Request *request =
          MakeRequest(10, vector_querys, 1, nullptr, 0, nullptr, 0, nullptr, 0,
                      1, 0, nullptr, conf->has_rank, 0, TRUE, FALSE);

      double start = utils::getmillisecs();
      Response *response = Search(t->engine_, request);
      stat->costs.push_back(utils::getmillisecs() - start);
      SearchResult *results = GetSearchResult(response, 0);
      if (results->result_num <= 0) {
        stat->nfail++;
      } else {
        stat->nsucc++;
      }
      DestroyRequest(request);
      DestroyResponse(response);
    }
    stat->End();
    // LOG(INFO) << "runner start=" << (long)stat->start
    //           << ", end=" << (long)stat->end;
  };

 private:
  int CreateTable() {
    ByteArray *table_name = MakeByteArray("test_table", 4);
    FieldInfo **field_infos = MakeFieldInfos(conf_->fields_vec.size());

    for (size_t i = 0; i < conf_->fields_vec.size(); ++i) {
      char is_index = 0;
      FieldInfo *field_info =
          MakeFieldInfo(StringToByteArray(conf_->fields_vec[i]),
                        conf_->fields_type[i], is_index);
      SetFieldInfo(field_infos, i, field_info);
    }

    VectorInfo **vectors_info = MakeVectorInfos(1);
    VectorInfo *vector_info =
        MakeVectorInfo(StringToByteArray(conf_->vector_name), FLOAT, TRUE,
                       conf_->dimension, StringToByteArray(conf_->model_id),
                       StringToByteArray(conf_->store_type),
                       StringToByteArray(conf_->store_param), FALSE);
    SetVectorInfo(vectors_info, 0, vector_info);

    std::vector<char> buff;
    buff.resize(256);
    int len = snprintf(buff.data(), 256,
                       "{\"nprobe\" : %d, \"metric_type\" : \"InnerProduct\", "
                       "\"ncentroids\" : "
                       "%d,\"nsubvector\" : %d}",
                       conf_->nprobe, conf_->ncentroids, conf_->nsubvector);
    string param(buff.data(), len);
    LOG(INFO) << "retrieve param=" << param;
    ByteArray *retrieve_param = StringToByteArray(param);

    Table *table = MakeTable(
        table_name, field_infos, conf_->fields_vec.size(), vectors_info, 1,
        StringToByteArray(conf_->retrieval_type), retrieve_param, 0);
    enum ResponseCode ret = ::CreateTable(engine_, table);
    DestroyTable(table);
    return ret;
  }

  int AddDoc() {
    std::ifstream fin;
    fin.open(conf_->add_profile_file.c_str());
    std::string str;

    FILE *fet_fp = fopen(conf_->add_feature_file.c_str(), "rb");
    assert(fet_fp != nullptr);
    float *vector = new float[conf_->dimension * sizeof(float)];

    long docid = 0;
    for (int i = 0; i < conf_->nadd; ++i) {
      Field **fields = MakeFields(conf_->fields_vec.size() + 1);

      std::getline(fin, str);
      if (str == "") break;
      std::vector<std::string> profile = utils::split(str, "\t");
      string url = profile[1];

      fread((void *)vector, sizeof(long), conf_->dimension, fet_fp);

      ByteArray *name = StringToByteArray(conf_->fields_vec[0]);
      ByteArray *value = ToByteArray(docid);
      Field *field = MakeField(name, value, nullptr, LONG);
      SetField(fields, 0, field);

      for (size_t j = 1; j < conf_->fields_vec.size(); ++j) {
        enum DataType data_type = conf_->fields_type[j];
        ByteArray *name = StringToByteArray(conf_->fields_vec[j]);
        ByteArray *value;

        string &data = profile[j];
        if (conf_->fields_type[j] == INT) {
          int v = atoi(data.c_str());
          value = ToByteArray(v);
        } else if (conf_->fields_type[j] == LONG) {
          long v = atol(data.c_str());
          value = ToByteArray(v);
        } else if (conf_->fields_type[j] == STRING) {
          value = StringToByteArray(data);
        } else {
          throw runtime_error("unsupport data type");
        }
        // ByteArray *source = nullptr;  // StringToByteArray(url);
        Field *field = MakeField(name, value, nullptr, data_type);
        SetField(fields, j, field);
      }

      name = StringToByteArray(conf_->vector_name);
      value = FloatToByteArray(vector, conf_->dimension);
      ByteArray *source = nullptr;
      field = MakeField(name, value, source, VECTOR);
      SetField(fields, conf_->fields_vec.size(), field);

      Doc *doc = MakeDoc(fields, conf_->fields_vec.size() + 1);
      AddOrUpdateDoc(engine_, doc);
      DestroyDoc(doc);
      ++docid;
    }
    delete[] vector;
    fclose(fet_fp);
    fin.close();
    return 0;
  }

  void BuildIndex() {
    LOG(INFO) << "begin to build index";
    std::thread t(::BuildIndex, engine_);
    t.detach();

    while (GetIndexStatus(engine_) != INDEXED) {
      std::this_thread::sleep_for(std::chrono::seconds(2));
    }

    LOG(INFO) << "Indexed!";
  }
};  // GammaPerfTest

}  // namespace test

int FindCPUConf(string &str, const char *name, string &value) {
  std::string::size_type begin = str.find(name);
  std::string::size_type end = str.find('\n', begin);
  begin = str.find(':', begin);
  value = str.substr(begin + 2, end - begin - 2);
  return 0;
}

int GetCPUInfo(string &info) {
  string path = "/proc/cpuinfo";
  vector<char> data;
  data.reserve(100 * 1024);
  char buff[1024];
  FILE *fp = fopen(path.c_str(), "r");
  assert(fp != NULL);
  size_t n = 0;
  while ((n = fread(buff, 1, sizeof(buff), fp)) > 0) {
    data.insert(data.end(), buff, buff + n);
  }
  fclose(fp);
  string str(data.data(), data.size());
  string model_name;
  string cache_size;
  FindCPUConf(str, "model name", model_name);
  FindCPUConf(str, "cache size", cache_size);

  stringstream ss;
  ss << "model name: " << model_name << "\n";
  ss << "processors: " << get_nprocs() << "\n";
  ss << "cache size: " << cache_size;
  info = ss.str();
  return 0;
}

int GetMemInfo(string &info) {
  utils::MEM_PACK *mem_pack = utils::get_memoccupy();
  stringstream ss;
  ss << "total: " << mem_pack->total << "G\n"
     << "available: " << mem_pack->available << "G\n"
     << "use rate: " << mem_pack->used_rate << "%";
  free(mem_pack);
  info = ss.str();
  return 0;
}

int GetOSInfo(string &info) {
  string release_path = "/etc/redhat-release";
  long file_size = utils::get_file_size(release_path.c_str());
  if (file_size <= 0) return -1;
  char *buff = new char[file_size];
  utils::FileIO hlp(release_path);
  hlp.Open("rb");
  hlp.Read(buff, 1, file_size);
  if (buff[file_size - 1] == '\n') file_size--;
  info.assign(buff, file_size);
  delete[] buff;
  return 0;
}

int main(int argc, char **argv) {
  if (argc != 2) {
    cerr << "Usage: " << argv[0] << " conf_path" << endl;
    return -1;
  }

  string cpu_info, mem_info, os_info;
  GetCPUInfo(cpu_info);
  GetMemInfo(mem_info);
  GetOSInfo(os_info);
  stringstream ss;
  ss << "\n";
  ss << "-------------------OS info-------------------\n" << os_info << "\n";
  ss << "-------------------CPU info------------------\n" << cpu_info << "\n";
  ss << "-------------------Mem info------------------\n" << mem_info << "\n";
  string host_info = ss.str();
  LOG(INFO) << host_info;

  string log_dir = "logs";
  utils::remove_dir(log_dir.c_str());
  utils::make_dir(log_dir.c_str());
  SetLogDictionary(StringToByteArray(log_dir));
  string conf_path = argv[1];
  LOG(INFO) << "gamma perf conf path=" << conf_path;
  test::GammaPerfConf conf;
  if (conf.Parse(conf_path)) {
    LOG(ERROR) << "parse conf error";
    return -1;
  }
  test::GammaPerfTest *t = new test::GammaPerfTest(&conf);
  t->Init();
  t->Run();
  string result;
  t->GetResult(result);
  LOG(INFO) << host_info << result;
  return 0;
}
