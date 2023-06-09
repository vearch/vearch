/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */
#include <stdint.h>
#include <stdlib.h>
#include <chrono>
#include <atomic>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>

#include "util/utils.h"
#include "storage/lru_cache.h"
#include "test.h"


using namespace std;

namespace test {

class GammaTest : public ::testing::Test {
 public:
  static int Init(int argc, char *argv[]) {
    GammaTest::my_argc = argc;
    GammaTest::my_argv = argv;
    return 0;
  }

 protected:
  GammaTest() {}

  ~GammaTest() override {}

  void SetUp() override {}

  void TearDown() override {}

  void *engine;

  static int my_argc;
  static char **my_argv;
};
class CacheOptions {
 public:
  std::atomic<int>   cur_key {-1};
  std::atomic<size_t> error_num {0};
  std::atomic<size_t> correct_num {0};
  std::atomic<size_t> timeout_num {0};
  std::unordered_map<int, int> update_doc;
  pthread_rwlock_t lck;

  std::string path;
  std::string table_name = "table_name";
  bool is_exist_file = false;
  int fd = -1;
  size_t offset = 0;

  uint64_t cache_capacity = 1024;                  // M
  int block_size = 64 * 1024;                      // 64k
  int max_key = 200000;
  int key_gap = 1000;
  int cache_gap = 2000000000 / (max_key / key_gap);
  double run_all_time = 3 * 60 * 1000;

  std::atomic<int> add_fail {0};
  std::atomic<int> update_fail {0};

  std::atomic<double> agv_spend_time {0};
  std::atomic<size_t> all_get_num  {0};
  std::atomic<double> hit_agv_spend_time {0};
  std::atomic<size_t> hit_all_get_num  {0};
  std::atomic<double> miss_agv_spend_time {0};
  std::atomic<size_t> miss_all_get_num  {0};

  std::atomic<size_t> no_getorset  {0};


  bool stop = false;
  int t_num = 20;


  CacheOptions() {
    path = std::string(getcwd(NULL, 0));
    std::string disk_file = path + "/" + table_name;
    if((access(disk_file.c_str(),F_OK)) != -1) {
      is_exist_file = true;
    }
    fd = open(disk_file.c_str(), O_RDWR | O_CREAT, 0666);
    pthread_rwlock_init(&lck, NULL);
  }

  ~CacheOptions() {
    pthread_rwlock_destroy(&lck);
    if(fd != -1){
      close(fd);
    }
  }

  int Key2CacheKey (int key) {
    int grp_id = key / key_gap;
    int cache_key = grp_id * cache_gap + key % key_gap;
    return cache_key;
  }

  int CacheKey2Key (int cache_key) {
    int grp_id = cache_key / cache_gap;
    int key = grp_id * key_gap + cache_key % cache_gap;
    return key;
  }
};

int GammaTest::my_argc = 0;
char **GammaTest::my_argv = nullptr;

std::string GetItem(CacheOptions* opt, int key) {
  std::vector<char> block(opt->block_size, 0);
  int time = opt->block_size / sizeof(uint64_t);
  for(int j = 0; j < time; ++j) {
    uint64_t val = (uint64_t)key + (uint64_t)(j % 3? -j % 100 : j);
    memcpy(block.data() + sizeof(val) * j, &val, sizeof(val));
  }
  return std::string(block.data(), block.size());
}

void MakeDiskFile(CacheOptions* opt) {
  if(opt->is_exist_file) {
    return;
  }
  std::vector<char> block(opt->block_size, 0);
  for(int i = 0; i < opt->max_key; ++i) {
    std::string item = GetItem(opt, i);
    pwrite(opt->fd, item.c_str(), item.size(), opt->offset);
    opt->offset += item.size();
  }
}

bool LoadItem(int cache_key, char *value, CacheOptions* opt) {
  int key = opt->CacheKey2Key(cache_key);
  size_t pos = (size_t)key * (opt->block_size);
  pread(opt->fd, value, opt->block_size, pos);
  return true;
}

void *DoGet(CacheOptions *opt, CacheBase<int, CacheOptions*> *cache){
  // char *cache_item = new char[opt->block_size];
  double begin_time = utils::getmillisecs();
  while(!opt->stop) {
    int key = rand() % opt->max_key;
    std::string correct_item = GetItem(opt, key);
    auto start_time = std::chrono::steady_clock::now();
    char *cache_item = nullptr;
    int cache_key = opt->Key2CacheKey(key);
    bool res = cache->Get(cache_key, cache_item);
    if(res) {
      auto end_time = std::chrono::steady_clock::now();
      auto time_span = std::chrono::duration_cast<std::chrono::duration<double>>(
                                                    end_time - start_time);
      opt->hit_agv_spend_time = (opt->hit_agv_spend_time * opt->hit_all_get_num + time_span.count()*1000) /
                                (opt->hit_all_get_num + 1);
      ++(opt->hit_all_get_num);
      opt->agv_spend_time = (opt->agv_spend_time * opt->all_get_num + time_span.count()*1000) /
                            (opt->all_get_num + 1);
      ++(opt->hit_all_get_num);
    } else {
      res = cache->SetOrGet(cache_key, cache_item, opt);
      if(res == false) {
        LOG(ERROR) << "LoadItem fail, key[" << key << "]";
        continue;
      }
      auto end_time = std::chrono::steady_clock::now();
      auto time_span = std::chrono::duration_cast<std::chrono::duration<double>>(
                                                    end_time - start_time);
      opt->miss_agv_spend_time = (opt->miss_agv_spend_time * opt->miss_all_get_num + time_span.count()*1000) /
                                 (opt->miss_all_get_num + 1);
      ++(opt->miss_all_get_num);
      opt->agv_spend_time = (opt->agv_spend_time * opt->all_get_num + time_span.count()*1000) /
                            (opt->all_get_num + 1);
      ++(opt->all_get_num);
    }

    if(strcmp(correct_item.c_str(), cache_item) == 0) {
      ++(opt->correct_num);
    }else {
      ++(opt->error_num);
    }
    if(opt->correct_num % 100000 == 0) {
      LOG(INFO) << "correct_num[" << opt->correct_num << "], error_num["
                << opt->error_num << "]";
      LOG(INFO) << "agv_spend_time[" << opt->agv_spend_time << "ms], hit_agv_spend_time["
                << opt->hit_agv_spend_time << "ms],miss_agv_spend_time[" 
                << opt->miss_agv_spend_time << "ms]";
    }
    double cur_time = utils::getmillisecs();
    if (cur_time - begin_time > opt->run_all_time) { break; }
  }
  return nullptr;
}

int TestLruCache() {
  CacheOptions opt;
  CacheBase<int, CacheOptions*> *lrucache = nullptr;
  lrucache = new LRUCache<int, CacheOptions*>("name", 
      (size_t)opt.cache_capacity, (size_t)opt.block_size, LoadItem);

  lrucache->Init();
  
  MakeDiskFile(&opt);
  std::thread *t = new std::thread[opt.t_num];
  
  for(int i = 0; i < opt.t_num; ++i) {
    LOG(INFO) << "run thread i:" << i;
    t[i] = std::thread(DoGet, &opt, lrucache);
  }

  for(int i = 0; i < opt.t_num; ++i) {
    t[i].join();
    if(i == 0) {
      opt.stop = true;
    }
  }
  delete[] t;
  t = nullptr;
  delete lrucache;
  lrucache = nullptr;
  sleep(2);
  return 0;
}

int TestSimpleCache() {
  CacheOptions opt;
  CacheBase<int, CacheOptions*> *simple_cache = nullptr;
  simple_cache = new SimpleCache<int, CacheOptions*>("name",
      (size_t)opt.block_size, LoadItem, (size_t)opt.cache_gap);
  simple_cache->Init();
  
  MakeDiskFile(&opt);
  std::thread *t = new std::thread[opt.t_num];
  
  for(int i = 0; i < opt.t_num; ++i) {
    LOG(INFO) << "run thread i:" << i;
    t[i] = std::thread(DoGet, &opt, simple_cache);
  }

  for(int i = 0; i < opt.t_num; ++i) {
    t[i].join();
    if(i == 0) {
      opt.stop = true;
    }
  }
  delete[] t;
  t = nullptr;
  delete simple_cache;
  simple_cache = nullptr;
  sleep(2);
  return 0;
}

TEST_F(GammaTest, LruCache) {
  ASSERT_EQ(TestLruCache(), 0);
}

TEST_F(GammaTest, SimpleCache) {
  ASSERT_EQ(TestSimpleCache(), 0);
}

}  // namespace test


int main(int argc, char *argv[]) {
  setvbuf(stdout, (char *)NULL, _IONBF, 0);
  ::testing::InitGoogleTest(&argc, argv);
  if (argc != 1) {
    std::cout << "Usage: [Program]\n";
    return 1;
  }
  ::testing::GTEST_FLAG(output) = "xml";
  test::GammaTest::Init(argc, argv);
  return RUN_ALL_TESTS();
}
