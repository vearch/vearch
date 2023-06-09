/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include <stdint.h>
#include <stdlib.h>
#include "storage/compress/compressor.h"
#include "storage/compress/compressor_zfp.h"
#include "storage/compress/compressor_zstd.h"
#include "test.h"


using namespace std;
using namespace tig_gamma;

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

int GammaTest::my_argc = 0;
char **GammaTest::my_argv = nullptr;

void test_compress_zstd(int data_len) {
  Compressor *zstd = new CompressorZSTD(CompressType::Zstd);
  std::vector<char> vec(data_len);
  for(size_t i = 0; i < vec.size()/sizeof(int); ++i) {
    int val = rand() % 1000;
    memcpy(vec.data() + sizeof(int) * i, &val, sizeof(int));
  }
  size_t cmprs_len = zstd->GetCompressLen(vec.size());
  char *cmprs_buf = new char[cmprs_len];
  
  auto start_time = std::chrono::steady_clock::now();
  cmprs_len = zstd->Compress(vec.data(), cmprs_buf, vec.size());
  auto end_time = std::chrono::steady_clock::now();
  auto time_span = std::chrono::duration_cast<std::chrono::duration<double>>(
                                                    end_time - start_time);
  LOG(INFO) << "zstd compress spend time: " << time_span.count() * 1000 << "ms";

  std::vector<char> de_vec(vec.size());
  start_time = std::chrono::steady_clock::now();
  zstd->Decompress(cmprs_buf, de_vec.data(), cmprs_len);
  end_time = std::chrono::steady_clock::now();
  time_span = std::chrono::duration_cast<std::chrono::duration<double>>(
                                                    end_time - start_time);
  LOG(INFO) << "zstd decompress spend time: " << time_span.count() * 1000 
            << "ms, compress rate:" << (float)vec.size() / cmprs_len;


  for(size_t i = 0; i < vec.size(); ++i){
    if(vec[i] != de_vec[i]){
      LOG(INFO) << "zstd decompress error";
    }
  }
  delete[] cmprs_buf;
  delete zstd;
}

void test_compress_zfp(int d) {
  Compressor *zfp = new CompressorZFP(CompressType::Zfp);
  zfp->Init(d);
  std::vector<int> vec(d);
  for(size_t i = 0; i < vec.size(); ++i) {
    vec[i] = rand() % 1000;
  }
  size_t cmprs_len = zfp->GetCompressLen();
  LOG(ERROR) << "ZFP comprs_len:" << cmprs_len; 
  char *cmprs_buf = new char[cmprs_len];
  
  auto start_time = std::chrono::steady_clock::now();
  cmprs_len = zfp->Compress((char*)vec.data(), cmprs_buf, sizeof(int) * vec.size());
  auto end_time = std::chrono::steady_clock::now();
  auto time_span = std::chrono::duration_cast<std::chrono::duration<double>>(
                                                    end_time - start_time);
  LOG(INFO) << "zfp compress spend time: " << time_span.count() * 1000 << "ms";

  std::vector<int> de_vec(d);
  start_time = std::chrono::steady_clock::now();
  zfp->Decompress(cmprs_buf, (char*)de_vec.data(), cmprs_len);
  end_time = std::chrono::steady_clock::now();
  time_span = std::chrono::duration_cast<std::chrono::duration<double>>(
                                                    end_time - start_time);
  LOG(INFO) << "zfp decompress spend time: " << time_span.count() * 1000 
            << "ms, compress rate:" << (float)vec.size() / (cmprs_len/sizeof(int));

  int error_num = 0;
  for(size_t i = 0; i < vec.size(); ++i) {
    if(vec[i] != de_vec[i]) {
      ++error_num;
    }
  }
  LOG(INFO) << "zfp decompress error_num:" << error_num;

  delete[] cmprs_buf;
  delete zfp;
}

void test_compress_zfp_batch(int d, int n) {
  Compressor *zfp = new CompressorZFP(CompressType::Zfp);
  zfp->Init(d);
  std::vector<int> vec(d * n);
  for(size_t i = 0; i < vec.size(); ++i) {
    vec[i] = rand() % 1000;
  }
  size_t cmprs_len = zfp->GetCompressLen();
  LOG(ERROR) << "ZFP comprs_len:" << cmprs_len; 
  char *cmprs_buf = new char[cmprs_len * n];
  
  auto start_time = std::chrono::steady_clock::now();
  cmprs_len = zfp->CompressBatch((char*)vec.data(), cmprs_buf, n, -1);
  auto end_time = std::chrono::steady_clock::now();
  auto time_span = std::chrono::duration_cast<std::chrono::duration<double>>(
                                                    end_time - start_time);
  LOG(INFO) << "zfp compress spend time: " << time_span.count() * 1000 << "ms";

  std::vector<int> de_vec(vec.size());
  start_time = std::chrono::steady_clock::now();
  zfp->DecompressBatch(cmprs_buf, (char*)de_vec.data(), n, -1);
  end_time = std::chrono::steady_clock::now();
  time_span = std::chrono::duration_cast<std::chrono::duration<double>>(
                                                    end_time - start_time);
  LOG(INFO) << "zfp decompress spend time: " << time_span.count() * 1000 
            << "ms, compress rate:" << (float)vec.size() / (cmprs_len/sizeof(int));

  int error_num = 0;
  for(size_t i = 0; i < vec.size(); ++i) {
    if(vec[i] != de_vec[i]) {
      ++error_num;
    }
  }
  LOG(INFO) << "zfp decompress error_num:" << error_num;

  delete[] cmprs_buf;
  delete zfp;
}

int TestStorageCompress() {
  int data_len = 2048;
  test_compress_zstd(data_len);
  test_compress_zfp(data_len / sizeof(int));
  test_compress_zfp_batch(data_len / sizeof(int), 100);
  return 0;
}

TEST_F(GammaTest, StorageCompress) {
  ASSERT_EQ(TestStorageCompress(), 0);
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
