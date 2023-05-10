/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "test.h"

/**
 * To run this demo, please download the ANN_SIFT10K dataset from
 *
 *   ftp://ftp.irisa.fr/local/texmex/corpus/siftsmall.tar.gz
 *
 * and unzip it.
 **/

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

TEST_F(GammaTest, IVFPQ) {
  struct Options opt;
  opt.profile_file = my_argv[1];
  opt.feature_file = my_argv[2];
  opt.retrieval_type = "IVFPQ";
  opt.retrieval_param = kIVFPQParam;
  ASSERT_EQ(TestIndexes(opt), 0);

  opt.b_load = true;
  ASSERT_EQ(TestIndexes(opt), 0);
}

TEST_F(GammaTest, IVFPQ_BATCH) {
  struct Options opt;
  opt.profile_file = my_argv[1];
  opt.feature_file = my_argv[2];
  opt.retrieval_type = "IVFPQ";
  opt.retrieval_param = kIVFPQParam;
  opt.add_type = 1;
  ASSERT_EQ(TestIndexes(opt), 0);

  opt.b_load = true;
  ASSERT_EQ(TestIndexes(opt), 0);
}

TEST_F(GammaTest, IVFPQ_MEMORYONLY) {
  struct Options opt;
  opt.profile_file = my_argv[1];
  opt.feature_file = my_argv[2];
  opt.retrieval_type = "IVFPQ";
  opt.retrieval_param = kIVFPQParam;
  opt.store_type = "MemoryOnly";
  ASSERT_EQ(TestIndexes(opt), 0);

  opt.b_load = true;
  ASSERT_EQ(TestIndexes(opt), 0);
}

TEST_F(GammaTest, IVFPQ_MEMORYONLY_BATCH) {
  struct Options opt;
  opt.profile_file = my_argv[1];
  opt.feature_file = my_argv[2];
  opt.retrieval_type = "IVFPQ";
  opt.retrieval_param = kIVFPQParam;
  opt.store_type = "MemoryOnly";
  opt.add_type = 1;
  ASSERT_EQ(TestIndexes(opt), 0);
  
  opt.b_load = true;
  ASSERT_EQ(TestIndexes(opt), 0);
}

TEST_F(GammaTest, IVFPQ_ROCKSDB) {
  struct Options opt;
  opt.profile_file = my_argv[1];
  opt.feature_file = my_argv[2];
  opt.retrieval_type = "IVFPQ";
  opt.retrieval_param = kIVFPQParam;
  opt.store_type = "RocksDB";
  ASSERT_EQ(TestIndexes(opt), 0);

  opt.b_load = true;
  ASSERT_EQ(TestIndexes(opt), 0);
}

TEST_F(GammaTest, IVFPQ_ROCKSDB_BATCH) {
  struct Options opt;
  opt.profile_file = my_argv[1];
  opt.feature_file = my_argv[2];
  opt.retrieval_type = "IVFPQ";
  opt.retrieval_param = kIVFPQParam;
  opt.store_type = "RocksDB";
  opt.add_type = 1;
  ASSERT_EQ(TestIndexes(opt), 0);

  opt.b_load = true;
  ASSERT_EQ(TestIndexes(opt), 0);
}

TEST_F(GammaTest, IVFPQ_HNSW) {
  struct Options opt;
  opt.profile_file = my_argv[1];
  opt.feature_file = my_argv[2];
  opt.retrieval_type = "IVFPQ";
  opt.retrieval_param = kIVFHNSWPQParam;
  ASSERT_EQ(TestIndexes(opt), 0);

  opt.b_load = true;
  ASSERT_EQ(TestIndexes(opt), 0);
}

TEST_F(GammaTest, IVFPQ_HNSW_BATCH) {
  struct Options opt;
  opt.profile_file = my_argv[1];
  opt.feature_file = my_argv[2];
  opt.retrieval_type = "IVFPQ";
  opt.retrieval_param = kIVFHNSWPQParam;
  opt.add_type = 1;
  ASSERT_EQ(TestIndexes(opt), 0);

  opt.b_load = true;
  ASSERT_EQ(TestIndexes(opt), 0);
}

TEST_F(GammaTest, IVFPQ_HNSW_OPQ) {
  struct Options opt;
  opt.profile_file = my_argv[1];
  opt.feature_file = my_argv[2];
  opt.retrieval_type = "IVFPQ";
  opt.retrieval_param = kIVFHNSWOPQParam;
  ASSERT_EQ(TestIndexes(opt), 0);

  opt.b_load = true;
  ASSERT_EQ(TestIndexes(opt), 0);
}

TEST_F(GammaTest, IVFPQ_HNSW_OPQ_BATCH) {
  struct Options opt;
  opt.profile_file = my_argv[1];
  opt.feature_file = my_argv[2];
  opt.retrieval_type = "IVFPQ";
  opt.retrieval_param = kIVFHNSWOPQParam;
  opt.add_type = 1;
  ASSERT_EQ(TestIndexes(opt), 0);

  opt.b_load = true;
  ASSERT_EQ(TestIndexes(opt), 0);
}

TEST_F(GammaTest, IVFFLAT) {
  struct Options opt;
  opt.profile_file = my_argv[1];
  opt.feature_file = my_argv[2];
  opt.retrieval_type = "IVFFLAT";
  opt.retrieval_param = kIVFPQParam;
  opt.store_type = "RocksDB";
  ASSERT_EQ(TestIndexes(opt), 0);
  
  opt.b_load = true;
  ASSERT_EQ(TestIndexes(opt), 0);
}

TEST_F(GammaTest, IVFFLAT_BATCH) {
  struct Options opt;
  opt.profile_file = my_argv[1];
  opt.feature_file = my_argv[2];
  opt.retrieval_type = "IVFFLAT";
  opt.retrieval_param = kIVFPQParam;
  opt.store_type = "RocksDB";
  opt.add_type = 1;
  ASSERT_EQ(TestIndexes(opt), 0);
  
  opt.b_load = true;
  ASSERT_EQ(TestIndexes(opt), 0);
}

TEST_F(GammaTest, FLAT) {
  struct Options opt;
  opt.profile_file = my_argv[1];
  opt.feature_file = my_argv[2];
  opt.retrieval_type = "FLAT";
  opt.retrieval_param = kIVFPQParam;
  opt.store_type = "MemoryOnly";
  opt.add_doc_num = 100000;
  ASSERT_EQ(TestIndexes(opt), 0);
  
  opt.b_load = true;
  ASSERT_EQ(TestIndexes(opt), 0);
}

TEST_F(GammaTest, FLAT_BATCH) {
  struct Options opt;
  opt.profile_file = my_argv[1];
  opt.feature_file = my_argv[2];
  opt.retrieval_type = "FLAT";
  opt.retrieval_param = kIVFPQParam;
  opt.store_type = "MemoryOnly";
  opt.add_doc_num = 100000;
  opt.add_type = 1;
  ASSERT_EQ(TestIndexes(opt), 0);
  
  opt.b_load = true;
  ASSERT_EQ(TestIndexes(opt), 0);
}

TEST_F(GammaTest, HNSW) {
  struct Options opt;
  opt.profile_file = my_argv[1];
  opt.feature_file = my_argv[2];
  opt.retrieval_type = "HNSW";
  opt.retrieval_param = kHNSWParam;
  opt.store_type = "MemoryOnly";
  opt.add_doc_num = 20000;
  ASSERT_EQ(TestIndexes(opt), 0);
  
  opt.b_load = true;
  ASSERT_EQ(TestIndexes(opt), 0);
}

TEST_F(GammaTest, HNSW_BATCH) {
  struct Options opt;
  opt.profile_file = my_argv[1];
  opt.feature_file = my_argv[2];
  opt.retrieval_type = "HNSW";
  opt.retrieval_param = kHNSWParam;
  opt.store_type = "MemoryOnly";
  opt.add_doc_num = 20000;
  opt.add_type = 1;
  ASSERT_EQ(TestIndexes(opt), 0);
  
  opt.b_load = true;
  ASSERT_EQ(TestIndexes(opt), 0);
}

TEST_F(GammaTest, BINARYIVF) {
  struct Options opt;
  opt.profile_file = my_argv[1];
  opt.feature_file = my_argv[2];
  opt.retrieval_type = "BINARYIVF";
  opt.retrieval_param = kIVFBINARYParam;
  ASSERT_EQ(TestIndexes(opt), 0);

  opt.b_load = true;
  ASSERT_EQ(TestIndexes(opt), 0);
}

TEST_F(GammaTest, BINARYIVF_BATCH) {
  struct Options opt;
  opt.profile_file = my_argv[1];
  opt.feature_file = my_argv[2];
  opt.retrieval_type = "BINARYIVF";
  opt.retrieval_param = kIVFBINARYParam;
  opt.add_type = 1;
  ASSERT_EQ(TestIndexes(opt), 0);
  
  opt.b_load = true;
  ASSERT_EQ(TestIndexes(opt), 0);
}

#ifdef USE_SCANN
TEST_F(GammaTest, SCANN_ROCKSDB) {
  struct Options opt;
  opt.profile_file = my_argv[1];
  opt.feature_file = my_argv[2];
  opt.retrieval_type = "VEARCH";
  opt.retrieval_param = kSCANNParam;
  opt.store_type = "RocksDB";
  ASSERT_EQ(TestIndexes(opt), 0);
}

TEST_F(GammaTest, SCANN_THREADPOOL_ROCKSDB) {
  struct Options opt;
  opt.profile_file = my_argv[1];
  opt.feature_file = my_argv[2];
  opt.retrieval_type = "VEARCH";
  opt.retrieval_param = kSCANNWithThreadPoolParam;
  opt.store_type = "RocksDB";
  ASSERT_EQ(TestIndexes(opt), 0);
}
#endif

#ifdef BUILD_WITH_GPU
TEST_F(GammaTest, GPU) {
  struct Options opt;
  opt.profile_file = my_argv[1];
  opt.feature_file = my_argv[2];
  opt.retrieval_type = "GPU";
  opt.retrieval_param = kIVFPQParam;
  ASSERT_EQ(TestIndexes(opt), 0);

  opt.b_load = true;
  ASSERT_EQ(TestIndexes(opt), 0);
}

TEST_F(GammaTest, GPU_BATCH) {
  struct Options opt;
  opt.profile_file = my_argv[1];
  opt.feature_file = my_argv[2];
  opt.retrieval_type = "GPU";
  opt.retrieval_param = kIVFPQParam;
  opt.add_type = 1;
  ASSERT_EQ(TestIndexes(opt), 0);

  opt.b_load = true;
  ASSERT_EQ(TestIndexes(opt), 0);
}

TEST_F(GammaTest, GPU_MEMORYONLY) {
  struct Options opt;
  opt.profile_file = my_argv[1];
  opt.feature_file = my_argv[2];
  opt.retrieval_type = "GPU";
  opt.store_type = "MemoryOnly";
  opt.retrieval_param = kIVFPQParam;
  ASSERT_EQ(TestIndexes(opt), 0);

  opt.b_load = true;
  ASSERT_EQ(TestIndexes(opt), 0);
}

TEST_F(GammaTest, GPU_MEMORYONLY_BATCH) {
  struct Options opt;
  opt.profile_file = my_argv[1];
  opt.feature_file = my_argv[2];
  opt.retrieval_type = "GPU";
  opt.store_type = "MemoryOnly";
  opt.retrieval_param = kIVFPQParam;
  opt.add_type = 1;
  ASSERT_EQ(TestIndexes(opt), 0);

  opt.b_load = true;
  ASSERT_EQ(TestIndexes(opt), 0);
}

TEST_F(GammaTest, GPU_ROCKSDB) {
  struct Options opt;
  opt.profile_file = my_argv[1];
  opt.feature_file = my_argv[2];
  opt.retrieval_type = "GPU";
  opt.store_type = "RocksDB";
  opt.retrieval_param = kIVFPQParam;
  ASSERT_EQ(TestIndexes(opt), 0);

  opt.b_load = true;
  ASSERT_EQ(TestIndexes(opt), 0);
}

TEST_F(GammaTest, GPU_ROCKSDB_BATCH) {
  struct Options opt;
  opt.profile_file = my_argv[1];
  opt.feature_file = my_argv[2];
  opt.retrieval_type = "GPU";
  opt.store_type = "RocksDB";
  opt.retrieval_param = kIVFPQParam;
  opt.add_type = 1;
  ASSERT_EQ(TestIndexes(opt), 0);

  opt.b_load = true;
  ASSERT_EQ(TestIndexes(opt), 0);
}
#endif

}  // namespace test

int main(int argc, char **argv) {
  setvbuf(stdout, (char *)NULL, _IONBF, 0);
  ::testing::InitGoogleTest(&argc, argv);
  if (argc != 3) {
    std::cout << "Usage: [Program] [profile_file] [vectors_file]\n";
    return 1;
  }
  ::testing::GTEST_FLAG(output) = "xml";
  test::GammaTest::Init(argc, argv);
  return RUN_ALL_TESTS();
}
