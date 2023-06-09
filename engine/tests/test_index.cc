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

std::string kSSGParam = R"(
    {
        "knn_number_control" : 100,
        "knn_graph_K" : 20,
        "knn_graph_L" : 10,
        "knn_graph_iter" : 12,
        "knn_graph_S" : 1,
        "knn_graph_R" : 10,
        "ssg_L" : 10,
        "ssg_R" : 5,
        "ssg_Angle" : 6,
        "ssg_n_try" : 2,
        "metric_type" : "Cosine"
    }
    )";

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

TEST_F(GammaTest, SSG) {
  struct Options opt;
  opt.set_file(my_argv, my_argc);
  opt.retrieval_type = "SSG";
  opt.store_type = "MemoryOnly";
  opt.add_doc_num = 20000;
  opt.indexing_size = 5000;
  opt.retrieval_param = kSSGParam;
  ASSERT_EQ(TestIndexes(opt), 0);
  opt.b_load = true;
  ASSERT_EQ(TestIndexes(opt), 0);
}

TEST_F(GammaTest, IVFPQFastScan) {
  struct Options opt;
  opt.set_file(my_argv, my_argc);
  opt.retrieval_type = "IVFPQFastScan";
  opt.store_type = "Mmap";
  opt.retrieval_param = kIVFPQParam;
  ASSERT_EQ(TestIndexes(opt), 0);
  //   opt.b_load = true;
  //   ASSERT_EQ(TestIndexes(opt), 0);
}

TEST_F(GammaTest, IVFPQFastScan_MemoryOnly) {
  struct Options opt;
  opt.set_file(my_argv, my_argc);
  opt.retrieval_type = "IVFPQFastScan";
  opt.store_type = "MemoryOnly";
  opt.retrieval_param = kIVFPQParam;
  ASSERT_EQ(TestIndexes(opt), 0);
  //   opt.b_load = true;
  //   ASSERT_EQ(TestIndexes(opt), 0);
}

TEST_F(GammaTest, IVFPQFastScan_ROCKSDB) {
  struct Options opt;
  opt.set_file(my_argv, my_argc);
  opt.retrieval_type = "IVFPQFastScan";
  opt.store_type = "RocksDB";
  opt.retrieval_param = kIVFPQParam;
  ASSERT_EQ(TestIndexes(opt), 0);
  //   opt.b_load = true;
  //   ASSERT_EQ(TestIndexes(opt), 0);
}

#ifdef OPT_IVFPQ_RELAYOUT
TEST_F(GammaTest, RELAYOUT) {
  struct Options opt;
  opt.set_file(my_argv, my_argc);
  opt.retrieval_type = "IVFPQ_RELAYOUT";
  opt.retrieval_param = kIVFPQParam;
  opt.store_type = "Mmap";
  ASSERT_EQ(TestIndexes(opt), 0);
  opt.b_load = true;
  ASSERT_EQ(TestIndexes(opt), 0);
}

TEST_F(GammaTest, RELAYOUT_MEMORYONLY) {
  struct Options opt;
  opt.set_file(my_argv, my_argc);
  opt.retrieval_type = "IVFPQ_RELAYOUT";
  opt.retrieval_param = kIVFPQParam;
  opt.store_type = "MemoryOnly";
  ASSERT_EQ(TestIndexes(opt), 0);
  opt.b_load = true;
  ASSERT_EQ(TestIndexes(opt), 0);
}

TEST_F(GammaTest, RELAYOUT_ROCKSDB) {
  struct Options opt;
  opt.set_file(my_argv, my_argc);
  opt.retrieval_type = "IVFPQ_RELAYOUT";
  opt.store_type = "RocksDB";
  opt.retrieval_param = kIVFPQParam;
  ASSERT_EQ(TestIndexes(opt), 0);
  opt.b_load = true;
  ASSERT_EQ(TestIndexes(opt), 0);
}

TEST_F(GammaTest, x86IVFFLAT) {
  struct Options opt;
  opt.set_file(my_argv, my_argc);
  opt.retrieval_type = "x86IVFFLAT";
  opt.store_type = "RocksDB";
  opt.retrieval_param = kIVFPQOPQParam;
  ASSERT_EQ(TestIndexes(opt), 0);
  opt.b_load = true;
  ASSERT_EQ(TestIndexes(opt), 0);
}

#endif

}  // namespace test

int main(int argc, char **argv) {
  setvbuf(stdout, (char *)NULL, _IONBF, 0);
  ::testing::InitGoogleTest(&argc, argv);
  if (argc != 3 && argc != 4) {
    std::cout << "Usage: [Program] [profile_file] [vectors_file]\n";
    std::cout << "Usage: [Program] [profile_file] [vectors_file] [raw_data_type]\n";
    return 1;
  }
  ::testing::GTEST_FLAG(output) = "xml";
  test::GammaTest::Init(argc, argv);
  return RUN_ALL_TESTS();
}
