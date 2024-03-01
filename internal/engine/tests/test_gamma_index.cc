/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */
#ifdef FAISSLIKE_INDEX
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cmath>
#include <fstream>
#include <functional>
#include <future>
#include <iostream>

#include "faiss/MetricType.h"
#include "index/index.h"
#include "test.h"
#include "util/utils.h"

/**
 * To run this demo, please download the ANN_SIFT10K dataset from
 *
 *   ftp://ftp.irisa.fr/local/texmex/corpus/siftsmall.tar.gz
 *
 * and unzip it.
 **/

using namespace std;

int SetLogDictionary(const std::string &log_dir);

int read_bvecs(string path, size_t nq, size_t d, float *query) {
  unsigned char row[2048];

  int fd = open(path.c_str(), O_RDONLY, 0);
  for (size_t i = 0; i < nq; i++) {
    int in = 0;
    read(fd, (char *)&in, 4);
    if (in != 128) {
      cout << "file error";
      exit(1);
    }
    read(fd, (char *)row, in);
    for (size_t j = 0; j < d; j++) {
      query[i * d + j] = row[j];
    }
  }
  close(fd);
  return 0;
}

namespace Test {

class IndexRecallTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {}
  static void TearDownTestSuite() {}
  virtual void SetUp() {}

  virtual void TearDown() {
    CHECK_DELETE_ARRAY(feature);
    CHECK_DELETE_ARRAY(query);
    CHECK_DELETE_ARRAY(gt);
  }

  int Evaluate(int batch = 1) {
    int n_1 = 0, n_10 = 0, n_100 = 0;
    float *dis = new float[k * nq];
    int64_t *labels = new int64_t[k * nq];
    vector<int> nprobes = {1, 5, 10, 20, 40, 80};
    // vector<int> nprobes = {1, 5, 10, 20, 40, 80, 160, 320, 640};
    // vector<int> nprobes = {80};

    for (size_t i = 0; i < nprobes.size(); i++) {
      int nprobe = nprobes[i];
      printf("nprobe: %d\n", nprobe);
      if (retrieval_type == "IVFPQ_RELAYOUT" || retrieval_type == "IVFPQ") {
        vearch::IndexIVFPQ *index =
            dynamic_cast<vearch::IndexIVFPQ *>(gamma_index);
        index->nprobe = nprobe;
      }
      if (retrieval_type == "x86IVFFLAT" || retrieval_type == "IVFFLAT") {
        vearch::IndexIVFFlat *index =
            dynamic_cast<vearch::IndexIVFFlat *>(gamma_index);
        index->nprobe = nprobe;
      }
#ifdef USE_SCANN
      if (retrieval_type == "Scann") {
        vearch::IndexScann *index =
            dynamic_cast<vearch::IndexScann *>(gamma_index);
        index->leaves_ = nprobe;
      }
#endif  // USE_SCANN
      double start = utils::getmillisecs();
      if (batch) {
        gamma_index->search(nq, query, k, dis, labels);
      } else {
        for (size_t i = 0; i < nq; i++)
          gamma_index->search(1, query + i * d, k, dis + i * k, labels + i * k);
      }
      double elap = utils::getmillisecs() - start;
      printf("average: %.4f ms, QPS: %d\n", elap / 10000,
             (int)(1.0 / (elap / 10000 / 1000)));

      n_1 = 0, n_10 = 0, n_100 = 0;

      for (size_t i = 0; i < nq; i++) {
        int gt_nn = gt[i * k];
        for (size_t j = 0; j < k; j++) {
          if (labels[i * k + j] == gt_nn) {
            if (j < 1) n_1++;
            if (j < 10) n_10++;
            if (j < 100) n_100++;
          }
        }
      }
      printf("R@1 = %.4f, R@10 = %.4f, R@100 = %.4f\n", n_1 / float(nq),
             n_10 / float(nq), n_100 / float(nq));
    }
    CHECK_DELETE_ARRAY(dis);
    CHECK_DELETE_ARRAY(labels);
    return 0;
  }

  int Create(std::string index_type, std::string data_type) {
    retrieval_type = index_type;
    SetLogDictionary("./log");
    size_t nlist = 2048;
    faiss::MetricType metric_type;
    if (data_type == "sift1m") {
      d = 128;
      metric_type = faiss::METRIC_L2;
    }
    if (data_type == "vgg1m") {
      d = 512;
      metric_type = faiss::METRIC_INNER_PRODUCT;
    }

    if (retrieval_type == "IVFPQ_RELAYOUT" || retrieval_type == "IVFPQ") {
      // gamma_index = vearch::index_factory(128, "IVF2048,PQ32x8",
      // faiss::METRIC_L2);
      gamma_index =
          new vearch::IndexIVFPQ(nullptr, d, nlist, d / 4, 8, metric_type);
      // gamma_index = new vearch::IndexIVFPQ(nullptr, 128, 2048, 32, 8,
      // faiss::METRIC_INNER_PRODUCT);
    }
#ifdef USE_SCANN
    if (retrieval_type == "Scann") {
      gamma_index = new vearch::IndexScann(d, nlist, d / 2, metric_type);
    }
#endif  // USE_SCANN
    if (retrieval_type == "x86IVFFLAT" || retrieval_type == "IVFFLAT") {
      // std::string param = "IVF" + std::to_string(nlist) + ",Flat";
      // gamma_index = vearch::index_factory(d, param.c_str(),
      // faiss::METRIC_L2);
      gamma_index = new vearch::IndexIVFFlat(nullptr, d, nlist, metric_type);
      // gamma_index = new vearch::IndexIVFFlat(nullptr, 128, 2048,
      // faiss::METRIC_INNER_PRODUCT);
    }
    if (!gamma_index) return -1;
    if (data_type == "sift1m") {
      feature_file = "./sift1m/sift_base.fvecs";
      query_file = "./sift1m/sift_query.fvecs";
      groundtruth_file = "./sift1m/sift_groundtruth.ivecs";
      feature = fvecs_read(feature_file.c_str(), &d, &nb);
      query = fvecs_read(query_file.c_str(), &d, &nq);
      size_t nq2;
      gt = (int *)fvecs_read(groundtruth_file.c_str(), &k, &nq2);
      assert(nq2 == nq || !"incorrect nb of ground truth entries");
    } else if (data_type == "vgg1m") {
      d = 512;
      k = 100;
      feature_file = "./vgg1m/vgg_base.dat";
      query_file = "./vgg1m/vgg_query.dat";
      groundtruth_file = "./vgg1m/vgg_groundtruth.dat";
      feature = fdat_read(feature_file.c_str(), d, &nb);
      query = fdat_read(query_file.c_str(), d, &nq);
      size_t nq2;
      gt = (int *)fdat_read(groundtruth_file.c_str(), k, &nq2);
      assert(nq2 == nq || !"incorrect nb of ground truth entries");
    }
    std::cout << "n [" << nb << "]" << std::endl;
    return 0;
  }

  int Fit() {
    if (retrieval_type == "Scann") {
      double start = utils::getmillisecs();
      gamma_index->train(nb, feature);
      double elap = utils::getmillisecs() - start;
      printf("train time: %.4f s\n", elap / 1000);
    } else {
      double start = utils::getmillisecs();
      gamma_index->train(10 * 10000, feature);
      double elap = utils::getmillisecs() - start;
      printf("train time: %.4f s\n", elap / 1000);

      start = utils::getmillisecs();
      gamma_index->add(nb, feature);
      elap = utils::getmillisecs() - start;
      printf("add time: %.4f s\n", elap / 1000);
    }
    return 0;
  }

  int Dump() {
    std::string dir = "faisslike_index";
    int ret = gamma_index->dump(dir);
    return ret;
  }

  int Load() {
    std::string dir = "faisslike_index";
    int ret = gamma_index->load(dir);
    return ret;
  }

  vearch::Index *gamma_index = nullptr;
  float *feature = nullptr;
  float *query = nullptr;
  int *gt = nullptr;
  size_t d;
  size_t k;
  size_t nq;
  size_t nb;
  std::string data_type;
  std::string feature_file;
  std::string query_file;
  std::string groundtruth_file;
  std::string retrieval_type;
};
/*
TEST_F(IndexRecallTest, IVFPQ) {
#ifdef OPT_IVFPQ_RELAYOUT
  ASSERT_EQ(0, Create("IVFPQ_RELAYOUT", "sift1m"));
#else
  ASSERT_EQ(0, Create("IVFPQ", "sift1m"));
#endif // OPT_IVFPQ_RELAYOUT
  ASSERT_EQ(0, Fit());
  ASSERT_EQ(0, Evaluate());
  ASSERT_EQ(0, Evaluate(0));
  ASSERT_EQ(0, Dump());
}

TEST_F(IndexRecallTest, IVFPQ_LOAD) {
#ifdef OPT_IVFPQ_RELAYOUT
  ASSERT_EQ(0, Create("IVFPQ_RELAYOUT", "sift1m"));
#else
  ASSERT_EQ(0, Create("IVFPQ", "sift1m"));
#endif // OPT_IVFPQ_RELAYOUT
  ASSERT_EQ(add_doc_num, Load());
  ASSERT_EQ(0, Evaluate());
  ASSERT_EQ(0, Evaluate(0));
}

TEST_F(IndexRecallTest, IVFFLAT) {
#ifdef OPT_IVFPQ_RELAYOUT
  ASSERT_EQ(0, Create("x86IVFFLAT", "sift1m"));
#else
  ASSERT_EQ(0, Create("IVFFLAT", "sift1m"));
#endif // OPT_IVFPQ_RELAYOUT
  ASSERT_EQ(0, Fit());
  ASSERT_EQ(0, Evaluate());
  ASSERT_EQ(0, Evaluate(0));
  ASSERT_EQ(0, Dump());
}

TEST_F(IndexRecallTest, IVFFLAT_LOAD) {
#ifdef OPT_IVFPQ_RELAYOUT
  ASSERT_EQ(0, Create("x86IVFFLAT", "sift1m"));
#else
  ASSERT_EQ(0, Create("IVFFLAT", "sift1m"));
#endif // OPT_IVFPQ_RELAYOUT
  ASSERT_EQ(add_doc_num, Load());
  ASSERT_EQ(0, Evaluate());
  ASSERT_EQ(0, Evaluate(0));
}
*/
TEST_F(IndexRecallTest, IVFPQ) {
#ifdef OPT_IVFPQ_RELAYOUT
  ASSERT_EQ(0, Create("IVFPQ_RELAYOUT", "vgg1m"));
#else
  ASSERT_EQ(0, Create("IVFPQ", "vgg1m"));
#endif  // OPT_IVFPQ_RELAYOUT
  ASSERT_EQ(0, Fit());
  ASSERT_EQ(0, Evaluate());
  ASSERT_EQ(0, Evaluate(0));
}

#ifdef USE_SCANN
TEST_F(IndexRecallTest, Scann_VGG1M) {
  ASSERT_EQ(0, Create("Scann", "vgg1m"));
  ASSERT_EQ(0, Fit());
  ASSERT_EQ(0, Evaluate());
  ASSERT_EQ(0, Evaluate(0));
}

TEST_F(IndexRecallTest, Scann_SIFT1M) {
  ASSERT_EQ(0, Create("Scann", "sift1m"));
  ASSERT_EQ(0, Fit());
  ASSERT_EQ(0, Evaluate());
  ASSERT_EQ(0, Evaluate(0));
}
#endif  // USE_SCANN

}  // namespace Test

#endif  // FAISSLIKE_API
