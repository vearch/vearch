/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include <fstream>
#include <iostream>

#include "index/index.h"

using namespace std;

int SetLogDictionary(const std::string &log_dir);

namespace test {

static vearch::Index *gamma_index;

int64_t *labels = nullptr;
float *feature = nullptr;
string index_type;
size_t d;
size_t xb;
string path = "files";

float *fvecs_read(const char *fname, size_t *d_out, size_t *n_out) {
  FILE *f = fopen(fname, "r");
  if (!f) {
    fprintf(stderr, "could not open %s\n", fname);
    perror("");
    abort();
  }
  int d;
  fread(&d, 1, sizeof(int), f);
  // LOG(INFO) << "d = " << d;
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

int Evaluate() {
  size_t k, nq;
  float *query = fvecs_read("sift1M/sift_query.fvecs", &d, &nq);

  int n_1 = 0, n_10 = 0, n_100 = 0;
  size_t nq2;
  int *gt = (int *)fvecs_read("sift1M/sift_groundtruth.ivecs", &k, &nq2);
  assert(nq2 == nq || !"incorrect nb of ground truth entries");

  float *dis = new float[k * nq];
  labels = new int64_t[k * nq];
  vector<int> nprobes = {1, 5, 10, 20, 40, 80, 160, 320};
  // vector<int> nprobes = {80};

  for (size_t i = 0; i < nprobes.size(); i++) {
    int nprobe = nprobes[i];
    printf("nprobe: %d\n", nprobe);
    if (index_type == "IVFPQ") {
      vearch::IndexIVFPQ *index =
          dynamic_cast<vearch::IndexIVFPQ *>(gamma_index);
      index->nprobe = nprobe;
    }
    if (index_type == "IVFFLAT") {
      vearch::IndexIVFFlat *index =
          dynamic_cast<vearch::IndexIVFFlat *>(gamma_index);
      index->nprobe = nprobe;
    }
    double start = utils::getmillisecs();
    gamma_index->search(nq, query, k, dis, labels);
    double elap = utils::getmillisecs() - start;
    printf("average: %.4f ms, QPS: %d\n", elap / nq,
           (int)(1.0 / (elap / nq / 1000)));

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
  delete[] dis;
  delete[] labels;
  delete[] query;
  delete[] gt;
  return 0;
}

int Init() {
  feature = fvecs_read("sift1M/sift_base.fvecs", &d, &xb);

  std::string log_dir = "./log";
  // if don't set, log will be output to stdout
  SetLogDictionary(log_dir);

  if (index_type == "IVFPQ") {
    gamma_index =
        vearch::index_factory(128, "IVF2048,PQ32x8", faiss::METRIC_L2);
  }
  if (index_type == "IVFFLAT") {
    gamma_index = vearch::index_factory(128, "IVF2048,Flat", faiss::METRIC_L2);
  }
  return 0;
}

void Add() {
  double start = utils::getmillisecs();
  gamma_index->add(xb, feature);
  double elap = utils::getmillisecs() - start;
  printf("add %ld cost time: %.4f s\n", xb, elap / 1000);
}

int Train() {
  size_t xt = 100000;
  double start = utils::getmillisecs();
  gamma_index->train(xt, feature);
  double elap = utils::getmillisecs() - start;
  printf("train %ld cost time: %.4f s\n", xt, elap / 1000);
  return 0;
}

int Dump() {
  int ret = gamma_index->dump(test::path);
  return ret;
}

int Load() {
  int ret = gamma_index->load(test::path);
  return ret;
}

int Close() {
  delete[] feature;
  delete gamma_index;
  return 0;
}

}  // namespace test

int main(int argc, char **argv) {
  // if want to do dump and load test
  // run as bLoad = false
  // then set bLoad = true and run it again
  bool bLoad = false;
  if (not bLoad) {
    utils::remove_dir(test::path.c_str());
  }
  setvbuf(stdout, (char *)NULL, _IONBF, 0);
  if (argc != 2) {
    std::cout << "Usage: [Program] [IVFPQ/IVFFLAT]\n";
    return 1;
  }
  test::index_type = argv[1];

  if (test::Init()) return 0;
  if (bLoad) {
    test::Load();
    test::Evaluate();
  } else {
    test::Train();
    test::Add();
    test::Evaluate();
  }

  if (not bLoad) {
    test::Dump();
  }
  test::Close();

  return 0;
}
