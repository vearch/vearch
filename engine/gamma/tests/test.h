/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include <faiss/utils/utils.h>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include "log.h"

#include "bitmap.h"
#include "c_api/gamma_api.h"
#include "util/utils.h"

using std::string;
using namespace std;

IVFPQParameters *kIVFPQParam =
    MakeIVFPQParameters(InnerProduct, 20, 256, 64, 8);

IVFPQParameters *GetIVFPQParam() {
  return MakeIVFPQParameters(InnerProduct, 20, 256, 64, 8);
}

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

string ByteArrayToString(const ByteArray *ba) {
  assert(ba != nullptr);
  if (ba->value == nullptr || ba->len <= 0) return string("");
  return string(ba->value, ba->len);
}

int ByteArrayToInt(const ByteArray *ba) {
  assert(ba != nullptr);
  int data = 0;
  memcpy(&data, ba->value, ba->len);
  return data;
}

template <typename T>
inline ByteArray *ToByteArray(const T v) {
  ByteArray *ba = static_cast<ByteArray *>(malloc(sizeof(ByteArray)));
  ba->value = static_cast<char *>(malloc(sizeof(T)));
  ba->len = sizeof(T);
  memcpy(ba->value, &v, ba->len);
  return ba;
}

void printDoc(const Doc *doc, std::string &msg) {
  for (int j = 0; j < doc->fields_num; ++j) {
    struct Field *field_value = GetField(doc, j);
    if (field_value->data_type == INT) {
      msg += "field name [" +
             string(field_value->name->value, field_value->name->len) +
             "], value [" +
             std::to_string(*((int *)field_value->value->value)) + "], type [" +
             std::to_string(field_value->data_type) + "], ";
    } else if (field_value->data_type == LONG) {
      msg += "field name [" +
             string(field_value->name->value, field_value->name->len) +
             "], value [" +
             std::to_string(*((long *)field_value->value->value)) +
             "], type [" + std::to_string(field_value->data_type) + "], ";
    } else if (field_value->data_type == FLOAT) {
      msg += "field name [" +
             string(field_value->name->value, field_value->name->len) +
             "], value [" +
             std::to_string(*((float *)field_value->value->value)) +
             "], type [" + std::to_string(field_value->data_type) + "], ";
    } else if (field_value->data_type == DOUBLE) {
      msg += "field name [" +
             string(field_value->name->value, field_value->name->len) +
             "], value [" +
             std::to_string(*((double *)field_value->value->value)) +
             "], type [" + std::to_string(field_value->data_type) + "], ";
    } else if (field_value->data_type == STRING) {
      msg += "field name [" +
             string(field_value->name->value, field_value->name->len) +
             "], value [" +
             string(field_value->value->value, field_value->value->len) +
             "], type [" + std::to_string(field_value->data_type) + "], ";
    } else if (field_value->data_type == VECTOR) {
      string str_vec;
      int d = -1;
      memcpy((void *)&d, field_value->value->value, sizeof(int));

      d /= sizeof(float);
      int cur = sizeof(int);

      float *feature =
          reinterpret_cast<float *>(field_value->value->value + cur);

      cur += d * sizeof(float);
      int len = field_value->value->len;
      char source[len - cur];

      memcpy(source, field_value->value->value + cur, len - cur);

      for (int i = 0; i < d; ++i) {
        str_vec += std::to_string(feature[i]) + ",";
      }
      str_vec.pop_back();

      std::string source_str = std::string(source, len - cur);
      msg += "field name [" +
             string(field_value->name->value, field_value->name->len) +
             "], value [" + str_vec + "], type [" +
             std::to_string(field_value->data_type) + "], source [" +
             source_str + "]";
    }
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

struct FileHelper {
  std::string file_path;
  FILE *fp;

  FileHelper(std::string path) {
    file_path = path;
  }

  ~FileHelper() {
    if (fp) fclose(fp);
  }

  int Open(const char *mode) {
    fp = fopen(file_path.c_str(), mode);
    if (fp == nullptr) return -1;
    return 0;
  }

  size_t Read(void *data, size_t len) {
    return fread(data, 1, len, fp);
  }
};

string GetCurrentCaseName() {
  const ::testing::TestInfo *const test_info =
      ::testing::UnitTest::GetInstance()->current_test_info();
  return string(test_info->test_case_name()) + "_" + test_info->name();
}

void Sleep(long milliseconds) {
  std::this_thread::sleep_for(std::chrono::milliseconds(milliseconds));
}
