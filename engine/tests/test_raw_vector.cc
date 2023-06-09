/**
 * Copyright (c) The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "vector/memory_raw_vector.h"
#include "vector/mmap_raw_vector.h"

#ifdef WITH_ROCKSDB
#include "vector/rocksdb_raw_vector.h"
#endif  // WITH_ROCKSDB

#include <fcntl.h>
#include <gtest/gtest.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cassert>
#include <cmath>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <string>
#include <thread>

#include "storage/segment.h"
#include "test.h"
#include "util/utils.h"
#include "vector/raw_vector_factory.h"

using namespace tig_gamma;
using namespace std;

float *BuildVector(int dim, float offset) {
  float *v = new float[dim];
  for (int i = 0; i < dim; i++) {
    v[i] = i + offset;
  }
  return v;
}

float *BuildVectors(int n, int dim, float offset) {
  float *vecs = new float[dim * n];
  for (int i = 0; i < n; i++) {
    float *v = vecs + dim * i;
    for (int j = 0; j < dim; j++) {
      v[j] = j + i + offset;
    }
  }
  return vecs;
}

Field *BuildVectorField(int dim, float offset) {
  float *data = BuildVector(dim, offset);
  Field *field = new Field();
  field->value = string((char *)data, sizeof(float) * dim);
  field->datatype = tig_gamma::DataType::VECTOR;
  // Field *field =
  //     MakeField(nullptr, MakeByteArray((char *)data, sizeof(float) * dim),
  //               nullptr, VECTOR);
  delete[] data;
  return field;
}

bool floatArrayEquals(const float *a, int m, const float *b, int n) {
  if (m != n) return false;
  for (int i = 0; i < m; i++) {
    if (std::fabs(a[i] - b[i]) > 0.0001f) {
      return false;
    }
  }
  return true;
}

void AddToRawVector(RawVector *raw_vector, int start_id, int num,
                    int dimension) {
  int end = start_id + num;
  for (int i = start_id; i < end; i++) {
    Field *field = BuildVectorField(dimension, i);
    int ret = raw_vector->Add(i, *field);
    assert(0 == ret);
    delete field;
  }
}

void UpdateToRawVector(RawVector *raw_vector, int start_id, int num,
                       int dimension, float addition = 0.0f) {
  int end = start_id + num;
  for (int i = start_id; i < end; i++) {
    Field *field = BuildVectorField(dimension, i + addition);
    int ret = raw_vector->Update(i, *field);
    assert(0 == ret);
    if (raw_vector->GetIO()) {
      ret = raw_vector->GetIO()->Update(i);
      assert(0 == ret);
    }
    delete field;
  }
}

void ValidateRandVectors(RawVector *raw_vector, vector<int64_t> ids,
                         int dimension) {
  ScopeVectors svecs;
  ASSERT_EQ(0, raw_vector->Gets(ids, svecs));
  for (size_t i = 0; i < ids.size(); i++) {
    int vid = (int)ids[i];
    const float *expect = BuildVector(dimension, vid);
    const float *peek_vector = (const float *)svecs.Get(i);
    ASSERT_TRUE(floatArrayEquals(expect, dimension, peek_vector, dimension))
        << "******Gets float array equal error, vid=" << vid << ", peek=["
        << peek_vector[0] << ", " << peek_vector[1] << "]"
        << ", expect=[" << expect[0] << ", " << expect[1] << "]";
    delete[] expect;
  }
}

void ValidateVector(RawVector *raw_vector, int start_id, int num, int dimension,
                    float addition = 0.0f) {
  for (int i = start_id; i < num; i++) {
    const float *expect = BuildVector(dimension, i + addition);
    ScopeVector scope_vec;
    ASSERT_EQ(0, raw_vector->GetVector(i, scope_vec));
    const float *peek_vector = (const float *)scope_vec.Get();
    ASSERT_TRUE(floatArrayEquals(expect, dimension, peek_vector, dimension))
        << "******GetVector float array equal error, vid=" << i << ", peek=["
        << peek_vector[0] << ", " << peek_vector[1] << "]"
        << ", expect=[" << expect[0] << ", " << expect[1] << "]";
    delete[] expect;
  }
}

void ValidateVectorHeader(RawVector *raw_vector, int start_id, int num,
                          int dimension, float addition = 0.0f) {
  ScopeVectors scope_vecs;
  std::vector<int> lens;
  raw_vector->GetVectorHeader(start_id, num, scope_vecs, lens);
  int vid = start_id;
  for (size_t i = 0; i < scope_vecs.Size(); ++i) {
    const float *vecs = (const float *)scope_vecs.Get(i);
    for (int j = 0; j < lens[i]; ++j) {
      const float *expect = BuildVector(dimension, vid + addition);
      const float *peek_vector = vecs + j * dimension;
      ASSERT_TRUE(floatArrayEquals(expect, dimension, peek_vector, dimension))
          << "******GetVectorHeader float array equal error, vid=" << vid
          << ", peek=[" << peek_vector[0] << ", " << peek_vector[1] << "]"
          << ", expect=[" << expect[0] << ", " << expect[1] << "]";
      delete[] expect;
      ++vid;
    }
  }
}

static int Dump(RawVector *raw_vector, int start, int end) {
  if (raw_vector->GetIO()) {
    return raw_vector->GetIO()->Dump(start, end);
  }
  return 0;
}

static int Load(RawVector *raw_vector, int num) {
  if (raw_vector->GetIO()) {
    return raw_vector->GetIO()->Load(num);
  }
  return 0;
}

static void Delete(RawVector *raw_vector) {
  if (raw_vector->GetIO()) {
    delete raw_vector->GetIO();
  }
  delete raw_vector;
}

void TestRawVectorNormal(VectorStorageType store_type, bool compress = false) {
  string root_path = "./" + GetCurrentCaseName();
  string name = "abc";
  int dimension = 512;
  utils::remove_dir(root_path.c_str());
  utils::make_dir(root_path.c_str());
  StoreParams store_params;
  store_params.cache_size = 1;
  store_params.segment_size = 100;
  if (compress) {
    ASSERT_EQ(0,
              store_params.compress.Parse("{\"type\":\"zfp\", \"rate\": 16}"));
  }
  int nadd = 350;
  cerr << "store params=" << store_params.ToJsonStr() << endl;

  VectorMetaInfo *meta_info =
      new VectorMetaInfo(name, dimension, VectorValueType::FLOAT);
  bitmap::BitmapManager *doc_bitmap = nullptr;

  RawVector *raw_vector = tig_gamma::RawVectorFactory::Create(
      meta_info, store_type, root_path, store_params, doc_bitmap);
  assert(0 == raw_vector->Init(name, false, false));
  int doc_num = nadd;
  for (int i = 0; i < doc_num; i++) {
    if (i % 100 == 0) cerr << "add i=" << i << endl;
    Field *field = BuildVectorField(dimension, i);
    ASSERT_EQ(0, raw_vector->Add(i, *field));
    delete field;
  }

  ASSERT_EQ(doc_num, raw_vector->GetVectorNum());
  vector<int64_t> ids = {1, 314, 78, 173, 256};
  ValidateRandVectors(raw_vector, ids, dimension);
  ValidateVector(raw_vector, 0, doc_num, dimension);
  ValidateVectorHeader(raw_vector, 0, doc_num, dimension);
  std::srand(std::time(nullptr));
  int start_vid = std::rand() % doc_num;
  cerr << "validate vector header, start vid=" << start_vid << endl;
  ValidateVectorHeader(raw_vector, start_vid, doc_num - start_vid, dimension);

  int update_num = 100;
  UpdateToRawVector(raw_vector, 50, update_num, dimension, 0.5f);
  ValidateVector(raw_vector, 50, update_num, dimension, 0.5f);
  ValidateVectorHeader(raw_vector, 50, update_num, dimension, 0.5f);

  delete raw_vector;
}

void TestRawVectorDumpLoad(VectorStorageType store_type,
                           bool compress = false) {
  string root_path = GetCurrentCaseName() + "/vectors";
  string name = "abc";
  int dimension = 512;

  utils::remove_dir(GetCurrentCaseName().c_str());
  utils::make_dir(GetCurrentCaseName().c_str());
  utils::make_dir(root_path.c_str());

  StoreParams store_params;
  store_params.cache_size = 1;
  store_params.segment_size = 100;
  if (compress) {
    ASSERT_EQ(0,
              store_params.compress.Parse("{\"type\":\"zfp\", \"rate\": 16}"));
  }
  cerr << "store params=" << store_params.ToJsonStr() << endl;

  VectorMetaInfo *meta_info =
      new VectorMetaInfo(name, dimension, VectorValueType::FLOAT);
  bitmap::BitmapManager *doc_bitmap = nullptr;

  RawVector *raw_vector = RawVectorFactory::Create(
      meta_info, store_type, root_path, store_params, doc_bitmap);
  ASSERT_EQ(0, raw_vector->Init(name, false, false));

  int doc_num = 500;
  AddToRawVector(raw_vector, 0, doc_num, dimension);

  ASSERT_EQ(doc_num, raw_vector->GetVectorNum());
  ValidateVectorHeader(raw_vector, 0, doc_num, dimension);

  int update_num = 100;
  UpdateToRawVector(raw_vector, 0, update_num, dimension, 0.5f);
  UpdateToRawVector(raw_vector, 400, update_num, dimension, 0.5f);

  Dump(raw_vector, 0, doc_num);
  Delete(raw_vector);

  cout << "---------------load all----------------" << endl;
  int load_num = doc_num;
  meta_info = new VectorMetaInfo(name, dimension, VectorValueType::FLOAT);
  raw_vector = RawVectorFactory::Create(meta_info, store_type, root_path,
                                        store_params, doc_bitmap);
  ASSERT_NE(nullptr, raw_vector);
  ASSERT_EQ(0, raw_vector->Init(name, false, false));
  Load(raw_vector, load_num);
  ValidateVector(raw_vector, update_num, load_num - update_num * 2, dimension);
  ValidateVector(raw_vector, 0, update_num, dimension, 0.5f);
  ValidateVector(raw_vector, 400, update_num, dimension, 0.5f);
  ASSERT_EQ(load_num, raw_vector->GetVectorNum());
  Delete(raw_vector);

  cout << "---------------load some----------------" << endl;
  // load: load_num < disk_doc_num;
  load_num = doc_num - 100;
  meta_info = new VectorMetaInfo(name, dimension, VectorValueType::FLOAT);
  raw_vector = RawVectorFactory::Create(meta_info, store_type, root_path,
                                        store_params, doc_bitmap);
  ASSERT_NE(nullptr, raw_vector);
  ASSERT_EQ(0, raw_vector->Init(name, false, false));
  ASSERT_EQ(0, Load(raw_vector, load_num));
  ValidateVector(raw_vector, 0, update_num, dimension, 0.5f);
  ValidateVector(raw_vector, 100, load_num - update_num, dimension);
  ASSERT_EQ(load_num, raw_vector->GetVectorNum());
  char *source1 = nullptr, *source2 = nullptr;
  int len1 = 0, len2 = 0;
  raw_vector->GetSource(0, source1, len1);
  raw_vector->GetSource(load_num - 1, source2, len2);

  cout << "---------------dump after load and add----------------" << endl;
  int add_num = 200;
  AddToRawVector(raw_vector, load_num, add_num, dimension);
  UpdateToRawVector(raw_vector, load_num, update_num, dimension, 0.8f);
  ASSERT_EQ(0, Dump(raw_vector, 0, load_num + add_num));
  Delete(raw_vector);

  cout << "---------------reload after dump----------------" << endl;
  load_num = load_num + add_num;
  meta_info = new VectorMetaInfo(name, dimension, VectorValueType::FLOAT);
  raw_vector = RawVectorFactory::Create(meta_info, store_type, root_path,
                                        store_params, doc_bitmap);

  ASSERT_NE(nullptr, raw_vector);
  ASSERT_EQ(0, raw_vector->Init(name, false, false));
  ASSERT_EQ(0, Load(raw_vector, load_num));
  ValidateVector(raw_vector, 0, update_num, dimension, 0.5f);
  ValidateVector(raw_vector, 400, update_num, dimension, 0.8f);
  ValidateVector(raw_vector, 100, 300, dimension);
  ValidateVector(raw_vector, 500, 100, dimension);
  ASSERT_EQ(load_num, raw_vector->GetVectorNum());
  Delete(raw_vector);
}

TEST(MmapRawVector, Normal) { TestRawVectorNormal(VectorStorageType::Mmap); }

TEST(MemoryRawVector, Normal) {
  TestRawVectorNormal(VectorStorageType::MemoryOnly);
}

TEST(MemoryRawVector, DumpLoad) {
  TestRawVectorDumpLoad(VectorStorageType::MemoryOnly);
}

TEST(MmapRawVector, DumpLoad) {
  TestRawVectorDumpLoad(VectorStorageType::Mmap);
}

#ifdef WITH_ROCKSDB
TEST(RocksDBRawVector, Normal) {
  TestRawVectorNormal(VectorStorageType::RocksDB);
}

TEST(RocksDBRawVector, DumpLoad) {
  TestRawVectorDumpLoad(VectorStorageType::RocksDB);
}

#ifdef WITH_ZFP
TEST(RocksDBRawVector, Compress_Normal) {
  TestRawVectorNormal(VectorStorageType::RocksDB, true);
}

TEST(RocksDBRawVector, Compress_DumpLoad) {
  TestRawVectorDumpLoad(VectorStorageType::RocksDB, true);
}
#endif  // WITH_ZFP

#endif

#ifdef WITH_ZFP
TEST(MemoryRawVector, Compress_Normal) {
  TestRawVectorNormal(VectorStorageType::MemoryOnly, true);
}

TEST(MemoryRawVector, Compress_DumpLoad) {
  TestRawVectorDumpLoad(VectorStorageType::MemoryOnly, true);
}

#endif

int main(int argc, char *argv[]) {
  string log_dir = "./test_raw_vector_log";
  utils::remove_dir(log_dir.c_str());
  // SetLogDictionary(tig_gamma::StringToByteArray(log_dir));
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
