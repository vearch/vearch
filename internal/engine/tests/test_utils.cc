/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "storage/storage_manager.h"
#include "util/utils.h"

namespace test {

// ---- utils::ToStringFieldKey -------------------------------------------------
// Pure function: "<field_name>:<ToRowKey(id)>". Read and write paths must
// produce byte-identical keys, so pin the exact format here.

TEST(ToStringFieldKey, Format) {
  EXPECT_EQ(utils::ToStringFieldKey("name", 42),
            "name:" + utils::ToRowKey(42));
  // ToRowKey zero-pads to 10 digits; keep this contract explicit.
  EXPECT_EQ(utils::ToStringFieldKey("name", 42), "name:0000000042");
  EXPECT_EQ(utils::ToStringFieldKey("name", 0), "name:0000000000");
}

TEST(ToStringFieldKey, EmptyFieldName) {
  EXPECT_EQ(utils::ToStringFieldKey("", 7), ":0000000007");
}

// ---- StorageManager::MultiGetByKeys -----------------------------------------
// The helper sorts keys internally, passes sorted_input=true to rocksdb, then
// de-permutes values/statuses back to the caller's original key order. These
// tests exercise that reorder-restore logic without any vector dataset.

class MultiGetByKeysTest : public ::testing::Test {
 protected:
  void SetUp() override {
    root_ = std::string("test_multiget_") +
            ::testing::UnitTest::GetInstance()->current_test_info()->name();
    utils::remove_dir(root_.c_str());
    mgr_ = new vearch::StorageManager(root_);
    cf_id_ = mgr_->CreateColumnFamily("scalar");
    ASSERT_TRUE(mgr_->Init(0).ok());
  }

  void TearDown() override {
    mgr_->Close();
    delete mgr_;
    utils::remove_dir(root_.c_str());
  }

  void Put(const std::string &key, const std::string &value) {
    std::string v = value;
    ASSERT_TRUE(mgr_->Put(cf_id_, key, v).ok());
  }

  std::string root_;
  int cf_id_ = 0;
  vearch::StorageManager *mgr_ = nullptr;
};

TEST_F(MultiGetByKeysTest, RestoresInputOrderForUnsortedKeys) {
  Put("b", "vb");
  Put("a", "va");
  Put("c", "vc");

  // Deliberately unsorted request order.
  std::vector<std::string> keys = {"c", "a", "b"};
  std::vector<std::string> values;
  auto statuses = mgr_->MultiGetByKeys(cf_id_, keys, values);

  ASSERT_EQ(values.size(), keys.size());
  ASSERT_EQ(statuses.size(), keys.size());
  EXPECT_TRUE(statuses[0].ok());
  EXPECT_TRUE(statuses[1].ok());
  EXPECT_TRUE(statuses[2].ok());
  // Values must line up with the original (unsorted) key positions.
  EXPECT_EQ(values[0], "vc");
  EXPECT_EQ(values[1], "va");
  EXPECT_EQ(values[2], "vb");
}

TEST_F(MultiGetByKeysTest, DuplicateKeysGetIndependentSlots) {
  Put("k", "vk");

  std::vector<std::string> keys = {"k", "k", "k"};
  std::vector<std::string> values;
  auto statuses = mgr_->MultiGetByKeys(cf_id_, keys, values);

  ASSERT_EQ(values.size(), 3u);
  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(statuses[i].ok());
    EXPECT_EQ(values[i], "vk");
  }
}

TEST_F(MultiGetByKeysTest, MissingKeysMapToNotFoundStatus) {
  Put("present1", "v1");
  Put("present2", "v2");

  std::vector<std::string> keys = {"missing_a", "present2", "missing_b",
                                    "present1"};
  std::vector<std::string> values;
  auto statuses = mgr_->MultiGetByKeys(cf_id_, keys, values);

  ASSERT_EQ(values.size(), keys.size());
  ASSERT_EQ(statuses.size(), keys.size());
  // Status/value must map back to each key's original slot, not the sorted one.
  EXPECT_FALSE(statuses[0].ok());
  EXPECT_TRUE(statuses[1].ok());
  EXPECT_EQ(values[1], "v2");
  EXPECT_FALSE(statuses[2].ok());
  EXPECT_TRUE(statuses[3].ok());
  EXPECT_EQ(values[3], "v1");
}

TEST_F(MultiGetByKeysTest, EmptyInput) {
  std::vector<std::string> keys;
  std::vector<std::string> values;
  auto statuses = mgr_->MultiGetByKeys(cf_id_, keys, values);
  EXPECT_TRUE(values.empty());
  EXPECT_TRUE(statuses.empty());
}

}  // namespace test
