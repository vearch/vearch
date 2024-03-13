#include "index/index_io.h"
#include "index/realtime/realtime_mem_data.h"
#include "test.h"
#include "util/bitmap_manager.h"
#include "util/utils.h"

using namespace std;
using namespace vearch;

double ExtendCoefficient(uint8_t extend_time) {
  double result = 1.1 + PI / 2 - atan(extend_time);
  return result;
}

namespace Test {

class RealTimeMemDataTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { random_generator = new RandomGenerator(); }

  static void TearDownTestSuite() {
    if (random_generator) delete random_generator;
  }

  // You can define per-test set-up logic as usual.
  virtual void SetUp() {
    buckets_num = 3;
    max_vec_size = 10000;
    docids_bitmap = new bitmap::BitmapManager();
    docids_bitmap->SetDumpFilePath("./bitmap");
    int init_bitmap_size = 5000 * 10000;
    int file_bytes_size = docids_bitmap->FileBytesSize();
    if (file_bytes_size != 0) {
      init_bitmap_size = file_bytes_size * 8;
    }

    if (docids_bitmap->Init(init_bitmap_size) != 0) {
      LOG(ERROR) << "Cannot create bitmap!";
      return;
    }
    vid_mgr = new VIDMgr(false);
    bucket_keys = 100;
    bucket_keys_limit = bucket_keys * 10;
    code_byte_size = 64;
    M_ = code_byte_size;
    group_size_ = 4;
    realtime_data = new realtime::RealTimeMemData(
        buckets_num, vid_mgr, docids_bitmap, bucket_keys, bucket_keys_limit,
        code_byte_size);
    ASSERT_EQ(true, realtime_data->Init());
  }

  // You can define per-test tear-down logic as usual.
  virtual void TearDown() {
    CHECK_DELETE(vid_mgr);
    CHECK_DELETE(realtime_data);
    // CHECK_DELETE_ARRAY(vid2docid);
    CHECK_DELETE(docids_bitmap);
  }

  const uint8_t *GetCode(int bucket_no, int pos) {
    return realtime_data->cur_invert_ptr_->codes_array_[bucket_no] +
           pos * code_byte_size;
  }

  void CreateData(int num, std::vector<long> &keys, std::vector<uint8_t> &codes,
                  int code_byte_size, int offset = 0) {
    keys.resize(num, -1);
    codes.resize(num * code_byte_size, 0);
    for (int i = 0; i < num; i++) {
      int id = i + offset;
      keys[i] = id;
      codes[i * code_byte_size] = (uint8_t)id;
    }
  }

  long GetVid(int bucket_no, int pos) {
    return realtime_data->cur_invert_ptr_->idx_array_[bucket_no][pos];
  }

  int GetRetrievePos(int bucket_no) {
    return realtime_data->cur_invert_ptr_->retrieve_idx_pos_[bucket_no];
  }

  int GetBucketKeys(int bucket_no) {
    return realtime_data->cur_invert_ptr_->cur_bucket_keys_[bucket_no];
  }

  int GetDeletedNum(int bucket_no) {
    return realtime_data->cur_invert_ptr_->deleted_nums_[bucket_no];
  }

  long GetTotalCompactedNum() {
    return realtime_data->cur_invert_ptr_->compacted_num_;
  }

  // member
  int bucket_keys;
  int bucket_keys_limit;
  int code_byte_size;
  int M_;
  int group_size_;
  // int *vid2docid;
  VIDMgr *vid_mgr;
  bitmap::BitmapManager *docids_bitmap;
  int max_vec_size;
  int buckets_num;
  realtime::RealTimeMemData *realtime_data;

  // Some expensive resource shared by all tests.
  // static T* shared_resource_;
  static RandomGenerator *random_generator;
};

RandomGenerator *RealTimeMemDataTest::random_generator = nullptr;

TEST_F(RealTimeMemDataTest, AddUpdate) {
  int num = 7, bucket_no = 0;
  std::vector<long> keys;
  std::vector<uint8_t> codes;
  CreateData(num, keys, codes, code_byte_size);

  // add
  ASSERT_TRUE(realtime_data->AddKeys(bucket_no, num, keys, codes));

  // validate
  std::vector<long> retrieve_keys;
  std::vector<uint8_t> retrieve_codes;
  retrieve_keys.resize(num);
  retrieve_codes.resize(num * code_byte_size);
  realtime_data->RetrieveCodes(bucket_no, 0, num, retrieve_codes.data(),
                               retrieve_keys.data());
  for (int i = 0; i < num; i++) {
    ASSERT_EQ(keys[i], retrieve_keys[i]);
    ASSERT_EQ(codes[i * code_byte_size], retrieve_codes[i * code_byte_size]);
  }

  // update vid 1 and 6
  codes[1 * code_byte_size] = 251;
  codes[6 * code_byte_size] = 252;
  std::vector<uint8_t> code1(codes.begin() + 1 * code_byte_size,
                             codes.begin() + 2 * code_byte_size);
  std::vector<uint8_t> code6(codes.begin() + 6 * code_byte_size,
                             codes.begin() + 7 * code_byte_size);
  realtime_data->Update(bucket_no, 1, code1);
  realtime_data->Update(bucket_no, 6, code6);

  // validate
  ASSERT_EQ(num, GetRetrievePos(bucket_no));
  realtime_data->RetrieveCodes(bucket_no, 0, num, retrieve_codes.data(),
                               retrieve_keys.data());
  for (int i = 0; i < num; i++) {
    ASSERT_EQ(keys[i], retrieve_keys[i]);
    ASSERT_EQ(codes[i * code_byte_size], retrieve_codes[i * code_byte_size]);
  }
}

TEST_F(RealTimeMemDataTest, CompactBucket) {
  int num = bucket_keys - 1, bucket_no = 0;
  std::vector<long> keys;
  std::vector<uint8_t> codes;
  CreateData(num, keys, codes, code_byte_size);
  ASSERT_TRUE(realtime_data->AddKeys(bucket_no, num, keys, codes));
  ASSERT_EQ(num, GetRetrievePos(bucket_no));
  vector<int> deleted_vids;
  for (int i = 0; i < 10; i++) {
    int pos = random_generator->Rand(num);
    if (!docids_bitmap->Test(pos)) {
      docids_bitmap->Set(pos);
      deleted_vids.push_back(pos);
    }
  }
  LOG(INFO) << "deleted vids="
            << utils::join(deleted_vids.data(), deleted_vids.size(), ',');
  realtime_data->Delete(deleted_vids.data(), deleted_vids.size() - 1);
  ASSERT_EQ(deleted_vids.size() - 1, GetDeletedNum(bucket_no));
  realtime_data->CompactBucket(bucket_no);
  ASSERT_EQ(num - deleted_vids.size(), GetRetrievePos(bucket_no));
  ASSERT_EQ(bucket_keys, GetBucketKeys(bucket_no));
  ASSERT_EQ(0, GetDeletedNum(bucket_no));
  ASSERT_EQ(deleted_vids.size(), GetTotalCompactedNum());
  long retrieve_idx = -1;
  vector<uint8_t> retrieve_code;
  retrieve_code.resize(code_byte_size);
  int j = 0;
  for (int i = 0; i < num; i++) {
    if (docids_bitmap->Test(i)) continue;
    realtime_data->RetrieveCodes(bucket_no, j++, 1, retrieve_code.data(),
                                 &retrieve_idx);
    ASSERT_EQ(i, retrieve_idx);
    ASSERT_EQ(i, retrieve_code[0]);
  }
}

TEST_F(RealTimeMemDataTest, ExtendBucket) {
  int num = bucket_keys + 1, bucket_no = 0;
  std::vector<long> keys;
  std::vector<uint8_t> codes;
  CreateData(num, keys, codes, code_byte_size);

  // add bucket_keys codes
  realtime::RTInvertBucketData *old_invert_prt = realtime_data->cur_invert_ptr_;
  std::vector<long> keys1(keys.begin(), keys.begin() + bucket_keys);
  std::vector<uint8_t> codes1(codes.begin(),
                              codes.begin() + bucket_keys * code_byte_size);
  ASSERT_TRUE(realtime_data->AddKeys(bucket_no, bucket_keys, keys1, codes1));

  // add one code to trigger extend
  std::vector<long> keys2(keys.begin() + bucket_keys, keys.end());
  std::vector<uint8_t> codes2(codes.begin() + bucket_keys * code_byte_size,
                              codes.end());
  ASSERT_TRUE(realtime_data->AddKeys(bucket_no, 1, keys2, codes2));

  // validate
  ASSERT_NE(old_invert_prt, realtime_data->cur_invert_ptr_);
  ASSERT_EQ(num, GetRetrievePos(bucket_no));
  ASSERT_EQ((int)(bucket_keys * ExtendCoefficient(1)),
            GetBucketKeys(bucket_no));
  std::vector<long> retrieve_keys;
  std::vector<uint8_t> retrieve_codes;
  retrieve_keys.resize(num);
  retrieve_codes.resize(num * code_byte_size);
  realtime_data->RetrieveCodes(bucket_no, 0, num, retrieve_codes.data(),
                               retrieve_keys.data());
  for (int i = 0; i < num; i++) {
    ASSERT_EQ(keys[i], retrieve_keys[i]);
    ASSERT_EQ(codes[i * code_byte_size], retrieve_codes[i * code_byte_size]);
  }
}

TEST_F(RealTimeMemDataTest, NoMaxSize) {
  int num = bucket_keys;
  for (int i = 0; i < buckets_num; ++i) {
    std::vector<long> keys;
    std::vector<uint8_t> codes;
    CreateData(num, keys, codes, code_byte_size, i * num);
    ASSERT_TRUE(realtime_data->AddKeys(i, num, keys, codes));
  }

  std::atomic<long> *old_pos =
      realtime_data->cur_invert_ptr_->vid_bucket_no_pos_;

  std::vector<long> keys1;
  std::vector<uint8_t> codes1;
  int num1 = 1;
  CreateData(num1, keys1, codes1, code_byte_size, buckets_num * num);
  ASSERT_TRUE(realtime_data->AddKeys(0, num1, keys1, codes1));

  ASSERT_NE(old_pos, realtime_data->cur_invert_ptr_->vid_bucket_no_pos_);
  long bucket_no_pos = realtime_data->cur_invert_ptr_
                           ->vid_bucket_no_pos_[buckets_num * bucket_keys];
  int bno = bucket_no_pos >> 32;
  int pos = bucket_no_pos & 0xffffffff;
  ASSERT_EQ(0, bno);
  ASSERT_EQ(bucket_keys, pos);
  cout << "bno=" << bno << ", pos=" << pos << endl;
}

TEST_F(RealTimeMemDataTest, DumpLoad) {
  int nums[2];
  nums[0] = bucket_keys;
  nums[1] = bucket_keys + 10;
  int ntotal = 0;
  for (int i = 0; i < 2; ++i) {
    std::vector<long> keys;
    std::vector<uint8_t> codes;
    CreateData(nums[i], keys, codes, code_byte_size, ntotal);
    ASSERT_TRUE(realtime_data->AddKeys(i, nums[i], keys, codes));
    ntotal += nums[i];
  }

  string dump_dir = GetCurrentCaseName();
  utils::remove_dir(dump_dir.c_str());
  utils::make_dir(dump_dir.c_str());

  // dump
  string index_file = dump_dir + "/index.data";
  realtime::RTInvertIndex *rt_invert_index =
      new realtime::RTInvertIndex(0, 0, nullptr, nullptr);
  rt_invert_index->cur_ptr_ = realtime_data;
  faiss::IOWriter *fw = new FileIOWriter(index_file.c_str());
  ASSERT_EQ(0, WriteInvertedLists(fw, rt_invert_index));
  delete fw;

  // load
  realtime::RealTimeMemData *new_realtime_data = new realtime::RealTimeMemData(
      buckets_num, vid_mgr, docids_bitmap, bucket_keys, bucket_keys_limit,
      code_byte_size);
  ASSERT_EQ(true, new_realtime_data->Init());

  rt_invert_index->cur_ptr_ = new_realtime_data;
  faiss::IOReader *fr = new FileIOReader(index_file.c_str());
  int nload = 0;
  ASSERT_EQ(0, ReadInvertedLists(fr, rt_invert_index, nload));
  delete fr;

  // validate
  int nexpect = ntotal;
  int expect_nums[2] = {nums[0], nums[1]};

  ASSERT_EQ(nexpect, nload);
  ASSERT_EQ(expect_nums[0],
            new_realtime_data->cur_invert_ptr_->retrieve_idx_pos_[0]);
  ASSERT_EQ(100, new_realtime_data->cur_invert_ptr_->cur_bucket_keys_[0]);
  ASSERT_EQ(expect_nums[1],
            new_realtime_data->cur_invert_ptr_->retrieve_idx_pos_[1]);
  ASSERT_EQ((int)(bucket_keys * ExtendCoefficient(1)),
            new_realtime_data->cur_invert_ptr_->cur_bucket_keys_[1]);
  int vid = 0;
  for (int bno = 0; bno < 2; bno++) {
    vector<long> retrieve_keys(expect_nums[bno], 0);
    vector<uint8_t> retrieve_codes(expect_nums[bno] * code_byte_size, 0);
    realtime_data->RetrieveCodes(bno, 0, expect_nums[bno],
                                 retrieve_codes.data(), retrieve_keys.data());
    for (int pos = 0; pos < expect_nums[bno]; pos++) {
      ASSERT_EQ(vid, (int)retrieve_keys[pos]);
      uint8_t *code = retrieve_codes.data() + pos * code_byte_size;
      ASSERT_EQ(vid, code[0]);
      ++vid;
    }
  }
}
}  // namespace Test
