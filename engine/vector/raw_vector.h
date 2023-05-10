/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <sstream>
#include <string>
#include <vector>

#include "c_api/api_data/gamma_doc.h"
#include "index/retrieval_model.h"
#include "io/io_common.h"
#include "storage/storage_manager.h"
#include "util/bitmap_manager.h"
#include "util/log.h"
#include "util/utils.h"
#include "vector/raw_vector_common.h"

namespace tig_gamma {

class RawVectorIO;
struct StoreParams;

static const int kInitSize = 1000 * 1000;

class RawVectorIO;
struct StoreParams : DumpConfig {
  long cache_size;  // bytes
  int segment_size;
  utils::JsonParser compress;

  StoreParams(std::string name_ = "") : DumpConfig(name_) {
    cache_size = 1024;  // 1024M
    segment_size = 500000;
  }

  StoreParams(const StoreParams &other) {
    name = other.name;
    cache_size = other.cache_size;
    segment_size = other.segment_size;
    compress = other.compress;
  }

  int Parse(const char *str);
  int Parse(utils::JsonParser &jp);
  int MergeRight(StoreParams &other);

  std::string ToJsonStr() {
    std::stringstream ss;
    ss << "{";
    ss << "\"cache_size\":" << cache_size << ",";
    ss << "\"segment_size\":" << segment_size << ",";
    ss << "\"compress\":" << compress.ToStr();
    ss << "}";
    return ss.str();
  }

  int ToJson(utils::JsonParser &jp) {
    jp.PutDouble("cache_size", cache_size);
    jp.PutInt("segment_size", segment_size);
    jp.PutObject("compress", compress);
    return 0;
  }
};

class RawVector : public VectorReader {
 public:
  RawVector(VectorMetaInfo *meta_info, const std::string &root_path,
            bitmap::BitmapManager *docids_bitmap,
            const StoreParams &store_params);

  virtual ~RawVector();

  /** initialize resource
   *
   * @return 0 if successed
   */
  int Init(std::string vec_name, bool has_source, bool multi_vids);

  /** get the header of vectors, so it can access vecotors through the
   * header if dimension is known
   *
   * @param start start vector id(include)
   * @param n number of vectors
   * @param vec[out] vector header address
   * @param m[out] the real number of vectors(0 < m <= n)
   * @return success: 0
   */
  virtual int GetVectorHeader(int start, int n, ScopeVectors &vec,
                              std::vector<int> &lens) = 0;

  /** dump vectors and sources to disk file
   *
   * @param path  the disk directory path
   * @return 0 if successed
   */
  // int Dump(const std::string &path, int dump_docid, int max_docid);

  /** load vectors and sources from disk file
   *
   * @param path  the disk directory path
   * @return 0 if successed
   */
  // int Load(const std::vector<std::string> &path, int doc_num);

  /** get vector by id
   *
   * @param id  vector id
   * @return    vector if successed, null if failed
   */
  int GetVector(long vid, ScopeVector &vec) const;

  /** get vectors by vecotor id list
   *
   * @param ids_list  vector id list
   * @param resultss  (output) vectors, Warning: the vectors must be destroyed
   * by Destroy()
   * @return 0 if successed
   */
  virtual int Gets(const std::vector<int64_t> &vids, ScopeVectors &vecs) const;

  /** get source of one vector, source is a string, for example the image url of
   *  vector
   *
   * @param vid   vector id
   * @param str   (output) the pointer of source string
   * @param len   (output) the len of source string
   * @return 0    if successed
   */
  int GetSource(int vid, char *&str, int &len);

  /** add one vector field
   *
   * @param docid doc id, one doc may has multiple vectors
   * @param field vector field, it contains vector(uint8_t array) and
   * source(string)
   * @return 0 if successed
   */
  int Add(int docid, struct Field &field);
  
  int Add(int docid, float *data);

  int Update(int docid, struct Field &field);

  virtual size_t GetStoreMemUsage() { return 0; }

  long GetTotalMemBytes() { return total_mem_bytes_ + GetStoreMemUsage(); };

  int GetVectorNum() const { return meta_info_->Size(); };

  /** add vector to the specific implementation of RawVector(memory or disk)
   *it is called by next common function Add()
   */
  virtual int AddToStore(uint8_t *v, int len) = 0;

  virtual int UpdateToStore(int vid, uint8_t *v, int len) = 0;

  virtual int GetCacheSize(int &cache_size) { return -1; };

  virtual int AlterCacheSize(int cache_size) { return -1; }

  RawVectorIO *GetIO() { return vio_; }

  void SetIO(RawVectorIO *vio) { vio_ = vio; }

  VIDMgr *VidMgr() const { return vid_mgr_; }
  bitmap::BitmapManager *Bitmap() { return docids_bitmap_; }
  int VectorByteSize() { return vector_byte_size_; }

  std::string RootPath() { return root_path_; }
  DumpConfig *GetDumpConfig();

  StorageManager *storage_mgr_;
  
  int HaveZFPCompressor() {
#ifdef WITH_ZFP
    if(zfp_compressor_) return 1;
#endif
    return 0;
  }

 protected:
  /** get vector by id
   *
   * @param id vector id
   * @return vector if successed, null if failed
   */
  virtual int GetVector(long vid, const uint8_t *&vec,
                        bool &deletable) const = 0;

  virtual int InitStore(std::string &vec_name) = 0;

  int Compress(uint8_t *v, ScopeVector &svec);
  int Decompress(uint8_t *cmpr_v, int n, uint8_t *&vec, bool &deletable) const;

 protected:
  std::string root_path_;
  int vector_byte_size_;
  int data_size_;

  long total_mem_bytes_;              // total used memory bytes
  char *str_mem_ptr_;                 // source memory
  std::vector<long> source_mem_pos_;  // position of each source
  bool has_source_;
  std::string desc_;  // description of this raw vector
  StoreParams store_params_;
  bool allow_use_zfp;
#ifdef WITH_ZFP
  ZFPCompressor *zfp_compressor_;
#endif
  bitmap::BitmapManager *docids_bitmap_;
  VIDMgr *vid_mgr_;
  RawVectorIO *vio_;
};

}  // namespace tig_gamma
