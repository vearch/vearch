/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifndef RAW_VECTOR_H_
#define RAW_VECTOR_H_

#include "gamma_api.h"
#include "utils.h"
#include <string>
#include <vector>

namespace tig_gamma {

const static int MAX_VECTOR_NUM_PER_DOC = 10;

class RawVector {
public:
  RawVector(const std::string name, int dimension, int max_vector_size)
      : vector_name_(name), dimension_(dimension),
        max_vector_size_(max_vector_size), ntotal_(0), total_mem_bytes_(0){};
  virtual ~RawVector(){};

  /** initialize resource
   *
   * @return 0 if successed
   */
  virtual int Init() = 0;
  /** release resource
   *
   * @return
   */
  virtual void Close() = 0;

  /** get vector by id
   *
   * @param id vector id
   * @return vector if successed, null if failed
   */
  virtual const float *GetVector(long vid) const = 0;
  /** get vectors by vecotor id list
   *
   * @param k the length of vector id list
   * @param ids_list vector id list
   * @param resultss(output) vectors
   * @return 0 if successed
   */
  virtual int Gets(int k, long *ids_list,
                   std::vector<const float *> &resultss) const = 0;
  /** get the header of all vectors, so it can access all vecotors through the
   * header if dimension is known
   *
   * @param id vector id
   * @return vector if successed, null if failed
   */
  virtual const float *GetVectorHeader() = 0;

  /** get source of one vector, source is a string, for example the image url of
   * vector
   *
   * @param vid vector id
   * @param str(output) the pointer of source string
   * @param len(output) the len of source string
   * @return 0 if successed
   */
  virtual int GetSource(int vid, char *&str, int &len) = 0;

  /** add one vector field
   *
   * @param docid doc id, one doc may has multiple vectors
   * @param field vector field, it contains vector(float array) and
   * source(string)
   * @return 0 if successed
   */
  virtual int Add(int docid, Field *&field) { return -1; };

  /** dump vectors and sources to disk file
   *
   * @param path the disk directory path
   * @return 0 if successed
   */
  virtual int Dump(const std::string &path, int dump_docid, int max_docid) {
    return -1;
  };
  /** load vectors and sources from disk file
   *
   * @param path the disk directory path
   * @return 0 if successed
   */
  virtual int Load(const std::vector<std::string> &path) { return -1; };

  long GetTotalMemBytes() { return total_mem_bytes_; };

  int GetVectorNum() const { return ntotal_; };
  int GetMaxVectorSize() const { return max_vector_size_; }
  int GetFirstVectorID(int docid) {
    int *vid_list = docid2vid_[docid];
    if (vid_list[0] <= 0)
      return -1;
    return vid_list[1];
  }
  int GetLastVectorID(int docid) {
    int *vid_list = docid2vid_[docid];
    if (vid_list[0] <= 0)
      return -1;
    return vid_list[vid_list[0]];
  }

  std::vector<int> vid2docid_;   // vector id to doc id
  std::vector<int *> docid2vid_; // doc id to vector id list
protected:
  std::string vector_name_; // vector name
  int dimension_;           // vector dimension
  int max_vector_size_;
  int ntotal_;           // vector num
  long total_mem_bytes_; // total used memory bytes
};
} // namespace tig_gamma
#endif /* RAW_VECTOR_H_ */
