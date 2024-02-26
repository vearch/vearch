/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifndef REALTIME_INVERT_INDEX_H_
#define REALTIME_INVERT_INDEX_H_

#include <stdlib.h>

#include <map>
#include <vector>

#include "faiss/Index.h"
#include "faiss/IndexIVF.h"
#include "realtime_mem_data.h"
#include "util/bitmap.h"
#include "util/bitmap_manager.h"

namespace tig_gamma {
namespace realtime {

struct RTInvertIndex {
 public:
  // bucket_keys should not be larger than bucket_keys_limit
  RTInvertIndex(size_t nlist, size_t code_size, VIDMgr *vid_mgr,
                bitmap::BitmapManager *docids_bitmap,
                size_t bucket_keys = 10000, size_t bucket_keys_limit = 1000000);

  ~RTInvertIndex();

  bool Init();

  /*  @param n : count of added keys
   *  @param keys : added key arrays
   *  @param keys_codes : added key code arrays*/
  bool AddKeys(std::map<int, std::vector<long>> &new_keys,
               std::map<int, std::vector<uint8_t>> &new_codes);

  int Update(int bucket_no, int vid, std::vector<uint8_t> &codes);

  bool GetIvtList(const size_t &bucket_no, long *&ivt_list, size_t &ivt_size,
                  uint8_t *&ivt_codes_list);

  long GetTotalMemBytes() {
    return cur_ptr_ ? cur_ptr_->GetTotalMemBytes() : 0;
  }

  void PrintBucketSize();
  int CompactIfNeed();
  int Delete(int *vids, int n);

  size_t nlist_;
  size_t code_size_;
  size_t bucket_keys_;
  size_t bucket_keys_limit_;
  VIDMgr *vid_mgr_;
  bitmap::BitmapManager *docids_bitmap_;

  RealTimeMemData *cur_ptr_;
};

using idx_t = faiss::Index::idx_t;
struct RTInvertedLists : faiss::InvertedLists {
  RTInvertedLists(realtime::RTInvertIndex *rt_invert_index_ptr, size_t nlist,
                  size_t code_size);

  /*************************
   *  Read only functions */

  // get the size of a list
  size_t list_size(size_t list_no) const override;

  /** get the codes for an inverted list
   * must be released by release_codes
   *
   * @return codes    size list_size * code_size
   */
  const uint8_t *get_codes(size_t list_no) const override;

  /** get the ids for an inverted list
   * must be released by release_ids
   *
   * @return ids      size list_size
   */
  const idx_t *get_ids(size_t list_no) const override;

  /*************************
   * writing functions     */

  size_t add_entries(size_t list_no, size_t n_entry, const idx_t *ids,
                     const uint8_t *code) override;

  void resize(size_t list_no, size_t new_size) override;

  void update_entries(size_t list_no, size_t offset, size_t n_entry,
                      const idx_t *ids_in, const uint8_t *codes_in) override;

  realtime::RTInvertIndex *rt_invert_index_ptr_;
};

}  // namespace realtime

}  // namespace tig_gamma

#endif
