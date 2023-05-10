/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>
#include <mutex>
#include "util/log.h"
#include "util/bitmap_manager.h"

#define INC_MIGRATE_DOC_QUEUE  "inc_migrate_doc_queue.txt"

namespace tig_gamma {

class MigrateData {
 public:
  MigrateData() {}

  virtual ~MigrateData();

  int Init(bitmap::BitmapManager *docids_bitmap,
           std::string path);

  int Load(bitmap::BitmapManager *docids_bitmap,
           std::string path);

  static bool HasMigrateFile(std::string path);

  bool BeginMigrate(int max_docid);

  void TerminateMigrate(std::string path);

  bool AddDocid(int docid);

  bool DeleteDocid(int docid);

  bool GetMigrateDocid(int &docid, bool &is_del);

 private:
  int next_migrate_docid_ = -1;
  int max_full_docid_ = 0;
  int inc_docid_fd_ = -1;                  // file queue
  int inc_fd_offset_ = -1;
  int inc_migrated_offset_ = -1;
  bitmap::BitmapManager *docids_bitmap_ = nullptr;
  std::mutex mtx_;
};

} // namespace tig_gamma
