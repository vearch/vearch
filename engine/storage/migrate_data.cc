#include "storage/migrate_data.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace tig_gamma {

MigrateData::~MigrateData() {

}

int MigrateData::Init(bitmap::BitmapManager *docids_bitmap,
                      std::string path) {
  std::lock_guard<std::mutex> lock(mtx_);
  docids_bitmap_ = docids_bitmap;
  inc_fd_offset_ = sizeof(next_migrate_docid_) + sizeof(max_full_docid_) +
                   sizeof(inc_migrated_offset_);
  inc_migrated_offset_ = inc_fd_offset_;
  if (inc_docid_fd_ >= 0) { close(inc_docid_fd_); }
  path += "/";
  path += INC_MIGRATE_DOC_QUEUE;
  remove(path.c_str());
  inc_docid_fd_ = open(path.c_str(), O_CREAT | O_RDWR, 0666);
  pwrite(inc_docid_fd_, &inc_migrated_offset_, sizeof(inc_migrated_offset_),
         sizeof(next_migrate_docid_) + sizeof(max_full_docid_));
  if (inc_docid_fd_ < 0) {
    LOG(INFO) << "open migrate_doc_inc_queue failed.";
    return -1;
  }
  return 0;
}

int MigrateData::Load(bitmap::BitmapManager *docids_bitmap,
                      std::string path) {
  return 0;
}

bool MigrateData::HasMigrateFile(std::string path) {
  return false;
}

bool MigrateData::BeginMigrate(int max_docid) {
  if (inc_docid_fd_ < 0) {
    LOG(INFO) << "MigrateData fd < 0";
    return false;
  }
  std::lock_guard<std::mutex> lock(mtx_);
  max_full_docid_ = max_docid;
  next_migrate_docid_ = 0;
  pwrite(inc_docid_fd_, &next_migrate_docid_,
         sizeof(next_migrate_docid_), 0);
  pwrite(inc_docid_fd_, &max_full_docid_,
         sizeof(max_full_docid_), sizeof(next_migrate_docid_));
  return true;
}

void MigrateData::TerminateMigrate(std::string path) {
  std::lock_guard<std::mutex> lock(mtx_);
  if (inc_docid_fd_ >= 0) {
    close(inc_docid_fd_);
    path += "/";
    path += INC_MIGRATE_DOC_QUEUE;
    remove(path.c_str());
  }
  next_migrate_docid_ = -1;
  max_full_docid_ = 0;
  inc_docid_fd_ = -1;
  inc_fd_offset_ = -1;
  inc_migrated_offset_ = -1;
  docids_bitmap_ = nullptr;
}

bool MigrateData::AddDocid(int docid) {
  std::lock_guard<std::mutex> lock(mtx_);
  if (inc_docid_fd_ < 0) return false;
  pwrite(inc_docid_fd_, &docid, sizeof(docid), inc_fd_offset_);
  inc_fd_offset_ += sizeof(docid);
  return true;
}

bool MigrateData::DeleteDocid(int docid) {
  std::lock_guard<std::mutex> lock(mtx_);
  if (inc_docid_fd_ < 0) return false;
  int docid_in_qu = (0X80000000 | docid);
  pwrite(inc_docid_fd_, &docid_in_qu, sizeof(docid_in_qu), inc_fd_offset_);
  inc_fd_offset_ += sizeof(docid_in_qu);
  return true;
}

bool MigrateData::GetMigrateDocid(int &docid, bool &is_del) {
  bool ret = false;
  is_del = false;
  int flag_next_migrate_docid = -1, flag_inc_migrated_offset = -1;
  {
    std::lock_guard<std::mutex> lock(mtx_);
    if (inc_docid_fd_ < 0 || next_migrate_docid_ < 0) return false;
    while (next_migrate_docid_ < max_full_docid_) {
      docid = next_migrate_docid_;
      if (docids_bitmap_->Test((uint32_t)(next_migrate_docid_++)) == false) {
        ret = true;
        flag_next_migrate_docid = next_migrate_docid_;
        break;
      }
    }
    if (ret == false && inc_migrated_offset_ < inc_fd_offset_) {
      flag_inc_migrated_offset = inc_migrated_offset_;
      inc_migrated_offset_ += sizeof(docid);
    }
  }

  if (flag_next_migrate_docid > 0) {
    pwrite(inc_docid_fd_, &flag_next_migrate_docid,
           sizeof(flag_next_migrate_docid), 0);
  } else if (flag_inc_migrated_offset > 0) {
    int docid_in_qu;
    pread(inc_docid_fd_, &docid_in_qu, sizeof(docid_in_qu), flag_inc_migrated_offset);
    flag_inc_migrated_offset += sizeof(docid_in_qu);
    pwrite(inc_docid_fd_, &flag_inc_migrated_offset, sizeof(flag_inc_migrated_offset),
           sizeof(next_migrate_docid_) + sizeof(max_full_docid_));
    if (docid_in_qu & 0X80000000) {
      docid = (docid_in_qu & 0X7fffffff);
      ret = true;
      is_del = true;
    } else if (docids_bitmap_->Test((uint32_t)docid_in_qu) == false) {
      docid = docid_in_qu;
      ret = true;
    }
  }
  return ret;
}

} // namespace tig_gamma
