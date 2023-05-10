#ifdef WITH_ROCKSDB

#include "rocksdb_wrapper.h"
#include "search/error_code.h"
#include "util/log.h"

using std::string;
using namespace rocksdb;

namespace tig_gamma {

RocksDBWrapper::RocksDBWrapper() : db_(nullptr) {}

RocksDBWrapper::~RocksDBWrapper() {
  if (db_) {
    delete db_;
    db_ = nullptr;
  }
}

int RocksDBWrapper::Open(string db_path, size_t block_cache_size) {
  Options options;
  if (block_cache_size) {
    std::shared_ptr<Cache> cache = NewLRUCache(block_cache_size);
    table_options_.block_cache = cache;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options_));
  }
  options.IncreaseParallelism();
  // options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;
  // open DB
  Status s = DB::Open(options, db_path, &db_);
  if (!s.ok()) {
    LOG(ERROR) << "open rocks db error: " << s.ToString();
    return IO_ERR;
  }
  return 0;
}

int RocksDBWrapper::Put(int key, const char *v, size_t len) {
  string key_str;
  ToRowKey(key, key_str);
  return Put(key_str, v, len);
}

int RocksDBWrapper::Put(const string &key, const char *v, size_t len) {
  Status s = db_->Put(WriteOptions(), Slice(key), Slice(v, len));
  if (!s.ok()) {
    LOG(ERROR) << "rocksdb put error:" << s.ToString() << ", key=" << key;
    return IO_ERR;
  }
  return 0;
}

int RocksDBWrapper::Put(const string &key, const string &value) {
  return Put(key, value.c_str(), value.size());
}

void RocksDBWrapper::ToRowKey(int key, string &key_str) {
  char data[11];
  snprintf(data, 11, "%010d", key);
  key_str.assign(data, 10);
}

}  // namespace tig_gamma

#endif // WITH_ROCKSDB
