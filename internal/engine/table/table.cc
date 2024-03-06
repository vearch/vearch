/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "table.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <fstream>
#include <string>

#include "field_range_index.h"
#include "util/utils.h"

using std::move;
using std::string;
using std::vector;

static const std::string dirName = std::string("/key_to_id/");

namespace vearch {

ItemToDocID::ItemToDocID(const std::string &root_path)
    : root_path_(root_path + dirName), db_(nullptr) {}

ItemToDocID::~ItemToDocID() { Close(); }

int ItemToDocID::Open() {
  if (!utils::isFolderExist(root_path_.c_str())) {
    mkdir(root_path_.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  }
  rocksdb::Options options;

  options.IncreaseParallelism();
  options.create_if_missing = true;

  rocksdb::Status s = rocksdb::DB::Open(options, root_path_, &db_);
  if (!s.ok()) {
    LOG(ERROR) << "open ItemToDocID error: " << s.ToString();
    return -1;
  }

  return 0;
}

int ItemToDocID::Close() {
  if (db_) {
    db_->Close();
    delete db_;
    db_ = nullptr;
  }
  return 0;
}

int ItemToDocID::Get(const std::string &key, std::string &value) {
  rocksdb::PinnableSlice pinnableVal;
  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(),
                               db_->DefaultColumnFamily(), key, &pinnableVal);

  if (!s.ok()) {
    return -1;
  }

  value = std::string(pinnableVal.data(), pinnableVal.size());
  return 0;
}

int ItemToDocID::Put(const std::string &key, const std::string &value) {
  rocksdb::Status s = db_->Put(rocksdb::WriteOptions(), key, value);
  if (!s.ok()) {
    LOG(ERROR) << "put rocksdb failed: " << key << " " << s.ToString();
    return 1;
  }
  return 0;
}

int ItemToDocID::Delete(const std::string &key) {
  rocksdb::WriteOptions write_options;
  rocksdb::Status s = db_->Delete(write_options, key);
  if (!s.ok()) {
    LOG(ERROR) << "delelte rocksdb failed: " << key << " " << s.ToString();
    return 1;
  }
  return 0;
}

Table::Table(const string &root_path, const string &space_name)
    : name_(space_name) {
  item_length_ = 0;
  field_num_ = 0;
  key_idx_ = -1;
  root_path_ = root_path + "/table";

  table_created_ = false;
  last_docid_ = -1;
  bitmap_mgr_ = nullptr;
  table_params_ = nullptr;
  storage_mgr_ = nullptr;
  item_to_docid_ = nullptr;
  key_field_name_ = "_id";
}

Table::~Table() {
  bitmap_mgr_ = nullptr;
  CHECK_DELETE(table_params_);
  if (storage_mgr_) {
    delete storage_mgr_;
    storage_mgr_ = nullptr;
  }

  delete item_to_docid_;
  item_to_docid_ = nullptr;
  LOG(INFO) << "Table " << name_ << " deleted.";
}

int Table::Load(int &num) {
  int doc_num = num;
  LOG(INFO) << "Load doc_num [" << doc_num << "] truncate to [" << num << "]";

  const auto &iter = attr_idx_map_.find(key_field_name_);
  if (iter == attr_idx_map_.end()) {
    LOG(ERROR) << "Cannot find field [" << key_field_name_ << "]";
    return -1;
  }

  if (!item_to_docid_) {
    item_to_docid_ = new ItemToDocID(root_path_);
    int ret = item_to_docid_->Open();
    if (ret) {
      LOG(ERROR) << name_ << " init item to docid error, ret=" << ret;
      return ret;
    }
  }
  LOG(INFO) << name_ << " load successed! doc num [" << doc_num << "]";
  last_docid_ = doc_num - 1;
  return 0;
}

int Table::CreateTable(TableInfo &table, TableParams &table_params,
                       bitmap::BitmapManager *bitmap_mgr) {
  if (table_created_) {
    return -10;
  }
  bitmap_mgr_ = bitmap_mgr;
  name_ = table.Name();
  std::vector<struct FieldInfo> &fields = table.Fields();

  size_t fields_num = fields.size();
  for (size_t i = 0; i < fields_num; ++i) {
    const string name = fields[i].name;
    DataType ftype = fields[i].data_type;
    bool is_index = fields[i].is_index;
    LOG(INFO) << name_ << " add field [" << name << "], type [" << (int)ftype
              << "], index [" << is_index << "]";
    int ret = AddField(name, ftype, is_index);
    if (ret != 0) {
      return ret;
    }
  }

  if (key_idx_ == -1) {
    LOG(ERROR) << "No field _id! ";
    return -1;
  }

  if (!utils::isFolderExist(root_path_.c_str())) {
    mkdir(root_path_.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  }

  item_to_docid_ = new ItemToDocID(root_path_);
  int ret = item_to_docid_->Open();
  if (ret) {
    LOG(ERROR) << name_ << " init item to docid error, ret=" << ret;
    return ret;
  }

  table_params_ = new TableParams("table");
  table_created_ = true;
  LOG(INFO) << "Create table " << name_
            << " success! item length=" << item_length_
            << ", field num=" << (int)field_num_;

  StorageManagerOptions options;
  options.fixed_value_bytes = item_length_;
  storage_mgr_ = new StorageManager(root_path_, options);
  int cache_size = 512;  // unit : M
  ret = storage_mgr_->Init(name_ + "_table", cache_size);
  if (ret) {
    LOG(ERROR) << "init gamma db error, ret=" << ret;
    return ret;
  }

  LOG(INFO) << "init storageManager success! vector byte size="
            << options.fixed_value_bytes << ", path=" << root_path_;
  return 0;
}

int Table::FTypeSize(DataType fType) {
  int length = 0;
  if (fType == DataType::INT) {
    length = sizeof(int32_t);
  } else if (fType == DataType::LONG) {
    length = sizeof(int64_t);
  } else if (fType == DataType::FLOAT) {
    length = sizeof(float);
  } else if (fType == DataType::DOUBLE) {
    length = sizeof(double);
  } else if (fType == DataType::STRING) {
    length = 0;
  }
  return length;
}

int Table::AddField(const string &name, DataType ftype, bool is_index) {
  if (attr_idx_map_.find(name) != attr_idx_map_.end()) {
    LOG(ERROR) << name_ << " duplicate field " << name;
    return -1;
  }
  if (name == key_field_name_) {
    key_idx_ = field_num_;
  }
  idx_attr_offset_.push_back(item_length_);
  attr_offset_map_.insert(std::pair<string, int>(name, item_length_));
  item_length_ += FTypeSize(ftype);
  attrs_.push_back(ftype);
  idx_attr_map_.insert(std::pair<int, string>(field_num_, name));
  attr_idx_map_.insert(std::pair<string, int>(name, field_num_));
  attr_type_map_.insert(std::pair<string, DataType>(name, ftype));
  attr_is_index_map_.insert(std::pair<string, bool>(name, is_index));
  ++field_num_;
  return 0;
}

int Table::GetDocIDByKey(const std::string &key, int &docid) {
  std::string v;
  int ret = item_to_docid_->Get(key, v);
  if (ret) {
    return ret;
  }

  memcpy(&docid, v.data(), sizeof(docid));
  return 0;
}

int Table::GetKeyByDocid(int docid, std::string &key) {
  if (docid > last_docid_) {
    LOG(ERROR) << name_ << " doc [" << docid << "] in front of [" << last_docid_
               << "]";
    return -1;
  }
  const uint8_t *doc_value = nullptr;
  int ret = storage_mgr_->Get(docid, doc_value);
  if (ret != 0) {
    return ret;
  }
  int field_id = attr_idx_map_[key_field_name_];
  GetFieldRawValue(docid, field_id, key, doc_value);
  delete[] doc_value;

  int check_docid;
  GetDocIDByKey(key, check_docid);
  if (check_docid != docid) {  // docid can be deleted.
    key = "";
    return -1;
  }
  return 0;
}

int Table::Add(const std::string &key,
               const std::unordered_map<std::string, struct Field> &fields,
               int docid) {
  if (key.size() == 0) {
    LOG(ERROR) << name_ << " add item error : _id is null!";
    return -3;
  }

  char vChar[sizeof(docid)];
  memcpy(vChar, &docid, sizeof(docid));
  std::string v = std::string(vChar, sizeof(docid));
  item_to_docid_->Put(key, v);

  for (size_t i = 0; i < attrs_.size(); i++) {
    DataType data_type = attrs_[i];
    if (data_type != DataType::STRING) {
      continue;
    }

    if (fields.find(idx_attr_map_[i]) == fields.end()) {
      storage_mgr_->AddString(docid, idx_attr_map_[i], "", 0);
    }
  }

  std::vector<uint8_t> doc_value(item_length_, 0);

  for (const auto &[name, field] : fields) {
    size_t offset = attr_offset_map_[name];

    DataType attr = attr_type_map_[name];

    if (attr != DataType::STRING) {
      int type_size = FTypeSize(attr);
      memcpy(doc_value.data() + offset, field.value.c_str(), type_size);
    } else {
      int len = field.value.size();
      storage_mgr_->AddString(docid, name, field.value.c_str(), len);
    }
  }

  storage_mgr_->Add(docid, doc_value.data(), item_length_);

  if (docid % 10000 == 0) {
    LOG(INFO) << name_ << " add item _id [" << key << "], num [" << docid
              << "]";
  }
  last_docid_ = docid;
  return 0;
}

int Table::Update(const std::unordered_map<std::string, struct Field> &fields,
                  int docid) {
  if (fields.size() == 0) return 0;

  int ret = 0;
  const uint8_t *ori_doc_value = nullptr;

  ret = storage_mgr_->Get(docid, ori_doc_value);
  if (ret != 0) {
    return ret;
  }

  uint8_t doc_value[item_length_];

  memcpy(doc_value, ori_doc_value, item_length_);
  delete[] ori_doc_value;

  for (const auto &[name, field] : fields) {
    const auto &it = attr_idx_map_.find(name);
    if (it == attr_idx_map_.end()) {
      LOG(ERROR) << name_ << " cannot find field name [" << name << "]";
      continue;
    }

    int field_id = it->second;
    int offset = idx_attr_offset_[field_id];

    if (field.datatype == DataType::STRING) {
      int len = field.value.size();
      storage_mgr_->UpdateString(docid, name, field.value.c_str(), len);
    } else {
      memcpy(doc_value + offset, field.value.data(), field.value.size());
    }
  }

  storage_mgr_->Update(docid, doc_value, item_length_);
  return 0;
}

std::unordered_map<std::string, bool> Table::CheckFieldIsEqual(
    const std::unordered_map<std::string, Field> &fields, int docid) {
  std::unordered_map<std::string, bool> is_equal;
  std::vector<std::string> field_names;
  Doc doc;
  if (GetDocInfo(docid, doc, field_names) == 0) {
    auto &table_fields = doc.TableFields();
    for (auto &[name, field] : fields) {
      const std::string &val = field.value;
      if (attr_idx_map_.count(name) == false) {
        continue;
      }

      const auto &it = table_fields.find(name);
      if (it == table_fields.end()) {
        continue;
      }
      if (name == it->second.name && val == it->second.value) {
        is_equal[name] = true;
      }
    }
  }
  return is_equal;
}

int Table::Delete(std::string &key) { return item_to_docid_->Delete(key); }

long Table::GetMemoryBytes() {
  long total_mem_bytes = 0;
  return total_mem_bytes;
}

const uint8_t *Table::GetDocBuffer(int docid) {
  const uint8_t *doc_value = nullptr;
  storage_mgr_->Get(docid, doc_value);
  return doc_value;
}

int Table::GetDocInfo(const std::string &key, Doc &doc,
                      const std::vector<std::string> &fields) {
  int doc_id = 0;
  int ret = GetDocIDByKey(key, doc_id);
  if (ret < 0) {
    return ret;
  }
  return GetDocInfo(doc_id, doc, fields);
}

int Table::GetDocInfo(const int docid, Doc &doc,
                      const std::vector<std::string> &fields) {
  if (docid > last_docid_) {
    LOG(ERROR) << "doc [" << docid << "] in front of [" << last_docid_ << "]";
    return -1;
  }
  const uint8_t *doc_value = nullptr;
  int ret = storage_mgr_->Get(docid, doc_value);
  if (ret != 0) {
    return ret;
  }
  auto &table_fields = doc.TableFields();

  auto assign_field = [&](struct Field &field, const std::string &field_name) {
    DataType type = attr_type_map_[field_name];
    field.name = field_name;
    field.datatype = type;
  };

  if (fields.size() == 0) {
    for (const auto &it : attr_idx_map_) {
      assign_field(table_fields[it.first], it.first);
      GetFieldRawValue(docid, it.second, table_fields[it.first].value,
                       doc_value);
    }
  } else {
    for (const std::string &f : fields) {
      const auto &iter = attr_idx_map_.find(f);
      if (iter == attr_idx_map_.end()) {
        LOG(ERROR) << name_ << " cannot find field [" << f << "]";
        continue;
      }
      int field_idx = iter->second;
      assign_field(table_fields[iter->first], f);
      GetFieldRawValue(docid, field_idx, table_fields[iter->first].value,
                       doc_value);
    }
  }
  delete[] doc_value;
  return 0;
}

int Table::GetFieldRawValue(int docid, const std::string &field_name,
                            std::string &value, const uint8_t *doc_v) {
  const auto iter = attr_idx_map_.find(field_name);
  if (iter == attr_idx_map_.end()) {
    LOG(ERROR) << name_ << " cannot find field [" << field_name << "]";
    return -1;
  }
  GetFieldRawValue(docid, iter->second, value, doc_v);
  return 0;
}

int Table::GetFieldRawValue(int docid, int field_id, std::string &value,
                            const uint8_t *doc_v) {
  if ((docid < 0) or (field_id < 0 || field_id >= field_num_)) return -1;

  const auto iter = idx_attr_map_.find(field_id);
  if (iter == idx_attr_map_.end()) {
    LOG(ERROR) << name_ << " cannot find field [" << field_id << "]";
    return -1;
  }

  std::string field_name = iter->second;

  const uint8_t *doc_value = doc_v;
  bool is_free = false;
  if (doc_value == nullptr) {
    is_free = true;
    int ret = storage_mgr_->Get(docid, doc_value);
    if (ret != 0) {
      return ret;
    }
  }

  DataType data_type = attrs_[field_id];
  size_t offset = idx_attr_offset_[field_id];

  if (data_type == DataType::STRING) {
    storage_mgr_->GetString(docid, field_name, value);
  } else {
    int value_len = FTypeSize(data_type);
    value = std::string((const char *)(doc_value + offset), value_len);
  }

  if (is_free) {
    delete[] doc_value;
  }

  return 0;
}

int Table::GetFieldRawValue(int docid, int field_id,
                            std::vector<uint8_t> &value, const uint8_t *doc_v) {
  if ((docid < 0) or (field_id < 0 || field_id >= field_num_)) return -1;

  const uint8_t *doc_value = doc_v;
  bool is_free = false;
  if (doc_value == nullptr) {
    is_free = true;
    int ret = storage_mgr_->Get(docid, doc_value);
    if (ret != 0) {
      return ret;
    }
  }

  DataType data_type = attrs_[field_id];
  size_t offset = idx_attr_offset_[field_id];

  if (data_type == DataType::STRING) {
    const auto iter = idx_attr_map_.find(field_id);
    if (iter == idx_attr_map_.end()) {
      LOG(ERROR) << name_ << " cannot find field [" << field_id << "]";
      return -1;
    }

    std::string field_name = iter->second;
    std::string str;
    storage_mgr_->GetString(docid, field_name, str);
    value.resize(str.size());
    memcpy(value.data(), str.c_str(), str.size());
  } else {
    int value_len = FTypeSize(data_type);
    value.resize(value_len);
    memcpy(value.data(), doc_value + offset, value_len);
  }

  if (is_free) {
    delete[] doc_value;
  }

  return 0;
}

int Table::GetFieldType(const std::string &field_name, DataType &type) {
  const auto &it = attr_type_map_.find(field_name);
  if (it == attr_type_map_.end()) {
    LOG(ERROR) << name_ << " cannot find field [" << field_name << "]";
    return -1;
  }
  type = it->second;
  return 0;
}

int Table::GetAttrType(std::map<std::string, DataType> &attr_type_map) {
  for (const auto &attr_type : attr_type_map_) {
    attr_type_map.insert(attr_type);
  }
  return 0;
}

int Table::GetAttrIsIndex(std::map<std::string, bool> &attr_is_index_map) {
  for (const auto &attr_is_index : attr_is_index_map_) {
    attr_is_index_map.insert(attr_is_index);
  }
  return 0;
}

int Table::GetAttrIdx(const std::string &field) const {
  const auto &iter = attr_idx_map_.find(field.c_str());
  return (iter != attr_idx_map_.end()) ? iter->second : -1;
}

bool Table::AlterCacheSize(int cache_size) {
  // return storage_mgr_->AlterCacheSize(cache_size);
  return 0;
}

void Table::GetCacheSize(int &cache_size) {
  storage_mgr_->GetCacheSize(cache_size);
}

int Table::GetStorageManagerSize() {
  int table_doc_num = 0;
  if (storage_mgr_) {
    table_doc_num = storage_mgr_->Size();
  }
  LOG(INFO) << name_ << " read doc_num=" << table_doc_num
            << " in table storage_mgr.";
  return table_doc_num;
}

}  // namespace vearch
