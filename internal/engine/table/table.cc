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

namespace vearch {

Table::Table(const string &space_name, StorageManager *storage_mgr, int cf_id)
    : name_(space_name), storage_mgr_(storage_mgr), cf_id_(cf_id) {
  item_length_ = 0;
  field_num_ = 0;
  key_idx_ = -1;

  table_created_ = false;
  last_docid_ = -1;
  table_params_ = nullptr;
  key_field_name_ = "_id";
}

Table::~Table() {
  if (id_load_thread_.joinable()) {
    id_load_thread_.join();
  }
  storage_mgr_ = nullptr;
  CHECK_DELETE(table_params_);

  LOG(INFO) << "Table " << name_ << " deleted.";
}

int Table::Load(int64_t &num) {
  int64_t max_doc_id = num - 1;
  int64_t doc_id = -1;
  LOG(INFO) << "Wanted load doc_num [" << num << "], doc_id [" << max_doc_id
            << "]";

  const auto &iter = attr_idx_map_.find(key_field_name_);
  if (iter == attr_idx_map_.end()) {
    LOG(ERROR) << "Cannot find field [" << key_field_name_ << "]";
    return -1;
  }

  if (max_doc_id <= -1) return 0;
  for (int64_t i = max_doc_id; i >= 0; --i) {
    auto result = storage_mgr_->Get(cf_id_, i);
    if (result.first.ok()) {
      doc_id = i;
      break;
    }
  }

  last_docid_ = doc_id;
  LOG(INFO) << name_ << " load successed! doc num [" << doc_id + 1
            << "], last docid [" << last_docid_ << "]";
  return 0;
}

int Table::Load_id() {
  std::unique_ptr<rocksdb::Iterator> it = storage_mgr_->NewIterator(cf_id_);
  int64_t max_doc_id = storage_mgr_->Size();

  std::string key_prefix = key_field_name_ + ":";
  std::string start_key = key_prefix + utils::ToRowKey(0);
  std::string end_key = key_prefix + utils::ToRowKey(max_doc_id);

  it->Seek(rocksdb::Slice(start_key));
  for (; it->Valid(); it->Next()) {
    rocksdb::Slice current_key = it->key();
    if (current_key.compare(end_key) >= 0) {
      break;
    }

    std::string docid_str = current_key.ToString().substr(key_prefix.size());
    int64_t docid = utils::FromRowKey(docid_str);

    if (docid < 0 || docid >= max_doc_id) {
      break;
    }

    doc_id_map_[docid] = it->value().ToString();
  }

  LOG(INFO) << name_ << " load docid to key map successed!";
  return 0;
}

Status Table::CreateTable(TableInfo &table) {
  if (table_created_) {
    return Status::IOError();
  }
  name_ = table.Name();
  std::vector<struct FieldInfo> &fields = table.Fields();

  size_t fields_num = fields.size();
  for (size_t i = 0; i < fields_num; ++i) {
    const string name = fields[i].name;
    DataType ftype = fields[i].data_type;
    bool is_index = fields[i].is_index;
    LOG(INFO) << name_ << " add field [" << name << "], type [" << (int)ftype
              << "], index [" << is_index << "]";
    Status status = AddField(name, ftype, is_index);
    if (!status.ok()) {
      return status;
    }
  }

  if (key_idx_ == -1) {
    std::string msg = "No field _id! ";
    LOG(ERROR) << msg;
    return Status::ParamError(msg);
  }

  key_cf_id_ = storage_mgr_->CreateColumnFamily("key_to_docid");

  table_params_ = new TableParams("table");
  enable_id_cache_ = table.EnableIdCache();
  table_created_ = true;
  LOG(INFO) << "Create table " << name_
            << " success, item length=" << item_length_
            << ", field num=" << (int)field_num_;

  return Status::OK();
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
  } else if (fType == DataType::STRING || fType == DataType::STRINGARRAY) {
    length = 0;
  }
  return length;
}

Status Table::AddField(const string &name, DataType ftype, bool is_index) {
  if (attr_idx_map_.find(name) != attr_idx_map_.end()) {
    std::string msg = name_ + " duplicate field " + name;
    LOG(ERROR) << msg;
    return Status::ParamError(msg);
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
  return Status::OK();
}

int Table::GetDocidByKey(const std::string &key, int64_t &docid) {
  std::string v;
  auto result = storage_mgr_->Get(key_cf_id_, key);
  if (!result.first.ok()) {
    return result.first.code();
  }

  // docid is int32_t
  if (result.second.size() == sizeof(int32_t)) {
    int32_t docid32;
    memcpy(&docid32, result.second.data(), sizeof(docid32));
    docid = docid32;
    return 0;
  }

  memcpy(&docid, result.second.data(), sizeof(docid));
  return 0;
}

int Table::GetKeyByDocid(int64_t docid, std::string &key) {
  if (docid > last_docid_) {
    LOG(ERROR) << name_ << " doc [" << docid << "] in front of [" << last_docid_
               << "]";
    return -1;
  }
  int field_id = attr_idx_map_[key_field_name_];
  GetFieldRawValue(docid, field_id, key);

  int64_t check_docid;
  GetDocidByKey(key, check_docid);
  if (check_docid != docid) {  // docid can be deleted.
    key = "";
    return -1;
  }
  return 0;
}

int Table::Add(const std::string &key,
               const std::unordered_map<std::string, struct Field> &fields,
               int64_t docid) {
  if (key.empty()) {
    LOG(ERROR) << name_ << " add item error : _id is null!";
    return -3;
  }

  std::string v(reinterpret_cast<char *>(&docid), sizeof(docid));
  storage_mgr_->Put(key_cf_id_, key, v);

  for (size_t i = 0; i < attrs_.size(); i++) {
    DataType data_type = attrs_[i];
    if (data_type != DataType::STRING && data_type != DataType::STRINGARRAY) {
      continue;
    }

    if (fields.find(idx_attr_map_[i]) == fields.end()) {
      storage_mgr_->AddString(cf_id_, docid, idx_attr_map_[i], "", 0);
    }
  }

  std::vector<uint8_t> doc_value(item_length_, 0);

  for (const auto &[name, field] : fields) {
    size_t offset = attr_offset_map_[name];

    DataType attr = attr_type_map_[name];

    if (attr != DataType::STRING && attr != DataType::STRINGARRAY) {
      int type_size = FTypeSize(attr);
      memcpy(doc_value.data() + offset, field.value.c_str(), type_size);
    } else {
      int len = field.value.size();
      storage_mgr_->AddString(cf_id_, docid, name, field.value.c_str(), len);
    }
  }

  storage_mgr_->Add(cf_id_, docid, doc_value.data(), item_length_);

  if (enable_id_cache_) {
    doc_id_map_[docid] = key;
  }

  if (docid % 10000 == 0) {
    LOG(DEBUG) << name_ << " add item _id [" << key << "], num [" << docid
               << "]";
  }
  last_docid_ = docid;
  return 0;
}

int Table::Update(const std::unordered_map<std::string, struct Field> &fields,
                  int64_t docid) {
  if (fields.size() == 0) return 0;

  auto result = storage_mgr_->Get(cf_id_, docid);
  if (!result.first.ok()) {
    return result.first.code();
  }

  uint8_t doc_value[item_length_];

  memcpy(doc_value, result.second.data(), item_length_);

  for (const auto &[name, field] : fields) {
    const auto &it = attr_idx_map_.find(name);
    if (it == attr_idx_map_.end()) {
      LOG(ERROR) << name_ << " cannot find field name [" << name << "]";
      continue;
    }

    int field_id = it->second;
    int offset = idx_attr_offset_[field_id];

    if (field.datatype == DataType::STRING ||
        field.datatype == DataType::STRINGARRAY) {
      int len = field.value.size();
      storage_mgr_->UpdateString(cf_id_, docid, name, field.value.c_str(), len);
    } else {
      memcpy(doc_value + offset, field.value.data(), field.value.size());
    }
  }

  storage_mgr_->Update(cf_id_, docid, doc_value, item_length_);
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

int Table::Delete(std::string &key) {
  int64_t docid = 0;
  int ret = GetDocidByKey(key, docid);
  if (ret < 0) {
    return ret;
  }

  doc_id_map_.unsafe_erase(docid);
  ret = storage_mgr_->Delete(cf_id_, key).code();
  if (ret != 0) {
    return ret;
  }

  for (size_t i = 0; i < attrs_.size(); i++) {
    DataType data_type = attrs_[i];
    if (data_type != DataType::STRING && data_type != DataType::STRINGARRAY) {
      continue;
    }

    Status status = storage_mgr_->DeleteString(cf_id_, docid, idx_attr_map_[i]);
    if (!status.ok()) {
      return status.code();
    }
  }

  return storage_mgr_->Delete(key_cf_id_, key).code();
}

long Table::GetMemoryBytes() {
  long total_mem_bytes = 0;
  return total_mem_bytes;
}

int Table::GetDocInfo(const std::string &key, Doc &doc,
                      const std::vector<std::string> &fields) {
  int64_t doc_id = 0;
  int ret = GetDocidByKey(key, doc_id);
  if (ret < 0) {
    return ret;
  }
  return GetDocInfo(doc_id, doc, fields);
}

int Table::GetDocInfo(const int64_t docid, Doc &doc,
                      const std::vector<std::string> &fields) {
  if (docid > last_docid_) {
    LOG(ERROR) << "doc [" << docid << "] in front of [" << last_docid_ << "]";
    return -1;
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
      GetFieldRawValue(docid, it.second, table_fields[it.first].value);
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
      GetFieldRawValue(docid, field_idx, table_fields[iter->first].value);
    }
  }
  return 0;
}

int Table::GetFieldRawValue(int64_t docid, const std::string &field_name,
                            std::string &value) {
  const auto iter = attr_idx_map_.find(field_name);
  if (iter == attr_idx_map_.end()) {
    LOG(ERROR) << name_ << " cannot find field [" << field_name << "]";
    return -1;
  }
  GetFieldRawValue(docid, iter->second, value);
  return 0;
}

int Table::GetFieldRawValue(int64_t docid, int field_id, std::string &value) {
  if ((docid < 0) or (field_id < 0 || field_id >= field_num_)) return -1;

  const auto iter = idx_attr_map_.find(field_id);
  if (iter == idx_attr_map_.end()) {
    LOG(ERROR) << name_ << " cannot find field [" << field_id << "]";
    return -1;
  }

  std::string field_name = iter->second;

  auto result = storage_mgr_->Get(cf_id_, docid);
  if (!result.first.ok()) {
    return result.first.code();
  }

  DataType data_type = attrs_[field_id];
  size_t offset = idx_attr_offset_[field_id];

  if (data_type == DataType::STRING || data_type == DataType::STRINGARRAY) {
    storage_mgr_->GetString(cf_id_, docid, field_name, value);
  } else {
    int value_len = FTypeSize(data_type);
    value =
        std::string((const char *)(result.second.data() + offset), value_len);
  }

  return 0;
}

int Table::GetFieldRawValue(int64_t docid, int field_id,
                            std::vector<uint8_t> &value, std::vector<uint8_t> &doc_value) {
  if ((docid < 0) or (field_id < 0 || field_id >= field_num_)) return -1;

  DataType data_type = attrs_[field_id];
  size_t offset = idx_attr_offset_[field_id];

  if (data_type == DataType::STRING || data_type == DataType::STRINGARRAY) {
    const auto iter = idx_attr_map_.find(field_id);
    if (iter == idx_attr_map_.end()) {
      LOG(ERROR) << name_ << " cannot find field [" << field_id << "]";
      return -1;
    }

    std::string field_name = iter->second;
    std::string str;
    auto status = storage_mgr_->GetString(cf_id_, docid, field_name, str);
    if (!status.ok()) {
      return status.code();
    }
    value.resize(str.size());
    memcpy(value.data(), str.c_str(), str.size());
  } else {
    if (doc_value.size() == 0) {
      auto result = storage_mgr_->Get(cf_id_, docid);
      if (!result.first.ok()) {
        return result.first.code();
      }
      doc_value.resize(item_length_);
      memcpy(doc_value.data(), result.second.data(), item_length_);
    }
    int value_len = FTypeSize(data_type);
    value.resize(value_len);
    memcpy(value.data(), doc_value.data() + offset, value_len);
  }

  return 0;
}

int Table::GetRawValue(int64_t docid, std::vector<uint8_t> &value) {
  if (docid > last_docid_) {
    LOG(ERROR) << name_ << " doc [" << docid << "] in front of [" << last_docid_
               << "]";
    return -1;
  }
  auto result = storage_mgr_->Get(cf_id_, docid);
  if (!result.first.ok()) {
    return result.first.code();
  }
  value.resize(item_length_);
  memcpy(value.data(), result.second.data(), item_length_);
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

int64_t Table::GetStorageManagerSize() {
  int64_t table_doc_num = 0;
  if (storage_mgr_) {
    table_doc_num = storage_mgr_->Size();
  }
  LOG(INFO) << name_ << " read doc_num=" << table_doc_num
            << " in table storage_mgr.";
  return table_doc_num;
}

int Table::SetStorageManagerSize(int64_t doc_num) {
  int ret = 0;
  if (storage_mgr_) {
    ret = storage_mgr_->SetSize(doc_num);
    if (ret != 0) {
      LOG(ERROR) << name_ << " set doc_num=" << doc_num
                 << " in table storage_mgr failed!, last_docid_="
                 << last_docid_;
    } else {
      return 0;
    }
  }
  return ret;
}

int Table::LoadIdFromTable() {
  if (id_load_thread_.joinable()) {
    id_load_thread_.join();
  }

  auto func_load_id = std::bind(&Table::Load_id, this);
  id_load_thread_ = std::thread(func_load_id);

  return 0;
}

void Table::SetEnableIdCache(bool enabled) {
  enable_id_cache_ = enabled;
  if (enabled) {
    LoadIdFromTable();
  } else {
    if (id_load_thread_.joinable()) {
      id_load_thread_.join();
    }
    doc_id_map_.clear();
  }
}

}  // namespace vearch
