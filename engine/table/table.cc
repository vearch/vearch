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

namespace tig_gamma {

Table::Table(const string &root_path, bool b_compress) {
  item_length_ = 0;
  field_num_ = 0;
  string_field_num_ = 0;
  key_idx_ = -1;
  root_path_ = root_path + "/table";
  seg_num_ = 0;
  b_compress_ = b_compress;

  table_created_ = false;
  last_docid_ = -1;
  bitmap_mgr_ = nullptr;
  table_params_ = nullptr;
  storage_mgr_ = nullptr;
  key_field_name_ = "_id";
  LOG(INFO) << "Table created success!";
}

Table::~Table() {
  bitmap_mgr_ = nullptr;
  CHECK_DELETE(table_params_);
  if (storage_mgr_) {
    delete storage_mgr_;
    storage_mgr_ = nullptr;
  }
  LOG(INFO) << "Table deleted.";
}

int Table::Load(int &num) {
  int doc_num = storage_mgr_->Size();
  storage_mgr_->Truncate(num);
  LOG(INFO) << "Load doc_num [" << doc_num << "] truncate to [" << num << "]";
  doc_num = num;

  const auto &iter = attr_idx_map_.find(key_field_name_);
  if (iter == attr_idx_map_.end()) {
    LOG(ERROR) << "Cannot find field [" << key_field_name_ << "]";
    return -1;
  }

  int idx = iter->second;
  if (id_type_ == 0) {
    for (int i = 0; i < doc_num; ++i) {
      if (bitmap_mgr_->Test(i)) { continue; }
      std::string key;
      GetFieldRawValue(i, idx, key);
      int64_t k = utils::StringToInt64(key);
      item_to_docid_.insert(k, i);
    }
  } else {
    for (int i = 0; i < doc_num; ++i) {
      if (bitmap_mgr_->Test(i)) { continue; }
      long key = -1;
      std::string key_str;
      GetFieldRawValue(i, idx, key_str);
      memcpy(&key, key_str.c_str(), sizeof(key));
      item_to_docid_.insert(key, i);
    }
  }

  LOG(INFO) << "Table load successed! doc num [" << doc_num << "]";
  last_docid_ = doc_num - 1;
  return 0;
}

int Table::Sync() {
  int ret = storage_mgr_->Sync();
  LOG(INFO) << "Table [" << name_ << "] sync, doc num[" << storage_mgr_->Size()
            << "]";
  return ret;
}

int Table::CreateTable(TableInfo &table, TableParams &table_params,
                       bitmap::BitmapManager *bitmap_mgr) {
  if (table_created_) {
    return -10;
  }
  bitmap_mgr_ = bitmap_mgr;
  name_ = table.Name();
  std::vector<struct FieldInfo> &fields = table.Fields();

  b_compress_ = table.IsCompress();
  LOG(INFO) << "Table compress [" << b_compress_ << "]";

  size_t fields_num = fields.size();
  for (size_t i = 0; i < fields_num; ++i) {
    const string name = fields[i].name;
    DataType ftype = fields[i].data_type;
    bool is_index = fields[i].is_index;
    LOG(INFO) << "Add field name [" << name << "], type [" << (int)ftype
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

  table_params_ = new TableParams("table");
  table_created_ = true;
  LOG(INFO) << "Create table " << name_
            << " success! item length=" << item_length_
            << ", field num=" << (int)field_num_;

  StorageManagerOptions options;
  options.segment_size = 500000;
  options.fixed_value_bytes = item_length_;
  options.seg_block_capacity = 400000;
  storage_mgr_ =
      new StorageManager(root_path_, BlockType::TableBlockType, options);
  int cache_size = 512;  // unit : M
  int str_cache_size = 512;
  int ret = storage_mgr_->Init(name_ + "_table", cache_size, str_cache_size);
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
    // block_id, in_block_pos, str_len
    length = sizeof(uint32_t) + sizeof(in_block_pos_t) + sizeof(str_len_t);
  }
  return length;
}

int Table::AddField(const string &name, DataType ftype, bool is_index) {
  if (attr_idx_map_.find(name) != attr_idx_map_.end()) {
    LOG(ERROR) << "Duplicate field " << name;
    return -1;
  }
  if (name == key_field_name_) {
    key_idx_ = field_num_;
    id_type_ = ftype == DataType::STRING ? 0 : 1;
  }
  if (ftype == DataType::STRING) {
    str_field_id_.insert(std::make_pair(field_num_, string_field_num_));
    ++string_field_num_;
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

int Table::ParseStrPosition(const uint8_t *buf, uint32_t &block_id,
                            in_block_pos_t &in_block_pos, str_len_t &len) {
  memcpy(&block_id, buf, sizeof(block_id));
  memcpy(&in_block_pos, buf + sizeof(block_id), sizeof(in_block_pos));
  memcpy(&len, buf + sizeof(block_id) + sizeof(in_block_pos), sizeof(len));
  return 0;
}

int Table::SetStrPosition(uint8_t *buf, uint32_t block_id,
                          in_block_pos_t in_block_pos, str_len_t len) {
  memcpy(buf, &block_id, sizeof(block_id));
  memcpy(buf + sizeof(block_id), &in_block_pos, sizeof(in_block_pos));
  memcpy(buf + sizeof(block_id) + sizeof(in_block_pos), &len, sizeof(len));
  return 0;
}

void Table::CheckStrLen(const std::string &field_name, str_len_t &len) {
  if (len > STR_MAX_INDEX_LEN && attr_is_index_map_[field_name] == true) {
    LOG(ERROR) << "Str len[" << len << "] greater than STR_MAX_INDEX_LEN["
               << STR_MAX_INDEX_LEN << "] does not support indexing.";
    len = STR_MAX_INDEX_LEN;
  }
  if (len > MAX_STRING_LEN) {
    LOG(ERROR) << "Str len[" << len << "] > MAX_STRING_LEN[" << MAX_STRING_LEN
               << "]. Truncation occurs.";
    len = MAX_STRING_LEN;
  }
}


int Table::GetDocIDByKey(const std::string &key, int &docid) {
  if (id_type_ == 0) {
    int64_t k = utils::StringToInt64(key);
    if (item_to_docid_.find(k, docid)) {
      return 0;
    }
  } else {
    long key_long = -1;
    memcpy(&key_long, key.data(), sizeof(key_long));

    if (item_to_docid_.find(key_long, docid)) {
      return 0;
    }
  }
  return -1;
}

int Table::GetKeyByDocid(int docid, std::string &key) {
  if (docid > last_docid_) {
    LOG(ERROR) << "doc [" << docid << "] in front of [" << last_docid_ << "]";
    return -1;
  }
  const uint8_t *doc_value = nullptr;
  if (0 != storage_mgr_->Get(docid, doc_value)) {
    return -1;
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

int Table::Add(const std::string &key, const std::vector<struct Field> &fields,
               int docid) {
  if (fields.size() != attr_idx_map_.size()) {
    LOG(ERROR) << "Field num [" << fields.size() << "] not equal to ["
               << attr_idx_map_.size() << "]";
    return -2;
  }
  if (key.size() == 0) {
    LOG(ERROR) << "Add item error : _id is null!";
    return -3;
  }

  if (id_type_ == 0) {
    int64_t k = utils::StringToInt64(key);
    item_to_docid_.insert(k, docid);
  } else {
    long key_long = -1;
    memcpy(&key_long, key.data(), sizeof(key_long));

    item_to_docid_.insert(key_long, docid);
  }

  uint8_t doc_value[item_length_];

  for (size_t i = 0; i < fields.size(); ++i) {
    const auto &field_value = fields[i];
    const std::string &name = field_value.name;
    size_t offset = attr_offset_map_[name];

    DataType attr = attr_type_map_[name];

    if (attr != DataType::STRING) {
      int type_size = FTypeSize(attr);
      memcpy(doc_value + offset, field_value.value.c_str(), type_size);
    } else {
      str_len_t len = field_value.value.size();
      CheckStrLen(name, len);
      uint32_t block_id;
      in_block_pos_t in_block_pos;
      storage_mgr_->AddString(field_value.value.c_str(), len, block_id,
                              in_block_pos);

      SetStrPosition(doc_value + offset, block_id, in_block_pos, len);
    }
  }

  storage_mgr_->Add((const uint8_t *)doc_value, item_length_);

  if (docid % 10000 == 0) {
    if (id_type_ == 0) {
      LOG(INFO) << "Add item _id [" << key << "], num [" << docid << "]";
    } else {
      long key_long = -1;
      memcpy(&key_long, key.data(), sizeof(key_long));
      LOG(INFO) << "Add item _id [" << key_long << "], num [" << docid << "]";
    }
  }
  last_docid_ = docid;
  return 0;
}

int Table::BatchAdd(int start_id, int batch_size, int docid,
                    std::vector<Doc> &doc_vec, BatchResult &result) {
#ifdef PERFORMANCE_TESTING
  double start = utils::getmillisecs();
#endif

#pragma omp parallel for
  for (int i = 0; i < batch_size; ++i) {
    int id = docid + i;
    Doc &doc = doc_vec[start_id + i];

    std::string &key = doc.Key();
    if (key.size() == 0) {
      std::string msg = "Add item error : _id is null!";
      result.SetResult(i, -1, msg);
      LOG(ERROR) << msg;
      continue;
    }

    if (id_type_ == 0) {
      int64_t k = utils::StringToInt64(key);
      item_to_docid_.insert(k, id);
    } else {
      long key_long = -1;
      memcpy(&key_long, key.data(), sizeof(key_long));

      item_to_docid_.insert(key_long, id);
    }
  }

  for (int i = 0; i < batch_size; ++i) {
    int id = docid + i;
    Doc &doc = doc_vec[start_id + i];
    std::vector<Field> &fields = doc.TableFields();
    uint8_t doc_value[item_length_];

    if(fields.size() == 0) {
        Field field;
        field.name = "_id";
        field.value = doc.Key();
        field.datatype = DataType::STRING;
        fields.push_back(field);
    }

    for (size_t j = 0; j < fields.size(); ++j) {
      const auto &field_value = fields[j];
      const string &name = field_value.name;
      
      size_t offset = attr_offset_map_[name];

      DataType attr = attr_type_map_[name];

      if (attr != DataType::STRING) {
        int type_size = FTypeSize(attr);
        memcpy(doc_value + offset, field_value.value.c_str(), type_size);
      } else {
        str_len_t len = field_value.value.size();
        CheckStrLen(name, len);

        uint32_t block_id;
        in_block_pos_t in_block_pos;
        storage_mgr_->AddString(field_value.value.c_str(), len, block_id,
                                in_block_pos);
        SetStrPosition(doc_value + offset, block_id, in_block_pos, len);
      }
    }

    storage_mgr_->Add((const uint8_t *)doc_value, item_length_);
    if (id % 10000 == 0) {
      std::string &key = doc_vec[i].Key();
      if (id_type_ == 0) {
        LOG(INFO) << "Add item _id [" << key << "], num [" << id << "]";
      } else {
        long key_long = -1;
        memcpy(&key_long, key.data(), sizeof(key_long));
        LOG(INFO) << "Add item _id [" << key_long << "], num [" << id << "]";
      }
    }
  }

  // Compress();
#ifdef PERFORMANCE_TESTING
  double end = utils::getmillisecs();
  if (docid % 10000 == 0) {
    LOG(INFO) << "table cost [" << end - start << "]ms";
  }
#endif
  last_docid_ = docid + batch_size;
  return 0;
}

int Table::Update(const std::vector<Field> &fields, int docid) {
  if (fields.size() == 0) return 0;

  const uint8_t *ori_doc_value = nullptr;
  storage_mgr_->Get(docid, ori_doc_value);

  uint8_t doc_value[item_length_];

  memcpy(doc_value, ori_doc_value, item_length_);
  delete[] ori_doc_value;

  for (size_t i = 0; i < fields.size(); ++i) {
    const struct Field &field_value = fields[i];
    const string &name = field_value.name;
    const auto &it = attr_idx_map_.find(name);
    if (it == attr_idx_map_.end()) {
      LOG(ERROR) << "Cannot find field name [" << name << "]";
      continue;
    }

    int field_id = it->second;
    int offset = idx_attr_offset_[field_id];

    if (field_value.datatype == DataType::STRING) {
      uint32_t old_block_id;
      in_block_pos_t old_in_block_pos;
      str_len_t old_len;
      ParseStrPosition(doc_value + offset, old_block_id, old_in_block_pos,
                       old_len);

      str_len_t len = field_value.value.size();
      CheckStrLen(name, len);
      uint32_t block_id = old_block_id;
      in_block_pos_t in_block_pos = old_in_block_pos;
      storage_mgr_->UpdateString(docid, field_value.value.c_str(), old_len, len,
                                 block_id, in_block_pos);
      SetStrPosition(doc_value + offset, block_id, in_block_pos, len);
    } else {
      memcpy(doc_value + offset, field_value.value.data(),
             field_value.value.size());
    }
  }

  storage_mgr_->Update(docid, doc_value, item_length_);
  return 0;
}

std::vector<bool> Table::CheckFieldIsEqual(const std::vector<Field> &fields,
                                           int docid) {
  int size = fields.size();
  std::vector<bool> is_equal(size, false);
  std::vector<std::string> field_names;
  Doc doc;
  if (GetDocInfo(docid, doc, field_names) == 0) {
    const std::vector<struct Field> &table_fields = doc.TableFields();
    for (int i = 0; i < size; ++i) {
      const std::string &name = fields[i].name;
      const std::string &val = fields[i].value;
      if (attr_idx_map_.count(name) == false) {
        continue;
      }
      size_t idx = attr_idx_map_[name];
      if (idx < table_fields.size() && name == table_fields[idx].name
          && val == table_fields[idx].value) {
        is_equal[i] = true;
        // LOG(INFO) << "doc [" << docid << "] field[" << name << "], value["
        //           << val << "] is equal.";
      }
    }
  }
  return is_equal;
}

int Table::Delete(std::string &key) {
  if (id_type_ == 0) {
    int64_t k = utils::StringToInt64(key);
    item_to_docid_.erase(k);
  } else {
    long key_long = -1;
    memcpy(&key_long, key.data(), sizeof(key_long));

    item_to_docid_.erase(key_long);
  }
  return 0;
}

long Table::GetMemoryBytes() {
  long total_mem_bytes = 0;
  // for (int i = 0; i < seg_num_; ++i) {
  //   total_mem_bytes += main_file_[i]->GetMemoryBytes();
  // }
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
  std::vector<struct Field> &table_fields = doc.TableFields();

  auto assign_field = [&](struct Field &field, const std::string &field_name) {
    DataType type = attr_type_map_[field_name];
    std::string source;
    field.name = field_name;
    field.source = source;
    field.datatype = type;
  };

  int i = 0;
  if (fields.size() == 0) {
    table_fields.resize(attr_idx_map_.size());

    for (const auto &it : attr_idx_map_) {
      assign_field(table_fields[it.second], it.first);
      GetFieldRawValue(docid, it.second, table_fields[it.second].value, doc_value);
    }
  } else {
    table_fields.resize(fields.size());
    for (const std::string &f : fields) {
      const auto &iter = attr_idx_map_.find(f);
      if (iter == attr_idx_map_.end()) {
        LOG(ERROR) << "Cannot find field [" << f << "]";
        continue;
      }
      int field_idx = iter->second;
      assign_field(table_fields[i], f);
      GetFieldRawValue(docid, field_idx, table_fields[i].value, doc_value);
      ++i;
    }
  }
  delete[] doc_value;
  return 0;
}

int Table::GetFieldRawValue(int docid, const std::string &field_name,
                            std::string &value, const uint8_t *doc_v) {
  const auto iter = attr_idx_map_.find(field_name);
  if (iter == attr_idx_map_.end()) {
    LOG(ERROR) << "Cannot find field [" << field_name << "]";
    return -1;
  }
  GetFieldRawValue(docid, iter->second, value, doc_v);
  return 0;
}

int Table::GetFieldRawValue(int docid, int field_id, std::string &value,
                            const uint8_t *doc_v) {
  if ((docid < 0) or (field_id < 0 || field_id >= field_num_)) return -1;

  const uint8_t *doc_value = doc_v;
  bool is_free = false;
  if (doc_value == nullptr) {
    is_free = true;
    storage_mgr_->Get(docid, doc_value);
  }

  DataType data_type = attrs_[field_id];
  size_t offset = idx_attr_offset_[field_id];

  if (data_type == DataType::STRING) {
    uint32_t block_id = 0;
    in_block_pos_t in_block_pos = 0;
    str_len_t len = 0;
    ParseStrPosition(doc_value + offset, block_id, in_block_pos, len);
    storage_mgr_->GetString(docid, value, block_id, in_block_pos, len);
  } else {
    int value_len = FTypeSize(data_type);
    value = std::string((const char *)(doc_value + offset), value_len);
  }

  if (is_free) {
    delete[] doc_value;
  }

  return 0;
}

int Table::GetFieldRawValue(int docid, int field_id, std::vector<uint8_t> &value,
                            const uint8_t *doc_v) {
  if ((docid < 0) or (field_id < 0 || field_id >= field_num_)) return -1;

  const uint8_t *doc_value = doc_v;
  bool is_free = false;
  if (doc_value == nullptr) {
    is_free = true;
    storage_mgr_->Get(docid, doc_value);
  }

  DataType data_type = attrs_[field_id];
  size_t offset = idx_attr_offset_[field_id];

  if (data_type == DataType::STRING) {
    uint32_t block_id = 0;
    in_block_pos_t in_block_pos = 0;
    str_len_t len = 0;
    ParseStrPosition(doc_value + offset, block_id, in_block_pos, len);
    std::string str;
    storage_mgr_->GetString(docid, str, block_id, in_block_pos, len);
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
    LOG(ERROR) << "Cannot find field [" << field_name << "]";
    return -1;
  }
  type = it->second;
  return 0;
}

int Table::GetAttrType(std::map<std::string, DataType> &attr_type_map) {
  for (const auto attr_type : attr_type_map_) {
    attr_type_map.insert(attr_type);
  }
  return 0;
}

int Table::GetAttrIsIndex(std::map<std::string, bool> &attr_is_index_map) {
  for (const auto attr_is_index : attr_is_index_map_) {
    attr_is_index_map.insert(attr_is_index);
  }
  return 0;
}

int Table::GetAttrIdx(const std::string &field) const {
  const auto &iter = attr_idx_map_.find(field.c_str());
  return (iter != attr_idx_map_.end()) ? iter->second : -1;
}

bool Table::AlterCacheSize(int cache_size, int str_cache_size) {
  return storage_mgr_->AlterCacheSize(cache_size, str_cache_size);
}

void Table::GetCacheSize(int &cache_size, int &str_cache_size) {
  storage_mgr_->GetCacheSize(cache_size, str_cache_size);
}

int Table::GetStorageManagerSize() {
  int table_doc_num = 0;
  if (storage_mgr_) {
    table_doc_num = storage_mgr_->Size();
  }
  LOG(INFO) << "read doc_num=" << table_doc_num << " in table storage_mgr.";
  return table_doc_num;
}

}  // namespace tig_gamma
