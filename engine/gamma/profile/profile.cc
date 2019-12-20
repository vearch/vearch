/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "profile.h"

#include <fcntl.h>
#include <sys/mman.h>

#include <fstream>
#include <string>

#include "utils.h"

using std::move;
using std::string;
using std::vector;

namespace tig_gamma {

Profile::Profile(const int max_doc_size) {
  item_length_ = 0;
  field_num_ = 0;
  key_idx_ = -1;
  mem_ = nullptr;
  str_mem_ = nullptr;
  max_profile_size_ = max_doc_size;
  max_str_size_ = max_profile_size_ * 128;
  str_offset_ = 0;

  // TODO : there is a failure.
  // if (!item_to_docid_.reserve(max_doc_size)) {
  //   LOG(ERROR) << "item_to_docid reserve failed, max_doc_size [" <<
  //   max_doc_size
  //              << "]";
  // }

  table_created_ = false;
  LOG(INFO) << "Profile created success!";
}

Profile::~Profile() {
  if (mem_ != nullptr) {
    delete[] mem_;
  }

  if (str_mem_ != nullptr) {
    delete[] str_mem_;
  }
}

int Profile::Load(const std::vector<string> &folders, int &doc_num) {
  uint64_t total_num = 0;
  uint64_t total_str_size = 0;
  char *cur_mem = mem_;
  char *cur_str_mem = str_mem_;

  for (const string &path : folders) {
    const string prf_name = path + "/" + name_ + ".prf";
    const string prf_str_name = path + "/" + name_ + ".str.prf";

    FILE *fp_prf = fopen(prf_name.c_str(), "rb");
    if (fp_prf == nullptr) {
      LOG(INFO) << "Cannot open file " << prf_name;
      return -1;
    }

    long profile_size = utils::get_file_size(prf_name.c_str());
    total_num += profile_size / item_length_;
    if (total_num > max_profile_size_) {
      LOG(ERROR) << "total_num [" << total_num
                 << "] larger than max_profile_size [" << max_profile_size_
                 << "]";
      fclose(fp_prf);
      return -1;
    }

    fread((void *)cur_mem, sizeof(char), profile_size, fp_prf);
    fclose(fp_prf);
    cur_mem += profile_size;

    FILE *fp_str = fopen(prf_str_name.c_str(), "rb");
    if (fp_str == nullptr) {
      LOG(ERROR) << "Cannot open file " << prf_str_name;
      return -1;
    }

    long profile_str_size = utils::get_file_size(prf_str_name.c_str());
    total_str_size += profile_str_size;
    if (total_str_size > max_str_size_) {
      LOG(ERROR) << "total_str_size [" << total_str_size
                 << "] larger than max_str_size [" << max_str_size_ << "]";
      fclose(fp_str);
      return -1;
    }

    fread((void *)cur_str_mem, sizeof(char), profile_str_size, fp_str);
    fclose(fp_str);

    cur_str_mem += profile_str_size;
    doc_num += profile_size / item_length_;

    LOG(INFO) << "Load profile doc_num [" << doc_num << "]";
  }

  const string str_id = "_id";
  const auto &iter = attr_idx_map_.find(str_id);
  if (iter == attr_idx_map_.end()) {
    LOG(ERROR) << "cannot find field [" << str_id << "]";
    return -1;
  }

  int idx = iter->second;

#pragma omp parallel for
  for (int i = 0; i < doc_num; ++i) {
    char *value = nullptr;
    int len = GetFieldString(i, idx, &value);
    string key = string(value, len);
    item_to_docid_.insert(key, i);
  }

  LOG(INFO) << "Profile load successed!";
  return 0;
}

int Profile::CreateTable(const Table *table) {
  if (table_created_) {
    return -10;
  }
  name_ = std::string(table->name->value, table->name->len);
  for (int i = 0; i < table->fields_num; ++i) {
    const string name =
        string(table->fields[i]->name->value, table->fields[i]->name->len);
    enum DataType ftype = table->fields[i]->data_type;
    int is_index = table->fields[i]->is_index;
    LOG(INFO) << "Add field name [" << name << "], type [" << ftype
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

  if (mem_) {
    delete[] mem_;
  }
  if (str_mem_) {
    delete[] str_mem_;
  }

  mem_ = new char[max_profile_size_ * item_length_];
  str_mem_ = new char[max_str_size_];

  table_created_ = true;
  LOG(INFO) << "Create table " << name_ << " success!";
  return 0;
}

int Profile::FTypeSize(enum DataType fType) {
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
    length = sizeof(uint64_t) + sizeof(uint16_t);
  }
  return length;
}

void Profile::SetFieldValue(int docid, const std::string &field,
                            const char *value, uint16_t len) {
  const auto &iter = attr_idx_map_.find(field);
  if (iter == attr_idx_map_.end()) {
    LOG(ERROR) << "Cannot find field [" << field << "]";
    return;
  }
  int idx = iter->second;
  size_t offset = (uint64_t)docid * item_length_ + idx_attr_offset_[idx];
  enum DataType attr = attrs_[idx];

  if (attr != DataType::STRING) {
    int type_size = FTypeSize(attr);
    memcpy(mem_ + offset, value, type_size);
  } else {
    int ofst = 0;
    ofst += sizeof(uint64_t);
    if ((str_offset_ + len) >= max_str_size_) {
      LOG(ERROR) << "Str memory reached max size [" << max_str_size_ << "]";
      return;
    }
    memcpy(mem_ + offset, &str_offset_, sizeof(uint64_t));
    memcpy(mem_ + offset + ofst, &len, sizeof(uint16_t));
    memcpy(str_mem_ + str_offset_, value, sizeof(char) * len);
    str_offset_ += len;
  }
}

int Profile::AddField(const string &name, enum DataType ftype, int is_index) {
  if (attr_idx_map_.find(name) != attr_idx_map_.end()) {
    LOG(ERROR) << "Duplicate field " << name;
    return -1;
  }
  if (name == "_id") {
    key_idx_ = field_num_;
  }
  idx_attr_offset_.push_back(item_length_);
  item_length_ += FTypeSize(ftype);
  attrs_.push_back(ftype);
  idx_attr_map_.insert(std::pair<int, string>(field_num_, name));
  attr_idx_map_.insert(std::pair<string, int>(name, field_num_));
  attr_type_map_.insert(std::pair<string, enum DataType>(name, ftype));
  attr_is_index_map_.insert(std::pair<string, int>(name, is_index));
  ++field_num_;
  return 0;
}

int Profile::GetDocIDByKey(const std::string &key, int &doc_id) {
  if (item_to_docid_.find(key, doc_id)) {
    return 0;
  }

  return -1;
}

int Profile::Add(const std::vector<Field *> &fields, int doc_id,
                 bool is_existed) {
  if (doc_id >= static_cast<int>(max_profile_size_)) {
    LOG(ERROR) << "Doc num reached upper limit [" << max_profile_size_ << "]";
    return -1;
  }
  string key;
  for (size_t i = 0; i < fields.size(); ++i) {
    const auto field_value = fields[i];
    const string &name =
        std::string(field_value->name->value, field_value->name->len);
    if (name == "_id") {
      key = string(field_value->value->value, field_value->value->len);
      break;
    }
  }
#ifdef DEBUG__
  printDoc(doc);
#endif
  if (key.empty()) {
    LOG(ERROR) << "Add item error : _id is null!";
    return -1;
  }

  if (is_existed) {
    item_to_docid_.erase(key);
  }

  item_to_docid_.insert(key, doc_id);

  for (size_t i = 0; i < fields.size(); ++i) {
    const auto field_value = fields[i];
    const string &name =
        std::string(field_value->name->value, field_value->name->len);

    auto it = attr_idx_map_.find(name);
    if (it == attr_idx_map_.end()) {
      LOG(ERROR) << "Cannot find field name [" << name << "]";
      continue;
    }
    SetFieldValue(doc_id, name.c_str(), field_value->value->value,
                  field_value->value->len);
  }

  if (doc_id % 10000 == 0) {
    LOG(INFO) << "Add item _id [" << key << "], num [" << doc_id << "]"
              << ", is_existed=" << is_existed;
  }
  return 0;
}

int Profile::Update(const std::vector<Field *> &fields, int doc_id) {
  for (size_t i = 0; i < fields.size(); ++i) {
    const auto field_value = fields[i];
    const string &name =
        string(field_value->name->value, field_value->name->len);
    auto it = attr_idx_map_.find(name);
    if (it == attr_idx_map_.end()) {
      LOG(ERROR) << "Cannot find field name [" << name << "]";
      continue;
    }
    // TODO several operations should be atomic
    // TODO update string
    if (field_value->data_type == STRING) {
      continue;
    }
    SetFieldValue(doc_id, name.c_str(), field_value->value->value,
                  field_value->value->len);
  }

  return 0;
}

int Profile::Dump(const string &path, int start_docid, int end_docid) {
  const string prf_name = path + "/" + name_ + ".prf";
  FILE *fp_output = fopen(prf_name.c_str(), "wb");
  if (fp_output == nullptr) {
    LOG(ERROR) << "Cannot write file [" << prf_name << "]";
    return -1;
  }

  LOG(INFO) << " item_length = " << item_length_ << " start [" << start_docid
            << "] num [" << end_docid - start_docid + 1 << "]";

  fwrite((void *)(mem_ + (uint64_t)start_docid * item_length_), sizeof(char),
         (uint64_t)(end_docid - start_docid + 1) * item_length_, fp_output);

  fclose(fp_output);

  int first_str_idx = -1;
  int last_str_idx = -1;
  string first_str_field, last_str_field;
  for (size_t i = 0; i < attrs_.size(); ++i) {
    if (attrs_[i] == DataType::STRING) {
      if (first_str_idx < 0) {
        first_str_idx = i;
      }
      last_str_idx = i;
    }
  }

  if (first_str_idx < 0) {
    LOG(INFO) << "Cannot find string field!";
    return 0;
  }

  const string prf_str_name = path + "/" + name_ + ".str.prf";
  FILE *fp_str = fopen(prf_str_name.c_str(), "wb");
  if (fp_str == nullptr) {
    LOG(ERROR) << "Cannot write file " << prf_str_name;
    fclose(fp_output);
    return -1;
  }

  // get str start location
  size_t offset =
      (uint64_t)start_docid * item_length_ + idx_attr_offset_[first_str_idx];
  size_t str_dumped_offset = 0;
  memcpy(&str_dumped_offset, mem_ + offset, sizeof(size_t));

  // get str end location
  offset = (uint64_t)end_docid * item_length_ + idx_attr_offset_[last_str_idx];
  size_t end_offset = 0;
  memcpy(&end_offset, mem_ + offset, sizeof(size_t));
  unsigned short len;
  memcpy(&len, mem_ + offset + sizeof(size_t), sizeof(unsigned short));
  uint64_t str_offset = end_offset + len;

  fwrite((void *)(str_mem_ + str_dumped_offset), sizeof(char),
         str_offset - str_dumped_offset, fp_str);

  fclose(fp_str);

  return 0;
}

long Profile::GetMemoryBytes() {
  return max_profile_size_ * item_length_ + max_str_size_;
}

int Profile::GetDocInfo(const int docid, Doc *&doc) {
  if (doc == nullptr) {
    doc = static_cast<Doc *>(malloc(sizeof(Doc)));
    doc->fields_num = attr_type_map_.size();
    doc->fields =
        static_cast<Field **>(malloc(doc->fields_num * sizeof(Field *)));
    memset(doc->fields, 0, doc->fields_num * sizeof(Field *));
  }

  int i = 0;
  for (const auto &it : attr_type_map_) {
    const string &attr = it.first;
    doc->fields[i] = GetFieldInfo(docid, attr);
    ++i;
  }

  return 0;
}

Field *Profile::GetFieldInfo(const int docid, const string &field_name) {
  const auto &it = attr_type_map_.find(field_name);
  if (it == attr_type_map_.end()) {
    LOG(ERROR) << "Cannot find field [" << field_name << "]";
    return nullptr;
  }

  enum DataType type = it->second;
  Field *field = static_cast<Field *>(malloc(sizeof(Field)));
  memset(field, 0, sizeof(Field));
  field->name = StringToByteArray(field_name);
  field->value = static_cast<ByteArray *>(malloc(sizeof(ByteArray)));

  if (type != DataType::STRING) {
    field->value->len = FTypeSize(type);
    field->value->value = static_cast<char *>(malloc(field->value->len));
  }

  if (type == DataType::INT) {
    int value = 0;
    GetField<int>(docid, field_name, value);
    memcpy(field->value->value, &value, field->value->len);
  } else if (type == DataType::LONG) {
    long value = 0;
    GetField<long>(docid, field_name, value);
    memcpy(field->value->value, &value, field->value->len);
  } else if (type == DataType::FLOAT) {
    float value = 0;
    GetField<float>(docid, field_name, value);
    memcpy(field->value->value, &value, field->value->len);
  } else if (type == DataType::DOUBLE) {
    double value = 0;
    GetField<double>(docid, field_name, value);
    memcpy(field->value->value, &value, field->value->len);
  } else if (type == DataType::STRING) {
    char *value;
    field->value->len = GetFieldString(docid, field_name, &value);
    field->value->value = static_cast<char *>(malloc(field->value->len));
    memcpy(field->value->value, value, field->value->len);
  }
  field->data_type = type;
  return field;
}

int Profile::GetDocInfo(const std::string &key, Doc *&doc) {
  int doc_id = 0;
  int ret = GetDocIDByKey(key, doc_id);
  if (ret < 0) {
    return ret;
  }
  return GetDocInfo(doc_id, doc);
}

int Profile::GetFieldString(int docid, const std::string &field,
                            char **value) const {
  const auto &iter = attr_idx_map_.find(field);
  if (iter == attr_idx_map_.end()) {
    LOG(ERROR) << "docid " << docid << " field " << field;
    return -1;
  }
  int idx = iter->second;
  return GetFieldString(docid, idx, value);
}

int Profile::GetFieldString(int docid, int field_id, char **value) const {
  size_t offset = (uint64_t)docid * item_length_ + idx_attr_offset_[field_id];
  size_t str_offset = 0;
  memcpy(&str_offset, mem_ + offset, sizeof(size_t));
  unsigned short len;
  memcpy(&len, mem_ + offset + sizeof(size_t), sizeof(unsigned short));
  offset += sizeof(unsigned short);
  *value = str_mem_ + str_offset;
  return len;
}

int Profile::GetFieldRawValue(int docid, int field_id, unsigned char **value,
                              int &data_len) {
  if ((docid < 0) or (field_id < 0 || field_id >= field_num_)) return -1;

  enum DataType data_type = attrs_[field_id];
  if (data_type != DataType::STRING) {
    size_t offset = (uint64_t)docid * item_length_ + idx_attr_offset_[field_id];
    data_len = FTypeSize(data_type);
    *value = reinterpret_cast<unsigned char *>(mem_ + offset);
  } else {
    data_len =
        GetFieldString(docid, field_id, reinterpret_cast<char **>(value));
  }
  return 0;
}

int Profile::GetAttrType(std::map<std::string, enum DataType> &attr_type_map) {
  for (const auto attr_type : attr_type_map_) {
    attr_type_map.insert(attr_type);
  }
  return 0;
}

int Profile::GetAttrIsIndex(std::map<std::string, int> &attr_is_index_map) {
  for (const auto attr_is_index : attr_is_index_map_) {
    attr_is_index_map.insert(attr_is_index);
  }
  return 0;
}

int Profile::GetAttrIdx(const std::string &field) const {
  const auto &iter = attr_idx_map_.find(field.c_str());
  return (iter != attr_idx_map_.end()) ? iter->second : -1;
}

}  // namespace tig_gamma
