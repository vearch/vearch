/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <map>
#include <string>
#include <vector>

#include "c_api/api_data/doc.h"
#include "c_api/api_data/table.h"
#include "io/io_common.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "storage/storage_manager.h"
#include "util/bitmap_manager.h"
#include "util/log.h"

namespace vearch {

class TableIO;

struct TableParams : DumpConfig {
  // currently no configure need to dump
  TableParams(const std::string &name_ = "") : DumpConfig(name_) {}
  int Parse(utils::JsonParser &jp) { return 0; }
};

/** table, support add, update, delete, dump and load.
 */
class Table {
 public:
  explicit Table(const std::string &space_name, StorageManager *storage_mgr,
                 int cf_id);

  ~Table();

  /** create table
   *
   * @param table  table definition
   * @return Status::OK() if successed
   */
  Status CreateTable(TableInfo &table, bitmap::BitmapManager *bitmap_mgr);

  /** add a doc to table
   *
   * @param key     doc's key
   * @param doc     doc to add
   * @param docid   doc index number
   * @return 0 if successed
   */
  int Add(const std::string &key,
          const std::unordered_map<std::string, struct Field> &fields,
          int64_t docid);

  /** update a doc
   *
   * @param doc     doc to update
   * @param docid   doc index number
   * @return 0 if successed
   */
  int Update(const std::unordered_map<std::string, struct Field> &fields,
             int64_t docid);

  std::unordered_map<std::string, bool> CheckFieldIsEqual(
      const std::unordered_map<std::string, Field> &fields, int docid);

  int Delete(std::string &key);

  /** get docid by key
   *
   * @param key key to get
   * @param docid output, the docid to key
   * @return 0 if successed, -1 key not found
   */
  int GetDocidByKey(const std::string &key, int64_t &docid);

  int GetKeyByDocid(int64_t docid, std::string &key);

  /** dump datas to disk
   *
   * @return ResultCode
   */
  // int Dump(const std::string &path, int start_docid, int end_docid);

  long GetMemoryBytes();

  int GetDocInfo(const std::string &key, Doc &doc,
                 const std::vector<std::string> &fields);
  int GetDocInfo(const int64_t docid, Doc &doc,
                 const std::vector<std::string> &fields);

  int GetFieldRawValue(int64_t docid, const std::string &field_name,
                       std::string &value);

  int GetFieldRawValue(int64_t docid, int field_id, std::string &value);

  int GetFieldRawValue(int64_t docid, int field_id,
                       std::vector<uint8_t> &value);

  int GetFieldType(const std::string &field, DataType &type);

  int GetAttrType(std::map<std::string, DataType> &attr_type_map);

  int GetAttrIsIndex(std::map<std::string, bool> &attr_is_index_map);

  int GetAttrIdx(const std::string &field) const;

  const std::string &GetName() { return name_; }

  void SetKeyFieldName(std::string name) { key_field_name_ = name; }

  const std::string &GetKeyFieldName() { return key_field_name_; }

  int Load(int64_t &doc_num);

  int FieldsNum() { return attrs_.size(); }

  const std::map<std::string, int> &FieldMap() { return attr_idx_map_; }

  DumpConfig *GetDumpConfig() { return table_params_; }

  int64_t GetStorageManagerSize();

  int64_t last_docid_;

 private:
  int FTypeSize(DataType fType);

  Status AddField(const std::string &name, DataType ftype, bool is_index);

  std::string name_;   // table name
  int item_length_;    // every doc item length
  uint8_t field_num_;  // field number
  int key_idx_;        // key postion
  std::string key_field_name_;

  std::map<std::string, int> attr_offset_map_;  // <field_id, field_name>

  std::map<int, std::string> idx_attr_map_;        // <field_id, field_name>
  std::map<std::string, int> attr_idx_map_;        // <field_name, field_id>
  std::map<std::string, DataType> attr_type_map_;  // <field_name, field_type>
  std::map<std::string, bool> attr_is_index_map_;  // <field_name, is index>
  std::vector<int> idx_attr_offset_;
  std::vector<DataType> attrs_;

  bool table_created_;

  bitmap::BitmapManager *bitmap_mgr_;
  TableParams *table_params_;
  StorageManager *storage_mgr_;
  int cf_id_;
  int key_cf_id_;
};

}  // namespace vearch
