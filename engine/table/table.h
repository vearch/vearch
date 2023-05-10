/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <libcuckoo/cuckoohash_map.hh>
#include <map>
#include <string>
#include <vector>

#include "c_api/api_data/gamma_batch_result.h"
#include "c_api/api_data/gamma_doc.h"
#include "c_api/api_data/gamma_table.h"
#include "util/bitmap_manager.h"
#include "io/io_common.h"
#include "storage/storage_manager.h"
#include "table_define.h"
#include "util/log.h"



namespace tig_gamma {


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
  explicit Table(const std::string &root_path, bool b_compress = false);

  ~Table();

  /** create table
   *
   * @param table  table definition
   * @param table_params unused
   * @return 0 if successed
   */
  int CreateTable(TableInfo &table, TableParams &table_params,
                  bitmap::BitmapManager *bitmap_mgr);

  /** add a doc to table
   *
   * @param key     doc's key
   * @param doc     doc to add
   * @param docid   doc index number
   * @return 0 if successed
   */
  int Add(const std::string &key, const std::vector<struct Field> &fields,
          int docid);

  int BatchAdd(int start_id, int batch_size, int docid,
               std::vector<Doc> &doc_vec, BatchResult &result);

  /** update a doc
   *
   * @param doc     doc to update
   * @param docid   doc index number
   * @return 0 if successed
   */
  int Update(const std::vector<struct Field> &fields, int docid);

  std::vector<bool> CheckFieldIsEqual(const std::vector<Field> &fields,
                                      int docid);

  int Delete(std::string &key);

  /** get docid by key
   *
   * @param key key to get
   * @param docid output, the docid to key
   * @return 0 if successed, -1 key not found
   */
  int GetDocIDByKey(const std::string &key, int &docid);

  int GetKeyByDocid(int docid, std::string &key);

  /** dump datas to disk
   *
   * @return ResultCode
   */
  // int Dump(const std::string &path, int start_docid, int end_docid);

  long GetMemoryBytes();

  const uint8_t *GetDocBuffer(int docid);

  int GetDocInfo(const std::string &key, Doc &doc,
                 const std::vector<std::string> &fields);
  int GetDocInfo(const int docid, Doc &doc,
                 const std::vector<std::string> &fields);

  int GetFieldRawValue(int docid, const std::string &field_name,
                       std::string &value, const uint8_t *doc_v = nullptr);

  int GetFieldRawValue(int docid, int field_id, std::string &value,
                       const uint8_t *doc_v = nullptr);
  
  int GetFieldRawValue(int docid, int field_id, std::vector<uint8_t> &value,
                       const uint8_t *doc_v = nullptr);

  int GetFieldType(const std::string &field, DataType &type);

  int GetAttrType(std::map<std::string, DataType> &attr_type_map);

  int GetAttrIsIndex(std::map<std::string, bool> &attr_is_index_map);

  int GetAttrIdx(const std::string &field) const;

  const std::string &GetName() { return name_; }

  void SetKeyFieldName(std::string name) { key_field_name_ = name; }

  const std::string &GetKeyFieldName() { return key_field_name_; }

  uint8_t StringFieldNum() const { return string_field_num_; }

  int Load(int &doc_num);

  int Sync();

  int FieldsNum() { return attrs_.size(); }

  const std::map<std::string, int> &FieldMap() { return attr_idx_map_; }

  DumpConfig *GetDumpConfig() { return table_params_; }

  uint8_t IdType() { return id_type_; }

  bool IsCompress() { return b_compress_; }

  bool AlterCacheSize(int cache_size, int str_cache_size);

  void GetCacheSize(int &cache_size, int &str_cache_size);

  int GetStorageManagerSize();

  std::string root_path_;
  int last_docid_;

 private:
  int FTypeSize(DataType fType);

  int AddField(const std::string &name, DataType ftype, bool is_index);

  int ParseStrPosition(const uint8_t *buf, uint32_t &block_id,
                       in_block_pos_t &in_block_pos, str_len_t &len);

  int SetStrPosition(uint8_t *buf, uint32_t block_id,
                     in_block_pos_t in_block_pos, str_len_t len);

  void CheckStrLen(const std::string &field_name, str_len_t &len);

  std::string name_;   // table name
  int item_length_;    // every doc item length
  uint8_t field_num_;  // field number
  uint8_t string_field_num_;
  int key_idx_;  // key postion
  std::string key_field_name_;

  std::map<std::string, int> attr_offset_map_;     // <field_id, field_name>
  
  std::map<int, std::string> idx_attr_map_;        // <field_id, field_name>
  std::map<std::string, int> attr_idx_map_;        // <field_name, field_id>
  std::map<std::string, DataType> attr_type_map_;  // <field_name, field_type>
  std::map<std::string, bool> attr_is_index_map_;  // <field_name, is index>
  std::vector<int> idx_attr_offset_;
  std::vector<DataType> attrs_;
  std::map<int, int> str_field_id_;  // <field_id, str_field_id>

  uint8_t id_type_;  // 0 string, 1 long, default 1
  bool b_compress_;
  cuckoohash_map<long, int> item_to_docid_;

  int seg_num_;  // cur segment num

  bool table_created_;

  bitmap::BitmapManager *bitmap_mgr_;
  TableParams *table_params_;
  StorageManager *storage_mgr_;
};

}  // namespace tig_gamma
