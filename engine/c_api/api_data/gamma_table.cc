/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "gamma_table.h"

#include "util/utils.h"

namespace tig_gamma {

int TableInfo::Serialize(char **out, int *out_len) {
  flatbuffers::FlatBufferBuilder builder;

  std::vector<flatbuffers::Offset<gamma_api::FieldInfo>> field_info_vector;
  for (const struct FieldInfo &f : fields_) {
    auto field = gamma_api::CreateFieldInfo(
        builder, builder.CreateString(f.name),
        static_cast<::DataType>(f.data_type), f.is_index);
    field_info_vector.push_back(field);
  }

  std::vector<flatbuffers::Offset<gamma_api::VectorInfo>> vector_info_vector;
  for (const struct VectorInfo &v : vectors_infos_) {
    auto vectorinfo = gamma_api::CreateVectorInfo(
        builder, builder.CreateString(v.name),
        static_cast<::DataType>(v.data_type), v.is_index, v.dimension,
        builder.CreateString(v.model_id), builder.CreateString(v.store_type),
        builder.CreateString(v.store_param), v.has_source);
    vector_info_vector.push_back(vectorinfo);
  }

  auto table = gamma_api::CreateTable(builder, builder.CreateString(name_),
                                      builder.CreateVector(field_info_vector),
                                      builder.CreateVector(vector_info_vector),
                                      indexing_size_, compress_mode_,
                                      builder.CreateString(retrieval_type_),
                                      builder.CreateString(retrieval_param_),
				      builder.CreateVectorOfStrings(retrieval_types_),
				      builder.CreateVectorOfStrings(retrieval_params_));
  builder.Finish(table);
  *out_len = builder.GetSize();
  *out = (char *)malloc(*out_len * sizeof(char));
  memcpy(*out, (char *)builder.GetBufferPointer(), *out_len);

  return 0;
}

void TableInfo::Deserialize(const char *data, int len) {
  table_ = const_cast<gamma_api::Table *>(gamma_api::GetTable(data));

  name_ = table_->name()->str();

  for (size_t i = 0; i < table_->fields()->size(); ++i) {
    auto f = table_->fields()->Get(i);
    struct FieldInfo field_info;
    field_info.name = f->name()->str();
    field_info.data_type = static_cast<DataType>(f->data_type());
    field_info.is_index = f->is_index();

    fields_.emplace_back(field_info);
  }

  for (size_t i = 0; i < table_->vectors_info()->size(); ++i) {
    auto v = table_->vectors_info()->Get(i);

    struct VectorInfo vector_info;
    vector_info.name = v->name()->str();
    vector_info.data_type = static_cast<DataType>(v->data_type());
    vector_info.is_index = v->is_index();
    vector_info.dimension = v->dimension();
    vector_info.model_id = v->model_id()->str();
    vector_info.store_type = v->store_type()->str();
    vector_info.store_param = v->store_param()->str();
    vector_info.has_source = v->has_source();

    vectors_infos_.emplace_back(vector_info);
  }

  indexing_size_ = table_->indexing_size();
  retrieval_type_ = table_->retrieval_type()->str();
  retrieval_param_ = table_->retrieval_param()->str();
  compress_mode_ = table_->compress_mode();

  retrieval_types_.resize(table_->retrieval_types()->size());
  for (size_t i = 0; i < table_->retrieval_types()->size(); ++i) {
    retrieval_types_[i] = table_->retrieval_types()->Get(i)->str();
  }

  retrieval_params_.resize(table_->retrieval_params()->size());
  for (size_t i = 0; i < table_->retrieval_params()->size(); ++i) {
    retrieval_params_[i] = table_->retrieval_params()->Get(i)->str();
  }
}

std::string &TableInfo::Name() { return name_; }

void TableInfo::SetName(std::string &name) { name_ = name; }

std::vector<struct FieldInfo> &TableInfo::Fields() { return fields_; }

std::vector<struct VectorInfo> &TableInfo::VectorInfos() {
  return vectors_infos_;
}

void TableInfo::AddVectorInfo(struct VectorInfo &vector_info) {
  vectors_infos_.emplace_back(vector_info);
}

void TableInfo::AddField(struct FieldInfo &field) {
  fields_.emplace_back(field);
}

int TableInfo::IndexingSize() { return indexing_size_; }

void TableInfo::SetIndexingSize(int indexing_size) {
  indexing_size_ = indexing_size;
}

std::string &TableInfo::RetrievalType() { return retrieval_type_; }

void TableInfo::SetRetrievalType(std::string &retrieval_type) {
  retrieval_type_ = retrieval_type;
}

std::string &TableInfo::RetrievalParam() { return retrieval_param_; }

void TableInfo::SetRetrievalParam(std::string &retrieval_param) {
  retrieval_param_ = retrieval_param;
}

int TableInfo::Read(const std::string &path) {
  assert(table_ == nullptr);
  long file_size = utils::get_file_size(path);
  FILE *fp = fopen(path.c_str(), "rb");

  char data[file_size];
  fread(data, file_size, 1, fp);
  fclose(fp);

  Deserialize(data, file_size);
  return 0;
}

int TableInfo::Write(const std::string &path) {
  char *data;
  int len;
  Serialize(&data, &len);

  FILE *fp = fopen(path.c_str(), "wb");

  fwrite(data, len, 1, fp);
  fclose(fp);
  free(data);
  return 0;
}
}  // namespace tig_gamma
