/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "gamma_api.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <chrono>
#include <iostream>
#include <sstream>
#include "gamma_engine.h"
#include "log.h"
#include "utils.h"

INITIALIZE_EASYLOGGINGPP

using std::string;

ByteArray **MakeByteArrays(int num) {
  ByteArray **byte_arrays =
      static_cast<ByteArray **>(malloc(sizeof(ByteArray *) * num));
  memset(byte_arrays, 0, sizeof(ByteArray *) * num);
  return byte_arrays;
}

ByteArray *MakeByteArray(const char *value, int len) {
  ByteArray *byteArray = static_cast<ByteArray *>(malloc(sizeof(ByteArray)));
  memset(byteArray, 0, sizeof(ByteArray));
  byteArray->value = static_cast<char *>(malloc(sizeof(char) * len));
  memcpy(byteArray->value, value, len);
  byteArray->len = len;
  return byteArray;
}

enum ResponseCode SetByteArray(ByteArray **byte_arrays, int idx,
                               ByteArray *byte_array) {
  byte_arrays[idx] = byte_array;
  return ResponseCode::SUCCESSED;
}

enum ResponseCode DestroyByteArray(ByteArray *byteArray) {
  if (byteArray != nullptr) {
    if (byteArray->value != nullptr) {
      free(byteArray->value);
    }
    free(byteArray);
  }
  return ResponseCode::SUCCESSED;
}

enum ResponseCode DestroyByteArrays(ByteArray **byte_arrays, int num) {
  if (byte_arrays != nullptr) {
    for (int i = 0; i < num; ++i) {
      DestroyByteArray(byte_arrays[i]);
    }
    free(byte_arrays);
  }
  return ResponseCode::SUCCESSED;
}

Config *MakeConfig(ByteArray *path, int max_doc_size) {
  Config *config = static_cast<Config *>(malloc(sizeof(Config)));
  memset(config, 0, sizeof(Config));
  config->path = path;
  config->max_doc_size = max_doc_size;
  return config;
}

enum ResponseCode DestroyConfig(Config *config) {
  if (config != nullptr) {
    DestroyByteArray(config->path);
    free(config);
  }
  return ResponseCode::SUCCESSED;
}

VectorInfo **MakeVectorInfos(int num) {
  VectorInfo **vectorInfos =
      static_cast<VectorInfo **>(malloc(sizeof(VectorInfo *) * num));
  memset(vectorInfos, 0, sizeof(VectorInfo *) * num);
  return vectorInfos;
}

VectorInfo *MakeVectorInfo(ByteArray *name, enum DataType data_type,
                           BOOL is_index, int dimension, ByteArray *model_id,
                           ByteArray *retrieval_type, ByteArray *store_type,
                           ByteArray *store_param) {
  VectorInfo *vectorInfo =
      static_cast<VectorInfo *>(malloc(sizeof(VectorInfo)));
  memset(vectorInfo, 0, sizeof(VectorInfo));
  vectorInfo->name = name;
  vectorInfo->data_type = data_type;
  vectorInfo->is_index = is_index;
  vectorInfo->dimension = dimension;
  vectorInfo->model_id = model_id;
  vectorInfo->retrieval_type = retrieval_type;
  vectorInfo->store_type = store_type;
  vectorInfo->store_param = store_param;
  return vectorInfo;
}

enum ResponseCode SetVectorInfo(VectorInfo **vector_infos, int idx,
                                VectorInfo *vector_info) {
  vector_infos[idx] = vector_info;
  return ResponseCode::SUCCESSED;
}

enum ResponseCode DestroyVectorInfo(VectorInfo *vector_info) {
  if (vector_info != nullptr) {
    DestroyByteArray(vector_info->name);
    DestroyByteArray(vector_info->model_id);
    DestroyByteArray(vector_info->retrieval_type);
    DestroyByteArray(vector_info->store_type);
    free(vector_info);
  }
  return ResponseCode::SUCCESSED;
}

enum ResponseCode DestroyVectorInfos(VectorInfo **vector_infos, int num) {
  if (vector_infos != nullptr) {
    for (int i = 0; i < num; ++i) {
      DestroyVectorInfo(vector_infos[i]);
    }
    free(vector_infos);
  }
  return ResponseCode::SUCCESSED;
}

FieldInfo **MakeFieldInfos(int num) {
  FieldInfo **field_infos =
      static_cast<FieldInfo **>(malloc(sizeof(FieldInfo *) * num));
  memset(field_infos, 0, sizeof(FieldInfo *) * num);
  return field_infos;
}

FieldInfo *MakeFieldInfo(ByteArray *name, enum DataType data_type,
                         BOOL is_index) {
  FieldInfo *field_info = static_cast<FieldInfo *>(malloc(sizeof(FieldInfo)));
  field_info->name = name;
  field_info->data_type = data_type;
  field_info->is_index = is_index;
  return field_info;
}

enum ResponseCode SetFieldInfo(FieldInfo **field_infos, int idx,
                               FieldInfo *field_info) {
  field_infos[idx] = field_info;
  return ResponseCode::SUCCESSED;
}

enum ResponseCode DestroyFieldInfos(FieldInfo **fieldInfo, int num) {
  for (int i = 0; i < num; ++i) {
    free(fieldInfo[i]);
  }
  free(fieldInfo);
  return ResponseCode::SUCCESSED;
}

Field **MakeFields(int num) {
  Field **fields = static_cast<Field **>(malloc(sizeof(Field *) * num));
  memset(fields, 0, sizeof(Field *) * num);
  return fields;
}

Field *MakeField(ByteArray *name, ByteArray *value, ByteArray *source,
                 enum DataType data_type) {
  Field *field_info = static_cast<Field *>(malloc(sizeof(Field)));
  memset(field_info, 0, sizeof(Field));
  field_info->name = name;
  field_info->data_type = data_type;
  field_info->value = value;
  field_info->source = source;
  return field_info;
}

enum ResponseCode SetField(Field **fields, int idx, Field *field) {
  fields[idx] = field;
  return ResponseCode::SUCCESSED;
}

enum ResponseCode DestroyField(Field *field) {
  if (field != nullptr) {
    DestroyByteArray(field->name);
    DestroyByteArray(field->value);
    DestroyByteArray(field->source);
    free(field);
  }
  return ResponseCode::SUCCESSED;
}

enum ResponseCode DestroyFields(Field **field, int num) {
  if (field != nullptr) {
    for (int i = 0; i < num; ++i) {
      DestroyField(field[i]);
    }
    free(field);
  }
  return ResponseCode::SUCCESSED;
}

IVFPQParameters *MakeIVFPQParameters(int metric_type, int nprobe,
                                     int ncentroids, int nsubvector,
                                     int nbits_per_idx) {
  IVFPQParameters *param =
      static_cast<IVFPQParameters *>(malloc(sizeof(IVFPQParameters)));
  memset(param, 0, sizeof(IVFPQParameters));
  param->metric_type = metric_type;
  param->nprobe = nprobe;
  param->ncentroids = ncentroids;
  param->nsubvector = nsubvector;
  param->nbits_per_idx = nbits_per_idx;
  return param;
}

enum ResponseCode DestroyIVFPQParameters(IVFPQParameters *param) {
  if (param != nullptr) {
    free(param);
  }
  return ResponseCode::SUCCESSED;
}

Table *MakeTable(ByteArray *name, FieldInfo **fields, int fields_num,
                 VectorInfo **vectors_info, int vectors_num,
                 IVFPQParameters *ivfpq_param) {
  Table *table = static_cast<Table *>(malloc(sizeof(Table)));
  memset(table, 0, sizeof(Table));
  table->name = name;
  table->fields = fields;
  table->fields_num = fields_num;
  table->vectors_info = vectors_info;
  table->vectors_num = vectors_num;
  table->ivfpq_param = ivfpq_param;

  return table;
}

enum ResponseCode DestroyTable(Table *table) {
  if (table != nullptr) {
    DestroyByteArray(table->name);
    DestroyFieldInfos(table->fields, table->fields_num);
    DestroyVectorInfos(table->vectors_info, table->vectors_num);
    DestroyIVFPQParameters(table->ivfpq_param);
    free(table);
  }
  return ResponseCode::SUCCESSED;
}

Doc *MakeDoc(Field **fields, int fields_num) {
  Doc *doc = static_cast<Doc *>(malloc(sizeof(Doc)));
  memset(doc, 0, sizeof(Doc));
  doc->fields = fields;
  doc->fields_num = fields_num;
  return doc;
}

enum ResponseCode DestroyDoc(Doc *doc) {
  if (doc != nullptr) {
    DestroyFields(doc->fields, doc->fields_num);
    free(doc);
  }
  return ResponseCode::SUCCESSED;
}

enum ResponseCode SetLogDictionary(ByteArray *log_dir) {
  string dir = string(log_dir->value, log_dir->len);
  if (!utils::isFolderExist(dir.c_str())) {
    mkdir(dir.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  }
  // FLAGS_log_dir = dir.c_str();
  // FLAGS_max_log_size = 100;
  // FLAGS_logbuflevel = -1;
  // FLAGS_logbufsecs = 0;
  // FLAGS_stop_logging_if_full_disk = true;
  // google::InitGoogleLogging("gamma");
  // google::SetStderrLogging(google::INFO);
  // google::InstallFailureSignalHandler();

  el::Configurations defaultConf;
  // defaultConf.setToDefault();
  // To set GLOBAL configurations you may use
  el::Loggers::addFlag(el::LoggingFlag::StrictLogFileSizeCheck);
  defaultConf.setGlobally(el::ConfigurationType::Format,
                          "%level %datetime %fbase:%line %msg");
  defaultConf.setGlobally(el::ConfigurationType::ToFile, "true");
  defaultConf.setGlobally(el::ConfigurationType::MaxLogFileSize,
                          "209715200");  // 200MB
  defaultConf.setGlobally(el::ConfigurationType::Filename, dir + "/gamma.log");
  el::Loggers::reconfigureLogger("default", defaultConf);
  el::Helpers::installPreRollOutCallback(
      [](const char *filename, std::size_t size) {
        // SHOULD NOT LOG ANYTHING HERE BECAUSE LOG FILE IS CLOSED!
        std::cout << "************** Rolling out [" << filename
                  << "] because it reached [" << size << " bytes]" << std::endl;
        std::time_t t = std::time(nullptr);
        char mbstr[100];
        if (std::strftime(mbstr, sizeof(mbstr), "%F-%T", std::localtime(&t))) {
          std::cout << mbstr << '\n';
        }
        std::stringstream ss;
        ss << "mv " << filename << " " << filename << "-" << mbstr;
        system(ss.str().c_str());
      });
  return ResponseCode::SUCCESSED;
}

void *Init(Config *config) {
  string path = string(config->path->value, config->path->len);
  tig_gamma::GammaEngine *engine =
      tig_gamma::GammaEngine::GetInstance(path, config->max_doc_size);
  if (engine == nullptr) {
    LOG(ERROR) << "Engine init faild!";
    return nullptr;
  }
  LOG(INFO) << "Engine init successed!";
  return static_cast<void *>(engine);
}

enum ResponseCode Close(void *engine) {
  LOG(INFO) << "Close";
  delete static_cast<tig_gamma::GammaEngine *>(engine);
  return ResponseCode::SUCCESSED;
}

enum ResponseCode CreateTable(void *engine, Table *table) {
  enum ResponseCode ret = static_cast<enum ResponseCode>(
      static_cast<tig_gamma::GammaEngine *>(engine)->CreateTable(table));
  return ret;
}

enum ResponseCode AddDoc(void *engine, Doc *doc) {
  enum ResponseCode ret = static_cast<enum ResponseCode>(
      static_cast<tig_gamma::GammaEngine *>(engine)->Add(doc));
  return ret;
}

enum ResponseCode AddOrUpdateDoc(void *engine, Doc *doc) {
  enum ResponseCode ret = static_cast<enum ResponseCode>(
      static_cast<tig_gamma::GammaEngine *>(engine)->AddOrUpdate(doc));
  return ret;
}

enum ResponseCode UpdateDoc(void *engine, Doc *doc) {
  enum ResponseCode ret = static_cast<enum ResponseCode>(
      static_cast<tig_gamma::GammaEngine *>(engine)->Update(doc));
  return ret;
}

enum ResponseCode DelDoc(void *engine, ByteArray *doc_id) {
  string doc_id_str = string(doc_id->value, doc_id->len);
  enum ResponseCode ret = static_cast<enum ResponseCode>(
      static_cast<tig_gamma::GammaEngine *>(engine)->Del(doc_id_str));
  return ret;
}

enum ResponseCode DelDocByQuery(void *engine, Request *request) {
  return static_cast<enum ResponseCode>(
      static_cast<tig_gamma::GammaEngine *>(engine)->DelDocByQuery(request));
}

int GetDocsNum(void *engine) {
  return static_cast<tig_gamma::GammaEngine *>(engine)->GetDocsNum();
}

long GetMemoryBytes(void *engine) {
  return static_cast<tig_gamma::GammaEngine *>(engine)->GetMemoryBytes();
}

Doc *GetDocByID(void *engine, ByteArray *doc_id) {
  string doc_id_str = string(doc_id->value, doc_id->len);

  Doc *doc = static_cast<tig_gamma::GammaEngine *>(engine)->GetDoc(doc_id_str);
  return doc;
}

enum ResponseCode BuildIndex(void *engine) {
  enum ResponseCode ret = static_cast<enum ResponseCode>(
      static_cast<tig_gamma::GammaEngine *>(engine)->BuildIndex());
  return ret;
}

enum IndexStatus GetIndexStatus(void *engine) {
  enum IndexStatus status = static_cast<enum IndexStatus>(
      static_cast<tig_gamma::GammaEngine *>(engine)->GetIndexStatus());
  return status;
}

enum ResponseCode Dump(void *engine) {
  enum ResponseCode ret = static_cast<enum ResponseCode>(
      static_cast<tig_gamma::GammaEngine *>(engine)->Dump());
  return ret;
}

enum ResponseCode Load(void *engine) {
  enum ResponseCode ret = static_cast<enum ResponseCode>(
      static_cast<tig_gamma::GammaEngine *>(engine)->Load());
  return ret;
}

RangeFilter **MakeRangeFilters(int num) {
  RangeFilter **range_filters =
      static_cast<RangeFilter **>(malloc(sizeof(RangeFilter *) * num));
  memset(range_filters, 0, sizeof(RangeFilter *) * num);
  return range_filters;
}

// warning: for lower_value and upper_value, it need convert long/int to
// char*/string first, then convert to ByteArray
RangeFilter *MakeRangeFilter(ByteArray *field, ByteArray *lower_value,
                             ByteArray *upper_value, BOOL include_lower,
                             BOOL include_upper) {
  RangeFilter *range_filter =
      static_cast<RangeFilter *>(malloc(sizeof(RangeFilter)));
  memset(range_filter, 0, sizeof(RangeFilter));
  range_filter->field = field;
  range_filter->lower_value = lower_value;
  range_filter->upper_value = upper_value;
  range_filter->include_lower = include_lower;
  range_filter->include_upper = include_upper;
  return range_filter;
}

enum ResponseCode SetRangeFilter(RangeFilter **range_filters, int idx,
                                 RangeFilter *range_filter) {
  range_filters[idx] = range_filter;
  return ResponseCode::SUCCESSED;
}

enum ResponseCode DestroyRangeFilter(RangeFilter *range_filter) {
  if (range_filter != nullptr) {
    DestroyByteArray(range_filter->field);
    DestroyByteArray(range_filter->lower_value);
    DestroyByteArray(range_filter->upper_value);
    free(range_filter);
  }
  return ResponseCode::SUCCESSED;
}

enum ResponseCode DestroyRangeFilters(RangeFilter **range_filters, int num) {
  if (range_filters != nullptr) {
    for (int i = 0; i < num; ++i) {
      DestroyRangeFilter(range_filters[i]);
    }
    free(range_filters);
  }
  return ResponseCode::SUCCESSED;
}

TermFilter **MakeTermFilters(int num) {
  TermFilter **term_filters =
      static_cast<TermFilter **>(malloc(sizeof(TermFilter *) * num));
  memset(term_filters, 0, sizeof(TermFilter *) * num);
  return term_filters;
}

TermFilter *MakeTermFilter(ByteArray *field, ByteArray *value, BOOL is_union) {
  TermFilter *term_filter =
      static_cast<TermFilter *>(malloc(sizeof(TermFilter)));
  memset(term_filter, 0, sizeof(TermFilter));
  term_filter->field = field;
  term_filter->value = value;
  term_filter->is_union = is_union;
  return term_filter;
}

enum ResponseCode SetTermFilter(TermFilter **term_filters, int idx,
                                TermFilter *term_filter) {
  term_filters[idx] = term_filter;
  return ResponseCode::SUCCESSED;
}

enum ResponseCode DestroyTermFilter(TermFilter *term_filter) {
  if (term_filter != nullptr) {
    DestroyByteArray(term_filter->field);
    DestroyByteArray(term_filter->value);
    free(term_filter);
  }
  return ResponseCode::SUCCESSED;
}

enum ResponseCode DestroyTermFilters(TermFilter **term_filters, int num) {
  if (term_filters != nullptr) {
    for (int i = 0; i < num; ++i) {
      DestroyTermFilter(term_filters[i]);
    }
    free(term_filters);
  }
  return ResponseCode::SUCCESSED;
}

VectorQuery *MakeVectorQuery(ByteArray *name, ByteArray *value,
                             double min_score, double max_score, double boost,
                             int has_boost) {
  VectorQuery *vector_query =
      static_cast<VectorQuery *>(malloc(sizeof(VectorQuery)));
  memset(vector_query, 0, sizeof(VectorQuery));
  vector_query->name = name;
  vector_query->value = value;
  vector_query->min_score = min_score;
  vector_query->max_score = max_score;
  vector_query->boost = boost;
  vector_query->has_boost = has_boost;
  return vector_query;
}

VectorQuery **MakeVectorQuerys(int num) {
  VectorQuery **vectorQuery =
      static_cast<VectorQuery **>(malloc(sizeof(VectorQuery *) * num));
  memset(vectorQuery, 0, sizeof(VectorQuery *) * num);
  return vectorQuery;
}

enum ResponseCode SetVectorQuery(VectorQuery **vector_querys, int idx,
                                 VectorQuery *vector_query) {
  vector_querys[idx] = vector_query;
  return ResponseCode::SUCCESSED;
}

enum ResponseCode DestroyVectorQuery(VectorQuery *vector_query) {
  if (vector_query != nullptr) {
    DestroyByteArray(vector_query->name);
    DestroyByteArray(vector_query->value);
    free(vector_query);
  }
  return ResponseCode::SUCCESSED;
}

enum ResponseCode DestroyVectorQuerys(VectorQuery **vector_querys, int num) {
  if (vector_querys != nullptr) {
    for (int i = 0; i < num; ++i) {
      DestroyVectorQuery(vector_querys[i]);
    }
    free(vector_querys);
  }
  return ResponseCode::SUCCESSED;
}

Request *MakeRequest(int topn, VectorQuery **vec_fields, int vec_fields_num,
                     ByteArray **fields, int fields_num,
                     RangeFilter **range_filters, int range_filters_num,
                     TermFilter **term_filters, int term_filters_num,
                     int req_num, int direct_search_type,
                     ByteArray *online_log_level, int has_rank, int multi_vector_rank) {
  Request *request = static_cast<Request *>(malloc(sizeof(Request)));
  memset(request, 0, sizeof(Request));
  request->topn = topn;
  request->vec_fields = vec_fields;
  request->vec_fields_num = vec_fields_num;
  request->fields = fields;
  request->fields_num = fields_num;
  request->range_filters = range_filters;
  request->range_filters_num = range_filters_num;
  request->term_filters = term_filters;
  request->term_filters_num = term_filters_num;
  request->req_num = req_num;
  request->direct_search_type = direct_search_type;
  request->online_log_level = online_log_level;
  request->has_rank = has_rank;
  request->multi_vector_rank = multi_vector_rank;
  return request;
}

enum ResponseCode DestroyRequest(Request *request) {
  if (request != nullptr) {
    DestroyVectorQuerys(request->vec_fields, request->vec_fields_num);
    DestroyByteArrays(request->fields, request->fields_num);
    DestroyRangeFilters(request->range_filters, request->range_filters_num);
    DestroyTermFilters(request->term_filters, request->term_filters_num);
    DestroyByteArray(request->online_log_level);
    free(request);
  }
  return ResponseCode::SUCCESSED;
}

Response *Search(void *engine, Request *request) {
  return static_cast<tig_gamma::GammaEngine *>(engine)->Search(request);
}

SearchResult *GetSearchResult(Response *response, int idx) {
  if (idx >= response->req_num) {
    return NULL;
  }
  return response->results[idx];
}

ResultItem *GetResultItem(SearchResult *search_result, int idx) {
  if (idx >= search_result->result_num) {
    return NULL;
  }
  return search_result->result_items[idx];
}

Field *GetField(const Doc *doc, int idx) {
  if (idx >= doc->fields_num) return NULL;
  return doc->fields[idx];
}

enum ResponseCode DestroyResponse(Response *response) {
  for (int i = 0; i < response->req_num; i++) {
    for (int j = 0; j < response->results[i]->result_num; j++) {
      DestroyDoc(response->results[i]->result_items[j]->doc);
      DestroyByteArray(response->results[i]->result_items[j]->extra);
      free(response->results[i]->result_items[j]);
    }
    DestroyByteArray(response->results[i]->msg);
    if (response->results[i]->result_items != nullptr) {
      free(response->results[i]->result_items);
    }
    if (response->results[i] != nullptr) {
      free(response->results[i]);
    }
  }
  DestroyByteArray(response->online_log_message);
  free(response->results);
  free(response);

  /*
   for (int i = 0; i < response->result_num; ++i) {
     DestroyDoc(response->result_items[i]->doc);
     free(response->result_items[i]);
   }
   free(response->result_items);
   free(response);
   */
  return ResponseCode::SUCCESSED;
}
