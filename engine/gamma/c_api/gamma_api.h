/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifndef GAMMA_API_H_
#define GAMMA_API_H_

#ifdef __cplusplus
extern "C" {
#endif

#define BOOL char
#define TRUE 1
#define FALSE 0

enum ResponseCode { SUCCESSED = 0, FAILED };

enum DistanceMetricType { InnerProduct = 0, L2 };
/** byte array struct
 */
typedef struct ByteArray {
  char *value;
  int len;
} ByteArray;

/** make ByteArray array
 *
 * @param num  ByteArray array length
 * @return     ByteArray array head pointer
 */
ByteArray **MakeByteArrays(int num);

/** make a byte array
 *
 * @param value  byte array value
 * @param len    byte array len
 * @return byte array pointer
 */
ByteArray *MakeByteArray(const char *value, int len);

/** Setting ByteArrays content
 *
 * @param byte_arrays    ByteArray array head pointer
 * @param idx            ByteArray array subscript
 * @param byte_array     ByteArray array content to set
 * @return ResponseCode
 */
enum ResponseCode SetByteArray(ByteArray **byte_arrays, int idx,
                               ByteArray *byte_array);

/** destroy byte array
 *
 * @param  byteArray byte array pointer
 */
enum ResponseCode DestroyByteArray(ByteArray *byteArray);

/** destroy byte_array array
 *
 * @param  byte_arrays byte_array array pointer
 * @param  num         byte_array array length
 */
enum ResponseCode DestroyByteArrays(ByteArray **byte_arrays, int num);

/** engine config
 * path : files dictionary, includes .idx, .fet, .str.prf, .prf, etc.
 * max_doc_size : max doc size, TODO maybe remove in future
 */
typedef struct Config {
  ByteArray *path;
  int max_doc_size;
} Config;

/** make Config
 *
 * @param path       files dictionarys
 * @param max_doc_size  max doc size
 * @return Config pointer
 */
Config *MakeConfig(ByteArray *path, int max_doc_size);

/** destroy Config
 *
 * @param path       files dictionarys
 * @return Config pointer
 */
enum ResponseCode DestroyConfig(Config *config);

/** data type
 */
enum DataType { INT = 0, LONG, FLOAT, DOUBLE, STRING, VECTOR };

// VectorInfo
typedef struct VectorInfo {
  ByteArray *name;            // vector's name
  enum DataType data_type;    // vector data type, float only supported now
  BOOL is_index;              // whether index to be trained
  int dimension;              // dimension
  ByteArray *model_id;        // model_id, temporarily useless
  ByteArray *retrieval_type;  // "IVFPQ"
  ByteArray *store_type;      // "Mmap", "RocksDB"
  ByteArray *store_param;     // parameters of store, json format
} VectorInfo;

/** make vector infos array
 *
 * @param num  VectorInfo array length
 * @return VectorInfo array pointer
 */
VectorInfo **MakeVectorInfos(int num);

/** make VectorInfo
 *
 * @param name       VectorInfo name
 * @param data_type  vector data type, float only supported now
 * @param is_index   whether index to be trained
 * @param dimension  vector dimension
 * @param model_id   model_id, temporarily useless
 * @return VectorInfo pointer
 */
VectorInfo *MakeVectorInfo(ByteArray *name, enum DataType data_type,
                           BOOL is_index, int dimension, ByteArray *model_id,
                           ByteArray *retrieval_type, ByteArray *store_type,
                           ByteArray *store_param);

/** Setting VectorInfo content
 *
 * @param vector_infos    VectorInfo array head pointer
 * @param idx             VectorInfo array subscript
 * @param vector_info     VectorInfo array content to set
 * @return ResponseCode
 */
enum ResponseCode SetVectorInfo(VectorInfo **vector_infos, int idx,
                                VectorInfo *vector_info);

/** destroy VectorInfo array
 *
 * @param vectorInfo VectorInfo array head pointer
 * @param num        VectorInfo array length
 * @return ResponseCode
 */
enum ResponseCode DestroyVectorInfos(VectorInfo **vectorInfo, int num);

// field info
typedef struct FieldInfo {
  ByteArray *name;
  enum DataType data_type;
  BOOL is_index;  // whether index numeric field
} FieldInfo;

/** make FieldInfo array
 *
 * @param num  FieldInfo array length
 * @return     FieldInfo array head pointer
 */
FieldInfo **MakeFieldInfos(int num);

/** make a FieldInfo
 *
 * @param name       field name
 * @param data_type  field data type
 * @return FieldInfo pointer
 */
FieldInfo *MakeFieldInfo(ByteArray *name, enum DataType data_type,
                         BOOL is_index);

/** Setting fieldInfo content
 *
 * @param field_infos  FieldInfo array head pointer
 * @param idx          FieldInfo array subscript
 * @param field_info   FieldInfo array content to set
 * @return
 */
enum ResponseCode SetFieldInfo(FieldInfo **field_infos, int idx,
                               FieldInfo *field_info);

/** destroy FieldInfo array
 *
 * @param field_infos FieldInfo array head pointer
 * @param num         FieldInfo array length
 * @return ResponseCode
 */
enum ResponseCode DestroyFieldInfos(FieldInfo **field_infos, int num);

/** field with value
 */
typedef struct Field {
  ByteArray *name;
  ByteArray *value;
  ByteArray *source;
  enum DataType data_type;
} Field;

/** make Field array
 *
 * @param num  Field array length
 * @return Field array head pointer
 */
Field **MakeFields(int num);

/** make a Field
 *
 * @param name       field name
 * @param value      field value
 * @param source     field source
 * @param data_type  field data type
 * @return a Field pointer
 */
Field *MakeField(ByteArray *name, ByteArray *value, ByteArray *source,
                 enum DataType data_type);

/** set Field arrary content
 *
 * @param fields  Field array head pointer
 * @param idx     Field array subscript
 * @param field   Field content to set
 * @return ResponseCode
 */
enum ResponseCode SetField(Field **fields, int idx, Field *field);

/** destroy Field
 *
 * @param fields  Field pointer
 * @return ResponseCode
 */
enum ResponseCode DestroyField(Field *field);

/** destroy Field array
 *
 * @param fields  Field array head pointer
 * @param num     Field array length
 * @return ResponseCode
 */
enum ResponseCode DestroyFields(Field **fields, int num);

#pragma pack(1)
typedef struct IVFPQParameters {
  int metric_type;
  int nprobe;      // scan nprobe
  int ncentroids;  // coarse cluster center number
  int nsubvector;
  int nbits_per_idx;  // bit number of sub cluster center
} IVFPQParameters;
#pragma pack()

/** make a IVFPQParameters pointer
 *
 * @param metric_type   metric type, 0 inner product, 1 L2, default(-1) inner
 * product
 * @param nprobe        scan nprobe, default(-1) 20, it should be less than
 * ncentroids
 * @param ncentroids    coarse cluster center number, default(-1) 256
 * @param nsubvector    the number of sub vector, default(-1) 64, only the value
 * which is multiple of 4 is supported now
 * @param nbits_per_idx bit number of sub cluster center, default(-1) 8, and 8
 * is the only value now
 * @return IVFPQParameters pointer
 */
IVFPQParameters *MakeIVFPQParameters(int metric_type, int nprobe,
                                     int ncentroids, int nsubvector,
                                     int nbits_per_idx);

/** destroy IVFPQParameters pointer
 *
 * @param param IVFPQParameters pointer
 * @return ResponseCode
 */
enum ResponseCode DestroyIVFPQParameters(IVFPQParameters *param);

// table info
typedef struct Table {
  ByteArray *name;            // table name
  FieldInfo **fields;         // FieldInfo array head pointer
  int fields_num;             // field num
  VectorInfo **vectors_info;  // VectorInfo array head pointer
  int vectors_num;            // vector num
  IVFPQParameters *ivfpq_param;
} Table;

/** make a Table pointer
 *
 * @param name          table name
 * @param fields        table FieldInfo array
 * @param fields_num    fields num
 * @param vectors_info  vectors info array
 * @param vectors_num   vectors num
 * @return a Table pointer
 */
Table *MakeTable(ByteArray *name, FieldInfo **fields, int fields_num,
                 VectorInfo **vectors_info, int vectors_num,
                 IVFPQParameters *ivfpq_param);

/** destroy Table
 *
 * @param table  Table pointer
 * @return ResponseCode
 */
enum ResponseCode DestroyTable(Table *table);

// document
typedef struct Doc {
  Field **fields;  // Field array head pointer
  int fields_num;  // Field array length
} Doc;

/** make a Doc pointer
 *
 * @param fields        Field array
 * @param fields_num    fields num
 * @return a Doc pointer
 */
Doc *MakeDoc(Field **fields, int fields_num);

/** destroy Doc
 *
 * @param doc  Doc pointer
 * @return ResponseCode
 */
enum ResponseCode DestroyDoc(Doc *doc);

enum IndexStatus { UNINDEXED = 0, INDEXING, INDEXED };

/** set the log dictionary, called once when the library is loaded
 *
 * @param log_dir  log dictionary
 * @return ResponseCode
 */
enum ResponseCode SetLogDictionary(ByteArray *log_dir);

/** init an engine pointer
 *
 * @param config  engine config pointer
 * @return engine pointer
 */
void *Init(Config *config);

/** destroy an engine point
 *
 * @param engine  a pointer to search engine
 * @return ResponseCode
 */
enum ResponseCode Close(void *engine);

/** create a table
 *
 * @param engine  search engine pointer
 * @param table   table info
 * @return ResponseCode
 */
enum ResponseCode CreateTable(void *engine, Table *table);

/** add a doc to table
 *
 * @param engine  search engine pointer
 * @param doc     doc pointer to add
 * @return ResponseCode
 */
enum ResponseCode AddDoc(void *engine, Doc *doc);

/** add a doc to table, if doc existed, update it
 *
 * @param engine  search engine pointer
 * @param doc     doc pointer to add
 * @return ResponseCode
 */
enum ResponseCode AddOrUpdateDoc(void *engine, Doc *doc);

/** update a doc, if _id not exist, equal to function @AddDoc
 *
 * @param engine  search engine pointer
 * @param doc     doc pointer to update
 * @return ResponseCode
 */
enum ResponseCode UpdateDoc(void *engine, Doc *doc);

/** delete a doc from table
 *
 * @param engine  search engine pointer
 * @param doc     doc pointer to delete
 * @return ResponseCode
 */
enum ResponseCode DelDoc(void *engine, ByteArray *doc_id);

/** get doc num from table
 *
 * @param engine  search engine pointer
 * @return dcos num
 */
int GetDocsNum(void *engine);

/** Get engine memory size
 *
 * @param engine  search engine pointer
 * @return memory size by Bytes
 */
long GetMemoryBytes(void *engine);

/** get a doc by id
 *
 * @param engine
 * @param doc_id  doc id
 * @return  a doc
 */
Doc *GetDocByID(void *engine, ByteArray *doc_id);

/** This is a blocking fuction, its role is to establish a faiss index,
 * after the completion, monitor the changes in the profile.
 * If an error occurs during the creation process,
 * the function returns immediately
 * Example:
 *   std::thread t(BuildIndex);
 *   t.join();
 *
 * @param engine  search engine pointer
 * @return ResponseCode
 */
enum ResponseCode BuildIndex(void *engine);

/** get the engine status
 *
 * @param engine  search engine pointer
 * @return Status: UNINDEXED = 0, INDEXING, INDEXED
 */
enum IndexStatus GetIndexStatus(void *engine);

/** dump datas into disk accord to Config
 *
 * @param engine
 * @return ResponseCode
 */
enum ResponseCode Dump(void *engine);

/** load datas from disk accord to Config
 *
 * @param engine
 * @return ResponseCode
 */
enum ResponseCode Load(void *engine);

typedef struct RangeFilter {
  ByteArray *field;        // field to filter
  ByteArray *lower_value;  // lower value
  ByteArray *upper_value;  // upper value
  BOOL include_lower;      // whether to include lower value
  BOOL include_upper;      // whether to include upper value
} RangeFilter;

/** make RangeFilter array
 *
 * @param num  RangeFilter array length
 * @return RangeFilter array head pointer
 */
RangeFilter **MakeRangeFilters(int num);

/** make a RangeFilter
 *
 * @param field          field to filter
 * @param lower_value    lower value
 * @param upper_value    upper value
 * @param include_lower  whether to include lower value
 * @param include_upper  whether to include upper value
 * @return a RangeFilter pointer
 */
RangeFilter *MakeRangeFilter(ByteArray *field, ByteArray *lower_value,
                             ByteArray *upper_value, BOOL include_lower,
                             BOOL include_upper);

/** set RangeFilter content
 *
 * @param range_filters  RangeFilter array head pointer
 * @param idx            RangeFilter array subscript
 * @param range_filter   RangeFilter content to set
 * @return ResponseCode
 */
enum ResponseCode SetRangeFilter(RangeFilter **range_filters, int idx,
                                 RangeFilter *range_filter);

/** destroy RangeFilter
 *
 * @param range_filter  RangeFilter pointer
 * @return ResponseCode
 */
enum ResponseCode DestroyRangeFilter(RangeFilter *range_filter);

/** destroy RangeFilter array
 *
 * @param range_filters  RangeFilter array head pointer
 * @param num            RangeFilter array length
 * @return ResponseCode
 */
enum ResponseCode DestroyRangeFilters(RangeFilter **range_filters, int num);

typedef struct TermFilter {
  ByteArray *field;  // field to filter
  ByteArray *value;  // filter value
  BOOL is_union;     // 0: intersect, 1: union
} TermFilter;

/** make TermFilter array
 *
 * @param num  TermFilter array length
 * @return TermFilter array head pointer
 */
TermFilter **MakeTermFilters(int num);

/** make a TermFilter
 *
 * @param field          field to filter
 * @param lower_value    lower value
 * @return a TermFilter pointer
 */
TermFilter *MakeTermFilter(ByteArray *field, ByteArray *value, BOOL is_union);

/** set TermFilter content
 *
 * @param term_filters  TermFilter array head pointer
 * @param idx           TermFilter array subscript
 * @param term_filter   TermFilter content to set
 * @return ResponseCode
 */
enum ResponseCode SetTermFilter(TermFilter **term_filters, int idx,
                                TermFilter *term_filter);

/** destroy TermFilter
 *
 * @param term_filter  TermFilter pointer
 * @return ResponseCode
 */
enum ResponseCode DestroyTermFilter(TermFilter *term_filter);

/** destroy RangeFilter array
 *
 * @param range_filters  RangeFilter array head pointer
 * @param num            RangeFilter array length
 * @return ResponseCode
 */
enum ResponseCode DestroyTermFilters(TermFilter **term_filters, int num);

typedef struct VectorQuery {
  struct ByteArray *name;   // vector name
  struct ByteArray *value;  // vector value
  double min_score;         // min score
  double max_score;         // max score
  double boost;             // min_score * boost = weighted result
  int has_boost;            // default 0, not activate boost; 1 activate boost
} VectorQuery;

/** make a VectorQuery
 *
 * @param name       VectorQuery name
 * @param value      VectorQuery value
 * @param min_score  min score
 * @param max_score  max score
 * @param boost      min_score * boost = weighted result
 * @param has_boost  0 not activate boost; 1 activate boost
 * @return a VectorQuery pointer
 */
VectorQuery *MakeVectorQuery(ByteArray *name, ByteArray *value,
                             double min_score, double max_score, double boost,
                             int has_boost);

/** make VectorQuery array
 *
 * @param num  VectorQuery array length
 * @return VectorQuery array head pointer
 */
VectorQuery **MakeVectorQuerys(int num);

/** set VectorQuery arrary content
 *
 * @param vector_querys  VectorQuery array head pointer
 * @param idx            VectorQuery array subscript
 * @param vector_query   VectorQuery content to set
 * @return ResponseCode
 */
enum ResponseCode SetVectorQuery(VectorQuery **vector_querys, int idx,
                                 VectorQuery *vector_query);

/** destroy VectorQuery
 *
 * @param fields  Field pointer
 * @return ResponseCode
 */
enum ResponseCode DestroyVectorQuery(VectorQuery *vector_query);

/** destroy VectorQuery array
 *
 * @param fields  Field array head pointer
 * @param num     Field array length
 * @return ResponseCode
 */
enum ResponseCode DestroyVectorQuerys(VectorQuery **vector_querys, int num);

/* sequence of vecs is the same as vec_fields */
typedef struct Request {
  int req_num;
  int topn;
  int direct_search_type;  // -1: no direct search, 0: auto, 1: always direct
                           // search, default 0

  VectorQuery **vec_fields;
  int vec_fields_num;

  ByteArray **fields;
  int fields_num;

  RangeFilter **range_filters;
  int range_filters_num;

  TermFilter **term_filters;
  int term_filters_num;
  enum DistanceMetricType metric_type;

  // online log level: debug|info|warn|error|none
  ByteArray *online_log_level;

  int has_rank;  // whether it needs ranking after recalling from PQ index.
                 // default 0, has not rank; 1, has rank
  int multi_vector_rank;  // whether it needs ranking after merging the
                          // searching result of multi-vectors. default 0, has
                          // not rank; 1, has rank
} Request;

/** make a Request
 *
 * @param topn                top n similar results
 * @param vec_fields          vec_field array
 * @param vec_fields_num      vec_field array length
 * @param fields              fields array
 * @param fields_num          fields array length
 * @param range_filters       rangeFilters array
 * @param range_filters_num   rangeFilters array length
 * @param term_filters        termFilters array
 * @param term_filters_num    termFilters array length
 * @param req_num             request number
 * @param direct_search_type  1 : direct search; 0 : normal search
 * @param online_log_level    DEBUG, INFO, WARN, ERROR
 * @param has_rank            default 0, has not rank; 1, has rank
 * @return  a request pointer
 */
Request *MakeRequest(int topn, VectorQuery **vec_fields, int vec_fields_num,
                     ByteArray **fields, int fields_num,
                     RangeFilter **range_filters, int range_filters_num,
                     TermFilter **term_filters, int term_filters_num,
                     int req_num, int direct_search_type,
                     ByteArray *online_log_level, int has_rank,
                     int multi_vector_rank);

/** destroy Request
 *
 * @param request  Request pointer
 * @return ResponseCode
 */
enum ResponseCode DestroyRequest(Request *request);

typedef struct ResultItem {
  double score;
  Doc *doc;
  ByteArray *extra;
} ResultItem;

enum SearchResultCode { SUCCESS = 0, INDEX_NOT_TRAINED, SEARCH_ERROR };

typedef struct SearchResult {
  int total;
  int result_num;
  enum SearchResultCode result_code;
  ByteArray *msg;

  ResultItem **result_items;
} SearchResult;

typedef struct Response {
  int req_num;
  SearchResult **results;

  ByteArray *online_log_message;  // may be null
} Response;

/** query vectors to index
 *
 * @param engine    search engine pointer
 * @param request   search request pointer
 * @return response, need to call @DestroyResponse to destroy
 */
Response *Search(void *engine, Request *request);

/** delete docs from table by query
 *
 * @param engine  search engine pointer
 * @param request delete request pointer
 * @return ResponseCode
 */
enum ResponseCode DelDocByQuery(void *engine, Request *request);

/** get SearchResult from response
 *
 * @param response  response pointer
 * @param idx       SearchResult array subscript
 * @return NULL if idx >= response->req_num
 */
SearchResult *GetSearchResult(Response *response, int idx);

/** get ResultItem from search_result
 *
 * @param search_result  search_result pointer
 * @param idx       ResultItem array subscript
 * @return NULL if idx >= search_result->result_num
 */
ResultItem *GetResultItem(SearchResult *search_result, int idx);

/** get Field from doc
 *
 * @param Doc  doc pointer
 * @param idx  Field array subscript
 * @return NULL if idx >= doc->fields_num
 */
Field *GetField(const Doc *doc, int idx);

/** destroy response
 *
 * @param response  response to destroy
 * @return ResponseCode
 */
enum ResponseCode DestroyResponse(Response *response);

#ifdef __cplusplus
}
#endif

#endif /* SEARCH_API_H */
