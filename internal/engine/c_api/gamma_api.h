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

struct CStatus {
  int code;
  char *msg;
};

/**
 * @brief init an engine pointer
 *
 * @param config  engine config pointer
 * @return engine pointer
 */
void *Init(const char *config_str, int len);

/**
 * @brief destroy an engine point
 *
 * @param engine  a pointer to search engine
 * @return 0 successed, 1 failed
 */
int Close(void *engine);

/**
 * @brief create a table
 *
 * @param engine  search engine pointer
 * @param table   table info
 * @return 0 successed, 1 failed
 */
struct CStatus CreateTable(void *engine, const char *table_str, int len);

/**
 * @brief add a doc to table, if doc existed, update it
 *
 * @param engine  search engine pointer
 * @param doc     doc pointer to add
 * @return 0 successed, 1 failed
 */
int AddOrUpdateDoc(void *engine, const char *doc_str, int len);

/**
 * @brief delete a doc from table
 *
 * @param engine  search engine pointer
 * @param doc     doc pointer to delete
 * @return 0 successed, 1 failed
 */
int DeleteDoc(void *engine, const char *docid, int docid_len);

/**
 * @brief get the engine status
 *
 * @param engine  search engine pointer
 */
void GetEngineStatus(void *engine, char **status, int *len);

void GetMemoryInfo(void *engine, char **memory_info, int *len);

/** get a doc by id
 *
 * @param engine
 * @param docid  doc id
 * @return  a doc
 */
int GetDocByID(void *engine, const char *docid, int docid_len, char **doc_str,
               int *len);

/**
 * @brief get a doc by docid
 *
 * @param engine
 * @param docid  doc id
 * @param 0: get by docid, 1: return the next undeleted doc with a docid value
 * @return  a doc
 */
int GetDocByDocID(void *engine, int docid, char next, char **doc_str, int *len);

/**
 * @brief build index
 * @param engine  search engine pointer
 * @return 0 successed, 1 failed
 */
int BuildIndex(void *engine);

/**
 * @brief rebuild index
 * @param engine  search engine pointer
 * @return 0 successed, 1 failed
 */
int RebuildIndex(void *engine, int drop_before_rebuild, int limit_cpu,
                 int describe);

/**
 * @brief dump datas into disk accord to Config
 *
 * @param engine
 * @return 0 successed, 1 failed
 */
int Dump(void *engine);

/**
 * @brief load datas from disk accord to Config
 *
 * @param engine
 * @return 0 successed, 1 failed
 */
int Load(void *engine);

/**
 * @brief query vectors to index with serialized result
 *
 * @param engine    search engine pointer
 * @param request   search request pointer
 * @return struct Status
 */
struct CStatus Search(void *engine, const char *request_str, int req_len,
                      char **response_str, int *res_len);

struct CStatus Query(void *engine, const char *request_str, int req_len,
                     char **response_str, int *res_len);
/**
 * @brief alter all cache size by query
 *
 * @param engine  search engine pointer
 * @param cache_str caches' serialized string
 * @return 0 successed, 1 failed
 */
int SetConfig(void *engine, const char *config_str, int len);

/**
 * @brief get all cache size by query
 *
 * @param engine  search engine pointer
 * @param cache_str caches' serialized string
 * @return 0 successed, 1 failed
 */
int GetConfig(void *engine, char **config_str, int *len);

struct CStatus Backup(void *engine, int command);

/**
 * @brief add an index identified by a user-defined name. The index covers
 *        one or more fields. A single vector field => vector index; a single
 *        scalar field => scalar index; 2+ scalar fields => composite index.
 *
 * @param engine            search engine pointer
 * @param index_name        user-defined index name (deletion handle)
 * @param index_name_len    length of index_name
 * @param field_names       array of C strings, one per covered field
 * @param field_name_lens   array of lengths, one per entry in field_names
 * @param field_name_count  number of entries in field_names / field_name_lens
 * @param index_type        index type (e.g., "HNSW", "FLAT", "SCALAR")
 * @param index_type_len    length of index_type
 * @param index_params      JSON string of index parameters
 * @param index_params_len  length of index_params
 * @return struct CStatus
 */
struct CStatus AddFieldIndexWithParams(void *engine,
                                       const char *index_name,
                                       int index_name_len,
                                       const char *const *field_names,
                                       const int *field_name_lens,
                                       int field_name_count,
                                       const char *index_type,
                                       int index_type_len,
                                       const char *index_params,
                                       int index_params_len);

/**
 * @brief remove an index by its user-defined name. Routes internally to
 *        vector / scalar / composite removal based on engine bookkeeping.
 *
 * @param engine          search engine pointer
 * @param index_name      user-defined index name to remove
 * @param index_name_len  length of index_name
 * @return struct CStatus
 */
struct CStatus RemoveFieldIndex(void *engine, const char *index_name,
                                int index_name_len);

void SetMemoryLimitConfig(int memory_limit);

void SetKillStatus(const char *request_id, int partition_id, int reason);

void DeleteKillStatus(const char *request_id, int partition_id);
#ifdef __cplusplus
}
#endif

#endif /* GAMMA_API_H */
