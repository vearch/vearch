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

#include "api_data/gamma_batch_result.h"
#include "api_data/gamma_config.h"
#include "api_data/gamma_doc.h"
#include "api_data/gamma_engine_status.h"
#include "api_data/gamma_response.h"
#include "api_data/gamma_table.h"
#include "search/gamma_engine.h"
#include "util/log.h"
#include "util/utils.h"

INITIALIZE_EASYLOGGINGPP

static int log_dir_flag = 0;

int SetLogDictionary(const std::string &log_dir);

void *Init(const char *config_str, int len) {
  tig_gamma::Config config;
  config.Deserialize(config_str, len);

  int flag = __sync_fetch_and_add(&log_dir_flag, 1);

  if (flag == 0) {
    const std::string &log_dir = config.LogDir();
    SetLogDictionary(log_dir);
  }

  const std::string &path = config.Path();
  tig_gamma::GammaEngine *engine = tig_gamma::GammaEngine::GetInstance(path);
  if (engine == nullptr) {
    LOG(ERROR) << "Engine init faild!";
    return nullptr;
  }

  tig_gamma::RequestConcurrentController::GetInstance();
  LOG(INFO) << "Engine init successed!";
  return static_cast<void *>(engine);
}

int SetLogDictionary(const std::string &log_dir) {
  const std::string &dir = log_dir;
  if (!utils::isFolderExist(dir.c_str())) {
    mkdir(dir.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  }

  el::Configurations defaultConf;
  // To set GLOBAL configurations you may use
  el::Loggers::addFlag(el::LoggingFlag::StrictLogFileSizeCheck);
  defaultConf.setGlobally(el::ConfigurationType::Format,
                          "%level %datetime %fbase:%line %msg");
  defaultConf.setGlobally(el::ConfigurationType::ToFile, "true");
  defaultConf.setGlobally(el::ConfigurationType::ToStandardOutput, "false");
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

  LOG(INFO) << "Version [" << GIT_SHA1 << "]";
  return 0;
}

int Close(void *engine) {
  LOG(INFO) << "Close";
  delete static_cast<tig_gamma::GammaEngine *>(engine);
  return 0;
}

int CreateTable(void *engine, const char *table_str, int len) {
  tig_gamma::TableInfo table;
  table.Deserialize(table_str, len);
  int ret = static_cast<tig_gamma::GammaEngine *>(engine)->CreateTable(table);
  return ret;
}

int AddOrUpdateDoc(void *engine, const char *doc_str, int len) {
  tig_gamma::Doc doc;
  doc.SetEngine(static_cast<tig_gamma::GammaEngine *>(engine));
  doc.Deserialize(doc_str, len);
  return static_cast<tig_gamma::GammaEngine *>(engine)->AddOrUpdate(doc);
}

int AddOrUpdateDocsNum(void *engine, int i) {
  return static_cast<tig_gamma::GammaEngine *>(engine)->SetBatchDocsNum(i);
}

int PrepareDocs(void *engine, char *doc_str, int id) {
  return static_cast<tig_gamma::GammaEngine *>(engine)->BatchDocsPrepare(
      doc_str, id);
}

int AddOrUpdateDocsFinish(void *engine, int len, char **result_str,
                          int *result_len) {
  char **docs_str =
      static_cast<tig_gamma::GammaEngine *>(engine)->BatchDocsStr();
  AddOrUpdateDocs(engine, docs_str, len, result_str, result_len);
  return 0;
}

int AddOrUpdateDocs(void *engine, char **doc_str, int len, char **result_str,
                    int *result_len) {
  tig_gamma::Docs docs;
  docs.SetEngine(static_cast<tig_gamma::GammaEngine *>(engine));
  docs.Deserialize(doc_str, len);

  tig_gamma::BatchResult result(docs.GetDocs().size());
  int ret = static_cast<tig_gamma::GammaEngine *>(engine)->AddOrUpdateDocs(
      docs, result);
  result.Serialize(result_str, result_len);

  return ret;
}

int UpdateDoc(void *engine, const char *doc_str, int len) {
  return -1;
}

int Search(void *engine, const char *request_str, int req_len,
           char **response_str, int *res_len) {
  tig_gamma::Response response;
  tig_gamma::Request request;
  request.Deserialize(request_str, req_len);

  int ret =
      static_cast<tig_gamma::GammaEngine *>(engine)->Search(request, response);
  if (ret != 0) { return ret; }

  response.Serialize(request.Fields(), response_str, res_len);

  return ret;
}

int DeleteDoc(void *engine, const char *docid, int docid_len) {
  std::string id = std::string(docid, docid_len);
  int ret = static_cast<tig_gamma::GammaEngine *>(engine)->Delete(id);
  return ret;
}

int GetDocByID(void *engine, const char *docid, int docid_len, char **doc_str,
               int *len) {
  tig_gamma::Doc doc;
  std::string id = std::string(docid, docid_len);
  int ret = static_cast<tig_gamma::GammaEngine *>(engine)->GetDoc(id, doc);
  doc.Serialize(doc_str, len);
  return ret;
}

int GetDocByDocID(void *engine, int docid, char **doc_str, int *len) {
  tig_gamma::Doc doc;
  int ret = static_cast<tig_gamma::GammaEngine *>(engine)->GetDoc(docid, doc);
  doc.Serialize(doc_str, len);
  return ret;
}

int BuildIndex(void *engine) {
  int ret = static_cast<tig_gamma::GammaEngine *>(engine)->BuildIndex();
  return ret;
}

void GetEngineStatus(void *engine, char **status_str, int *len) {
  tig_gamma::EngineStatus engine_status;
  static_cast<tig_gamma::GammaEngine *>(engine)->GetIndexStatus(engine_status);
  engine_status.Serialize(status_str, len);
}

int Dump(void *engine) {
  int ret = static_cast<tig_gamma::GammaEngine *>(engine)->Dump();
  return ret;
}

int Load(void *engine) {
  int ret = static_cast<tig_gamma::GammaEngine *>(engine)->Load();
  return ret;
}

int DelDocByQuery(void *engine, const char *request_str, int len) {
  tig_gamma::Request request;
  request.Deserialize(request_str, len);
  int ret =
      static_cast<tig_gamma::GammaEngine *>(engine)->DelDocByQuery(request);
  return ret;
}

int DelDocByFilter(void *engine, const char *request_str, int len,
                   char **deleted_ids, int *str_len) {
  tig_gamma::Request request;
  request.Deserialize(request_str, len);
  int ret = static_cast<tig_gamma::GammaEngine *>(engine)->DelDocByFilter(
                                           request, deleted_ids, str_len);
  return ret;
}

int SetConfig(void *engine, const char *config_str, int len) {
  tig_gamma::Config config;
  config.Deserialize(config_str, len);
  int ret =
      static_cast<tig_gamma::GammaEngine *>(engine)->SetConfig(config);
  return ret;
}

int GetConfig(void *engine, char **config_str, int *len) {
  tig_gamma::Config config;
  int res = 
      static_cast<tig_gamma::GammaEngine *>(engine)->GetConfig(config);
  if (res == 0) { res = config.Serialize(config_str, len); }
  return res;
}

int BeginMigrate(void *engine) {
  int res = 
      static_cast<tig_gamma::GammaEngine *>(engine)->BeginMigrate();
  return res;
}

int GetMigrageDoc(void *engine, char **doc_str, int *len, int *is_del) {
  tig_gamma::Doc doc;
  int ret = static_cast<tig_gamma::GammaEngine *>(engine)->GetMigrageDoc(doc, is_del);
  if (ret == 0) { doc.Serialize(doc_str, len); }
  return ret;
}

int TerminateMigrate(void *engine) {
  int res = 
      static_cast<tig_gamma::GammaEngine *>(engine)->TerminateMigrate();
  return res;
}
