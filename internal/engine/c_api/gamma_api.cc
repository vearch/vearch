/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "gamma_api.h"

#include <fcntl.h>
#include <sys/stat.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <sstream>
#include <string>

#include "api_data/doc.h"
#include "api_data/response.h"
#include "api_data/table.h"
#include "search/engine.h"
#include "third_party/nlohmann/json.hpp"
#include "util/log.h"
#include "util/status.h"
#include "util/utils.h"

INITIALIZE_EASYLOGGINGPP

static std::atomic<int> log_dir_flag(0);

int SetLogDictionary(const std::string &log_dir);

void *Init(const char *config_str, int len) {
  nlohmann::json j = nlohmann::json::parse(std::string(config_str, len));

  int flag = log_dir_flag.fetch_add(1);

  if (flag == 0) {
    if (j.find("log_dir") == j.end()) {
      LOG(ERROR) << "log_dir not found in config";
      return nullptr;
    }
    const std::string &log_dir = j["log_dir"];
    SetLogDictionary(log_dir);
  }

  if (j.find("path") == j.end()) {
    LOG(ERROR) << "path not found in config";
    return nullptr;
  }
  const std::string &path = j["path"];

  if (j.find("space_name") == j.end()) {
    LOG(WARNING) << "space_name not found in config";
    j["space_name"] = "default";
  }
  vearch::Engine *engine = vearch::Engine::GetInstance(path, j["space_name"]);
  if (engine == nullptr) {
    LOG(ERROR) << "engine init failed!";
    return nullptr;
  }

  vearch::RequestConcurrentController::GetInstance();
  LOG(INFO) << j["space_name"] << " init succeeded!"
            << ", path=" << path
            << ", log_dir=" << j["log_dir"];
  return static_cast<void *>(engine);
}

int SetLogDictionary(const std::string &log_dir) {
  const std::string &dir = log_dir;
  if (!utils::isFolderExist(dir.c_str())) {
    if (mkdir(dir.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) != 0) {
      LOG(ERROR) << "Failed to create directory: " << dir;
      return -1;
    }
  }

  el::Configurations defaultConf;
  // To set GLOBAL configurations you may use
  el::Loggers::addFlag(el::LoggingFlag::StrictLogFileSizeCheck);
  defaultConf.setGlobally(el::ConfigurationType::Format,
                          "%datetime %fbase:%line %msg");
  defaultConf.setGlobally(el::ConfigurationType::ToFile, "true");
  defaultConf.setGlobally(el::ConfigurationType::ToStandardOutput, "false");
  defaultConf.setGlobally(el::ConfigurationType::MaxLogFileSize,
                          "209715200");  // 200MB
  std::string filename;
  std::string prefix = "gamma.";
  filename =
      prefix + el::LevelHelper::convertToString(el::Level::Debug) + ".log";
  defaultConf.set(el::Level::Debug, el::ConfigurationType::Filename,
                  dir + "/" + filename);

  filename =
      prefix + el::LevelHelper::convertToString(el::Level::Error) + ".log";
  defaultConf.set(el::Level::Error, el::ConfigurationType::Filename,
                  dir + "/" + filename);

  filename =
      prefix + el::LevelHelper::convertToString(el::Level::Info) + ".log";
  defaultConf.set(el::Level::Info, el::ConfigurationType::Filename,
                  dir + "/" + filename);

  filename =
      prefix + el::LevelHelper::convertToString(el::Level::Trace) + ".log";
  defaultConf.set(el::Level::Trace, el::ConfigurationType::Filename,
                  dir + "/" + filename);

  filename =
      prefix + el::LevelHelper::convertToString(el::Level::Warning) + ".log";
  defaultConf.set(el::Level::Warning, el::ConfigurationType::Filename,
                  dir + "/" + filename);

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
  LOG(INFO) << "close";
  delete static_cast<vearch::Engine *>(engine);
  return 0;
}

static void Status2CStatus(vearch::Status status, struct CStatus &cstatus) {
  cstatus.code = status.code();
  if (cstatus.code == 0) {
    cstatus.msg = nullptr;
  } else {
    const std::string &tmpMsg = status.ToString();
    cstatus.msg = new (std::nothrow) char[tmpMsg.size() + 1];
    if (cstatus.msg != nullptr) {
      std::strcpy(cstatus.msg, tmpMsg.c_str());
    }
  }
}

struct CStatus CreateTable(void *engine, const char *table_str, int len) {
  vearch::TableInfo table;
  table.Deserialize(table_str, len);
  vearch::Status status =
      static_cast<vearch::Engine *>(engine)->CreateTable(table);

  struct CStatus cstatus;
  Status2CStatus(status, cstatus);
  return cstatus;
}

int AddOrUpdateDoc(void *engine, const char *doc_str, int len) {
  vearch::Doc doc;
  doc.SetEngine(static_cast<vearch::Engine *>(engine));
  doc.Deserialize(doc_str, len);
  return static_cast<vearch::Engine *>(engine)->AddOrUpdate(doc);
}

struct CStatus Search(void *engine, const char *request_str, int req_len,
                      char **response_str, int *res_len) {
  vearch::Request request;
  request.Deserialize(request_str, req_len);

  vearch::Response response(request.Trace());
  response.SetRequestId(request.RequestId());
  vearch::Status status;
  status = static_cast<vearch::Engine *>(engine)->Search(request, response);
  struct CStatus cstatus;
  Status2CStatus(status, cstatus);
  if (status.code() != 0) {
    return cstatus;
  }

  response.Serialize(static_cast<vearch::Engine *>(engine)->SpaceName(),
                     request.Fields(), status, response_str, res_len);

  return cstatus;
}

struct CStatus Query(void *engine, const char *request_str, int req_len,
                     char **response_str, int *res_len) {
  vearch::QueryRequest request;
  request.Deserialize(request_str, req_len);

  vearch::Response response(false);
  vearch::Status status;
  status = static_cast<vearch::Engine *>(engine)->Query(request, response);
  struct CStatus cstatus;
  Status2CStatus(status, cstatus);
  if (status.code() != 0) {
    return cstatus;
  }

  response.Serialize(static_cast<vearch::Engine *>(engine)->SpaceName(),
                     request.Fields(), status, response_str, res_len);

  return cstatus;
}

int DeleteDoc(void *engine, const char *docid, int docid_len) {
  std::string id = std::string(docid, docid_len);
  int ret = static_cast<vearch::Engine *>(engine)->Delete(id);
  return ret;
}

int GetDocByID(void *engine, const char *docid, int docid_len, char **doc_str,
               int *len) {
  vearch::Doc doc;
  std::string id = std::string(docid, docid_len);
  int ret = static_cast<vearch::Engine *>(engine)->GetDoc(id, doc);
  doc.Serialize(doc_str, len);
  return ret;
}

int GetDocByDocID(void *engine, int docid, char next, char **doc_str,
                  int *len) {
  vearch::Doc doc;
  bool bNext = false;
  if (next != 0) {
    bNext = true;
  }
  int ret = static_cast<vearch::Engine *>(engine)->GetDoc(docid, doc, bNext);
  doc.Serialize(doc_str, len);
  return ret;
}

int BuildIndex(void *engine) {
  int ret = static_cast<vearch::Engine *>(engine)->BuildIndex();
  return ret;
}

int RebuildIndex(void *engine, int drop_before_rebuild, int limit_cpu,
                 int describe) {
  int ret = static_cast<vearch::Engine *>(engine)->RebuildIndex(
      drop_before_rebuild, limit_cpu, describe);
  return ret;
}

void GetEngineStatus(void *engine, char **status_str, int *len) {
  std::string status = static_cast<vearch::Engine *>(engine)->EngineStatus();
  *len = status.length();
  *status_str = (char *)malloc(*len * sizeof(char));
  memcpy(*status_str, status.c_str(), *len);
}

void GetMemoryInfo(void *engine, char **memory_info_str, int *len) {
  std::string memory_info =
      static_cast<vearch::Engine *>(engine)->GetMemoryInfo();
  *len = memory_info.length();
  *memory_info_str = (char *)malloc(*len * sizeof(char));
  memcpy(*memory_info_str, memory_info.c_str(), *len);
}

int Dump(void *engine) {
  int ret = static_cast<vearch::Engine *>(engine)->Dump();
  return ret;
}

int Load(void *engine) {
  int ret = static_cast<vearch::Engine *>(engine)->Load();
  return ret;
}

int SetConfig(void *engine, const char *config_str, int len) {
  int ret = static_cast<vearch::Engine *>(engine)->SetConfig(
      std::string(config_str, len));
  return ret;
}

int GetConfig(void *engine, char **config_str, int *len) {
  std::string config;
  int res = static_cast<vearch::Engine *>(engine)->GetConfig(config);
  *len = config.length();
  *config_str = (char *)malloc(*len * sizeof(char));
  memcpy(*config_str, config.c_str(), *len);
  return res;
}

struct CStatus Backup(void *engine, int command) {
  LOG(INFO) << "backup command: " << command;
  vearch::Status status;
  status = static_cast<vearch::Engine *>(engine)->Backup(command);
  struct CStatus cstatus;
  Status2CStatus(status, cstatus);
  return cstatus;
}

struct CStatus AddFieldIndexWithParams(void *engine, const char *field_name,
                                       int field_name_len,
                                       const char *index_type,
                                       int index_type_len,
                                       const char *index_params,
                                       int index_params_len) {
  std::string fieldName = std::string(field_name, field_name_len);
  std::string indexType = std::string(index_type, index_type_len);
  std::string indexParams = std::string(index_params, index_params_len);

  LOG(INFO) << "adding index for field: " << fieldName
            << " with type: " << indexType << " and params: " << indexParams;

  vearch::Status status;
  status = static_cast<vearch::Engine *>(engine)->AddFieldIndex(
      fieldName, indexType, indexParams);

  struct CStatus cstatus;
  Status2CStatus(status, cstatus);
  return cstatus;
}

struct CStatus RemoveFieldIndex(void *engine, const char *field_name,
                                int field_name_len) {
  std::string fieldName = std::string(field_name, field_name_len);
  LOG(INFO) << "removing index for field: " << fieldName;
  vearch::Status status;
  status = static_cast<vearch::Engine *>(engine)->RemoveFieldIndex(fieldName);
  struct CStatus cstatus;
  Status2CStatus(status, cstatus);
  return cstatus;
}
