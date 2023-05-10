/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "log_stream.h"

using framework::LogStream;

namespace utils {

// level: DEBUG, INFO, WARN, ERROR
#define OLOG(plog, level, msg)                       \
  do {                                               \
    if (auto *ls = (plog)->GetLogStream()) {         \
      level##_ONLINE(*ls) << msg << framework::endl; \
    }                                                \
  } while (0)

/***
 * this is a *RAII* wrapper of online log stream.
 */
class OnlineLogger {
 public:
  OnlineLogger() : log_stream_(nullptr) {}

  ~OnlineLogger() {
    delete log_stream_;
    log_stream_ = nullptr;
  }

  int Init(const std::string &log_level_str) {
    auto log_level = LogStream::GetLevel(log_level_str.c_str());
    if (log_level != LogStream::LOG_NONE) {
      log_stream_ = new (std::nothrow) LogStream();
      if (log_stream_ == nullptr) {
        return -1;
      }
      log_stream_->SetLevel(log_level);
    }
    return 0;
  }

  void Debug(const std::string &msg) { OLOG(this, DEBUG, msg); }
  void Info(const std::string &msg) { OLOG(this, INFO, msg); }
  void Warn(const std::string &msg) { OLOG(this, WARN, msg); }
  void Error(const std::string &msg) { OLOG(this, ERROR, msg); }

  framework::LogStream *GetLogStream() { return log_stream_; }

  const char *Data() const {
    if (log_stream_ && log_stream_->Length() > 0) {
      return log_stream_->Data();
    }
    return nullptr;
  }

  int Length() const {
    return log_stream_ ? log_stream_->Length() : -1;  // -1 instead of 0
  }

 private:
  framework::LogStream *log_stream_;
};

}  // namespace utils
