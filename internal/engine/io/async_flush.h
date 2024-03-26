/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include "util/log.h"
#include "util/status.h"
#include "util/utils.h"

struct AsyncFlusher {
  AsyncFlusher(std::string name) : name_(name) { nflushed_ = 0; }
  ~AsyncFlusher() {}

  vearch::Status Until(int vid, int timeout = 0) {
    double begin = utils::getmillisecs();
    while (nflushed_ < vid) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      if (timeout > 0 && utils::getmillisecs() - begin > timeout) {
        return vearch::Status::TimedOut();
      }
    }
    return vearch::Status::OK();
  }

  // reset after load()
  void Reset(int num) { nflushed_ = num; }

  virtual vearch::Status FlushOnce() = 0;

  std::string name_;
  std::atomic<long> nflushed_;
};

struct AsyncFlushExecutor {
  std::vector<AsyncFlusher *> async_flushers_;
  std::thread *runner_;
  bool stopped_;
  int interval_;

  AsyncFlushExecutor() {
    stopped_ = false;
    interval_ = 100;  // ms
    runner_ = nullptr;
  }

  ~AsyncFlushExecutor() {
    if (runner_) delete runner_;
  }

  void Start() {
    if (!stopped_) {
      Stop();
    }
    stopped_ = false;
    runner_ = new std::thread(Handler, this);
  }

  void Stop() {
    stopped_ = true;
    if (runner_) {
      runner_->join();
      delete runner_;
      runner_ = nullptr;
    }
  }

  static void Handler(AsyncFlushExecutor *executor) {
    LOG(INFO) << "async flush executor is started!";
    vearch::Status status = executor->Flush();
    if (!status.ok()) {
      LOG(ERROR) << "async flush executor exit unexpectedly! ret="
                 << status.ToString();
    } else {
      LOG(INFO) << "async flush executor exit successfully!";
    }
  }

  vearch::Status Flush() {
    while (!stopped_) {
      for (AsyncFlusher *af : async_flushers_) {
        vearch::Status status = af->FlushOnce();
        if (!status.ok()) {
          LOG(ERROR) << "aysnc flush error, ret=" << status.ToString()
                     << ", name=" << af->name_;
          return status;
        }
        if (stopped_) return vearch::Status::OK();
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(interval_));
    }
    return vearch::Status::OK();
  }

  void Add(AsyncFlusher *af) { async_flushers_.push_back(af); }
};
