#ifndef ASYNC_FLUSH_H_
#define ASYNC_FLUSH_H_

#include <atomic>
#include <string>
#include <thread>
#include <vector>
#include "common/error_code.h"
#include "util/log.h"
#include "util/utils.h"

struct AsyncFlusher {
  AsyncFlusher(std::string name) : name_(name) { nflushed_ = 0; }
  ~AsyncFlusher() {}

  int Until(int vid, int timeout = 0) {
    double begin = utils::getmillisecs();
    while (nflushed_ < vid) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      if (timeout > 0 && utils::getmillisecs() - begin > timeout) {
        return TIMEOUT_ERR;
      }
    }
    return 0;
  }

  // reset after load()
  void Reset(int num) { nflushed_ = num; }

  virtual int FlushOnce() = 0;

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
    int ret = executor->Flush();
    if (ret != 0) {
      LOG(ERROR) << "async flush executor exit unexpectedly! ret=" << ret;
    } else {
      LOG(INFO) << "async flush executor exit successfully!";
    }
  }

  int Flush() {
    while (!stopped_) {
      for (AsyncFlusher *af : async_flushers_) {
        int ret = af->FlushOnce();
        if (ret) {
          LOG(ERROR) << "aysnc flush error, ret=" << ret
                     << ", name=" << af->name_;
          return ret;
        }
        if (stopped_) return 0;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(interval_));
    }
    return 0;
  }

  void Add(AsyncFlusher *af) { async_flushers_.push_back(af); }
};

#endif
