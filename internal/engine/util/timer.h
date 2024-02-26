/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <sys/time.h>
#include <unistd.h>

#include <string>
#include <vector>

namespace utils {

class Timer {
  struct StatInfo;

 public:
  void Start(const std::string &tag) {
    // always override
    StatInfo si(tag);
    si.sys_start = CurrentTime();
    si.cpu_start = clock();
    stats_.emplace_back(si);
  }

  void Stop(const std::string &tag) {
    for (auto &si : stats_) {
      if (si.tag == tag) {
        si.sys_cost = CurrentTime() - si.sys_start;
        si.cpu_cost = (clock() - si.cpu_start) / CLOCKS_PER_SEC;
        break;
      }
    }
  }

  void Stop() {
    if (stats_.empty()) {
      return;
    }
    StatInfo &si = stats_.back();
    si.sys_cost = CurrentTime() - si.sys_start;
    si.cpu_cost = (clock() - si.cpu_start) / CLOCKS_PER_SEC;
  }

  void Output() {
    for (auto &si : stats_) {
      std::cout << "[" << si.tag << "] sys: " << std::fixed << si.sys_cost
                << "ms, cpu: " << std::fixed << si.cpu_cost << "s.\n";
    }
  }

  void Output(const std::string &tag) {
    for (auto &si : stats_) {
      if (si.tag == tag) {
        std::cout << "[" << si.tag << "] sys: " << std::fixed << si.sys_cost
                  << "ms, cpu: " << std::fixed << si.cpu_cost << "s.\n";
        break;
      }
    }
  }

 private:
  static long CurrentTime() {
    struct timeval t;
    gettimeofday(&t, NULL);
    return t.tv_sec * 1000 + t.tv_usec / 1000;
  }

 private:
  struct StatInfo {
    explicit StatInfo(const std::string &tag) : tag(tag) {}

    std::string tag;
    int64_t sys_start;
    clock_t cpu_start;
    int sys_cost;  // change to double for more precison
    int cpu_cost;  // change to double for more precison
  };

  std::vector<StatInfo> stats_;
};

}  // namespace utils
