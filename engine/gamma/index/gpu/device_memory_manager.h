/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <faiss/gpu/utils/StackDeviceMemory.h>
#include <map>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

namespace tig_gamma {
namespace gamma_gpu {

class MemItem;
class DeviceMemoryManager;

class GammaMemManager {
 public:
  GammaMemManager();

  ~GammaMemManager();

  int Init(int ngpus);

  DeviceMemoryManager *GetManager(int device);

  int ReturnMem(std::thread::id tid);

  int GetMem(MemItem *);

  int ReturnMem();

 private:
  static std::mutex mutex_;
  static bool is_initialized_;
  static int total_mem_num_;
  static int ngpus_;
  static std::vector<DeviceMemoryManager *> memorys_;
};

class DeviceMemoryManager {
 public:
  DeviceMemoryManager(int device);
  DeviceMemoryManager() {}

  int Init(int num);

  faiss::gpu::StackDeviceMemory *Get();

  int ReturnMem(std::thread::id tid);

  int Erase(std::thread::id tid);

 private:
  std::list<int> mem_queue_;
  std::vector<faiss::gpu::StackDeviceMemory *> memorys_;
  std::map<std::thread::id, MemItem *> device_maps_;
  std::list<MemItem *> wait_queue_;
  std::mutex mutex_;
  int device_;
};

}  // namespace gamma_gpu
}  // namespace tig_gamma