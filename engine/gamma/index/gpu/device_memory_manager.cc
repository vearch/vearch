/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "device_memory_manager.h"

#include <chrono>
#include <condition_variable>
#include <set>
#include "log.h"

namespace tig_gamma {
namespace gamma_gpu {

namespace {
size_t kTmpMemorySize = (size_t)1536 * 1024 * 1024;
}  // namespace

class DeviceMemoryManager;

class MemItem {
 public:
  MemItem(std::thread::id tid) {
    tid_ = tid;
    Clear();
  }

  bool GetDone() { return done_; }

  void SetDone() { done_ = true; }

  void Notify() {
    done_ = true;
    cv_.notify_one();
  }

  void Clear() {
    done_ = false;
    mem_id_ = -1;
    mem_ = nullptr;
  }

  void Set(faiss::gpu::StackDeviceMemory *mem) { mem_ = mem; }

  faiss::gpu::StackDeviceMemory *Get() { return mem_; }

  int GetID() { return mem_id_; }

  void SetID(int id) { mem_id_ = id; }

  int WaitForMem() {
    std::unique_lock<std::mutex> lck(mtx_);
    while (not done_) {
      cv_.wait_for(lck, std::chrono::seconds(1),
                   [this]() -> bool { return done_; });
    }
    return 0;
  }

  std::thread::id GetTid() { return tid_; }

 private:
  faiss::gpu::StackDeviceMemory *mem_;
  int mem_id_;
  std::condition_variable cv_;
  std::mutex mtx_;
  bool done_;
  std::thread::id tid_;
};

class ThreadScope {
 public:
  ThreadScope(MemItem *item, DeviceMemoryManager *manager) {
    item_ = item;
    manager_ = manager;
  }

  ~ThreadScope() {
    manager_->ReturnMem(item_->GetTid());
    manager_->Erase(item_->GetTid());
    LOG(INFO) << "ThreadScope exit";
    delete item_;
  }

 private:
  MemItem *item_;
  DeviceMemoryManager *manager_;
};

DeviceMemoryManager::DeviceMemoryManager(int device) : device_(device) {}

int DeviceMemoryManager::Init(int num) {
  memorys_.resize(num);
  for (int i = 0; i < num; ++i) {
    auto mem = new faiss::gpu::StackDeviceMemory(device_, kTmpMemorySize);
    mem->setCudaMallocWarning(true);
    memorys_[i] = mem;
    mem_queue_.push_back(i);
  }

  return 0;
}

faiss::gpu::StackDeviceMemory *DeviceMemoryManager::Get() {
  std::thread::id tid = std::this_thread::get_id();
  MemItem *item = nullptr;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = device_maps_.find(tid);
    if (it == device_maps_.end()) {
      item = new MemItem(tid);
      thread_local ThreadScope deleter(item, this);
      device_maps_.insert(std::make_pair(tid, item));
    } else {
      item = it->second;
    }
    if (item->GetID() >= 0) {
      return item->Get();
    }
    if (mem_queue_.size() > 0) {
      int mem_id = mem_queue_.front();
      mem_queue_.pop_front();
      item->SetDone();
      item->SetID(mem_id);
      item->Set(memorys_[mem_id]);
      return item->Get();
    }
    wait_queue_.push_back(item);
  }

  item->WaitForMem();
  item->Set(memorys_[item->GetID()]);

  return item->Get();
}

int DeviceMemoryManager::ReturnMem(std::thread::id tid) {
  MemItem *item = nullptr;
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = device_maps_.find(tid);
  if (it == device_maps_.end()) {
    return -1;
  }
  item = it->second;
  if (wait_queue_.size() > 0) {
    MemItem *wait_item = wait_queue_.front();
    wait_queue_.pop_front();
    wait_item->SetID(item->GetID());
    wait_item->Set(item->Get());
    wait_item->Notify();
  } else {
    mem_queue_.push_back(item->GetID());
  }

  item->Clear();
  return 0;
}

int DeviceMemoryManager::Erase(std::thread::id tid) {
  std::lock_guard<std::mutex> lock(mutex_);
  device_maps_.erase(tid);
  return 0;
}

GammaMemManager::GammaMemManager() {}

int GammaMemManager::Init(int ngpus) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (is_initialized_) {
    return 0;
  }

  ngpus_ = ngpus;
  int dev = 0;
  cudaDeviceProp devProp;
  cudaGetDeviceProperties(&devProp, dev);
  LOG(INFO) << "-----------------GPU Info-----------------";
  LOG(INFO) << "Number of GPUs [" << ngpus << "]";
  LOG(INFO) << "GPU device [" << dev << "]: [" << devProp.name << "]";
  LOG(INFO) << "SM num [" << devProp.multiProcessorCount << "]";
  LOG(INFO) << "Total global memory ["
            << devProp.totalGlobalMem / 1024.0 / 1024.0 << "] MB";
  LOG(INFO) << "Shared memory per block [" << devProp.sharedMemPerBlock / 1024.0
            << "] KB";
  LOG(INFO) << "Max threads per block [" << devProp.maxThreadsPerBlock << "]";
  LOG(INFO) << "Max threads per MP [" << devProp.maxThreadsPerMultiProcessor
            << "]";
  LOG(INFO) << "------------------------------------------";

  total_mem_num_ = devProp.totalGlobalMem / 2 / kTmpMemorySize;

  memorys_.resize(ngpus_);

  for (int i = 0; i < ngpus_; ++i) {
    memorys_[i] = new DeviceMemoryManager(i);
    memorys_[i]->Init(total_mem_num_);
  }

  is_initialized_ = true;
  return total_mem_num_;
}

DeviceMemoryManager *GammaMemManager::GetManager(int device) {
  if (not is_initialized_) {
    return nullptr;
  }
  return memorys_[device];
}

int GammaMemManager::ReturnMem(std::thread::id tid) {
  for (int i = 0; i < ngpus_; ++i) {
    memorys_[i]->ReturnMem(tid);
  }
  return 0;
}

GammaMemManager::~GammaMemManager() {}

std::mutex GammaMemManager::mutex_;
bool GammaMemManager::is_initialized_ = false;
int GammaMemManager::total_mem_num_ = 0;
int GammaMemManager::ngpus_ = 0;
std::vector<DeviceMemoryManager *> GammaMemManager::memorys_;

}  // namespace gamma_gpu
}  // namespace tig_gamma