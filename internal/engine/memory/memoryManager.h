#pragma once

#include <cstdint>
#include <atomic>
#include <thread>
#include <mutex>
#include <chrono>

namespace vearch {

class MemoryManager {
public:
    static MemoryManager& GetInstance();

    MemoryManager(const MemoryManager&) = delete;
    MemoryManager& operator=(const MemoryManager&) = delete;
    MemoryManager(MemoryManager&&) = delete;
    MemoryManager& operator=(MemoryManager&&) = delete;

    void StartMonitoring(int interval_sec = 60);

    void StartMonitoring(int64_t max_memory, int interval_sec);

    void StopMonitoring();

    void TrimMemory();

    bool CheckMemoryUsageExceed(int estimatedSize);

    int64_t GetCurrentMemoryUsage();

    int64_t GetTotalMemory() const { return total_memory_; }

    int64_t GetMemoryLimit() const { return max_memory_; }

    bool IsInitialized() const { return is_initialized_; }

    int GetMemoryLimitThreshold() const { return memory_limit_threshold_; }

    void SetMemoryLimitThreshold(uint32_t memory_limit_threshold) { 
        memory_limit_threshold_ = memory_limit_threshold; 
        return;
    }

    void SetMemoryLimit() {
        max_memory_ = total_memory_ * memory_limit_threshold_ / 100;
        return; 
    }

private:
    MemoryManager() 
        : max_memory_(0),
          total_memory_(0),
          interval_sec_(60),
          is_running_(false),
          is_initialized_(false),
          last_trim_time_(std::chrono::steady_clock::now()),
          mem_trim_thread_(nullptr) {}

    ~MemoryManager() {
        StopMonitoring();
    }

    void MonitorThread();

    int64_t GetCurrentMemoryUsageImpl();

    int64_t CalculateTotalMemory();

    int64_t max_memory_;                   // memory limit size(B)
    volatile int64_t current_memory_;      // current memory usage(B), fetch per second
    int memory_limit_threshold_;           // memory limit threshold
    int64_t total_memory_;                 // total memory(B)
    int interval_sec_;
    std::atomic<bool> is_running_;
    std::atomic<bool> is_initialized_;
    std::chrono::time_point<std::chrono::steady_clock> last_trim_time_;
    std::thread* mem_trim_thread_;
    mutable std::mutex mtx_;
};

}  // namespace vearch