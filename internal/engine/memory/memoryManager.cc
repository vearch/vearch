#include "memoryManager.h"
#include <malloc.h>
#include <fstream>
#include <string>
#include <cstdlib>
#include <unistd.h>
#include <iostream>
#include <sstream>
#include "util/log.h"

namespace vearch {

MemoryManager& MemoryManager::GetInstance() {
    static MemoryManager instance;
    return instance;
}

void MemoryManager::StartMonitoring(int interval_sec) {
    total_memory_ = CalculateTotalMemory();
    
    if (total_memory_ <= 0) {
        LOG(ERROR) << "Failed to calculate total memory";
    }

    int64_t limit_bytes = total_memory_ * memory_limit_threshold_ / 100;
    StartMonitoring(limit_bytes, interval_sec); 
}

void MemoryManager::StartMonitoring(int64_t max_memory, int interval_sec) {
    std::lock_guard<std::mutex> lock(mtx_);
    if (is_running_) return;

    max_memory_ = max_memory;
    interval_sec_ = interval_sec;
    is_running_ = true;
    is_initialized_ = true;
    mem_trim_thread_ = new std::thread(&MemoryManager::MonitorThread, this);
}

void MemoryManager::StopMonitoring() {
    std::lock_guard<std::mutex> lock(mtx_);
    if (!is_running_) return;

    is_running_ = false;
    if (mem_trim_thread_ && mem_trim_thread_->joinable()) {
        mem_trim_thread_->join();
        delete mem_trim_thread_;
        mem_trim_thread_ = nullptr;
    }
    is_initialized_ = false;
    LOG(INFO) << "Memory monitoring thread stopped\n";
}

int64_t MemoryManager::CalculateTotalMemory() {
    int64_t meminfo_total = -1;
    int64_t cgroup_limit = -1;

    std::ifstream meminfo("/proc/meminfo");
    if (meminfo.is_open()) {
        std::string line;
        while (std::getline(meminfo, line)) {
            size_t pos = line.find("MemTotal:");
            if (pos != std::string::npos) {
                std::istringstream iss(line.substr(pos + 9));
                iss >> meminfo_total;
                break;
            }
        }
        meminfo.close();
    }
    meminfo_total = meminfo_total * 1024;

    std::ifstream cgroup("/sys/fs/cgroup/memory/memory.limit_in_bytes");
    if (cgroup.is_open()) {
        std::string limit_str;
        std::getline(cgroup, limit_str);
        cgroup_limit = std::stoull(limit_str);
        cgroup.close();
    }

    if (meminfo_total == -1) return cgroup_limit;
    if (cgroup_limit == -1) return meminfo_total;
    return std::min(meminfo_total, cgroup_limit);
}

int64_t MemoryManager::GetCurrentMemoryUsage() {
    return current_memory_;
}

int64_t MemoryManager::GetCurrentMemoryUsageImpl() {
    // first fetch from smaps_rollup
    std::ifstream smaps("/proc/self/smaps_rollup");
    if (smaps.is_open()) {
        std::string line;
        while (std::getline(smaps, line)) {
            size_t pos = line.find("Rss:");
            if (pos != std::string::npos) {
                std::istringstream iss(line.substr(pos + 4));
                int64_t rss;
                iss >> rss;
                return rss * 1024;
            }
        }
        smaps.close();
    }

    // fetch from statm
    std::ifstream statm("/proc/self/statm");
    if (statm.is_open()) {
        int64_t resident_pages;
        statm >> std::ws;
        statm.ignore(1024, ' ');
        statm >> resident_pages;
        
        int64_t page_size = sysconf(_SC_PAGESIZE);
        return resident_pages * page_size;
    }

    return -1;
}

void MemoryManager::TrimMemory() {
    LOG(INFO) << "Executing memory trim\n";
    malloc_trim(0);
}

bool MemoryManager::CheckMemoryUsageExceed(int estimatedSize) {
    if (!is_initialized_ || max_memory_ == 0) {
        return false;
    }
    static int check_count = 0;
    check_count = (check_count + 1) % 10;
    if (check_count == 0) {
        return current_memory_ >= 0 && (current_memory_ + estimatedSize) > max_memory_;
    }

    return false;
}

void MemoryManager::MonitorThread() {
    LOG(INFO) << "Memory monitor thread started\n";
    while (is_running_) {
        if (max_memory_ > 0) {
            current_memory_ = GetCurrentMemoryUsageImpl();

            if (current_memory_ > 0 && current_memory_ > max_memory_) {
                auto now = std::chrono::steady_clock::now();
                auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - last_trim_time_).count();
                if (elapsed >= interval_sec_) {
                    malloc_trim(0);
                    last_trim_time_ = now;
                    LOG(INFO) << "Memory trimmed. Current: " << current_memory_
                          << "B, Limit: " << max_memory_<< "B\n";
                }
            }
        }

        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    LOG(INFO) << "Memory monitor thread exited\n";
}

}  // namespace vearch
    