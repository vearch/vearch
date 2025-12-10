#pragma once

#include <unordered_map>
#include <string>

#include "raw_data.h"
#include "util/log.h"
#include <tbb/concurrent_hash_map.h>

namespace vearch {

enum class RequestStatusCode : std::int16_t {
 UNDEFINED= 0,
 //not used by gamma engine, just keep same order as in request.go
 Running_1,
 Running_2,
 //query canceled or memory exceed
 CANCELED,
 MEMORY_EXCEED
};

struct RequestKey {
    std::string request_id;
    int partition_id;

    bool operator==(const RequestKey& other) const {
        return request_id == other.request_id && partition_id == other.partition_id;
    }
};

struct RequestKeyHash {
    std::size_t operator()(const RequestKey& p) const {
        std::size_t h1 = std::hash<std::string>()(p.request_id);
        std::size_t h2 = std::hash<int>()(p.partition_id);
        return h1 ^ (h2 + 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
    }

    bool operator()(const RequestKey& a, const RequestKey& b) const noexcept {
        return a.request_id == b.request_id && a.partition_id == b.partition_id;
    }

    std::size_t hash(const RequestKey& key) const noexcept {
        return operator()(key);
    }

    bool equal(const RequestKey& a, const RequestKey& b) const noexcept {
        return operator()(a, b);
    }
};

class RequestContext {
 private:
  static thread_local RawData* current_request_;
  static thread_local int partition_id_;

  static tbb::concurrent_hash_map<RequestKey, RequestStatusCode, RequestKeyHash> kill_status_;

  RequestContext() = delete;

 public:
  class ScopedContext {
   public:
    ScopedContext(RawData* req, int partition_id) : request_ptr(req) {            
     current_request_ = req;
     partition_id_ = partition_id;
    }

    ~ScopedContext() {
     current_request_ = nullptr;
     partition_id_ = -1;
    }

    ScopedContext(const ScopedContext&) = delete;
    ScopedContext& operator=(const ScopedContext&) = delete;

   private:
    RawData* request_ptr;
  };

  static RawData* get_current_request();
  static int get_partition_id();

  static void set_kill_status(std::string request_id, int partition_id, int reason);
  static bool is_killed(std::string request_id, int partition_id);
  static bool is_killed();
  static void delete_kill_status(std::string request_id, int partition_id);

};

} // namespace vearch


