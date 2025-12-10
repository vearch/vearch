#include "request_context.h"

namespace vearch {

thread_local RawData* RequestContext::current_request_ = nullptr;
thread_local int RequestContext::partition_id_ = -1;
tbb::concurrent_hash_map<RequestKey, RequestStatusCode, RequestKeyHash> RequestContext::kill_status_;

RawData* RequestContext::get_current_request() {
  return current_request_;
}

int RequestContext::get_partition_id() {
  return partition_id_;
}

void RequestContext::set_kill_status(std::string request_id, int partition_id, int reason) {
  RequestStatusCode status = RequestStatusCode(reason);
  if (status == RequestStatusCode::CANCELED || status == RequestStatusCode::MEMORY_EXCEED) {
    LOG(WARNING) << "Request " << request_id << " for partition " << partition_id
                 << " killed with request status code " << static_cast<int>(status);
  }
  RequestKey key = {request_id, partition_id};
  kill_status_.insert({key, status});
}

bool RequestContext::is_killed(std::string request_id, int partition_id) {
  RequestKey key = {request_id, partition_id};
  tbb::concurrent_hash_map<RequestKey, RequestStatusCode, RequestKeyHash>::const_accessor accessor;
  if (kill_status_.find(accessor, key)) {
    return accessor->second == RequestStatusCode::CANCELED || accessor->second == RequestStatusCode::MEMORY_EXCEED;
  } else {
    return false;
  }
}

bool RequestContext::is_killed() {
  if (current_request_ == nullptr) {
    return false;
  }

  std::string request_id = current_request_->RequestId();
  int partition_id = partition_id_;
  return is_killed(request_id, partition_id);
}

void RequestContext::delete_kill_status(std::string request_id, int partition_id) {
  RequestKey key = {request_id, partition_id};
  tbb::concurrent_hash_map<RequestKey, RequestStatusCode, RequestKeyHash>::accessor accessor;
  if (kill_status_.find(accessor, key)) {
    kill_status_.erase(accessor);
  }
}

} // namespace vearch