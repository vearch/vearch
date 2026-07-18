/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include <string>

#include "c_api/api_data/request_context.h"

namespace test {

using vearch::RequestContext;
using vearch::RequestStatusCode;

namespace {

constexpr int kCanceled = static_cast<int>(RequestStatusCode::CANCELED);
constexpr int kMemoryExceed = static_cast<int>(RequestStatusCode::MEMORY_EXCEED);
constexpr int kRunning = static_cast<int>(RequestStatusCode::Running_1);

// RawData is abstract; a minimal concrete subclass lets us drive a
// ScopedContext so the no-arg is_killed() has a current request to resolve.
class TestRawData : public vearch::RawData {
 public:
  int Serialize(char **, int *) override { return 0; }
  void Deserialize(const char *, int) override {}
};

}  // namespace

// ---------------------------------------------------------------------------
// Two-arg kill-status map: set / query / delete lifecycle.
// ---------------------------------------------------------------------------
TEST(RequestContextTest, KillStatusLifecycle) {
  const std::string id = "req-lifecycle";
  const int pid = 3;
  RequestContext::delete_kill_status(id, pid);  // start from a clean slate

  EXPECT_FALSE(RequestContext::is_killed(id, pid));

  RequestContext::set_kill_status(id, pid, kCanceled);
  EXPECT_TRUE(RequestContext::is_killed(id, pid));

  // Different (request_id, partition_id) keys are independent.
  EXPECT_FALSE(RequestContext::is_killed(id, pid + 1));
  EXPECT_FALSE(RequestContext::is_killed("other", pid));

  RequestContext::delete_kill_status(id, pid);
  EXPECT_FALSE(RequestContext::is_killed(id, pid));
}

TEST(RequestContextTest, MemoryExceedCountsAsKilled) {
  const std::string id = "req-mem";
  const int pid = 0;
  RequestContext::set_kill_status(id, pid, kMemoryExceed);
  EXPECT_TRUE(RequestContext::is_killed(id, pid));
  RequestContext::delete_kill_status(id, pid);
}

TEST(RequestContextTest, RunningStatusIsNotKilled) {
  const std::string id = "req-running";
  const int pid = 0;
  // set_kill_status records the entry regardless of reason, but is_killed only
  // treats CANCELED / MEMORY_EXCEED as killed, so a Running_* reason is not.
  RequestContext::set_kill_status(id, pid, kRunning);
  EXPECT_FALSE(RequestContext::is_killed(id, pid));
  RequestContext::delete_kill_status(id, pid);
}

// ---------------------------------------------------------------------------
// No-arg is_killed() resolves through the thread-local ScopedContext.
// ---------------------------------------------------------------------------
TEST(RequestContextTest, NoArgIsKilledFalseWithoutContext) {
  // No ScopedContext is active on this thread, so current_request_ is null.
  EXPECT_FALSE(RequestContext::is_killed());
}

TEST(RequestContextTest, NoArgIsKilledUsesScopedContext) {
  const std::string id = "req-scoped";
  const int pid = 5;
  TestRawData req;
  req.SetRequestId(id);

  {
    RequestContext::ScopedContext ctx(&req, pid);
    EXPECT_FALSE(RequestContext::is_killed());  // not killed yet
    RequestContext::set_kill_status(id, pid, kCanceled);
    EXPECT_TRUE(RequestContext::is_killed());
  }
  // ScopedContext torn down: current_request_ reset, so no longer resolvable.
  EXPECT_FALSE(RequestContext::is_killed());

  RequestContext::delete_kill_status(id, pid);
}

// ---------------------------------------------------------------------------
// is_killed_every<Stride>(): the amortized hot-loop kill-check.
// ---------------------------------------------------------------------------
TEST(RequestContextTest, IsKilledEveryGatesByStride) {
  const std::string id = "req-stride";
  const int pid = 1;
  TestRawData req;
  req.SetRequestId(id);
  RequestContext::ScopedContext ctx(&req, pid);
  RequestContext::set_kill_status(id, pid, kCanceled);

  // At a stride boundary (i % Stride == 0, including i == 0) the kill is seen.
  EXPECT_TRUE(RequestContext::is_killed_every<1024>(0));
  EXPECT_TRUE(RequestContext::is_killed_every<1024>(1024));
  EXPECT_TRUE(RequestContext::is_killed_every<1024>(2048));
  EXPECT_TRUE(RequestContext::is_killed_every<512>(512));
  EXPECT_TRUE(RequestContext::is_killed_every<256>(256));

  // Off boundary: gated out, returns false even though the request is killed.
  EXPECT_FALSE(RequestContext::is_killed_every<1024>(1));
  EXPECT_FALSE(RequestContext::is_killed_every<1024>(1023));
  EXPECT_FALSE(RequestContext::is_killed_every<1024>(1025));
  EXPECT_FALSE(RequestContext::is_killed_every<512>(256));  // 256 % 512 != 0
  EXPECT_FALSE(RequestContext::is_killed_every<256>(255));

  RequestContext::delete_kill_status(id, pid);
}

TEST(RequestContextTest, IsKilledEveryFalseWhenNotKilled) {
  const std::string id = "req-stride-alive";
  const int pid = 2;
  TestRawData req;
  req.SetRequestId(id);
  RequestContext::ScopedContext ctx(&req, pid);
  RequestContext::delete_kill_status(id, pid);  // ensure not killed

  // Even at a stride boundary, an un-killed request is never reported killed.
  EXPECT_FALSE(RequestContext::is_killed_every<1024>(0));
  EXPECT_FALSE(RequestContext::is_killed_every<1024>(1024));
}

}  // namespace test

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
