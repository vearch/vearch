/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

namespace vearch {

// Build state of a dynamically-added index. Shared by the scalar and vector
// index managers so both report through the same EngineStatus surface. A newly
// added index is registered as BUILDING, becomes READY once its build (scalar:
// publish-then-backfill; vector: create+train+swap) completes, or FAILED if the
// build is rolled back. Values are stringified verbatim into the describe API —
// keep in sync with IndexStateToString.
//
// Note on query gating: the scalar manager additionally treats non-READY
// indexes as absent during queries (its publish-then-backfill exposes a
// half-built index). The vector manager does NOT need that — it swaps the fully
// built index in atomically under a write lock, so search never sees a partial
// one. Here the state is observability only for the vector side.
enum class IndexState : int { BUILDING = 0, READY = 1, FAILED = 2 };

inline const char *IndexStateToString(IndexState s) {
  switch (s) {
    case IndexState::BUILDING:
      return "BUILDING";
    case IndexState::READY:
      return "READY";
    case IndexState::FAILED:
      return "FAILED";
    default:
      return "UNKNOWN";
  }
}

}  // namespace vearch
