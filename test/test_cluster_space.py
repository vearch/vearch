#
# Copyright 2019 The Vearch Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.

# -*- coding: UTF-8 -*-

import requests
import json
import pytest
from utils.vearch_utils import *
from utils.data_utils import *

__description__ = """cluster test cases that span a PS restart.

These cases cannot run against a single standalone instance: each is split
into a Prepare class and a Verify class that run in SEPARATE pytest
invocations, with CI restarting a PS (docker-compose stop/start ps1) in
between. Running them in one process (or without the restart) makes the
Verify half fail, which is why they live here and not in
test_module_space_indexes.py. See CI_cluster_ps.yml for the driver.
"""


def _index_states_from_describe(db, space, index_name):
    """Collect the build state of `index_name` across all partitions.

    Returns a list of state strings (one per partition that reports the
    index). An empty list means no partition reported it yet.
    """
    resp = describe_space(router_url, db, space)
    body = resp.json()
    states = []
    for p in body.get("data", {}).get("partitions", []):
        ibs = p.get("index_build_state") or {}
        if index_name in ibs:
            states.append(ibs[index_name])
    return states


def _wait_index_ready(db, space, index_name, timeout=60):
    """Poll describe until every partition reporting the index is READY.

    NOTE: describe reports one entry per partition, taken from that partition's
    LEADER only (master DescribeSpace queries partition.LeaderID, not every
    replica). So this observes the leader's build state, not "every replica".
    A restarted follower's recovery is exercised (it rejoins raft and reloads)
    but its per-replica state is not directly asserted here. For all-replica
    build state use GET /indexes?detail=true (see TestListIndexesDetail*).

    Returns True once all reported states are READY (and at least one
    partition reported it); False on timeout.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        states = _index_states_from_describe(db, space, index_name)
        if states and all(s == "READY" for s in states):
            return True
        time.sleep(1)
    return False


def _skip_if_space_absent(db, space):
    """Prepare/Verify run in separate pytest invocations. If Prepare skipped
    (e.g. an index type unsupported on this build), the space never got created
    and Verify must skip too instead of failing on a missing space."""
    resp = describe_space(router_url, db, space)
    if resp.json().get("code", -1) != 0:
        pytest.skip(f"space {db}/{space} absent (Prepare likely skipped)")


# ---------------------------------------------------------------------------
# Crash-recovery of a dynamically-added index (split across a PS restart).
#
# These two classes are NOT run back-to-back in one process. CI drives:
#   pytest -k TestIndexCrashRecoveryPrepare   # create + seed + fire add-index
#   docker-compose stop ps1                   # kill mid-backfill
#   docker-compose start ps1                  # restart → Engine::Load recovery
#   pytest -k TestIndexCrashRecoveryVerify    # assert READY + complete results
#
# The PS data dir lives in the container's writable layer (only config is bind
# mounted), so stop/start preserves the persisted BUILDING marker that
# Engine::Load reads to drop-and-rebuild a half-built inverted/composite index.
# The classes share hardcoded db/space names (not instance state) precisely
# because they run in different processes against the surviving cluster.
# ---------------------------------------------------------------------------

RECOVERY_DB = "ts_db_idx_recovery"
RECOVERY_SPACE = "ts_space_idx_recovery"
RECOVERY_INDEX = "idx_int_recovery"
RECOVERY_EMBEDDING = 32
# Large enough that the backfill is still running when CI issues stop ps1,
# so the kill lands mid-build and exercises the drop-and-rebuild recovery path
# rather than the trivial already-READY case.
RECOVERY_DOC_COUNT = 50000


class TestIndexCrashRecoveryPrepare:
    def test_prepare_db(self):
        response = create_db(router_url, RECOVERY_DB)
        assert response.json()["code"] == 0

    def test_prepare_space(self):
        space_config = {
            "name": RECOVERY_SPACE,
            "partition_num": 1,
            # replica_num=3 puts a replica on every PS, so CI's `stop ps1`
            # reliably kills a replica that is mid-backfill (regardless of
            # which PS the single partition's leader landed on).
            "replica_num": 3,
            "fields": [
                {"name": "field_int", "type": "integer"},
                {
                    "name": "field_vector",
                    "type": "vector",
                    "dimension": RECOVERY_EMBEDDING,
                    "index": {"name": "gamma", "type": "FLAT"},
                },
            ],
        }
        response = create_space(router_url, RECOVERY_DB, space_config)
        assert response.json()["code"] == 0

    def test_seed_documents(self):
        url = router_url + "/document/upsert"
        batch = 500
        for base in range(0, RECOVERY_DOC_COUNT, batch):
            docs = [{
                "_id": str(i),
                "field_int": i,
                "field_vector": [random.random() for _ in range(RECOVERY_EMBEDDING)],
            } for i in range(base, min(base + batch, RECOVERY_DOC_COUNT))]
            resp = requests.post(url, auth=(username, password), json={
                "db_name": RECOVERY_DB,
                "space_name": RECOVERY_SPACE,
                "documents": docs,
            })
            assert resp.json()["code"] == 0, resp.json()
        time.sleep(2)

    def test_fire_add_index(self):
        # Request the index and return immediately. CI stops ps1 right after
        # this class finishes, aiming to interrupt the backfill in progress.
        # We intentionally do NOT wait for READY here.
        resp = add_space_indexes(router_url, RECOVERY_DB, RECOVERY_SPACE, [{
            "name": RECOVERY_INDEX,
            "type": "SCALAR",
            "field_name": "field_int",
        }])
        assert resp.json()["code"] == 0, resp.json()


class TestIndexCrashRecoveryVerify:
    def test_index_recovers_to_ready(self):
        # After restart, Engine::Load either finished the pre-crash build or
        # finds the BUILDING marker and rebuilds. Either way it must converge
        # to READY. Allow a generous window for a full rebuild of the dataset.
        assert _wait_index_ready(RECOVERY_DB, RECOVERY_SPACE, RECOVERY_INDEX,
                                 timeout=180), \
            "index did not recover to READY after restart"

    def test_filter_complete_after_recovery(self):
        # The recovered index must return every doc — a half-built index left
        # trusted-as-persisted would silently miss the un-backfilled tail.
        url = router_url + "/document/query"
        data = {
            "db_name": RECOVERY_DB,
            "space_name": RECOVERY_SPACE,
            "filters": {
                "operator": "AND",
                "conditions": [
                    {"field": "field_int", "operator": ">=", "value": 0},
                    {"field": "field_int", "operator": "<=",
                     "value": RECOVERY_DOC_COUNT - 1},
                ],
            },
            "limit": RECOVERY_DOC_COUNT,
        }
        resp = requests.post(url, auth=(username, password), json=data)
        body = resp.json()
        assert body.get("code", -1) == 0, body
        ids = {d["_id"] for d in body.get("data", {}).get("documents", [])}
        expected = {str(i) for i in range(RECOVERY_DOC_COUNT)}
        missing = expected - ids
        assert not missing, \
            f"{len(missing)} docs missing after recovery " \
            f"(e.g. {sorted(missing, key=int)[:10]})"

    def test_destroy_space(self):
        response = drop_space(router_url, RECOVERY_DB, RECOVERY_SPACE)
        assert response.json()["code"] == 0

    def test_destroy_db(self):
        drop_db(router_url, RECOVERY_DB)


# ---------------------------------------------------------------------------
# Per-replica index build state via GET /indexes?detail=true.
#
# index build state is per-replica local (each PS applies the index change and
# builds independently), so /indexes?detail=true fans out to every replica and
# reports each one's own state. The describe API only queries the leader and
# cannot show this. These classes cover, on a replica_num=3 space:
#   - structure + is_leader: each partition reports exactly ReplicaNum replicas,
#     exactly one flagged is_leader (Prepare).
#   - BUILDING observability: states are collected right after a dynamic add and
#     must converge to READY through only legal values (Prepare). BUILDING is a
#     brief transient, so it is logged if seen but not required (a fast IVFPQ
#     train can skip the poll window).
#   - unreachable replica: after CI stops ps1, the stopped replica's entry must
#     carry a non-empty error while the survivors still report normally and the
#     call as a whole stays code==0 (Verify).
#
# Split into Prepare/Verify because the error branch needs a PS stopped between
# two pytest invocations, driven by CI (mirrors the crash-recovery classes).
# ---------------------------------------------------------------------------

LID_DB = "ts_db_list_idx_detail"
LID_SPACE = "ts_space_list_idx_detail"
LID_EMBEDDING = 16
# 10k docs > IVFPQ training_threshold (1500) so the index trains; large enough
# that the build is not instantaneous, giving the Prepare poll a chance to catch
# a BUILDING state (not required, just more likely).
LID_DOC_COUNT = 10000
LID_REPLICA_NUM = 3
LID_VEC_INDEX = "gamma"
LID_SCALAR_INDEX = "idx_int_lid"


def _list_indexes_detail(db, space):
    """GET /indexes?detail=true, returning the parsed data dict (or None)."""
    resp = list_space_indexes(router_url, db, space, detail=True)
    body = resp.json()
    if body.get("code", -1) != 0:
        return None
    return body.get("data", {})


class TestListIndexesDetailPrepare:
    def test_prepare_db(self):
        response = create_db(router_url, LID_DB)
        assert response.json()["code"] == 0

    def test_prepare_space(self):
        # IVFPQ so training takes real work (observable BUILDING window);
        # replica_num=3 puts a replica on every PS. A scalar index is created
        # inline too so the space starts with a mix. training_threshold=1500 <
        # doc_count and nprobe<=ncentroids clear both the Go and engine gates.
        space_config = {
            "name": LID_SPACE,
            "partition_num": 1,
            "replica_num": LID_REPLICA_NUM,
            "fields": [
                {"name": "field_int", "type": "integer",
                 "index": {"name": LID_SCALAR_INDEX, "type": "SCALAR"}},
                # An unindexed field reserved for the dynamic add below. A field
                # may hold only one single-field index, so the dynamically-added
                # index must target a field that has none yet.
                {"name": "field_int_dyn", "type": "integer"},
                {
                    "name": "field_vector",
                    "type": "vector",
                    "dimension": LID_EMBEDDING,
                    "index": {
                        "name": LID_VEC_INDEX,
                        "type": "IVFPQ",
                        "params": {"ncentroids": 32, "nsubvector": 8,
                                   "training_threshold": 1500, "nprobe": 16},
                    },
                },
            ],
        }
        resp = create_space(router_url, LID_DB, space_config)
        if resp.json().get("code", -1) != 0:
            pytest.skip(f"IVFPQ 3-replica space not supported here: {resp.json()}")

    def test_seed(self):
        url = router_url + "/document/upsert"
        batch = 500
        for base in range(0, LID_DOC_COUNT, batch):
            docs = [{
                "_id": str(i),
                "field_int": i,
                "field_int_dyn": i,
                "field_vector": [random.random() for _ in range(LID_EMBEDDING)],
            } for i in range(base, min(base + batch, LID_DOC_COUNT))]
            resp = requests.post(url, auth=(username, password), json={
                "db_name": LID_DB, "space_name": LID_SPACE,
                "documents": docs,
            })
            assert resp.json()["code"] == 0, resp.json()
        time.sleep(2)

    def test_add_index_and_observe_states(self):
        # Add an index on the reserved unindexed field so there is a fresh build
        # to observe (the inline indexes may already be READY by now).
        resp = add_space_indexes(router_url, LID_DB, LID_SPACE, [{
            "name": "idx_int_lid_dyn",
            "type": "SCALAR",
            "field_name": "field_int_dyn",
        }])
        assert resp.json()["code"] == 0, resp.json()

        # Poll /indexes?detail=true. Collect every per-replica state seen for the
        # dynamically-added index across all partitions/replicas; require it to
        # converge to READY on every replica, through only legal state values.
        seen = set()
        deadline = time.time() + 180
        all_ready = False
        while time.time() < deadline:
            data = _list_indexes_detail(LID_DB, LID_SPACE)
            if data and data.get("build_state"):
                states = []
                for pbs in data["build_state"]:
                    for r in pbs.get("replicas") or []:
                        st = (r.get("index_build_state") or {}).get("idx_int_lid_dyn")
                        if st is not None:
                            states.append(st)
                seen.update(states)
                # Every replica of every partition reports it and all are READY.
                expected_reports = len(data["build_state"]) * LID_REPLICA_NUM
                if len(states) == expected_reports and all(s == "READY" for s in states):
                    all_ready = True
                    break
            time.sleep(2)

        assert all_ready, f"dynamic index did not reach READY on all replicas; seen={seen}"
        assert seen <= {"BUILDING", "READY", "FAILED"}, \
            f"unexpected state value(s): {seen}"
        assert "FAILED" not in seen, "index build reported FAILED on some replica"
        if "BUILDING" in seen:
            logger.info("observed BUILDING state on at least one replica")
        else:
            logger.info("BUILDING not observed (index trained faster than poll window)")

    def test_structure_and_is_leader(self):
        # With every replica READY, assert the structural contract: each
        # partition reports exactly ReplicaNum replicas, exactly one is_leader,
        # each with a resolvable node_id and no error.
        data = _list_indexes_detail(LID_DB, LID_SPACE)
        assert data is not None, "list indexes ?detail=true failed"
        build_state = data.get("build_state")
        assert build_state, f"detail=true must return build_state: {data}"

        for pbs in build_state:
            replicas = pbs.get("replicas") or []
            assert len(replicas) == LID_REPLICA_NUM, \
                f"partition {pbs.get('pid')} expected {LID_REPLICA_NUM} replicas, got {replicas}"
            leaders = [r for r in replicas if r.get("is_leader")]
            assert len(leaders) == 1, \
                f"partition {pbs.get('pid')} must have exactly one leader, got {leaders}"
            node_ids = {r.get("node_id") for r in replicas}
            assert len(node_ids) == LID_REPLICA_NUM and None not in node_ids, \
                f"partition {pbs.get('pid')} replica node_ids not distinct/resolved: {replicas}"
            for r in replicas:
                assert not r.get("error"), \
                    f"reachable replica should have no error: {r}"


class TestListIndexesDetailVerify:
    def test_stopped_replica_reports_error(self):
        # CI has stopped ps1. The stopped replica's entry must carry a non-empty
        # error, while the surviving replicas still report normally and the call
        # as a whole succeeds (code==0) — the whole point is that one dead node
        # does not blind the operator to the others.
        _skip_if_space_absent(LID_DB, LID_SPACE)

        data = _list_indexes_detail(LID_DB, LID_SPACE)
        assert data is not None, "list indexes ?detail=true failed while a PS is down"
        build_state = data.get("build_state")
        assert build_state, f"detail=true must return build_state: {data}"

        errored = 0
        healthy = 0
        for pbs in build_state:
            replicas = pbs.get("replicas") or []
            assert len(replicas) == LID_REPLICA_NUM, \
                f"partition {pbs.get('pid')} should still list all {LID_REPLICA_NUM} replicas, got {replicas}"
            for r in replicas:
                if r.get("error"):
                    errored += 1
                else:
                    healthy += 1
        # At least one replica (the stopped ps1) must be errored, and at least
        # one must still report cleanly.
        assert errored >= 1, f"expected the stopped replica to report an error: {build_state}"
        assert healthy >= 1, f"expected surviving replicas to report normally: {build_state}"


class TestListIndexesDetailCleanup:
    # Teardown runs as its own -k selection AFTER CI restarts ps1, so the drop
    # happens with all replicas up (mirrors every other cluster step, which
    # starts the stopped PS before dropping). Tolerant of a skipped Prepare.
    def test_destroy_space(self):
        drop_space(router_url, LID_DB, LID_SPACE)

    def test_destroy_db(self):
        drop_db(router_url, LID_DB)
