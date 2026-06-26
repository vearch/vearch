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

import json
import time

import pytest
import requests

from utils.data_utils import DatasetSift10K
from utils.vearch_utils import (
    add,
    create_db,
    create_space,
    get_space_num,
    logger,
    router_url,
    db_name,
    space_name,
    username,
    password,
    waiting_index_finish_with_timeout,
)

__description__ = """Integration tests for DISKANN static index (full build)."""


def _diskann_fields(embedding_size: int, training_threshold: int):
    return [
        {
            "name": "field_int",
            "type": "integer",
            "index": {"name": "field_int", "type": "SCALAR"},
        },
        {
            "name": "field_vector",
            "type": "vector",
            "dimension": embedding_size,
            "store_type": "RocksDB",
            "index": {
                "name": "gamma",
                "type": "DISKANN_STATIC",
                "params": {
                    "metric_type": "L2",
                    "training_threshold": training_threshold,
                    "R": 32,
                    "L": 64,
                    "num_threads": 2,
                    "beam_width": 4,
                    "num_nodes_to_cache": 100000,
                    "search_dram_budget_gb": 0.5,
                    "build_dram_budget_gb": 0.56,
                    "disk_pq_bytes": 0,
                    "use_opq": 0,
                    "append_reorder_data": 0,
                },
            },
        },
    ]


def _create_diskann_space(router_url: str, embedding_size: int, training_threshold: int):
    space_config = {
        "name": space_name,
        "partition_num": 2,
        "replica_num": 1,
        "enable_realtime": False,
        "fields": _diskann_fields(embedding_size, training_threshold),
    }
    r = create_db(router_url, db_name)
    logger.info(r.json())
    assert r.json()["code"] == 0
    r = create_space(router_url, db_name, space_config)
    logger.info(r.json())
    assert r.json()["code"] == 0


def _space_exists() -> bool:
    url = f"{router_url}/dbs/{db_name}/spaces/{space_name}"
    rs = requests.get(url, auth=(username, password))
    if rs.status_code != 200:
        return False
    body = rs.json()
    return body.get("code") == 0


def _wait_space_ready_after_index(total_docs: int, max_rounds: int = 60, timewait: int = 5):
    """Wait until space status is not red and partitions reach INDEXED (index_status==2)."""
    url = f"{router_url}/dbs/{db_name}/spaces/{space_name}?detail=true"
    for round_i in range(max_rounds):
        rs = requests.get(url, auth=(username, password))
        assert rs.status_code == 200
        body = rs.json()
        assert body["code"] == 0, body
        data = body.get("data", {})
        status = data.get("status", "")
        partitions = data.get("partitions", [])
        index_statuses = [p.get("index_status", -1) for p in partitions]
        logger.info(
            "space readiness round=%d status=%s index_statuses=%s",
            round_i,
            status,
            index_statuses,
        )
        if status != "red" and len(partitions) > 0 and all(s == 2 for s in index_statuses):
            return
        time.sleep(timewait)
    assert False, f"space not ready after {max_rounds} rounds"


def _get_space_detail() -> dict:
    url = f"{router_url}/dbs/{db_name}/spaces/{space_name}?detail=true"
    rs = requests.get(url, auth=(username, password))
    assert rs.status_code == 200
    body = rs.json()
    assert body["code"] == 0, body
    return body.get("data", {})

def _force_merge(partition_id: int = 0) -> dict:
    """Trigger a one-shot index build via /index/forcemerge."""
    url = f"{router_url}/index/forcemerge"
    payload = {"db_name": db_name, "space_name": space_name, "partition_id": partition_id}
    rs = requests.post(url, auth=(username, password), json=payload)
    assert rs.status_code == 200, rs.text
    body = rs.json()
    assert body.get("code") == 0, body
    return body


sift10k = DatasetSift10K()
xb = sift10k.get_database()
xq = sift10k.get_queries()
gt = sift10k.get_groundtruth()


@pytest.mark.diskann_static
def test_vearch_index_diskann_static_search():
    """
    Load data, explicitly trigger DiskANN full build (static index doesn't auto-build),
    wait for index_status, then vector search.
    """
    embedding_size = xb.shape[1]
    batch_size = 100
    total_docs = xb.shape[0]
    training_threshold = total_docs
    total_batch = total_docs // batch_size

    if _space_exists():
        logger.info(
            "space %s/%s already exists, skip create+insert and search directly",
            db_name,
            space_name,
        )
        detail = _get_space_detail()
        doc_num = detail.get("doc_num", 0)
        partitions = detail.get("partitions", [])
        index_num = sum(p.get("index_num", 0) for p in partitions)

        if index_num < min(total_docs, doc_num):
            logger.info(
                "space exists but index not ready (index_num=%d doc_num=%d), trigger force merge once",
                index_num,
                doc_num,
            )
            logger.info(_force_merge())
        _wait_space_ready_after_index(total_docs, max_rounds=180, timewait=10)
        assert doc_num >= total_docs, (
            f"existing space doc_num={doc_num} is less than expected {total_docs}, "
            "cannot evaluate recall against SIFT10K groundtruth"
        )
    else:
        _create_diskann_space(router_url, embedding_size, training_threshold)
        add(total_batch, batch_size, xb, with_id=True, full_field=False)
        assert get_space_num() == total_docs
        time.sleep(10)
        logger.info(_force_merge())
        waiting_index_finish_with_timeout(
            total_docs, timewait=20, max_rounds=180
        )
        _wait_space_ready_after_index(total_docs, max_rounds=36, timewait=5)

    data = {
        "db_name": db_name,
        "space_name": space_name,
        "limit": 100,
        "vectors": [
            {
                "field": "field_vector",
                "feature": xb[0].flatten().tolist(),
            }
        ],
        "index_params": {
            "l_search": 100,
            "beam_width": 4,
            "metric_type": "L2",
        },
    }
    url = router_url + "/document/search"
    rs = requests.post(url, auth=(username, password), data=json.dumps(data))
    assert rs.status_code == 200
    body = rs.json()
    assert body["code"] == 0, body
    assert len(body["data"]["documents"][0]) >= 1

    # Compute and print average latency and recall metrics on a small SIFT query subset.
    eval_nq = min(100, xq.shape[0])
    hit_at_1 = 0
    hit_at_10 = 0
    hit_at_100 = 0
    total_latency_ms = 0.0
    for i in range(eval_nq):
        q = {
            "db_name": db_name,
            "space_name": space_name,
            "limit": 100,
            "vectors": [
                {
                    "field": "field_vector",
                    "feature": xq[i].flatten().tolist(),
                }
            ],
            "index_params": {
                "l_search": 100,
                "beam_width": 4,
                "metric_type": "L2",
            },
            "fields": ["field_int"],
        }
        start_ms = time.time()
        qrs = requests.post(url, auth=(username, password), data=json.dumps(q))
        total_latency_ms += (time.time() - start_ms) * 1000.0
        assert qrs.status_code == 200
        qbody = qrs.json()
        assert qbody["code"] == 0, qbody
        docs = qbody["data"]["documents"][0]
        preds = [int(d["field_int"]) for d in docs if "field_int" in d]
        true_nn = int(gt[i][0])
        if len(preds) >= 1 and preds[0] == true_nn:
            hit_at_1 += 1
        if true_nn in preds[:10]:
            hit_at_10 += 1
        if true_nn in preds[:100]:
            hit_at_100 += 1

    avg_latency_ms = total_latency_ms / float(eval_nq)
    recall_at_1 = hit_at_1 / float(eval_nq)
    recall_at_10 = hit_at_10 / float(eval_nq)
    recall_at_100 = hit_at_100 / float(eval_nq)
    logger.info(
        "DISKANN_STATIC SIFT10K compare: avg=%.2fms recall@1=%.4f recall@10=%.4f recall@100=%.4f",
        avg_latency_ms,
        recall_at_1,
        recall_at_10,
        recall_at_100,
    )
