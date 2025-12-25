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

import pytest
from utils.vearch_utils import *
from utils.data_utils import *
import json

__description__ = """ test case for function of realtime searching """


def create(router_url, index_type, embedding_size, enable_realtime):
    properties = {}
    properties["fields"] = [
        {
            "name": "field_int",
            "type": "integer",
            "index": {
                "name": "field_int",
                "type": "SCALAR",
            },
        },
        {
            "name": "field_vector",
            "type": "vector",
            "dimension": embedding_size,
            "index": {
                "name": "gamma",
                "type": index_type,
                "params": {
                     "metric_type": "L2",
                     "ncentroids": 2048,
                     "nlinks": 32,
                     "efConstruction": 160
                 }
            },
            # "format": "normalization"
        },
    ]

    space_config = {
        "name": space_name,
        "partition_num": 1,
        "replica_num": 1,
        "fields": properties["fields"],
        "enable_realtime": enable_realtime,
    }
    response = create_db(router_url, db_name)
    logger.info(response.json())

    response = create_space(router_url, db_name, space_config)
    logger.info(response.json())
    assert response.status_code == 200
    assert response.json()["code"] == 0


def check_recall(batch, xq, gt, k, enable_realtime):
    query_dict = {
        "vectors": [],
        "vector_value": False,
        "fields": ["field_int"],
        "limit": k,
        "db_name": db_name,
        "space_name": space_name,
    }

    avarage, recalls = evaluate(xq, gt, k, batch, query_dict)
    result = (
        "batch: %d, avg: %.2f ms, " % (batch, avarage)
    )
    for recall in recalls:
        result += "recall@%d = %.2f%% " % (recall, recalls[recall] * 100)

    logger.info(result)
    if enable_realtime:
        assert 1 >= recalls[1] >= 0.6
        assert 1 >= recalls[10] >= 0.9
        assert 1 >= recalls[100] >= 0.95


def check_filter(xq, k, enable_realtime, index_type, check_realtime):
    data = {
        "db_name": db_name,
        "space_name": space_name,
        "limit": k
    }

    data["vectors"] = []
    vector_info = {
        "field": "field_vector",
        "feature": xq[:1].flatten().tolist(),
    }
    data["vectors"].append(vector_info)

    data["filters"] = {
        "operator": "AND",
        "conditions": [
            {
                "field": "field_int",
                "operator": ">=",
                "value": 10001,
            },
        ],
    }

    url = router_url + "/document/search"

    json_str = json.dumps(data)
    response = requests.post(url, auth=(username, password), data=json_str)
    if enable_realtime or index_type == "FLAT" or index_type == "HNSW":
        assert response.status_code == 200
        assert response.json()["code"] == 0
        assert len(response.json()["data"]["documents"]) == 0
    else:
        logger.info("non-realtime search filter >= 10001, response: %s", response.json())

    data["filters"] = {
        "operator": "AND",
        "conditions": [
            {
                "field": "field_int",
                "operator": "<=",
                "value": -1,
            },
        ],
    }

    url = router_url + "/document/search"

    json_str = json.dumps(data)
    response = requests.post(url, auth=(username, password), data=json_str)
    if enable_realtime or index_type == "FLAT" or index_type == "HNSW":
        assert response.status_code == 200
        assert response.json()["code"] == 0
        assert len(response.json()["data"]["documents"]) == 0
    else:
        logger.info("non-realtime search filter <= -1, response: %s", response.json())

    data["filters"] = {
        "operator": "AND",
        "conditions": [
            {
                "field": "field_int",
                "operator": ">=",
                "value": 0,
            },
            {
                "field": "field_int",
                "operator": "<=",
                "value": 100,
            },
        ],
    }

    url = router_url + "/document/search"

    json_str = json.dumps(data)
    response = requests.post(url, auth=(username, password), data=json_str)
    if enable_realtime or index_type == "FLAT" or index_type == "HNSW":
        assert response.status_code == 200
        assert response.json()["code"] == 0
        for documents in  response.json()["data"]["documents"]:
            for document in documents:
                assert 0 <= document["field_int"] <= 100
    else:
        logger.info("non-realtime search filter in [1, 100], response: %s", response.json())

    if enable_realtime and check_realtime:
        data["filters"] = {
            "operator": "AND",
            "conditions": [
                {
                    "field": "field_int",
                    "operator": ">=",
                    "value": 9900,
                },
                {
                    "field": "field_int",
                    "operator": "<=",
                    "value": 10000,
                },
            ],
        }

        url = router_url + "/document/search"

        json_str = json.dumps(data)
        response = requests.post(url, auth=(username, password), data=json_str)
        assert response.status_code == 200
        assert response.json()["code"] == 0
        assert len(response.json()["data"]["documents"]) > 0
        for documents in  response.json()["data"]["documents"]:
            for document in documents:
                assert 9900 <= document["field_int"] <= 10000


def benchmark(index_type, xb, xq, gt, enable_realtime=True, check_realtime=True):
    embedding_size = xb.shape[1]
    batch_size = 100
    k = 100

    total = xb.shape[0]
    total_batch = int(total / batch_size)

    # 9900 - 10000 will be added finally
    if check_realtime:
        total_batch = int(total / batch_size) - 1

    logger.info(
        "dataset num: %d, total_batch: %d, dimension: %d, search num: %d, topK: %d, enable_realtime: %d, check_realtime: %d"
        % (
            total,
            total_batch,
            embedding_size,
            xq.shape[0],
            k,
            enable_realtime,
            check_realtime
        )
    )

    create(router_url, index_type, embedding_size, enable_realtime)

    add(total_batch, batch_size, xb, with_id=True)
    if total - total_batch * batch_size:
        add(1, total - total_batch * batch_size, xb[total_batch * batch_size:], offset=total_batch * batch_size)

    for batch in [True, False]:
        if not enable_realtime and index_type not in ["FLAT", "HNSW"]:
            logger.info("skip non-realtime vearch index %s search test with batch=%s", index_type, batch)
            continue
        check_recall(batch, xq, gt, k, enable_realtime)

    check_filter(xq, k, enable_realtime, index_type, check_realtime)

    destroy(router_url, db_name, space_name)


sift10k = DatasetSift10K()
xb = sift10k.get_database()
xq = sift10k.get_queries()
gt = sift10k.get_groundtruth()

@pytest.mark.parametrize(
    ["enable_realtime", "check_realtime"],
    [
        [True, True],
        [True, False],
        [False, True],
        [False, False],
    ],
)
def test_vearch_index_flat(enable_realtime, check_realtime):
    benchmark("FLAT", xb, xq, gt, enable_realtime, check_realtime)

@pytest.mark.parametrize(
    ["enable_realtime", "check_realtime"],
    [
        [True, True],
        [True, False],
        [False, True],
        [False, False],
    ],
)
def test_vearch_index_ivfpq(enable_realtime, check_realtime):
    benchmark("IVFPQ", xb, xq, gt, enable_realtime, check_realtime)

@pytest.mark.parametrize(
    ["enable_realtime", "check_realtime"],
    [
        [True, True],
        [True, False],
        [False, True],
        [False, False],
    ],
)
def test_vearch_index_ivfflat_l2(enable_realtime, check_realtime):
    benchmark("IVFFLAT", xb, xq, gt, enable_realtime, check_realtime)

@pytest.mark.parametrize(
    ["enable_realtime", "check_realtime"],
    [
        [True, True],
        [True, False],
        [False, True],
        [False, False],
    ],
)
def test_vearch_index_hnsw(enable_realtime, check_realtime):
    benchmark("HNSW", xb, xq, gt, enable_realtime, check_realtime)