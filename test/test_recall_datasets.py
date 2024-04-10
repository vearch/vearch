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
import logging
from utils.vearch_utils import *
from utils.data_utils import *

logging.basicConfig()
logger = logging.getLogger(__name__)

__description__ = """ test case for index recall of datasets """


def create(router_url, embedding_size, index_type="FLAT", store_type="MemoryOnly", metric_type="L2"):
    properties = {}
    properties["fields"] = [
        {
            "name": "field_int",
            "type": "integer",
        },
        {
            "name": "field_vector",
            "type": "vector",
            "dimension": embedding_size,
            "store_type": store_type,
            "index": {
                "name": "gamma",
                "type": index_type,
                "params": {
                    "metric_type": metric_type
                }
            },
        }
    ]

    space_config = {
        "name": space_name,
        "partition_num": 1,
        "replica_num": 1,
        "fields": properties["fields"]
    }
    response = create_db(router_url, db_name)
    assert response["code"] == 200

    response = create_space(router_url, db_name, space_config)
    assert response["code"] == 200
    logger.info(response["data"]["space_properties"]["field_vector"])


def benchmark(index_type, store_type, metric_type, xb, xq, gt):
    embedding_size = xb.shape[1]
    batch_size = 100
    k = 100

    total = xb.shape[0]
    total_batch = int(total / batch_size)
    logger.info("dataset num: %d, total_batch: %d, dimension: %d, search num: %d, topK: %d" % (
        total, total_batch, embedding_size, xq.shape[0], k))

    create(router_url, embedding_size, index_type, store_type, metric_type)

    add(total_batch, batch_size, xb)
    if total - total_batch * batch_size:
        add(total - total_batch * batch_size, 1, xb[total_batch * batch_size:])

    waiting_index_finish(logger, total, 15)

    query_dict = {
        "vectors": [],
        "vector_value": False,
        "fields": ["field_int"],
        "quick": True,
        "limit": k,
        "db_name": db_name,
        "space_name": space_name,
    }

    for batch in [True, False]:
        avarage, recalls = evaluate(xq, gt, k, batch, query_dict, logger)
        result = "%s batch: %d, search avarage time: %.2f ms, " % (index_type, batch, avarage)
        for recall in recalls:
            result += "recall@%d = %.2f%% " % (recall, recalls[recall] * 100)
        logger.info(result)

        # assert recalls[100] >= 0.95
        assert recalls[10] >= 0.8
        assert recalls[1] >= 0.5

    destroy(router_url, db_name, space_name)


@ pytest.mark.parametrize(["dataset_name", "metric_type"], [
    ["sift", "L2"],
    ["glove", "InnerProduct"],
])
def test_vearch_index_dataset_recall(dataset_name, metric_type):
    xb, xq, gt = get_dataset_by_name(logger, dataset_name)

    index_infos = [
        ["HNSW", "MemoryOnly"],
        ["IVFPQ", "MemoryOnly"],
        ["IVFPQ", "RocksDB"],
        ["IVFFLAT", "RocksDB"],
        ["FLAT", "MemoryOnly"]
    ]
    for index_type, store_type in index_infos:
        benchmark(index_type, store_type, metric_type, xb, xq, gt)
