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

__description__ = """ test case for index recall of glove-100 """


def create(router_url, embedding_size, index_type="FLAT", store_type="MemoryOnly"):
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
                    "metric_type": "InnerProduct"
                }
            },
            # "format": "normalization"
        }
    ]

    space_config = {
        "name": space_name,
        "partition_num": 1,
        "replica_num": 1,
        "fields": properties["fields"]
    }
    logger.info(create_db(router_url, db_name))

    logger.info(create_space(router_url, db_name, space_config))


def benchmark(index_type, store_type, xb, xq, gt):
    embedding_size = xb.shape[1]
    batch_size = 100
    k = 100

    total = xb.shape[0]
    total_batch = int(total / batch_size)
    logger.info("dataset num: %d, total_batch: %d, dimension: %d, search num: %d, topK: %d" % (
        total, total_batch, embedding_size, xq.shape[0], k))

    create(router_url, embedding_size, index_type, store_type)

    add(total_batch, batch_size, xb)

    add(total - total_batch * batch_size, 1, xb[total_batch * batch_size:])

    waiting_index_finish(logger, total)

    query_dict = {
        "vectors": [],
        "vector_value": False,
        "index_params": {
            "efSearch": 200
        },
        "fields": ["field_int"],
        "quick": True,
        "size": k,
        "db_name": db_name,
        "space_name": space_name,
    }

    for batch in [True, False]:
        avarage, recalls = evaluate(xq, gt, k, batch, query_dict, logger)
        result = "batch: %d, search avarage time: %.2f ms, " % (batch, avarage)
        for recall in recalls:
            result += "recall@%d = %.2f%% " % (recall, recalls[recall] * 100)
        logger.info(result)

        # assert recalls[100] >= 0.98
        # assert recalls[10] >= 0.95
        assert recalls[1] >= 0.5

    destroy(router_url, db_name, space_name)


glove = DatasetGlove(logger)
xb = glove.get_database()
xq = glove.get_queries()
gt = glove.get_groundtruth()


@ pytest.mark.parametrize(["index_type", "store_type"], [
    ["HNSW", "MemoryOnly"],
    # ["IVFPQ", "MemoryOnly"],
    # ["IVFPQ", "RocksDB"],
    # ["IVFFLAT", "RocksDB"],
    # ["FLAT", "MemoryOnly"]
])
def test_vearch_index_recall_glove(index_type: str, store_type: str):
    benchmark(index_type, store_type, xb, xq, gt)
