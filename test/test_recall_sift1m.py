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

__description__ = """ test case for index recall of sift1M """


def create(router_url, embedding_size, index_type="FLAT", store_type="MemoryOnly"):
    training_threshold = 1
    ncentroids = 1024
    if index_type == "IVFFLAT" or index_type == "IVFPQ":
        training_threshold = ncentroids * 39
    
    properties = {}
    properties["fields"] = {
        "field_int": {
            "type": "integer",
            "index": False
        },
        "field_vector": {
            "type": "vector",
            "index": True,
            "dimension": embedding_size,
            "store_type": store_type,
            #"format": "normalization"
        }
    }

    space_config = {
        "name": space_name,
        "partition_num": 1,
        "replica_num": 1,
        "index": {
            "index_name": "gamma",
            "index_type": index_type,
            "index_params": {
                "metric_type": "L2"
            }
        },
        "fields": properties["fields"]
    }
    logger.info(create_db(router_url, db_name))

    logger.info(create_space(router_url, db_name, space_config))


def benchmark(index_type, store_type, xb, xq, xt, gt):
    embedding_size = xb.shape[1]
    batch_size = 100
    k = 100

    total = xb.shape[0]
    total_batch = int(total / batch_size)
    logger.info("dataset num: %d, total_batch: %d, dimension: %d, search num: %d, topK: %d" %(total, total_batch, embedding_size, xq.shape[0], k))

    create(router_url, embedding_size, index_type, store_type)

    add(total_batch, batch_size, xb)

    waiting_index_finish(logger, total)

    query_dict = {
        "query": {
            "vector": []
        },
        "vector_value":False,
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
            if recall == k:
                assert recalls[recall] >= 0.98
        logger.info(result)

    destroy(router_url, db_name, space_name)

xb, xq, xt, gt = get_sift1M(logger)


@ pytest.mark.parametrize(["index_type", "store_type"], [
    ["HNSW", "MemoryOnly"],
    ["IVFPQ", "MemoryOnly"],
    ["IVFPQ", "RocksDB"],
    ["IVFFLAT", "RocksDB"],
    ["FLAT", "MemoryOnly"]
])
def test_vearch_index_recall(index_type: str, store_type: str):
    benchmark(index_type, store_type, xb, xq, xt, gt)