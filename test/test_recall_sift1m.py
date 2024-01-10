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
from vearch_utils import *

logging.basicConfig()
logger = logging.getLogger(__name__)

__description__ = """ test case for index recall of sift1M """


def create(router_url, embedding_size, retrieval_type="FLAT", store_type="MemoryOnly"):
    index_size = 1
    ncentroids = 1024
    if retrieval_type == "IVFFLAT" or retrieval_type == "IVFPQ":
        index_size = ncentroids * 39
    
    properties = {}
    properties["properties"] = {
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
        "engine": {
            "name": "gamma",
            "index_size": index_size,
            "retrieval_type": retrieval_type,
            "retrieval_param": {
                "metric_type": "L2",
                "ncentroids": ncentroids,
                "nsubvector": int(embedding_size / 4),
                "nlinks": 32,
                "efConstruction": 160,
                "efSearch": 64
            }
        },
        "properties": properties["properties"]
    }
    logger.info(create_db(router_url, db_name))

    logger.info(create_space(router_url, db_name, space_config))


def benchmark(retrieval_type, store_type, xb, xq, xt, gt):
    embedding_size = xb.shape[1]
    batch_size = 100
    k = 100

    total = xb.shape[0]
    total_batch = int(total / batch_size)
    logger.info("dataset num: %d, total_batch: %d, dimension: %d, search num: %d, topK: %d" %(total, total_batch, embedding_size, xq.shape[0], k))

    create(router_url, embedding_size, retrieval_type, store_type)

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
        logger.info(result)

    destroy(router_url, db_name, space_name)

xb, xq, xt, gt = get_sift1M(logger)


@ pytest.mark.parametrize(["retrieval_type", "store_type"], [
    ["HNSW", "MemoryOnly"],
    ["IVFPQ", "MemoryOnly"],
    ["IVFPQ", "RocksDB"],
    ["IVFFLAT", "RocksDB"],
    ["FLAT", "MemoryOnly"]
])
def test_vearch_index_recall(retrieval_type: str, store_type: str):
    benchmark(retrieval_type, store_type, xb, xq, xt, gt)