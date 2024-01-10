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
import random
import sys
import time
import numpy as np
import pytest
import logging
from multiprocessing import Pool as ThreadPool

from vearch_utils import *

logging.basicConfig()
logger = logging.getLogger(__name__)

__description__ = """ test case for index ivfpq """


def create(router_url, embedding_size, store_type="MemoryOnly", ncentroids=256):
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
            "index_size": ncentroids * 39,
            "retrieval_type": "IVFPQ",
            "retrieval_param": {
                "metric_type": "L2",
                "ncentroids": ncentroids,
                "nsubvector": int(embedding_size / 4),
            }
        },
        "properties": properties["properties"]
    }
    logger.info(create_db(router_url, db_name))

    logger.info(create_space(router_url, db_name, space_config))

def query(quick, nprobe, xq, gt, k, logger):
    query_dict = {
        "query": {
            "vector": []
        },
        "retrieval_param": {
            "nprobe": nprobe,
        },
        "vector_value":False,
        "fields": ["field_int"],
        "quick": quick,
        "size": k,
        "db_name": db_name,
        "space_name": space_name,
    }

    for batch in [True, False]:
        avarage, recalls = evaluate(xq, gt, k, batch, query_dict, logger)
        result = "batch: %d, nprobe: %d, quick: %d, avarage time: %.2f ms, " % (batch, nprobe, quick, avarage)
        for recall in recalls:
            result += "recall@%d = %.2f%% " % (recall, recalls[recall] * 100)
            assert recalls[recall] >= 0.8
        logger.info(result)    

def benchmark(store_type, xb, xq, xt, gt):
    embedding_size = xb.shape[1]
    batch_size = 100
    k = 100

    total = xb.shape[0]
    total_batch = int(total / batch_size)
    logger.info("dataset num: %d, total_batch: %d, dimension: %d, search num: %d, topK: %d" %(total, total_batch, embedding_size, xq.shape[0], k))
    
    create(router_url, embedding_size, store_type)

    add(total_batch, batch_size, xb)

    waiting_index_finish(logger, total)

    for quick in [True, False]:
        for nprobe in [10, 40]:
            query(quick, nprobe, xq, gt, k, logger)

    destroy(router_url, db_name, space_name)

xb, xq, xt, gt = get_sift10K(logger)


@ pytest.mark.parametrize(["store_type"], [
    ["MemoryOnly"],
    ["RocksDB"],
])
def test_vearch_index_ivfpq(store_type: str):
    benchmark(store_type, xb, xq, xt, gt)