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

__description__ = """ test case for index hnsw """


def create(router_url, embedding_size, nlinks=32, efConstruction=120):
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
            "store_type": "MemoryOnly",
            "index": {
                "name": "gamma",
                "type": "HNSW",
                "params": {
                    "metric_type": "L2",
                    "nlinks": nlinks,
                    "efConstruction": efConstruction,
                    "efSearch": 64
                }
            },
            #"format": "normalization"
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

def query(do_efSearch_check, efSearch, xq, gt, k, logger):
    query_dict = {
        "query": {
            "vector": []
        },
        "index_params": {
            "efSearch": efSearch,
            "do_efSearch_check": do_efSearch_check
        },
        "vector_value":False,
        "fields": ["field_int"],
        "size": k,
        "db_name": db_name,
        "space_name": space_name,
    }

    for batch in [True, False]:
        avarage, recalls = evaluate(xq, gt, k, batch, query_dict, logger)
        result = "batch: %d, efSearch: %d, do_efSearch_check: %d, avarage time: %.2f ms, " \
                 % (batch, efSearch, do_efSearch_check, avarage)
        for recall in recalls:
            result += "recall@%d = %.2f%% " % (recall, recalls[recall] * 100)
            if recall == k:
                assert recalls[recall] >= 0.9
        logger.info(result)

def benchmark(nlinks, efConstruction, xb, xq, xt, gt):
    embedding_size = xb.shape[1]
    batch_size = 100
    k = 100

    total = xb.shape[0]
    total_batch = int(total / batch_size)
    logger.info("dataset num: %d, total_batch: %d, dimension: %d, nlinks: %d, efConstruction: %d, search num: %d, topK: %d " \
                %(total, total_batch, embedding_size, nlinks, efConstruction, xq.shape[0], k))

    create(router_url, embedding_size, nlinks, efConstruction)

    add(total_batch, batch_size, xb)

    waiting_index_finish(logger, total)

    for do_efSearch_check in [1, 0]:
        for efSearch in [16, 32]:
            query(do_efSearch_check, efSearch, xq, gt, k, logger)

    destroy(router_url, db_name, space_name)

xb, xq, xt, gt = get_sift10K(logger)


@ pytest.mark.parametrize(["nlinks", "efConstruction"], [
    [32, 40],
    [32, 80],
])
def test_vearch_index_hnsw(nlinks: int, efConstruction: int):
    benchmark(nlinks, efConstruction, xb, xq, xt, gt)