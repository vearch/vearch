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

__description__ = """ test case for index ivfflat """


def create(router_url, embedding_size, store_type="MemoryOnly", ncentroids=256):
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
                "type": "IVFFLAT",
                "params": {
                    "metric_type": "L2",
                    "ncentroids": ncentroids,
                    "training_threshold": ncentroids * 39
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


def query(nprobe, parallel_on_queries, xq, gt, k, logger):
    query_dict = {
        "vectors": [],
        "index_params": {
            "nprobe": nprobe,
            "parallel_on_queries": parallel_on_queries
        },
        "vector_value": False,
        "fields": ["field_int"],
        "limit": k,
        "db_name": db_name,
        "space_name": space_name,
    }

    for batch in [True, False]:
        avarage, recalls = evaluate(xq, gt, k, batch, query_dict, logger)
        result = "batch: %d, nprobe: %d, parallel_on_queries: %d, avarage time: %.2f ms, " \
            % (batch, nprobe, parallel_on_queries, avarage)
        for recall in recalls:
            result += "recall@%d = %.2f%% " % (recall, recalls[recall] * 100)
            if recall == k and nprobe > 1:
                assert recalls[recall] >= 0.9
        logger.info(result)
        if nprobe > 1:
            assert recalls[1] >= 0.8
            assert recalls[10] >= 0.95


def benchmark(store_type, ncentroids, xb, xq, gt):
    embedding_size = xb.shape[1]
    batch_size = 100
    k = 100

    total = xb.shape[0]
    total_batch = int(total / batch_size)
    logger.info("dataset num: %d, total_batch: %d, dimension: %d, ncentroids %d, search num: %d, topK: %d"
                % (total, total_batch, embedding_size, ncentroids, xq.shape[0], k))

    create(router_url, embedding_size, store_type, ncentroids)

    add(total_batch, batch_size, xb)

    waiting_index_finish(logger, total, 10)

    for nprobe in [1, 10, 20]:
        for parallel_on_queries in [0, 1]:
            query(nprobe, parallel_on_queries, xq, gt, k, logger)

    destroy(router_url, db_name, space_name)


sift10k = DatasetSift10K(logger)
xb = sift10k.get_database()
xq = sift10k.get_queries()
gt = sift10k.get_groundtruth()


@ pytest.mark.parametrize(["store_type", "ncentroids"], [
    ["RocksDB", 256],
    ["RocksDB", 128],
])
def test_vearch_index_ivfflat(store_type: str, ncentroids: int):
    benchmark(store_type, ncentroids, xb, xq, gt)
