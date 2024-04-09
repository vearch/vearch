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

__description__ = """ test case for index ivfpq """


def create(router_url, embedding_size, store_type="MemoryOnly", index_params={}):
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
            "index": True,
            "dimension": embedding_size,
            "store_type": store_type,
            "index": {
                "name": "gamma",
                "type": "IVFPQ",
                "params": index_params
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


def query(quick, nprobe, parallel_on_queries, xq, gt, k, logger):
    query_dict = {
        "vectors": [],
        "index_params": {
            "nprobe": nprobe,
            "parallel_on_queries": parallel_on_queries
        },
        "vector_value": False,
        "fields": ["field_int"],
        "quick": quick,
        "size": k,
        "db_name": db_name,
        "space_name": space_name,
    }

    for batch in [True, False]:
        avarage, recalls = evaluate(xq, gt, k, batch, query_dict, logger)
        result = "batch: %d, nprobe: %d, quick: %d, parallel_on_queries: %d, avarage time: %.2f ms, " \
            % (batch, nprobe, quick, parallel_on_queries, avarage)
        for recall in recalls:
            result += "recall@%d = %.2f%% " % (recall, recalls[recall] * 100)
            if recall == k and nprobe > 10:
                assert recalls[recall] >= 0.9
        logger.info(result)
        if nprobe > 1:
            assert recalls[1] >= 0.6
            assert recalls[10] >= 0.9
            assert recalls[100] >= 0.95


def benchmark(store_type, index_params, xb, xq, gt):
    embedding_size = xb.shape[1]
    batch_size = 100
    k = 100

    total = xb.shape[0]
    total_batch = int(total / batch_size)
    logger.info("dataset num: %d, total_batch: %d, dimension: %d, ncentroids %d, search num: %d, topK: %d"
                % (total, total_batch, embedding_size, index_params["ncentroids"], xq.shape[0], k))

    create(router_url, embedding_size, store_type, index_params)

    add(total_batch, batch_size, xb)

    waiting_index_finish(logger, total)

    for quick in [True, False]:
        for nprobe in [10, 20]:
            for parallel_on_queries in [0, 1]:
                query(quick, nprobe, parallel_on_queries, xq, gt, k, logger)

    destroy(router_url, db_name, space_name)


sift10k = DatasetSift10K(logger)
xb = sift10k.get_database()
xq = sift10k.get_queries()
gt = sift10k.get_groundtruth()


@ pytest.mark.parametrize(["store_type", "ncentroids"], [
    ["MemoryOnly", 128],
    ["RocksDB", 128],
])
def test_vearch_index_ivfpq_without_nsubvector(store_type: str, ncentroids: int):
    index_params = {}
    index_params["metric_type"] = "L2"
    index_params["ncentroids"] = ncentroids
    benchmark(store_type, index_params, xb, xq, gt)


@ pytest.mark.parametrize(["store_type", "ncentroids", "nsubvector", "training_threshold"], [
    ["MemoryOnly", 256, 32, 10000],
    ["MemoryOnly", 256, 64, 10000],
    ["MemoryOnly", 128, 32, 10000],
    ["MemoryOnly", 128, 64, 10000],
    ["MemoryOnly", 256, 32, 1000],
    ["MemoryOnly", 256, 64, 1000],
    ["MemoryOnly", 128, 32, 1000],
    ["MemoryOnly", 128, 64, 1000],
    ["RocksDB", 256, 32, 10000],
    ["RocksDB", 256, 64, 10000],
    ["RocksDB", 128, 32, 10000],
    ["RocksDB", 128, 64, 10000],
    ["RocksDB", 256, 32, 1000],
    ["RocksDB", 256, 64, 1000],
    ["RocksDB", 128, 32, 1000],
    ["RocksDB", 128, 64, 1000],
])
def test_vearch_index_ivfpq_index_params(store_type: str, ncentroids: int, nsubvector: int, training_threshold: int):
    index_params = {}
    index_params["metric_type"] = "L2"
    index_params["ncentroids"] = ncentroids
    index_params["nsubvector"] = nsubvector
    index_params["training_threshold"] = training_threshold
    benchmark(store_type, index_params, xb, xq, gt)


@ pytest.mark.parametrize(["store_type", "ncentroids", "nsubvector", "training_threshold", "with_opq"], [
    ["MemoryOnly", 256, 32, 10000, False],
    ["MemoryOnly", 256, 32, 10000, True],
    ["RocksDB", 256, 32, 10000, False],
    ["RocksDB", 256, 32, 10000, True],
])
def test_vearch_index_ivfpq_hnsw_opq(store_type: str, ncentroids: int, nsubvector: int, training_threshold: int, with_opq: bool):
    index_params = {}
    index_params["metric_type"] = "L2"
    index_params["ncentroids"] = ncentroids
    index_params["nsubvector"] = nsubvector
    index_params["training_threshold"] = training_threshold
    index_params["hnsw"] = {}
    index_params["hnsw"]["nlinks"] = 32
    index_params["hnsw"]["efConstruction"] = 200
    index_params["hnsw"]["efSearch"] = 120
    if with_opq:
        index_params["opq"] = {}
        index_params["opq"]["nsubvector"] = 32
    benchmark(store_type, index_params, xb, xq, gt)
