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
            "index": {"name": "gamma", "type": "IVFPQ", "params": index_params},
            # "format": "normalization"
        },
    ]

    space_config = {
        "name": space_name,
        "partition_num": 1,
        "replica_num": 1,
        "fields": properties["fields"],
    }
    response = create_db(router_url, db_name)
    logger.info(response.json())

    response = create_space(router_url, db_name, space_config)
    logger.info(response.json())


def query(
    rerank,
    nprobe,
    parallel_on_queries,
    metric_type,
    metric_is_same,
    batch,
    xq,
    gt,
    k,
):
    query_dict = {
        "vectors": [],
        "index_params": {
            "nprobe": nprobe,
            "parallel_on_queries": parallel_on_queries,
            "recall_num": rerank,
            "metric_type": metric_type,
        },
        "vector_value": False,
        "fields": ["field_int"],
        "limit": k,
        "db_name": db_name,
        "space_name": space_name,
    }

    if nprobe == -1 and parallel_on_queries == -1 and metric_type == "":
        query_dict.pop("index_params")
    else:
        if nprobe == -1:
            query_dict["index_params"].pop("nprobe")
        if parallel_on_queries == -1:
            query_dict["index_params"].pop("parallel_on_queries")
        if rerank == -1:
            query_dict["index_params"].pop("recall_num")
        if metric_type == "":
            query_dict["index_params"].pop("metric_type")

    avarage, recalls = evaluate(xq, gt, k, batch, query_dict)
    result = (
        "batch: %d, nprobe: %d, rerank: %d, parallel_on_queries: %d, metric_type: %s, avg: %.2f ms, "
        % (batch, nprobe, rerank, parallel_on_queries, metric_type, avarage)
    )
    for recall in recalls:
        result += "recall@%d = %.2f%% " % (recall, recalls[recall] * 100)
        if metric_is_same and recall == k and nprobe > 10:
            assert recalls[recall] >= 0.9
    logger.info(result)
    if metric_is_same and nprobe > 10:
        assert recalls[1] >= 0.6
        assert recalls[10] >= 0.9
        assert recalls[100] >= 0.95


def benchmark(store_type, index_params, xb, xq, gt):
    embedding_size = xb.shape[1]
    batch_size = 100
    k = 100

    total = xb.shape[0]
    total_batch = int(total / batch_size)
    logger.info(
        "dataset num: %d, total_batch: %d, dimension: %d, ncentroids %d, search num: %d, topK: %d"
        % (
            total,
            total_batch,
            embedding_size,
            index_params["ncentroids"],
            xq.shape[0],
            k,
        )
    )

    create(router_url, embedding_size, store_type, index_params)

    add(total_batch, batch_size, xb)
    if total - total_batch * batch_size:
        add(total - total_batch * batch_size, 1, xb[total_batch * batch_size:])

    waiting_index_finish(total, 10)

    if index_params["metric_type"] == "L2":
        for rerank in [0, 100, -1]:
            for nprobe in [10, 20, -1]:
                for parallel_on_queries in [0, 1, -1]:
                    for query_metric_type in ["L2", "InnerProduct", ""]:
                        for batch in [0, 1]:
                            metric_is_same = (
                                query_metric_type == ""
                                or query_metric_type == index_params["metric_type"]
                            )
                            query(
                                rerank,
                                nprobe,
                                parallel_on_queries,
                                query_metric_type,
                                metric_is_same,
                                batch,
                                xq,
                                gt,
                                k,
                            )
    else:
        for rerank in [-1]:
            for nprobe in [-1]:
                for parallel_on_queries in [-1]:
                    for query_metric_type in ["L2", "InnerProduct", ""]:
                        for batch in [1]:
                            metric_is_same = (
                                query_metric_type == ""
                                or query_metric_type == index_params["metric_type"]
                            )
                            query(
                                rerank,
                                nprobe,
                                parallel_on_queries,
                                query_metric_type,
                                metric_is_same,
                                batch,
                                xq,
                                gt,
                                k,
                            )

    destroy(router_url, db_name, space_name)


sift10k = DatasetSift10K()
xb = sift10k.get_database()
xq = sift10k.get_queries()
gt = sift10k.get_groundtruth()


@pytest.mark.parametrize(
    ["store_type", "ncentroids", "training_threshold"],
    [
        ["MemoryOnly", 128, 10000],
        ["RocksDB", 128, 10000],
        ["MemoryOnly", 128, 1000],
        ["RocksDB", 128, 1000],
    ],
)
def test_vearch_index_ivfpq_without_nsubvector(store_type: str, ncentroids: int, training_threshold: int):
    index_params = {}
    index_params["metric_type"] = "L2"
    index_params["ncentroids"] = ncentroids
    index_params["training_threshold"] = training_threshold
    benchmark(store_type, index_params, xb, xq, gt)


@pytest.mark.parametrize(
    ["store_type", "ncentroids", "nsubvector", "training_threshold"],
    [
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
    ],
)
def test_vearch_index_ivfpq_index_params(
    store_type: str, ncentroids: int, nsubvector: int, training_threshold: int
):
    index_params = {}
    index_params["metric_type"] = "L2"
    index_params["ncentroids"] = ncentroids
    index_params["nsubvector"] = nsubvector
    index_params["training_threshold"] = training_threshold
    benchmark(store_type, index_params, xb, xq, gt)


@pytest.mark.parametrize(
    ["store_type", "ncentroids", "nsubvector", "training_threshold", "with_opq"],
    [
        ["MemoryOnly", 256, 32, 10000, False],
        ["MemoryOnly", 256, 32, 10000, True],
        ["RocksDB", 256, 32, 10000, False],
        ["RocksDB", 256, 32, 10000, True],
    ],
)
def test_vearch_index_ivfpq_hnsw_opq(
    store_type: str,
    ncentroids: int,
    nsubvector: int,
    training_threshold: int,
    with_opq: bool,
):
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


glove25 = DatasetGlove25()
glove_xb = glove25.get_database()
glove_xq = glove25.get_queries()[:100]
glove_gt = glove25.get_groundtruth()[:100]


@pytest.mark.parametrize(
    ["store_type", "ncentroids"],
    [
        ["MemoryOnly", 1024],
        ["RocksDB", 1024],
    ],
)
def test_vearch_index_ivfpq_ip(store_type: str, ncentroids: int):
    index_params = {}
    index_params["metric_type"] = "InnerProduct"
    index_params["ncentroids"] = ncentroids
    index_params["nsubvector"] = glove25.d
    benchmark(store_type, index_params, glove_xb, glove_xq, glove_gt)
