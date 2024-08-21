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
import logging
from utils.vearch_utils import *
from utils.data_utils import *

logging.basicConfig()
logger = logging.getLogger(__name__)

__description__ = """ test case for index ivfflat """


def create(
    router_url,
    embedding_size,
    store_type="MemoryOnly",
    ncentroids=256,
    metric_type="L2",
):
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
                    "metric_type": metric_type,
                    "ncentroids": ncentroids,
                    "training_threshold": ncentroids * 39,
                },
            },
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
    nprobe, parallel_on_queries, metric_type, metric_is_same, batch, xq, gt, k, logger
):
    query_dict = {
        "vectors": [],
        "index_params": {
            "nprobe": nprobe,
            "parallel_on_queries": parallel_on_queries,
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
        if metric_type == "":
            query_dict["index_params"].pop("metric_type")

    avarage, recalls = evaluate(xq, gt, k, batch, query_dict, logger)
    result = (
        "batch: %d, nprobe: %d, parallel_on_queries: %d, metric_type: %s, avg: %.2f ms, "
        % (batch, nprobe, parallel_on_queries, metric_type, avarage)
    )
    for recall in recalls:
        result += "recall@%d = %.2f%% " % (recall, recalls[recall] * 100)
        if metric_is_same and recall == k and nprobe > 1:
            assert recalls[recall] >= 0.9
    logger.info(result)
    if metric_is_same and nprobe > 1:
        assert recalls[1] >= 0.8
        assert recalls[10] >= 0.95


def benchmark(store_type, ncentroids, metric_type, xb, xq, gt):
    embedding_size = xb.shape[1]
    batch_size = 100
    k = 100

    total = xb.shape[0]
    total_batch = int(total / batch_size)
    logger.info(
        "dataset num: %d, total_batch: %d, dimension: %d, ncentroids %d, search num: %d, topK: %d"
        % (total, total_batch, embedding_size, ncentroids, xq.shape[0], k)
    )

    create(router_url, embedding_size, store_type, ncentroids, metric_type)

    add(total_batch, batch_size, xb)
    if total - total_batch * batch_size:
        add(total - total_batch * batch_size, 1, xb[total_batch * batch_size:])

    waiting_index_finish(logger, total, 10)

    if metric_type == "L2":
        for nprobe in [1, 10, 20, -1]:
            for parallel_on_queries in [0, 1, -1]:
                for query_metric_type in ["L2", "InnerProduct", ""]:
                    for batch in [0, 1]:
                        metric_is_same = (
                            query_metric_type == "" or query_metric_type == metric_type
                        )
                        query(
                            nprobe,
                            parallel_on_queries,
                            query_metric_type,
                            metric_is_same,
                            batch,
                            xq,
                            gt,
                            k,
                            logger,
                        )
    else:
        for nprobe in [-1]:
            for parallel_on_queries in [-1]:
                for query_metric_type in ["L2", "InnerProduct", ""]:
                    for batch in [1]:
                        metric_is_same = (
                            query_metric_type == "" or query_metric_type == metric_type
                        )
                        query(
                            nprobe,
                            parallel_on_queries,
                            query_metric_type,
                            metric_is_same,
                            batch,
                            xq,
                            gt,
                            k,
                            logger,
                        )

    destroy(router_url, db_name, space_name)


sift10k = DatasetSift10K(logger)
xb = sift10k.get_database()
xq = sift10k.get_queries()
gt = sift10k.get_groundtruth()


@pytest.mark.parametrize(
    ["store_type", "ncentroids"],
    [
        ["RocksDB", 256],
        ["RocksDB", 128],
    ],
)
def test_vearch_index_ivfflat_l2(store_type: str, ncentroids: int):
    benchmark(store_type, ncentroids, "L2", xb, xq, gt)


glove25 = DatasetGlove25(logger)
glove_xb = glove25.get_database()
glove_xq = glove25.get_queries()[:100]
glove_gt = glove25.get_groundtruth()[:100]


@pytest.mark.parametrize(
    ["store_type", "ncentroids"],
    [
        ["RocksDB", 1024],
        ["RocksDB", 512],
    ],
)
def test_vearch_index_ivfflat_ip(store_type: str, ncentroids: int):
    benchmark(store_type, ncentroids, "InnerProduct",
              glove_xb, glove_xq, glove_gt)
