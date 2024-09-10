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

__description__ = """ test case for index flat """


def create(router_url, embedding_size, store_type="MemoryOnly", metric_type="L2"):
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
                "type": "FLAT",
                "params": {
                    "metric_type": metric_type,
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


def query(parallel_on_queries, metric_type, metric_is_same, batch, xq, gt, k):
    query_dict = {
        "vectors": [],
        "index_params": {
            "parallel_on_queries": parallel_on_queries,
            "metric_type": metric_type,
        },
        "vector_value": False,
        "fields": ["field_int"],
        "limit": k,
        "db_name": db_name,
        "space_name": space_name,
    }

    if parallel_on_queries == -1 and metric_type == "":
        query_dict.pop("index_params")
    else:
        if parallel_on_queries == -1:
            query_dict["index_params"].pop("parallel_on_queries")
        if metric_type == "":
            query_dict["index_params"].pop("metric_type")

    avarage, recalls = evaluate(xq, gt, k, batch, query_dict)
    result = "batch: %d, parallel_on_queries: %d, metric_type: %s, avg: %.2f ms, " % (
        batch,
        parallel_on_queries,
        metric_type,
        avarage,
    )
    for recall in recalls:
        result += "recall@%d = %.2f%% " % (recall, recalls[recall] * 100)
    logger.info(result)

    if metric_is_same:
        assert recalls[1] >= 0.95
        assert recalls[10] >= 1.0


def benchmark(store_type, metric_type, xb, xq, gt):
    embedding_size = xb.shape[1]
    batch_size = 100
    k = 100

    total = xb.shape[0]
    total_batch = int(total / batch_size)
    logger.info(
        "dataset num: %d, total_batch: %d, dimension: %d, search num: %d, topK: %d"
        % (total, total_batch, embedding_size, xq.shape[0], k)
    )

    create(router_url, embedding_size, store_type, metric_type)

    add(total_batch, batch_size, xb)
    if total - total_batch * batch_size:
        add(total - total_batch * batch_size, 1, xb[total_batch * batch_size:])

    waiting_index_finish(total)

    if metric_type == "L2":
        for parallel_on_queries in [0, 1, -1]:
            for query_metric_type in ["L2", "InnerProduct", ""]:
                for batch in [0, 1]:
                    metric_is_same = (
                        query_metric_type == "" or query_metric_type == metric_type
                    )
                    query(
                        parallel_on_queries,
                        query_metric_type,
                        metric_is_same,
                        batch,
                        xq,
                        gt,
                        k,
                    )
    else:
        for parallel_on_queries in [-1]:
            for query_metric_type in ["L2", "InnerProduct", ""]:
                for batch in [1]:
                    metric_is_same = (
                        query_metric_type == "" or query_metric_type == metric_type
                    )
                    query(
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
    ["store_type"],
    [
        ["MemoryOnly"],
    ],
)
def test_vearch_index_flat_l2(store_type: str):
    benchmark(store_type, "L2", xb, xq, gt)


glove25 = DatasetGlove25()
glove_xb = glove25.get_database()
glove_xq = glove25.get_queries()[:100]
glove_gt = glove25.get_groundtruth()[:100]


@pytest.mark.parametrize(
    ["store_type"],
    [
        ["MemoryOnly"],
    ],
)
def test_vearch_index_flat_ip(store_type: str):
    benchmark(store_type, "InnerProduct", glove_xb, glove_xq, glove_gt)
