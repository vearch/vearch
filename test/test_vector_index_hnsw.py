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

__description__ = """ test case for index hnsw """


def create(router_url, embedding_size, nlinks=32, efConstruction=120, metric_type="L2"):
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
                    "metric_type": metric_type,
                    "nlinks": nlinks,
                    "efConstruction": efConstruction,
                    "efSearch": 64,
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
    do_efSearch_check, efSearch, metric_type, metric_is_same, batch, xq, gt, k, logger
):
    query_dict = {
        "vectors": [],
        "index_params": {
            "efSearch": efSearch,
            "do_efSearch_check": do_efSearch_check,
            "metric_type": metric_type,
        },
        "vector_value": False,
        "fields": ["field_int"],
        "limit": k,
        "db_name": db_name,
        "space_name": space_name,
    }

    if do_efSearch_check == -1 and efSearch == -1 and metric_type == "":
        query_dict.pop("index_params")
    else:
        if do_efSearch_check == -1:
            query_dict["index_params"].pop("do_efSearch_check")
        if efSearch == -1:
            query_dict["index_params"].pop("efSearch")
        if metric_type == "":
            query_dict["index_params"].pop("metric_type")

    avarage, recalls = evaluate(xq, gt, k, batch, query_dict, logger)
    result = (
        "batch: %d, efSearch: %d, do_efSearch_check: %d, metric_type: %s, avg: %.2f ms, "
        % (batch, efSearch, do_efSearch_check, metric_type, avarage)
    )
    for recall in recalls:
        result += "recall@%d = %.2f%% " % (recall, recalls[recall] * 100)
    logger.info(result)

    if metric_is_same:
        assert recalls[1] >= 0.8
        assert recalls[10] >= 0.9


def benchmark(nlinks, efConstruction, metric_type, xb, xq, gt):
    embedding_size = xb.shape[1]
    batch_size = 100
    k = 100

    total = xb.shape[0]
    total_batch = int(total / batch_size)
    logger.info(
        "dataset num: %d, total_batch: %d, dimension: %d, nlinks: %d, efConstruction: %d, search num: %d, topK: %d "
        % (total, total_batch, embedding_size, nlinks, efConstruction, xq.shape[0], k)
    )

    create(router_url, embedding_size, nlinks, efConstruction, metric_type)

    add(total_batch, batch_size, xb)
    if total - total_batch * batch_size:
        add(total - total_batch * batch_size, 1, xb[total_batch * batch_size:])

    waiting_index_finish(logger, total)

    if metric_type == "L2":
        for do_efSearch_check in [1, 0, -1]:
            for efSearch in [16, 32, -1]:
                for query_metric_type in ["L2", "InnerProduct", ""]:
                    for batch in [0, 1]:
                        metric_is_same = (
                            query_metric_type == "" or query_metric_type == metric_type
                        )
                        query(
                            do_efSearch_check,
                            efSearch,
                            query_metric_type,
                            metric_is_same,
                            batch,
                            xq,
                            gt,
                            k,
                            logger,
                        )
    else:
        for do_efSearch_check in [-1]:
            for efSearch in [-1]:
                for query_metric_type in ["L2", "InnerProduct", ""]:
                    for batch in [1]:
                        metric_is_same = (
                            query_metric_type == "" or query_metric_type == metric_type
                        )
                        query(
                            do_efSearch_check,
                            efSearch,
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
xq = sift10k.get_queries()[:100]
gt = sift10k.get_groundtruth()[:100]


@pytest.mark.parametrize(
    ["nlinks", "efConstruction"],
    [
        [32, 40],
        [32, 80],
    ],
)
def test_vearch_index_hnsw(nlinks: int, efConstruction: int):
    benchmark(nlinks, efConstruction, "L2", xb, xq, gt)


glove25 = DatasetGlove25(logger)
glove_xb = glove25.get_database()
glove_xq = glove25.get_queries()
glove_gt = glove25.get_groundtruth()


@pytest.mark.parametrize(
    ["nlinks", "efConstruction"],
    [
        [32, 40],
        [32, 80],
    ],
)
def test_vearch_index_hnsw_ip(nlinks, efConstruction):
    benchmark(nlinks, efConstruction, "InnerProduct",
              glove_xb, glove_xq, glove_gt)
