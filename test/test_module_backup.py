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

__description__ = """ test case for index flat """


def backup(router_url, db_name, space_name, command):
    url = router_url + "/backup/dbs/" + db_name + "/spaces/" + space_name
    data = {
        "command": command,
        "s3_param": {
            "access_key": "minioadmin",
            "secret_key": "minioadmin",
            "bucket_name": "test",
            "endpoint": "minio:9000",
            "use_ssl": False
        },
    }
    response = requests.post(url, auth=(username, password), json=data)
    assert response.json()["code"] == 0


def create(router_url, embedding_size, store_type="MemoryOnly"):
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
                    "metric_type": "L2",
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
    response = create_db(router_url, db_name)
    logger.info(response.json())

    response = create_space(router_url, db_name, space_config)
    logger.info(response.json())


def query(parallel_on_queries, xq, gt, k, logger):
    query_dict = {
        "vectors": [],
        "index_params": {
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
        result = "batch: %d, parallel_on_queries: %d, avarage time: %.2f ms, " % (
            batch, parallel_on_queries, avarage)
        for recall in recalls:
            result += "recall@%d = %.2f%% " % (recall, recalls[recall] * 100)
        logger.info(result)

        assert recalls[1] >= 0.95
        assert recalls[10] >= 1.0


def benchmark(store_type, xb, xq, gt):
    embedding_size = xb.shape[1]
    batch_size = 100
    k = 100

    total = xb.shape[0]
    total_batch = int(total / batch_size)
    logger.info("dataset num: %d, total_batch: %d, dimension: %d, search num: %d, topK: %d" % (
        total, total_batch, embedding_size, xq.shape[0], k))

    create(router_url, embedding_size, store_type)

    add(total_batch, batch_size, xb)

    waiting_index_finish(logger, total)

    backup(router_url, db_name, space_name, "create")
    time.sleep(30)

    destroy(router_url, db_name, space_name)

    create(router_url, embedding_size, store_type)
    backup(router_url, db_name, space_name, "restore")
    waiting_index_finish(logger, total)

    for parallel_on_queries in [0, 1]:
        query(parallel_on_queries, xq, gt, k, logger)

    destroy(router_url, db_name, space_name)


sift10k = DatasetSift10K(logger)
xb = sift10k.get_database()
xq = sift10k.get_queries()
gt = sift10k.get_groundtruth()


@ pytest.mark.parametrize(["store_type"], [
    ["MemoryOnly"],
])
def test_vearch_index_flat(store_type: str):
    benchmark(store_type, xb, xq, gt)
