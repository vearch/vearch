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

__description__ = """ test case for module vector """


def create(router_url, embedding_size, store_type="MemoryOnly"):
    properties = {}
    properties["fields"] = {
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
        },
        "field_vector1": {
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
        "index": {
            "index_name": "gamma",
            "index_type": "FLAT",
            "index_params": {
                "metric_type": "L2",
            }
        },
        "fields": properties["fields"]
    }
    logger.info(create_db(router_url, db_name))

    logger.info(create_space(router_url, db_name, space_config))

def search_result(xq, k:int, batch:bool, query_dict:dict, multi_vector:bool, logger):
    url = router_url + "/document/search?timeout=2000000"

    field_ints = []

    if multi_vector:
        vector_dict = {
            "vector": [
            {
                "field": "field_vector",
                "feature": []
            },
            {
                "field": "field_vector1",
                "feature": []
            }
            ]
        }
    else:
        vector_dict = {
            "vector": [{
                "field": "field_vector",
                "feature": []
            }]
        }
        
    if batch:
        vector_dict["vector"][0]["feature"] = xq.flatten().tolist()
        if multi_vector:
            vector_dict["vector"][1]["feature"] = xq.flatten().tolist()
        query_dict["query"]["vector"] = vector_dict["vector"]
        json_str = json.dumps(query_dict)
        rs = requests.post(url, json_str)

        if rs.status_code != 200 or "documents" not in rs.json():
            logger.info(rs.json())
            logger.info(json_str)

        for results in rs.json()["documents"]:
            field_int = []
            for result in results:
                field_int.append(result["_source"]["field_int"])
            if len(field_int) != k:
                logger.info("len(field_int)=" + str(len(field_int)))
                logger.info(field_int)
                [field_int.append(-1) for i in range(k - len(field_int))]
            assert len(field_int) == k
            field_ints.append(field_int)
    else:
        for i in range(xq.shape[0]):
            vector_dict["vector"][0]["feature"] = xq[i].tolist()
            if multi_vector:
                vector_dict["vector"][1]["feature"] = xq[i].tolist()

            query_dict["query"]["vector"] = vector_dict["vector"]
            json_str = json.dumps(query_dict)
            rs = requests.post(url, json_str)

            if rs.status_code != 200 or "documents" not in rs.json():
                logger.info(rs.json())
                logger.info(json_str)

            field_int = []
            for results in rs.json()["documents"]:
                for result in results:
                    field_int.append(result["_source"]["field_int"])
            if len(field_int) != k:
                logger.debug("len(field_int)=" + str(len(field_int)))
                [field_int.append(-1) for i in range(k - len(field_int))]
            assert len(field_int) == k
            field_ints.append(field_int)
    assert len(field_ints) == xq.shape[0]  
    return np.array(field_ints)
    

def evaluate_recall(xq, gt, k, batch, query_dict, multi_vector, logger):
    nq = xq.shape[0]
    t0 = time.time()
    I = search_result(xq, k, batch, query_dict, multi_vector, logger)
    t1 = time.time()

    recalls = {}
    i = 1
    while i <= k:
        recalls[i] = (I[:, :i] == gt[:, :1]).sum() / float(nq)
        i *= 10

    return (t1 - t0) * 1000.0 / nq, recalls

def query(parallel_on_queries, xq, gt, k, multi_vector, logger):
    query_dict = {
        "query": {
            "vector": []
        },
        "index_params": {
            "parallel_on_queries": parallel_on_queries
        },
        "vector_value":False,
        "fields": ["field_int"],
        "size": k,
        "db_name": db_name,
        "space_name": space_name,
    }

    for batch in [True, False]:
        avarage, recalls = evaluate_recall(xq, gt, k, batch, query_dict, multi_vector, logger)
        result = "batch: %d, parallel_on_queries: %d, avarage time: %.2f ms, " % (batch, parallel_on_queries, avarage)
        for recall in recalls:
            result += "recall@%d = %.2f%% " % (recall, recalls[recall] * 100)
            if recall == k:
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

    get_space(router_url, db_name, space_name)

    add_multi_vector(total_batch, batch_size, xb)

    waiting_index_finish(logger, total)

    for parallel_on_queries in [0, 1]:
        query(parallel_on_queries, xq, gt, k, True, logger)

    destroy(router_url, db_name, space_name)

xb, xq, xt, gt = get_sift10K(logger)


@ pytest.mark.parametrize(["store_type"], [
    ["MemoryOnly"],
])
def test_vearch_module_multi_vector(store_type: str):
    benchmark(store_type, xb, xq, xt, gt)