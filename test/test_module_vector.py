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
    properties["fields"] = [
        {
            "name": "field_int",
            "type": "integer",
        },
        {
            "name": "field_vector",
            "type": "vector",
            "index": {
                "name": "gamma",
                "type": "FLAT",
                "params": {
                    "metric_type": "L2",
                }
            },
            "dimension": embedding_size,
            "store_type": store_type,
            # "format": "normalization"
        },
        {
            "name": "field_vector1",
            "type": "vector",
            "dimension": embedding_size,
            "store_type": store_type,
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


def search_result(xq, k: int, batch: bool, query_dict: dict, multi_vector: bool, logger):
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
        query_dict["vectors"] = vector_dict["vector"]
        json_str = json.dumps(query_dict)
        rs = requests.post(url, json_str)

        if rs.status_code != 200 or "documents" not in rs.json()["data"]:
            logger.info(rs.json())
            logger.info(json_str)

        for results in rs.json()["data"]["documents"]:
            field_int = []
            for result in results:
                field_int.append(result["field_int"])
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

            query_dict["vectors"] = vector_dict["vector"]
            json_str = json.dumps(query_dict)
            rs = requests.post(url, json_str)

            if rs.status_code != 200 or "documents" not in rs.json()["data"]:
                logger.info(rs.json())
                logger.info(json_str)

            field_int = []
            for results in rs.json()["data"]["documents"]:
                for result in results:
                    field_int.append(result["field_int"])
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
        avarage, recalls = evaluate_recall(
            xq, gt, k, batch, query_dict, multi_vector, logger)
        result = "batch: %d, parallel_on_queries: %d, avarage time: %.2f ms, " % (
            batch, parallel_on_queries, avarage)
        for recall in recalls:
            result += "recall@%d = %.2f%% " % (recall, recalls[recall] * 100)
            if recall == k:
                assert recalls[recall] >= 0.8

        assert recalls[1] >= 0.95
        assert recalls[10] >= 1.0
        logger.info(result)


def benchmark(store_type, xb, xq, gt):
    embedding_size = xb.shape[1]
    batch_size = 100
    k = 100

    total = xb.shape[0]
    total_batch = int(total / batch_size)
    logger.info("dataset num: %d, total_batch: %d, dimension: %d, search num: %d, topK: %d" % (
        total, total_batch, embedding_size, xq.shape[0], k))

    create(router_url, embedding_size, store_type)

    get_space(router_url, db_name, space_name)

    add_multi_vector(total_batch, batch_size, xb)

    waiting_index_finish(logger, total)

    for parallel_on_queries in [0, 1]:
        query(parallel_on_queries, xq, gt, k, True, logger)

    destroy(router_url, db_name, space_name)


sift10k = DatasetSift10K(logger)
xb = sift10k.get_database()
xq = sift10k.get_queries()
gt = sift10k.get_groundtruth()


@ pytest.mark.parametrize(["store_type"], [
    ["MemoryOnly"],
])
def test_vearch_module_multi_vector(store_type: str):
    benchmark(store_type, xb, xq, gt)


class TestUpsertMultiVectorBadCase:
    def setup_class(self):
        self.logger = logger
        self.xb = xb

    # prepare for badcase
    def test_prepare_cluster_badcase(self):
        create(router_url, self.xb.shape[1], "MemoryOnly")

    @pytest.mark.parametrize(
        ["index", "wrong_type"],
        [
            [0, "only one vector"],
            [1, "bad vector length"],
        ],
    )
    def test_badcase(self, index, wrong_type):
        wrong_parameters = [False for i in range(3)]
        wrong_parameters[index] = True
        batch_size = 1
        total = 1
        if total == 0:
            total = xb.shape[0]
        total_batch = int(total / batch_size)
        add_multi_vector_error(total_batch, batch_size,
                               self.xb, self.logger, wrong_parameters)
        assert get_space_num() == 0

    # destroy for badcase
    def test_destroy_cluster_badcase(self):
        destroy(router_url, db_name, space_name)


class TestSearchWeightRanker:
    def setup_class(self):
        self.logger = logger
        self.xb = xb

    # prepare for badcase
    def test_prepare_cluster_badcase(self):
        create(router_url, self.xb.shape[1], "MemoryOnly")

    def test_prepare_upsert(self):
        batch_size = 1
        total = 1
        total_batch = int(total / batch_size)
        add_multi_vector(total_batch, batch_size, xb)
        assert get_space_num() == 1

    def test_search_weight_ranker(self):
        data = {}
        data["vectors"] = []
        vector_info = {
            "field": "field_vector",
            "feature": xq[:1].flatten().tolist(),
        }
        vector_info1 = {
            "field": "field_vector1",
            "feature": xq[:1].flatten().tolist(),
        }
        data["vectors"].append(vector_info)
        data["vectors"].append(vector_info1)
        query_dict = {
            "vectors": data["vectors"],
            "fields": ["field_int"],
            "limit": 100,
            "db_name": db_name,
            "space_name": space_name,
            "ranker": {
                "type": "WeightedRanker",
                "params": [0.8, 0.2]
            }
        }
        url = router_url + "/document/search"
        json_str = json.dumps(query_dict)
        rs = requests.post(url, json_str)
        # logger.info(rs.json())
        assert rs.json()["code"] == 200
        score =np.sum(np.square(xq[:1] - xb[:1]))
        assert abs(rs.json()["data"]["documents"][0][0]["_score"] - score) <= 0.1

        query_dict["ranker"]["params"] = [1, 1]
        url = router_url + "/document/search"
        json_str = json.dumps(query_dict)
        rs = requests.post(url, json_str)
        # logger.info(rs.json()["data"]["documents"][0][0]["_score"])
        assert abs(rs.json()["data"]["documents"][0][0]["_score"] - 2 * score) <= 0.1

    # destroy for badcase
    def test_destroy_cluster_badcase(self):
        destroy(router_url, db_name, space_name)

class TestSearchScore:
    def setup_class(self):
        self.logger = logger
        self.xb = xb

    # prepare for badcase
    def test_prepare_cluster_badcase(self):
        create(router_url, self.xb.shape[1], "MemoryOnly")

    def test_prepare_upsert(self):
        batch_size = 1
        total_batch = 1
        add_multi_vector(total_batch, batch_size, xb)
        assert get_space_num() == 1

    def test_search_score(self):
        for i in range(100):
            data = {}
            data["vectors"] = []
            vector_info = {
                "field": "field_vector",
                "feature": xq[i:i+1].flatten().tolist(),
            }
            data["vectors"].append(vector_info)
            query_dict = {
                "vectors": data["vectors"],
                "fields": ["field_int"],
                "limit": 1,
                "db_name": db_name,
                "space_name": space_name,
                "ranker": {
                    "type": "WeightedRanker",
                    "params": [0.8, 0.2]
                }
            }
            url = router_url + "/document/search"
            json_str = json.dumps(query_dict)
            rs = requests.post(url, json_str)
            # logger.info(rs.json())
            assert rs.json()["code"] == 200
            score = np.sum(np.square(xq[i:i+1] - xb[:1]))
            assert abs(rs.json()["data"]["documents"][0][0]["_score"] - score) <= 0.1

    # destroy for badcase
    def test_destroy_cluster_badcase(self):
        destroy(router_url, db_name, space_name)