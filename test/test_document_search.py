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
from utils.vearch_utils import *
from utils.data_utils import *

__description__ = """ test case for document search """


sift10k = DatasetSift10K()
xb = sift10k.get_database()
xq = sift10k.get_queries()
gt = sift10k.get_groundtruth()


def check(total, bulk, full_field, with_filter, query_type, xb):
    embedding_size = xb.shape[1]
    batch_size = 1
    if bulk:
        batch_size = 100
    k = 100
    if total == 0:
        total = xb.shape[0]
    total_batch = int(total / batch_size)
    with_id = True
    seed = 1

    logger.info(
        "dataset num: %d, total_batch: %d, dimension: %d, search num: %d, topK: %d"
        % (total, total_batch, embedding_size, xq.shape[0], k)
    )

    properties = {}
    properties["fields"] = [
        {
            "name": "field_int",
            "type": "integer",
            "index": {"name": "field_int", "type": "SCALAR"},
        },
        {
            "name": "field_long",
            "type": "long",
            "index": {"name": "field_long", "type": "SCALAR"},
        },
        {
            "name": "field_float",
            "type": "float",
            "index": {"name": "field_float", "type": "SCALAR"},
        },
        {
            "name": "field_double",
            "type": "double",
            "index": {"name": "field_double", "type": "SCALAR"},
        },
        {
            "name": "field_string",
            "type": "string",
            "index": {"name": "field_string", "type": "SCALAR"},
        },
        {
            "name": "field_vector",
            "type": "vector",
            "index": {
                "name": "gamma",
                "type": "FLAT",
                "params": {
                    "metric_type": "L2",
                },
            },
            "dimension": embedding_size,
            "store_type": "MemoryOnly",
            # "format": "normalization"
        },
    ]

    create_for_document_test(router_url, embedding_size, properties)

    add(total_batch, batch_size, xb, with_id, full_field)

    logger.info("%s doc_num: %d" % (space_name, get_space_num()))

    search_interface(
        total_batch, batch_size, xb, full_field, with_filter, seed, query_type
    )

    destroy(router_url, db_name, space_name)


@pytest.mark.parametrize(
    ["bulk", "full_field", "with_filter", "query_type"],
    [
        [True, True, True, "by_vector"],
        [True, True, False, "by_vector"],
        [False, True, True, "by_vector"],
        [False, True, False, "by_vector"],
        [False, True, False, "by_vector_with_symbol"],
    ],
)
def test_vearch_document_search(
    bulk: bool, full_field: bool, with_filter: bool, query_type: str
):
    check(100, bulk, full_field, with_filter, query_type, xb)


@pytest.mark.parametrize(
    ["index_type"],
    [["IVFPQ"], ["IVFFLAT"]],
)
def test_vearch_document_search_brute_force_search_threshold(index_type):
    embedding_size = xb.shape[1]
    batch_size = 100
    k = 100
    with_id = False
    full_field = True
    seed = 1
    brute_force_search_threshold = 100
    total_batch = int(brute_force_search_threshold / batch_size)

    logger.info(
        "dataset num: %d, total_batch: %d, dimension: %d, search num: %d, topK: %d"
        % (brute_force_search_threshold, total_batch, embedding_size, xq.shape[0], k)
    )

    properties = {}
    properties["fields"] = [
        {
            "name": "field_int",
            "type": "integer",
            "index": {"name": "field_int", "type": "SCALAR"},
        },
        {
            "name": "field_long",
            "type": "long",
            "index": {"name": "field_long", "type": "SCALAR"},
        },
        {
            "name": "field_float",
            "type": "float",
            "index": {"name": "field_float", "type": "SCALAR"},
        },
        {
            "name": "field_double",
            "type": "double",
            "index": {"name": "field_double", "type": "SCALAR"},
        },
        {
            "name": "field_string",
            "type": "string",
            "index": {"name": "field_string", "type": "SCALAR"},
        },
        {
            "name": "field_vector",
            "type": "vector",
            "index": {
                "name": "gamma",
                "type": index_type,
                "params": {
                    "metric_type": "L2",
                    "training_threshold": 3 * brute_force_search_threshold,
                    "ncentroids": 2,
                    "nprobe": 1,
                },
            },
            "dimension": embedding_size,
            # "format": "normalization"
        },
    ]

    create_for_document_test(router_url, embedding_size, properties)

    add(total_batch, batch_size, xb, with_id, full_field)
    logger.info("%s doc_num: %d" % (space_name, get_space_num()))

    data = {}
    data["db_name"] = db_name
    data["space_name"] = space_name
    data["vectors"] = []
    vector_info = {
        "field": "field_vector",
        "feature": xb[:1].flatten().tolist(),
    }
    data["vectors"].append(vector_info)
    data["index_params"] = {"nprobe": 1}

    json_str = json.dumps(data)
    logger.info(json_str)
    url = router_url + "/document/search"
    rs = requests.post(url, auth=(username, password), data=json_str)
    assert rs.status_code == 200

    add(total_batch, batch_size, xb, with_id, full_field)
    logger.info("%s doc_num: %d" % (space_name, get_space_num()))

    rs = requests.post(url, auth=(username, password), data=json_str)
    logger.info(rs.json())
    assert rs.status_code != 200

    destroy(router_url, db_name, space_name)


@pytest.mark.parametrize(
    ["index_type"],
    [["IVFPQ"], ["IVFFLAT"], ["FLAT"], ["HNSW"]],
)
def test_vearch_document_search_with_score_filter(index_type):
    embedding_size = xb.shape[1]
    batch_size = 100
    k = 10
    with_id = False
    full_field = True
    seed = 1
    brute_force_search_threshold = 100
    total_batch = 3

    logger.info(
        "dataset num: %d, total_batch: %d, dimension: %d, search num: %d, topK: %d"
        % (brute_force_search_threshold, total_batch, embedding_size, xq.shape[0], k)
    )

    properties = {}
    properties["fields"] = [
        {
            "name": "field_int",
            "type": "integer",
            "index": {"name": "field_int", "type": "SCALAR"},
        },
        {
            "name": "field_long",
            "type": "long",
            "index": {"name": "field_long", "type": "SCALAR"},
        },
        {
            "name": "field_float",
            "type": "float",
            "index": {"name": "field_float", "type": "SCALAR"},
        },
        {
            "name": "field_double",
            "type": "double",
            "index": {"name": "field_double", "type": "SCALAR"},
        },
        {
            "name": "field_string",
            "type": "string",
            "index": {"name": "field_string", "type": "SCALAR"},
        },
        {
            "name": "field_vector",
            "type": "vector",
            "index": {
                "name": "gamma",
                "type": index_type,
                "params": {
                    "metric_type": "L2",
                    "training_threshold": 3 * brute_force_search_threshold,
                    "ncentroids": 2,
                    "nprobe": 1,
                },
            },
            "dimension": embedding_size,
            # "format": "normalization"
        },
    ]

    create_for_document_test(router_url, embedding_size, properties)

    add(total_batch, batch_size, xb, with_id, full_field)
    logger.info("%s doc_num: %d" % (space_name, get_space_num()))

    waiting_index_finish(total_batch * batch_size, 1)

    data = {}
    data["db_name"] = db_name
    data["space_name"] = space_name
    data["vectors"] = []
    vector_info = {
        "field": "field_vector",
        "feature": xb[:1].flatten().tolist(),
    }
    data["vectors"].append(vector_info)
    data["index_params"] = {"nprobe": 1}

    json_str = json.dumps(data)
    url = router_url + "/document/search"

    rs = requests.post(url, auth=(username, password), data=json_str)
    logger.info(rs.json())
    assert rs.json()["code"] == 0
    # hava result
    assert len(rs.json()["data"]["documents"][0]) >= 1

    data["vectors"] = []
    vector_info = {
        "field": "field_vector",
        "feature": xb[:1].flatten().tolist(),
        "min_score": -10,
        "max_score": -1,
    }
    data["vectors"].append(vector_info)

    json_str = json.dumps(data)
    url = router_url + "/document/search"

    rs = requests.post(url, auth=(username, password), data=json_str)
    logger.info(rs.json())
    assert rs.json()["code"] == 0
    # no result
    assert len(rs.json()["data"]["documents"][0]) == 0

    data["vectors"] = []
    vector_info = {
        "field": "field_vector",
        "feature": xb[:1].flatten().tolist(),
        "min_score": 10000000,
        "max_score": 100000000,
    }
    data["vectors"].append(vector_info)

    json_str = json.dumps(data)
    url = router_url + "/document/search"

    rs = requests.post(url, auth=(username, password), data=json_str)
    logger.info(rs.json())
    assert rs.json()["code"] == 0
    # no result
    assert len(rs.json()["data"]["documents"][0]) == 0

    destroy(router_url, db_name, space_name)


@pytest.mark.parametrize(
    ["index_type"],
    [["IVFPQ"]],
)
def test_vearch_document_search_with_ivfpq_recall(index_type):
    embedding_size = xb.shape[1]
    batch_size = 100
    k = 10
    with_id = True
    full_field = True
    seed = 1
    brute_force_search_threshold = 100
    total_batch = 3

    logger.info(
        "dataset num: %d, total_batch: %d, dimension: %d, search num: %d, topK: %d"
        % (brute_force_search_threshold, total_batch, embedding_size, xq.shape[0], k)
    )

    properties = {}
    properties["fields"] = [
        {
            "name": "field_int",
            "type": "integer",
            "index": {"name": "field_int", "type": "SCALAR"},
        },
        {
            "name": "field_long",
            "type": "long",
            "index": {"name": "field_long", "type": "SCALAR"},
        },
        {
            "name": "field_float",
            "type": "float",
            "index": {"name": "field_float", "type": "SCALAR"},
        },
        {
            "name": "field_double",
            "type": "double",
            "index": {"name": "field_double", "type": "SCALAR"},
        },
        {
            "name": "field_string",
            "type": "string",
            "index": {"name": "field_string", "type": "SCALAR"},
        },
        {
            "name": "field_vector",
            "type": "vector",
            "index": {
                "name": "gamma",
                "type": index_type,
                "params": {
                    "metric_type": "L2",
                    "training_threshold": 3 * brute_force_search_threshold,
                    "ncentroids": 2,
                    "nprobe": 1,
                },
            },
            "dimension": embedding_size,
            # "format": "normalization"
        },
    ]

    create_for_document_test(router_url, embedding_size, properties)

    add(total_batch, batch_size, xb, with_id, full_field)
    logger.info("%s doc_num: %d" % (space_name, get_space_num()))

    waiting_index_finish(total_batch * batch_size, 1)

    data = {}
    data["db_name"] = db_name
    data["space_name"] = space_name
    data["vectors"] = []
    vector_info = {
        "field": "field_vector",
        "feature": xb[:1].flatten().tolist(),
    }
    data["vectors"].append(vector_info)
    data["index_params"] = {"nprobe": 2, "recall_num": 200}
    data["limit"] = 100

    json_str = json.dumps(data)
    url = router_url + "/document/search"

    rs = requests.post(url, auth=(username, password), data=json_str)
    assert rs.json()["code"] == 0
    # hava result
    assert len(rs.json()["data"]["documents"][0]) == 100

    total_batch = 2
    delete_interface(total_batch, batch_size, full_field, seed, "by_ids")
    logger.info("%s doc_num: %d" % (space_name, get_space_num()))
    assert get_space_num() == 100

    rs = requests.post(url, auth=(username, password), data=json_str)
    assert rs.json()["code"] == 0
    # hava result
    assert len(rs.json()["data"]["documents"][0]) == 100

    destroy(router_url, db_name, space_name)


def process_search_error_data(items):
    data = {}
    data["db_name"] = db_name
    data["space_name"] = space_name
    index = items[0]
    batch_size = items[1]
    features = items[2]
    logger = items[3]
    url = router_url + "/document/search"

    [
        wrong_db,
        wrong_space,
        wrong_range_filter,
        wrong_term_filter,
        wrong_filter_index,
        wrong_vector_length,
        wrong_vector_name,
        wrong_vector_type,
        empty_query,
        empty_vector,
        wrong_range_filter_name,
        wrong_term_filter_name,
        wrong_timeout_param,
        timeout,
    ] = items[4]

    if wrong_db:
        data["db_name"] = "wrong_db"
    if wrong_space:
        data["space_name"] = "wrong_space"

    if wrong_range_filter:
        data["vectors"] = []
        vector_info = {
            "field": "field_vector",
            "feature": features[:batch_size].flatten().tolist(),
        }
        data["vectors"].append(vector_info)
        data["filters"] = []
        prepare_wrong_range_filter(data["filters"], index, batch_size)
        data["limit"] = batch_size

    if wrong_term_filter:
        data["vectors"] = []
        vector_info = {
            "field": "field_vector",
            "feature": features[:batch_size].flatten().tolist(),
        }
        data["vectors"].append(vector_info)
        data["filters"] = []
        prepare_wrong_term_filter(data["filters"], index, batch_size)
        data["limit"] = batch_size

    if wrong_filter_index:
        data["vectors"] = []
        vector_info = {
            "field": "field_vector",
            "feature": features[:batch_size].flatten().tolist(),
        }
        data["vectors"].append(vector_info)
        data["filters"] = []
        prepare_wrong_index_filter(data["filters"], index, batch_size)
        data["limit"] = batch_size

    if wrong_range_filter_name:
        data["vectors"] = []
        vector_info = {
            "field": "field_vector",
            "feature": features[:batch_size].flatten().tolist(),
        }
        data["vectors"].append(vector_info)
        data["filters"] = []
        prepare_wrong_range_filter_name(data["filters"], index, batch_size)
        data["limit"] = batch_size

    if wrong_term_filter_name:
        data["vectors"] = []
        vector_info = {
            "field": "field_vector",
            "feature": features[:batch_size].flatten().tolist(),
        }
        data["vectors"].append(vector_info)
        data["filters"] = []
        prepare_wrong_term_filter_name(data["filters"], index, batch_size)
        data["limit"] = batch_size

    if wrong_vector_length:
        data["vectors"] = []
        vector_info = {
            "field": "field_vector",
            "feature": features[:batch_size].flatten()[:1].tolist(),
        }
        data["vectors"].append(vector_info)

    if wrong_vector_name:
        data["vectors"] = []
        vector_info = {
            "field": "wrong_name",
            "feature": features[:batch_size].flatten().tolist(),
        }
        data["vectors"].append(vector_info)

    if wrong_vector_type:
        data["vectors"] = []
        vector_info = {
            "field": "field_int",
            "feature": features[:batch_size].flatten().tolist(),
        }
        data["vectors"].append(vector_info)

    if empty_vector:
        data["vectors"] = []

    if wrong_timeout_param:
        url = url + "?timeout=10.5"

    if timeout:
        url = url + "?timeout=1"
        data["vectors"] = []
        vector_info = {
            "field": "field_vector",
            "feature": features[:batch_size].flatten().tolist(),
        }
        data["vectors"].append(vector_info)

    json_str = json.dumps(data)
    logger.info(json_str)

    rs = requests.post(
        url, auth=(username, password), data=json_str, headers={"Connection": "close"}
    )
    logger.info(rs.json())

    if "code" in rs.json():
        assert rs.json()["code"] != 0
    else:
        assert rs.status_code != 200

    if timeout:
        # waiting for faulty node expired
        time.sleep(60)


def search_error(total, batch_size, xb, wrong_parameters: dict):
    for i in range(total):
        process_search_error_data(
            (
                i,
                batch_size,
                xb[i * batch_size : (i + 1) * batch_size],
                logger,
                wrong_parameters,
            )
        )


class TestDocumentSearchBadCase:
    def setup_class(self):
        self.xb = xb

    # prepare for badcase
    def test_prepare_cluster_badcase(self):
        prepare_cluster_for_document_test(100, self.xb)

    @pytest.mark.parametrize(
        ["index", "wrong_type"],
        [
            [0, "wrong_db"],
            [1, "wrong_space"],
            [2, "wrong_range_filter"],
            [3, "wrong_term_filter"],
            [4, "wrong_filter_index"],
            [5, "wrong_vector_length"],
            [6, "wrong_vector_name"],
            [7, "wrong_vector_type"],
            [8, "empty_query"],
            [9, "empty_vector"],
            [10, "wrong_range_filter_name"],
            [11, "wrong_term_filter_name"],
            [12, "wrong_timeout_param"],
            [13, "timeout"],
        ],
    )
    def test_vearch_document_search_badcase(self, index, wrong_type):
        wrong_parameters = [False for i in range(14)]
        wrong_parameters[index] = True
        search_error(1, 1, self.xb, wrong_parameters)

    # destroy for badcase
    def test_destroy_cluster_badcase(self):
        destroy(router_url, db_name, space_name)


class TestDocumentSearchEmptyShard:
    def setup_class(self):
        self.xb = xb

    # prepare for cluster
    def test_prepare_cluster(self):
        embedding_size = self.xb.shape[1]
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
                "name": "field_long",
                "type": "long",
            },
            {
                "name": "field_float",
                "type": "float",
            },
            {
                "name": "field_double",
                "type": "double",
                "index": {
                    "name": "field_double",
                    "type": "SCALAR",
                },
            },
            {
                "name": "field_string",
                "type": "string",
                "index": {
                    "name": "field_string",
                    "type": "SCALAR",
                },
            },
            {
                "name": "field_vector",
                "type": "vector",
                "index": {
                    "name": "gamma",
                    "type": "FLAT",
                    "params": {
                        "metric_type": "L2",
                    },
                },
                "dimension": embedding_size,
                "store_type": "MemoryOnly",
                # "format": "normalization"
            },
        ]
        create_for_document_test(router_url, embedding_size, properties, 5)

    def test_vearch_document_search(self):
        total_batch = 1
        batch_size = 1
        with_id = False
        full_field = True

        data = {}
        data["db_name"] = db_name
        data["space_name"] = space_name
        data["limit"] = 10
        data["vectors"] = []
        vector_info = {
            "field": "field_vector",
            "feature": xb[:1].flatten().tolist(),
        }
        data["vectors"].append(vector_info)

        json_str = json.dumps(data)
        url = router_url + "/document/search"

        rs = requests.post(url, auth=(username, password), data=json_str)
        assert rs.json()["code"] == 0
        # hava result
        assert len(rs.json()["data"]["documents"][0]) == 0

        add(total_batch, batch_size, xb, with_id, full_field)

        rs = requests.post(url, auth=(username, password), data=json_str)
        assert rs.json()["code"] == 0
        # hava result
        assert len(rs.json()["data"]["documents"][0]) == 1

        add(total_batch, batch_size, xb, with_id, full_field)

        rs = requests.post(url, auth=(username, password), data=json_str)
        assert rs.json()["code"] == 0
        # hava result
        assert len(rs.json()["data"]["documents"][0]) == 2

        add(total_batch, batch_size, xb, with_id, full_field)

        rs = requests.post(url, auth=(username, password), data=json_str)
        assert rs.json()["code"] == 0
        # hava result
        assert len(rs.json()["data"]["documents"][0]) == 3

        add(total_batch, batch_size, xb, with_id, full_field)

        rs = requests.post(url, auth=(username, password), data=json_str)
        assert rs.json()["code"] == 0
        # hava result
        assert len(rs.json()["data"]["documents"][0]) == 4

        add(total_batch, batch_size, xb, with_id, full_field)

        rs = requests.post(url, auth=(username, password), data=json_str)
        assert rs.json()["code"] == 0
        # hava result
        assert len(rs.json()["data"]["documents"][0]) == 5

    # destroy
    def test_destroy_cluster(self):
        destroy(router_url, db_name, space_name)


class TestDocumentSearchReturnFields:
    def setup_class(self):
        self.xb = xb

    # prepare for cluster
    def test_prepare_cluster(self):
        embedding_size = self.xb.shape[1]
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
                "name": "field_long",
                "type": "long",
            },
            {
                "name": "field_float",
                "type": "float",
            },
            {
                "name": "field_double",
                "type": "double",
                "index": {
                    "name": "field_double",
                    "type": "SCALAR",
                },
            },
            {
                "name": "field_string",
                "type": "string",
                "index": {
                    "name": "field_string",
                    "type": "SCALAR",
                },
            },
            {
                "name": "field_vector",
                "type": "vector",
                "index": {
                    "name": "gamma",
                    "type": "FLAT",
                    "params": {
                        "metric_type": "L2",
                    },
                },
                "dimension": embedding_size,
                "store_type": "MemoryOnly",
                # "format": "normalization"
            },
        ]
        create_for_document_test(router_url, embedding_size, properties, 5)

    def test_vearch_document_search(self):
        total_batch = 1
        batch_size = 1
        with_id = False
        full_field = True

        add(total_batch, batch_size, xb, with_id, full_field)

        data = {}
        data["db_name"] = db_name
        data["space_name"] = space_name
        data["limit"] = 10
        data["filters"] = {
            "operator": "AND",
            "conditions": [
                {
                    "field": "field_int",
                    "operator": ">=",
                    "value": 0,
                },
            ],
        }
        data["vectors"] = []
        vector_info = {
            "field": "field_vector",
            "feature": xb[:batch_size].flatten().tolist(),
        }
        data["vectors"].append(vector_info)

        url = router_url + "/document/search"

        data["fields"] = []
        json_str = json.dumps(data)
        rs = requests.post(url, auth=(username, password), data=json_str)
        assert rs.json()["code"] == 0
        assert len(rs.json()["data"]["documents"][0][0]) == 7

        data["fields"] = ["_id"]
        json_str = json.dumps(data)
        rs = requests.post(url, auth=(username, password), data=json_str)
        assert rs.json()["code"] == 0
        assert len(rs.json()["data"]["documents"][0][0]) == 2

        data["fields"] = ["_id", "field_int"]
        json_str = json.dumps(data)
        rs = requests.post(url, auth=(username, password), data=json_str)
        assert rs.json()["code"] == 0
        assert len(rs.json()["data"]["documents"][0][0]) == 3
        for field in data["fields"]:
            assert field in rs.json()["data"]["documents"][0][0]

        data["fields"] = ["field_int"]
        json_str = json.dumps(data)
        rs = requests.post(url, auth=(username, password), data=json_str)
        assert rs.json()["code"] == 0
        assert len(rs.json()["data"]["documents"][0][0]) == 3
        for field in data["fields"]:
            assert field in rs.json()["data"]["documents"][0][0]

        data["fields"] = ["field_int", "field_long"]
        json_str = json.dumps(data)
        rs = requests.post(url, auth=(username, password), data=json_str)
        assert rs.json()["code"] == 0
        assert len(rs.json()["data"]["documents"][0][0]) == 4
        for field in data["fields"]:
            assert field in rs.json()["data"]["documents"][0][0]

        data["fields"] = ["field_int", "field_long", "field_float"]
        json_str = json.dumps(data)
        rs = requests.post(url, auth=(username, password), data=json_str)
        assert rs.json()["code"] == 0
        assert len(rs.json()["data"]["documents"][0][0]) == 5
        for field in data["fields"]:
            assert field in rs.json()["data"]["documents"][0][0]

        data["fields"] = ["field_int", "field_long", "field_float", "field_double"]
        json_str = json.dumps(data)
        rs = requests.post(url, auth=(username, password), data=json_str)
        assert rs.json()["code"] == 0
        assert len(rs.json()["data"]["documents"][0][0]) == 6
        for field in data["fields"]:
            assert field in rs.json()["data"]["documents"][0][0]

        data["fields"] = [
            "field_int",
            "field_long",
            "field_float",
            "field_double",
            "field_string",
        ]
        json_str = json.dumps(data)
        rs = requests.post(url, auth=(username, password), data=json_str)
        assert rs.json()["code"] == 0
        assert len(rs.json()["data"]["documents"][0][0]) == 7
        for field in data["fields"]:
            assert field in rs.json()["data"]["documents"][0][0]

        data["fields"] = [
            "field_int",
            "field_long",
            "field_float",
            "field_double",
            "field_string",
            "field_vector",
        ]
        json_str = json.dumps(data)
        rs = requests.post(url, auth=(username, password), data=json_str)
        assert rs.json()["code"] == 0
        assert len(rs.json()["data"]["documents"][0][0]) == 8
        for field in data["fields"]:
            assert field in rs.json()["data"]["documents"][0][0]

    # destroy
    def test_destroy_cluster(self):
        destroy(router_url, db_name, space_name)


class TestDocumentSearchPagination:
    def setup_class(self):
        self.xb = xb

    def test_prepare_cluster(self):
        embedding_size = self.xb.shape[1]
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
                "name": "field_long",
                "type": "long",
            },
            {
                "name": "field_float",
                "type": "float",
            },
            {
                "name": "field_double",
                "type": "double",
            },
            {
                "name": "field_string",
                "type": "string",
                "index": {
                    "name": "field_string",
                    "type": "SCALAR",
                },
            },
            {
                "name": "field_vector",
                "type": "vector",
                "index": {
                    "name": "gamma",
                    "type": "FLAT",
                    "params": {
                        "metric_type": "L2",
                    },
                },
                "dimension": embedding_size,
                "store_type": "MemoryOnly",
            },
        ]
        create_for_document_test(router_url, embedding_size, properties)

    def test_numberic_pagination(self):
        total_batch = 10
        batch_size = 10
        total = total_batch * batch_size
        add(total_batch, batch_size, self.xb, with_id=True, full_field=True)

        data = {
            "db_name": db_name,
            "space_name": space_name,
            "limit": 10,
            "page_size": 5,
            "page_num": 1,
            "offset": 0,
        }

        data["filters"] = {
            "operator": "AND",
            "conditions": [
                {
                    "field": "field_int",
                    "operator": ">=",
                    "value": 0,
                },
            ],
        }

        data["vectors"] = []
        vector_info = {
            "field": "field_vector",
            "feature": xb[:batch_size].flatten().tolist(),
        }
        data["vectors"].append(vector_info)

        url = router_url + "/document/search"

        # Test with initial offset
        json_str = json.dumps(data)
        response = requests.post(url, auth=(username, password), data=json_str)
        assert response.status_code == 200
        assert response.json()["code"] == 0
        assert len(response.json()["data"]["documents"]) == batch_size
        for documents in response.json()["data"]["documents"]:
            assert len(documents) == 5

        data["page_num"] = 2
        json_str = json.dumps(data)
        response = requests.post(url, auth=(username, password), data=json_str)
        assert response.status_code == 200
        assert response.json()["code"] == 0
        assert len(response.json()["data"]["documents"]) == batch_size
        for documents in response.json()["data"]["documents"]:
            assert len(documents) == 5

        data["offset"] = 5
        data["page_num"] = 1
        json_str = json.dumps(data)
        response = requests.post(url, auth=(username, password), data=json_str)
        assert response.status_code == 200
        assert response.json()["code"] == 0
        assert len(response.json()["data"]["documents"]) == batch_size
        for documents in response.json()["data"]["documents"]:
            assert len(documents) == 5

        # Test offset in the middle
        data["offset"] = 15
        json_str = json.dumps(data)
        response = requests.post(url, auth=(username, password), data=json_str)
        assert response.status_code == 200
        assert response.json()["code"] == 0
        assert len(response.json()["data"]["documents"]) == batch_size
        for documents in response.json()["data"]["documents"]:
            assert len(documents) == 5

        data["page_num"] = 2
        json_str = json.dumps(data)
        response = requests.post(url, auth=(username, password), data=json_str)
        assert response.status_code == 200
        assert response.json()["code"] == 0
        assert len(response.json()["data"]["documents"]) == batch_size
        for documents in response.json()["data"]["documents"]:
            assert len(documents) == 5

        # Test offset at the end
        data["offset"] = total - 5
        data["page_num"] = 1
        json_str = json.dumps(data)
        response = requests.post(url, auth=(username, password), data=json_str)
        assert response.status_code == 200
        assert response.json()["code"] == 0
        assert len(response.json()["data"]["documents"]) == batch_size
        for documents in response.json()["data"]["documents"]:
            assert len(documents) == 5

        # Test offset out of bounds
        data["offset"] = total + 10
        json_str = json.dumps(data)
        response = requests.post(url, auth=(username, password), data=json_str)
        assert response.status_code == 200
        assert response.json()["code"] == 0
        assert len(response.json()["data"]["documents"]) == batch_size
        for documents in response.json()["data"]["documents"]:
            assert len(documents) == 0

        # Test offset half out of bounds
        data["offset"] = total - 2
        json_str = json.dumps(data)
        response = requests.post(url, auth=(username, password), data=json_str)
        assert response.status_code == 200
        assert response.json()["code"] == 0
        assert len(response.json()["data"]["documents"]) == batch_size
        for documents in response.json()["data"]["documents"]:
            assert len(documents) == 2

    def test_string_pagination(self):
        total_batch = 10
        batch_size = 10
        total = total_batch * batch_size

        data = {
            "db_name": db_name,
            "space_name": space_name,
            "limit": 10,
            "page_size": 5,
            "page_num": 1,
            "offset": 0,
        }

        data["filters"] = {
            "operator": "AND",
            "conditions": [
                {
                    "field": "field_string",
                    "operator": "IN",
                    "value": [str(i) for i in range(0, total)],
                },
            ],
        }

        data["vectors"] = []
        vector_info = {
            "field": "field_vector",
            "feature": xb[:batch_size].flatten().tolist(),
        }
        data["vectors"].append(vector_info)

        url = router_url + "/document/search"

        # Test with initial offset
        json_str = json.dumps(data)
        response = requests.post(url, auth=(username, password), data=json_str)
        assert response.status_code == 200
        assert response.json()["code"] == 0
        assert len(response.json()["data"]["documents"]) == batch_size
        for documents in response.json()["data"]["documents"]:
            assert len(documents) == 5

        data["page_num"] = 2
        json_str = json.dumps(data)
        response = requests.post(url, auth=(username, password), data=json_str)
        assert response.status_code == 200
        assert response.json()["code"] == 0
        assert len(response.json()["data"]["documents"]) == batch_size
        for documents in response.json()["data"]["documents"]:
            assert len(documents) == 5

        data["offset"] = 5
        data["page_num"] = 1
        json_str = json.dumps(data)
        response = requests.post(url, auth=(username, password), data=json_str)
        assert response.status_code == 200
        assert response.json()["code"] == 0
        assert len(response.json()["data"]["documents"]) == batch_size
        for documents in response.json()["data"]["documents"]:
            assert len(documents) == 5

        # Test offset in the middle
        data["offset"] = 15
        json_str = json.dumps(data)
        response = requests.post(url, auth=(username, password), data=json_str)
        assert response.status_code == 200
        assert response.json()["code"] == 0
        assert len(response.json()["data"]["documents"]) == batch_size
        for documents in response.json()["data"]["documents"]:
            assert len(documents) == 5

        data["page_num"] = 2
        json_str = json.dumps(data)
        response = requests.post(url, auth=(username, password), data=json_str)
        assert response.status_code == 200
        assert response.json()["code"] == 0
        assert len(response.json()["data"]["documents"]) == batch_size
        for documents in response.json()["data"]["documents"]:
            assert len(documents) == 5

        # Test offset at the end
        data["offset"] = total - 5
        data["page_num"] = 1
        json_str = json.dumps(data)
        response = requests.post(url, auth=(username, password), data=json_str)
        assert response.status_code == 200
        assert response.json()["code"] == 0
        assert len(response.json()["data"]["documents"]) == batch_size
        for documents in response.json()["data"]["documents"]:
            assert len(documents) == 5

        # Test offset out of bounds
        data["offset"] = total + 10
        json_str = json.dumps(data)
        response = requests.post(url, auth=(username, password), data=json_str)
        assert response.status_code == 200
        assert response.json()["code"] == 0
        assert len(response.json()["data"]["documents"]) == batch_size
        for documents in response.json()["data"]["documents"]:
            assert len(documents) == 0

        # Test offset half out of bounds
        data["offset"] = total - 2
        json_str = json.dumps(data)
        response = requests.post(url, auth=(username, password), data=json_str)
        assert response.status_code == 200
        assert response.json()["code"] == 0
        assert len(response.json()["data"]["documents"]) == batch_size
        for documents in response.json()["data"]["documents"]:
            assert len(documents) == 2

    def test_destroy_cluster(self):
        destroy(router_url, db_name, space_name)