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

__description__ = """ test case for document search """


sift10k = DatasetSift10K(logger)
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

    create_for_document_test(logger, router_url, embedding_size, properties)

    add(total_batch, batch_size, xb, with_id, full_field)

    logger.info("%s doc_num: %d" % (space_name, get_space_num()))

    search_interface(
        logger, total_batch, batch_size, xb, full_field, with_filter, seed, query_type
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

    create_for_document_test(logger, router_url, embedding_size, properties)

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

    json_str = json.dumps(data)
    logger.info(json_str)

    rs = requests.post(
        url, auth=(username, password), data=json_str, headers={"Connection": "close"}
    )
    logger.info(rs.json())

    if "data" in rs.json():
        pass  # TODO
    else:
        assert rs.status_code != 200


def search_error(logger, total, batch_size, xb, wrong_parameters: dict):
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
        self.logger = logger
        self.xb = xb

    # prepare for badcase
    def test_prepare_cluster_badcase(self):
        prepare_cluster_for_document_test(self.logger, 100, self.xb)

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
        ],
    )
    def test_vearch_document_search_badcase(self, index, wrong_type):
        wrong_parameters = [False for i in range(12)]
        wrong_parameters[index] = True
        search_error(self.logger, 1, 1, self.xb, wrong_parameters)

    # destroy for badcase
    def test_destroy_cluster_badcase(self):
        destroy(router_url, db_name, space_name)
