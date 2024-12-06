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

__description__ = """ test case for document upsert """


def query(xq, gt, k):
    query_dict = {
        "vectors": [],
        "vector_value": False,
        "fields": ["field_int"],
        "limit": k,
        "db_name": db_name,
        "space_name": space_name,
    }

    for batch in [True]:
        avarage, recalls = evaluate(xq, gt, k, batch, query_dict)
        result = "batch: %d, avarage time: %.2f ms, " % (batch, avarage)
        for recall in recalls:
            result += "recall@%d = %.2f%% " % (recall, recalls[recall] * 100)
            if recall == k:
                assert recalls[recall] >= 1.0
        logger.info(result)


sift10k = DatasetSift10K()
xb = sift10k.get_database()
xq = sift10k.get_queries()
gt = sift10k.get_groundtruth()


def benchmark(total, bulk, with_id, full_field, xb, xq, gt):
    embedding_size = xb.shape[1]
    batch_size = 1
    if bulk:
        batch_size = 100
    k = 100
    if total == 0:
        total = xb.shape[0]
    total_batch = int(total / batch_size)
    logger.info(
        "dataset num: %d, total_batch: %d, dimension: %d, search num: %d, topK: %d"
        % (total, total_batch, embedding_size, xq.shape[0], k)
    )

    properties = {}
    properties["fields"] = [
        {"name": "field_int", "type": "integer"},
        {"name": "field_long", "type": "long"},
        {"name": "field_float", "type": "float"},
        {"name": "field_double", "type": "double"},
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

    waiting_index_finish(total, 1)

    query(xq, gt, k)

    destroy(router_url, db_name, space_name)


@pytest.mark.parametrize(
    ["bulk", "with_id", "full_field"],
    [
        [True, True, True],
        [True, True, False],
        [True, False, True],
        [True, False, False],
        [False, True, True],
        [False, True, False],
        [False, False, True],
        [False, False, False],
    ],
)
def test_vearch_document_upsert_benchmark(bulk: bool, with_id: bool, full_field: bool):
    benchmark(0, bulk, with_id, full_field, xb, xq, gt)


def update(total, bulk, full_field, xb):
    embedding_size = xb.shape[1]
    batch_size = 1
    if bulk:
        batch_size = 100
    k = 100
    if total == 0:
        total = xb.shape[0]
    total_batch = int(total / batch_size)
    with_id = True

    logger.info(
        "dataset num: %d, total_batch: %d, dimension: %d, search num: %d, topK: %d"
        % (total, total_batch, embedding_size, xq.shape[0], k)
    )

    properties = {}
    properties["fields"] = [
        {"name": "field_int", "type": "integer"},
        {"name": "field_long", "type": "long"},
        {"name": "field_float", "type": "float"},
        {"name": "field_double", "type": "double"},
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

    query_interface(total_batch, batch_size, xb, full_field)

    add(total_batch, batch_size, xb, with_id, full_field, 2)

    query_interface(total_batch, batch_size, xb, full_field, 2)

    destroy(router_url, db_name, space_name)


@pytest.mark.parametrize(
    ["bulk", "full_field"],
    [
        [True, True],
        [True, False],
        [False, True],
        [False, False],
    ],
)
def test_vearch_document_upsert_update(bulk: bool, full_field: bool):
    update(100, bulk, full_field, xb)


class TestDocumentUpsertBadCase:
    def setup_class(self):
        self.xb = xb

    # prepare for badcase
    def test_prepare_cluster_badcase(self):
        embedding_size = xb.shape[1]

        properties = {}
        properties["fields"] = [
            {"name": "field_int", "type": "integer"},
            {"name": "field_long", "type": "long"},
            {"name": "field_float", "type": "float"},
            {"name": "field_double", "type": "double"},
            {
                "name": "field_string",
                "type": "string",
                "index": {"name": "field_string", "type": "SCALAR"},
            },
            {"name": "field_string1", "type": "string"},
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

    @pytest.mark.parametrize(
        ["index", "wrong_type"],
        [
            [0, "wrong_number_value"],
            [1, "wrong_str_value"],
            [2, "without_vector"],
            [3, "wrong_db"],
            [4, "wrong_space"],
            [5, "wrong_field"],
            [6, "empty_documents"],
            [7, "wrong_index_string_length"],
            [8, "wrong_string_length"],
            [9, "wrong_vector_type"],
            [10, "wrong_vector_feature_length"],
            [11, "wrong_vector_feature_type"],
            [12, "mismatch_field_type"],
            [13, "wrong partition id"],
        ],
    )
    def test_vearch_document_upsert_badcase(self, index, wrong_type):
        wrong_parameters = [False for i in range(14)]
        wrong_parameters[index] = True
        batch_size = 1
        total = 1
        if total == 0:
            total = xb.shape[0]
        total_batch = int(total / batch_size)
        add_error(total_batch, batch_size, self.xb, wrong_parameters)
        assert get_space_num() == 0

    @pytest.mark.parametrize(
        ["index", "wrong_type"],
        [
            [0, "params_both_wrong"],
            [1, "params_just_one_wrong"],
            [2, "params_just_one_wrong_with_bad_vector"],
        ],
    )
    def test_vearch_document_upsert_multiple_badcase(self, index: int, wrong_type: str):
        wrong_parameters = [False for i in range(3)]
        wrong_parameters[index] = True
        total_batch = 1
        batch_size = 2
        add_error(total_batch, batch_size, self.xb, wrong_parameters)

        assert get_space_num() == 0

    # destroy for badcase
    def test_destroy_cluster_badcase(self):
        destroy(router_url, db_name, space_name)


class TestDocumentUpsertDuplicate:
    def setup_class(self):
        self.xb = xb

    # prepare for badcase
    def test_prepare_cluster_duplicate(self):
        embedding_size = xb.shape[1]

        properties = {}
        properties["fields"] = [
            {"name": "field_int", "type": "integer"},
            {"name": "field_long", "type": "long"},
            {"name": "field_float", "type": "float"},
            {"name": "field_double", "type": "double"},
            {
                "name": "field_string",
                "type": "string",
                "index": {"name": "field_string", "type": "SCALAR"},
            },
            {"name": "field_string1", "type": "string"},
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

    def test_vearch_document_upsert_duplicate(self):
        batch_size = 1
        total = 1
        url = router_url + "/document/upsert"
        data = {}
        data["db_name"] = db_name
        data["space_name"] = space_name
        data["documents"] = []

        for j in range(2):
            param_dict = {}
            param_dict["_id"] = "00001"
            param_dict["field_int"] = j
            param_dict["field_vector"] = xb[j].tolist()
            param_dict["field_long"] = param_dict["field_int"]
            param_dict["field_float"] = float(param_dict["field_int"])
            param_dict["field_double"] = float(param_dict["field_int"])
            param_dict["field_string"] = str(param_dict["field_int"])
            data["documents"].append(param_dict)

        for j in range(2, 3):
            param_dict = {}
            param_dict["_id"] = "0000" + str(j)
            param_dict["field_int"] = j
            param_dict["field_vector"] = xb[j].tolist()
            param_dict["field_long"] = param_dict["field_int"]
            param_dict["field_float"] = float(param_dict["field_int"])
            param_dict["field_double"] = float(param_dict["field_int"])
            param_dict["field_string"] = str(param_dict["field_int"])
            data["documents"].append(param_dict)

        rs = requests.post(url, auth=(username, password), json=data)
        logger.info(rs.json())
        assert rs.json()["code"] == 0
        assert get_space_num() == 2

    # destroy for badcase
    def test_destroy_cluster_duplicate(self):
        destroy(router_url, db_name, space_name)
