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
import time
from utils.vearch_utils import *
from utils.data_utils import *

logging.basicConfig()
logger = logging.getLogger(__name__)

__description__ = """ test case for document query """


sift10k = DatasetSift10K(logger)
xb = sift10k.get_database()
xq = sift10k.get_queries()
gt = sift10k.get_groundtruth()


def check(total, bulk, full_field, query_type, xb):
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

    if query_type == "by_filter":
        time.sleep(3)

    query_interface(logger, total_batch, batch_size, xb, full_field, seed, query_type)

    destroy(router_url, db_name, space_name)


@pytest.mark.parametrize(
    ["bulk", "full_field", "query_type"],
    [
        [True, True, "by_ids"],
        [True, True, "by_filter"],
        [True, True, "by_partition"],
        [True, True, "by_partition_next"],
        [False, True, "by_ids"],
        [False, True, "by_filter"],
        [False, True, "by_partition"],
        [False, True, "by_partition_next"],
    ],
)
def test_vearch_document_query(bulk: bool, full_field: bool, query_type: str):
    check(100, bulk, full_field, query_type, xb)


class TestDocumentQueryMultiPartition:
    def setup_class(self):
        self.logger = logger
        self.xb = xb

    # prepare for badcase
    def test_prepare_cluster_multi_partition(self):
        prepare_cluster_for_document_test(self.logger, 100, self.xb, 2)

    def test_destroy_cluster_multi_partition(self):
        destroy(router_url, db_name, space_name)


class TestDocumentQueryBadCase:
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
            [2, "wrong_id"],
            [3, "wrong_partition"],
            [4, "wrong_range_filter"],
            [5, "wrong_term_filter"],
            [6, "wrong_filter_index"],
            [7, "wrong_vector"],
            [8, "wrong_length_document_ids"],
            [9, "wrong_both_id_and_filter"],
            [10, "empty_query"],
            [11, "empty_document_ids"],
            [12, "empty_filter"],
            [13, "wrong_range_filter_name"],
            [14, "wrong_term_filter_name"],
            [15, "out_of_bounds_ids"],
            [16, "wrong_partition_of_bad_type"],
        ],
    )
    def test_document_query_badcase(self, index, wrong_type):
        wrong_parameters = [False for i in range(17)]
        wrong_parameters[index] = True
        query_error(self.logger, 1, 1, self.xb, "query", wrong_parameters)

    def test_prepare_add_for_badcase(self):
        add(1, 100, self.xb, with_id=True, full_field=True)

    @pytest.mark.parametrize(
        ["index", "wrong_type"],
        [
            [0, "params_both_wrong"],
            [1, "params_just_one_wrong"],
            [2, "return_both_wrong"],
            [3, "return_just_one_wrong"],
        ],
    )
    def test_document_query_multiple_badcase(self, index: int, wrong_type: str):
        wrong_parameters = [False for i in range(4)]
        wrong_parameters[index] = True
        total_batch = 1
        batch_size = 2
        query_error(
            self.logger, total_batch, batch_size, self.xb, "query", wrong_parameters
        )

    # destroy for badcase

    def test_destroy_cluster_badcase(self):
        destroy(router_url, db_name, space_name)


class TestDocumentQueryOnSpecifyPartition:
    def setup_class(self):
        self.logger = logger
        self.xb = xb

    # prepare for badcase
    def test_prepare_cluster(self):
        prepare_cluster_for_document_test(self.logger, 100, self.xb)

    def test_prepare_add(self):
        batch_size = 100
        total_batch = 1
        total = int(batch_size * total_batch)
        add(total_batch, batch_size, self.xb, with_id=True, full_field=True)

        waiting_index_finish(logger, total, 1)

        partition_ids = get_partition(router_url, db_name, space_name)
        assert len(partition_ids) == 1
        partition_id = partition_ids[0]

        query_dict = {
            "document_ids": ["0"],
            "partition_id": partition_id,
            "limit": 1,
            "db_name": db_name,
            "space_name": space_name,
        }
        delete_url = router_url + "/document/delete"
        for i in range(int(total / 2)):
            query_dict["document_ids"] = [str(i)]
            json_str = json.dumps(query_dict)
            response = requests.post(
                delete_url, auth=(username, password), data=json_str
            )
            assert response.json()["data"]["total"] == 1

        query_url = router_url + "/document/query"
        for i in range(int(total / 2)):
            query_dict["document_ids"] = [str(i)]
            query_dict["partition_id"] = partition_id
            query_dict["next"] = True
            json_str = json.dumps(query_dict)
            response = requests.post(
                query_url, auth=(username, password), data=json_str
            )
            assert len(response.json()["data"]["documents"]) == 1
            assert response.json()["data"]["documents"][0]["_docid"] == "50"

        query_url = router_url + "/document/query"
        for i in range(int(total / 2), total - 1):
            query_dict["document_ids"] = [str(i)]
            query_dict["partition_id"] = partition_id
            query_dict["next"] = True
            json_str = json.dumps(query_dict)
            response = requests.post(
                query_url, auth=(username, password), data=json_str
            )
            assert len(response.json()["data"]["documents"]) == 1
            assert response.json()["data"]["documents"][0]["_docid"] == str(i + 1)

    def test_destroy_cluster(self):
        destroy(router_url, db_name, space_name)
