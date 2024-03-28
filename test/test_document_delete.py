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

__description__ = """ test case for document delete """


xb, xq, _, gt = get_sift10K(logger)


def check(total, bulk, full_field, delete_type, xb):
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
        {"name": "field_int", "type": "integer", "index": {"name": "field_int","type": "SCALAR"}},
        {"name": "field_long", "type": "long", "index": {"name": "field_long","type": "SCALAR"}},
        {"name": "field_float", "type": "float", "index": {"name": "field_float","type": "SCALAR"}},
        {"name": "field_double", "type": "double", "index": {"name": "field_double","type": "SCALAR"}},
        {"name": "field_string", "type": "string", "index": {"name": "field_string","type": "SCALAR"}},
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

    if delete_type == "by_filter":
        time.sleep(3)

    query_interface(logger, total_batch, batch_size, xb, full_field, seed, "by_ids")

    delete_interface(logger, total_batch, batch_size, full_field, seed, delete_type)

    assert get_space_num() == 0

    destroy(router_url, db_name, space_name)


@pytest.mark.parametrize(
    ["bulk", "full_field", "delete_type"],
    [
        [True, True, "by_ids"],
        [True, True, "by_filter"],
        [False, True, "by_ids"],
        [False, True, "by_filter"],
    ],
)
def test_vearch_document_delete(bulk: bool, full_field: bool, delete_type: str):
    check(100, bulk, full_field, delete_type, xb)


class TestDocumentDeleteBadCase:
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
        ],
    )
    def test_vearch_document_delete_badcase(self, index, wrong_type):
        wrong_parameters = [False for i in range(16)]
        wrong_parameters[index] = True
        query_error(self.logger, 1, 1, self.xb, "delete", wrong_parameters)

    # destroy for badcase
    def test_destroy_cluster_badcase(self):
        destroy(router_url, db_name, space_name)
