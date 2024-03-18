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

__description__ = """ test case for document search """


xb, xq, _, gt = get_sift10K(logger)


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
    properties["fields"] = {
        "field_int": {"type": "integer", "index": True},
        "field_long": {"type": "long", "index": True},
        "field_float": {"type": "float", "index": True},
        "field_double": {"type": "double", "index": True},
        "field_string": {"type": "string", "index": True},
        "field_vector": {
            "type": "vector",
            "index": True,
            "dimension": embedding_size,
            "store_type": "MemoryOnly",
            # "format": "normalization"
        },
    }

    create_for_document_test(logger, router_url, embedding_size, properties)

    add(total_batch, batch_size, xb, with_id, full_field)

    logger.info("%s doc_num: %d" % (space_name, get_space_num()))

    if with_filter:
        time.sleep(3)

    search_interface(
        logger, total_batch, batch_size, xb, full_field, with_filter, seed, query_type
    )

    destroy(router_url, db_name, space_name)


@pytest.mark.parametrize(
    ["bulk", "full_field", "with_filter", "query_type"],
    [
        [True, True, True, "by_ids"],
        [True, True, False, "by_ids"],
        [True, True, True, "by_vector"],
        [True, True, False, "by_vector"],
        [False, True, True, "by_ids"],
        [False, True, False, "by_ids"],
        [False, True, True, "by_vector"],
        [False, True, False, "by_vector"],
        [False, True, False, "by_vector_with_symbol"],
    ],
)
def test_vearch_document_search(
    bulk: bool, full_field: bool, with_filter: bool, query_type: str
):
    check(100, bulk, full_field, with_filter, query_type, xb)


def process_search_error_data(items):
    data = {}
    data["db_name"] = db_name
    data["space_name"] = space_name
    data["query"] = {}
    index = items[0]
    batch_size = items[1]
    features = items[2]
    logger = items[3]
    url = router_url + "/document/search"

    [
        wrong_db,
        wrong_space,
        wrong_id,
        wrong_range_filter,
        wrong_term_filter,
        wrong_filter_index,
        wrong_vector_length,
        wrong_vector_name,
        wrong_vector_type,
        wrong_length_document_ids,
        wrong_both_id_and_vector,
        empty_query,
        empty_document_ids,
        empty_vector,
        wrong_range_filter_name,
        wrong_term_filter_name,
    ] = items[4]

    max_document_ids_length = 101

    if wrong_db:
        data["db_name"] = "wrong_db"
    if wrong_space:
        data["space_name"] = "wrong_space"

    if wrong_id:
        data["query"]["document_ids"] = ["wrong_id"]

    if wrong_range_filter:
        data["query"]["document_ids"] = ["0"]
        data["query"]["filter"] = []
        prepare_wrong_range_filter(data["query"]["filter"], index, batch_size)
        data["size"] = batch_size

    if wrong_term_filter:
        data["query"]["document_ids"] = ["0"]
        data["query"]["filter"] = []
        prepare_wrong_term_filter(data["query"]["filter"], index, batch_size)
        data["size"] = batch_size

    if wrong_filter_index:
        data["query"]["document_ids"] = ["0"]
        data["query"]["filter"] = []
        prepare_wrong_index_filter(data["query"]["filter"], index, batch_size)
        data["size"] = batch_size

    if wrong_range_filter_name:
        data["query"]["document_ids"] = ["0"]
        data["query"]["filter"] = []
        prepare_wrong_range_filter_name(data["query"]["filter"], index, batch_size)
        data["size"] = batch_size

    if wrong_term_filter_name:
        data["query"]["document_ids"] = ["0"]
        data["query"]["filter"] = []
        prepare_wrong_term_filter_name(data["query"]["filter"], index, batch_size)
        data["size"] = batch_size

    if wrong_vector_length:
        data["query"]["vector"] = []
        vector_info = {
            "field": "field_vector",
            "feature": features[:batch_size].flatten()[:1].tolist(),
        }
        data["query"]["vector"].append(vector_info)

    if wrong_vector_name:
        data["query"]["vector"] = []
        vector_info = {
            "field": "wrong_name",
            "feature": features[:batch_size].flatten().tolist(),
        }
        data["query"]["vector"].append(vector_info)

    if wrong_vector_type:
        data["query"]["vector"] = []
        vector_info = {
            "field": "field_int",
            "feature": features[:batch_size].flatten().tolist(),
        }
        data["query"]["vector"].append(vector_info)

    if wrong_length_document_ids:
        data["query"]["document_ids"] = [str(i) for i in range(max_document_ids_length)]

    if wrong_both_id_and_vector:
        data["query"]["document_ids"] = ["0"]
        data["query"]["vector"] = []
        vector_info = {
            "field": "field_vector",
            "feature": features[:batch_size].flatten().tolist(),
        }
        data["query"]["vector"].append(vector_info)

    if empty_query:
        data["query"] = {}

    if empty_document_ids:
        data["query"]["document_ids"] = []

    if empty_vector:
        data["query"]["vector"] = []

    json_str = json.dumps(data)
    logger.info(json_str)

    rs = requests.post(url, data=json_str, headers={"Connection": "close"})
    logger.info(rs.json())

    if "documents" not in rs.json():
        assert rs.status_code != 200
    else:
        if len(rs.json()["documents"]) > 0:
            assert len(rs.json()["documents"][0]) == 0


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
    def setup(self):
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
            [3, "wrong_range_filter"],
            [4, "wrong_term_filter"],
            [5, "wrong_filter_index"],
            [6, "wrong_vector_length"],
            [7, "wrong_vector_name"],
            [8, "wrong_vector_type"],
            [9, "wrong_length_document_ids"],
            [10, "wrong_both_id_and_vector"],
            [11, "empty_query"],
            [12, "empty_document_ids"],
            [13, "empty_vector"],
            [14, "wrong_range_filter_name"],
            [15, "wrong_term_filter_name"],
        ],
    )
    def test_vearch_document_search_badcase(self, index, wrong_type):
        wrong_parameters = [False for i in range(16)]
        wrong_parameters[index] = True
        search_error(self.logger, 1, 1, self.xb, wrong_parameters)

    # destroy for badcase
    def test_destroy_cluster_badcase(self):
        destroy(router_url, db_name, space_name)
