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
from vearch_utils import *

logging.basicConfig()
logger = logging.getLogger(__name__)

__description__ = """ test case for document search """


def create(router_url, embedding_size, properties):
    space_config = {
        "name": space_name,
        "partition_num": 1,
        "replica_num": 1,
        "engine": {
            "name": "gamma",
            "index_size": 1,
            "retrieval_type": "FLAT",
            "retrieval_param": {
                "metric_type": "L2",
            }
        },
        "properties": properties["properties"]
    }
    logger.info(create_db(router_url, db_name))

    logger.info(create_space(router_url, db_name, space_config))

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

    logger.info("dataset num: %d, total_batch: %d, dimension: %d, search num: %d, topK: %d" %(total, total_batch, embedding_size, xq.shape[0], k))

    properties = {}
    properties["properties"] = {
        "field_int": {
            "type": "integer",
            "index": True
        },
        "field_long": {
            "type": "long",
            "index": True
        },
        "field_float": {
            "type": "float",
            "index": True
        },
        "field_double": {
            "type": "double",
            "index": True
        },
        "field_string": {
            "type": "string",
            "index": True
        },
        "field_vector": {
            "type": "vector",
            "index": True,
            "dimension": embedding_size,
            "store_type": "MemoryOnly",
            #"format": "normalization"
        }
    }

    create(router_url, embedding_size, properties)

    add(total_batch, batch_size, xb, with_id, full_field)

    logger.info("%s doc_num: %d" %(space_name, get_space_num()))

    if with_filter:
        time.sleep(3)

    search_interface(logger, total_batch, batch_size, xb, full_field, with_filter, seed, query_type)

    destroy(router_url, db_name, space_name)

@ pytest.mark.parametrize(["bulk", "full_field", "with_filter", "query_type"], [
    [True, True, True, "by_ids"],
    [True, True, False, "by_ids"],
    [True, True, True, "by_vector"],
    [True, True, False, "by_vector"],
    [False, True, True, "by_ids"],
    [False, True, False, "by_ids"],
    [False, True, True, "by_vector"],
    [False, True, False, "by_vector"],
])
def test_vearch_document_search(bulk: bool, full_field: bool, with_filter: bool, query_type: str):
    check(100, bulk, full_field, with_filter, query_type, xb)
