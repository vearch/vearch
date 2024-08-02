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

import functools
import requests
import json
import pytest
import logging
from concurrent.futures import ThreadPoolExecutor
from utils.vearch_utils import *
from utils.data_utils import *

logging.basicConfig()
logger = logging.getLogger(__name__)

__description__ = """ test case for module filter """


def create(router_url, properties):
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


sift10k = DatasetSift10K(logger)
xb = sift10k.get_database()
xq = sift10k.get_queries()
gt = sift10k.get_groundtruth()


def prepare_filter_bound(conditions, index, batch_size, full_field, left, right):
    if full_field:
        range_filter = [
            {
                "field": "field_int",
                "operator": left,
                "value": index * batch_size
            },
            {
                "field": "field_int",
                "operator": right,
                "value": (index + 1) * batch_size
            },
            {
                "field": "field_long",
                "operator": left,
                "value": index * batch_size
            },
            {
                "field": "field_long",
                "operator": right,
                "value": (index + 1) * batch_size
            },
            {
                "field": "field_float",
                "operator": left,
                "value": float(index * batch_size)
            },
            {
                "field": "field_float",
                "operator": right,
                "value": float((index + 1) * batch_size)
            },
            {
                "field": "field_double",
                "operator": left,
                "value": float(index * batch_size)
            },
            {
                "field": "field_double",
                "operator": right,
                "value": float((index + 1) * batch_size)
            },
        ]
        conditions.extend(range_filter)
    else:
        range_filter = [
            {
                "field": "field_int",
                "operator": left,
                "value": index * batch_size
            },
            {
                "field": "field_int",
                "operator": right,
                "value": (index + 1) * batch_size
            },
        ]
        conditions.extend(range_filter)


def process_get_data_by_filter(items):
    url = router_url + "/document/query"
    data = {}
    data["db_name"] = db_name
    data["space_name"] = space_name
    data["vector_value"] = False

    logger = items[0]
    index = items[1]
    batch_size = 1
    full_field = items[2]
    mode = items[3]
    total = items[4]

    data["filters"] = {
        "operator": "AND",
        "conditions": []
    }
    if mode == "[]":
        prepare_filter_bound(data["filters"]["conditions"], index,
                             batch_size, full_field, ">=", "<=")
    elif mode == "[)":
        prepare_filter_bound(data["filters"]["conditions"], index,
                             batch_size, full_field, ">=", "<")
    elif mode == "(]":
        prepare_filter_bound(data["filters"]["conditions"], index,
                             batch_size, full_field, ">", "<=")
    elif mode == "()":
        prepare_filter_bound(data["filters"]["conditions"], index,
                             batch_size, full_field, ">", "<")
    data["limit"] = batch_size

    json_str = json.dumps(data)
    rs = requests.post(url, auth=(username, password), data=json_str)
    if rs.status_code != 200 or "documents" not in rs.json()["data"]:
        logger.info(rs.json())
        logger.info(json_str)
        assert False

    documents = rs.json()["data"]["documents"]
    if len(documents) != batch_size:
        logger.info("len(documents) = " + str(len(documents)))
        logger.info(json_str)

    if mode == "[]":
        assert len(documents) == batch_size
        assert rs.text.find("\"total\":" + str(batch_size)) >= 0

        for j in range(batch_size):
            value = int(index)
            logger.debug(value)
            logger.debug(documents[j])
            assert documents[j]["field_int"] == value or documents[j]["field_int"] == value + 1

            if full_field:
                assert documents[j]["field_long"] == value or documents[j]["field_long"] == value + 1
                assert documents[j]["field_float"] == float(
                    value) or documents[j]["field_float"] == float(value) + 1
                assert documents[j]["field_double"] == float(
                    value) or documents[j]["field_double"] == float(value) + 1
    elif mode == "[)":
        assert len(documents) == batch_size
        assert rs.text.find("\"total\":" + str(batch_size)) >= 0

        for j in range(batch_size):
            value = int(index)
            logger.debug(value)
            logger.debug(documents[j])
            assert documents[j]["field_int"] == value

            if full_field:
                assert documents[j]["field_long"] == value
                assert documents[j]["field_float"] == float(value)
                assert documents[j]["field_double"] == float(value)
    elif mode == "(]" and index != total - 1:
        assert len(documents) == batch_size
        assert rs.text.find("\"total\":" + str(batch_size)) >= 0

        for j in range(batch_size):
            value = int(index) + 1
            logger.debug(value)
            logger.debug(documents[j])
            assert documents[j]["field_int"] == value

            if full_field:
                assert documents[j]["field_long"] == value
                assert documents[j]["field_float"] == float(value)
                assert documents[j]["field_double"] == float(value)
    elif mode == "()":
        assert len(documents) == 0
        assert rs.text.find("\"total\":" + str(0)) >= 0


def query_by_filter_interface(logger, total, full_field, mode: str):
    for i in range(total):
        process_get_data_by_filter((logger, i, full_field, mode, total))
    logger.info("query_by_filter_interface finished")


def parallel_filter(id, total_batch, full_field: bool, mode: str):
    try:
        add(total_batch, 1, xb, full_field, True)
        logger.info("%s doc_num: %d" % (space_name, get_space_num()))
        query_by_filter_interface(logger, total_batch, full_field, mode)
        delete_interface(logger, total_batch, 1, full_field, 1, "by_filter")
    except Exception as e:
        logger.warn(f"Thread {id}: encountered an error: {e}")
    finally:
        logger.info(f"Thread {id}: exited")

def check(total, full_field, xb, mode: str):
    dim = xb.shape[1]
    batch_size = 1
    k = 100
    if total == 0:
        total = xb.shape[0]
    total_batch = int(total / batch_size)
    with_id = True

    logger.info("dataset num: %d, total_batch: %d, dimension: %d, search num: %d, topK: %d" % (
        total, total_batch, dim, xq.shape[0], k))

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
            "index": {
                "name": "field_long",
                "type": "SCALAR",
            },
        },
        {
            "name": "field_float",
            "type": "float",
            "index": {
                "name": "field_float",
                "type": "SCALAR",
            },
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
                "name": "name",
                "type": "FLAT",
                "params": {
                    "metric_type": "L2",
                }
            },
            "dimension": dim,
            "store_type": "MemoryOnly",
            # "format": "normalization"
        }
    ]

    create(router_url, properties)

    add(total_batch, 1, xb, with_id, full_field)

    logger.info("%s doc_num: %d" % (space_name, get_space_num()))

    query_by_filter_interface(logger, total_batch, full_field, mode)

    delete_interface(logger, total_batch, batch_size, full_field, 1, "by_filter")

    assert get_space_num() == 0
    for i in range(total):
        process_add_data((i, batch_size, xb[i * batch_size : (i + 1) * batch_size], with_id, full_field, 1, "", None, []))
        process_get_data_by_filter((logger, i, full_field, "[)", total))
        assert get_space_num() == i + 1

    for i in range(total):
        process_delete_data(
            (logger, i, batch_size, full_field, 1, "by_filter", "", db_name, space_name, True)
        )
        assert get_space_num() == total - i - 1

    with ThreadPoolExecutor(max_workers=10, thread_name_prefix="non_daemon_thread") as executor:
        partial_parallel_filter = functools.partial(parallel_filter, total_batch=total_batch, full_field=full_field, mode=mode)
        args_array = [(index,) for index in range(10)]
        futures = [executor.submit(partial_parallel_filter, *args) for args in args_array]

        for future in futures:
            future.result()
    destroy(router_url, db_name, space_name)


@pytest.mark.parametrize(["full_field", "mode"], [
    [True, "()"],
    [True, "[]"],
    [True, "(]"],
    [True, "[)"],
])
def test_module_filter(full_field: bool, mode: str):
    check(100, full_field, xb, mode)
