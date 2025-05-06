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
import sys
from concurrent.futures import ThreadPoolExecutor
from utils.vearch_utils import *
from utils.data_utils import *

__description__ = """ test case for module filter """


def create(router_url, properties):
    space_config = {
        "name": space_name,
        "partition_num": 1,
        "replica_num": 1,
        "fields": properties["fields"],
    }
    response = create_db(router_url, db_name)
    logger.info(response.json())
    response = create_space(router_url, db_name, space_config)
    logger.info(response.json())


sift10k = DatasetSift10K()
xb = sift10k.get_database()
xq = sift10k.get_queries()
gt = sift10k.get_groundtruth()


def prepare_filter_bound(conditions, index, batch_size, full_field, left, right):
    if full_field:
        range_filter = [
            {"field": "field_int", "operator": left, "value": index * batch_size},
            {
                "field": "field_int",
                "operator": right,
                "value": (index + 1) * batch_size,
            },
            {"field": "field_long", "operator": left, "value": index * batch_size},
            {
                "field": "field_long",
                "operator": right,
                "value": (index + 1) * batch_size,
            },
            {
                "field": "field_float",
                "operator": left,
                "value": float(index * batch_size),
            },
            {
                "field": "field_float",
                "operator": right,
                "value": float((index + 1) * batch_size),
            },
            {
                "field": "field_double",
                "operator": left,
                "value": float(index * batch_size),
            },
            {
                "field": "field_double",
                "operator": right,
                "value": float((index + 1) * batch_size),
            },
        ]
        conditions.extend(range_filter)
    else:
        range_filter = [
            {"field": "field_int", "operator": left, "value": index * batch_size},
            {
                "field": "field_int",
                "operator": right,
                "value": (index + 1) * batch_size,
            },
        ]
        conditions.extend(range_filter)


def process_get_data_by_filter(index: int, full_field: bool, mode: str, total: int):
    url = f"{router_url}/document/query"
    batch_size = 1

    data = {
        "db_name": db_name,
        "space_name": space_name,
        "vector_value": False,
        "filters": {"operator": "AND", "conditions": []},
        "limit": batch_size,
    }

    if mode == "[]":
        prepare_filter_bound(
            data["filters"]["conditions"], index, batch_size, full_field, ">=", "<="
        )
    elif mode == "[)":
        prepare_filter_bound(
            data["filters"]["conditions"], index, batch_size, full_field, ">=", "<"
        )
    elif mode == "(]":
        prepare_filter_bound(
            data["filters"]["conditions"], index, batch_size, full_field, ">", "<="
        )
    elif mode == "()":
        prepare_filter_bound(
            data["filters"]["conditions"], index, batch_size, full_field, ">", "<"
        )
    elif mode == "upper_outbound":
        range_filter = [
            {"field": "field_int", "operator": ">=", "value": 2**31 - 1},
            {"field": "field_long", "operator": ">=", "value": 2**63 - 1},
            {
                "field": "field_double",
                "operator": ">=",
                "value": sys.float_info.max - 1,
            },
        ]
        data["filters"]["conditions"].extend(range_filter)
    elif mode == "lower_outbound":
        range_filter = [
            {"field": "field_int", "operator": "<=", "value": -(2**31) + 1},
            {"field": "field_long", "operator": "<=", "value": -(2**63) + 1},
            {
                "field": "field_double",
                "operator": "<=",
                "value": -sys.float_info.max + 1,
            },
        ]
        data["filters"]["conditions"].extend(range_filter)
    elif mode == "[lower_bound, valid_value]":
        range_filter = [
            {"field": "field_int", "operator": ">=", "value": -(2**31) + 1},
            {"field": "field_int", "operator": "<=", "value": (index + 1) * batch_size},
            {"field": "field_long", "operator": ">=", "value": -(2**63) + 1},
            {
                "field": "field_long",
                "operator": "<=",
                "value": (index + 1) * batch_size,
            },
            {
                "field": "field_double",
                "operator": ">=",
                "value": -sys.float_info.max + 1,
            },
            {
                "field": "field_double",
                "operator": "<=",
                "value": (index + 1) * batch_size,
            },
        ]
        data["filters"]["conditions"].extend(range_filter)
    elif mode == "[valid_value, upper_bound]":
        range_filter = [
            {"field": "field_int", "operator": "<=", "value": 2**31 - 1},
            {"field": "field_int", "operator": ">=", "value": index * batch_size},
            {"field": "field_long", "operator": "<=", "value": 2**63 - 1},
            {"field": "field_long", "operator": ">=", "value": index * batch_size},
            {
                "field": "field_double",
                "operator": "<=",
                "value": sys.float_info.max - 1,
            },
            {"field": "field_double", "operator": ">=", "value": index * batch_size},
        ]
        data["filters"]["conditions"].extend(range_filter)
    elif mode == "=" :
        range_filter = [
            {
                "field": "field_int",
                "operator": "=",
                "value": index * batch_size
            },
            {
                "field": "field_long",
                "operator": "=",
                "value": index * batch_size
            },
            {
                "field": "field_double",
                "operator": "=",
                "value": index * batch_size
            },
        ]
        data["filters"]["conditions"].extend(range_filter)
    elif mode == "<>" :
        range_filter = [
            {
                "field": "field_int",
                "operator": "<>",
                "value": index * batch_size
            },
            {
                "field": "field_long",
                "operator": "<>",
                "value": index * batch_size
            },
            {
                "field": "field_double",
                "operator": "<>",
                "value": index * batch_size
            },
        ]
        data["filters"]["conditions"].extend(range_filter)
    elif mode == "IN":
        term_filter = [
            {
                "field": "field_string",
                "operator": "IN",
                "value": [str(index * batch_size), str((index + 1) * batch_size)],
            },
        ]
        data["filters"]["conditions"].extend(term_filter)
    elif mode == "NOT IN":
        term_filter = [
            {
                "field": "field_string",
                "operator": "NOT IN",
                "value": [str(index * batch_size), str((index + 1) * batch_size)],
            },
        ]
        data["filters"]["conditions"].extend(term_filter)
    elif mode == "Hybrid range NOT IN":
        range_filter = [
            {
                "field": "field_int",
                "operator": ">=",
                "value": index * batch_size
            }
        ]
        term_filter = [
            {
                "field": "field_string",
                "operator": "NOT IN",
                "value": [str(index * batch_size), str((index + 1) * batch_size)],
            },
        ]
        data["filters"]["conditions"].extend(range_filter)
        data["filters"]["conditions"].extend(term_filter)
    elif mode == "Hybrid range IN":
        prepare_filter_bound(
            data["filters"]["conditions"], index, batch_size, full_field, ">=", "<"
        )
        term_filter = [
            {
                "field": "field_string",
                "operator": "IN",
                "value": [str(index * batch_size), str((index + 1) * batch_size)],
            },
        ]
        data["filters"]["conditions"].extend(term_filter)
    elif mode == "Hybrid IN NOT IN":
        in_filter = [
            {
                "field": "field_string",
                "operator": "IN",
                "value": [str(index * batch_size)],
            }
        ]
        not_in_filter = [
            {
                "field": "field_string2",
                "operator": "NOT IN",
                "value": [str(index * batch_size)],
            }
        ]
        data["filters"]["conditions"].extend(in_filter)
        data["filters"]["conditions"].extend(not_in_filter)
    elif mode == "No result":
        term_filter = [
            {
                "field": "field_string",
                "operator": "IN",
                "value": [str(index * batch_size), str((index + 1) * batch_size)],
            },
        ]
        term_filter2 = [
            {
                "field": "field_string2",
                "operator": "IN",
                "value": [str((index - 1) * batch_size)],
            }
        ]
        data["filters"]["conditions"].extend(term_filter)
        data["filters"]["conditions"].extend(term_filter2)
    elif mode == "Hybrid And range":
        range_filter = [
            {
                "field": "field_int",
                "operator": "<=",
                "value": index * batch_size
            },
            {
                "field": "field_int",
                "operator": ">=",
                "value": (index + 1) * batch_size
            }
        ]
        data["filters"]["conditions"].extend(range_filter)
    elif mode == "Hybrid Or range":
        data["filters"]["operator"] = "OR"
        range_filter = [
            {
                "field": "field_int",
                "operator": "<=",
                "value": index * batch_size
            },
            {
                "field": "field_int",
                "operator": ">=",
                "value": (index + 1) * batch_size
            }
        ]
        data["filters"]["conditions"].extend(range_filter)
    elif mode == "Hybrid Or range NOT IN":
        data["filters"]["operator"] = "OR"
        range_filter = [
            {
                "field": "field_int",
                "operator": "=",
                "value": index * batch_size
            }
        ]
        term_filter = [
            {
                "field": "field_string",
                "operator": "NOT IN",
                "value": [str(index * batch_size)]
            }
        ]
        data["filters"]["conditions"].extend(range_filter)
        data["filters"]["conditions"].extend(term_filter)
    elif mode == "Hybrid And range NOT Equal":
        range_filter = [
            {
                "field": "field_int",
                "operator": "<>",
                "value": index * batch_size
            },
            {
                "field": "field_int",
                "operator": ">=",
                "value": index * batch_size
            }
        ]
        data["filters"]["conditions"].extend(range_filter)
    elif mode == "Hybrid Or range NOT Equal":
        data["filters"]["operator"] = "OR"
        range_filter = [
            {
                "field": "field_int",
                "operator": "<>",
                "value": index * batch_size
            },
            {
                "field": "field_int",
                "operator": ">=",
                "value": index * batch_size
            }
        ]
        data["filters"]["conditions"].extend(range_filter)
    elif mode == "Wrong operator":
        data["filters"]["operator"] = "NOT"
        prepare_filter_bound(
            data["filters"]["conditions"], index, batch_size, full_field, ">=", "<"
        )

    data["limit"] = batch_size

    json_str = json.dumps(data)
    rs = requests.post(url, auth=(username, password), data=json_str)
    if mode == "Wrong operator":
        assert rs.status_code != 200
    elif rs.status_code != 200 or "documents" not in rs.json()["data"]:
        logger.info(rs.json())
        logger.info(json_str)
        assert False
    else:
        documents = rs.json()["data"]["documents"]
        if len(documents) != batch_size:
            logger.debug("len(documents) = " + str(len(documents)))
            logger.debug(json_str)

    if mode == "[]":
        assert len(documents) == batch_size
        assert rs.text.find('"total":' + str(batch_size)) >= 0

        for j in range(batch_size):
            value = int(index)
            logger.debug(value)
            logger.debug(documents[j])
            assert (
                documents[j]["field_int"] == value
                or documents[j]["field_int"] == value + 1
            )

            if full_field:
                assert (
                    documents[j]["field_long"] == value
                    or documents[j]["field_long"] == value + 1
                )
                assert (
                    documents[j]["field_float"] == float(value)
                    or documents[j]["field_float"] == float(value) + 1
                )
                assert (
                    documents[j]["field_double"] == float(value)
                    or documents[j]["field_double"] == float(value) + 1
                )
    elif mode == "[)":
        assert len(documents) == batch_size
        assert rs.text.find('"total":' + str(batch_size)) >= 0

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
        assert rs.text.find('"total":' + str(batch_size)) >= 0

        for j in range(batch_size):
            value = int(index) + 1
            logger.debug(value)
            logger.debug(documents[j])
            assert documents[j]["field_int"] == value

            if full_field:
                assert documents[j]["field_long"] == value
                assert documents[j]["field_float"] == float(value)
                assert documents[j]["field_double"] == float(value)
    elif mode == "=" :
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
    elif mode == "<>" :
        assert len(documents) == batch_size
        assert rs.text.find('"total":' + str(batch_size)) >= 0

        for j in range(batch_size):
            value = int(index)
            logger.debug(value)
            logger.debug(documents[j])
            assert documents[j]["field_int"] != value

            if full_field:
                assert documents[j]["field_long"] != value
                assert documents[j]["field_float"] != float(value)
                assert documents[j]["field_double"] != float(value)
    elif mode == "()":
        assert len(documents) == 0
        assert rs.text.find('"total":' + str(0)) >= 0
    elif mode == "upper_outbound" or mode == "lower_outbound":
        assert len(documents) == 0
        assert rs.text.find('"total":' + str(0)) >= 0
    elif mode == "[lower_bound, valid_value]" or mode == "[valid_value, upper_bound]":
        assert len(documents) > 0
    elif mode == "IN":
        for doc in documents:
            assert doc["field_string"] == str(index) or doc["field_string"] == str(
                index + 1
            )
    elif mode == "NOT IN":
        assert len(documents) > 0
        for doc in documents:
            assert doc["field_string"] != str(index) and doc["field_string"] != str(
                index + 1
            )
    elif mode == "Hybrid range NOT IN":
        for doc in documents:
            assert doc["field_string"] != str(index) and doc["field_string"] != str(
                index + 1
            )
            assert doc["field_int"] > index + 1
    elif mode == "Hybrid range IN":
        assert len(documents) > 0
        for doc in documents:
            assert doc["field_string"] == str(index) or doc["field_string"] == str(
                index + 1
            )
            assert doc["field_int"] == index
    elif mode == "Hybrid IN NOT IN":
        for doc in documents:
            assert doc["field_string"] == str(index * batch_size)
            assert doc["field_string2"] != str(index * batch_size)
    elif mode == "No result":
        assert len(documents) == 0
    elif mode == "Hybrid And range":
        assert len(documents) == 0
    elif mode == "Hybrid Or range":
        assert len(documents) > 0
    elif mode == "Hybrid And range NOT Equal":
        for doc in documents:
            assert doc["field_int"] > index
    elif mode == "Hybrid Or range NOT Equal":
        assert len(documents) > 0
    elif mode == "Hybrid Or range NOT IN":
        assert len(documents) > 0

def query_by_filter_interface(total, full_field, mode: str):
    for i in range(total):
        process_get_data_by_filter(i, full_field, mode, total)
    logger.info("query_by_filter_interface finished")


def parallel_filter(id, total_batch, full_field: bool, mode: str):
    try:
        add(total_batch, 1, xb, full_field, True)
        logger.info("%s doc_num: %d" % (space_name, get_space_num()))
        query_by_filter_interface(total_batch, full_field, mode)
        delete_interface(total_batch, 1, full_field, 1, "by_filter")
    except Exception as e:
        logger.warning(f"Thread {id}: encountered an error: {e}")
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

    logger.info(
        "dataset num: %d, total_batch: %d, dimension: %d, search num: %d, topK: %d"
        % (total, total_batch, dim, xq.shape[0], k)
    )

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
            "name": "field_string2",
            "type": "string",
            "index": {
                "name": "field_string2",
                "type": "SCALAR",
            },
        },
        {
            "name": "field_string_array",
            "type": "string_array",
            "index": {
                "name": "field_string_array",
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
                },
            },
            "dimension": dim,
            "store_type": "MemoryOnly",
            # "format": "normalization"
        },
    ]

    create(router_url, properties)

    add(
        total=total_batch,
        batch_size=1,
        xb=xb,
        with_id=with_id,
        full_field=full_field,
        has_string2=True,
    )

    logger.info("%s doc_num: %d" % (space_name, get_space_num()))

    query_by_filter_interface(total_batch, full_field, mode)

    delete_interface(total_batch, batch_size, full_field, 1, "by_filter")

    assert get_space_num() == 0
    for i in range(total):
        process_add_data(
            (
                i,
                batch_size,
                xb[i * batch_size : (i + 1) * batch_size],
                with_id,
                full_field,
                1,
                "",
                [],
                False,
                True,
                db_name,
                space_name,
            )
        )
        process_get_data_by_filter(i, full_field, "[)", total)
        assert get_space_num() == i + 1

    for i in range(total):
        process_delete_data(
            (i, batch_size, full_field, 1, "by_filter", "", db_name, space_name, True)
        )
        assert get_space_num() == total - i - 1

    with ThreadPoolExecutor(
        max_workers=10, thread_name_prefix="non_daemon_thread"
    ) as executor:
        partial_parallel_filter = functools.partial(
            parallel_filter, total_batch=total_batch, full_field=full_field, mode=mode
        )
        args_array = [(index,) for index in range(10)]
        futures = [
            executor.submit(partial_parallel_filter, *args) for args in args_array
        ]

        for future in futures:
            future.result()

    url = router_url + "/document/upsert"
    data = {}
    data["db_name"] = db_name
    data["space_name"] = space_name
    data["documents"] = []
    param_dict = {}
    param_dict["_id"] = "0"
    param_dict["field_int"] = "0"
    param_dict["field_vector"] = xb[0:1].tolist()[0]
    param_dict["field_long"] = param_dict["field_int"]
    param_dict["field_float"] = float(param_dict["field_int"])
    param_dict["field_double"] = float(param_dict["field_int"])
    param_dict["field_string_array"] = [str(i) for i in range(1024)]
    data["documents"].append(param_dict)
    rs = requests.post(url, auth=(username, password), json=data)
    assert rs.json()["code"] == 0

    param_dict["field_string_array"] = ["".join(str(i) for i in range(1024))]
    rs = requests.post(url, auth=(username, password), json=data)
    assert rs.json()["code"] != 0

    destroy(router_url, db_name, space_name)


@pytest.mark.parametrize(
    ["full_field", "mode"], 
    [
        [True, "()"],
        [True, "[]"],
        [True, "(]"],
        [True, "[)"],
        [True, "upper_outbound"],
        [True, "lower_outbound"],
        [True, "[lower_bound, valid_value]"],
        [True, "[valid_value, upper_bound]"],
        [True, "="],
        [True, "<>"],
        [True, "IN"],
        [True, "NOT IN"],
        [True, "Hybrid range NOT IN"],
        [True, "Hybrid range IN"],
        [True, "Hybrid IN NOT IN"],
        [True, "Hybrid And range"],
        [True, "Hybrid Or range"],
        [True, "Hybrid Or range NOT IN"],
        [True, "Hybrid And range NOT Equal"],
        [True, "Hybrid Or range NOT Equal"],
        [True, "No result"],
        [True, "Wrong operator"]
    ],
)
def test_module_filter(full_field: bool, mode: str):
    check(100, full_field, xb, mode)
