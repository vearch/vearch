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

import pytest
import logging
import numpy as np
from utils.vearch_utils import *
from utils.data_utils import *
import concurrent.futures
import time

logging.basicConfig()
logger = logging.getLogger(__name__)

__description__ = """ test case for cluster index """


def document_upsert(add_db_name, add_space_name, embedding_size, crud_time):
    end_time = time.time() + crud_time
    items = [add_db_name, add_space_name, 0, 1, embedding_size]
    verbose = False
    if crud_time <= 10:
        verbose = True

    count = 0
    while time.time() < end_time:
        # TODO why should sleep here
        time.sleep(0.01)
        try:
            items[2] = random.randint(0, 30000000)
            items[3] = random.randint(0, 10)
            rs = process_add_embedding_size_data(items)
            count += 1
            if count % 10000 == 0 or verbose:
                logger.info("upsert: " + str(count))
                logger.info(rs.json())
        except Exception as e:
            pytest.fail(f"Error calling: {e}")


def document_query(query_db_name, query_space_name, crud_time):
    end_time = time.time() + crud_time
    verbose = False
    if crud_time <= 10:
        verbose = True

    count = 0
    index = 0
    batch_size = 1
    feature = None
    full_field = False
    seed = 1
    query_type = "by_ids"
    alias_name = ""
    check_vector = False
    check = False
    items = [
        logger,
        index,
        batch_size,
        feature,
        full_field,
        seed,
        query_type,
        alias_name,
        query_db_name,
        query_space_name,
        check_vector,
        check,
    ]

    while time.time() < end_time:
        # TODO why should sleep here
        time.sleep(0.01)
        try:
            items[1] = random.randint(0, 30000000)
            items[2] = random.randint(0, 100)
            items[4] = random.randint(0, 1)
            items[6] = random.choice(
                ["by_ids", "by_filter", "by_partition_next", "by_partition"]
            )
            # if "partition" in items[6]:
            #     items[1] = random.randint(0, 100000)

            rs = process_query_data(items)
            count += 1
            if count % 10000 == 0 or verbose:
                logger.info("query: " + str(count))
                logger.info(rs.json())
        except Exception as e:
            pytest.fail(f"Error calling: {e}")


def document_search(search_db_name, search_space_name, embedding_size, crud_time):
    end_time = time.time() + crud_time
    verbose = False
    if crud_time <= 10:
        verbose = True

    count = 0
    index = 0
    batch_size = 1
    feature = None
    full_field = False
    with_filter = False
    seed = 1
    query_type = "by_vector"
    alias_name = ""
    check = False

    items = [
        logger,
        index,
        batch_size,
        feature,
        full_field,
        with_filter,
        seed,
        query_type,
        alias_name,
        search_db_name,
        search_space_name,
        check,
    ]
    while time.time() < end_time:
        time.sleep(0.01)
        try:
            items[1] = random.randint(0, 30000000)
            items[2] = random.randint(0, 100)
            items[3] = np.random.random((batch_size, embedding_size))
            items[4] = random.randint(0, 1)
            items[5] = random.randint(0, 1)
            items[7] = random.choice(["by_vector", "by_vector_with_symbol"])
            rs = process_search_data(items)
            count += 1
            if count % 10000 == 0 or verbose:
                logger.info("search: " + str(count))
                logger.info(rs.json())
        except Exception as e:
            pytest.fail(f"Error calling: {e}")


def document_delete(delete_db_name, delete_space_name, crud_time):
    end_time = time.time() + crud_time

    verbose = False
    if crud_time <= 10:
        verbose = True

    count = 0
    index = 0
    batch_size = 1
    full_field = False
    seed = 1
    delete_type = "by_ids"
    alias_name = ""
    check = False

    items = [
        logger,
        index,
        batch_size,
        full_field,
        seed,
        delete_type,
        alias_name,
        delete_db_name,
        delete_space_name,
        check,
    ]
    while time.time() < end_time:
        time.sleep(0.01)
        try:
            items[1] = random.randint(0, 30000000)
            items[2] = random.randint(0, 100)
            items[3] = random.randint(0, 1)
            items[5] = random.choice(["by_ids", "by_filter"])
            rs = process_delete_data(items)
            count += 1
            if count % 10000 == 0 or verbose:
                logger.info("delete: " + str(count))
                logger.info(rs.json())
        except Exception as e:
            pytest.fail(f"Error calling: {e}")


class TestClusterIndex:
    db_name = ""
    space_name = ""
    embedding_size = 64
    index_type = ""
    crud_time = 3600

    def setup_class(self):
        self.logger = logger

    @pytest.mark.parametrize(
        ["index_type", "embedding_size"],
        [
            ["BFFLAT", 64], # avoid pytest -k mix with ivfflat, BF as brute search
            ["IVFPQ", 64],
            ["IVFFLAT", 64],
            ["HNSW", 64],
        ],
    )
    def test_vearch_set(self, index_type, embedding_size):
        TestClusterIndex.db_name = (
            "cluster_index_" + str(embedding_size) + "_" + index_type
        )
        TestClusterIndex.space_name = (
            "cluster_index_" + str(embedding_size) + "_" + index_type
        )
        TestClusterIndex.embedding_size = embedding_size
        if index_type == "BFFLAT":
            TestClusterIndex.index_type = "FLAT"
        else:
            TestClusterIndex.index_type = index_type

    def test_vearch_index_space_create(self):
        response = create_db(router_url, TestClusterIndex.db_name)
        logger.info(response.json())
        space_config = {
            "name": TestClusterIndex.space_name,
            "partition_num": 2,
            "replica_num": 3,
            "fields": [
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
                    "dimension": TestClusterIndex.embedding_size,
                    "index": {
                        "name": "gamma",
                        "type": TestClusterIndex.index_type,
                        "params": {
                            "metric_type": "InnerProduct",
                            "ncentroids": 2048,
                            "nsubvector": int(TestClusterIndex.embedding_size / 4),
                            "nlinks": 32,
                            "efConstruction": 100,
                        },
                    },
                    # "format": "normalization"
                },
            ],
        }

        response = create_space(router_url, TestClusterIndex.db_name, space_config)
        logger.info(response.json())

    def test_vearch_index_api_tasks(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(
                    document_upsert,
                    TestClusterIndex.db_name,
                    TestClusterIndex.space_name,
                    TestClusterIndex.embedding_size,
                    TestClusterIndex.crud_time,
                ),
                executor.submit(
                    document_query,
                    TestClusterIndex.db_name,
                    TestClusterIndex.space_name,
                    TestClusterIndex.crud_time,
                ),
                executor.submit(
                    document_search,
                    TestClusterIndex.db_name,
                    TestClusterIndex.space_name,
                    TestClusterIndex.embedding_size,
                    TestClusterIndex.crud_time,
                ),
                executor.submit(
                    document_delete,
                    TestClusterIndex.db_name,
                    TestClusterIndex.space_name,
                    TestClusterIndex.crud_time,
                ),
            ]
            for future in concurrent.futures.as_completed(futures):
                future.result()

    def test_vearch_index_destroy_db(self):
        response = drop_space(
            router_url, TestClusterIndex.db_name, TestClusterIndex.space_name
        )
        assert response.json()["code"] == 0
        response = drop_db(router_url, TestClusterIndex.db_name)
        assert response.json()["code"] == 0
