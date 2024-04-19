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
from multiprocessing import Pool as ThreadPool
from utils.vearch_utils import *
from utils.data_utils import *

logging.basicConfig()
logger = logging.getLogger(__name__)

__description__ = """ test case for module alias """


sift10k = DatasetSift10K(logger)
xb = sift10k.get_database()
xq = sift10k.get_queries()
gt = sift10k.get_groundtruth()


class TestAlias:
    def setup_class(self):
        self.logger = logger

    def test_create_db(self):
        response = create_db(router_url, db_name)
        logger.info(response.json())
        assert response.json()["code"] == 0

    @pytest.mark.parametrize(
        ["space_name"],
        [["ts_space"], ["ts_space1"]],
    )
    def test_create_space(self, space_name):
        embedding_size = 128
        space_config = {
            "name": space_name,
            "partition_num": 1,
            "replica_num": 1,
            "fields": [
                {
                    "name": "field_int",
                    "type": "integer",
                    "index": {
                        "name": "field_int",
                        "type": "SCALAR"
                    },
                },
                {
                    "name": "field_vector",
                    "type": "vector",
                    "dimension": embedding_size,
                    "index": {
                        "name": "gamma",
                        "type": "FLAT",
                        "params": {
                            "metric_type": "L2",
                        }
                    },
                    # "format": "normalization"
                }
            ]
        }

        response = create_space(router_url, db_name, space_config)
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_create_alias(self):
        response = create_alias(router_url, "alias_name", db_name, space_name)
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_get_alias(self):
        response = get_alias(router_url, "alias_name")
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_update_alias(self):
        response = update_alias(router_url, "alias_name", db_name, "ts_space1")
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_drop_alias(self):
        response = drop_alias(router_url, "alias_name")
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_alias_array(self):
        response = create_alias(router_url, "alias_name1", db_name, space_name)
        assert response.json()["code"] == 0

        response = create_alias(router_url, "alias_name2", db_name, space_name)
        assert response.json()["code"] == 0

        response = get_all_alias(router_url)
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = drop_alias(router_url, "alias_name1")
        assert response.json()["code"] == 0

        response = drop_alias(router_url, "alias_name2")
        assert response.json()["code"] == 0

        response = get_all_alias(router_url)
        logger.info(response.json())
        assert response.json()["code"] == 0

    @pytest.mark.parametrize(
        ["wrong_index", "wrong_type"],
        [
            [0, "create db not exits"],
            [1, "create space not exits"],
            [2, "update db not exits"],
            [3, "update space not exits"],
            [4, "get alias not exits"],
            [5, "delete alias not exits"],
            [6, "create alias exits"],
            [7, "update alias not exits"],
        ],
    )
    def test_alias_badcase(self, wrong_index, wrong_type):
        db_param = db_name
        if wrong_index == 0:
            db_param = "wrong_db"
            response = create_alias(
                router_url, "alias_name", db_param, space_name)
            logger.info(response.json())
            assert response.json()["code"] != 0

        if wrong_index == 1:
            space_param = "wrong_space"
            response = create_alias(
                router_url, "alias_name", db_name, space_param)
            logger.info(response.json())
            assert response.json()["code"] != 0

        if wrong_index == 2:
            db_param = "wrong_db"
            response = update_alias(
                router_url, "alias_name", db_param, space_name)
            logger.info(response.json())
            assert response.json()["code"] != 0

        if wrong_index == 3:
            space_param = "wrong_space"
            response = update_alias(
                router_url, "alias_name", db_name, space_param)
            logger.info(response.json())
            assert response.json()["code"] != 0

        if wrong_index == 4:
            response = get_alias(router_url, "alias_not_exist")
            logger.info(response.json())
            assert response.json()["code"] != 0

        if wrong_index == 5:
            response = drop_alias(router_url, "alias_not_exist")
            logger.info(response.json())
            assert response.json()["code"] != 0

        if wrong_index == 6:
            response = create_alias(
                router_url, "alias_name", db_name, space_name)
            assert response.json()["code"] == 0
            response = create_alias(
                router_url, "alias_name", db_name, space_name)
            logger.info(response.json())
            assert response.json()["code"] != 0
            response = drop_alias(router_url, "alias_name")
            assert response.json()["code"] == 0

        if wrong_index == 7:
            response = update_alias(
                router_url, "alias_not_exist", db_name, space_name)
            logger.info(response.json())
            assert response.json()["code"] != 0

    def process_alias(self, operation):
        if operation == "create":
            response = create_alias(
                router_url, "alias_name", db_name, space_name)
            logger.info(response.json())
        if operation == "delete":
            response = drop_alias(router_url, "alias_name")
            logger.info(response.json())
        if operation == "update":
            response = update_alias(
                router_url, "alias_not_exist", db_name, space_name)
            logger.info(response.json())

    def test_multithread(self):
        pool = ThreadPool()
        total_data = ["create", "create", "create", "delete",
                      "delete", "delete", "update", "update", "update"]
        results = pool.map(self.process_alias, total_data)
        pool.close()
        pool.join()
        response = get_all_alias(router_url)
        assert response.json()["code"] == 0
        for alias in response.json()["data"]:
            response = drop_alias(router_url, alias["name"])
            assert response.json()["code"] == 0

    def test_document_operation(self):
        embedding_size = xb.shape[1]
        batch_size = 100
        k = 100

        total_batch = 1
        total = int(total_batch * batch_size)

        response = create_alias(router_url, "alias_name", db_name, space_name)
        assert response.json()["code"] == 0

        add(total_batch, batch_size, xb, with_id=True, alias_name="alias_name")

        waiting_index_finish(logger, total)

        query_interface(logger, total_batch, batch_size, xb,
                        query_type="by_ids", alias_name="alias_name")
        query_interface(logger, total_batch, batch_size, xb,
                        query_type="by_filter", alias_name="alias_name")

        search_interface(logger, total_batch, batch_size, xb,
                         query_type="by_vector", alias_name="alias_name")

        delete_interface(logger, total_batch, batch_size,
                         delete_type="by_filter", alias_name="alias_name")

        add(total_batch, batch_size, xb, with_id=True, alias_name="alias_name")
        delete_interface(logger, total_batch, batch_size,
                         delete_type="by_ids", alias_name="alias_name")

        response = drop_alias(router_url, "alias_name")
        assert response.json()["code"] == 0

    def test_destroy_db_and_space(self):
        response = list_spaces(router_url, db_name)
        for space in response.json()["data"]:
            response = create_alias(
                router_url, "alias_name", db_name, space["space_name"])
            assert response.json()["code"] == 0

            response = get_alias(router_url, "alias_name")
            logger.info(response.json())
            assert response.json()["code"] == 0

            response = drop_space(router_url, db_name, space["space_name"])
            assert response.json()["code"] == 0

            # delete space should also delete correspond alias
            response = get_alias(router_url, "alias_name")
            logger.info(response.json())
            assert response.json()["code"] != 0
        drop_db(router_url, db_name)
