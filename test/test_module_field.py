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

__description__ = """ test case for module vector """


sift10k = DatasetSift10K(logger)
xb = sift10k.get_database()
xq = sift10k.get_queries()
gt = sift10k.get_groundtruth()


def create(router_url, embedding_size, store_type="MemoryOnly"):
    properties = {}
    properties["fields"] = [
        {
            "name": "field_int",
            "type": "integer",
        },
        {
            "name": "field_string_array",
            "type": "stringArray",
            "index": {
                "name": "field_string_array",
                "type": "SCALAR"
            }
        },
        {
            "name": "field_vector",
            "type": "vector",
            "index": {
                "name": "gamma",
                "type": "FLAT",
                "params": {
                    "metric_type": "L2",
                }
            },
            "dimension": embedding_size,
            "store_type": store_type,
            # "format": "normalization"
        }
    ]

    space_config = {
        "name": space_name,
        "partition_num": 1,
        "replica_num": 1,
        "fields": properties["fields"]
    }
    logger.info(create_db(router_url, db_name))

    logger.info(create_space(router_url, db_name, space_config))


class TestStringArray:
    def setup_class(self):
        self.logger = logger
        self.xb = xb

    # prepare
    def test_prepare_cluster(self):
        create(router_url, self.xb.shape[1], "MemoryOnly")

    def test_prepare_upsert(self):
        batch_size = 100
        total_batch = 1
        add_string_array(total_batch, batch_size, xb, with_id=True)
        assert get_space_num() == int(total_batch * batch_size)

    def test_query_string_array(self):
        # wait for scalar index finished
        time.sleep(3)

        for i in range(100):
            query_dict = {
                "document_ids":[str(i)],
                "limit": 1,
                "db_name": db_name,
                "space_name": space_name,
            }
            url = router_url + "/document/query"
            json_str = json.dumps(query_dict)
            rs = requests.post(url, auth=(username, password), data=json_str)
            # logger.info(rs.json())
            assert rs.json()["code"] == 200
            assert len(rs.json()["data"]["documents"][0]["field_string_array"]) == 2

            query_dict["document_ids"] = []
            query_dict["filters"] = {
                "operator": "AND",
                "conditions": [
                    {
                        "field": "field_string_array",
                        "operator": "IN",
                        "value": [str(i)]
                    },
                    {
                        "field": "field_string_array",
                        "operator": "IN",
                        "value": [str(i+1000)]
                    }
                ]
            }
            json_str = json.dumps(query_dict)
            rs = requests.post(url, auth=(username, password), data=json_str)
            # logger.info(rs.json())
            assert rs.json()["code"] == 200
            assert len(rs.json()["data"]["documents"][0]["field_string_array"]) == 2

    # destroy
    def test_destroy_cluster(self):
        destroy(router_url, db_name, space_name)