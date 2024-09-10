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

__description__ = """ test case for module vector """


sift10k = DatasetSift10K()
xb = sift10k.get_database()
xq = sift10k.get_queries()
gt = sift10k.get_groundtruth()


def create(router_url, store_type="MemoryOnly"):
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
            "dimension": 128,
            "store_type": store_type,
        }
    ]

    space_config = {
        "name": space_name,
        "partition_num": 1,
        "replica_num": 1,
        "fields": properties["fields"]
    }
    create_db(router_url, db_name)

    logger.info(create_space(router_url, db_name, space_config))


class TestConfig:
    def setup_class(self):
        self.xb = xb

    # prepare
    def test_prepare_cluster(self):
        create(router_url, "MemoryOnly")

    def test_prepare_upsert(self):
        batch_size = 100
        total_batch = 1
        add_string_array(total_batch, batch_size, xb, with_id=True)
        assert get_space_num() == int(total_batch * batch_size)

    def test_modify_config(self):
        cache_size = 512 * 1024 # bytes

        for i in range(100):
            cache_dict = {
                "engine_cache_size": cache_size + i,
            }
            url = router_url + "/config/" + db_name + "/" + space_name 
            json_str = json.dumps(cache_dict)
            rs = requests.post(url, auth=(username, password), data=json_str)
            assert rs.status_code == 200
            assert rs.json()["data"]["engine_cache_size"] == cache_size + i

            rs = requests.get(url, auth=(username, password))
            assert rs.status_code == 200
            assert rs.json()["data"]["engine_cache_size"] == cache_size + i

    # destroy
    def test_destroy_cluster(self):
        destroy(router_url, db_name, space_name)
