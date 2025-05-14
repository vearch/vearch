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


def create(router_url, store_type="MemoryOnly", refresh_interval=None):
    properties = {}
    properties["fields"] = [
        {
            "name": "field_int",
            "type": "integer",
        },
        {
            "name": "field_string_array",
            "type": "stringArray",
            "index": {"name": "field_string_array", "type": "SCALAR"},
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
            "dimension": 128,
            "store_type": store_type,
        },
    ]

    space_config = {
        "name": space_name,
        "partition_num": 1,
        "replica_num": 1,
        "fields": properties["fields"],
    }
    if refresh_interval:
        space_config["refresh_interval"] = refresh_interval

    create_db(router_url, db_name)

    logger.info(create_space(router_url, db_name, space_config))


class TestConfigCacheSize:
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

    def test_modify_cache_size(self):
        cache_size = 512 * 1024  # bytes

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


class TestConfigRefreshIntervalUpdate:
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

    def test_modify_refresh_interval(self):
        url = router_url + "/config/" + db_name + "/" + space_name

        rs = requests.get(url, auth=(username, password))
        assert rs.status_code == 200
        assert rs.json()["data"]["refresh_interval"] == 1000
        space_id = rs.json()["data"]["id"]
        db_id = rs.json()["data"]["db_id"]

        refresh_interval = -1
        refresh_interval_dict = {
            "refresh_interval": refresh_interval,
        }
        for i in range(100):
            refresh_interval_dict["refresh_interval"] = refresh_interval + i
            json_str = json.dumps(refresh_interval_dict)
            rs = requests.post(url, auth=(username, password), data=json_str)
            assert rs.status_code == 200
            assert rs.json()["data"]["refresh_interval"] == refresh_interval + i

            rs = requests.get(url, auth=(username, password))
            assert rs.status_code == 200
            assert rs.json()["data"]["refresh_interval"] == refresh_interval + i
            assert rs.json()["data"]["id"] == space_id
            assert rs.json()["data"]["db_id"] == db_id

    # destroy
    def test_destroy_cluster(self):
        destroy(router_url, db_name, space_name)


class TestConfigRefreshIntervalInital:
    def setup_class(self):
        self.xb = xb
        self.refresh_interval = -1

    # prepare
    def test_prepare_cluster(self):
        create(router_url, "MemoryOnly", self.refresh_interval)

    def test_get_refresh_interval(self):
        url = router_url + "/config/" + db_name + "/" + space_name

        rs = requests.get(url, auth=(username, password))
        assert rs.status_code == 200
        assert rs.json()["data"]["refresh_interval"] == self.refresh_interval

    def test_prepare_upsert(self):
        batch_size = 100
        total_batch = 1
        add_string_array(total_batch, batch_size, xb, with_id=True)
        assert get_space_num() == int(total_batch * batch_size)
        time.sleep(10)

    def test_not_build_index(self):
        url = router_url + "/dbs/" + db_name + "/spaces/" + space_name
        num = 0
        response = requests.get(url, auth=(username, password))
        partitions = response.json()["data"]["partitions"]
        for p in partitions:
            num += p["index_num"]
        logger.info("index num: %d" % (num))
        assert num == 0

    # destroy
    def test_destroy_cluster(self):
        destroy(router_url, db_name, space_name)
