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

__description__ = """ test case for module resource limit """


sift10k = DatasetSift10K()
xb = sift10k.get_database()
xq = sift10k.get_queries()
gt = sift10k.get_groundtruth()


class TestResourceLimit:
    def setup_class(self):
        pass

    def test_empty_cluster_update_limit(self):
        response = server_resource_limit(router_url, resource_exhausted=True)
        logger.info(response.json())
        assert response.json()["code"] != 0

    def test_create_db_and_space(self):
        properties = {}
        properties["fields"] = [
            {
                "name": "field_int",
                "type": "integer",
            },
            {
                "name": "field_vector",
                "type": "vector",
                "dimension": 128,
                "index": {
                    "name": "gamma",
                    "type": "FLAT",
                    "params": {
                        "metric_type": "L2",
                    },
                },
                # "format": "normalization"
            },
        ]

        space_config = {
            "name": space_name,
            "partition_num": 1,
            "replica_num": 1,
            "fields": properties["fields"],
        }
        response = create_db(router_url, db_name)
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = create_space(router_url, db_name, space_config)
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_upsert_document(self):
        url = router_url + "/document/upsert"
        data = {}
        data["db_name"] = db_name
        data["space_name"] = space_name
        data["documents"] = []
        param_dict = {}
        param_dict["field_int"] = 1
        param_dict["field_vector"] = [random.random() for i in range(0, 128)]
        data["documents"].append(param_dict)

        rs = requests.post(url, auth=(username, password), json=data)
        assert rs.json()["data"]["total"] == 1

    def test_servers_status(self):
        response = get_servers_status(router_url)
        logger.info(response.json()["data"])
        assert response.json()["code"] == 0

    def test_set_resource_exhausted(self):
        response = server_resource_limit(router_url, resource_exhausted=True)
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_upsert_document_with_resource_exhausted(self):
        url = router_url + "/document/upsert"
        data = {}
        data["db_name"] = db_name
        data["space_name"] = space_name
        data["documents"] = []
        param_dict = {}
        param_dict["field_int"] = 1
        param_dict["field_vector"] = [random.random() for i in range(0, 128)]
        data["documents"].append(param_dict)

        response = requests.post(url, auth=(username, password), json=data)
        logger.info(response.json())
        assert response.json()["data"]["total"] == 0

    def test_resource_limit(self):
        # wait for etcd recode take effect
        time.sleep(5)
        response = server_resource_limit(router_url)
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_upsert_document_with_resource(self):
        url = router_url + "/document/upsert"
        data = {}
        data["db_name"] = db_name
        data["space_name"] = space_name
        data["documents"] = []
        param_dict = {}
        param_dict["field_int"] = 1
        param_dict["field_vector"] = [random.random() for i in range(0, 128)]
        data["documents"].append(param_dict)

        response = requests.post(url, auth=(username, password), json=data)
        logger.info(response.json())
        assert response.json()["data"]["total"] == 1

    def test_destroy(self):
        drop_space(router_url, db_name, space_name)
        drop_db(router_url, db_name)
