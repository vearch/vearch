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

__description__ = """ test case for cluster master """

class TestClusterMasterPrepare:
    def setup_class(self):
        pass

    def test_prepare_db(self):
        response = create_db(router_url, db_name)
        logger.info(response.json())

    @pytest.mark.parametrize(
        ["embedding_size", "index_type"],
        [
            [128, "FLAT"],
        ],
    )
    def test_vearch_space_create(self, embedding_size, index_type):
        space_config = {
            "name": space_name,
            "partition_num": 3,
            "replica_num": 3,
            "fields": [
                {"name": "field_int", "type": "integer"},
                {"name": "field_long", "type": "long"},
                {"name": "field_float", "type": "float"},
                {"name": "field_double", "type": "double"},
                {
                    "name": "field_string",
                    "type": "string",
                    "index": {"name": "field_string", "type": "SCALAR"},
                },
                {
                    "name": "field_vector",
                    "type": "vector",
                    "dimension": embedding_size,
                    "index": {
                        "name": "gamma",
                        "type": index_type,
                        "params": {
                            "metric_type": "InnerProduct",
                            "ncentroids": 2048,
                            "nsubvector": int(embedding_size / 4),
                            "nlinks": 32,
                            "efConstruction": 100,
                        },
                    },
                },
            ],
        }

        response = create_space(router_url, db_name, space_config)
        logger.info(response.json())


class TestClusterMasterOperateDocument:
    def setup_class(self):
        pass

    def test_vearch_document(self):
        sift10k = DatasetSift10K()
        xb = sift10k.get_database()
        add(50, 100, xb, with_id=True, full_field=True)
        search_interface(10, 100, xb, True)


class TestClusterMasterOperateMetaData:
    def setup_class(self):
        pass

    def test_destroy_db(self):
        response = list_spaces(router_url, db_name)
        logger.info(response.json())
        if response.json()["code"] == 0:
            for space in response.json()["data"]:
                response = drop_space(router_url, db_name, space["space_name"])
                assert response.json()["code"] == 0
            drop_db(router_url, db_name)
