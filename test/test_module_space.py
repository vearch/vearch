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
from vearch_utils import *

logging.basicConfig()
logger = logging.getLogger(__name__)

__description__ = """ test case for module space """


class TestSpaceCreate:
    def setup(self):
        self.logger = logger

    def test_prepare_db(self):
        response = create_db(router_url, db_name)
        assert response["code"] == 200

    @pytest.mark.parametrize(
        ["index_type"],
        [["FLAT"], ["IVFPQ"], ["IVFFLAT"], ["HNSW"]],
    )
    def test_vearch_space_create_without_vector_storetype(self, index_type):
        embedding_size = 128
        space_config = {
            "name": space_name,
            "partition_num": 1,
            "replica_num": 1,
            "index": {
                "index_name": "gamma",
                "index_type": index_type,
                "index_params": {
                    "metric_type": "InnerProduct",
                    "ncentroids": 2048,
                    "nsubvector": 32,
                    "nlinks": 32,
                    "efConstruction": 40,
                    "nprobe":80,
                    "efSearch":64,
                    "training_threshold":70000
                }
            },
            "fields": {
                "field_string": {"type": "keyword"},
                "field_int": {"type": "integer"},
                "field_float": {"type": "float", "index": True},
                "field_string_array": {"type": "string", "array": True, "index": True},
                "field_int_index": {"type": "integer", "index": True},
                "field_vector": {"type": "vector", "dimension": embedding_size},
                "field_vector_normal": {
                    "type": "vector",
                    "dimension": int(embedding_size * 2),
                    "format": "normalization"
                }
            }
        }

        response = create_space(router_url, db_name, space_config)
        assert response["code"] == 200

        response = describe_space(logger, router_url, db_name, space_name)
        logger.info(response)
        assert response["code"] == 200

        response = drop_space(router_url, db_name, space_name)
        assert response["code"] == 200

    @pytest.mark.parametrize(
        ["wrong_index", "wrong_type", "index_type"],
        [
            [0, "bad index size", "IVFPQ"], 
            [1, "bad index size", "IVFFLAT"],
            [2, "bad space name", "FLAT"],
            [3, "not enough partition server", "FLAT"],
        ],
    )
    def test_vearch_space_create_badcase(self, wrong_index, wrong_type, index_type):
        embedding_size = 128
        training_threshold = 70000
        create_space_name = space_name
        replica_num = 1
        if wrong_index <= 1:
            training_threshold = 1
        if wrong_index == 2:
            create_space_name = "wrong-name"
        if wrong_index == 3:
            replica_num = 3
        space_config = {
            "name": create_space_name,
            "partition_num": 1,
            "replica_num": replica_num,
            "index": {
                "index_type": index_type,
                "index_params": {
                    "metric_type": "InnerProduct",
                    "ncentroids": 2048,
                    "nsubvector": 32,
                    "nlinks": 32,
                    "efConstruction": 40,
                    "training_threshold": training_threshold
                },
            },
            "fields": {
                "field_string": {"type": "keyword"},
                "field_int": {"type": "integer"},
                "field_float": {"type": "float", "index": True},
                "field_string_array": {"type": "string", "array": True, "index": True},
                "field_int_index": {"type": "integer", "index": True},
                "field_vector": {"type": "vector", "dimension": embedding_size},
                "field_vector_normal": {
                    "type": "vector",
                    "dimension": int(embedding_size * 2),
                    "format": "normalization",
                },
            },
        }

        response = create_space(router_url, db_name, space_config)
        logger.info(response)
        assert response["code"] != 200

    @pytest.mark.parametrize(
        ["wrong_index", "wrong_type"],
        [
            [0, "bad db name"], 
            [1, "bad space name"],
        ],
    )
    def test_vearch_space_describe_badcase(self, wrong_index, wrong_type):
        embedding_size = 128
        space_config = {
            "name": space_name,
            "partition_num": 1,
            "replica_num": 1,
            "index": {
                "index_size": 70000,
                "index_type": "FLAT",
                "index_params": {
                    "metric_type": "InnerProduct",
                    "ncentroids": 2048,
                    "nsubvector": 32,
                    "nlinks": 32,
                    "efConstruction": 40,
                },
            },
            "fields": {
                "field_string": {"type": "keyword"},
                "field_int": {"type": "integer"},
                "field_float": {"type": "float", "index": True},
                "field_string_array": {"type": "string", "array": True, "index": True},
                "field_int_index": {"type": "integer", "index": True},
                "field_vector": {"type": "vector", "dimension": embedding_size},
                "field_vector_normal": {
                    "type": "vector",
                    "dimension": int(embedding_size * 2),
                    "format": "normalization",
                },
            },
        }

        response = create_space(router_url, db_name, space_config)
        assert response["code"] == 200

        describe_db_name = db_name
        describe_space_name = space_name
        if wrong_index == 0:
            describe_db_name = "wrong_db"
        if wrong_index == 1:
            describe_space_name = "wrong_space"
        response = describe_space(logger, router_url, describe_db_name, describe_space_name)
        logger.info(response)
        assert response["code"] != 200

        response = drop_space(router_url, db_name, space_name)
        assert response["code"] == 200

    def test_destroy_db(self):
        drop_db(router_url, db_name)