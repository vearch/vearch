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

__description__ = """ test case for module partition """


class TestPartitionEmptySpaceMemorySize:
    def setup(self):
        self.logger = logger

    def test_prepare_db(self):
        logger.info(create_db(router_url, db_name))

    @pytest.mark.parametrize(
        ["embedding_size", "index_type"],
        [[128, "FLAT"], [128, "IVFPQ"], [128, "IVFFLAT"], [128, "HNSW"],
        [512, "FLAT"], [512, "IVFPQ"], [512, "IVFFLAT"], [512, "HNSW"],
        [1536, "FLAT"], [1536, "IVFPQ"], [1536, "IVFFLAT"], [1536, "HNSW"]],
    )
    def test_vearch_space_create(self, embedding_size, index_type):
        space_config = {
            "name": space_name + "empty" + str(embedding_size) + index_type,
            "partition_num": 1,
            "replica_num": 1,
            "index": {
                "index_name": "gamma",
                "index_type": index_type,
                "index_params": {
                    "metric_type": "InnerProduct",
                    "ncentroids": 2048,
                    "nsubvector": int(embedding_size / 4),
                    "nlinks": 32,
                    "efConstruction": 100,
                },
            },
            "fields": {
                "field_int": {"type": "integer", "index": False},
                "field_long": {"type": "long", "index": False},
                "field_float": {"type": "float", "index": False},
                "field_double": {"type": "double", "index": False},
                "field_string": {"type": "string", "index": True},
                "field_vector": {
                    "type": "vector",
                    "index": True,
                    "dimension": embedding_size
                    # "format": "normalization"
                },
            },
        }

        logger.info(create_space(router_url, db_name, space_config))


    def test_destroy_db(self):
        space_info = list_spaces(router_url, db_name)
        logger.info(space_info)
        for space in space_info["data"]:
            logger.info(drop_space(router_url, db_name, space["space_name"]))
        drop_db(router_url, db_name)

class TestPartitionSmallDataMemorySize:
    def setup(self):
        self.logger = logger

    def test_prepare_db(self):
        logger.info(create_db(router_url, db_name))

    @pytest.mark.parametrize(
        ["embedding_size", "index_type"],
        [[128, "FLAT"], [128, "IVFPQ"], [128, "IVFFLAT"], [128, "HNSW"],
        [512, "FLAT"], [512, "IVFPQ"], [512, "IVFFLAT"], [512, "HNSW"],
        [1536, "FLAT"], [1536, "IVFPQ"], [1536, "IVFFLAT"], [1536, "HNSW"]],
    )
    def test_vearch_space_create(self, embedding_size, index_type):
        space_name_each = space_name + "smalldata" + str(embedding_size) + index_type
        space_config = {
            "name": space_name_each,
            "partition_num": 1,
            "replica_num": 1,
            "index": {
                "index_name": "gamma",
                "index_type": index_type,
                "index_params": {
                    "metric_type": "InnerProduct",
                    "ncentroids": 2048,
                    "nsubvector": int(embedding_size / 4),
                    "nlinks": 32,
                    "efConstruction": 100,
                },
            },
            "fields": {
                "field_int": {"type": "integer", "index": False},
                "field_long": {"type": "long", "index": False},
                "field_float": {"type": "float", "index": False},
                "field_double": {"type": "double", "index": False},
                "field_string": {"type": "string", "index": True},
                "field_vector": {
                    "type": "vector",
                    "index": True,
                    "dimension": embedding_size
                    # "format": "normalization"
                },
            },
        }

        logger.info(create_space(router_url, db_name, space_config))

        add_embedding_size(db_name, space_name_each, 100, 100, embedding_size)


    def test_destroy_db(self):
        space_info = list_spaces(router_url, db_name)
        logger.info(space_info)
        for space in space_info["data"]:
            logger.info(drop_space(router_url, db_name, space["space_name"]))
        drop_db(router_url, db_name)