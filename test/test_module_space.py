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
        logger.info(create_db(router_url, db_name))

    @pytest.mark.parametrize(
        ["retrieval_type"],
        [["FLAT"], ["IVFPQ"], ["IVFFLAT"], ["HNSW"]],
    )
    def test_vearch_space_create(self, retrieval_type):
        embedding_size = 128
        space_config = {
            "name": space_name,
            "partition_num": 1,
            "replica_num": 1,
            "engine": {
                "index_size": 70000,
                "id_type": "String",
                "retrieval_type": retrieval_type,
                "retrieval_param": {
                    "metric_type": "InnerProduct",
                    "ncentroids": 2048,
                    "nsubvector": 32,
                    "nlinks": 32,
                    "efConstruction": 40,
                },
            },
            "properties": {
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

        logger.info(create_space(router_url, db_name, space_config))

        logger.info(drop_space(router_url, db_name, space_name))

    def test_destroy_db(self):
        drop_db(router_url, db_name)
