# -*- coding: UTF-8 -*-

import pytest
import requests
import json
from utils.vearch_utils import *
from utils.data_utils import *

__description__ = """ test case for cluster monitor """

# @pytest.mark.author('')
# @pytest.mark.level(2)
# @pytest.mark.cover(["VEARCH"])


class TestVearchClusterMonitor:
    def setup_class(self):
        pass

    def test_prepare_db(self):
        response = create_db(router_url, db_name)
        assert response.json()["code"] == 0

    @pytest.mark.parametrize(
        ["embedding_size", "index_type"],
        [[512, "FLAT"], [512, "IVFPQ"], [512, "IVFFLAT"], [512, "HNSW"]],
    )
    def test_vearch_space_create(self, embedding_size, index_type):
        space_config = {
            "name": space_name + "empty" + str(embedding_size) + index_type,
            "partition_num": 1,
            "replica_num": 1,
            "fields": [
                {
                    "name": "field_int",
                    "type": "integer",
                },
                {
                    "name": "field_long",
                    "type": "long",
                },
                {
                    "name": "field_float",
                    "type": "float",
                },
                {
                    "name": "field_double",
                    "type": "double",
                },
                {
                    "name": "field_string",
                    "type": "string",
                    "index": {
                        "name": "field_int",
                        "type": "SCALAR",
                    },
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
                    # "format": "normalization"
                },
            ],
        }

        response = create_space(router_url, db_name, space_config)
        logger.info(response.json())

    def test_stats(self):
        response = get_cluster_stats(router_url)
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_health(self):
        response = get_cluster_health(router_url)
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_destroy_db(self):
        response = list_spaces(router_url, db_name)
        logger.info(response.json())
        for space in response.json()["data"]:
            response = drop_space(router_url, db_name, space["space_name"])
            logger.info(response)
        drop_db(router_url, db_name)
