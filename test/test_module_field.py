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
from utils.vearch_utils import *
from utils.data_utils import *
import datetime

__description__ = """ test case for module vector """


sift10k = DatasetSift10K()
xb = sift10k.get_database()
xq = sift10k.get_queries()
gt = sift10k.get_groundtruth()


def create(router_url, properties=None):
    space_config = {
        "name": space_name,
        "partition_num": 1,
        "replica_num": 1,
        "fields": properties["fields"],
    }
    create_db(router_url, db_name)

    response = create_space(router_url, db_name, space_config)
    assert response.json()["code"] == 0


class TestStringArrayField:
    def setup_class(self):
        self.xb = xb

    # prepare
    def test_prepare_cluster(self):
        embedding_size = self.xb.shape[1]
        store_type = "MemoryOnly"
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
                "dimension": embedding_size,
                "store_type": store_type,
                # "format": "normalization"
            },
        ]
        create(router_url, properties)

    def test_prepare_upsert(self):
        batch_size = 100
        total_batch = 1
        add_string_array(total_batch, batch_size, xb, with_id=True)
        assert get_space_num() == int(total_batch * batch_size)

    def test_query_string_array(self):
        for i in range(100):
            query_dict = {
                "document_ids": [str(i)],
                "limit": 1,
                "db_name": db_name,
                "space_name": space_name,
            }
            url = router_url + "/document/query?trace=true"
            json_str = json.dumps(query_dict)
            rs = requests.post(url, auth=(username, password), data=json_str)
            # logger.info(rs.json())
            assert rs.status_code == 200
            assert len(rs.json()["data"]["documents"][0]["field_string_array"]) == 2

            query_dict["document_ids"] = []
            query_dict["filters"] = {
                "operator": "AND",
                "conditions": [
                    {
                        "field": "field_string_array",
                        "operator": "IN",
                        "value": [str(i), str(i + 1000)],
                    },
                ],
            }
            json_str = json.dumps(query_dict)
            rs = requests.post(url, auth=(username, password), data=json_str)
            # logger.info(rs.json())
            assert rs.status_code == 200
            assert len(rs.json()["data"]["documents"][0]["field_string_array"]) == 2

        for i in range(100):
            query_dict = {
                "document_ids": [str(i)],
                "limit": 1,
                "db_name": db_name,
                "space_name": space_name,
                "get_by_query": True,
            }
            url = router_url + "/document/query?trace=true"
            json_str = json.dumps(query_dict)
            rs = requests.post(url, auth=(username, password), data=json_str)
            # logger.info(rs.json())
            assert rs.status_code == 200
            assert len(rs.json()["data"]["documents"][0]["field_string_array"]) == 2

            query_dict["document_ids"] = []
            query_dict["filters"] = {
                "operator": "AND",
                "conditions": [
                    {
                        "field": "field_string_array",
                        "operator": "IN",
                        "value": [str(i)],
                    },
                    {
                        "field": "field_string_array",
                        "operator": "IN",
                        "value": [str(i + 1000)],
                    },
                ],
            }
            json_str = json.dumps(query_dict)
            rs = requests.post(url, auth=(username, password), data=json_str)
            # logger.info(rs.json())
            assert rs.status_code == 200
            assert len(rs.json()["data"]["documents"][0]["field_string_array"]) == 2

    # destroy
    def test_destroy_cluster(self):
        destroy(router_url, db_name, space_name)


class TestDateField:
    def setup_class(self):
        self.xb = xb

    # prepare
    def test_prepare_cluster(self):
        embedding_size = self.xb.shape[1]
        properties = {}
        properties["fields"] = [
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
                "name": "field_date",
                "type": "date",
                "index": {"name": "field_string", "type": "SCALAR"},
            },
            {
                "name": "field_vector",
                "type": "vector",
                "dimension": embedding_size,
                "index": {
                    "name": "gamma",
                    "type": "FLAT",
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
        ]
        create(router_url, properties)

    def test_prepare_upsert(self):
        embedding_size = self.xb.shape[1]
        batch_size = 100
        total_batch = 1
        add_date(
            db_name,
            space_name,
            0,
            total_batch,
            batch_size,
            embedding_size,
            "str",
        )
        assert get_space_num() == int(total_batch * batch_size)

        add_date(db_name, space_name, 1, 2, batch_size, embedding_size, "timestamp")
        assert get_space_num() == int(total_batch * batch_size * 2)

    def test_query_date(self):
        today_str = datetime.date.today().strftime("%Y-%m-%d")
        for i in range(100):
            query_dict = {
                "document_ids": [str(i)],
                "limit": 1,
                "db_name": db_name,
                "space_name": space_name,
            }
            url = router_url + "/document/query?trace=true"
            json_str = json.dumps(query_dict)
            rs = requests.post(url, auth=(username, password), data=json_str)
            logger.info(rs.json())
            assert rs.status_code == 200
            dt = datetime.datetime.strptime(
                rs.json()["data"]["documents"][0]["field_date"], "%Y-%m-%dT%H:%M:%S%z"
            )
            date_str = dt.strftime("%Y-%m-%d")
            assert date_str == today_str

        for i in range(100, 200):
            query_dict = {
                "document_ids": [str(i)],
                "limit": 1,
                "db_name": db_name,
                "space_name": space_name,
            }
            url = router_url + "/document/query?trace=true"
            json_str = json.dumps(query_dict)
            rs = requests.post(url, auth=(username, password), data=json_str)
            logger.info(rs.json())
            assert rs.status_code == 200
            dt = datetime.datetime.strptime(
                rs.json()["data"]["documents"][0]["field_date"], "%Y-%m-%dT%H:%M:%S%z"
            )
            date_str = dt.strftime("%Y-%m-%d")
            assert date_str == today_str

        for i in range(100):
            query_dict = {
                "document_ids": [str(i)],
                "limit": 1,
                "db_name": db_name,
                "space_name": space_name,
                "get_by_query": True,
            }
            url = router_url + "/document/query?trace=true"
            json_str = json.dumps(query_dict)
            rs = requests.post(url, auth=(username, password), data=json_str)
            logger.info(rs.json())
            assert rs.status_code == 200
            dt = datetime.datetime.strptime(
                rs.json()["data"]["documents"][0]["field_date"], "%Y-%m-%dT%H:%M:%S%z"
            )
            date_str = dt.strftime("%Y-%m-%d")
            assert date_str == today_str

        for i in range(100, 200):
            query_dict = {
                "document_ids": [str(i)],
                "limit": 1,
                "db_name": db_name,
                "space_name": space_name,
                "get_by_query": True,
            }
            url = router_url + "/document/query?trace=true"
            json_str = json.dumps(query_dict)
            rs = requests.post(url, auth=(username, password), data=json_str)
            logger.info(rs.json())
            assert rs.status_code == 200
            dt = datetime.datetime.strptime(
                rs.json()["data"]["documents"][0]["field_date"], "%Y-%m-%dT%H:%M:%S%z"
            )
            date_str = dt.strftime("%Y-%m-%d")
            assert date_str == today_str

        query_dict = {
            "document_ids": [],
            "limit": 200,
            "db_name": db_name,
            "space_name": space_name,
        }
        query_dict["filters"] = {
            "operator": "AND",
            "conditions": [
                {
                    "field": "field_date",
                    "operator": "<=",
                    "value": today_str,
                },
                {
                    "field": "field_date",
                    "operator": ">=",
                    "value": today_str,
                },
            ],
        }
        json_str = json.dumps(query_dict)
        rs = requests.post(url, auth=(username, password), data=json_str)
        # logger.info(rs.json())
        assert rs.status_code == 200
        assert len(rs.json()["data"]["documents"]) == 200

        query_dict = {
            "document_ids": [],
            "limit": 200,
            "db_name": db_name,
            "space_name": space_name,
        }
        query_dict["filters"] = {
            "operator": "AND",
            "conditions": [
                {
                    "field": "field_date",
                    "operator": "<=",
                    "value": int(
                        datetime.datetime.strptime(today_str, "%Y-%m-%d")
                        .replace(tzinfo=datetime.timezone.utc)
                        .timestamp()
                    ),
                },
                {
                    "field": "field_date",
                    "operator": ">=",
                    "value": int(
                        datetime.datetime.strptime(today_str, "%Y-%m-%d")
                        .replace(tzinfo=datetime.timezone.utc)
                        .timestamp()
                    ),
                },
            ],
        }
        json_str = json.dumps(query_dict)
        rs = requests.post(url, auth=(username, password), data=json_str)
        # logger.info(rs.json())
        assert rs.status_code == 200
        assert len(rs.json()["data"]["documents"]) == 200

    # destroy
    def test_destroy_cluster(self):
        destroy(router_url, db_name, space_name)
