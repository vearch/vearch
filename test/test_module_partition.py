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
import datetime

__description__ = """ test case for module partition """


class TestPartitionEmptySpaceMemorySize:
    def setup_class(self):
        pass

    def test_prepare_db(self):
        logger.info(create_db(router_url, db_name))

    @pytest.mark.parametrize(
        ["embedding_size", "index_type"],
        [
            [128, "FLAT"],
            [128, "IVFPQ"],
            [128, "IVFFLAT"],
            [128, "HNSW"],
            [512, "FLAT"],
            [512, "IVFPQ"],
            [512, "IVFFLAT"],
            [512, "HNSW"],
            [1536, "FLAT"],
            [1536, "IVFPQ"],
            [1536, "IVFFLAT"],
            [1536, "HNSW"],
        ],
    )
    def test_vearch_space_create(self, embedding_size, index_type):
        space_config = {
            "name": space_name + "empty" + str(embedding_size) + index_type,
            "partition_num": 1,
            "replica_num": 1,
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
                    # "format": "normalization"
                },
            ],
        }

        logger.info(create_space(router_url, db_name, space_config))

    def test_destroy_db(self):
        response = list_spaces(router_url, db_name)
        logger.info(response.json())
        for space in response.json()["data"]:
            response = drop_space(router_url, db_name, space["space_name"])
            logger.info(response)
        drop_db(router_url, db_name)


class TestPartitionSmallDataMemorySize:
    def setup_class(self):
        pass

    def test_prepare_db(self):
        logger.info(create_db(router_url, db_name))

    @pytest.mark.parametrize(
        ["embedding_size", "index_type"],
        [
            [128, "FLAT"],
            [128, "IVFPQ"],
            [128, "IVFFLAT"],
            [128, "HNSW"],
            [512, "FLAT"],
            [512, "IVFPQ"],
            [512, "IVFFLAT"],
            [512, "HNSW"],
            [1536, "FLAT"],
            [1536, "IVFPQ"],
            [1536, "IVFFLAT"],
            [1536, "HNSW"],
        ],
    )
    def test_vearch_space_create(self, embedding_size, index_type):
        space_name_each = space_name + "smalldata" + str(embedding_size) + index_type
        space_config = {
            "name": space_name_each,
            "partition_num": 1,
            "replica_num": 1,
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
                    # "format": "normalization"
                },
            ],
        }

        response = create_space(router_url, db_name, space_config)
        logger.info(response)

        add_embedding_size(db_name, space_name_each, 10, 100, embedding_size)

    def test_destroy_db(self):
        response = list_spaces(router_url, db_name)
        logger.info(response.json())
        for space in response.json()["data"]:
            response = drop_space(router_url, db_name, space["space_name"])
            logger.info(response)
        drop_db(router_url, db_name)


class TestPartitionRule:
    def setup_class(self):
        pass

    def test_prepare_db(self):
        logger.info(create_db(router_url, db_name))

    @pytest.mark.parametrize(
        ["embedding_size", "index_type"],
        [[128, "FLAT"]],
    )
    def test_vearch_space_create(self, embedding_size, index_type):
        today = datetime.datetime.today().date()
        tomorrow = today + datetime.timedelta(days=1)
        day_after_tomorrow = today + datetime.timedelta(days=2)
        date_format = "%Y-%m-%d"
        space_config = {
            "name": space_name,
            "partition_num": 2,
            "replica_num": 1,
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
                    "name": "field_date",
                    "type": "date",
                    "index": {"name": "field_date", "type": "SCALAR"},
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
            "partition_rule": {
                "type": "RANGE",
                "field": "field_date",
                "ranges": [
                    {"name": "p0", "value": today.strftime(date_format)},
                    {"name": "p1", "value": tomorrow.strftime(date_format)},
                    {"name": "p2", "value": day_after_tomorrow.strftime(date_format)},
                ],
            },
        }

        response = create_space(router_url, db_name, space_config)
        logger.info(response.json())
        assert response.json()["code"] == 0
        add_date(db_name, space_name, 0, 10, 100, embedding_size, "str")

        response = describe_space(router_url, db_name, space_name)
        logger.info(response.json())
        assert response.json()["code"] == 0

        assert get_partitions_doc_num("p1") == 10 * 100

        waiting_index_finish(10 * 100, 1)

        for i in range(1000):
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
            assert rs.json()["code"] == 0
            assert rs.json()["data"]["documents"][0]["field_float"] == float(i)

    def test_drop_partitions(self):
        response = update_space_partition_rule(
            router_url, db_name, space_name, partition_name="p1", operator_type="DROP"
        )
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = describe_space(router_url, db_name, space_name)
        logger.info(response.json())
        assert response.json()["code"] == 0

        assert get_space_num() == 0

    def test_add_partitions(self):
        embedding_size = 128
        today = datetime.datetime.today().date()
        today_str = today.strftime("%Y-%m-%d")
        tomorrow = today + datetime.timedelta(days=1)
        tomorrow_str = tomorrow.strftime("%Y-%m-%d")
        date_format = "%Y-%m-%d"
        day_after_3 = today + datetime.timedelta(days=3)

        rule = {
            "partition_rule": {
                "type": "RANGE",
                "ranges": [
                    {"name": "p3", "value": day_after_3.strftime(date_format)},
                    {"name": "p1", "value": tomorrow.strftime(date_format)},
                ],
            }
        }

        response = update_space_partition_rule(
            router_url, db_name, space_name, operator_type="ADD", partition_rule=rule
        )
        logger.info(response.json())
        assert response.json()["code"] == 0

        add_date(db_name, space_name, 0, 10, 100, embedding_size, "str")

        response = describe_space(router_url, db_name, space_name)
        logger.info(response.json())
        assert response.json()["code"] == 0

        assert get_partitions_doc_num("p1") == 10 * 100

        waiting_index_finish(10 * 100, 1)

        # _id is same and partition field is same
        add_date(db_name, space_name, 0, 10, 100, embedding_size, "str")

        assert get_partitions_doc_num("p1") == 10 * 100

        # _id is same but partition field is different
        add_date(db_name, space_name, 0, 10, 100, embedding_size, "str", delta=1)

        assert get_partitions_doc_num("p2") == 10 * 100

        assert get_space_num() == 2 * 10 * 100

        days = [today_str, tomorrow_str]
        for i in range(1000):
            query_dict = {
                "document_ids": [str(i)],
                "limit": 2,
                "db_name": db_name,
                "space_name": space_name,
            }
            url = router_url + "/document/query?trace=true"
            json_str = json.dumps(query_dict)
            rs = requests.post(url, auth=(username, password), data=json_str)
            # logger.info(rs.json())
            assert rs.json()["code"] == 0
            assert len(rs.json()["data"]["documents"]) == 2
            assert rs.json()["data"]["documents"][0]["field_float"] == float(i)

            results = []

            dt = datetime.datetime.strptime(
                rs.json()["data"]["documents"][0]["field_date"], "%Y-%m-%dT%H:%M:%S%z"
            )
            results.append(dt.strftime("%Y-%m-%d"))

            dt1 = datetime.datetime.strptime(
                rs.json()["data"]["documents"][1]["field_date"], "%Y-%m-%dT%H:%M:%S%z"
            )
            results.append(dt1.strftime("%Y-%m-%d"))
            results.sort()
            assert days == results

        for i in range(1000):
            query_dict = {
                "document_ids": [str(i)],
                "limit": 2,
                "db_name": db_name,
                "space_name": space_name,
                "get_by_hash": True,
            }
            url = router_url + "/document/query?trace=true"
            json_str = json.dumps(query_dict)
            rs = requests.post(url, auth=(username, password), data=json_str)
            logger.info(rs.json())
            assert rs.json()["code"] == 0
            assert len(rs.json()["data"]["documents"]) == 1
            if rs.json()["data"]["total"] == 0:
                assert rs.json()["data"]["documents"][0]["code"] == 404
            else:
                assert rs.json()["data"]["documents"][0]["field_float"] == float(i)

        # _id is different but partition field is same
        add_date(db_name, space_name, 10, 20, 100, embedding_size, "random")

        waiting_index_finish(10 * 100 * 2, 1)

        assert get_space_num() == 3 * 10 * 100

        for i in range(1000, 2000):
            query_dict = {
                "document_ids": [str(i)],
                "limit": 2,
                "db_name": db_name,
                "space_name": space_name,
            }
            url = router_url + "/document/query?trace=true"
            json_str = json.dumps(query_dict)
            rs = requests.post(url, auth=(username, password), data=json_str)
            # logger.info(rs.json())
            assert rs.json()["code"] == 0
            assert len(rs.json()["data"]["documents"]) == 1
            assert rs.json()["data"]["documents"][0]["field_float"] == float(i)

        for i in range(1000, 2000):
            query_dict = {
                "document_ids": [str(i)],
                "limit": 2,
                "db_name": db_name,
                "space_name": space_name,
                "get_by_hash": True,
            }
            url = router_url + "/document/query?trace=true"
            json_str = json.dumps(query_dict)
            rs = requests.post(url, auth=(username, password), data=json_str)
            # logger.info(rs.json())
            assert rs.json()["code"] == 0
            assert len(rs.json()["data"]["documents"]) == 1
            if rs.json()["data"]["total"] == 0:
                assert rs.json()["data"]["documents"][0]["code"] == 404
            else:
                assert rs.json()["data"]["documents"][0]["field_float"] == float(i)

        response = describe_space(router_url, db_name, space_name)
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_destroy_db(self):
        response = list_spaces(router_url, db_name)
        logger.info(response.json())
        for space in response.json()["data"]:
            response = drop_space(router_url, db_name, space["space_name"])
            logger.info(response)
        drop_db(router_url, db_name)


class TestPartitionRuleWithWeek:
    def setup_class(self):
        pass

    def test_prepare_db(self):
        logger.info(create_db(router_url, db_name))

    @pytest.mark.parametrize(
        ["embedding_size", "index_type"],
        [[128, "FLAT"]],
    )
    def test_vearch_space_create(self, embedding_size, index_type):
        today = datetime.datetime.today().date()
        next_week = today + datetime.timedelta(days=7)
        week_after_next = today + datetime.timedelta(days=14)
        date_format = "%Y-%m-%d"
        space_config = {
            "name": space_name,
            "partition_num": 2,
            "replica_num": 1,
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
                    "name": "field_date",
                    "type": "date",
                    "index": {"name": "field_date", "type": "SCALAR"},
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
            "partition_rule": {
                "type": "RANGE",
                "field": "field_date",
                "ranges": [
                    {"name": "p0", "value": today.strftime(date_format)},
                    {"name": "p1", "value": next_week.strftime(date_format)},
                    {"name": "p2", "value": week_after_next.strftime(date_format)},
                ],
            },
        }

        response = create_space(router_url, db_name, space_config)
        logger.info(response.json())
        assert response.json()["code"] == 0

        add_date(db_name, space_name, 0, 10, 100, embedding_size, "str")

        response = describe_space(router_url, db_name, space_name)
        logger.info(response.json())
        assert response.json()["code"] == 0

        assert get_partitions_doc_num("p1") == 10 * 100

        waiting_index_finish(10 * 100, 1)

        # _id is same and partition field is same
        add_date(db_name, space_name, 0, 10, 100, embedding_size, "str")

        assert get_partitions_doc_num("p1") == 10 * 100

        # _id is same but partition field value is different, range is same
        add_date(db_name, space_name, 0, 10, 100, embedding_size, "str", delta=1)

        assert get_partitions_doc_num("p1") == 10 * 100

        assert get_space_num() == 1 * 10 * 100

        # _id is different but partition field is same
        add_date(db_name, space_name, 10, 20, 100, embedding_size, "random")

        waiting_index_finish(10 * 100 * 2, 1)

        assert get_partitions_doc_num("p1") == 20 * 100

        assert get_space_num() == 2 * 10 * 100

        response = describe_space(router_url, db_name, space_name)
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_destroy_db(self):
        response = list_spaces(router_url, db_name)
        logger.info(response.json())
        for space in response.json()["data"]:
            response = drop_space(router_url, db_name, space["space_name"])
            logger.info(response)
        drop_db(router_url, db_name)


class TestPartitionRuleBadCase:
    def setup_class(self):
        pass

    def test_prepare_db(self):
        logger.info(create_db(router_url, db_name))

    @pytest.mark.parametrize(
        ["wrong_index", "bad_type"],
        [
            [0, "partition type is not range"],
            [1, "partition field type is not date"],
            [2, "partition rule is empty"],
            [3, "partition type is empty"],
            [4, "partition field is empty"],
            [5, "partition ranges is not increasing"],
            [6, "partition ranges name has some"],
            [7, "partition ranges value has some"],
            [8, "partition ranges is empty"],
        ],
    )
    def test_vearch_space_create_badcase(self, wrong_index, bad_type):
        embedding_size = 128
        index_type = "HNSW"
        today = datetime.datetime.today().date()
        tomorrow = today + datetime.timedelta(days=1)
        day_after_tomorrow = today + datetime.timedelta(days=2)
        date_format = "%Y-%m-%d"
        space_config = {
            "name": space_name,
            "partition_num": 1,
            "replica_num": 1,
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
                    "name": "field_date",
                    "type": "date",
                    "index": {"name": "field_date", "type": "SCALAR"},
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
            "partition_rule": {
                "type": "RANGE",
                "field": "field_date",
                "ranges": [
                    {"name": "p0", "value": today.strftime(date_format)},
                    {"name": "p1", "value": tomorrow.strftime(date_format)},
                    {"name": "p2", "value": day_after_tomorrow.strftime(date_format)},
                ],
            },
        }
        if wrong_index == 0:
            space_config["partition_rule"]["type"] = "KEY"
        if wrong_index == 1:
            space_config["partition_rule"]["field"] = "field_string"
        if wrong_index == 2:
            space_config["partition_rule"] = ""
        if wrong_index == 3:
            space_config["partition_rule"]["type"] = ""
        if wrong_index == 4:
            space_config["partition_rule"]["field"] = ""
        if wrong_index == 5:
            space_config["partition_rule"]["ranges"] = [
                {"name": "p0", "value": today.strftime(date_format)},
                {"name": "p2", "value": day_after_tomorrow.strftime(date_format)},
                {"name": "p1", "value": tomorrow.strftime(date_format)},
            ]
        if wrong_index == 6:
            space_config["partition_rule"]["ranges"] = [
                {"name": "p0", "value": today.strftime(date_format)},
                {"name": "p0", "value": day_after_tomorrow.strftime(date_format)},
                {"name": "p1", "value": tomorrow.strftime(date_format)},
            ]
        if wrong_index == 7:
            space_config["partition_rule"]["ranges"] = [
                {"name": "p0", "value": today.strftime(date_format)},
                {"name": "p1", "value": today.strftime(date_format)},
                {"name": "p2", "value": tomorrow.strftime(date_format)},
            ]
        if wrong_index == 8:
            space_config["partition_rule"]["ranges"] = []

        response = create_space(router_url, db_name, space_config)
        logger.info(response.json())
        assert response.json()["code"] != 0

    @pytest.mark.parametrize(
        ["embedding_size", "index_type"],
        [[128, "HNSW"]],
    )
    def test_vearch_space_create(self, embedding_size, index_type):
        today = datetime.datetime.today().date()
        tomorrow = today + datetime.timedelta(days=1)
        day_after_tomorrow = today + datetime.timedelta(days=2)
        date_format = "%Y-%m-%d"
        space_config = {
            "name": space_name,
            "partition_num": 1,
            "replica_num": 1,
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
                    "name": "field_date",
                    "type": "date",
                    "index": {"name": "field_date", "type": "SCALAR"},
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
            "partition_rule": {
                "type": "RANGE",
                "field": "field_date",
                "ranges": [
                    {"name": "p0", "value": today.strftime(date_format)},
                    {"name": "p1", "value": tomorrow.strftime(date_format)},
                    {"name": "p2", "value": day_after_tomorrow.strftime(date_format)},
                ],
            },
        }

        response = create_space(router_url, db_name, space_config)
        logger.info(response.json())
        assert response.json()["code"] == 0
        add_date(db_name, space_name, 0, 10, 100, embedding_size, "str")

        waiting_index_finish(10 * 100, 1)

        response = describe_space(router_url, db_name, space_name)
        logger.info(response.json())
        assert response.json()["code"] == 0

    @pytest.mark.parametrize(
        ["wrong_index", "bad_type"],
        [
            [0, "don't have partition key"],
            [1, "can't set partition"],
            [2, "bad date timestamp"],
        ],
    )
    def test_rule_add_badcase(self, wrong_index, bad_type):
        today = datetime.datetime.today().date()
        date_format = "%Y-%m-%d"
        embedding_size = 128
        url = router_url + "/document/upsert"
        data = {}
        data["documents"] = []

        data["db_name"] = db_name
        data["space_name"] = space_name
        param_dict = {}
        param_dict["_id"] = "10000"
        param_dict["field_int"] = 10000
        param_dict["field_vector"] = [random.random() for i in range(embedding_size)]
        param_dict["field_long"] = 10000
        param_dict["field_float"] = 10000
        param_dict["field_double"] = 10000
        param_dict["field_string"] = "10000"
        date_str = datetime.date.today().strftime(date_format)
        param_dict["field_date"] = date_str
        data["documents"].append(param_dict)

        if wrong_index == 0:
            data["documents"][0].pop("field_date")
        if wrong_index == 1:
            day_after_3 = today + datetime.timedelta(days=3)
            data["documents"][0]["field_date"] = day_after_3.strftime(date_format)
        if wrong_index == 2:
            data["documents"][0]["field_date"] = (
                datetime.datetime.strptime(date_str, date_format)
                .replace(tzinfo=datetime.timezone.utc)
                .timestamp()
            )

        response = requests.post(url, auth=(username, password), json=data)
        logger.info(response.json())
        assert response.json()["code"] != 0

    @pytest.mark.parametrize(
        ["wrong_index", "bad_type"],
        [
            [0, "partition name not exist"],
            [1, "partition type is not DROP"],
            [2, "db not exist"],
            [3, "space not exist"],
        ],
    )
    def test_drop_partitions_badcase(self, wrong_index, bad_type):
        db = db_name
        space = space_name
        partition_name = "p0"
        operator_type = "DROP"
        if wrong_index == 0:
            partition_name = "p"
        if wrong_index == 1:
            operator_type = "drop"
        if wrong_index == 2:
            db = "p"
        if wrong_index == 3:
            space = "p"

        response = update_space_partition_rule(
            router_url,
            db,
            space,
            partition_name=partition_name,
            operator_type=operator_type,
        )
        logger.info(response.json())
        assert response.json()["code"] != 0

    @pytest.mark.parametrize(
        ["wrong_index", "bad_type"],
        [
            [0, "partition name exist"],
            [1, "partition type is not ADD"],
            [2, "partition range value exist"],
            [3, "db not exist"],
            [4, "space not exist"],
        ],
    )
    def test_add_partitions_badcase(self, wrong_index, bad_type):
        today = datetime.datetime.today().date()
        date_format = "%Y-%m-%d"
        day_after_3 = today + datetime.timedelta(days=3)
        day_after_tomorrow = today + datetime.timedelta(days=2)
        operator_type = "ADD"
        db = db_name
        space = space_name

        rule = {
            "partition_rule": {
                "type": "RANGE",
                "ranges": [
                    {"name": "p3", "value": day_after_3.strftime(date_format)},
                    {"name": "p0", "value": today.strftime(date_format)},
                ],
            }
        }

        if wrong_index == 0:
            rule["partition_rule"]["ranges"][0]["name"] = "p2"
        if wrong_index == 1:
            operator_type = "add"
        if wrong_index == 2:
            rule["partition_rule"]["ranges"][0]["value"] = day_after_tomorrow.strftime(
                date_format
            )
        if wrong_index == 3:
            db = "p"
        if wrong_index == 4:
            space = "p"

        response = update_space_partition_rule(
            router_url,
            db,
            space,
            operator_type=operator_type,
            partition_rule=rule,
        )
        logger.info(response.json())
        assert response.json()["code"] != 0

    def test_destroy_db(self):
        response = list_spaces(router_url, db_name)
        logger.info(response.json())
        for space in response.json()["data"]:
            response = drop_space(router_url, db_name, space["space_name"])
            logger.info(response)
        drop_db(router_url, db_name)
