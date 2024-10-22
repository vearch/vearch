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

__description__ = """ test case for module space """

sift10k = DatasetSift10K()
xb = sift10k.get_database()
xq = sift10k.get_queries()
gt = sift10k.get_groundtruth()


class TestSpaceCreate:
    def setup_class(self):
        pass

    def test_prepare_db(self):
        response = create_db(router_url, db_name)
        assert response.json()["code"] == 0

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
            "fields": [
                {"name": "field_string", "type": "keyword"},
                {"name": "field_int", "type": "integer"},
                {
                    "name": "field_float",
                    "type": "float",
                    "index": {
                        "name": "field_float",
                        "type": "SCALAR",
                    },
                },
                {
                    "name": "field_string_array",
                    "type": "string",
                    "array": True,
                    "index": {
                        "name": "field_string_array",
                        "type": "SCALAR",
                    },
                },
                {
                    "name": "field_int_index",
                    "type": "integer",
                    "index": {
                        "name": "field_int_index",
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
                            "nsubvector": 32,
                            "nlinks": 32,
                            "efConstruction": 40,
                            "nprobe": 80,
                            "efSearch": 64,
                            "training_threshold": 70000,
                        },
                    },
                },
                # {
                #     "name": "field_vector_normal",
                #     "type": "vector",
                #     "dimension": int(embedding_size * 2),
                #     "format": "normalization"
                # }
            ],
        }

        response = create_space(router_url, db_name, space_config)
        assert response.json()["code"] == 0

        response = describe_space(router_url, db_name, space_name)
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = drop_space(router_url, db_name, space_name)
        assert response.json()["code"] == 0

    @pytest.mark.parametrize(
        ["index_type"],
        [["FLAT"], ["IVFPQ"], ["IVFFLAT"], ["HNSW"]],
    )
    def test_vearch_space_create_empty_index_params(self, index_type):
        embedding_size = 128
        space_config = {
            "name": space_name,
            "partition_num": 1,
            "replica_num": 1,
            "fields": [
                {"name": "field_string", "type": "keyword"},
                {"name": "field_int", "type": "integer"},
                {
                    "name": "field_float",
                    "type": "float",
                    "index": {
                        "name": "field_float",
                        "type": "SCALAR",
                    },
                },
                {
                    "name": "field_double",
                    "type": "double",
                    "array": True,
                    "index": {
                        "name": "field_double",
                        "type": "SCALAR",
                    },
                },
                {
                    "name": "field_long",
                    "type": "long",
                    "index": {
                        "name": "field_long_index",
                        "type": "SCALAR",
                    },
                },
                {
                    "name": "field_vector",
                    "type": "vector",
                    "dimension": embedding_size,
                    "index": {"name": "gamma", "type": index_type},
                },
            ],
        }

        response = create_space(router_url, db_name, space_config)
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = describe_space(router_url, db_name, space_name)
        logger.info(response.json())
        assert response.json()["code"] == 0

        total = 10
        add(total, 1, xb, False, True)
        time.sleep(3)

        search_url = router_url + "/document/search"
        data = {}
        data["db_name"] = db_name
        data["space_name"] = space_name
        data["vectors"] = []
        vector_info = {
            "field": "field_vector",
            "feature": xq[:1].flatten().tolist(),
        }
        data["vectors"].append(vector_info)
        json_str = json.dumps(data)
        response = requests.post(
            search_url, auth=(username, password), data=json_str
        )
        logger.info(response.json())
        assert len(response.json()["data"]["documents"]) == 1
        assert len(response.json()["data"]["documents"][0]) == total

        response = drop_space(router_url, db_name, space_name)
        assert response.json()["code"] == 0

    @pytest.mark.parametrize(
        ["wrong_index", "wrong_type", "index_type"],
        [
            [0, "bad training_threshold", "IVFPQ"],
            [1, "bad training_threshold", "IVFFLAT"],
            [2, "bad space name", "FLAT"],
            [3, "beyond max nlinks", "HNSW"],
            [4, "below min nlinks", "HNSW"],
            [5, "beyond max efConstruction", "HNSW"],
            [6, "below min efConstruction", "HNSW"],
            [7, "beyond max ncentroids", "IVFPQ"],
            [8, "beyond max ncentroids", "IVFFLAT"],
            [9, "below min ncentroids", "IVFPQ"],
            [10, "below min ncentroids", "IVFFLAT"],
            [11, "bad nsubvector", "IVFPQ"],
            [12, "bad metric type", "FLAT"],
            [13, "bad nprobe", "IVFPQ"],
            [14, "empty index", "IVFPQ"],
            [15, "null index", "IVFPQ"],
            [16, "unsupported field type", "IVFPQ"],
            [17, "unsupported index type", "unsupported"],
            [18, "empty index type", ""],
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
        nlinks = 32
        if wrong_index == 3:
            nlinks = 97
        if wrong_index == 4:
            nlinks = 7
        efConstruction = 100
        if wrong_index == 5:
            efConstruction = 1025
        if wrong_index == 6:
            efConstruction = 15
        ncentroids = 2048
        if wrong_index == 7 or wrong_index == 8:
            ncentroids = 262145
        if wrong_index == 9 or wrong_index == 10:
            ncentroids = 0
        nsubvector = int(embedding_size / 2)
        if wrong_index == 11:
            nsubvector = 33
        metric_type = "InnerProduct"
        if wrong_index == 12:
            metric_type = "WRONG_TYPE"
        nprobe = 80
        if wrong_index == 13:
            nprobe = 99999
        space_config = {
            "name": create_space_name,
            "partition_num": 1,
            "replica_num": replica_num,
            "fields": [
                {"name": "field_string", "type": "keyword"},
                {"name": "field_int", "type": "integer"},
                {
                    "name": "field_float",
                    "type": "float",
                    "index": {"name": "field_float", "type": "SCALAR"},
                },
                {
                    "name": "field_string_array",
                    "type": "string",
                    "array": True,
                    "index": {"name": "field_string_array", "type": "SCALAR"},
                },
                {
                    "name": "field_int_index",
                    "type": "integer",
                    "index": {"name": "field_int_index", "type": "SCALAR"},
                },
                {
                    "name": "field_vector_normal",
                    "type": "vector",
                    "dimension": int(embedding_size * 2),
                    "format": "normalization",
                    "index": {
                        "name": "gamma",
                        "type": index_type,
                        "params": {
                            "metric_type": metric_type,
                            "ncentroids": ncentroids,
                            "nsubvector": nsubvector,
                            "nlinks": nlinks,
                            "nprobe": nprobe,
                            "efConstruction": efConstruction,
                            "training_threshold": training_threshold,
                        },
                    },
                },
            ],
        }
        if wrong_index == 14:
            space_config["fields"][5]["index"] = {}
        if wrong_index == 15:
            space_config["fields"][5].pop("index")
        if wrong_index == 16:
            field_unspported = {"name": "field_string", "type": "unsupported"}
            space_config["fields"].append(field_unspported)

        response = create_space(router_url, db_name, space_config)
        logger.info(response.json())
        assert response.json()["code"] != 0

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
            "fields": [
                {
                    "name": "field_string",
                    "type": "keyword",
                    "index": {"name": "field_string", "type": "SCALAR"},
                },
                {"name": "field_int", "type": "integer"},
                {
                    "name": "field_float",
                    "type": "float",
                    "index": {"name": "field_float", "type": "SCALAR"},
                },
                {
                    "name": "field_string_array",
                    "type": "string",
                    "array": True,
                    "index": {"name": "field_string_array", "type": "SCALAR"},
                },
                {
                    "name": "field_int_index",
                    "type": "integer",
                    "index": {"name": "field_int_index", "type": "SCALAR"},
                },
                {
                    "name": "field_vector_normal",
                    "type": "vector",
                    "dimension": int(embedding_size * 2),
                    "format": "normalization",
                    "index": {
                        "name": "gamma",
                        "type": "FLAT",
                        "params": {
                            "metric_type": "InnerProduct",
                            "ncentroids": 2048,
                            "nsubvector": 32,
                            "nlinks": 32,
                            "efConstruction": 40,
                        },
                    },
                },
            ],
        }

        response = create_space(router_url, db_name, space_config)
        logger.info(response.json())
        assert response.json()["code"] == 0

        describe_db_name = db_name
        describe_space_name = space_name
        if wrong_index == 0:
            describe_db_name = "wrong_db"
        if wrong_index == 1:
            describe_space_name = "wrong_space"
        response = describe_space(
            router_url, describe_db_name, describe_space_name
        )
        logger.info(response.json())
        assert response.json()["code"] != 0

        response = drop_space(router_url, db_name, space_name)
        assert response.json()["code"] == 0

    def test_destroy_db(self):
        drop_db(router_url, db_name)


class TestSpaceExpansion:
    def setup_class(self):
        pass

    @pytest.mark.parametrize(
        ["index_type"],
        [["FLAT"]],
    )
    def test_vearch_update_space_partition(self, index_type):
        embedding_size = xb.shape[1]
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
                "name": "field_vector",
                "type": "vector",
                "index": {
                    "name": "gamma",
                    "type": index_type,
                    "params": {
                        "metric_type": "L2",
                    },
                },
                "dimension": embedding_size,
                "store_type": "MemoryOnly",
                # "format": "normalization"
            },
        ]

        create_for_document_test(router_url, embedding_size, properties)

    def test_document_upsert(self):
        batch_size = 100
        total_batch = 1
        total = int(batch_size * total_batch)

        add(total_batch, batch_size, xb, False, True)

        waiting_index_finish(total, 1)

    def test_update_space_partition(self):
        response = update_space_partition(router_url, db_name, space_name, 2)
        assert response.json()["code"] == 0
        logger.info(response.json()["data"]["partitions"])

        # check cache whether updated
        response = get_space_cache(router_url, db_name, space_name)
        logger.info(response.text)
        assert response.json()["code"] == 0

        # now not support reduce partition num
        response = update_space_partition(router_url, db_name, space_name, 1)
        logger.info(response.json())
        assert response.json()["code"] != 0

        response = describe_space(router_url, db_name, space_name)
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_document_operation(self):
        # add aggin
        batch_size = 100
        total_batch = 1
        total = int(batch_size * total_batch)

        partition_id = get_partition(router_url, db_name, space_name)
        assert len(partition_id) == 2
        old_partition, new_partition = partition_id

        add(
            total_batch,
            batch_size,
            xb,
            False,
            True,
            partitions=[new_partition],
        )

        waiting_index_finish(total * 2, 1)

        response = get_space(router_url, db_name, space_name)
        logger.info(response.json())
        query_dict = {
            "document_ids": ["0"],
            "partition_id": old_partition,
            "limit": 1,
            "db_name": db_name,
            "space_name": space_name,
        }
        document_ids = []
        query_url = router_url + "/document/query"
        for i in range(total):
            query_dict["document_ids"] = [str(i)]
            json_str = json.dumps(query_dict)
            response = requests.post(
                query_url, auth=(username, password), data=json_str
            )
            assert len(response.json()["data"]["documents"]) == 1
            assert response.json()["data"]["documents"][0]["field_int"] == i
            document_ids.append(response.json()["data"]["documents"][0]["_id"])

        for i in range(total):
            query_dict["document_ids"] = [str(i)]
            query_dict["partition_id"] = new_partition
            json_str = json.dumps(query_dict)
            response = requests.post(
                query_url, auth=(username, password), data=json_str
            )
            assert len(response.json()["data"]["documents"]) == 1
            assert response.json()["data"]["documents"][0]["field_int"] == i
            document_ids.append(response.json()["data"]["documents"][0]["_id"])

        query_dict.pop("partition_id")
        for i in range(len(document_ids)):
            query_dict["document_ids"] = [document_ids[i]]
            json_str = json.dumps(query_dict)
            response = requests.post(
                query_url, auth=(username, password), data=json_str
            )
            assert len(response.json()["data"]["documents"]) == 1
            field_int = i
            if i >= 100:
                field_int = i % 100
            assert response.json()["data"]["documents"][0]["field_int"] == field_int

        # add again, Each shard will have roughly half of the data
        add(total_batch, batch_size, xb, False, True)
        waiting_index_finish(total * 3, 1)

        response = get_space(router_url, db_name, space_name)
        partitions = response.json()["data"]["partitions"]
        p1_num = partitions[0]["index_num"]
        p2_num = partitions[1]["index_num"]
        logger.info(partitions)
        assert abs(p1_num - p2_num) <= 100

    def test_destroy_space(self):
        response = drop_space(router_url, db_name, space_name)
        assert response.json()["code"] == 0

    def test_destroy_db(self):
        drop_db(router_url, db_name)