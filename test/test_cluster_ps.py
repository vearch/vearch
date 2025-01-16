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
import random
import time
from utils import vearch_utils
from utils.vearch_utils import logger
from utils.data_utils import *

__description__ = """ test case for cluster partition server """


class TestClusterPartitionServerAdd:
    def setup_class(self):
        pass

    def test_prepare_db(self):
        response = vearch_utils.create_db(vearch_utils.router_url, vearch_utils.db_name)
        logger.info(response.json())

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
        ],
    )
    def test_vearch_space_create(self, embedding_size, index_type):
        space_name_each = (
            vearch_utils.space_name + "smalldata" + str(embedding_size) + index_type
        )
        space_config = {
            "name": space_name_each,
            "partition_num": 2,
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

        response = vearch_utils.create_space(
            vearch_utils.router_url, vearch_utils.db_name, space_config
        )
        logger.info(response.json())
        # default now should be 3
        assert response.json()["data"]["replica_num"] == 3

        vearch_utils.add_embedding_size(
            vearch_utils.db_name, space_name_each, 50, 100, embedding_size
        )

        vearch_utils.delete_interface(
            10,
            100,
            delete_type="by_ids",
            delete_db_name=vearch_utils.db_name,
            delete_space_name=space_name_each,
        )


def waitfor_leader_status():
    leader_status = 0
    while leader_status == 0:
        response = vearch_utils.list_spaces(
            vearch_utils.router_url, vearch_utils.db_name
        )
        for space in response.json()["data"]:
            for partition in space["partitions"]:
                leader_status = partition["raft_status"]["Leader"]
                if leader_status == 0:
                    break
            if leader_status == 0:
                break
        time.sleep(15)
    return leader_status


class TestClusterPartitionServerRecover:
    def test_ps_recover(self):
        num = 0
        while num != 3:
            response = vearch_utils.get_servers_status(vearch_utils.router_url)
            num = response.json()["data"]["count"]
            time.sleep(5)
        logger.info(num)

        leader_status = waitfor_leader_status()
        logger.info(leader_status)
        response = vearch_utils.list_spaces(
            vearch_utils.router_url, vearch_utils.db_name
        )
        logger.info(response.json())


class TestClusterPartitionServerCheckSpace:
    def test_check_space(self):
        response = vearch_utils.list_spaces(
            vearch_utils.router_url, vearch_utils.db_name
        )
        logger.info(response.json())
        for space in response.json()["data"]:
            assert space["doc_num"] == 4000


class TestClusterPartitionServerDestroy:
    def test_destroy_db(self):
        leader_status = waitfor_leader_status()
        logger.info(leader_status)

        response = vearch_utils.list_spaces(
            vearch_utils.router_url, vearch_utils.db_name
        )
        while response == None:
            time.sleep(10)
            response = vearch_utils.list_spaces(
                vearch_utils.router_url, vearch_utils.db_name
            )

        logger.info(response.text)
        assert response.json()["code"] == 0

        while len(response.json()["data"]) == 0:
            time.sleep(10)
            response = vearch_utils.list_spaces(
                vearch_utils.router_url, vearch_utils.db_name
            )
        for space in response.json()["data"]:
            response = vearch_utils.drop_space(
                vearch_utils.router_url, vearch_utils.db_name, space["space_name"]
            )
            assert response.json()["code"] == 0
        vearch_utils.drop_db(vearch_utils.router_url, vearch_utils.db_name)


class TestClusterPartitionChange:
    def setup_class(self):
        pass

    def test_prepare_db(self):
        response = vearch_utils.create_db(vearch_utils.router_url, vearch_utils.db_name)
        logger.info(response.json())

    @pytest.mark.parametrize(
        ["embedding_size", "index_type"],
        [
            [128, "FLAT"],
        ],
    )
    def test_vearch_space_create(self, embedding_size, index_type):
        space_config = {
            "name": vearch_utils.space_name,
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

        response = vearch_utils.create_space(
            vearch_utils.router_url, vearch_utils.db_name, space_config
        )
        logger.info(response.json())

        vearch_utils.add_embedding_size(
            vearch_utils.db_name, vearch_utils.space_name, 50, 100, embedding_size
        )

        vearch_utils.delete_interface(
            10,
            100,
            delete_type="by_ids",
            delete_db_name=vearch_utils.db_name,
            delete_space_name=vearch_utils.space_name,
        )

    def test_check_space(self):
        response = vearch_utils.list_spaces(
            vearch_utils.router_url, vearch_utils.db_name
        )
        logger.info(response.json())
        for space in response.json()["data"]:
            assert space["doc_num"] == 4000

    def test_change_partition(self):
        pids = []
        nodes = [1, 2, 3]
        response = vearch_utils.get_space(
            vearch_utils.router_url,
            vearch_utils.db_name,
            vearch_utils.space_name,
            detail=True,
        )
        logger.info(response.json())
        target_node = 0
        nums = {}
        for partition in response.json()["data"]["partitions"]:
            nums[partition["pid"]] = partition["doc_num"]
            pids.append(partition["pid"])
            nodes.remove(partition["node_id"])
        target_node = nodes[0]
        # add partitons
        logger.info(pids)
        logger.info(target_node)
        response = vearch_utils.change_partitons(
            vearch_utils.router_url, pids, target_node, 0
        )
        logger.info(response.json())
        assert response.json()["code"] == 0
        response = vearch_utils.get_servers_status(vearch_utils.router_url)
        logger.info(response.json())

        time.sleep(30)

        response = vearch_utils.get_servers_status(vearch_utils.router_url)
        for server in response.json()["data"]["servers"]:
            if server["server"]["name"] == target_node:
                for partition in server["partitions"]:
                    if partition["pid"] in nums:
                        assert nums[partition["pid"]] == partition["doc_num"]
                break

        # delete partitions
        response = vearch_utils.change_partitons(
            vearch_utils.router_url, pids, target_node, 1
        )
        logger.info(response.json())
        assert response.json()["code"] == 0
        response = vearch_utils.get_servers_status(vearch_utils.router_url)
        logger.info(response.json())

    def test_destroy_db(self):
        response = vearch_utils.list_spaces(
            vearch_utils.router_url, vearch_utils.db_name
        )
        assert response.json()["code"] == 0
        for space in response.json()["data"]:
            response = vearch_utils.drop_space(
                vearch_utils.router_url, vearch_utils.db_name, space["space_name"]
            )
            assert response.json()["code"] == 0
        vearch_utils.drop_db(vearch_utils.router_url, vearch_utils.db_name)


def create_space(partition_num, replica_num, embedding_size, index_type):
    space_config = {
        "name": vearch_utils.space_name,
        "partition_num": partition_num,
        "replica_num": replica_num,
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

    response = vearch_utils.create_space(
        vearch_utils.router_url, vearch_utils.db_name, space_config
    )
    logger.info(response.json())
    return response


class TestClusterFaultyPartitionServerCreateSpace:
    def setup_class(self):
        pass

    def test_prepare_db(self):
        response = vearch_utils.create_db(vearch_utils.router_url, vearch_utils.db_name)
        logger.info(response.json())

    @pytest.mark.parametrize(
        ["embedding_size", "index_type"],
        [
            [128, "FLAT"],
        ],
    )
    def test_prepare_space(self, embedding_size, index_type):
        create_space(3, 3, embedding_size, index_type)


class TestClusterFaultyPartitionServerPrepareData:
    def setup_class(self):
        pass

    @pytest.mark.parametrize(
        ["embedding_size", "index_type"],
        [
            [128, "FLAT"],
        ],
    )
    def test_prepare_data(self, embedding_size, index_type):
        vearch_utils.add_embedding_size(
            vearch_utils.db_name, vearch_utils.space_name, 50, 100, embedding_size
        )


class TestClusterFaultyPartitionServerSearch:
    def setup_class(self):
        pass

    def test_vearch_search(self):
        sift10k = DatasetSift10K()
        xb = sift10k.get_database()
        vearch_utils.search_interface(10, 100, xb, True)


class TestClusterFaultyPartitionServerGetMetaData:
    def test_list_space(self):
        response = vearch_utils.list_spaces(
            vearch_utils.router_url, vearch_utils.db_name
        )
        while response == None:
            time.sleep(10)
            response = vearch_utils.list_spaces(
                vearch_utils.router_url, vearch_utils.db_name
            )

        logger.info(response.text)
        assert response.json()["code"] == 0

        while len(response.json()["data"]) == 0:
            time.sleep(10)
            response = vearch_utils.list_spaces(
                vearch_utils.router_url, vearch_utils.db_name
            )
        for space in response.json()["data"]:
            response = vearch_utils.get_space(
                vearch_utils.router_url, vearch_utils.db_name, space["space_name"]
            )
            assert response.json()["code"] == 0

    def test_vearch_usage_read_metadata(self):
        response = vearch_utils.get_router_info(vearch_utils.router_url)
        logger.debug("router_info:" + json.dumps(response.json()))
        assert response.json()["code"] == 0

        response = vearch_utils.get_cluster_stats(vearch_utils.router_url)
        logger.debug("cluster_stats:" + json.dumps(response.json()))
        assert response.json()["code"] == 0

        response = vearch_utils.get_cluster_version(vearch_utils.router_url)
        logger.debug("cluster_stats:" + json.dumps(response.json()))
        assert response.json()["code"] == 0

        response = vearch_utils.get_cluster_health(vearch_utils.router_url)
        logger.debug("cluster_health---\n" + json.dumps(response.json()))
        assert response.json()["code"] == 0

        response = vearch_utils.get_servers_status(vearch_utils.router_url)
        logger.debug("list_server---\n" + json.dumps(response.json()))
        assert response.json()["code"] == 0

        response = vearch_utils.get_space(
            vearch_utils.router_url, vearch_utils.db_name, vearch_utils.space_name
        )
        logger.info("get_space---\n" + json.dumps(response.json()))
        assert response.json()["code"] == 0

        response = vearch_utils.get_cluster_partition(vearch_utils.router_url)
        logger.debug("get_space---\n" + json.dumps(response.json()))
        assert response.json()["code"] == 0

        response = vearch_utils.list_dbs(vearch_utils.router_url)
        logger.debug("list_db---\n" + json.dumps(response.json()))
        assert response.json()["code"] == 0

        response = vearch_utils.get_db(vearch_utils.router_url, vearch_utils.db_name)
        logger.debug("db_search---\n" + json.dumps(response.json()))
        assert response.json()["code"] == 0

        response = vearch_utils.list_spaces(
            vearch_utils.router_url, vearch_utils.db_name
        )
        logger.debug("list_space---\n" + json.dumps(response.json()))
        assert response.json()["code"] == 0


class TestIncompleteShardPrepare:
    def setup_class(self):
        pass

    def test_prepare_db(self):
        response = vearch_utils.create_db(vearch_utils.router_url, vearch_utils.db_name)
        logger.info(response.json())

    @pytest.mark.parametrize(
        ["embedding_size", "index_type"],
        [
            [128, "FLAT"],
        ],
    )
    def test_prepare_data(self, embedding_size, index_type):
        create_space(3, 1, embedding_size, index_type)
        vearch_utils.add_embedding_size(
            vearch_utils.db_name, vearch_utils.space_name, 50, 100, embedding_size
        )


class TestIncompleteShardSearch:
    def setup_class(self):
        pass

    def test_vearch_search(self):
        sift10k = DatasetSift10K()
        xb = sift10k.get_database()
        vearch_utils.search_interface(10, 100, xb, True)

    def test_search(self):
        url = vearch_utils.router_url + "/document/search"
        data = {}
        data["db_name"] = vearch_utils.db_name
        data["space_name"] = vearch_utils.space_name
        data["vector_value"] = False
        data["vectors"] = []
        sift10k = DatasetSift10K()
        xb = sift10k.get_database()

        for i in range(100):
            data["load_balance"] = random.choice(
                ["leader", "random", "not_leader", "least_connection"]
            )
            vector_info = {
                "field": "field_vector",
                "feature": xb[i : i + 1].flatten().tolist(),
            }
            data["vectors"].append(vector_info)
            data["limit"] = 1

            json_str = json.dumps(data)
            rs = requests.post(
                url, auth=(vearch_utils.username, vearch_utils.password), data=json_str
            )
            if data["load_balance"] == "not_leader":
                rs.json()["code"] == 703
                continue
            if rs.status_code != 200 or "documents" not in rs.json()["data"]:
                logger.info(rs.json())
                logger.info(json_str)
                assert False

            documents = rs.json()["data"]["documents"]
            if data["load_balance"] in ["leader"]:
                assert len(documents) >= 0
            else:
                assert len(documents) > 0

    def test_query(self):
        url = vearch_utils.router_url + "/document/query"
        data = {}
        data["db_name"] = vearch_utils.db_name
        data["space_name"] = vearch_utils.space_name
        data["vector_value"] = False

        for i in range(100):
            data["load_balance"] = random.choice(
                ["leader", "random", "not_leader", "least_connection"]
            )
            data["document_ids"] = [str(i)]
            data["limit"] = 1

            json_str = json.dumps(data)
            rs = requests.post(
                url, auth=(vearch_utils.username, vearch_utils.password), data=json_str
            )

            if data["load_balance"] == "not_leader":
                rs.json()["code"] == 703
                continue
            if rs.status_code != 200 or "documents" not in rs.json()["data"]:
                logger.info(rs.json())
                logger.info(json_str)
                assert False

            documents = rs.json()["data"]["documents"]
            assert len(documents) >= 0


class TestFailServerPrepare:
    def setup_class(self):
        pass

    def test_prepare_db(self):
        response = vearch_utils.create_db(vearch_utils.router_url, vearch_utils.db_name)
        logger.info(response.json())

    @pytest.mark.parametrize(
        ["embedding_size", "index_type"],
        [
            [128, "FLAT"],
        ],
    )
    def test_prepare_data(self, embedding_size, index_type):
        create_space(1, 3, embedding_size, index_type)
        vearch_utils.add_embedding_size(
            vearch_utils.db_name, vearch_utils.space_name, 50, 100, embedding_size
        )


class TestAntiAffinity:
    def setup_class(self):
        pass

    def test_prepare_db(self):
        response = vearch_utils.create_db(vearch_utils.router_url, vearch_utils.db_name)
        logger.info(response.json())

    @pytest.mark.parametrize(
        ["embedding_size", "index_type"],
        [
            [128, "FLAT"],
        ],
    )
    def test_prepare_data(self, embedding_size, index_type):
        create_space(1, 4, embedding_size, index_type)


class TestFailAntiAffinity:
    def setup_class(self):
        pass

    def test_prepare_db(self):
        response = vearch_utils.create_db(vearch_utils.router_url, vearch_utils.db_name)
        logger.info(response.json())

    @pytest.mark.parametrize(
        ["embedding_size", "index_type"],
        [
            [128, "FLAT"],
        ],
    )
    def test_prepare_data(self, embedding_size, index_type):
        response = create_space(1, 5, embedding_size, index_type)
        assert response.json()["code"] != 0
        vearch_utils.drop_db(vearch_utils.router_url, vearch_utils.db_name)


class TestFailServerUpsertPrepare:
    def setup_class(self):
        self.embedding_size = 128

    def test_prepare_db(self):
        response = vearch_utils.create_db(vearch_utils.router_url, vearch_utils.db_name)
        logger.info(response.json())

        create_space(1, 3, self.embedding_size, "FLAT")


class TestFailServerUpsertDocument:
    def setup_class(self):
        self.embedding_size = 128

    def test_fail_upsert(self):
        # upsert
        vearch_utils.add_embedding_size(
            vearch_utils.db_name, vearch_utils.space_name, 10, 100, self.embedding_size
        )

        # update
        for i in range(10):
            vearch_utils.add_embedding_size(
                vearch_utils.db_name, vearch_utils.space_name, 10, 100, 0
            )


class TestFailServerUpsertDestroy:
    def setup_class(self):
        pass

    def test_destroy_db(self):
        response = vearch_utils.list_spaces(
            vearch_utils.router_url, vearch_utils.db_name
        )
        assert response.json()["code"] == 0
        for space in response.json()["data"]:
            response = vearch_utils.drop_space(
                vearch_utils.router_url, vearch_utils.db_name, space["space_name"]
            )
            assert response.json()["code"] == 0
        vearch_utils.drop_db(vearch_utils.router_url, vearch_utils.db_name)
