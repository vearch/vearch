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
from multiprocessing import Pool as ThreadPool
from utils.vearch_utils import *
from utils.data_utils import *

__description__ = """ test case for module role """


class TestRole:
    def setup_class(self):
        pass

    def test_create_role(self):
        response = create_role(router_url, "role_name", {"ResourceDocument": "ReadOnly"})
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_get_role(self):
        response = get_role(router_url, "role_name")
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_revoke_role_privilege(self):
        response = change_role_privilege(
            router_url, "role_name", "Revoke", {"ResourceDocument": "ReadOnly"})
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = get_role(router_url, "role_name")
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_grant_role_privilege(self):
        response = change_role_privilege(
            router_url, "role_name", "Grant", {"ResourceDocument": "ReadOnly"})
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = get_role(router_url, "role_name")
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_drop_role(self):
        response = drop_role(router_url, "role_name")
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_roles(self):
        response = create_role(router_url, "role_name1", {"ResourceDocument": "ReadOnly"})
        assert response.json()["code"] == 0

        response = create_role(router_url, "role_name2", {"ResourceDocument": "ReadOnly"})
        assert response.json()["code"] == 0

        response = get_all_roles(router_url)
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = drop_role(router_url, "role_name1")
        assert response.json()["code"] == 0

        response = drop_role(router_url, "role_name2")
        assert response.json()["code"] == 0

        response = get_all_roles(router_url)
        logger.info(response.json())
        assert response.json()["code"] == 0

    @pytest.mark.parametrize(
        ["wrong_index", "wrong_type"],
        [
            [0, "get role not exits"],
            [1, "delete role not exits"],
            [2, "create role exits"],
            [3, "update role not exits"],
            [4, "bad operator type"],
            [5, "bad privilege"],
            [6, "create root role"],
        ],
    )
    def test_user_badcase(self, wrong_index, wrong_type):
        if wrong_index == 0:
            response = get_role(
                router_url, "role_not_exist")
            logger.info(response.json())
            assert response.json()["code"] != 0

        if wrong_index == 1:
            response = drop_role(
                router_url, "role_not_exist")
            logger.info(response.json())
            assert response.json()["code"] != 0

        if wrong_index == 2:
            response = create_role(
                router_url, "role_exist", {})
            assert response.json()["code"] == 0
            response = create_role(
                router_url, "role_exist", {})
            logger.info(response.json())
            assert response.json()["code"] != 0
            response = drop_role(router_url, "role_exist")
            assert response.json()["code"] == 0

        if wrong_index == 3:
            response = change_role_privilege(
                router_url, "role_not_exist", "Grant", {"ResourceDocument": "ReadOnly"})
            logger.info(response.json())
            assert response.json()["code"] != 0

        if wrong_index == 4:
            response = change_role_privilege(
                router_url, "role_not_exist", "Bad Type", {"ResourceDocument": "ReadOnly"})
            logger.info(response.json())
            assert response.json()["code"] != 0

        if wrong_index == 5:
            response = change_role_privilege(
                router_url, "role_not_exist", "Grant", {"ResourceDocument": "Update"})
            logger.info(response.json())
            assert response.json()["code"] != 0

        if wrong_index == 6:
            response = create_role(
                router_url, "root", {})
            logger.info(response.json())
            assert response.json()["code"] != 0

    def process_role(self, operation):
        if operation == "create":
            response = create_role(
                router_url, "role_name_mul", {"ResourceDocument": "ReadOnly"})
            logger.info(response.json())
        if operation == "delete":
            response = drop_role(router_url, "role_name_mul")
            logger.info(response.json())
        if operation == "update":
            response = change_role_privilege(
                router_url, "role_name_mul", "Grant", {"ResourceDocument": "ReadOnly"})
            logger.info(response.json())

    def test_multithread(self):
        pool = ThreadPool()
        total_data = ["create", "create", "create", "delete",
                      "delete", "delete", "update", "update", "update"]
        results = pool.map(self.process_role, total_data)
        pool.close()
        pool.join()
        response = drop_role(router_url, "role_name_mul")
        logger.info(response.json()["code"])


class TestRolePrivilege:
    def setup_class(self):
        pass

    def test_create_role(self):
        response = create_role(router_url, "role_name", {"ResourceCluster": "ReadOnly"})
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = get_role(router_url, "role_name")
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_create_user(self):
        response = create_user(
            router_url, "user_name", "password", "role_name"
        )
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = get_user(router_url, "user_name")
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_cluster_privileges(self):
        response = requests.get(
            router_url + "/cluster/health", auth=("user_name", "password")
        )
        logger.info(response.text)
        assert response.json()["code"] == 0

        response = requests.get(
            router_url + "/cluster/stats", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = change_role_privilege(
            router_url, "role_name", "Revoke", {"ResourceCluster": "ReadOnly"})
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = get_role(router_url, "role_name")
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = get_cache_role(router_url, "role_name")
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = requests.get(
            router_url + "/cluster/health", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] == 3

        response = requests.get(
            router_url + "/cluster/stats", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] == 3

    def test_server_privileges(self):
        response = requests.get(
            router_url + "/servers", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] == 3

        response = change_role_privilege(
            router_url, "role_name", "Grant", {"ResourceServer": "ReadOnly"})
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = requests.get(
            router_url + "/servers", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = change_role_privilege(
            router_url, "role_name", "Revoke", {"ResourceServer": "ReadOnly"})
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = requests.get(
            router_url + "/servers", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] == 3

    def test_partition_privileges(self):
        response = requests.get(
            router_url + "/partitions", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] == 3

        response = change_role_privilege(
            router_url, "role_name", "Grant", {"ResourcePartition": "WriteRead"})
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = requests.get(
            router_url + "/partitions", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] == 0
        data = {}
        response = requests.post(
            router_url + "/partitions/resource_limit", json = data, auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] == 1

        response = change_role_privilege(
            router_url, "role_name", "Revoke", {"ResourcePartition": "ReadOnly"})
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = requests.get(
            router_url + "/partitions", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] == 3

        response = requests.post(
            router_url + "/partitions/resource_limit",json = data, auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] == 3

    def test_space_privileges(self):
        response = requests.get(
            router_url + "/dbs/ts_role/spaces", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] == 3

        response = requests.delete(
            router_url + "/dbs/ts_role/spaces/ts_role", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] == 3

        response = change_role_privilege(
            router_url, "role_name", "Grant", {"ResourceSpace": "ReadOnly"})
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = change_role_privilege(
            router_url, "role_name", "Grant", {"ResourceSpace": "WriteOnly"})
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = requests.get(
            router_url + "/dbs/ts_role/spaces", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] == 200

        response = requests.delete(
            router_url + "/dbs/ts_role/spaces/ts_role", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] == 200

        response = change_role_privilege(
            router_url, "role_name", "Revoke", {"ResourceSpace": "ReadOnly"})
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = change_role_privilege(
            router_url, "role_name", "Revoke", {"ResourceSpace": "WriteOnly"})
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = requests.get(
            router_url + "/dbs/ts_role/spaces", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] == 3

        response = requests.delete(
            router_url + "/dbs/ts_role/spaces/ts_role", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] == 3

    def test_db_privileges(self):
        response = requests.get(
            router_url + "/dbs", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] == 3

        response = requests.post(
            router_url + "/dbs/ts_role", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] == 3

        response = change_role_privilege(
            router_url, "role_name", "Grant", {"ResourceDB": "ReadOnly"})
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = change_role_privilege(
            router_url, "role_name", "Grant", {"ResourceDB": "WriteOnly"})
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = requests.get(
            router_url + "/dbs", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = requests.post(
            router_url + "/dbs/ts_role", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = requests.delete(
            router_url + "/dbs/ts_role", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = change_role_privilege(
            router_url, "role_name", "Revoke", {"ResourceDB": "ReadOnly"})
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = change_role_privilege(
            router_url, "role_name", "Revoke", {"ResourceDB": "WriteOnly"})
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = requests.get(
            router_url + "/dbs", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] == 3

        response = requests.post(
            router_url + "/dbs/ts_role", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] == 3

        response = requests.delete(
            router_url + "/dbs/ts_role", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] == 3

    def test_document_privileges(self):
        data = {}
        response = requests.post(
            router_url + "/document/delete", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] == 3

        response = requests.post(
            router_url + "/document/query", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] == 3

        response = change_role_privilege(
            router_url, "role_name", "Grant", {"ResourceDocument": "ReadOnly"})
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = change_role_privilege(
            router_url, "role_name", "Grant", {"ResourceDocument": "WriteOnly"})
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = requests.post(
            router_url + "/document/delete", json = data, auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] != 0

        response = requests.post(
            router_url + "/document/query", json = data,auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] != 0

        response = change_role_privilege(
            router_url, "role_name", "Revoke", {"ResourceDocument": "ReadOnly"})
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = change_role_privilege(
            router_url, "role_name", "Revoke", {"ResourceDocument": "WriteOnly"})
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = requests.post(
            router_url + "/document/delete", json = data,auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] == 3

        response = requests.post(
            router_url + "/document/query", json = data,auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] == 3

    def test_drop_role(self):
        response = drop_role(router_url, "role_name")
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_drop_user(self):
        response = drop_user(router_url, "user_name")
        logger.info(response.json())
        assert response.json()["code"] == 0