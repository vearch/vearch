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

__description__ = """ test case for module user """


class TestUser:
    def setup_class(self):
        pass

    def test_create_user(self):
        response = create_user(router_url, "user_name", "password", "defaultSpaceAdmin")
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_get_user(self):
        response = get_user(router_url, "user_name")
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_update_user_role(self):
        response = update_user(router_url, "user_name", role_name="defaultSpaceAdmin")
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = get_user(router_url, "user_name")
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = update_user(
            router_url, "user_name", role_name="defaultClusterAdmin"
        )
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = get_user(router_url, "user_name")
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_update_user_password_with_root(self):
        # root can update without checking old password
        response = update_user(router_url, "user_name", new_password="password_new")
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_update_user_password(self):
        url = f"{router_url}/users"
        data = {"name": "user_name"}
        new_password="password_new2"
        data["password"] = new_password
        data["old_password"] = "password_new"

        response = requests.put(url, json=data, auth=("user_name", "password_new"))
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_update_root_password(self):
        url = f"{router_url}/users"
        data = {"name": "root"}
        new_password="password_new"
        global password
        data["password"] = new_password
        data["old_password"] = password

        response = requests.put(url, json=data, auth=(username, password))
        logger.info(response.json())
        assert response.json()["code"] == 0
        set_password(new_password)

    def test_drop_user(self):
        response = drop_user(router_url, "user_name")
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_users(self):
        response = create_user(
            router_url, "user_name1", "password", "defaultSpaceAdmin"
        )
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = create_user(
            router_url, "user_name2", "password", "defaultSpaceAdmin"
        )
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = get_all_users(router_url)
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = drop_user(router_url, "user_name1")
        assert response.json()["code"] == 0

        response = drop_user(router_url, "user_name2")
        assert response.json()["code"] == 0

        response = get_all_users(router_url)
        logger.info(response.json())
        assert response.json()["code"] == 0

    @pytest.mark.parametrize(
        ["wrong_index", "wrong_type"],
        [
            [0, "get user not exits"],
            [1, "delete user not exits"],
            [2, "create user exits"],
            [3, "update password for user not exits"],
            [4, "create user with root role"],
            [5, "update user with role not exits"],
            [6, "delete root user"],
            [7, "create user without password"],
            [8, "create user without role name"],
            [9, "update user password without password"],
            [10, "update user password without old password"],
            [11, "update user password with same password"],
            [12, "update user withboth password and role name"],
            [13, "create root user"],
        ],
    )
    def test_user_badcase(self, wrong_index, wrong_type):
        if wrong_index == 0:
            response = get_user(router_url, "user_not_exist")
            logger.info(response.json())
            assert response.json()["code"] != 0

        if wrong_index == 1:
            response = drop_user(router_url, "user_not_exist")
            logger.info(response.json())
            assert response.json()["code"] != 0

        if wrong_index == 2:
            response = create_user(
                router_url, "user_exist", "password", "defaultSpaceAdmin"
            )
            assert response.json()["code"] == 0
            response = create_user(
                router_url, "user_exist", "password", "defaultSpaceAdmin"
            )
            logger.info(response.json())
            assert response.json()["code"] != 0
            response = drop_user(router_url, "user_exist")
            assert response.json()["code"] == 0

        if wrong_index == 3:
            response = update_user(
                router_url, "user_not_exist", new_password="password_new", old_password="password"
            )
            logger.info(response.json())
            assert response.json()["code"] != 0

        if wrong_index == 4:
            response = create_user(router_url, "user_exist", "password", "root")
            logger.info(response.json())
            assert response.json()["code"] != 0

        if wrong_index == 5:
            response = create_user(
                router_url, "user_exist", "password", "defaultSpaceAdmin"
            )
            assert response.json()["code"] == 0

            response = update_user(router_url, "user_exist", role_name="role_not_exist")
            logger.info(response.json())
            assert response.json()["code"] != 0
            response = drop_user(router_url, "user_exist")
            assert response.json()["code"] == 0

        if wrong_index == 6:
            # check case none-sensitive
            response = drop_user(router_url, "RoOt")
            assert response.json()["code"] != 0

        if wrong_index == 7:
            response = create_user(
                router_url, "user_exist", role_name="defaultSpaceAdmin"
            )
            assert response.json()["code"] != 0

        if wrong_index == 8:
            response = create_user(router_url, "user_exist", user_password="password")
            assert response.json()["code"] != 0

        if wrong_index == 9:
            response = create_user(
                router_url, "user_exist", "password", "defaultClusterAdmin"
            )
            assert response.json()["code"] == 0

            response = update_user(router_url, "user_exist", new_password="password_new", auth_user="user_exist", auth_password="password")
            logger.info(response.json())
            assert response.json()["code"] != 0
            response = drop_user(router_url, "user_exist")
            assert response.json()["code"] == 0

        if wrong_index == 10:
            response = create_user(
                router_url, "user_exist", "password", "defaultClusterAdmin"
            )
            assert response.json()["code"] == 0

            response = update_user(router_url, "user_exist", old_password="password")
            logger.info(response.json())
            assert response.json()["code"] != 0
            response = drop_user(router_url, "user_exist")
            assert response.json()["code"] == 0

        if wrong_index == 11:
            response = create_user(
                router_url, "user_exist", "password", "defaultClusterAdmin"
            )
            assert response.json()["code"] == 0

            response = update_user(router_url, "user_exist", new_password="password", old_password="password")
            logger.info(response.json())
            assert response.json()["code"] != 0
            response = drop_user(router_url, "user_exist")
            assert response.json()["code"] == 0

        if wrong_index == 12:
            response = create_user(
                router_url, "user_exist", "password", "defaultClusterAdmin"
            )
            assert response.json()["code"] == 0

            response = update_user(router_url, "user_exist", new_password="password_new", old_password="password", role_name="defaultDocumentAdmin")
            logger.info(response.json())
            assert response.json()["code"] != 0
            response = drop_user(router_url, "user_exist")
            assert response.json()["code"] == 0

        if wrong_index == 13:
            response = create_user(router_url, "root", "password", "defaultSpaceAdmin")
            logger.info(response.json())
            assert response.json()["code"] != 0

    def process_user(self, operation):
        if operation == "create":
            response = create_user(
                router_url, "user_name_mul", "password", "defaultSpaceAdmin"
            )
            logger.info(response.json())
        if operation == "delete":
            response = drop_user(router_url, "user_name_mul")
            logger.info(response.json())
        if operation == "update":
            response = update_user(
                router_url, "user_name_mul", new_password="password_new", old_password="password"
            )
            logger.info(response.json())

    def test_multithread(self):
        pool = ThreadPool()
        total_data = [
            "create",
            "create",
            "create",
            "delete",
            "delete",
            "delete",
            "update",
            "update",
            "update",
        ]
        results = pool.map(self.process_user, total_data)
        pool.close()
        pool.join()
        response = drop_user(router_url, "user_name_mul")
        logger.info(response.json())


class TestUserDefaultSpaceAdmin:
    def setup_class(self):
        pass

    def test_create_user(self):
        response = create_user(router_url, "user_name", "password", "defaultSpaceAdmin")
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = get_user(router_url, "user_name")
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_privileges(self):
        response = requests.get(
            router_url + "/cluster/health", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] != 0

        response = requests.get(
            router_url + "/servers", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] != 0

        response = requests.get(
            router_url + "/partitions", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] != 0

        response = requests.get(
            router_url + "/dbs", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] != 0

    def test_drop_user(self):
        response = drop_user(router_url, "user_name")
        logger.info(response.json())
        assert response.json()["code"] == 0


class TestUserDefalutDocumentAdmin:
    def setup_class(self):
        pass

    def test_create_user(self):
        response = create_user(
            router_url, "user_name", "password", "defaultDocumentAdmin"
        )
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = get_user(router_url, "user_name")
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_privileges(self):
        response = requests.get(
            router_url + "/cluster/health", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] != 0

        response = requests.get(
            router_url + "/servers", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] != 0

        response = requests.get(
            router_url + "/partitions", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] != 0

        response = requests.get(
            router_url + "/dbs", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] != 0

    def test_drop_user(self):
        response = drop_user(router_url, "user_name")
        logger.info(response.json())
        assert response.json()["code"] == 0

class TestUserDefaultReadDBSpaceEditDocument:
    def setup_class(self):
        pass

    def test_create_user(self):
        response = create_user(
            router_url, "user_name", "password", "defaultReadDBSpaceEditDocument"
        )
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = get_user(router_url, "user_name")
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_privileges(self):
        response = requests.get(
            router_url + "/cluster/health", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = requests.get(
            router_url + "/servers", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] != 0

        response = requests.get(
            router_url + "/partitions", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] != 0

        response = requests.get(
            router_url + "/dbs", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_drop_user(self):
        response = drop_user(router_url, "user_name")
        logger.info(response.json())
        assert response.json()["code"] == 0

class TestUserDefaultReadSpaceEditDocument:
    def setup_class(self):
        pass

    def test_create_user(self):
        response = create_user(
            router_url, "user_name", "password", "defaultReadSpaceEditDocument"
        )
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = get_user(router_url, "user_name")
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_privileges(self):
        response = requests.get(
            router_url + "/cluster/health", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] != 0

        response = requests.get(
            router_url + "/servers", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] != 0

        response = requests.get(
            router_url + "/partitions", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] != 0

        response = requests.get(
            router_url + "/dbs", auth=("user_name", "password")
        )
        logger.info(response.json())
        assert response.json()["code"] != 0

    def test_drop_user(self):
        response = drop_user(router_url, "user_name")
        logger.info(response.json())
        assert response.json()["code"] == 0


class TestResetRoot:
    def test_update_root_password(self):
        url = f"{router_url}/users"
        data = {"name": "root"}
        new_password="secret"
        global password
        data["password"] = new_password
        data["old_password"] = "password_new"

        response = requests.put(url, json=data, auth=(username, "password_new"))
        logger.info(response.json())
        assert response.json()["code"] == 0
        set_password(new_password)