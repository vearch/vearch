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

__description__ = """ test case for module database """


class TestDB:
    def setup_class(self):
        pass

    def test_create_db(self):
        response = create_db(router_url, db_name)
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_get_db(self):
        response = get_db(router_url, db_name)
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_destroy_db(self):
        response = drop_db(router_url, db_name)
        assert response.json()["code"] == 0

    def test_create_dbs(self):
        response = create_db(router_url, db_name + "1")
        logger.info(response.json())
        assert response.json()["code"] == 0
        response = create_db(router_url, db_name + "2")
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_get_dbs(self):
        response = list_dbs(router_url)
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_destroy_dbs(self):
        response = drop_db(router_url, db_name + "1")
        assert response.json()["code"] == 0
        response = drop_db(router_url, db_name + "2")
        assert response.json()["code"] == 0

    @pytest.mark.parametrize(
        ["wrong_index", "wrong_type"],
        [
            [0, "bad db name"],
        ],
    )
    def test_get_db_badcase(self, wrong_index, wrong_type):
        db_param = db_name
        if wrong_index == 0:
            db_param = "wrong_db"
        response = get_db(router_url, db_param)
        logger.info(response.json())
        assert response.json()["code"] != 0
