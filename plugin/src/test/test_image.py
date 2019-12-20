# Copyright 2019 The Vearch Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# ==============================================================================

# -*- coding: UTF-8 -*-

import logging
import os

import pytest
import requests
import json
from concurrent.futures import ThreadPoolExecutor, wait

logging.basicConfig()
logger = logging.getLogger(__name__)

ip_db = "127.0.0.1:4101"
ip_data = "127.0.0.1:4101"
db_name = "test_vector_db"
space_name = "vector_space"
fileData = "data/test_data.json"
headers = {"content-type": "application/json"}
plugin_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
image_path = os.path.join(plugin_path, 'images', 'image_retrieval', 'test')


@pytest.mark.author('')
@pytest.mark.level(2)
@pytest.mark.cover(["VDB"])
def test_stats():
    logger.info("_cluster_information")
    url = "http://" + ip_db + "/_cluster/stats"
    response = requests.get(url)
    print("cluster_stats:" + response.text)
    assert response.status_code == 200


def test_health():
    url = "http://" + ip_db + "/_cluster/health"
    response = requests.get(url)
    print("cluster_health---\n" + response.text)
    assert response.status_code == 200


def test_server():
    url = "http://" + ip_db + "/list/server"
    response = requests.get(url)
    print("list_server---\n" + response.text)
    assert response.status_code == 200


def test_db():
    url = "http://" + ip_db + "/list/db"
    response = requests.get(url)
    print("list_db---\n" + response.text)
    assert response.status_code == 200


def test_createDB():
    logger.info("------------")
    url = "http://" + ip_db + "/db/_create"
    data = {
        'name': db_name
    }
    response = requests.put(url, headers=headers, data=json.dumps(data))
    print("db_create---\n" + response.text)
    assert response.status_code == 200


def test_dbsearch():
    url = "http://" + ip_db + "/db/" + db_name
    response = requests.get(url)
    print("db_search---\n" + response.text)
    assert response.status_code == 200


def test_dbspace():
    url = "http://" + ip_db + "/list/space?db=" + db_name
    response = requests.get(url)
    print("space_search---\n" + response.text)
    assert response.status_code == 200


def test_createspace():
    url = "http://" + ip_db + "/space/" + db_name + "/_create"
    headers = {"content-type": "application/json"}
    data = {
        "name": space_name,
        "dynamic_schema": "strict",
        "partition_num": 1,  # "partition_num": 2-6之间
        "replica_num": 1,
        "engine": {"name": "gamma", "index_size": 9999, "max_size": 100000},
        "properties": {
            "string": {
                "type": "keyword",
                "index": "true"
            },
            "int": {
                "type": "integer",
                "index": "true"
            },
            "float": {
                "type": "float",
                "index": "true"
            },
            "vector1": {
                "type": "vector",
                "model_id": "img",
                "dimension": 512,
                "format": "normalization"
            },
            "vector2": {
                "type": "vector",
                "model_id": "text",
                "dimension": 768,
                "format": "normalization"
            },
            "string_tags": {
                "type": "string",
                "array": True,
                "index": "true"
            },
            "int_tags": {
                "type": "integer",
                "array": True,
                "index": "true"
            },
            "float_tags": {
                "type": "float",
                "array": True,
                "index": "true"
            }
        },
        "models": [{
            "model_id": "vgg16",
            "fields": ["string"],
            "out": "feature"
        }]
    }
    print(url + "---" + json.dumps(data))
    response = requests.put(url, headers=headers, data=json.dumps(data))
    print("space_create---\n" + response.text)
    assert response.status_code == 200


def test_space():
    url = "http://" + ip_db + "/space/" + db_name + "/" + space_name
    response = requests.get(url)
    print("space---\n" + response.text)
    assert response.status_code == 200


logger.info("router(PS)")


def test_insertWithId():
    logger.info("insert")

    def multi(filename):
        file_path = os.path.join(image_path, filename)
        data = dict(string=file_path, vector1=dict(feature=file_path))
        idx = os.path.splitext(filename)[0]
        url = "http://" + ip_data + "/" + db_name + "/" + space_name + "/" + idx
        response = requests.post(url, headers=headers, data=json.dumps(data))
        print("insertWithID:" + response.text)
        assert response.status_code == 200 and response.json()['status'] == 200
    with ThreadPoolExecutor(20) as pool:
        futures = [pool.submit(multi, filename) for filename in os.listdir(image_path)]
    wait(futures)


def test_searchById():
    logger.info("test_searchById")
    for filename in os.listdir(image_path):
        idx = os.path.splitext(filename)[0]
        url = "http://" + ip_data + "/" + db_name + "/" + space_name + "/" + idx
        response = requests.get(url)
        print("searchById:" + response.text)
        assert response.status_code == 200 and response.json()['found'] is True


def test_insterNoId():
    logger.info("insertDataNoId")

    def multi(filename):
        file_path = os.path.join(image_path, filename)
        data = dict(string=file_path, vector1=dict(feature=file_path))
        url = "http://" + ip_data + "/" + db_name + "/" + space_name
        response = requests.post(url, headers=headers, data=json.dumps(data))
        print("insertWithNOID:" + response.text)
        assert response.status_code == 200 and response.json()['status'] == 201
    with ThreadPoolExecutor(20) as pool:
        futures = [pool.submit(multi, filename) for filename in os.listdir(image_path)]
    wait(futures)


def test_searchByFeature():
    url = "http://" + ip_data + "/" + db_name + "/" + space_name + "/_search?size=100"
    for filename in os.listdir(image_path):
        file_path = os.path.join(image_path, filename)
        data = {
            "query": {
                "sum": [{
                    "field": "vector1",
                    "feature": file_path,
                    "format": "normalization"
                }]
            }
        }
        response = requests.post(url, headers=headers, data=json.dumps(data))
        print("searchByFeature---\n" + response.text)
        assert response.status_code == 200


def test_deleteDoc():
    logger.info("test_deleteDoc")
    # fileData = "/home/vearch/test/data/test_data.json"
    for filename in os.listdir(image_path):
        idx = os.path.splitext(filename)[0]
        url = "http://" + ip_data + "/" + db_name + "/" + space_name + "/" + idx
        response = requests.delete(url)
        print("deleteDoc:" + response.text)
        assert response.status_code == 200


def test_deleteSpace():
    url = "http://" + ip_db + "/space/" + db_name + "/" + space_name
    response = requests.delete(url)
    print("deleteSpace:" + response.text)
    assert response.status_code == 200


def test_deleteDB():
    url = "http://" + ip_db + "/db/" + db_name
    response = requests.delete(url)
    print("deleteDB:" + response.text)
    assert response.status_code == 200


