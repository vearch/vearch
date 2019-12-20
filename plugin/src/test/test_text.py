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
headers = {"content-type": "application/json"}
TEXTS = ['君不见，黄河之水天上来，奔流到海不复回。',
         '君不见，高堂明镜悲白发，朝如青丝暮成雪。',
         '人生得意须尽欢，莫使金樽空对月。',
         '天生我材必有用，千金散尽还复来。',
         '烹羊宰牛且为乐，会须一饮三百杯。',
         '岑夫子，丹丘生，将进酒，杯莫停。',
         '与君歌一曲，请君为我倾耳听。',
         '钟鼓馔玉不足贵，但愿长醉不复醒。',
         '古来圣贤皆寂寞，惟有饮者留其名。',
         '陈王昔时宴平乐，斗酒十千恣欢谑。',
         '主人何为言少钱，径须沽取对君酌。',
         '五花马，千金裘，呼儿将出换美酒，与尔同销万古愁。']


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

    def multi(text):
        data = dict(string=text, vector2=dict(feature=text))
        idx = str(abs(hash(text)))
        url = "http://" + ip_data + "/" + db_name + "/" + space_name + "/" + idx
        response = requests.post(url, headers=headers, data=json.dumps(data))
        print("insertWithID:" + response.text)
        assert response.status_code == 200 and response.json()['status'] == 200

    with ThreadPoolExecutor(20) as pool:
        futures = [pool.submit(multi, text) for text in TEXTS]
    wait(futures)


def test_searchById():
    logger.info("test_searchById")
    for text in TEXTS:
        idx = str(abs(hash(text)))
        url = "http://" + ip_data + "/" + db_name + "/" + space_name + "/" + idx
        response = requests.get(url)
        print("searchById:" + response.text)
        assert response.status_code == 200 and response.json()['found'] is True


def test_insterNoId():
    logger.info("insertDataNoId")

    def multi(text):
        data = dict(string=text, vector2=dict(feature=text))
        url = "http://" + ip_data + "/" + db_name + "/" + space_name
        response = requests.post(url, headers=headers, data=json.dumps(data))
        print("insertWithNOID:" + response.text)
        assert response.status_code == 200 and response.json()['status'] == 201

    with ThreadPoolExecutor(20) as pool:
        futures = [pool.submit(multi, text) for text in TEXTS]
    wait(futures)


def test_searchByFeature():
    url = "http://" + ip_data + "/" + db_name + "/" + space_name + "/_search?size=100"
    for text in TEXTS:
        data = {
            "query": {
                "sum": [{
                    "field": "vector2",
                    "feature": text,
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
    for text in TEXTS:
        idx = str(abs(hash(text)))
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
