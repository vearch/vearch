# -*- coding: UTF-8 -*-

import logging
import pytest
import requests
import json

logging.basicConfig()
logger = logging.getLogger(__name__)

__author__ = 'wangjiangjuan'
__date__ = '2019-07-22 09:25:00'
__description__ = """ """

ip_db = "127.0.0.1:8817"
ip_data = "127.0.0.1:9001"
db_name = "test_vector_db"
space_name = "vector_space"
fileData = "data/test_data.json"


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
    headers = {"content-type": "application/json"}
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
                "index": True
            },
            "int": {
                "type": "integer",
                "index": True
            },
            "float": {
                "type": "float",
                "index": True
            },
            "vector": {
                "type": "vector",
                "model_id": "img",
                "dimension": 128,
                "format": "normalization"
            },
            "string_tags": {
                "type": "string",
                "array": True,
                "index": True
            },
            "int_tags": {
                "type": "integer",
                "array": True,
                "index": True
            },
            "float_tags": {
                "type": "float",
                "array": True,
                "index": True
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
    headers = {"content-type": "application/json"}
    with open(fileData, "r") as dataLine1:
        for dataLine in dataLine1:
            data = eval(dataLine)
            id = data["_id"]
            del data["_id"]
            url = "http://" + ip_data + "/" + db_name + "/" + space_name + "/" + id
            response = requests.post(url, headers=headers, data=json.dumps(data))
            print("insertWithID:" + response.text)
            assert response.status_code == 200


def test_searchById():
    logger.info("test_searchById")
    # fileData = "/home/vearch/test/data/test_data.json"
    with open(fileData, "r") as dataLine1:
        for dataLine in dataLine1:
            idStr = dataLine.split(',', 1)[0].replace('{', '')
            id = eval(idStr.split(':')[1])
            url = "http://" + ip_data + "/" + db_name + "/" + space_name + "/" + id
            response = requests.get(url)
            print("searchById:" + response.text)
            assert response.status_code == 200


def test_insterNoId():
    logger.info("insertDataNoId")
    headers = {"content-type": "application/json"}
    # fileData = "/home/vearch/test/data/test_data.json"
    with open(fileData, "r") as dataLine1:
        for dataLine in dataLine1:
            idStr = dataLine.split(',', 1)[0].replace('{', '')
            id = eval(idStr.split(':')[1])
            data = "{" + dataLine.split(',', 1)[1]
            url = "http://" + ip_data + "/" + db_name + "/" + space_name
            response = requests.post(url, headers=headers, data=data)
            print("insertNoID:" + response.text)
            assert response.status_code == 200


def test_searchByFeature():
    headers = {"content-type": "application/json"}
    url = "http://" + ip_data + "/" + db_name + "/" + space_name + "/_search?size=100"
    # fileData = "/home/vearch/test/data/test_data.json"
    with open(fileData, "r") as dataLine1:
        for dataLine in dataLine1:
            print(dataLine)
            idStr = dataLine.split(',', 1)[0].replace('{', '')
            id = eval(idStr.split(':')[1])
            feature = "{" + dataLine.split(',', 1)[1]
            print("_id:" + id)
            print("_data:" + feature)
            feature = json.loads(feature)
            feature = feature["vector"]["feature"]
            data = {
                "query": {
                    "sum": [{
                        "field": "vector",
                        "feature": feature,
                        "format": "normalization"
                    }],
                    "vector_value": True
                }
            }
            print(json.dumps(data))
            response = requests.post(url, headers=headers, data=json.dumps(data))
            print("searchByFeature---\n" + response.text)
            assert response.status_code == 200


def test_searchByFeatureandFilter():
    url = "http://" + ip_data + "/" + db_name + "/" + space_name + "/_search"
    headers = {"content-type": "application/json"}
    # fileData = "/home/vearch/test/data/test_data.json"
    with open(fileData, "r") as dataLine1:
        for dataLine in dataLine1:
            idStr = dataLine.split(',', 1)[0].replace('{', '')
            id = eval(idStr.split(':')[1])
            feature = "{" + dataLine.split(',', 1)[1]
            feature = json.loads(feature)
            string_tags = feature["string_tags"]
            feature = feature["vector"]["feature"]
            data = {
                "query": {
                    "filter": [{
                        "string_tags": string_tags
                    }],
                    "sum": [{
                        "field": "vector",
                        "feature": feature,
                        "format": "normalization"
                    }]
                }
            }
            response = requests.post(url, headers=headers, data=json.dumps(data))
            print("searchByFeature---\n" + response.text)
            assert response.status_code == 200


def test_updateDoc():
    logger.info("updateDoc")
    headers = {"content-type": "application/json"}
    # fileData = "/home/vearch/test/data/test_data.json"
    with open(fileData, "r") as dataLine1:
        for dataLine in dataLine1:
            idStr = dataLine.split(',', 1)[0].replace('{', '')
            id = eval(idStr.split(':')[1])
            data = "{" + dataLine.split(',', 1)[1]
            url = "http://" + ip_data + "/" + db_name + "/" + space_name + "/" + id
            response = requests.post(url, headers=headers, data=data)
            print("updateDoc:" + response.text)
            assert response.status_code == 200


def test_insertBulk():
    logger.info("insertBulk")
    url = "http://" + ip_data + "/" + db_name + "/" + space_name + "/_bulk"
    headers = {"content-type": "application/json"}
    # fileData = "/home/vearch/test/data/test_data.json"
    with open(fileData, "r") as dataLine1:
        for dataLine in dataLine1:
            idStr = dataLine.split(',', 1)[0].replace('{', '')
            id = eval(idStr.split(':')[1])
            data = "{" + dataLine.split(',', 1)[1]
            response = requests.post(url, headers=headers, data=data)
            print("insertBulk:" + response.text)
            assert response.status_code == 200


def test_deleteDoc():
    logger.info("test_deleteDoc")
    # fileData = "/home/vearch/test/data/test_data.json"
    with open(fileData, "r") as dataLine1:
        for dataLine in dataLine1:
            idStr = dataLine.split(',', 1)[0].replace('{', '')
            id = eval(idStr.split(':')[1])
            data = "{" + dataLine.split(',', 1)[1]
            url = "http://" + ip_data + "/" + db_name + "/" + space_name + "/" + id
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
