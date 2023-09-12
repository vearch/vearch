# -*- coding: UTF-8 -*-

import logging
import pytest
import requests
import json

logging.basicConfig()
logger = logging.getLogger(__name__)

__description__ = """ test case for vearch """

ip = "127.0.0.1"
ip_master = ip + ":8817"
ip_router = ip + ":9001"
proxy = ip_router
db_name = "ts_db"
space_name = "ts_space"
fileData = "./data/test_data.json"
add_num = 1000
search_num = 10


# @pytest.mark.author('')
# @pytest.mark.level(2)
# @pytest.mark.cover(["VEARCH"])


class VearchCase():
    logger.info("test class")

    def setup(self, index_size, id_type, retrieval_type, store_type):
        self.index_size = index_size
        self.id_type = id_type
        self.retrieval_type = retrieval_type
        self.store_type = store_type

    logging.info("cluster_information")

    def test_stats(self):
        url = "http://" + proxy + "/_cluster/stats"
        response = requests.get(url)
        logger.debug("cluster_stats:" + response.text)
        assert response.status_code == 200
        assert response.text.find("\"status\":200") >= 0

    def test_health(self):
        url = "http://" + proxy + "/_cluster/health"
        response = requests.get(url)
        logger.debug("cluster_health---\n" + response.text)
        assert response.status_code == 200
        #  assert response.text.find("\"status\":\"green\"")>=0

    def test_server(self):
        url = "http://" + proxy + "/list/server"
        response = requests.get(url)
        logger.debug("list_server---\n" + response.text)
        assert response.status_code == 200
        assert response.text.find("\"msg\":\"success\"") >= 0

    logger.info("database")

    def test_dblist(self):
        url = "http://" + proxy + "/list/db"
        response = requests.get(url)
        logger.debug("list_db---\n" + response.text)
        assert response.status_code == 200
        assert response.text.find("\"msg\":\"success\"") >= 0

    def test_createDB(self):
        logger.info("------------")
        url = "http://" + proxy + "/db/_create"
        headers = {"content-type": "application/json"}
        data = {
            'name': db_name
        }
        response = requests.put(url, headers=headers, data=json.dumps(data))
        logger.debug("db_create---\n" + response.text)
        assert response.status_code == 200
        assert response.text.find("\"msg\":\"success\"") >= 0

    def test_getDB(self):
        url = "http://" + proxy + "/db/" + db_name
        response = requests.get(url)
        logger.debug("db_search---\n" + response.text)
        assert response.status_code == 200
        assert response.text.find("\"msg\":\"success\"") >= 0

    def test_listspace(self):
        url = "http://" + proxy + "/list/space?db=" + db_name
        response = requests.get(url)
        logger.debug("list_space---\n" + response.text)
        assert response.status_code == 200
        assert response.text.find("\"msg\":\"success\"") >= 0

    def test_createspace(self):
        url = "http://" + proxy + "/space/" + db_name + "/_create"
        headers = {"content-type": "application/json"}
        data = {
            "name": space_name,
            "partition_num": 1,
            "replica_num": 1,
            "engine": {
                "index_size": self.index_size,
                "id_type": self.id_type,
                "retrieval_type": self.retrieval_type,
                "retrieval_param": {
                    "metric_type": "InnerProduct",
                    "nprobe": 15,
                    "ncentroids": 256,
                    "nsubvector": 16,
                    "nlinks": 16,
                    "efConstruction": 60,
                    "efSearch": 32
                }
            },
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
                    "format": "normalization",
                    # "retrieval_type": "GPU",
                    "store_type": self.store_type,
                    "store_param":
                        {
                            "cache_size": 1024
                        }
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
        logger.debug(url+"---"+json.dumps(data))
        response = requests.put(url, headers=headers, data=json.dumps(data))
        logger.debug("space_create---\n" + response.text)
        assert response.status_code == 200
        assert response.text.find("\"msg\":\"success\"") >= 0

    def test_getspace(self):
        url = "http://" + proxy + "/space/"+db_name+"/" + space_name
        response = requests.get(url)
        logger.debug("get_space---\n" + response.text)
        assert response.status_code == 200
        assert response.text.find("\"msg\":\"success\"") >= 0

    # def test_changemember():
    #     url = "http://" + ip_master + "/partition/change_member"
    #     headers = {"content-type": "application/json"}
    #     data = {
    #         "partition_id":7,
    #         "node_id":1,
    #         "method":0
    #     }
    #     response = requests.post(url, headers=headers, data=json.dumps(data))
    #     logger.debug("change_member:" + response.text)
    #     assert response.status_code == 200
    #     assert response.text.find("\"msg\":\"success\"")>=0

    logger.info("router(PS)")

    def test_insertWithId(self):
        logger.info("insert")
        headers = {"content-type": "application/json"}
        with open(fileData, "r") as dataLine1:
            for i, dataLine in zip(range(add_num), dataLine1):
                idStr = dataLine.split(',', 1)[0].replace('{', '')
                flag = 0
                flag1 = idStr.split(':')[1].replace('\"', '')
                id = str(int(flag1)+flag)
                data = "{"+dataLine.split(',', 1)[1]
                url = "http://" + proxy + "/" + db_name + "/" + space_name + "/" + id
                response = requests.post(url, headers=headers, data=data)
                logger.debug("insertWithID:" + response.text)
                assert response.status_code == 200
                #  assert response.text.find("\"status\":201")>=0

    def test_getById(self):
        logger.info("test_getById")
        with open(fileData, "r") as dataLine1:
            for i, dataLine in zip(range(add_num), dataLine1):
                idStr = dataLine.split(',', 1)[0].replace('{', '')
                # id = eval(idStr.split(':')[1])
                flag = 0
                flag1 = idStr.split(':')[1].replace('\"', '')
                id = str(int(flag1)+flag)
                url = "http://" + proxy + "/" + db_name + "/" + space_name + "/" + id
                response = requests.get(url)
                logger.debug("searchById:" + response.text)
                assert response.status_code == 200
                assert response.text.find("\"found\":true") >= 0

    def test_insertNoId(self):
        logger.info("insertDataNoId")
        headers = {"content-type": "application/json"}
        with open(fileData, "r") as dataLine1:
            for i, dataLine in zip(range(add_num), dataLine1):
                idStr = dataLine.split(',', 1)[0].replace('{', '')
                id = eval(idStr.split(':')[1])
                data = "{"+dataLine.split(',', 1)[1]
                url = "http://" + proxy + "/" + db_name + "/" + space_name
                response = requests.post(url, headers=headers, data=data)
                logger.debug("insertNoID:" + response.text)
                assert response.status_code == 200
                #  assert response.text.find("\"successful\":1")>=0

    def test_searchByFeature(self):
        headers = {"content-type": "application/json"}
        url = "http://" + proxy + "/"+db_name+"/"+space_name+"/_search?size=100"
        with open(fileData, "r") as dataLine1:
            for i, dataLine in zip(range(search_num), dataLine1):
                idStr = dataLine.split(',', 1)[0].replace('{', '')
                id = eval(idStr.split(':')[1])
                feature = "{"+dataLine.split(',', 1)[1]
                feature = json.loads(feature)
                feature = feature["vector"]["feature"]
                data = {
                    "query": {
                        "sum": [{
                            "field": "vector",
                            "feature": feature,
                            "format": "normalization"
                        }]
                    },
                    "is_brute_search": 1
                }
                response = requests.post(
                    url, headers=headers, data=json.dumps(data))
                logger.debug("searchByFeature---\n" + response.text)
                assert response.status_code == 200
                #  assert response.text.find("\"failed\":0")>=0

    def test_bulk_searchByFeature(self):
        headers = {"content-type": "application/json"}
        url = "http://" + proxy + "/"+db_name+"/"+space_name+"/_bulk_search"
        for ii in range(10):
            request_body = []
            with open(fileData, "r") as f:
                for i, dataLine in zip(range(search_num), f):
                    data = json.loads(dataLine)
                    feature = data["vector"]["feature"]
                    data = {
                        "query": {
                            "sum": [{
                                "field": "vector",
                                "feature": feature,
                                "format": "normalization"
                            }],
                            "is_brute_search": 1,
                            "size": 10
                        }
                    }
                    request_body.append(data)
            response = requests.post(
                url, headers=headers, data=json.dumps(request_body))
            logger.debug("searchByFeature---\n" + response.text)
            assert response.status_code == 200

    def test_searchByFeatureandFilter(self):
        url = "http://" + proxy + "/"+db_name+"/"+space_name+"/_search"
        headers = {"content-type": "application/json"}
        with open(fileData, "r") as dataLine1:
            for i, dataLine in zip(range(search_num), dataLine1):
                idStr = dataLine.split(',', 1)[0].replace('{', '')
                id = eval(idStr.split(':')[1])
                feature = "{"+dataLine.split(',', 1)[1]
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
                    },
                    "is_brute_search": 1
                }
                response = requests.post(
                    url, headers=headers, data=json.dumps(data))
                logger.debug("searchByFeature---\n" + response.text)
                assert response.status_code == 200
                #  assert response.text.find("\"failed\":0") >= 0

    def test_searchByFeatureandFilter(self):
        url = "http://" + proxy + "/"+db_name+"/"+space_name+"/_search"
        headers = {"content-type": "application/json"}
        with open(fileData, "r") as dataLine1:
            for i, dataLine in zip(range(search_num), dataLine1):
                idStr = dataLine.split(',', 1)[0].replace('{', '')
                id = eval(idStr.split(':')[1])
                feature = "{"+dataLine.split(',', 1)[1]
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
                    },
                    "is_brute_search": 1
                }
                response = requests.post(
                    url, headers=headers, data=json.dumps(data))
                logger.debug("searchByFeature---\n" + response.text)
                assert response.status_code == 200
                #  assert response.text.find("\"failed\":0") >= 0

    def test_searchByFeatureandRange(self):
        url = "http://" + proxy + "/"+db_name+"/"+space_name+"/_search"
        headers = {"content-type": "application/json"}
        with open(fileData, "r") as dataLine1:
            for i, dataLine in zip(range(search_num), dataLine1):
                idStr = dataLine.split(',', 1)[0].replace('{', '')
                id = eval(idStr.split(':')[1])
                feature = "{"+dataLine.split(',', 1)[1]
                feature = json.loads(feature)
                string_tags = feature["string_tags"]
                feature = feature["vector"]["feature"]
                data = {
                    "query": {
                        "filter": [{
                            "range": {
                                "int": {
                                    "gte": 0,
                                    "lte": 0
                                }
                            }
                        }],
                        "sum": [{
                            "field": "vector",
                            "feature": feature,
                            "format": "normalization"
                        }]
                    }
                }
                # logger.debug("data:" + json.dumps(data))
                response = requests.post(
                    url, headers=headers, data=json.dumps(data))
                logger.debug("searchByFeature---\n" + response.text)
                assert response.status_code == 200
                #  assert response.text.find("\"failed\":0") >= 0

    def test_searchByTerm(self):
        url = "http://" + proxy + "/"+db_name+"/"+space_name+"/_search"
        headers = {"content-type": "application/json"}
        with open(fileData, "r") as dataLine1:
            for i, dataLine in zip(range(search_num), dataLine1):
                idStr = dataLine.split(',', 1)[0].replace('{', '')
                id = eval(idStr.split(':')[1])
                feature = "{"+dataLine.split(',', 1)[1]
                feature = json.loads(feature)
                string_tags = feature["string_tags"]
                feature = feature["vector"]["feature"]
                data = {
                    "query": {
                        "filter": [{
                            "term": {
                                "string": "0AW1mK_j19FyJvn5NR4Eb",
                                "operator": "or"
                            }
                        }],
                        "sum": [{
                            "field": "vector",
                            "feature": feature,
                            "format": "normalization"
                        }]
                    },
                    "is_brute_search": 1
                }

                response = requests.post(
                    url, headers=headers, data=json.dumps(data))
                logger.debug("searchByFeature---\n" + response.text)
                assert response.status_code == 200

    def test_deleteDocById(self):
        logger.info("test_deleteDoc")
        with open(fileData, "r") as dataLine1:
            for i, dataLine in zip(range(add_num), dataLine1):
                idStr = dataLine.split(',', 1)[0].replace('{', '')
                id = eval(idStr.split(':')[1])
                data = "{"+dataLine.split(',', 1)[1]
                url = "http://" + proxy + "/" + db_name + "/" + space_name + "/" + id
                response = requests.delete(url)
                logger.debug("deleteDocById:" + response.text)
                assert response.status_code == 200

    def test_insertBulk(self):
        logger.info("insertBulk")
        url = "http://" + proxy + "/"+db_name+"/"+space_name+"/_bulk"
        headers = {"content-type": "application/json"}
        data = ''
        with open(fileData, "r") as dataLine1:
            for i, dataLine in zip(range(add_num), dataLine1):
                idStr = dataLine.split(',', 1)[0]+"}"
                index = "{\"index\":"+idStr+"}"
                index = index + "\n"
                dataStr = "{"+dataLine.split(',', 1)[1]
                data = data + index + dataStr
            response = requests.post(url, headers=headers, data=data)
            logger.debug("insertBulk:" + response.text)
            assert response.status_code == 200

    def test_insertBulkNoId(self):
        logger.info("insertBulkDataNoId")
        headers = {"content-type": "application/json"}
        with open(fileData, "r") as dataLine1:
            for i, dataLine in zip(range(add_num), dataLine1):
                idStr = dataLine.split(',', 1)[0].replace('{', '')
                id = eval(idStr.split(':')[1])
                data = "{"+dataLine.split(',', 1)[1]
                url = "http://" + proxy + "/" + db_name + "/" + space_name
                response = requests.post(url, headers=headers, data=data)
                logger.debug("insertNoID:" + response.text)
                assert response.status_code == 200

    def test_deleteSpace(self):
        url = "http://" + proxy + "/space/"+db_name+"/"+space_name
        response = requests.delete(url)
        logger.debug("deleteSpace:" + response.text)
        assert response.status_code == 200

    def test_deleteDB(self):
        url = "http://" + proxy + "/db/"+db_name
        response = requests.delete(url)
        logger.debug("deleteDB:" + response.text)
        assert response.status_code == 200

    def test_db_space_create_delete(self):
        for i in range(10):
            self.test_stats()
            self.test_health()
            self.test_server()
            self.test_dblist()
            self.test_createDB()
            self.test_getDB()
            self.test_listspace()
            self.test_createspace()
            self.test_getspace()
            self.test_deleteSpace()
            self.test_deleteDB()

    def run_basic_usage_test(self):
        self.test_stats()
        self.test_health()
        self.test_server()
        self.test_dblist()
        self.test_createDB()
        self.test_getDB()
        self.test_listspace()
        self.test_createspace()
        self.test_getspace()
        self.test_insertWithId()
        self.test_getById()
        self.test_insertNoId()
        self.test_searchByFeature()
        self.test_bulk_searchByFeature()
        self.test_searchByFeatureandFilter()
        self.test_searchByFeatureandRange()
        self.test_searchByTerm()
        self.test_deleteDocById()
        self.test_insertBulk()
        self.test_insertBulkNoId()
        self.test_deleteSpace()
        self.test_deleteDB()
        self.test_db_space_create_delete()


@pytest.mark.parametrize(["index_size", "id_type", "retrieval_type", "store_type"], [
    [1, "Long", "FLAT", "MemoryOnly"],
    [1, "String", "FLAT", "MemoryOnly"],
    [990, "Long", "IVFPQ", "MemoryOnly"],
    [990, "Long", "IVFPQ", "RocksDB"],
    [990, "String", "IVFPQ", "MemoryOnly"],
    [990, "String", "IVFPQ", "RocksDB"],
    [1, "Long", "HNSW", "MemoryOnly"],
    [1, "String", "HNSW", "MemoryOnly"],
    [990, "Long", "IVFFLAT", "RocksDB"],
    [990, "String", "IVFFLAT", "RocksDB"]
])
def test_vearch(index_size:int, id_type: str, retrieval_type: str, store_type: str):
    case = VearchCase()
    case.setup(index_size, id_type, retrieval_type, store_type)
    case.run_basic_usage_test()
