# -*- coding: UTF-8 -*-

import logging
import pytest
import requests
import json
from utils.vearch_utils import *

logging.basicConfig()
logger = logging.getLogger(__name__)

__description__ = """ test case for vearch """


fileData = "./data/test_data.json"
total = 10000
add_num = 1000
search_num = 10


# @pytest.mark.author('')
# @pytest.mark.level(2)
# @pytest.mark.cover(["VEARCH"])


class VearchCase:
    logger.info("test class")

    def setup_class(
        self, training_threshold: int, index_type: str, store_type: str
    ):
        self.training_threshold = training_threshold
        self.index_type = index_type
        self.store_type = store_type

    logging.info("cluster_information")

    def test_stats(self):
        response = get_cluster_stats(router_url)
        logger.debug("cluster_stats:" + json.dumps(response))
        assert response["code"] == 200

    def test_version(self):
        response = get_cluster_version(router_url)
        logger.debug("cluster_stats:" + json.dumps(response))
        assert response["code"] == 200

    def test_health(self):
        response = get_cluster_health(router_url)
        logger.debug("cluster_health---\n" + json.dumps(response))
        assert response["code"] == 200

    def test_server(self):
        response = get_servers_status(router_url)
        logger.debug("list_server---\n" + json.dumps(response))
        assert response["code"] == 200

    logger.info("database")

    def test_dblist(self):
        response = list_dbs(router_url)
        logger.debug("list_db---\n" + json.dumps(response))
        assert response["code"] == 200

    def test_createDB(self):
        response = create_db(router_url, db_name)
        logger.debug("db_create---\n" + json.dumps(response))
        assert response["code"] == 200

    def test_getDB(self):
        response = get_db(router_url, db_name)
        logger.debug("db_search---\n" + json.dumps(response))
        assert response["code"] == 200

    def test_listspace(self):
        response = list_spaces(router_url, db_name)
        logger.debug("list_space---\n" + json.dumps(response))
        assert response["code"] == 200

    def test_createspace(self, supported=True):
        data = {
            "name": space_name,
            "partition_num": 1,
            "replica_num": 1,
            "fields": [
                {
                    "name": "string",
                    "type": "keyword",
                    "index": {
                        "name": "string",
                        "type": "SCALAR",
                    },
                },
                {
                    "name": "int",
                    "type": "integer",
                    "index": {
                        "name": "int",
                        "type": "SCALAR",
                    },
                },
                {
                    "name": "float",
                    "type": "float",
                    "index": {
                        "name": "float",
                        "type": "SCALAR",
                    },
                },
                {
                    "name": "vector",
                    "type": "vector",
                    "dimension": 128,
                    "format": "normalization",
                    "store_type": self.store_type,
                    "store_param": {"cache_size": 1024},
                    "index": {
                        "name": "gamma",
                        "type": self.index_type,
                        "params": {
                            "metric_type": "InnerProduct",
                            "nprobe": 15,
                            "ncentroids": 256,
                            "nsubvector": 16,
                            "nlinks": 16,
                            "efConstruction": 60,
                            "efSearch": 32,
                            "training_threshold": self.training_threshold,
                        },
                    },
                },
                {
                    "name": "string_tags",
                    "type": "string",
                    "array": True,
                    "index": {
                        "name": "float",
                        "type": "SCALAR",
                    },
                },
            ],
        }
        logger.debug(router_url + "---" + json.dumps(data))
        response = create_space(router_url, db_name, data)
        logger.debug("space_create---\n" + json.dumps(response))
        if supported:
            assert response["code"] == 200
        else:
            assert response["code"] != 200

    def test_getspace(self):
        response = get_space(router_url, db_name, space_name)
        logger.debug("get_space---\n" + json.dumps(response))
        assert response["code"] == 200

    def test_getpartition(self):
        response = get_cluster_partition(router_url)
        logger.debug("get_space---\n" + json.dumps(response))
        assert response["code"] == 200

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

    def test_documentUpsert(self):
        logger.info("documentUpsert")
        url = router_url + "/document/upsert"
        headers = {"content-type": "application/json"}
        with open(fileData, "r") as dataLine1:
            for i, dataLine in zip(range(add_num), dataLine1):
                idStr = dataLine.split(",", 1)[0].replace("{", "")
                doc_id = eval(idStr.split(":")[1])
                data = "{" + dataLine.split(",", 1)[1]
                upsert_data = {}
                upsert_data["documents"] = [json.loads(data) for i in range(1)]
                upsert_data["db_name"] = db_name
                upsert_data["space_name"] = space_name
                response = requests.post(
                    url, headers=headers, data=json.dumps(upsert_data)
                )
                logger.debug("documentUpsert:" + response.text)
                assert response.status_code == 200

    def test_documentUpsertWithId(self):
        logger.info("documentUpsertWithId")
        url = router_url + "/document/upsert"
        headers = {"content-type": "application/json"}
        with open(fileData, "r") as dataLine1:
            for i, dataLine in zip(range(add_num), dataLine1):
                idStr = dataLine.split(",", 1)[0].replace("{", "")
                doc_id = eval(idStr.split(":")[1])
                data = "{" + dataLine.split(",", 1)[1]
                json_data = {}
                upsert_data = json.loads(data)
                upsert_data["_id"] = doc_id
                json_data["documents"] = [upsert_data for i in range(1)]
                json_data["db_name"] = db_name
                json_data["space_name"] = space_name
                logger.debug("documentUpsertWithId:" + json.dumps(json_data))
                response = requests.post(
                    url, headers=headers, data=json.dumps(json_data)
                )
                logger.debug("documentUpsertWithId:" + response.text)
                assert response.status_code == 200

    def test_documentUpsertBulkWithId(self):
        logger.info("documentUpsertBulkWithId")
        url = router_url + "/document/upsert"
        headers = {"content-type": "application/json"}

        with open(fileData, "r") as dataLine1:
            data_lines = dataLine1.readlines()

            for i in range(0, len(data_lines), 100):
                batch_data_lines = data_lines[i : i + 100]
                documents_batch = []

                for dataLine in batch_data_lines:
                    idStr = dataLine.split(",", 1)[0].replace("{", "")
                    doc_id = eval(idStr.split(":")[1])
                    data = "{" + dataLine.split(",", 1)[1]
                    data_json = json.loads(data)
                    data_json["_id"] = doc_id
                    documents_batch.append(data_json)

                upsert_data = {
                    "documents": documents_batch,
                    "db_name": db_name,
                    "space_name": space_name,
                }

                response = requests.post(
                    url, headers=headers, data=json.dumps(upsert_data)
                )
                logger.debug("test_documentUpsertBulkWithId:" + response.text)
                assert response.status_code == 200

    def test_documentQueryByDocumentIds(self):
        logger.info("documentQueryByDocumentIds")
        headers = {"content-type": "application/json"}
        url = router_url + "/document/query"
        with open(fileData, "r") as dataLine1:
            for i, dataLine in zip(range(add_num), dataLine1):
                idStr = dataLine.split(",", 1)[0].replace("{", "")
                doc_id = eval(idStr.split(":")[1])
                data = "{" + dataLine.split(",", 1)[1]
                data = json.loads(data)
                data["db_name"] = db_name
                data["space_name"] = space_name
                data["query"] = {}
                data["query"]["document_ids"] = [doc_id for i in range(1)]
                response = requests.post(url, headers=headers, data=json.dumps(data))
                logger.debug("insertNoID:" + response.text)
                assert response.status_code == 200

    def test_documentQueryOnSpecifyPartiton(self):
        logger.info("documentQueryOnSpecifyPartiton")
        response = get_space(router_url, db_name, space_name)
        assert response["code"] == 200

        partitions = response["data"]["partitions"]
        assert len(partitions) > 0
        partition = str(partitions[0]["pid"])

        url = router_url + "/document/query"
        headers = {"content-type": "application/json"}

        add_num_end = add_num + 100
        if add_num_end > total:
            add_num_end = total
        for i in range(add_num, add_num_end):
            data = {}
            data["db_name"] = db_name
            data["space_name"] = space_name
            data["query"] = {}
            data["query"]["document_ids"] = [str(i) for j in range(1)]
            data["query"]["partition_id"] = partition
            response = requests.post(url, headers=headers, data=json.dumps(data))
            logger.debug("documentQueryOnSpecifyPartiton:" + response.text)
            assert response.status_code == 200
            assert response.text.find('"total":1') >= 0
            assert len(response.json()["data"]["documents"]) == 1

    def test_documentQueryByFilter(self):
        logger.info("documentQueryByFilter")
        headers = {"content-type": "application/json"}
        url = router_url + "/document/query"
        with open(fileData, "r") as dataLine1:
            for i, dataLine in zip(range(search_num), dataLine1):
                idStr = dataLine.split(",", 1)[0].replace("{", "")
                id = eval(idStr.split(":")[1])
                feature = "{" + dataLine.split(",", 1)[1]
                feature = json.loads(feature)
                string_tags = feature["string_tags"]
                feature = feature["vector"]
                data = {
                    "query": {
                        "filter": [{"term": {"string": string_tags, "operator": "or"}}],
                    },
                    "db_name": db_name,
                    "space_name": space_name,
                }

                response = requests.post(url, headers=headers, data=json.dumps(data))
                logger.debug("searchByFeature---\n" + response.text)
                assert response.status_code == 200

    def test_documentSearchByVector(self):
        logger.info("documentSearchByVector")
        headers = {"content-type": "application/json"}
        url = router_url + "/document/search"
        with open(fileData, "r") as dataLine1:
            for i, dataLine in zip(range(search_num), dataLine1):
                idStr = dataLine.split(",", 1)[0].replace("{", "")
                id = eval(idStr.split(":")[1])
                feature = "{" + dataLine.split(",", 1)[1]
                feature = json.loads(feature)
                string_tags = feature["string_tags"]
                feature = feature["vector"]
                data = {
                    "query": {
                        "vector": [
                            {
                                "field": "vector",
                                "feature": feature,
                            }
                        ]
                    },
                    "db_name": db_name,
                    "space_name": space_name,
                    "size": 3,
                    "is_brute_search": 1,
                }

                response = requests.post(url, headers=headers, data=json.dumps(data))
                logger.debug("searchByFeature---\n" + response.text)
                assert response.status_code == 200

    def test_documentModifySinglefield(self):
        logger.info("documentModifySinglefield")
        headers = {"content-type": "application/json"}
        # modify single field
        url = router_url + "/document/upsert"
        json_data = {
            "db_name": db_name,
            "space_name": space_name,
            "documents": [{"_id": "0", "float": 888.88, "string": "test"}],
        }
        logger.debug("documentUpsertWithId:" + json.dumps(json_data))
        response = requests.post(url, headers=headers, data=json.dumps(json_data))
        logger.debug("documentUpsertWithId:" + response.text)
        assert response.status_code == 200

        # check result
        url = router_url + "/document/query"

        data = {
            "db_name": db_name,
            "space_name": space_name,
            "query": {"document_ids": ["0"]},
        }

        response = requests.post(url, headers=headers, data=json.dumps(data))
        logger.debug("getById:" + response.text)
        assert response.status_code == 200
        result = json.loads(response.text)
        assert result["data"]["total"] == 1
        assert result["data"]["documents"][0]["_source"]["float"] == 888.88
        assert result["data"]["documents"][0]["_source"]["string"] == "test"

    def test_documentUpsertSinglefield(self):
        logger.info("documentUpsertSinglefield")
        headers = {"content-type": "application/json"}
        # upsert single field
        url = router_url + "/document/upsert"
        json_data = {
            "db_name": db_name,
            "space_name": space_name,
            "documents": [{"float": 888.88, "string": "test"}],
        }
        logger.debug("documentUpsertSinglefield:" + json.dumps(json_data))
        response = requests.post(url, headers=headers, data=json.dumps(json_data))
        logger.debug("documentUpsertSinglefield:" + response.text)
        assert response.status_code != 200

    def test_documentDeleteByDocumentIds(self):
        logger.info("documentDeleteByDocumentIds")
        headers = {"content-type": "application/json"}
        url = router_url + "/document/delete"
        with open(fileData, "r") as dataLine1:
            for i, dataLine in zip(range(add_num), dataLine1):
                idStr = dataLine.split(",", 1)[0].replace("{", "")
                doc_id = eval(idStr.split(":")[1])
                data = "{" + dataLine.split(",", 1)[1]
                data = json.loads(data)
                data["db_name"] = db_name
                data["space_name"] = space_name
                data["query"] = {}
                data["query"]["document_ids"] = [doc_id for i in range(1)]
                response = requests.post(url, headers=headers, data=json.dumps(data))
                logger.debug("insertNoID:" + response.text)
                assert response.status_code == 200

    def test_documentDeleteByFilter(self):
        logger.info("documentQueryByFilter")
        headers = {"content-type": "application/json"}
        url = router_url + "/document/delete"
        with open(fileData, "r") as dataLine1:
            for i, dataLine in zip(range(search_num), dataLine1):
                idStr = dataLine.split(",", 1)[0].replace("{", "")
                id = eval(idStr.split(":")[1])
                feature = "{" + dataLine.split(",", 1)[1]
                feature = json.loads(feature)
                string_tags = feature["string_tags"]
                feature = feature["vector"]
                data = {
                    "query": {
                        "filter": [{"term": {"string": string_tags, "operator": "or"}}],
                    },
                    "db_name": db_name,
                    "space_name": space_name,
                }

                response = requests.post(url, headers=headers, data=json.dumps(data))
                logger.debug("searchByFeature---\n" + response.text)
                assert response.status_code == 200

    def test_deleteSpace(self):
        response = drop_space(router_url, db_name, space_name)
        logger.debug("deleteSpace:" + json.dumps(response))
        assert response["code"] == 200

    def test_deleteDB(self):
        response = drop_db(router_url, db_name)
        logger.debug("deleteDB:" + json.dumps(response))
        assert response["code"] == 200

    def run_db_space_create_test(self, supported=True):
        self.test_createDB()
        self.test_createspace(supported)
        self.test_stats()
        self.test_health()
        self.test_server()
        if supported:
            self.test_deleteSpace()
        self.test_deleteDB()

    def run_db_space_create_multi_test(self):
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
            self.test_getpartition()
            self.test_deleteSpace()
            self.test_deleteDB()

    def test_documentInterface(self):
        self.test_documentUpsert()
        self.test_documentUpsertWithId()
        self.test_documentUpsertBulkWithId()
        self.test_documentQueryByDocumentIds()
        self.test_documentQueryOnSpecifyPartiton()
        self.test_documentQueryByFilter()
        self.test_documentSearchByVector()
        self.test_documentModifySinglefield()
        self.test_documentUpsertSinglefield()
        self.test_documentDeleteByDocumentIds()
        self.test_documentDeleteByFilter()

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
        self.test_getpartition()
        self.test_documentInterface()
        self.test_deleteSpace()
        self.test_deleteDB()


# for FLAT HNSW IVFFLAT, now only support one store_type, no need to set


@pytest.mark.parametrize(
    ["training_threshold", "index_type", "store_type"],
    [
        [1, "FLAT", ""],
        [990, "IVFPQ", "MemoryOnly"],
        [990, "IVFPQ", "RocksDB"],
        [1, "HNSW", ""],
        [990, "IVFFLAT", ""],
    ],
)
def test_vearch_usage(
    training_threshold: int, index_type: str, store_type: str
):
    case = VearchCase()
    case.setup_class(training_threshold, index_type, store_type)
    case.run_basic_usage_test()
    case.run_db_space_create_multi_test()


# Not support now so should be failed


@pytest.mark.parametrize(
    ["training_threshold", "index_type", "store_type"],
    [
        [1, "FLAT", "RocksDB"],
        [1, "FLAT", "NOTSUPPORTTYPE"],
        [1, "HNSW", "RocksDB"],
        [990, "IVFFLAT", "MemoryOnly"],
    ],
)
def test_vearch_create_space(
    training_threshold: int, index_type: str, store_type: str
):
    case = VearchCase()
    case.setup_class(training_threshold, index_type, store_type)
    case.run_db_space_create_test(False)
