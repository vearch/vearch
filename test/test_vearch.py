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
proxy = "http://" + ip_router
db_name = "ts_db"
space_name = "ts_space"
fileData = "./data/test_data.json"
total = 10000
add_num = 1000
search_num = 10


# @pytest.mark.author('')
# @pytest.mark.level(2)
# @pytest.mark.cover(["VEARCH"])


class VearchCase:
    logger.info("test class")

    def setup(
        self, training_threshold: int, index_type: str, store_type: str
    ):
        self.training_threshold = training_threshold
        self.index_type = index_type
        self.store_type = store_type

    logging.info("cluster_information")

    def test_stats(self):
        url = proxy + "/_cluster/stats"
        response = requests.get(url)
        logger.debug("cluster_stats:" + response.text)
        assert response.status_code == 200
        assert response.text.find('"status":200') >= 0

    def test_health(self):
        url = proxy + "/_cluster/health?detail=true"
        response = requests.get(url)
        logger.debug("cluster_health---\n" + response.text)
        assert response.status_code == 200
        #  assert response.text.find("\"status\":\"green\"")>=0

    def test_server(self):
        url = proxy + "/list/server"
        response = requests.get(url)
        logger.debug("list_server---\n" + response.text)
        assert response.status_code == 200
        assert response.text.find('"msg":"success"') >= 0

    logger.info("database")

    def test_dblist(self):
        url = proxy + "/dbs"
        response = requests.get(url)
        logger.debug("list_db---\n" + response.text)
        assert response.status_code == 200
        assert response.text.find('"msg":"success"') >= 0

    def test_createDB(self):
        logger.info("------------")
        url = proxy + "/dbs/" + db_name
        headers = {"content-type": "application/json"}
        response = requests.post(url, headers=headers)
        logger.debug("db_create---\n" + response.text)
        assert response.status_code == 200
        assert response.text.find('"msg":"success"') >= 0

    def test_getDB(self):
        url = proxy + "/dbs/" + db_name
        response = requests.get(url)
        logger.debug("db_search---\n" + response.text)
        assert response.status_code == 200
        assert response.text.find('"msg":"success"') >= 0

    def test_listspace(self):
        url = proxy + "/list/space?db=" + db_name
        response = requests.get(url)
        logger.debug("list_space---\n" + response.text)
        assert response.status_code == 200
        assert response.text.find('"msg":"success"') >= 0

    def test_createspace(self, supported=True):
        url = proxy + "/dbs/" + db_name + "/spaces"
        headers = {"content-type": "application/json"}
        data = {
            "name": space_name,
            "partition_num": 1,
            "replica_num": 1,
            "index": {
                "index_name": "gamma",
                "index_type": self.index_type,
                "index_params": {
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
            "fields": {
                "string": {"type": "keyword", "index": True},
                "int": {"type": "integer", "index": True},
                "float": {"type": "float", "index": True},
                "vector": {
                    "type": "vector",
                    "dimension": 128,
                    "format": "normalization",
                    "store_type": self.store_type,
                    "store_param": {"cache_size": 1024},
                },
                "string_tags": {"type": "string", "array": True, "index": True},
            },
        }
        logger.debug(url + "---" + json.dumps(data))
        response = requests.post(url, headers=headers, data=json.dumps(data))
        logger.debug("space_create---\n" + response.text)
        assert response.status_code == 200
        if supported:
            assert response.text.find('"msg":"success"') >= 0
        else:
            assert response.text.find('"code":550') >= 0

    def test_getspace(self):
        url = proxy + "/dbs/" + db_name + "/spaces/" + space_name
        response = requests.get(url)
        logger.debug("get_space---\n" + response.text)
        assert response.status_code == 200
        assert response.text.find('"msg":"success"') >= 0

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
        url = proxy + "/document/upsert"
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
        url = proxy + "/document/upsert"
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
        logger.info("test_documentUpsertBulkWithId")
        url = proxy + "/document/upsert"
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
        url = proxy + "/document/query"
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
        url = proxy + "/dbs/" + db_name + "/spaces/" + space_name
        response = requests.get(url)
        assert response.status_code == 200
        assert response.text.find('"msg":"success"') >= 0

        partitions = response.json()["data"]["partitions"]
        assert len(partitions) > 0
        partition = str(partitions[0]["pid"])

        url = proxy + "/document/query"
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
            assert len(response.json()["documents"]) == 1

    def test_documentQueryByFilter(self):
        logger.info("documentQueryByFilter")
        headers = {"content-type": "application/json"}
        url = proxy + "/document/query"
        with open(fileData, "r") as dataLine1:
            for i, dataLine in zip(range(search_num), dataLine1):
                idStr = dataLine.split(",", 1)[0].replace("{", "")
                id = eval(idStr.split(":")[1])
                feature = "{" + dataLine.split(",", 1)[1]
                feature = json.loads(feature)
                string_tags = feature["string_tags"]
                feature = feature["vector"]["feature"]
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

    def test_documentSearchByDocumentIds(self):
        logger.info("documentSearchByDocumentIds")
        headers = {"content-type": "application/json"}
        url = proxy + "/document/search"
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

    def test_documentSearchByVector(self):
        logger.info("documentSearchByVector")
        headers = {"content-type": "application/json"}
        url = proxy + "/document/search"
        with open(fileData, "r") as dataLine1:
            for i, dataLine in zip(range(search_num), dataLine1):
                idStr = dataLine.split(",", 1)[0].replace("{", "")
                id = eval(idStr.split(":")[1])
                feature = "{" + dataLine.split(",", 1)[1]
                feature = json.loads(feature)
                string_tags = feature["string_tags"]
                feature = feature["vector"]["feature"]
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

    def test_document_modify_singlefield(self):
        logger.info("document_modify_singlefield")
        headers = {"content-type": "application/json"}
        # modify single field
        url = proxy + "/document/upsert"
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
        url = proxy + "/document/query"

        data = {
            "db_name": db_name,
            "space_name": space_name,
            "query": {"document_ids": ["0"]},
        }

        response = requests.post(url, headers=headers, data=json.dumps(data))
        logger.debug("getById:" + response.text)
        assert response.status_code == 200
        result = json.loads(response.text)
        assert result["total"] == 1
        assert result["documents"][0]["_source"]["float"] == 888.88
        assert result["documents"][0]["_source"]["string"] == "test"

    def test_document_upsert_singlefield(self):
        logger.info("document_upsert_singlefield")
        headers = {"content-type": "application/json"}
        # upsert single field
        url = proxy + "/document/upsert"
        json_data = {
            "db_name": db_name,
            "space_name": space_name,
            "documents": [{"float": 888.88, "string": "test"}],
        }
        logger.debug("document_upsert_singlefield:" + json.dumps(json_data))
        response = requests.post(url, headers=headers, data=json.dumps(json_data))
        logger.debug("document_upsert_singlefield:" + response.text)
        assert response.status_code != 200

    def test_documentDeleteByDocumentIds(self):
        logger.info("documentDeleteByDocumentIds")
        headers = {"content-type": "application/json"}
        url = proxy + "/document/delete"
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
        url = proxy + "/document/delete"
        with open(fileData, "r") as dataLine1:
            for i, dataLine in zip(range(search_num), dataLine1):
                idStr = dataLine.split(",", 1)[0].replace("{", "")
                id = eval(idStr.split(":")[1])
                feature = "{" + dataLine.split(",", 1)[1]
                feature = json.loads(feature)
                string_tags = feature["string_tags"]
                feature = feature["vector"]["feature"]
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
        url = proxy + "/dbs/" + db_name + "/spaces/" + space_name
        response = requests.delete(url)
        logger.debug("deleteSpace:" + response.text)
        assert response.status_code == 200

    def test_deleteDB(self):
        url = proxy + "/dbs/" + db_name
        response = requests.delete(url)
        logger.debug("deleteDB:" + response.text)
        assert response.status_code == 200

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
            self.test_deleteSpace()
            self.test_deleteDB()

    def run_new_document_interface_test(self):
        self.test_documentUpsert()
        self.test_documentUpsertWithId()
        self.test_documentUpsertBulkWithId()
        self.test_documentQueryByDocumentIds()
        self.test_documentQueryOnSpecifyPartiton()
        self.test_documentQueryByFilter()
        self.test_documentSearchByDocumentIds()
        self.test_documentSearchByVector()
        self.test_document_modify_singlefield()
        self.test_document_upsert_singlefield()
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
        self.run_new_document_interface_test()
        self.test_deleteSpace()
        self.test_deleteDB()


# for FLAT HNSW IVFFLAT, now only support one store_type, no need to set


@pytest.mark.parametrize(
    ["training_threshold", "index_type", "store_type"],
    [
        [1, "FLAT", ""],
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
    case.setup(training_threshold, index_type, store_type)
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
    case.setup(training_threshold, index_type, store_type)
    case.run_db_space_create_test(False)
