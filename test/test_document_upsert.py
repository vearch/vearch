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

__description__ = """ test case for document upsert """


def query(xq, gt, k):
    query_dict = {
        "vectors": [],
        "vector_value": False,
        "fields": ["field_int"],
        "limit": k,
        "db_name": db_name,
        "space_name": space_name,
    }

    for batch in [True]:
        avarage, recalls = evaluate(xq, gt, k, batch, query_dict)
        result = "batch: %d, avarage time: %.2f ms, " % (batch, avarage)
        for recall in recalls:
            result += "recall@%d = %.2f%% " % (recall, recalls[recall] * 100)
            if recall == k:
                assert recalls[recall] >= 1.0
        logger.info(result)


sift10k = DatasetSift10K()
xb = sift10k.get_database()
xq = sift10k.get_queries()
gt = sift10k.get_groundtruth()


def benchmark(total, bulk, with_id, full_field, xb, xq, gt):
    embedding_size = xb.shape[1]
    batch_size = 1
    if bulk:
        batch_size = 100
    k = 100
    if total == 0:
        total = xb.shape[0]
    total_batch = int(total / batch_size)
    logger.info(
        "dataset num: %d, total_batch: %d, dimension: %d, search num: %d, topK: %d"
        % (total, total_batch, embedding_size, xq.shape[0], k)
    )

    properties = {}
    properties["fields"] = [
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
            "index": {
                "name": "gamma",
                "type": "FLAT",
                "params": {
                    "metric_type": "L2",
                },
            },
            "dimension": embedding_size,
            "store_type": "MemoryOnly",
            # "format": "normalization"
        },
    ]

    create_for_document_test(router_url, embedding_size, properties)

    add(total_batch, batch_size, xb, with_id, full_field)

    waiting_index_finish(total, 1)

    query(xq, gt, k)

    destroy(router_url, db_name, space_name)


@pytest.mark.parametrize(
    ["bulk", "with_id", "full_field"],
    [
        [True, True, True],
        [True, True, False],
        [True, False, True],
        [True, False, False],
        [False, True, True],
        [False, True, False],
        [False, False, True],
        [False, False, False],
    ],
)
def test_vearch_document_upsert_benchmark(bulk: bool, with_id: bool, full_field: bool):
    benchmark(0, bulk, with_id, full_field, xb, xq, gt)


def test_vearch_document_upsert_with_and_without_id():
    """Test batch upsert with one document having _id and one without _id, then query to verify."""
    embedding_size = xb.shape[1]
    
    properties = {}
    properties["fields"] = [
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
            "index": {
                "name": "gamma",
                "type": "FLAT",
                "params": {
                    "metric_type": "L2",
                },
            },
            "dimension": embedding_size,
            "store_type": "MemoryOnly",
        },
    ]

    create_for_document_test(router_url, embedding_size, properties)

    # Prepare two documents: one with _id, one without _id
    url = router_url + "/document/upsert"
    data = {
        "db_name": db_name,
        "space_name": space_name,
        "documents": [
            {
                "_id": "test_id_001",
                "field_int": 100,
                "field_long": 100,
                "field_float": 100.0,
                "field_double": 100.0,
                "field_string": "test_with_id",
                "field_vector": xb[0].tolist(),
            },
            {
                "field_int": 200,
                "field_long": 200,
                "field_float": 200.0,
                "field_double": 200.0,
                "field_string": "test_without_id",
                "field_vector": xb[1].tolist(),
            },
        ],
    }

    rs = requests.post(url, auth=(username, password), json=data)
    logger.info(f"Upsert response: {rs.json()}")
    assert rs.json()["code"] == 0
    assert rs.json()["data"]["total"] == 2
    
    # Get the document_ids returned
    document_ids = rs.json()["data"]["document_ids"]
    logger.info(f"Document IDs: {document_ids}")
    assert len(document_ids) == 2

    # Query by document_ids using document/query interface
    query_document_ids = []
    for document_id in document_ids:
        query_document_ids.append(document_id["_id"])
    query_url = router_url + "/document/query"
    query_data = {
        "db_name": db_name,
        "space_name": space_name,
        "document_ids": query_document_ids,
        "fields": ["field_int", "field_long", "field_float", "field_double", "field_string", "field_vector"],
    }

    query_rs = requests.post(query_url, auth=(username, password), json=query_data)
    logger.info(f"Query response: {query_rs.json()}")
    assert query_rs.json()["code"] == 0
    
    documents = query_rs.json()["data"]["documents"]
    logger.info(f"Queried documents: {documents}")
    assert len(documents) == 2

    # Verify the document with specified _id
    doc_with_id = None
    doc_without_id = None
    for doc in documents:
        if doc.get("_id") == "test_id_001":
            doc_with_id = doc
        else:
            doc_without_id = doc

    # Check document with _id
    assert doc_with_id is not None, "Document with _id should exist"
    assert doc_with_id["_id"] == "test_id_001"
    assert doc_with_id["field_int"] == 100
    assert doc_with_id["field_long"] == 100
    assert doc_with_id["field_float"] == 100.0
    assert doc_with_id["field_double"] == 100.0
    assert doc_with_id["field_string"] == "test_with_id"
    # Check vector values
    assert doc_with_id["field_vector"] is not None
    assert len(doc_with_id["field_vector"]) == embedding_size

    # Check document without _id (should have auto-generated _id)
    assert doc_without_id is not None, "Document without _id should exist"
    assert doc_without_id["_id"] != ""
    assert doc_without_id["field_int"] == 200
    assert doc_without_id["field_long"] == 200
    assert doc_without_id["field_float"] == 200.0
    assert doc_without_id["field_double"] == 200.0
    assert doc_without_id["field_string"] == "test_without_id"
    # Check vector values
    assert doc_without_id["field_vector"] is not None
    assert len(doc_without_id["field_vector"]) == embedding_size

    # Cleanup
    destroy(router_url, db_name, space_name)


def update(total, bulk, full_field, xb):
    embedding_size = xb.shape[1]
    batch_size = 1
    if bulk:
        batch_size = 100
    k = 100
    if total == 0:
        total = xb.shape[0]
    total_batch = int(total / batch_size)
    with_id = True

    logger.info(
        "dataset num: %d, total_batch: %d, dimension: %d, search num: %d, topK: %d"
        % (total, total_batch, embedding_size, xq.shape[0], k)
    )

    properties = {}
    properties["fields"] = [
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
            "index": {
                "name": "gamma",
                "type": "FLAT",
                "params": {
                    "metric_type": "L2",
                },
            },
            "dimension": embedding_size,
            "store_type": "MemoryOnly",
            # "format": "normalization"
        },
    ]

    create_for_document_test(router_url, embedding_size, properties)

    for i in range(5):
        add(total_batch, batch_size, xb, with_id, full_field, i)

        add(total_batch, batch_size, xb, with_id, full_field, i, has_vector=False)

        query_interface(total_batch, batch_size, xb, full_field, i)

    destroy(router_url, db_name, space_name)


@pytest.mark.parametrize(
    ["bulk", "full_field"],
    [
        [True, True],
        [True, False],
        [False, True],
        [False, False],
    ],
)
def test_vearch_document_upsert_update(bulk: bool, full_field: bool):
    update(100, bulk, full_field, xb)


class TestDocumentUpsertBadCase:
    def setup_class(self):
        self.xb = xb

    # prepare for badcase
    def test_prepare_cluster_badcase(self):
        embedding_size = xb.shape[1]

        properties = {}
        properties["fields"] = [
            {"name": "field_int", "type": "integer"},
            {"name": "field_long", "type": "long"},
            {"name": "field_float", "type": "float"},
            {"name": "field_double", "type": "double"},
            {
                "name": "field_string",
                "type": "string",
                "index": {"name": "field_string", "type": "SCALAR"},
            },
            {"name": "field_string1", "type": "string"},
            {
                "name": "field_vector",
                "type": "vector",
                "index": {
                    "name": "gamma",
                    "type": "FLAT",
                    "params": {
                        "metric_type": "L2",
                    },
                },
                "dimension": embedding_size,
                "store_type": "MemoryOnly",
                # "format": "normalization"
            },
        ]

        create_for_document_test(router_url, embedding_size, properties)

    @pytest.mark.parametrize(
        ["index", "wrong_type"],
        [
            [0, "wrong_number_value"],
            [1, "wrong_str_value"],
            [2, "without_vector"],
            [3, "wrong_db"],
            [4, "wrong_space"],
            [5, "wrong_field"],
            [6, "empty_documents"],
            [7, "wrong_index_string_length"],
            [8, "wrong_string_length"],
            [9, "wrong_vector_type"],
            [10, "wrong_vector_feature_length"],
            [11, "wrong_vector_feature_type"],
            [12, "mismatch_field_type"],
            [13, "wrong partition id"],
            [14, "upsert_with_master_url"],
            [15, "wrong_url_path"],
        ],
    )
    def test_vearch_document_upsert_badcase(self, index, wrong_type):
        wrong_parameters = [False for i in range(16)]
        wrong_parameters[index] = True
        batch_size = 1
        total = 1
        if total == 0:
            total = xb.shape[0]
        total_batch = int(total / batch_size)
        add_error(total_batch, batch_size, self.xb, wrong_parameters)
        assert get_space_num() == 0

    @pytest.mark.parametrize(
        ["index", "wrong_type"],
        [
            [0, "params_both_wrong"],
            [1, "params_just_one_wrong"],
            [2, "params_just_one_wrong_with_bad_vector"],
        ],
    )
    def test_vearch_document_upsert_multiple_badcase(self, index: int, wrong_type: str):
        wrong_parameters = [False for i in range(3)]
        wrong_parameters[index] = True
        total_batch = 1
        batch_size = 2
        add_error(total_batch, batch_size, self.xb, wrong_parameters)

        assert get_space_num() == 0

    # destroy for badcase
    def test_destroy_cluster_badcase(self):
        destroy(router_url, db_name, space_name)


class TestDocumentUpsertDuplicate:
    def setup_class(self):
        self.xb = xb

    # prepare for badcase
    def test_prepare_cluster_duplicate(self):
        embedding_size = xb.shape[1]

        properties = {}
        properties["fields"] = [
            {"name": "field_int", "type": "integer"},
            {"name": "field_long", "type": "long"},
            {"name": "field_float", "type": "float"},
            {"name": "field_double", "type": "double"},
            {
                "name": "field_string",
                "type": "string",
                "index": {"name": "field_string", "type": "SCALAR"},
            },
            {"name": "field_string1", "type": "string"},
            {
                "name": "field_vector",
                "type": "vector",
                "index": {
                    "name": "gamma",
                    "type": "FLAT",
                    "params": {
                        "metric_type": "L2",
                    },
                },
                "dimension": embedding_size,
                "store_type": "MemoryOnly",
                # "format": "normalization"
            },
        ]

        create_for_document_test(router_url, embedding_size, properties)

    def test_vearch_document_upsert_duplicate(self):
        batch_size = 1
        total = 1
        url = router_url + "/document/upsert"
        data = {}
        data["db_name"] = db_name
        data["space_name"] = space_name
        data["documents"] = []

        for j in range(2):
            param_dict = {}
            param_dict["_id"] = "00001"
            param_dict["field_int"] = j
            param_dict["field_vector"] = xb[j].tolist()
            param_dict["field_long"] = param_dict["field_int"]
            param_dict["field_float"] = float(param_dict["field_int"])
            param_dict["field_double"] = float(param_dict["field_int"])
            param_dict["field_string"] = str(param_dict["field_int"])
            data["documents"].append(param_dict)

        for j in range(2, 3):
            param_dict = {}
            param_dict["_id"] = "0000" + str(j)
            param_dict["field_int"] = j
            param_dict["field_vector"] = xb[j].tolist()
            param_dict["field_long"] = param_dict["field_int"]
            param_dict["field_float"] = float(param_dict["field_int"])
            param_dict["field_double"] = float(param_dict["field_int"])
            param_dict["field_string"] = str(param_dict["field_int"])
            data["documents"].append(param_dict)

        rs = requests.post(url, auth=(username, password), json=data)
        logger.info(rs.json())
        assert rs.json()["code"] == 0
        assert len(rs.json()["data"]["document_ids"]) == 3
        assert rs.json()["data"]["total"] == 3
        assert get_space_num() == 2

    # destroy for badcase
    def test_destroy_cluster_duplicate(self):
        destroy(router_url, db_name, space_name)


class TestDocumentUpdateFail:
    def setup_class(self):
        self.xb = xb

    # prepare for badcase
    def test_prepare_cluster_update_fail(self):
        embedding_size = xb.shape[1]

        properties = {}
        properties["fields"] = [
            {"name": "field_int", "type": "integer"},
            {"name": "field_long", "type": "long"},
            {"name": "field_float", "type": "float"},
            {"name": "field_double", "type": "double"},
            {
                "name": "field_string",
                "type": "string",
                "index": {"name": "field_string", "type": "SCALAR"},
            },
            {"name": "field_string1", "type": "string"},
            {
                "name": "field_vector",
                "type": "vector",
                "index": {
                    "name": "gamma",
                    "type": "FLAT",
                    "params": {
                        "metric_type": "L2",
                    },
                },
                "dimension": embedding_size,
                "store_type": "MemoryOnly",
                # "format": "normalization"
            },
        ]

        create_for_document_test(router_url, embedding_size, properties)

    def test_vearch_document_update_fail(self):
        batch_size = 1
        total = 1
        url = router_url + "/document/upsert"
        data = {}
        data["db_name"] = db_name
        data["space_name"] = space_name
        data["documents"] = []

        # upsert and update
        for j in range(2):
            param_dict = {}
            param_dict["_id"] = "00001"
            param_dict["field_int"] = j
            param_dict["field_vector"] = xb[j].tolist()
            param_dict["field_long"] = param_dict["field_int"]
            param_dict["field_float"] = float(param_dict["field_int"])
            param_dict["field_double"] = float(param_dict["field_int"])
            param_dict["field_string"] = str(param_dict["field_int"])
            data["documents"].append(param_dict)

        rs = requests.post(url, auth=(username, password), json=data)
        logger.info(rs.json())
        assert rs.json()["code"] == 0
        assert len(rs.json()["data"]["document_ids"]) == 2
        assert rs.json()["data"]["total"] == 2
        assert get_space_num() == 1

        # update with vector
        data["documents"] = []
        for j in range(1):
            param_dict = {}
            param_dict["_id"] = "00001"
            param_dict["field_int"] = j
            param_dict["field_vector"] = xb[j].tolist()
            param_dict["field_long"] = param_dict["field_int"]
            param_dict["field_float"] = float(param_dict["field_int"])
            param_dict["field_double"] = float(param_dict["field_int"])
            param_dict["field_string"] = str(param_dict["field_int"])
            data["documents"].append(param_dict)

        rs = requests.post(url, auth=(username, password), json=data)
        logger.info(rs.json())
        assert rs.json()["code"] == 0
        assert len(rs.json()["data"]["document_ids"]) == 1
        assert rs.json()["data"]["total"] == 1
        assert get_space_num() == 1

        # update without vector
        data["documents"] = []
        for j in range(1):
            param_dict = {}
            param_dict["_id"] = "00001"
            param_dict["field_int"] = j
            param_dict["field_long"] = param_dict["field_int"]
            param_dict["field_float"] = float(param_dict["field_int"])
            param_dict["field_double"] = float(param_dict["field_int"])
            param_dict["field_string"] = str(param_dict["field_int"])
            data["documents"].append(param_dict)

        rs = requests.post(url, auth=(username, password), json=data)
        logger.info(rs.json())
        assert rs.json()["code"] == 0
        assert len(rs.json()["data"]["document_ids"]) == 1
        assert rs.json()["data"]["total"] == 1
        assert get_space_num() == 1

        # update without numeric field
        data["documents"] = []
        for j in range(1):
            param_dict = {}
            param_dict["_id"] = "00001"
            param_dict["field_int"] = j
            param_dict["field_vector"] = xb[j].tolist()
            param_dict["field_float"] = float(param_dict["field_int"])
            param_dict["field_double"] = float(param_dict["field_int"])
            param_dict["field_string"] = str(param_dict["field_int"])
            data["documents"].append(param_dict)

        rs = requests.post(url, auth=(username, password), json=data)
        logger.info(rs.json())
        assert rs.json()["code"] == 0
        assert len(rs.json()["data"]["document_ids"]) == 1
        assert rs.json()["data"]["total"] == 1
        assert get_space_num() == 1

        url = router_url + "/document/delete"
        data["documents"] = []
        data["document_ids"] = ["00001"]
        rs = requests.post(url, auth=(username, password), json=data)
        logger.info(rs.json())
        assert rs.json()["code"] == 0
        assert len(rs.json()["data"]["document_ids"]) == 1
        assert rs.json()["data"]["total"] == 1
        assert get_space_num() == 0

        # update fail
        url = router_url + "/document/upsert"
        data["documents"] = []
        for j in range(1):
            param_dict = {}
            param_dict["_id"] = "00001"
            param_dict["field_int"] = j
            # param_dict["field_vector"] = xb[j].tolist()
            param_dict["field_long"] = param_dict["field_int"]
            param_dict["field_float"] = float(param_dict["field_int"])
            param_dict["field_double"] = float(param_dict["field_int"])
            param_dict["field_string"] = str(param_dict["field_int"])
            data["documents"].append(param_dict)

        rs = requests.post(url, auth=(username, password), json=data)
        logger.info(rs.json())
        assert rs.json()["code"] == 0
        assert len(rs.json()["data"]["document_ids"]) == 1
        assert rs.json()["data"]["total"] == 0
        assert get_space_num() == 0

        # update one fail
        url = router_url + "/document/upsert"
        data["documents"] = []
        for j in range(2):
            param_dict = {}
            param_dict["_id"] = "0000" + str(j)
            param_dict["field_int"] = j
            if j == 1:
                param_dict["field_vector"] = xb[j].tolist()
            param_dict["field_long"] = param_dict["field_int"]
            param_dict["field_float"] = float(param_dict["field_int"])
            param_dict["field_double"] = float(param_dict["field_int"])
            param_dict["field_string"] = str(param_dict["field_int"])
            data["documents"].append(param_dict)

        rs = requests.post(url, auth=(username, password), json=data)
        logger.info(rs.json())
        assert rs.json()["code"] == 0
        assert len(rs.json()["data"]["document_ids"]) == 2
        assert rs.json()["data"]["total"] == 1
        assert get_space_num() == 1

    # destroy for badcase
    def test_destroy_cluster_update_fail(self):
        destroy(router_url, db_name, space_name)
