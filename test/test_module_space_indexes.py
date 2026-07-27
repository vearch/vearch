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
import threading
from utils.vearch_utils import *
from utils.data_utils import *

__description__ = """ test case for module space indexes"""

sift10k = DatasetSift10K()
xb = sift10k.get_database()
xq = sift10k.get_queries()
gt = sift10k.get_groundtruth()

class TestSpaceIndexes:
    def setup_class(self):
        pass

    def test_prepare_db(self):
        response = create_db(router_url, db_name)
        assert response.json()["code"] == 0

    @pytest.mark.parametrize(
        ["index_type"],
        [["FLAT"]],
    )
    def test_vearch_space_create_with_indexes(self, index_type):
        embedding_size = 128
        space_config = {
            "name": space_name,
            "partition_num": 1,
            "replica_num": 1,
            "fields": [
                {"name": "field_string", "type": "keyword"},
                {"name": "field_int", "type": "integer"},
                {
                    "name": "field_float",
                    "type": "float",
                    "index": {
                        "name": "field_float",
                        "type": "SCALAR",
                    },
                },
                {
                    "name": "field_double",
                    "type": "double",
                    "array": True,
                    "index": {
                        "name": "field_double",
                        "type": "SCALAR",
                    },
                },
                {
                    "name": "field_long",
                    "type": "long",
                    "index": {
                        "name": "field_long_index",
                        "type": "SCALAR",
                    },
                },
                {
                    "name": "field_vector",
                    "type": "vector",
                    "dimension": embedding_size,
                    "index": {"name": "gamma", "type": index_type},
                },
            ],
            "indexes": [
                {
                    "name": "idx_string",
                    "type": "SCALAR",
                    "field_name": "field_string",
                },
                {
                    "name": "idx_int",
                    "type": "SCALAR",
                    "field_name": "field_int",
                },
                {
                    "name": "idx_composite",
                    "type": "COMPOSITE",
                    "field_names": ["field_string", "field_int", "field_float"],
                }
            ],
        }

        response = create_space(router_url, db_name, space_config)
        logger.info(response.json())
        assert response.json()["code"] == 0

        response = describe_space(router_url, db_name, space_name)
        logger.info(response.json())
        assert response.json()["code"] == 0

        total = 10
        add(total, 1, xb, False, True)
        time.sleep(3)

        search_url = router_url + "/document/search"
        data = {}
        data["db_name"] = db_name
        data["space_name"] = space_name
        data["vectors"] = []
        vector_info = {
            "field": "field_vector",
            "feature": xq[:1].flatten().tolist(),
        }
        data["vectors"].append(vector_info)
        json_str = json.dumps(data)
        response = requests.post(search_url, auth=(username, password), data=json_str)
        logger.info(response.json())
        assert len(response.json()["data"]["documents"]) == 1
        assert len(response.json()["data"]["documents"][0]) == total

        response = drop_space(router_url, db_name, space_name)
        assert response.json()["code"] == 0

    @pytest.mark.parametrize(
        ["wrong_index", "wrong_type"],
        [
            [0, "not supported scalar index type in fields"],
            [1, "not supported scalar index type in indexes"],
            [2, "non exist field name in single scalar index"],
            [3, "non exist field name in composite index"],
            [4, "duplicate index in fields"],
            [5, "duplicate index in indexes"],
            [6, "duplicate index in fields and indexes"],
            [7, "composite index can not set in fields"],
            [8, "composite index fieldNames is same"],
            [9, "vector field scalar index in fields"],
            [10, "scalar field vector index in fields"],
            [11, "vector field scalar index in indexes"],
            [12, "scalar field vector index in indexes"],
        ],
    )
    def test_vearch_space_create_indexes_badcase(self, wrong_index, wrong_type):
        embedding_size = 128
        training_threshold = 1
        metric_type = "L2"
        space_config = {
            "name": space_name,
            "partition_num": 1,
            "replica_num": 1,
            "fields": [
                {"name": "field_string", "type": "keyword"},
                {"name": "field_int", "type": "integer"},
                {
                    "name": "field_float",
                    "type": "float",
                    "index": {"name": "field_float", "type": "SCALAR"},
                },
                {
                    "name": "field_string_array",
                    "type": "string",
                    "index": {"name": "field_string_array", "type": "SCALAR"},
                },
                {
                    "name": "field_int_index",
                    "type": "integer",
                    "index": {"name": "field_int_index", "type": "SCALAR"},
                },
                {
                    "name": "field_vector_normal",
                    "type": "vector",
                    "dimension": embedding_size,
                    "format": "normalization",
                    "index": {
                        "name": "gamma",
                        "type": "FLAT",
                        "params": {
                            "metric_type": metric_type,
                            "training_threshold": training_threshold,
                        },
                    },
                },
            ],
            "indexes": [{
                "name": "idx_string",
                "type": "SCALAR",
                "field_name": "field_string",
            }, {
                "name": "idx_int",
                "type": "SCALAR",
                "field_name": "field_int",
            }, {
                "name": "idx_composite",
                "type": "COMPOSITE",
                "field_names": ["field_string", "field_int", "field_float"],
            }]
        }
        if wrong_index == 0:
            space_config["fields"][2]["index"]["type"] = "WRONG_TYPE"
        if wrong_index == 1:
            space_config["indexes"][2]["type"] = "WRONG_TYPE"
        if wrong_index == 2:
            space_config["indexes"][1]["field_name"] = "wrong_field"
        if wrong_index == 3:
            space_config["indexes"][2]["field_names"] = ["wrong_field", "field_int", "field_float"]
        if wrong_index == 4:
            space_config["fields"][2]["index"]["name"] = "idx_string"
            space_config["fields"][2]["index"]["name"] = "idx_string"
        if wrong_index == 5:
            space_config["indexes"][0]["name"] = "idx_string"
            space_config["indexes"][1]["name"] = "idx_string"
        if wrong_index == 6:
            space_config["fields"][2]["index"]["name"] = "idx_string"
            space_config["indexes"][1]["name"] = "idx_string"
        if wrong_index == 7:
            space_config["fields"][2]["index"]["type"] = "COMPOSITE"
        if wrong_index == 8:
            space_config["indexes"][2]["field_names"] = ["field_string", "field_int", "field_string"]
        # wrong_index 9: vector field with scalar index in fields
        if wrong_index == 9:
            space_config["fields"][5]["index"]["type"] = "SCALAR"
        # wrong_index 10: scalar field with vector index in fields
        if wrong_index == 10:
            space_config["fields"][2]["index"]["type"] = "HNSW"
        # wrong_index 11: vector field scalar index in indexes (references vector field)
        if wrong_index == 11:
            space_config["indexes"].append({
                "name": "idx_vector_scalar",
                "type": "SCALAR",
                "field_name": "field_vector_normal",
            })
        # wrong_index 12: scalar field vector index in indexes (references scalar field)
        if wrong_index == 12:
            space_config["indexes"].append({
                "name": "idx_string_hnsw",
                "type": "HNSW",
                "field_name": "field_string",
            })
        response = create_space(router_url, db_name, space_config)
        logger.info(response.json())
        assert response.json()["code"] != 0

    def test_destroy_space(self):
        response = drop_space(router_url, db_name, space_name)
        assert response.json()["code"] != 0

    def test_destroy_db(self):
        drop_db(router_url, db_name)


class TestSpaceIndexesList:
    def setup_class(self):
        pass

    def test_prepare_db(self):
        response = create_db(router_url, db_name)
        assert response.json()["code"] == 0

    def test_prepare_space(self):
        embedding_size = 128
        space_config = {
            "name": space_name,
            "partition_num": 1,
            "replica_num": 1,
            "fields": [
                {"name": "field_string", "type": "keyword"},
                {"name": "field_int", "type": "integer"},
                {
                    "name": "field_float",
                    "type": "float",
                    "index": {"name": "field_float", "type": "SCALAR"},
                },
                {
                    "name": "field_vector",
                    "type": "vector",
                    "dimension": embedding_size,
                    "index": {"name": "gamma", "type": "FLAT"},
                },
            ],
            "indexes": [
                {
                    "name": "idx_string",
                    "type": "SCALAR",
                    "field_name": "field_string",
                },
                {
                    "name": "idx_int",
                    "type": "SCALAR",
                    "field_name": "field_int",
                },
                {
                    "name": "idx_composite",
                    "type": "COMPOSITE",
                    "field_names": ["field_string", "field_int", "field_float"],
                },
            ],
        }
        response = create_space(router_url, db_name, space_config)
        assert response.json()["code"] == 0

    def test_list_space_indexes(self):
        response = list_space_indexes(router_url, db_name, space_name)
        logger.info(response.json())
        assert response.json()["code"] == 0

        data = response.json()["data"]
        assert data["db_name"] == db_name
        assert data["space_name"] == space_name
        # The 3 explicit `indexes` plus the inline one defined under field_float
        index_names = {idx["name"] for idx in data["indexes"]}
        assert "idx_string" in index_names
        assert "idx_int" in index_names
        assert "idx_composite" in index_names
        assert "field_float" in index_names

    def test_list_space_indexes_not_exist_space(self):
        response = list_space_indexes(router_url, db_name, "not_exist_space")
        logger.info(response.json())
        assert response.json()["code"] != 0

    def test_list_space_indexes_not_exist_db(self):
        response = list_space_indexes(router_url, "not_exist_db", space_name)
        logger.info(response.json())
        assert response.json()["code"] != 0

    def test_destroy_space(self):
        response = drop_space(router_url, db_name, space_name)
        assert response.json()["code"] == 0

    def test_destroy_db(self):
        drop_db(router_url, db_name)


class TestSpaceIndexesAdd:
    def setup_class(self):
        pass

    def test_prepare_db(self):
        response = create_db(router_url, db_name)
        assert response.json()["code"] == 0

    def test_prepare_space(self):
        embedding_size = 128
        space_config = {
            "name": space_name,
            "partition_num": 1,
            "replica_num": 1,
            "fields": [
                {"name": "field_string", "type": "keyword"},
                {"name": "field_int", "type": "integer"},
                {"name": "field_float", "type": "float"},
                {
                    "name": "field_vector",
                    "type": "vector",
                    "dimension": embedding_size,
                    "index": {"name": "gamma", "type": "FLAT"},
                },
            ],
            "indexes": [
                {
                    "name": "idx_string",
                    "type": "SCALAR",
                    "field_name": "field_string",
                },
            ],
        }
        response = create_space(router_url, db_name, space_config)
        assert response.json()["code"] == 0

    def test_add_duplicate_index_name_in_params(self):
        new_indexes = [
            {
                "name": "idx_string",
                "type": "SCALAR",
                "field_name": "field_string",
            },
            {
                "name": "idx_string",
                "type": "SCALAR",
                "field_name": "field_float",
            },
        ]
        response = add_space_indexes(router_url, db_name, space_name, new_indexes)
        logger.info(response.json())
        assert response.json()["code"] != 0

    def test_query_with_index_before_add(self):
        query_url = router_url + "/document/query"
        data = {
            "db_name": db_name,
            "space_name": space_name,
            "vectors": [{
                "field": "field_vector",
                "feature": xq[0].tolist(),
            }],
            "filters": {
                "operator": "AND",
                "conditions": [
                    {
                        "field": "field_int",
                        "operator": ">=",
                        "value": 10
                    },
                    {
                        "field": "field_int",
                        "operator": "<=",
                        "value": 20
                    }
                ]
            }
        }
        response = requests.post(query_url, auth=(username, password), json=data)
        logger.info(response.json())
        assert response.json()["code"] != 0

    def test_add_single_index(self):
        new_indexes = [
            {
                "name": "idx_int",
                "type": "SCALAR",
                "field_name": "field_int",
            }
        ]
        response = add_space_indexes(router_url, db_name, space_name, new_indexes)
        logger.info(response.json())
        assert response.json()["code"] == 0

        # verify it shows up via list API
        list_resp = list_space_indexes(router_url, db_name, space_name)
        names = {idx["name"] for idx in list_resp.json()["data"]["indexes"]}
        assert "idx_int" in names

    def test_query_with_index_after_add(self):
        query_url = router_url + "/document/query"
        data = {
            "db_name": db_name,
            "space_name": space_name,
            "vectors": [{
                "field": "field_vector",
                "feature": xq[0].tolist(),
            }],
            "filters": {
                "operator": "AND",
                "conditions": [
                    {
                        "field": "field_int",
                        "operator": ">=",
                        "value": 10
                    },
                    {
                        "field": "field_int",
                        "operator": "<=",
                        "value": 20
                    }
                ]
            }
        }
        response = requests.post(query_url, auth=(username, password), json=data)
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_add_multiple_indexes(self):
        new_indexes = [
            {
                "name": "idx_float",
                "type": "SCALAR",
                "field_name": "field_float",
            },
            {
                "name": "idx_composite",
                "type": "COMPOSITE",
                "field_names": ["field_string", "field_int", "field_float"],
            },
        ]
        response = add_space_indexes(router_url, db_name, space_name, new_indexes)
        logger.info(response.json())
        assert response.json()["code"] == 0

        list_resp = list_space_indexes(router_url, db_name, space_name)
        names = {idx["name"] for idx in list_resp.json()["data"]["indexes"]}
        assert "idx_float" in names
        assert "idx_composite" in names

    def test_add_duplicate_index_name(self):
        new_indexes = [
            {
                "name": "idx_string",
                "type": "SCALAR",
                "field_name": "field_string",
            }
        ]
        response = add_space_indexes(router_url, db_name, space_name, new_indexes)
        logger.info(response.json())
        assert response.json()["code"] != 0

    def test_add_with_field_already_indexed(self):
        new_indexes = [
            {
                "name": "idx_string_new",
                "type": "SCALAR",
                "field_name": "field_string",
            }
        ]
        response = add_space_indexes(router_url, db_name, space_name, new_indexes)
        logger.info(response.json())
        assert response.json()["code"] != 0

    def test_add_index_with_non_exist_field(self):
        new_indexes = [
            {
                "name": "idx_not_exist",
                "type": "SCALAR",
                "field_name": "not_exist_field",
            }
        ]
        response = add_space_indexes(router_url, db_name, space_name, new_indexes)
        logger.info(response.json())
        assert response.json()["code"] != 0

    def test_add_empty_indexes(self):
        response = add_space_indexes(router_url, db_name, space_name, [])
        logger.info(response.json())
        assert response.json()["code"] != 0

    def test_add_index_to_not_exist_space(self):
        new_indexes = [
            {
                "name": "idx_string_2",
                "type": "SCALAR",
                "field_name": "field_string",
            }
        ]
        response = add_space_indexes(
            router_url, db_name, "not_exist_space", new_indexes
        )
        logger.info(response.json())
        assert response.json()["code"] != 0

    def test_add_index_to_not_exist_type(self):
        new_indexes = [
            {
                "name": "idx_string_2",
                "type": "not_exist_type",
                "field_name": "field_string",
            }
        ]
        response = add_space_indexes(
            router_url, db_name, space_name, new_indexes
        )
        logger.info(response.json())
        assert response.json()["code"] != 0

    def test_destroy_space(self):
        response = drop_space(router_url, db_name, space_name)
        assert response.json()["code"] == 0

    def test_destroy_db(self):
        drop_db(router_url, db_name)


class TestSpaceIndexesDelete:
    def setup_class(self):
        pass

    def test_prepare_db(self):
        response = create_db(router_url, db_name)
        assert response.json()["code"] == 0

    def test_prepare_space(self):
        embedding_size = 128
        space_config = {
            "name": space_name,
            "partition_num": 1,
            "replica_num": 1,
            "fields": [
                {"name": "field_string", "type": "keyword"},
                {"name": "field_int", "type": "integer"},
                {"name": "field_float", "type": "float"},
                {
                    "name": "field_vector",
                    "type": "vector",
                    "dimension": embedding_size,
                    "index": {"name": "gamma", "type": "FLAT"},
                },
            ],
            "indexes": [
                {
                    "name": "idx_string",
                    "type": "SCALAR",
                    "field_name": "field_string",
                },
                {
                    "name": "idx_int",
                    "type": "SCALAR",
                    "field_name": "field_int",
                },
                {
                    "name": "idx_composite",
                    "type": "COMPOSITE",
                    "field_names": ["field_string", "field_float"],
                },
            ],
        }
        response = create_space(router_url, db_name, space_config)
        assert response.json()["code"] == 0

    def test_query_with_index_before_delete(self):
        query_url = router_url + "/document/query"
        data = {
            "db_name": db_name,
            "space_name": space_name,
            "vectors": [{
                "field": "field_vector",
                "feature": xq[0].tolist(),
            }],
            "filters": {
                "operator": "AND",
                "conditions": [
                    {
                        "field": "field_int",
                        "operator": ">=",
                        "value": 10
                    },
                    {
                        "field": "field_int",
                        "operator": "<=",
                        "value": 20
                    }
                ]
            }
        }
        response = requests.post(query_url, auth=(username, password), json=data)
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_delete_existing_index(self):
        response = delete_space_index(router_url, db_name, space_name, "idx_int")
        logger.info(response.json())
        assert response.json()["code"] == 0

        list_resp = list_space_indexes(router_url, db_name, space_name)
        names = {idx["name"] for idx in list_resp.json()["data"]["indexes"]}
        assert "idx_int" not in names
        # other indexes must remain
        assert "idx_string" in names
        assert "idx_composite" in names

    def test_query_with_index_after_delete(self):
        query_url = router_url + "/document/query"
        data = {
            "db_name": db_name,
            "space_name": space_name,
            "vectors": [{
                "field": "field_vector",
                "feature": xq[0].tolist(),
            }],
            "filters": {
                "operator": "AND",
                "conditions": [
                    {
                        "field": "field_int",
                        "operator": ">=",
                        "value": 10
                    },
                    {
                        "field": "field_int",
                        "operator": "<=",
                        "value": 20
                    }
                ]
            }
        }
        response = requests.post(query_url, auth=(username, password), json=data)
        logger.info(response.json())
        assert response.json()["code"] != 0

    def test_delete_composite_index(self):
        response = delete_space_index(
            router_url, db_name, space_name, "idx_composite"
        )
        logger.info(response.json())
        assert response.json()["code"] == 0

        list_resp = list_space_indexes(router_url, db_name, space_name)
        names = {idx["name"] for idx in list_resp.json()["data"]["indexes"]}
        assert "idx_composite" not in names

    def test_delete_not_exist_index(self):
        response = delete_space_index(
            router_url, db_name, space_name, "not_exist_index"
        )
        logger.info(response.json())
        assert response.json()["code"] != 0

    def test_delete_already_deleted_index(self):
        # idx_int has been removed in test_delete_existing_index, deleting again should fail
        response = delete_space_index(router_url, db_name, space_name, "idx_int")
        logger.info(response.json())
        assert response.json()["code"] != 0

    def test_delete_index_in_not_exist_space(self):
        response = delete_space_index(
            router_url, db_name, "not_exist_space", "idx_string"
        )
        logger.info(response.json())
        assert response.json()["code"] != 0

    def test_add_back_after_delete(self):
        # Make sure deleted indexes can be added back with the same name
        new_indexes = [
            {
                "name": "idx_int",
                "type": "SCALAR",
                "field_name": "field_int",
            }
        ]
        response = add_space_indexes(router_url, db_name, space_name, new_indexes)
        logger.info(response.json())
        assert response.json()["code"] == 0

        list_resp = list_space_indexes(router_url, db_name, space_name)
        names = {idx["name"] for idx in list_resp.json()["data"]["indexes"]}
        assert "idx_int" in names

    def test_query_with_index_after_add_back(self):
        query_url = router_url + "/document/query"
        data = {
            "db_name": db_name,
            "space_name": space_name,
            "vectors": [{
                "field": "field_vector",
                "feature": xq[0].tolist(),
            }],
            "filters": {
                "operator": "AND",
                "conditions": [
                    {
                        "field": "field_int",
                        "operator": ">=",
                        "value": 10
                    },
                    {
                        "field": "field_int",
                        "operator": "<=",
                        "value": 20
                    }
                ]
            }
        }
        response = requests.post(query_url, auth=(username, password), json=data)
        logger.info(response.json())
        assert response.json()["code"] == 0

    def test_destroy_space(self):
        response = drop_space(router_url, db_name, space_name)
        assert response.json()["code"] == 0

    def test_destroy_db(self):
        drop_db(router_url, db_name)


class TestDeleteFieldInlineIndex:
    """Regression: deleting an index that was declared INLINE on a field (fields[]
    .index) must also clear the field's inline definition, not just the
    space.Indexes list.

    A single-field index defined inside the field is mirrored into both the
    space indexes list and the field schema / space_properties. If delete only
    removes the list entry, the field schema still carries it: the metadata is
    inconsistent, and once the indexes list is empty the PS rebuild falls back
    to the field-level definition and the index resurrects on reload. This test
    creates a space whose only index is inline on a scalar field, deletes it,
    and asserts it stays gone (list empty) and does not reappear.
    """

    db_name = "ts_db_inline_del"
    space_name = "ts_space_inline_del"
    dim = 16

    def test_prepare_db(self):
        response = create_db(router_url, self.db_name)
        assert response.json()["code"] == 0

    def test_prepare_space_inline_index(self):
        # field_int carries its index INLINE (inside the field), not via the
        # top-level "indexes" list. It is the only scalar index in the space.
        space_config = {
            "name": self.space_name,
            "partition_num": 1,
            "replica_num": 1,
            "fields": [
                {"name": "field_int", "type": "integer",
                 "index": {"name": "idx_int_inline", "type": "SCALAR"}},
                {"name": "field_vector", "type": "vector", "dimension": self.dim,
                 "index": {"name": "gamma", "type": "FLAT"}},
            ],
        }
        resp = create_space(router_url, self.db_name, space_config)
        assert resp.json()["code"] == 0, resp.json()

    def _index_names(self):
        resp = list_space_indexes(router_url, self.db_name, self.space_name)
        body = resp.json()
        assert body.get("code", -1) == 0, body
        return {idx["name"] for idx in body.get("data", {}).get("indexes", []) or []}

    def _schema_index_names(self):
        # describe exposes the merged index list under data.schema.indexes.
        resp = describe_space(router_url, self.db_name, self.space_name)
        body = resp.json()
        assert body.get("code", -1) == 0, body
        schema = body.get("data", {}).get("schema", {}) or {}
        return {idx["name"] for idx in schema.get("indexes", []) or []}

    def test_inline_index_present_before_delete(self):
        assert "idx_int_inline" in self._index_names(), \
            "inline field index should be listed before delete"

    def test_delete_inline_index(self):
        resp = delete_space_index(router_url, self.db_name, self.space_name,
                                  "idx_int_inline")
        assert resp.json()["code"] == 0, resp.json()
        # Gone from the indexes list.
        assert "idx_int_inline" not in self._index_names(), \
            "inline field index still listed after delete"

    def test_inline_index_does_not_resurrect(self):
        # Re-read metadata a few times: the removed inline index must not
        # reappear (it would if the field's inline definition were left behind
        # and the empty-list fallback re-derived it).
        for _ in range(3):
            assert "idx_int_inline" not in self._index_names(), \
                "removed inline index reappeared in indexes list"
            assert "idx_int_inline" not in self._schema_index_names(), \
                "removed inline index reappeared in schema"
            time.sleep(1)

    def test_destroy_space(self):
        response = drop_space(router_url, self.db_name, self.space_name)
        assert response.json()["code"] == 0

    def test_destroy_db(self):
        drop_db(router_url, self.db_name)


class TestRemoveOneVectorFieldKeepsOthers:
    """
    Regression for issue #5: removing one vector field's index must not break
    other vector fields' indexes. Vector index types/params are now keyed
    per-field (field_index_params_: field -> type -> params), so
    RemoveVectorIndex(field) erases only that field's entry and its
    vector_indexes_ objects, leaving other fields untouched. (Previously two
    parallel space-level lists were positionally paired and a value-scan
    removal could corrupt another field's type.)

    This test creates a space with two vector fields, deletes the index of one,
    and verifies the other still answers vector queries.
    """

    db_name = "ts_db_remove_vec"
    space_name = "ts_space_remove_vec"
    dim = 16

    def test_prepare_db(self):
        response = create_db(router_url, self.db_name)
        assert response.json()["code"] == 0

    def test_prepare_space_two_vector_fields(self):
        space_config = {
            "name": self.space_name,
            "partition_num": 1,
            "replica_num": 1,
            "fields": [
                {"name": "v1", "type": "vector", "dimension": self.dim,
                 "index": {"name": "idx_v1", "type": "FLAT"}},
                {"name": "v2", "type": "vector", "dimension": self.dim,
                 "index": {"name": "idx_v2", "type": "FLAT"}},
            ],
        }
        resp = create_space(router_url, self.db_name, space_config)
        # Some Vearch builds reject multi-vector spaces; skip cleanly if so.
        if resp.json().get("code", -1) != 0:
            pytest.skip(f"multi-vector space not supported: {resp.json()}")

    def _upsert(self, n: int):
        url = router_url + "/document/upsert"
        docs = []
        for i in range(n):
            docs.append({
                "_id": str(i),
                "v1": [random.random() for _ in range(self.dim)],
                "v2": [random.random() for _ in range(self.dim)],
            })
        resp = requests.post(url, auth=(username, password), json={
            "db_name": self.db_name,
            "space_name": self.space_name,
            "documents": docs,
        })
        assert resp.json()["code"] == 0, resp.json()

    def _vector_search(self, field: str):
        url = router_url + "/document/search"
        data = {
            "db_name": self.db_name,
            "space_name": self.space_name,
            "vectors": [{
                "field": field,
                "feature": [random.random() for _ in range(self.dim)],
            }],
            "limit": 5,
        }
        return requests.post(url, auth=(username, password), json=data)

    def test_remove_one_keeps_other(self):
        self._upsert(50)
        time.sleep(1)

        # Sanity: both fields searchable before removal.
        for f in ("v1", "v2"):
            r = self._vector_search(f)
            assert r.status_code == 200 and r.json().get("code", -1) == 0, \
                f"pre-removal search on {f} failed: {r.text}"

        # Remove v1's index.
        resp = delete_space_index(router_url, self.db_name, self.space_name, "idx_v1")
        assert resp.json()["code"] == 0, resp.json()
        time.sleep(2)

        # v2 must still be searchable. This is what regressed before the fix:
        # the corrupted index_types_ list caused all vector searches to fail
        # or return no results.
        r2 = self._vector_search("v2")
        assert r2.status_code == 200, r2.text
        body = r2.json()
        assert body.get("code", -1) == 0, f"v2 search broken after removing v1 index: {body}"
        hits = body.get("data", {}).get("documents", [[]])
        assert len(hits) > 0 and len(hits[0]) > 0, \
            f"v2 returned no hits after v1 index removal: {body}"

        logger.info("Remove one vector field keeps others: PASSED")

    def test_destroy_space(self):
        response = drop_space(router_url, self.db_name, self.space_name)
        assert response.json()["code"] == 0

    def test_destroy_db(self):
        drop_db(router_url, self.db_name)


class TestRemoveOneVectorIndexKeepsOthersLive:
    """Regression: removing one vector field's index must not suspend the
    surviving field's incremental indexing, nor flip the space-global
    index_status_ to UNINDEXED.

    RemoveFieldIndexTask used to call StopIndexingThread("field removal") and set
    index_status_ = UNINDEXED when dropping a vector index. But that indexing
    thread is shared by ALL vector fields, and index_status_ is a space-global
    "vectors are trained" flag. Dropping one field therefore (a) stopped the
    other field's incremental maintenance until the next write happened to
    restart the thread, and (b) forced subsequent normal searches to return
    Status::IndexNotTrained once max_docid_ crossed brute_force_search_threshold
    (100). The fix removes both: vector_indexes_mutex_ already makes the erase
    safe, so the thread keeps running and the remaining trained index keeps
    serving real (non-brute) queries.

    Uses HNSW (an incremental index, unlike FLAT) and seeds >100 docs so the
    index_status_ gate is actually exercised — this is what makes the test able
    to distinguish the buggy behavior from the fix."""

    db_name = "ts_db_remove_vec_live"
    space_name = "ts_space_remove_vec_live"
    dim = 16
    seed_count = 200  # > brute_force_search_threshold (100)

    def test_prepare_db(self):
        response = create_db(router_url, self.db_name)
        assert response.json()["code"] == 0

    def test_prepare_space_two_vector_fields(self):
        hnsw = {
            "nlinks": 16,
            "efConstruction": 60,
            "efSearch": 32,
            "metric_type": "L2",
        }
        space_config = {
            "name": self.space_name,
            "partition_num": 1,
            "replica_num": 1,
            "fields": [
                {"name": "v1", "type": "vector", "dimension": self.dim,
                 "index": {"name": "idx_v1", "type": "HNSW", "params": hnsw}},
                {"name": "v2", "type": "vector", "dimension": self.dim,
                 "index": {"name": "idx_v2", "type": "HNSW", "params": hnsw}},
            ],
        }
        resp = create_space(router_url, self.db_name, space_config)
        # Some Vearch builds reject multi-vector spaces; skip cleanly if so.
        if resp.json().get("code", -1) != 0:
            pytest.skip(f"multi-vector space not supported: {resp.json()}")

    def _upsert(self, start: int, count: int):
        url = router_url + "/document/upsert"
        docs = []
        for i in range(start, start + count):
            docs.append({
                "_id": str(i),
                "v1": [random.random() for _ in range(self.dim)],
                "v2": [random.random() for _ in range(self.dim)],
            })
        resp = requests.post(url, auth=(username, password), json={
            "db_name": self.db_name,
            "space_name": self.space_name,
            "documents": docs,
        })
        assert resp.json()["code"] == 0, resp.json()

    def _normal_search(self, field: str):
        # brute_force_search omitted (default 0) so the query goes through the
        # real index and hits the index_status_ gate — a UNINDEXED flag would
        # make this return IndexNotTrained instead of code 0.
        url = router_url + "/document/search"
        data = {
            "db_name": self.db_name,
            "space_name": self.space_name,
            "vectors": [{
                "field": field,
                "feature": [random.random() for _ in range(self.dim)],
            }],
            "limit": 5,
        }
        return requests.post(url, auth=(username, password), json=data)

    def test_seed_and_index(self):
        self._upsert(0, self.seed_count)
        time.sleep(3)  # let the shared indexing thread train both fields
        for f in ("v1", "v2"):
            r = self._normal_search(f)
            assert r.status_code == 200 and r.json().get("code", -1) == 0, \
                f"pre-removal normal search on {f} failed: {r.text}"

    def test_remove_v1_then_v2_ingests_and_serves(self):
        # Drop v1's index. Before the fix this stopped the shared thread and set
        # index_status_ = UNINDEXED.
        resp = delete_space_index(router_url, self.db_name, self.space_name, "idx_v1")
        assert resp.json()["code"] == 0, resp.json()
        time.sleep(2)

        # Write NEW docs after removal. If the shared indexing thread stayed
        # stopped, these never get added to v2's index.
        self._upsert(self.seed_count, self.seed_count)
        time.sleep(3)

        # v2 must still answer a NORMAL (non-brute) search with code 0. With the
        # buggy UNINDEXED flag and max_docid_ > 100 this returned IndexNotTrained.
        r = self._normal_search("v2")
        assert r.status_code == 200, r.text
        body = r.json()
        assert body.get("code", -1) == 0, \
            f"v2 normal search broken after removing v1 index: {body}"
        hits = body.get("data", {}).get("documents", [[]])
        assert len(hits) > 0 and len(hits[0]) > 0, \
            f"v2 returned no hits after v1 index removal + new writes: {body}"

        logger.info("Remove one vector index keeps others live: PASSED")

    def test_destroy_space(self):
        response = drop_space(router_url, self.db_name, self.space_name)
        assert response.json()["code"] == 0

    def test_destroy_db(self):
        drop_db(router_url, self.db_name)


class TestCompositeIndexLifecycle:
    """
    End-to-end add/delete lifecycle for a COMPOSITE scalar index.

    Covers:
      - Dynamically adding a composite index over fields that already have
        single-field scalar indexes (verifies AddFieldIndexThread's
        composite path: two-phase backfill, then InsertIndex publish).
      - Filter queries that touch *all* composite fields can resolve through
        the composite index after add.
      - Deleting the composite index leaves other single-field indexes
        intact and continues to serve filter queries (now via fallback).
      - Re-adding the composite with the same name after deletion works
        (verifies RemoveIndex cleaned up the name registration).
      - Negative cases: duplicate composite names, non-existent field,
        single-field composite.
    """

    db_name = "ts_db_composite_lifecycle"
    space_name = "ts_space_composite_lifecycle"
    embedding_size = 32
    doc_count = 100

    def test_prepare_db(self):
        response = create_db(router_url, self.db_name)
        assert response.json()["code"] == 0

    def test_prepare_space(self):
        # Start with single-field scalar indexes only. Composite is added
        # dynamically in test_add_composite_index.
        space_config = {
            "name": self.space_name,
            "partition_num": 1,
            "replica_num": 1,
            "fields": [
                {"name": "field_string", "type": "keyword",
                 "index": {"name": "idx_string", "type": "SCALAR"}},
                {"name": "field_int", "type": "integer",
                 "index": {"name": "idx_int", "type": "SCALAR"}},
                {"name": "field_float", "type": "float",
                 "index": {"name": "idx_float", "type": "SCALAR"}},
                {
                    "name": "field_vector",
                    "type": "vector",
                    "dimension": self.embedding_size,
                    "index": {"name": "gamma", "type": "FLAT"},
                },
            ],
        }
        response = create_space(router_url, self.db_name, space_config)
        assert response.json()["code"] == 0

    def _upsert_docs(self, count: int):
        url = router_url + "/document/upsert"
        docs = []
        # Deterministic content so the test can predict exact match counts.
        # field_string in {"A", "B"}, field_int in [0, 4], field_float = int * 1.5.
        for i in range(count):
            docs.append({
                "_id": str(i),
                "field_string": "A" if i % 2 == 0 else "B",
                "field_int": i % 5,
                "field_float": (i % 5) * 1.5,
                "field_vector": [random.random() for _ in range(self.embedding_size)],
            })
        resp = requests.post(url, auth=(username, password), json={
            "db_name": self.db_name,
            "space_name": self.space_name,
            "documents": docs,
        })
        assert resp.json()["code"] == 0, resp.json()

    def _query_composite(self, string_val: str, int_val: int, float_val: float):
        """Filter on all three composite fields; returns the result set ids."""
        url = router_url + "/document/query"
        data = {
            "db_name": self.db_name,
            "space_name": self.space_name,
            "filters": {
                "operator": "AND",
                "conditions": [
                    {"field": "field_string", "operator": "IN", "value": [string_val]},
                    {"field": "field_int", "operator": "=", "value": int_val},
                    {"field": "field_float", "operator": "=", "value": float_val},
                ],
            },
            "limit": 1000,
        }
        resp = requests.post(url, auth=(username, password), json=data)
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body.get("code", -1) == 0, body
        return {d["_id"] for d in body.get("data", {}).get("documents", [])}

    def test_seed_documents(self):
        self._upsert_docs(self.doc_count)
        time.sleep(2)

        # Sanity: before any composite index, the three-field equality filter
        # should still be answerable via single-field indexes.
        ids = self._query_composite("A", 2, 3.0)
        # i % 2 == 0 AND i % 5 == 2 AND float == 3.0 → i ∈ {2, 12, 22, ..., 92}
        expected = {str(i) for i in range(self.doc_count)
                    if i % 2 == 0 and i % 5 == 2}
        assert ids == expected, f"pre-composite query mismatch: " \
                                 f"missing={expected - ids} extra={ids - expected}"

    def test_add_composite_index(self):
        new_indexes = [{
            "name": "idx_composite_lifecycle",
            "type": "COMPOSITE",
            "field_names": ["field_string", "field_int", "field_float"],
        }]
        resp = add_space_indexes(router_url, self.db_name, self.space_name, new_indexes)
        logger.info(resp.json())
        assert resp.json()["code"] == 0

        # Wait for AddFieldIndexThread's two-phase backfill + publish.
        time.sleep(3)

        names = {idx["name"] for idx in
                 list_space_indexes(router_url, self.db_name, self.space_name)
                 .json()["data"]["indexes"]}
        assert "idx_composite_lifecycle" in names

    def test_query_after_add_composite(self):
        # Same predicate as test_seed_documents; result must remain identical
        # after the composite index is published. This validates that the
        # backfill covered all 100 seeded docs.
        ids = self._query_composite("A", 2, 3.0)
        expected = {str(i) for i in range(self.doc_count)
                    if i % 2 == 0 and i % 5 == 2}
        assert ids == expected, f"post-add query mismatch: " \
                                 f"missing={expected - ids} extra={ids - expected}"

    def test_add_duplicate_composite_name(self):
        # Re-adding the same index name should be rejected.
        resp = add_space_indexes(router_url, self.db_name, self.space_name, [{
            "name": "idx_composite_lifecycle",
            "type": "COMPOSITE",
            "field_names": ["field_string", "field_int"],
        }])
        logger.info(resp.json())
        assert resp.json()["code"] != 0

    def test_add_composite_with_non_existent_field(self):
        resp = add_space_indexes(router_url, self.db_name, self.space_name, [{
            "name": "idx_composite_bad_field",
            "type": "COMPOSITE",
            "field_names": ["field_string", "not_exist_field"],
        }])
        logger.info(resp.json())
        assert resp.json()["code"] != 0

    def test_add_composite_with_single_field_rejected(self):
        # A composite of only one field is not a valid composite.
        resp = add_space_indexes(router_url, self.db_name, self.space_name, [{
            "name": "idx_composite_single",
            "type": "COMPOSITE",
            "field_names": ["field_string"],
        }])
        logger.info(resp.json())
        assert resp.json()["code"] != 0

    def test_delete_composite_index(self):
        resp = delete_space_index(
            router_url, self.db_name, self.space_name, "idx_composite_lifecycle"
        )
        logger.info(resp.json())
        assert resp.json()["code"] == 0

        names = {idx["name"] for idx in
                 list_space_indexes(router_url, self.db_name, self.space_name)
                 .json()["data"]["indexes"]}
        assert "idx_composite_lifecycle" not in names
        # Single-field indexes must survive composite deletion.
        assert {"idx_string", "idx_int", "idx_float"}.issubset(names)

    def test_query_after_delete_composite(self):
        # After composite deletion, the query planner should fall back to
        # single-field indexes; result set must remain correct.
        ids = self._query_composite("A", 2, 3.0)
        expected = {str(i) for i in range(self.doc_count)
                    if i % 2 == 0 and i % 5 == 2}
        assert ids == expected, f"post-delete query mismatch: " \
                                 f"missing={expected - ids} extra={ids - expected}"

    def test_delete_already_deleted_composite(self):
        resp = delete_space_index(
            router_url, self.db_name, self.space_name, "idx_composite_lifecycle"
        )
        logger.info(resp.json())
        assert resp.json()["code"] != 0

    def test_readd_composite_after_delete(self):
        # Re-adding the same name should succeed once the previous one was
        # removed (verifies name registration cleanup in RemoveIndex).
        resp = add_space_indexes(router_url, self.db_name, self.space_name, [{
            "name": "idx_composite_lifecycle",
            "type": "COMPOSITE",
            "field_names": ["field_string", "field_int", "field_float"],
        }])
        logger.info(resp.json())
        assert resp.json()["code"] == 0
        time.sleep(2)

        ids = self._query_composite("B", 3, 4.5)
        expected = {str(i) for i in range(self.doc_count)
                    if i % 2 == 1 and i % 5 == 3}
        assert ids == expected, f"re-add query mismatch: " \
                                 f"missing={expected - ids} extra={ids - expected}"

    def test_destroy_space(self):
        response = drop_space(router_url, self.db_name, self.space_name)
        assert response.json()["code"] == 0

    def test_destroy_db(self):
        drop_db(router_url, self.db_name)


def _index_states_from_describe(db, space, index_name):
    """Collect the build state of `index_name` across all partitions.

    Returns a list of state strings (one per partition that reports the
    index). An empty list means no partition reported it yet.
    """
    resp = describe_space(router_url, db, space)
    body = resp.json()
    states = []
    for p in body.get("data", {}).get("partitions", []):
        ibs = p.get("index_build_state") or {}
        if index_name in ibs:
            states.append(ibs[index_name])
    return states


def _wait_index_ready(db, space, index_name, timeout=60):
    """Poll describe until every partition reporting the index is READY.

    Returns True once all reported states are READY (and at least one
    partition reported it); False on timeout.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        states = _index_states_from_describe(db, space, index_name)
        if states and all(s == "READY" for s in states):
            return True
        time.sleep(1)
    return False


def _wait_index_absent(db, space, index_name, timeout=60):
    """Poll describe until the index name no longer appears in any partition's
    index_build_state (i.e. removal cleared the tracked state). Returns True
    once absent everywhere; False on timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not _index_states_from_describe(db, space, index_name):
            return True
        time.sleep(1)
    return False


class TestSpaceIndexAddConcurrentWithWrites:
    """Regression test for the publish-then-backfill data-loss race.

    Adds a scalar index while documents are being written concurrently, then
    asserts every concurrently-written doc is findable through the new index.

    Before the fix, docs written during the backfill window (docid >=
    snapshot) were missed by the new index: the backfill loop stopped at the
    snapshot and the index was not yet published so incremental AddDoc could
    not reach it. After the fix the empty index is published BUILDING first,
    so concurrent writes flow in, and the inclusive snapshot backfill covers
    the boundary — no doc is lost.
    """

    db_name = "ts_db_idx_concurrent"
    space_name = "ts_space_idx_concurrent"
    embedding_size = 32
    seed_count = 2000        # baseline docs backfilled from
    concurrent_count = 3000  # docs written while the index builds

    def test_prepare_db(self):
        response = create_db(router_url, self.db_name)
        assert response.json()["code"] == 0

    def test_prepare_space(self):
        # field_int has NO index at creation; it is added dynamically below.
        space_config = {
            "name": self.space_name,
            "partition_num": 1,
            "replica_num": 1,
            "fields": [
                {"name": "field_int", "type": "integer"},
                {
                    "name": "field_vector",
                    "type": "vector",
                    "dimension": self.embedding_size,
                    "index": {"name": "gamma", "type": "FLAT"},
                },
            ],
        }
        response = create_space(router_url, self.db_name, space_config)
        assert response.json()["code"] == 0

    def _upsert_range(self, start: int, count: int):
        url = router_url + "/document/upsert"
        # field_int == docid so we can predict exact filter matches.
        batch = 200
        for base in range(start, start + count, batch):
            docs = []
            for i in range(base, min(base + batch, start + count)):
                docs.append({
                    "_id": str(i),
                    "field_int": i,
                    "field_vector": [random.random()
                                     for _ in range(self.embedding_size)],
                })
            resp = requests.post(url, auth=(username, password), json={
                "db_name": self.db_name,
                "space_name": self.space_name,
                "documents": docs,
            })
            assert resp.json()["code"] == 0, resp.json()

    def test_seed_baseline(self):
        self._upsert_range(0, self.seed_count)
        time.sleep(2)

    def test_add_index_while_writing(self):
        # Launch the concurrent writer, then immediately request the index so
        # its backfill overlaps the writes.
        writer = threading.Thread(
            target=self._upsert_range,
            args=(self.seed_count, self.concurrent_count),
        )
        writer.start()
        try:
            resp = add_space_indexes(router_url, self.db_name, self.space_name, [{
                "name": "idx_int_concurrent",
                "type": "SCALAR",
                "field_name": "field_int",
            }])
            assert resp.json()["code"] == 0, resp.json()
        finally:
            writer.join()

        assert _wait_index_ready(
            self.db_name, self.space_name, "idx_int_concurrent"), \
            "index did not reach READY in time"
        # Give the last incremental writes time to settle into the index.
        time.sleep(3)

    def test_all_docs_indexed(self):
        total = self.seed_count + self.concurrent_count
        # Range filter over the whole id space must be served by the index and
        # return every doc — including those written during the backfill.
        url = router_url + "/document/query"
        data = {
            "db_name": self.db_name,
            "space_name": self.space_name,
            "filters": {
                "operator": "AND",
                "conditions": [
                    {"field": "field_int", "operator": ">=", "value": 0},
                    {"field": "field_int", "operator": "<=", "value": total - 1},
                ],
            },
            "limit": total,
        }
        resp = requests.post(url, auth=(username, password), json=data)
        body = resp.json()
        assert body.get("code", -1) == 0, body
        ids = {d["_id"] for d in body.get("data", {}).get("documents", [])}
        expected = {str(i) for i in range(total)}
        missing = expected - ids
        assert not missing, \
            f"{len(missing)} docs missing from index (e.g. {sorted(missing)[:10]})"

    def test_destroy_space(self):
        response = drop_space(router_url, self.db_name, self.space_name)
        assert response.json()["code"] == 0

    def test_destroy_db(self):
        drop_db(router_url, self.db_name)


class TestSpaceIndexAddConcurrentValueChange:
    """Regression for the backfill stale-key race (H2).

    While a scalar index is BUILDING, its backfill reads each doc's value
    off-lock. If a concurrent Update changes that value in the window between
    the backfill reading the old value and writing its key, the backfill writes
    a key for the OLD value that the Update no longer overwrites — a stale key
    pointing at a live doc. Since the bitmap filters by liveness (not value),
    a later query on the OLD value would wrongly return the moved doc.

    The fix records docids changed during BUILDING and replays them (delete +
    re-add at the final value) under a write lock right before flipping READY.

    Test: seed docs with field_int in a LOW band, then while adding the index
    concurrently move a subset into a HIGH band. After READY, querying the HIGH
    band must return exactly the moved docs, and querying each moved doc's OLD
    low value must NOT return it (no stale key survived).
    """

    db_name = "ts_db_idx_valchange"
    space_name = "ts_space_idx_valchange"
    embedding_size = 32
    seed_count = 5000          # docs written & backfilled from
    move_count = 1500          # docs whose value is changed during BUILDING
    HIGH = 10_000_000          # offset moving a doc into the high band

    def test_prepare_db(self):
        response = create_db(router_url, self.db_name)
        assert response.json()["code"] == 0

    def test_prepare_space(self):
        # field_int has NO index at creation; added dynamically below.
        space_config = {
            "name": self.space_name,
            "partition_num": 1,
            "replica_num": 1,
            "fields": [
                {"name": "field_int", "type": "integer"},
                {
                    "name": "field_vector",
                    "type": "vector",
                    "dimension": self.embedding_size,
                    "index": {"name": "gamma", "type": "FLAT"},
                },
            ],
        }
        response = create_space(router_url, self.db_name, space_config)
        assert response.json()["code"] == 0

    def _upsert(self, docs):
        resp = requests.post(router_url + "/document/upsert",
                             auth=(username, password), json={
            "db_name": self.db_name,
            "space_name": self.space_name,
            "documents": docs,
        })
        assert resp.json()["code"] == 0, resp.json()

    def _seed(self):
        # field_int == docid initially (all in the LOW band [0, seed_count)).
        batch = 200
        for base in range(0, self.seed_count, batch):
            docs = [{
                "_id": str(i),
                "field_int": i,
                "field_vector": [random.random()
                                 for _ in range(self.embedding_size)],
            } for i in range(base, min(base + batch, self.seed_count))]
            self._upsert(docs)

    def _move_values(self):
        # Move docs [0, move_count) into the HIGH band: field_int = HIGH + i.
        # Their OLD value was i (the low band); after the move a query on the
        # old value i must not return doc i.
        batch = 200
        for base in range(0, self.move_count, batch):
            docs = [{
                "_id": str(i),
                "field_int": self.HIGH + i,
                "field_vector": [random.random()
                                 for _ in range(self.embedding_size)],
            } for i in range(base, min(base + batch, self.move_count))]
            self._upsert(docs)

    def test_seed_baseline(self):
        self._seed()
        time.sleep(2)

    def test_add_index_while_changing_values(self):
        # Launch the value-mover, then immediately request the index so its
        # backfill overlaps the value changes.
        mover = threading.Thread(target=self._move_values)
        mover.start()
        try:
            resp = add_space_indexes(router_url, self.db_name, self.space_name, [{
                "name": "idx_int_valchange",
                "type": "SCALAR",
                "field_name": "field_int",
            }])
            assert resp.json()["code"] == 0, resp.json()
        finally:
            mover.join()

        assert _wait_index_ready(
            self.db_name, self.space_name, "idx_int_valchange"), \
            "index did not reach READY in time"
        time.sleep(3)  # let the last incremental writes settle

    def _query_ids(self, low, high):
        url = router_url + "/document/query"
        data = {
            "db_name": self.db_name,
            "space_name": self.space_name,
            "filters": {
                "operator": "AND",
                "conditions": [
                    {"field": "field_int", "operator": ">=", "value": low},
                    {"field": "field_int", "operator": "<=", "value": high},
                ],
            },
            "limit": self.seed_count + self.move_count,
        }
        resp = requests.post(url, auth=(username, password), json=data)
        body = resp.json()
        assert body.get("code", -1) == 0, body
        return {d["_id"] for d in body.get("data", {}).get("documents", [])}

    def test_high_band_has_moved_docs(self):
        # Every moved doc must be findable by its NEW (high) value.
        ids = self._query_ids(self.HIGH, self.HIGH + self.move_count - 1)
        expected = {str(i) for i in range(self.move_count)}
        missing = expected - ids
        assert not missing, \
            f"{len(missing)} moved docs missing from new value (e.g. " \
            f"{sorted(missing, key=int)[:10]})"

    def test_old_band_has_no_stale_keys(self):
        # The moved docs' OLD low values [0, move_count) must NOT resolve to
        # them anymore. A stale backfill key would make doc i wrongly match the
        # old value i. Only docs that were NOT moved ([move_count, seed_count))
        # should remain in the low band.
        ids = self._query_ids(0, self.seed_count - 1)
        moved = {str(i) for i in range(self.move_count)}
        stale = ids & moved
        assert not stale, \
            f"{len(stale)} moved docs still matched by their OLD value — stale " \
            f"backfill keys survived (e.g. {sorted(stale, key=int)[:10]})"
        # Sanity: the un-moved docs are still present in the low band.
        survivors = {str(i) for i in range(self.move_count, self.seed_count)}
        missing = survivors - ids
        assert not missing, \
            f"{len(missing)} un-moved docs missing from low band (e.g. " \
            f"{sorted(missing, key=int)[:10]})"

    def test_destroy_space(self):
        response = drop_space(router_url, self.db_name, self.space_name)
        assert response.json()["code"] == 0

    def test_destroy_db(self):
        drop_db(router_url, self.db_name)


class TestSpaceIndexBuildStateVisibility:
    """The per-index build state surfaces through the space-describe API, and
    a filter on a not-yet-READY index falls back to 'no index' (no error, no
    partial result) rather than reading a half-built index."""

    db_name = "ts_db_idx_state"
    space_name = "ts_space_idx_state"
    embedding_size = 32
    doc_count = 500

    def test_prepare_db(self):
        response = create_db(router_url, self.db_name)
        assert response.json()["code"] == 0

    def test_prepare_space(self):
        space_config = {
            "name": self.space_name,
            "partition_num": 1,
            "replica_num": 1,
            "fields": [
                {"name": "field_int", "type": "integer"},
                {
                    "name": "field_vector",
                    "type": "vector",
                    "dimension": self.embedding_size,
                    "index": {"name": "gamma", "type": "FLAT"},
                },
            ],
        }
        response = create_space(router_url, self.db_name, space_config)
        assert response.json()["code"] == 0

    def test_seed_documents(self):
        url = router_url + "/document/upsert"
        docs = [{
            "_id": str(i),
            "field_int": i,
            "field_vector": [random.random() for _ in range(self.embedding_size)],
        } for i in range(self.doc_count)]
        resp = requests.post(url, auth=(username, password), json={
            "db_name": self.db_name,
            "space_name": self.space_name,
            "documents": docs,
        })
        assert resp.json()["code"] == 0, resp.json()
        time.sleep(2)

    def test_state_absent_before_add(self):
        # No dynamic index yet → the name must not appear in any partition.
        states = _index_states_from_describe(
            self.db_name, self.space_name, "idx_int_state")
        assert states == [], f"unexpected pre-add state: {states}"

    def test_state_reaches_ready_after_add(self):
        resp = add_space_indexes(router_url, self.db_name, self.space_name, [{
            "name": "idx_int_state",
            "type": "SCALAR",
            "field_name": "field_int",
        }])
        assert resp.json()["code"] == 0, resp.json()

        # Eventually READY, and the only states we ever observe are the three
        # legal values (never a garbage/UNKNOWN string).
        seen = set()
        deadline = time.time() + 60
        ready = False
        while time.time() < deadline:
            states = _index_states_from_describe(
                self.db_name, self.space_name, "idx_int_state")
            seen.update(states)
            if states and all(s == "READY" for s in states):
                ready = True
                break
            time.sleep(0.5)
        assert ready, f"index never became READY; observed states={seen}"
        assert seen <= {"BUILDING", "READY", "FAILED"}, \
            f"unexpected state value(s): {seen}"
        assert "FAILED" not in seen, "index build reported FAILED"

    def test_filter_correct_after_ready(self):
        # Once READY the index serves the filter; result must be complete.
        url = router_url + "/document/query"
        data = {
            "db_name": self.db_name,
            "space_name": self.space_name,
            "filters": {
                "operator": "AND",
                "conditions": [
                    {"field": "field_int", "operator": ">=", "value": 100},
                    {"field": "field_int", "operator": "<=", "value": 199},
                ],
            },
            "limit": 1000,
        }
        resp = requests.post(url, auth=(username, password), json=data)
        body = resp.json()
        assert body.get("code", -1) == 0, body
        ids = {d["_id"] for d in body.get("data", {}).get("documents", [])}
        expected = {str(i) for i in range(100, 200)}
        assert ids == expected, \
            f"filter mismatch: missing={expected - ids} extra={ids - expected}"

    def test_state_absent_after_delete(self):
        # Removing the index must clear its tracked build state, so the name
        # disappears from index_build_state (mirrors the pre-add absence).
        resp = delete_space_index(
            router_url, self.db_name, self.space_name, "idx_int_state")
        assert resp.json()["code"] == 0, resp.json()
        assert _wait_index_absent(self.db_name, self.space_name, "idx_int_state"), \
            "state lingered in index_build_state after index removal"

    def test_destroy_space(self):
        response = drop_space(router_url, self.db_name, self.space_name)
        assert response.json()["code"] == 0

    def test_destroy_db(self):
        drop_db(router_url, self.db_name)


class TestSpaceIndexReAddNoStaleData:
    """Regression for the delete-then-re-add stale-data bug.

    A scalar index's RocksDB keys live in a shared cf keyed by
    field_id+value+docid, with no index epoch. If a prior index's deletion
    left keys behind, a re-add over the same field reuses that key space; the
    backfill only writes, so residual keys survive and get read by the new
    index — returning docs whose value has since CHANGED (the deletion bitmap
    filters by docid, not value, so it cannot catch a stale-value hit).

    Scenario that would expose it without the "DropAll before backfill" fix:
      1. add index on field_int, seed docs, verify filter works;
      2. delete the index;
      3. UPDATE some docs so their field_int moves from an old value to a new
         one (old-value keys would linger if delete didn't fully clean, or if
         the re-add didn't clear the prefix);
      4. re-add the index on the same field;
      5. filtering by an OLD value must return ONLY docs that still hold it;
         filtering by a NEW value must return exactly the moved docs.
    A stale residual key would make step 5's old-value query wrongly return a
    doc that has moved to the new value.
    """

    db_name = "ts_db_idx_readd"
    space_name = "ts_space_idx_readd"
    embedding_size = 32
    doc_count = 1000

    def test_prepare_db(self):
        response = create_db(router_url, self.db_name)
        assert response.json()["code"] == 0

    def test_prepare_space(self):
        space_config = {
            "name": self.space_name,
            "partition_num": 1,
            "replica_num": 1,
            "fields": [
                {"name": "field_int", "type": "integer"},
                {
                    "name": "field_vector",
                    "type": "vector",
                    "dimension": self.embedding_size,
                    "index": {"name": "gamma", "type": "FLAT"},
                },
            ],
        }
        response = create_space(router_url, self.db_name, space_config)
        assert response.json()["code"] == 0

    def _upsert(self, id_val_pairs):
        url = router_url + "/document/upsert"
        docs = [{
            "_id": str(i),
            "field_int": v,
            "field_vector": [random.random() for _ in range(self.embedding_size)],
        } for (i, v) in id_val_pairs]
        resp = requests.post(url, auth=(username, password), json={
            "db_name": self.db_name,
            "space_name": self.space_name,
            "documents": docs,
        })
        assert resp.json()["code"] == 0, resp.json()

    def _query_eq(self, value):
        url = router_url + "/document/query"
        data = {
            "db_name": self.db_name,
            "space_name": self.space_name,
            "filters": {
                "operator": "AND",
                "conditions": [
                    {"field": "field_int", "operator": ">=", "value": value},
                    {"field": "field_int", "operator": "<=", "value": value},
                ],
            },
            "limit": self.doc_count,
        }
        resp = requests.post(url, auth=(username, password), json=data)
        body = resp.json()
        assert body.get("code", -1) == 0, body
        return {d["_id"] for d in body.get("data", {}).get("documents", [])}

    def test_seed_and_add_index(self):
        # All docs start with field_int == 100.
        self._upsert([(i, 100) for i in range(self.doc_count)])
        time.sleep(2)
        resp = add_space_indexes(router_url, self.db_name, self.space_name, [{
            "name": "idx_int_readd",
            "type": "SCALAR",
            "field_name": "field_int",
        }])
        assert resp.json()["code"] == 0, resp.json()
        assert _wait_index_ready(self.db_name, self.space_name, "idx_int_readd")
        # Every doc matches value 100.
        assert len(self._query_eq(100)) == self.doc_count

    def test_delete_index(self):
        resp = delete_space_index(
            router_url, self.db_name, self.space_name, "idx_int_readd")
        assert resp.json()["code"] == 0, resp.json()
        time.sleep(2)

    def test_move_values_while_unindexed(self):
        # Move the first half of the docs from value 100 to value 200 while no
        # index exists. If the deleted index left residual keys for value 100,
        # they still point at these docids.
        self._upsert([(i, 200) for i in range(self.doc_count // 2)])
        time.sleep(2)

    def test_readd_index_then_filter_is_exact(self):
        resp = add_space_indexes(router_url, self.db_name, self.space_name, [{
            "name": "idx_int_readd",
            "type": "SCALAR",
            "field_name": "field_int",
        }])
        assert resp.json()["code"] == 0, resp.json()
        assert _wait_index_ready(self.db_name, self.space_name, "idx_int_readd")
        time.sleep(2)

        moved = {str(i) for i in range(self.doc_count // 2)}          # now 200
        stayed = {str(i) for i in range(self.doc_count // 2, self.doc_count)}  # still 100

        got_100 = self._query_eq(100)
        got_200 = self._query_eq(200)

        # The moved docs must NOT appear under their old value 100. A residual
        # key from the previous index life would wrongly surface them here.
        stale = got_100 & moved
        assert not stale, \
            f"{len(stale)} moved docs still match old value 100 " \
            f"(stale residual keys, e.g. {sorted(stale, key=int)[:10]})"
        assert got_100 == stayed, \
            f"value=100 mismatch: missing={stayed - got_100} extra={got_100 - stayed}"
        assert got_200 == moved, \
            f"value=200 mismatch: missing={moved - got_200} extra={got_200 - moved}"

    def test_destroy_space(self):
        response = drop_space(router_url, self.db_name, self.space_name)
        assert response.json()["code"] == 0

    def test_destroy_db(self):
        drop_db(router_url, self.db_name)


class TestVectorIndexBuildStateVisibility:
    """Vector index build state surfaces through the describe API too (not just
    scalar), reaching READY after a dynamic add and disappearing after removal.

    A vector field must carry an index at space creation (the engine rejects a
    vector field with no index), and a *statically* created vector index does
    not register a tracked name/state. So to observe state transitions we
    create with the index, delete it, then dynamically re-add it — the re-add
    goes through AddFieldIndex which registers the name and BUILDING→READY.

    Vector search does not gate on state (ResetVectorIndexes swaps the built
    index in atomically), so this only checks observability: the name shows up
    with a legal state, converges to READY, and is cleared on delete."""

    db_name = "ts_db_vec_state"
    space_name = "ts_space_vec_state"
    embedding_size = 16
    doc_count = 500

    def test_prepare_db(self):
        response = create_db(router_url, self.db_name)
        assert response.json()["code"] == 0

    def test_prepare_space(self):
        # Vector field must have an index at creation; name it "vidx_state" so
        # we can delete and dynamically re-add the same index below.
        space_config = {
            "name": self.space_name,
            "partition_num": 1,
            "replica_num": 1,
            "fields": [
                {"name": "field_int", "type": "integer"},
                {
                    "name": "field_vector",
                    "type": "vector",
                    "dimension": self.embedding_size,
                    "index": {"name": "vidx_state", "type": "FLAT"},
                },
            ],
        }
        response = create_space(router_url, self.db_name, space_config)
        assert response.json()["code"] == 0

    def test_seed_documents(self):
        url = router_url + "/document/upsert"
        docs = [{
            "_id": str(i),
            "field_int": i,
            "field_vector": [random.random() for _ in range(self.embedding_size)],
        } for i in range(self.doc_count)]
        resp = requests.post(url, auth=(username, password), json={
            "db_name": self.db_name,
            "space_name": self.space_name,
            "documents": docs,
        })
        assert resp.json()["code"] == 0, resp.json()
        time.sleep(2)

    def test_delete_static_index(self):
        # Remove the statically-created vector index so we can dynamically
        # re-add it and observe its build-state transitions.
        resp = delete_space_index(
            router_url, self.db_name, self.space_name, "vidx_state")
        assert resp.json()["code"] == 0, resp.json()
        assert _wait_index_absent(self.db_name, self.space_name, "vidx_state"), \
            "vector index state lingered after removal"

    def test_state_reaches_ready_after_dynamic_add(self):
        resp = add_space_indexes(router_url, self.db_name, self.space_name, [{
            "name": "vidx_state",
            "type": "FLAT",
            "field_name": "field_vector",
        }])
        assert resp.json()["code"] == 0, resp.json()

        seen = set()
        deadline = time.time() + 60
        ready = False
        while time.time() < deadline:
            states = _index_states_from_describe(
                self.db_name, self.space_name, "vidx_state")
            seen.update(states)
            if states and all(s == "READY" for s in states):
                ready = True
                break
            time.sleep(0.5)
        assert ready, f"vector index never became READY; observed states={seen}"
        assert seen <= {"BUILDING", "READY", "FAILED"}, \
            f"unexpected state value(s): {seen}"
        assert "FAILED" not in seen, "vector index build reported FAILED"

    def test_state_absent_after_delete(self):
        resp = delete_space_index(
            router_url, self.db_name, self.space_name, "vidx_state")
        assert resp.json()["code"] == 0, resp.json()
        assert _wait_index_absent(self.db_name, self.space_name, "vidx_state"), \
            "vector index state lingered after removal"

    def test_destroy_space(self):
        response = drop_space(router_url, self.db_name, self.space_name)
        assert response.json()["code"] == 0

    def test_destroy_db(self):
        drop_db(router_url, self.db_name)


class TestMixedIndexStateAggregation:
    """EngineStatus merges scalar, composite, and vector build states into one
    index_build_state map. Add all three kinds dynamically in one space and
    assert describe reports every name (unique across kinds, no collision),
    each reaching READY."""

    db_name = "ts_db_mixed_state"
    space_name = "ts_space_mixed_state"
    embedding_size = 16
    doc_count = 500

    def test_prepare_db(self):
        response = create_db(router_url, self.db_name)
        assert response.json()["code"] == 0

    def test_prepare_space(self):
        # Vector field must carry an index at creation; name it "mix_vector" so
        # it can be deleted and dynamically re-added with the others below (a
        # static vector index does not register a tracked build state).
        space_config = {
            "name": self.space_name,
            "partition_num": 1,
            "replica_num": 1,
            "fields": [
                {"name": "field_int", "type": "integer"},
                {"name": "field_str", "type": "keyword"},
                {"name": "field_float", "type": "float"},
                {
                    "name": "field_vector",
                    "type": "vector",
                    "dimension": self.embedding_size,
                    "index": {"name": "mix_vector", "type": "FLAT"},
                },
            ],
        }
        response = create_space(router_url, self.db_name, space_config)
        assert response.json()["code"] == 0

    def test_seed_documents(self):
        url = router_url + "/document/upsert"
        docs = [{
            "_id": str(i),
            "field_int": i,
            "field_str": str(i % 10),
            "field_float": float(i),
            "field_vector": [random.random() for _ in range(self.embedding_size)],
        } for i in range(self.doc_count)]
        resp = requests.post(url, auth=(username, password), json={
            "db_name": self.db_name,
            "space_name": self.space_name,
            "documents": docs,
        })
        assert resp.json()["code"] == 0, resp.json()
        time.sleep(2)

    def test_delete_static_vector_index(self):
        # Drop the statically-created vector index so it is re-added
        # dynamically (and thus state-tracked) in the batch below.
        resp = delete_space_index(
            router_url, self.db_name, self.space_name, "mix_vector")
        assert resp.json()["code"] == 0, resp.json()
        assert _wait_index_absent(self.db_name, self.space_name, "mix_vector")

    def test_add_all_three_kinds(self):
        resp = add_space_indexes(router_url, self.db_name, self.space_name, [
            {"name": "mix_scalar", "type": "SCALAR", "field_name": "field_int"},
            {"name": "mix_composite", "type": "COMPOSITE",
             "field_names": ["field_str", "field_float"]},
            {"name": "mix_vector", "type": "FLAT", "field_name": "field_vector"},
        ])
        assert resp.json()["code"] == 0, resp.json()

    def test_all_three_states_visible_and_ready(self):
        # Every kind's name must appear in index_build_state and reach READY,
        # proving the scalar+composite+vector states are merged without one
        # kind's map clobbering another's.
        for name in ("mix_scalar", "mix_composite", "mix_vector"):
            assert _wait_index_ready(self.db_name, self.space_name, name), \
                f"index [{name}] did not reach READY / not visible in describe"

    def test_destroy_space(self):
        response = drop_space(router_url, self.db_name, self.space_name)
        assert response.json()["code"] == 0

    def test_destroy_db(self):
        drop_db(router_url, self.db_name)


class TestSpaceIndexAddConcurrentInsertBoundary:
    """Regression for the in-flight boundary-doc loss during build-under-lock.

    Engine::AddOrUpdate writes a new doc to the index (AddDoc(max_docid_), under
    build_write_mutex_) and only later does ++max_docid_ OUTSIDE that lock. A
    build that grabs build_write_mutex_ in that window reads a max_docid one
    short of the doc just written, so an open-interval rebuild [0, max_docid)
    would DropAll that doc and never re-add it — a silent false-negative. The
    fix rebuilds the closed interval [0, max_docid].

    This is timing-sensitive, so we repeat add/delete rounds and, in each round,
    fire the index add while a writer streams NEW docs one at a time (maximizing
    the chance the build's lock acquisition lands on a doc mid-write). After each
    round reaches READY we assert EVERY doc is findable through the index by an
    exact filter — the boundary doc included.
    """

    db_name = "ts_db_idx_insert_boundary"
    space_name = "ts_space_idx_insert_boundary"
    embedding_size = 32
    seed_count = 2000       # baseline present before each round's add
    round_writes = 800      # NEW docs streamed during each add
    rounds = 4              # repeat to raise the odds of hitting the window

    def test_prepare_db(self):
        response = create_db(router_url, self.db_name)
        assert response.json()["code"] == 0

    def test_prepare_space(self):
        # field_int has NO index at creation; added/removed each round below.
        space_config = {
            "name": self.space_name,
            "partition_num": 1,
            "replica_num": 1,
            "fields": [
                {"name": "field_int", "type": "integer"},
                {
                    "name": "field_vector",
                    "type": "vector",
                    "dimension": self.embedding_size,
                    "index": {"name": "gamma", "type": "FLAT"},
                },
            ],
        }
        response = create_space(router_url, self.db_name, space_config)
        assert response.json()["code"] == 0

    def _insert_one(self, i: int):
        # field_int == _id == docid so an exact filter predicts the match.
        resp = requests.post(router_url + "/document/upsert",
                             auth=(username, password), json={
            "db_name": self.db_name,
            "space_name": self.space_name,
            "documents": [{
                "_id": str(i),
                "field_int": i,
                "field_vector": [random.random() for _ in range(self.embedding_size)],
            }],
        })
        assert resp.json()["code"] == 0, resp.json()

    def _insert_range_stream(self, start: int, count: int):
        # One doc per request so writes are spread across the whole build,
        # maximizing the chance a build lock acquisition lands between a doc's
        # index-write and the ++max_docid_ that follows it.
        for i in range(start, start + count):
            self._insert_one(i)

    def _ids_via_index(self, hi: int) -> set:
        url = router_url + "/document/query"
        data = {
            "db_name": self.db_name,
            "space_name": self.space_name,
            "filters": {"operator": "AND", "conditions": [
                {"field": "field_int", "operator": ">=", "value": 0},
                {"field": "field_int", "operator": "<=", "value": hi},
            ]},
            "limit": hi + 1,
        }
        resp = requests.post(url, auth=(username, password), json=data)
        body = resp.json()
        assert body.get("code", -1) == 0, body
        return {d["_id"] for d in body.get("data", {}).get("documents", [])}

    def test_seed_baseline(self):
        for base in range(0, self.seed_count, 200):
            docs = [{
                "_id": str(i), "field_int": i,
                "field_vector": [random.random() for _ in range(self.embedding_size)],
            } for i in range(base, min(base + 200, self.seed_count))]
            resp = requests.post(router_url + "/document/upsert",
                                 auth=(username, password), json={
                "db_name": self.db_name, "space_name": self.space_name,
                "documents": docs,
            })
            assert resp.json()["code"] == 0, resp.json()
        time.sleep(2)

    def test_add_delete_rounds_keep_every_doc(self):
        next_id = self.seed_count
        for r in range(self.rounds):
            start = next_id
            # Stream new docs concurrently with the index add.
            writer = threading.Thread(
                target=self._insert_range_stream, args=(start, self.round_writes))
            writer.start()
            try:
                resp = add_space_indexes(router_url, self.db_name, self.space_name, [{
                    "name": "idx_int_boundary",
                    "type": "SCALAR",
                    "field_name": "field_int",
                }])
                assert resp.json()["code"] == 0, resp.json()
            finally:
                writer.join()

            assert _wait_index_ready(self.db_name, self.space_name, "idx_int_boundary"), \
                f"round {r}: index did not reach READY"
            time.sleep(2)  # let any last incremental writes settle

            next_id = start + self.round_writes
            hi = next_id - 1
            # Every doc [0, hi] must be served by the index. A dropped in-flight
            # boundary doc (e.g. the last one written this round) shows up here
            # as a missing id.
            ids = self._ids_via_index(hi)
            expected = {str(i) for i in range(next_id)}
            missing = expected - ids
            assert not missing, \
                f"round {r}: {len(missing)} docs missing via index " \
                f"(e.g. {sorted(missing, key=int)[:10]}); boundary doc likely dropped"

            # Delete the index so the next round rebuilds from scratch.
            resp = delete_space_index(router_url, self.db_name, self.space_name,
                                      "idx_int_boundary")
            assert resp.json()["code"] == 0, resp.json()
            assert _wait_index_absent(self.db_name, self.space_name, "idx_int_boundary"), \
                f"round {r}: index not cleared before next round"

    def test_destroy_space(self):
        response = drop_space(router_url, self.db_name, self.space_name)
        assert response.json()["code"] == 0

    def test_destroy_db(self):
        drop_db(router_url, self.db_name)


class TestRemoveLastVectorIndexSearchErrors:
    """Regression: removing the space's ONLY vector index must leave the engine
    responsive (no spinning indexing thread, no crash) while making vector
    search return a clean error rather than hanging or asserting.

    The engine-side fix in RemoveFieldIndexTask's vector branch, once the last
    vector index is gone, stops the shared indexing thread and resets
    index_status_ to UNINDEXED. Without stopping the thread first, the indexing
    loop keeps running and rewrites index_status_ = INDEXED every iteration
    (wasted work over an index set that no longer exists, and it would clobber
    the UNINDEXED reset). That thread state is not directly assertable over
    HTTP, so it is covered indirectly: the engine must stay alive and answer a
    non-vector query.

    Both vector search modes error after removal, but at the ROUTER, before the
    request reaches the engine. Once the index is removed, master also clears
    the field's inline index (space.Indexes + SpaceProperties), so
    Space.GetFieldIndexType("v") returns "" and parseVectors rejects the query
    with PARAM_ERROR "vector index type is empty" (doc_query.go). This holds for
    is_brute_search=0 and =1 alike — the router check runs before any brute-force
    branch, so brute force is not a usable fallback once the index is gone. The
    HTTP status is 4xx with a non-zero body code, not 200.

    A document/query by _id (no vector) must still succeed, proving the engine
    is alive and only the vector search paths are gated.

    HNSW is used (an incremental index) so the single index trains immediately
    on the seeded data — the pre-removal sanity search reliably returns hits,
    the same proven starting state as TestRemoveOneVectorIndexKeepsOthersLive."""

    db_name = "ts_db_remove_last_vec"
    space_name = "ts_space_remove_last_vec"
    dim = 16
    seed_count = 200  # > brute_force_search_threshold (100)

    def test_prepare_db(self):
        response = create_db(router_url, self.db_name)
        assert response.json()["code"] == 0

    def test_prepare_space_single_vector_index(self):
        hnsw = {
            "nlinks": 16,
            "efConstruction": 60,
            "efSearch": 32,
            "metric_type": "L2",
        }
        space_config = {
            "name": self.space_name,
            "partition_num": 1,
            "replica_num": 1,
            "fields": [
                {"name": "field_int", "type": "integer"},
                {"name": "v", "type": "vector", "dimension": self.dim,
                 "index": {"name": "idx_v", "type": "HNSW", "params": hnsw}},
            ],
        }
        resp = create_space(router_url, self.db_name, space_config)
        assert resp.json()["code"] == 0, resp.json()

    def _upsert(self, start: int, count: int):
        url = router_url + "/document/upsert"
        docs = [{
            "_id": str(i),
            "field_int": i,
            "v": [random.random() for _ in range(self.dim)],
        } for i in range(start, start + count)]
        resp = requests.post(url, auth=(username, password), json={
            "db_name": self.db_name,
            "space_name": self.space_name,
            "documents": docs,
        })
        assert resp.json()["code"] == 0, resp.json()

    def _search(self, is_brute_search: int):
        url = router_url + "/document/search"
        data = {
            "db_name": self.db_name,
            "space_name": self.space_name,
            "vectors": [{
                "field": "v",
                "feature": [random.random() for _ in range(self.dim)],
            }],
            "limit": 5,
            "is_brute_search": is_brute_search,
        }
        return requests.post(url, auth=(username, password), json=data)

    def test_seed_and_index(self):
        self._upsert(0, self.seed_count)
        time.sleep(3)  # let the indexing thread train the single index
        # Sanity: before removal a normal search returns hits (index trained).
        r = self._search(0)
        assert r.status_code == 200 and r.json().get("code", -1) == 0, \
            f"pre-removal normal search failed: {r.text}"

    def test_remove_last_vector_index(self):
        resp = delete_space_index(
            router_url, self.db_name, self.space_name, "idx_v")
        assert resp.json()["code"] == 0, resp.json()
        assert _wait_index_absent(self.db_name, self.space_name, "idx_v"), \
            "vector index state lingered after removing the only vector index"
        # Give RemoveFieldIndexTask time to stop the indexing thread and reset
        # index_status_ = UNINDEXED (it runs after the raft apply that cleared
        # the tracked state polled above).
        time.sleep(2)

    def test_normal_search_errors_after_removal(self):
        # After removal the router can no longer resolve an index type for the
        # field (GetFieldIndexType == ""), so parseVectors rejects the query
        # before it reaches the engine: HTTP 4xx with a non-zero body code.
        r = self._search(0)
        assert r.status_code != 200, \
            f"normal search should be rejected after removal, got 200: {r.text}"
        assert r.json().get("code", 0) != 0, \
            f"normal search should error after last vector index removed: {r.json()}"

    def test_brute_search_also_errors_after_removal(self):
        # is_brute_search=1 is rejected by the SAME router check (it runs before
        # any brute-force branch), so brute force is not a fallback once the
        # index is gone — it errors identically.
        r = self._search(1)
        assert r.status_code != 200, \
            f"brute search should be rejected after removal, got 200: {r.text}"
        assert r.json().get("code", 0) != 0, \
            f"brute search should also error (no index to resolve): {r.json()}"

    def test_engine_alive_via_doc_query(self):
        # A non-vector document/query must still work: proves the engine did not
        # crash or hang when the last vector index was removed — only the vector
        # search paths are gated.
        url = router_url + "/document/query"
        data = {
            "db_name": self.db_name,
            "space_name": self.space_name,
            "document_ids": ["0", "1", "2"],
            "limit": 10,
        }
        resp = requests.post(url, auth=(username, password), json=data)
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body.get("code", -1) == 0, \
            f"document/query failed after last vector index removal: {body}"
        docs = body.get("data", {}).get("documents", [])
        assert len(docs) > 0, f"expected seeded docs to still be retrievable: {body}"

    def test_destroy_space(self):
        response = drop_space(router_url, self.db_name, self.space_name)
        assert response.json()["code"] == 0

    def test_destroy_db(self):
        drop_db(router_url, self.db_name)


class TestListIndexesBuildStateDetail:
    """GET /indexes?detail=true reports per-replica index build state.

    The plain GET /indexes stays a pure-metadata, zero-RPC read (no build_state
    key). With ?detail=true the master fans out to every replica of every
    partition and returns each replica's own index build state — index build
    state is per-replica local, so this is the only way to observe follower
    state (describe only ever queries the leader).

    This runs standalone (replica_num=1), so it verifies the response SHAPE:
    build_state present, one entry per partition, each with a replicas[] whose
    items carry node_id/is_leader/index_status/index_build_state. The multi-
    replica divergence and unreachable-node (error) branches are exercised in
    the cluster CI, not here."""

    db_name = "ts_db_idx_buildstate"
    space_name = "ts_space_idx_buildstate"
    embedding_size = 16
    doc_count = 300

    def test_prepare_db(self):
        response = create_db(router_url, self.db_name)
        assert response.json()["code"] == 0

    def test_prepare_space(self):
        space_config = {
            "name": self.space_name,
            "partition_num": 1,
            "replica_num": 1,
            "fields": [
                {"name": "field_int", "type": "integer",
                 "index": {"name": "idx_int", "type": "SCALAR"}},
                {"name": "field_vector", "type": "vector",
                 "dimension": self.embedding_size,
                 "index": {"name": "idx_vec", "type": "FLAT"}},
            ],
        }
        response = create_space(router_url, self.db_name, space_config)
        assert response.json()["code"] == 0, response.json()

    def test_seed(self):
        url = router_url + "/document/upsert"
        docs = [{
            "_id": str(i),
            "field_int": i,
            "field_vector": [random.random() for _ in range(self.embedding_size)],
        } for i in range(self.doc_count)]
        resp = requests.post(url, auth=(username, password), json={
            "db_name": self.db_name, "space_name": self.space_name,
            "documents": docs,
        })
        assert resp.json()["code"] == 0, resp.json()
        time.sleep(2)

    def test_list_without_detail_has_no_build_state(self):
        resp = list_space_indexes(router_url, self.db_name, self.space_name)
        assert resp.json()["code"] == 0, resp.json()
        data = resp.json()["data"]
        # Backward-compat: the plain list is unchanged, no build_state key.
        assert "build_state" not in data or data.get("build_state") in (None, []), \
            f"plain /indexes should not carry build_state: {data}"
        names = {idx["name"] for idx in data["indexes"]}
        assert {"idx_int", "idx_vec"} <= names, f"indexes missing: {names}"

    def test_list_with_detail_reports_per_replica_state(self):
        resp = list_space_indexes(router_url, self.db_name, self.space_name, detail=True)
        assert resp.json()["code"] == 0, resp.json()
        data = resp.json()["data"]
        build_state = data.get("build_state")
        assert build_state, f"detail=true must return build_state: {data}"
        # One entry per partition (single partition here).
        assert len(build_state) == 1, f"expected 1 partition, got {build_state}"

        pbs = build_state[0]
        assert "pid" in pbs, pbs
        replicas = pbs.get("replicas") or []
        assert len(replicas) == 1, f"expected 1 replica, got {replicas}"

        r = replicas[0]
        assert "node_id" in r, r
        assert r.get("is_leader") is True, f"sole replica must be leader: {r}"
        # index_status is the engine enum (0/1/2); the field is always present.
        assert r.get("index_status") in (0, 1, 2), f"unexpected index_status: {r}"
        assert "error" not in r or not r.get("error"), \
            f"reachable replica should have no error: {r}"

    def test_destroy_space(self):
        response = drop_space(router_url, self.db_name, self.space_name)
        assert response.json()["code"] == 0

    def test_destroy_db(self):
        drop_db(router_url, self.db_name)