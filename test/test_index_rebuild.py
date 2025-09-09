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
import concurrent.futures

__description__ = """ test case for index rebuild """


sift10k = DatasetSift10K()
xb = sift10k.get_database()
xq = sift10k.get_queries()
gt = sift10k.get_groundtruth()


def check_search(full_field, case_space_name, times=5):
    logger.info("check_search")
    url = router_url + "/document/search?timeout=2000000"

    for i in range(times):
        data = {}
        data["vector_value"] = True

        data["db_name"] = db_name
        data["space_name"] = case_space_name
        data["vectors"] = []
        query_size = 1
        vector_info = {
            "field": "field_vector",
            "feature": xb[i : i + query_size].flatten().tolist(),
        }

        data["vectors"].append(vector_info)

        json_str = json.dumps(data)
        rs = requests.post(url, auth=(username, password), data=json_str)

        if rs.status_code != 200 or "documents" not in rs.json()["data"]:
            logger.info(rs.json())
            logger.info(json_str)

        if rs.json()["code"] != 0:
            return

        documents = rs.json()["data"]["documents"]
        if len(documents) != query_size:
            logger.info("len(documents) = " + str(len(documents)))
            logger.debug(json_str)
            logger.info(rs.json())

        assert len(documents) == query_size

        for j in range(query_size):
            for document in documents[j]:
                if document["_id"] == "":  # may be deleted
                    continue
                value = int(document["_id"])
                assert document["field_int"] == value
                if full_field:
                    assert document["field_long"] == value
                    assert document["field_float"] == float(value)
                    assert document["field_double"] == float(value)
                if "field_vector" in document:
                    assert document["field_vector"] == xb[value].flatten().tolist()

        if times > 1:
            time.sleep(0.1)

    logger.info("check_search finish")


def check_delete(case_space_name, times=5):
    logger.info("check_delete")
    url = router_url + "/document/delete?timeout=300000"
    data = {}
    data["db_name"] = db_name
    data["space_name"] = case_space_name

    data["document_ids"] = []
    for i in range(times):
        # delete half of the data
        batch = 20
        for i in range(batch):
            document_ids = [
                str(i)
                for i in range(
                    (int(xb.shape[0] / batch * i)),
                    (int(xb.shape[0] / batch * (i + 1))),
                )
            ]
            data["document_ids"] = random.sample(
                document_ids, k=int(xb.shape[0] / 2 / batch)
            )

            json_str = json.dumps(data)
            rs = requests.post(url, auth=(username, password), data=json_str)
            assert rs.json()["code"] == 0

        if times > 1:
            time.sleep(random.randint(1, 3))
    logger.info("check_delete finish")


def check_upsert(
    total_batch, batch_size, with_id, full_field, case_space_name, times=5
):
    logger.info("check_upsert")
    for i in range(times):
        add(
            total_batch,
            batch_size,
            xb,
            with_id,
            full_field,
            space_name=case_space_name,
        )
        if times > 1:
            time.sleep(random.randint(1, 3))
    logger.info("check_upsert finish")


def check_rebuild(case_space_name, times=5):
    logger.info("check_rebuild")
    for i in range(times):
        drop_before_rebuild = random.choice([True, False])
        response = index_rebuild(
            router_url,
            db_name,
            case_space_name,
            drop_before_rebuild=drop_before_rebuild,
        )
        logger.info(response.json())
        if times > 1:
            time.sleep(random.randint(1, 3))
    logger.info("check_rebuild finish")


class TestIndexRebuildBase:
    def setup_class(self):
        pass

    def test_prepare_db(self):
        logger.info(create_db(router_url, db_name))

    @pytest.mark.parametrize(
        ["training_threshold", "index_type"],
        [[1, "FLAT"], [3999, "IVFPQ"], [3999, "IVFFLAT"], [1, "HNSW"]],
    )
    def test_space_create(self, training_threshold, index_type):
        embedding_size = xb.shape[1]
        batch_size = 100
        total = xb.shape[0]
        total_batch = int(total / batch_size)
        with_id = True
        full_field = True
        logger.info(
            "dataset num: %d, total_batch: %d, dimension: %d"
            % (total, total_batch, embedding_size)
        )

        space_config = {
            "name": space_name,
            "partition_num": 2,
            "replica_num": 1,
            "fields": [
                {
                    "name": "field_int",
                    "type": "integer",
                },
                {
                    "name": "field_long",
                    "type": "long",
                },
                {
                    "name": "field_float",
                    "type": "float",
                },
                {
                    "name": "field_double",
                    "type": "double",
                },
                {
                    "name": "field_string",
                    "type": "string",
                    "index": {
                        "name": "field_string",
                        "type": "SCALAR",
                    },
                },
                {
                    "name": "field_vector",
                    "type": "vector",
                    "index": {
                        "name": "gamma",
                        "type": index_type,
                        "params": {
                            "metric_type": "InnerProduct",
                            "ncentroids": 128,
                            "nsubvector": 32,
                            "nlinks": 32,
                            "efConstruction": 40,
                            "training_threshold": training_threshold,
                        },
                    },
                    "dimension": embedding_size,
                    # "format": "normalization"
                },
            ],
        }

        response = create_space(router_url, db_name, space_config)
        logger.info(response.json())
        assert response.json()["code"] == 0

        add(total_batch, batch_size, xb, with_id, full_field)

        waiting_index_finish(total)

        response = index_rebuild(router_url, db_name, space_name, partition_id=12345)
        logger.info(response.json())
        assert response.json()["code"] != 0

        response = index_rebuild(router_url, db_name, space_name)
        logger.info(response.json())

        waiting_index_finish(total)

        partitions = get_partition(router_url, db_name, space_name)
        for partition in partitions:
            response = index_rebuild(
                router_url, db_name, space_name, partition_id=partition
            )
            logger.info(response.json())

        waiting_index_finish(total)

        response = index_rebuild(
            router_url, db_name, space_name, drop_before_rebuild=False
        )
        logger.info(response.json())

        waiting_index_finish(total)

        check_search(full_field, space_name)

        drop_space(router_url, db_name, space_name)

    def test_destroy_db(self):
        drop_db(router_url, db_name)


class TestIndexRebuildWithDelete:
    def setup_class(self):
        pass

    def test_prepare_db(self):
        logger.info(create_db(router_url, db_name))

    @pytest.mark.parametrize(
        ["training_threshold", "index_type"],
        [[1, "FLAT"], [9999, "IVFPQ"], [9999, "IVFFLAT"], [1, "HNSW"]],
    )
    def test_space_create(self, training_threshold, index_type):
        embedding_size = xb.shape[1]
        batch_size = 100
        total = xb.shape[0]
        total_batch = int(total / batch_size)
        with_id = True
        full_field = True
        logger.info(
            "dataset num: %d, total_batch: %d, dimension: %d"
            % (total, total_batch, embedding_size)
        )

        case_space_name = space_name + "_rdbd_" + index_type
        space_config = {
            "name": case_space_name,
            "partition_num": 1,
            "replica_num": 1,
            "fields": [
                {
                    "name": "field_int",
                    "type": "integer",
                },
                {
                    "name": "field_long",
                    "type": "long",
                },
                {
                    "name": "field_float",
                    "type": "float",
                },
                {
                    "name": "field_double",
                    "type": "double",
                },
                {
                    "name": "field_string",
                    "type": "string",
                    "index": {
                        "name": "field_string",
                        "type": "SCALAR",
                    },
                },
                {
                    "name": "field_vector",
                    "type": "vector",
                    "index": {
                        "name": "gamma",
                        "type": index_type,
                        "params": {
                            "metric_type": "InnerProduct",
                            "ncentroids": 256,
                            "nsubvector": 32,
                            "nlinks": 32,
                            "efConstruction": 40,
                            "training_threshold": training_threshold,
                        },
                    },
                    "dimension": embedding_size,
                    # "format": "normalization"
                },
            ],
        }

        response = create_space(router_url, db_name, space_config)
        logger.info(response.json())
        assert response.json()["code"] == 0

        # do 3 times
        for i in range(3):
            add(
                total_batch,
                batch_size,
                xb,
                with_id,
                full_field,
                space_name=case_space_name,
            )
            target = int(total / 2)
            waiting_index_finish(target, space_name=case_space_name)

            check_delete(case_space_name, 1)

            drop_before_rebuild = True
            if i == 2:
                drop_before_rebuild = False
            response = index_rebuild(
                router_url, db_name, case_space_name, drop_before_rebuild
            )
            logger.info(response.json())

            waiting_index_finish(target, space_name=case_space_name)

            check_search(full_field, case_space_name)

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(check_search, full_field, case_space_name, 200),
                executor.submit(
                    check_delete,
                    case_space_name,
                ),
                executor.submit(
                    check_upsert,
                    total_batch,
                    batch_size,
                    with_id,
                    full_field,
                    case_space_name,
                ),
                executor.submit(
                    check_rebuild,
                    case_space_name,
                ),
            ]
            for future in concurrent.futures.as_completed(futures):
                future.result()

        response = drop_space(router_url, db_name, case_space_name)
        logger.info(response.json())

    def test_destroy_db(self):
        drop_db(router_url, db_name)
