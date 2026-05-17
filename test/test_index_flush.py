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

import os
import requests
import json
import pytest
from utils.vearch_utils import *
from utils.data_utils import *

__description__ = """ test case for index flush """


sift10k = DatasetSift10K()
xb = sift10k.get_database()
xq = sift10k.get_queries()
gt = sift10k.get_groundtruth()


def check_search(full_field, case_space_name, times=5):
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


def check_flush(case_space_name, index_type):
    """Check flush result and verify index files exist."""
    logger.info(f"check_flush for {index_type}")

    # Call index flush API
    response = index_flush(router_url, db_name, case_space_name)
    logger.info(f"flush response: {response.json()}")
    assert response.json()["code"] == 0

    # Get partition info from cache
    partition_infos = get_partition_with_path(router_url, db_name, case_space_name)
    assert len(partition_infos) > 0, "should have at least one partition"
    # logger.info(f"partition infos: {partition_infos}")

    # Get base path from cluster stats
    base_path = get_partition_path_from_cluster_stats(router_url)
    # logger.info(f"base path from cluster stats: {base_path}")
    assert base_path, "base path should not be empty"

    # Map index_type to file name
    index_file_map = {
        "IVFPQ": "ivfpq.index",
        "IVFFLAT": "ivfflat.index",
        "IVFRABITQ": "ivfrabitq.index",
        "HNSW": "hnswlib.index",
    }
    index_file_name = index_file_map.get(index_type, "")

    for partition in partition_infos:
        partition_id = partition.get("id", 0)

        # Construct index directory path
        index_dir = os.path.join(
            base_path,
            "data",
            str(partition_id),
            "retrieval_model_index",
        )
        # logger.info(f"checking index dir: {index_dir}")

        # FLAT type does not have dump file, just check directory exists
        if index_type == "FLAT":
            if os.path.exists(index_dir):
                logger.info(f"index directory exists for FLAT: {index_dir}")
            else:
                logger.warning(f"index directory does not exist: {index_dir}")
            continue

        # Check if index directory exists
        if os.path.exists(index_dir):
            # List files in the directory
            files = os.listdir(index_dir)

            # For each potential index name, check if index file exists
            for index_name in files:
                assert os.path.isdir(os.path.join(index_dir, index_name))

                if index_type == "DISKANN_STATIC":
                    fv_dir = os.path.join(index_dir, index_name, "field_vector.000")
                    assert os.path.isdir(
                        fv_dir
                    ), "field_vector.000 dir does not exist: %s" % fv_dir
                    subdirs = os.listdir(fv_dir)
                    found = False
                    for d in subdirs:
                        full_dir = os.path.join(fv_dir, d)
                        if not os.path.isdir(full_dir):
                            continue
                        meta_file = os.path.join(full_dir, "diskann_static_meta.bin")
                        if os.path.exists(meta_file):
                            file_size = os.path.getsize(meta_file)
                            logger.info(
                                "diskann meta file: %s, size: %d bytes",
                                meta_file,
                                file_size,
                            )
                            found = True
                            break
                    assert found, "diskann_static_meta.bin not found under: %s" % fv_dir
                    continue

                index_file = os.path.join(
                    index_dir, index_name, "field_vector.000", index_file_name
                )
                assert os.path.exists(index_file), f"index file does not exist: {index_file}"

                # Output file size information
                file_size = os.path.getsize(index_file)
                logger.info(f"index file: {index_file}, size: {file_size} bytes")

        else:
            logger.error(f"index directory does not exist: {index_dir}")
            assert False

    logger.info("check_flush finish")


class TestIndexFlush:
    def setup_class(self):
        pass

    def test_prepare_db(self):
        logger.info(create_db(router_url, db_name))

    @pytest.mark.parametrize(
        ["training_threshold", "index_type"],
        [
            [1, "FLAT"],
            [3999, "IVFPQ"],
            [3999, "IVFFLAT"],
            [3999, "IVFRABITQ"],
            [1, "HNSW"],
            [10000, "DISKANN_STATIC"],
        ],
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

        enable_realtime = True
        if index_type == "DISKANN_STATIC":
            enable_realtime = False

        space_config = {
            "name": space_name,
            "partition_num": 2,
            "replica_num": 1,
            "enable_realtime": enable_realtime,
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
                            "nb_bits": 4,
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

        if index_type == "DISKANN_STATIC":
            space_config["fields"][-1]["store_type"] = "RocksDB"
            space_config["fields"][-1]["index"]["params"] = {
                "metric_type": "InnerProduct",
                "training_threshold": training_threshold,
                "R": 32,
                "L": 64,
                "num_threads": 2,
                "beam_width": 4,
                "num_nodes_to_cache": 100000,
                "search_dram_budget_gb": 0.5,
                "build_dram_budget_gb": 0.56,
                "disk_pq_bytes": 0,
                "use_opq": 0,
                "append_reorder_data": 0,
            }

        response = create_space(router_url, db_name, space_config)
        logger.info(response.json())
        assert response.json()["code"] == 0

        add(total_batch, batch_size, xb, with_id, full_field)

        if index_type == "DISKANN_STATIC":
            time.sleep(10)
            fm = requests.post(
                f"{router_url}/index/forcemerge",
                auth=(username, password),
                json={"db_name": db_name, "space_name": space_name, "partition_id": 0},
            )
            logger.info(fm.json())
            assert fm.status_code == 200
            assert fm.json().get("code") == 0, fm.json()

            detail_url = f"{router_url}/dbs/{db_name}/spaces/{space_name}?detail=true"
            for round_i in range(180):
                rs = requests.get(detail_url, auth=(username, password))
                assert rs.status_code == 200
                body = rs.json()
                assert body.get("code") == 0, body
                data = body.get("data", {})
                status = data.get("status", "")
                partitions = data.get("partitions", [])
                idx_statuses = [p.get("index_status", -1) for p in partitions]
                logger.info(
                    "diskann wait indexed round=%d status=%s partitions=%s",
                    round_i,
                    status,
                    idx_statuses,
                )
                if status != "red" and len(partitions) > 0 and all(s == 2 for s in idx_statuses):
                    break
                time.sleep(10)
            else:
                assert False, "diskann index_status did not reach INDEXED within timeout"
        else:
            waiting_index_finish(total)

        # Check flush and verify index files
        check_flush(space_name, index_type)

        check_search(full_field, space_name)

        drop_space(router_url, db_name, space_name)

    def test_destroy_db(self):
        drop_db(router_url, db_name)
