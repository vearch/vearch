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


def _vector_dump_dirs(base_path, partition_id, field_name):
    """Return the on-disk dump directories for a vector FIELD across all dump
    timestamps of one partition.

    Vector index files live at:
      <base>/data/<pid>/retrieval_model_index/<timestamp>/<field>.<ver>/<type>.index
    RemoveVectorIndex deletes the "<field>.<ver>" dirs by "<field>." prefix
    (vector_manager.cc), so those are exactly what we look for. Returns a list
    of matching directory paths (may be empty)."""
    index_root = os.path.join(
        base_path, "data", str(partition_id), "retrieval_model_index")
    if not os.path.isdir(index_root):
        return []
    prefix = field_name + "."
    hits = []
    for ts_dir in os.listdir(index_root):
        ts_path = os.path.join(index_root, ts_dir)
        if not os.path.isdir(ts_path):
            continue
        for fd in os.listdir(ts_path):
            if fd.startswith(prefix) and os.path.isdir(os.path.join(ts_path, fd)):
                hits.append(os.path.join(ts_path, fd))
    return hits


class TestVectorIndexRemoveClearsDumpFiles:
    """Removing a vector index must delete its dumped files on disk, so a later
    restart cannot resurrect the removed index from a stale file.

    This is the file-level counterpart to the search-based cluster test: rather
    than restart a PS and infer staleness from search ranking, it asserts the
    dump directory directly. It requires the engine's data dir to be visible on
    the test host (true for a standalone `make test` run; a dockerized PS whose
    data dir is not bind-mounted is skipped) — the same reason this whole file
    is standalone-only and not in the cluster CI matrix.

    Flow: create an IVFPQ space, seed past the train threshold, index_flush to
    force a real ivfpq.index dump, assert the field's dump dir exists, remove
    the index, then assert the dump dir is gone."""

    rm_db = "ts_db_vec_dump_rm"
    rm_space = "ts_space_vec_dump_rm"
    dim = 16
    doc_count = 2000

    def test_prepare_db(self):
        response = create_db(router_url, self.rm_db)
        assert response.json()["code"] == 0

    def test_prepare_space(self):
        # IVFPQ dumps a real ivfpq.index file. training_threshold below the seed
        # count so the index trains; nprobe <= ncentroids or Init rejects it.
        space_config = {
            "name": self.rm_space,
            "partition_num": 1,
            "replica_num": 1,
            "fields": [
                {"name": "field_int", "type": "integer"},
                {
                    "name": "field_vector",
                    "type": "vector",
                    "dimension": self.dim,
                    "index": {
                        "name": "gamma",
                        "type": "IVFPQ",
                        "params": {"ncentroids": 32, "nsubvector": 8,
                                   "training_threshold": 1500, "nprobe": 16},
                    },
                },
            ],
        }
        resp = create_space(router_url, self.rm_db, space_config)
        if resp.json().get("code", -1) != 0:
            pytest.skip(f"IVFPQ space not supported here: {resp.json()}")

    def test_seed(self):
        url = router_url + "/document/upsert"
        batch = 500
        for base in range(0, self.doc_count, batch):
            docs = [{
                "_id": str(i),
                "field_int": i,
                "field_vector": [random.random() for _ in range(self.dim)],
            } for i in range(base, min(base + batch, self.doc_count))]
            resp = requests.post(url, auth=(username, password), json={
                "db_name": self.rm_db, "space_name": self.rm_space,
                "documents": docs,
            })
            assert resp.json()["code"] == 0, resp.json()
        # Wait until all docs are indexed. waiting_index_finish() is hardcoded to
        # the module-global db_name, so poll describe against our own db here.
        deadline = time.time() + 120
        while time.time() < deadline:
            resp = describe_space(router_url, self.rm_db, self.rm_space)
            parts = resp.json().get("data", {}).get("partitions", [])
            if parts and sum(p.get("index_num", 0) for p in parts) >= self.doc_count:
                break
            time.sleep(5)

    def test_flush_then_remove_clears_dump_dir(self):
        # Force a dump so there are real files to clear (ordinary writes do not
        # dump: the periodic flush needs 10min/200k, and shutdown does not dump).
        resp = index_flush(router_url, self.rm_db, self.rm_space)
        assert resp.json()["code"] == 0, resp.json()
        time.sleep(2)

        partition_infos = get_partition_with_path(
            router_url, self.rm_db, self.rm_space)
        assert len(partition_infos) > 0
        base_path = get_partition_path_from_cluster_stats(router_url)
        assert base_path, "base path should not be empty"

        pid = partition_infos[0].get("id", 0)
        index_root = os.path.join(
            base_path, "data", str(pid), "retrieval_model_index")
        if not os.path.isdir(index_root):
            pytest.skip(
                f"engine data dir not visible on test host ({index_root}); "
                "run standalone with a host-visible data dir")

        before = _vector_dump_dirs(base_path, pid, "field_vector")
        assert before, \
            f"expected a field_vector dump dir after flush, found none under {index_root}"

        # Remove the vector index; RemoveVectorIndex must delete the dump dirs.
        resp = delete_space_index(router_url, self.rm_db, self.rm_space, "gamma")
        assert resp.json()["code"] == 0, resp.json()
        # Removal is async on the PS (raft enqueue -> maintenance worker runs
        # RemoveFieldIndexTask). Poll the disk until the dirs are gone.
        deadline = time.time() + 60
        remaining = before
        while time.time() < deadline:
            remaining = _vector_dump_dirs(base_path, pid, "field_vector")
            if not remaining:
                break
            time.sleep(1)
        assert not remaining, \
            f"dump dirs not cleared after index removal: {remaining}"

    def test_destroy_space(self):
        drop_space(router_url, self.rm_db, self.rm_space)

    def test_destroy_db(self):
        drop_db(router_url, self.rm_db)
