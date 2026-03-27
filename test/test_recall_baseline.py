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

import math
import pytest
import faiss
from utils.vearch_utils import *
from utils.data_utils import *

__description__ = """ test case for index recall compare to baseline of faiss """


# Define datasets at module level (similar to test_vector_index_ivfrabitq.py)
sift1m = DatasetSift1M()
sift_xb = sift1m.get_database()
sift_xq = sift1m.get_queries()
sift_gt = sift1m.get_groundtruth()

glove = DatasetGlove()
glove_xb = glove.get_database()
glove_xq = glove.get_queries()
glove_gt = glove.get_groundtruth()

nytimes = DatasetNytimes()
nytimes_xb = nytimes.get_database()
nytimes_xq = nytimes.get_queries()
nytimes_gt = nytimes.get_groundtruth()


def create(
    router_url,
    embedding_size,
    index_type="FLAT",
    store_type="MemoryOnly",
    index_params={},
):
    properties = {}
    properties["fields"] = [
        {
            "name": "field_int",
            "type": "integer",
        },
        {
            "name": "field_vector",
            "type": "vector",
            "dimension": embedding_size,
            "store_type": store_type,
            "index": {
                "name": "gamma",
                "type": index_type,
                "params": {
                    "metric_type": index_params["metric_type"],
                    "ncentroids": index_params["ncentroids"],
                    "training_threshold": index_params["training_threshold"],
                    "nprobe": index_params["nprobe"],
                    "efConstruction": index_params["efConstruction"],
                    "efSearch": index_params["efSearch"],
                    "nlinks": index_params["nlinks"],
                    "nb_bits": index_params["nb_bits"],
                },
            },
        },
    ]

    space_config = {
        "name": space_name,
        "partition_num": 1,
        "replica_num": 1,
        "fields": properties["fields"],
    }
    response = create_db(router_url, db_name)
    assert response.json()["code"] == 0

    response = create_space(router_url, db_name, space_config)
    assert response.json()["code"] == 0
    logger.info(response.json()["data"]["space_properties"]["field_vector"])


def create_faiss_index(index_type, index_params, xb):
    dimension = xb.shape[1]
    metric = None
    if index_params["metric_type"] == "L2":
        metric = faiss.METRIC_L2
    else:
        metric = faiss.METRIC_INNER_PRODUCT
    if index_type == "HNSW":
        index = faiss.IndexHNSWFlat(dimension, index_params["nlinks"], metric)
        index.hnsw.efConstruction = index_params["efConstruction"]
        index.hnsw.efSearch = index_params["efSearch"]

        index.add(xb)
        return index
    elif index_type == "IVFPQ":
        quantizer = faiss.IndexFlat(dimension, metric)
        index = faiss.IndexIVFPQ(
            quantizer,
            dimension,
            index_params["ncentroids"],
            int(dimension / 2),
            8,
            metric,
        )
        index.nprobe = index_params["nprobe"]

        index.train(xb[: index_params["training_threshold"]])

        index.add(xb)
        if index_params["verbose"]:
            logger.info("index.code_size: %d", index.code_size)
        return index
    elif index_type == "IVFFLAT":
        quantizer = faiss.IndexFlat(dimension, metric)
        index = faiss.IndexIVFFlat(
            quantizer, dimension, index_params["ncentroids"], metric
        )
        index.nprobe = index_params["nprobe"]

        index.train(xb[: index_params["training_threshold"]])

        index.add(xb)
        if index_params["verbose"]:
            logger.info("index.code_size: %d", index.code_size)
        return index
    elif index_type == "IVFRABITQ":
        quantizer = faiss.IndexFlat(dimension, metric)
        index = faiss.IndexIVFRaBitQ(
            quantizer, dimension, index_params["ncentroids"], metric, True, index_params["nb_bits"]
        )
        index.nprobe = index_params["nprobe"]
        index.train(xb[: index_params["training_threshold"]])
        index.add(xb)
        if index_params["verbose"]:
            logger.info("index.code_size: %d", index.code_size)
        return index
    elif index_type == "FLAT":
        index = faiss.IndexFlat(dimension, metric)
        index.add(xb)
        return index
    else:
        raise ValueError(f"Invalid index type: {index_type}")

def evaluate_faiss_index(index_type, index_params, xb, xq, gt, k):
    index = create_faiss_index(index_type, index_params, xb)

    nq = xq.shape[0]
    D, I = index.search(xq, k)

    recalls = {}
    i = 1
    while i <= k:
        recalls[i] = (I[:, :i] == gt[:, :1]).sum() / float(nq)
        i *= 10

    return recalls


def benchmark(index_type, store_type, index_params, xb, xq, gt):
    embedding_size = xb.shape[1]
    batch_size = 100
    k = 100

    total = xb.shape[0]
    total_batch = int(total / batch_size)
    logger.info(
        "dataset num: %d, total_batch: %d, dimension: %d, search num: %d, topK: %d"
        % (total, total_batch, embedding_size, xq.shape[0], k)
    )

    create(router_url, embedding_size, index_type, store_type, index_params)

    add(total_batch, batch_size, xb)
    if total - total_batch * batch_size:
        add(total - total_batch * batch_size, 1, xb[total_batch * batch_size :])

    waiting_index_finish(total, 15)

    query_dict = {
        "vectors": [],
        "vector_value": False,
        "fields": ["field_int"],
        "limit": k,
        "db_name": db_name,
        "space_name": space_name,
    }

    batch = True
    _, recalls = evaluate(xq, gt, k, batch, query_dict)
    result = "vearch %s: " % (index_type)
    for recall in recalls:
        result += "recall@%d = %.2f%% " % (recall, recalls[recall] * 100)
    logger.info(result)

    if (index_type == "IVFRABITQ" and index_params["nb_bits"] >= 2) or index_type != "IVFRABITQ":
        assert recalls[100] >= 0.9
        assert recalls[10] >= 0.8
        assert recalls[1] >= 0.5

    recalls = evaluate_faiss_index(index_type, index_params, xb, xq, gt, k)
    result = "faiss  %s: " % (index_type)
    for recall in recalls:
        result += "recall@%d = %.2f%% " % (recall, recalls[recall] * 100)
    logger.info(result)

    destroy(router_url, db_name, space_name)


@pytest.mark.parametrize(
    ["dataset_name", "metric_type"],
    [
        ["sift", "L2"],
        ["glove", "InnerProduct"],
        ["nytimes", "InnerProduct"],
    ],
)
def test_vearch_index_dataset_recall(dataset_name, metric_type):
    if dataset_name == "sift":
        xb, xq, gt = sift_xb, sift_xq, sift_gt
    elif dataset_name == "glove":
        xb, xq, gt = glove_xb, glove_xq, glove_gt
    elif dataset_name == "nytimes":
        xb, xq, gt = nytimes_xb, nytimes_xq, nytimes_gt
    else:
        raise ValueError(f"Unknown dataset: {dataset_name}")
    
    ncentroids = int(4 * math.sqrt(xb.shape[0]))
    if ncentroids * 39 > xb.shape[0]:
        ncentroids = int(xb.shape[0] / 39)
    training_threshold = 1
    index_params = {
        "metric_type": metric_type,
        "ncentroids": ncentroids,
        "training_threshold": training_threshold,
        "nprobe": 80,
        "efConstruction": 160,
        "efSearch": 100,
        "nlinks": 32,
        "nb_bits": 4,
        "verbose": False,
    }

    index_infos = [
        ["HNSW", "MemoryOnly"],
        ["IVFPQ", "RocksDB"],
        ["IVFFLAT", "RocksDB"],
        ["IVFRABITQ", "RocksDB"],
        # ["FLAT", "MemoryOnly"],
    ]
    for index_type, store_type in index_infos:
        if index_type == "IVFFLAT" or index_type == "IVFPQ" or index_type == "IVFRABITQ":
            index_params["training_threshold"] = ncentroids * 200
        if index_params["training_threshold"] > xb.shape[0]:
            index_params["training_threshold"] = xb.shape[0]
        benchmark(index_type, store_type, index_params, xb, xq, gt)


@pytest.mark.parametrize(
    ["dataset_name", "metric_type", "index_type", "store_type", "nb_bits"],
    [
        ["sift", "L2", "IVFFLAT", "RocksDB", 1],
        ["sift", "L2", "IVFPQ", "RocksDB", 1],
        ["sift", "L2", "IVFRABITQ", "RocksDB", 1],
        ["sift", "L2", "IVFRABITQ", "RocksDB", 2],
        ["sift", "L2", "IVFRABITQ", "RocksDB", 4],
        ["sift", "L2", "IVFRABITQ", "RocksDB", 8],
        ["glove", "InnerProduct", "IVFFLAT", "RocksDB", 1],
        ["glove", "InnerProduct", "IVFPQ", "RocksDB", 1],
        ["glove", "InnerProduct", "IVFRABITQ", "RocksDB", 1],
        ["glove", "InnerProduct", "IVFRABITQ", "RocksDB", 2],
        ["glove", "InnerProduct", "IVFRABITQ", "RocksDB", 4],
        ["glove", "InnerProduct", "IVFRABITQ", "RocksDB", 8],
        ["nytimes", "InnerProduct", "IVFFLAT", "RocksDB", 1],
        ["nytimes", "InnerProduct", "IVFPQ", "RocksDB", 1],
        ["nytimes", "InnerProduct", "IVFRABITQ", "RocksDB", 1],
        ["nytimes", "InnerProduct", "IVFRABITQ", "RocksDB", 2],
        ["nytimes", "InnerProduct", "IVFRABITQ", "RocksDB", 4],
        ["nytimes", "InnerProduct", "IVFRABITQ", "RocksDB", 8],
    ],
)
def test_vearch_ivf_series_recall(dataset_name, metric_type, index_type, store_type, nb_bits):
    """Test IVF series with configurable nb_bits and ncentroids parameters."""
    if dataset_name == "sift":
        xb, xq, gt = sift_xb, sift_xq, sift_gt
    elif dataset_name == "glove":
        xb, xq, gt = glove_xb, glove_xq, glove_gt
    elif dataset_name == "nytimes":
        xb, xq, gt = nytimes_xb, nytimes_xq, nytimes_gt
    else:
        raise ValueError(f"Unknown dataset: {dataset_name}")
    
    ncentroids = int(4 * math.sqrt(xb.shape[0]))
    if ncentroids * 39 > xb.shape[0]:
        ncentroids = int(xb.shape[0] / 39)
    training_threshold = 1
    if index_type == "IVFFLAT" or index_type == "IVFPQ" or index_type == "IVFRABITQ":
        training_threshold = ncentroids * 200
    if training_threshold > xb.shape[0]:
        training_threshold = xb.shape[0]
    index_params = {
        "metric_type": metric_type,
        "ncentroids": ncentroids,
        "training_threshold": training_threshold,
        "nprobe": 80,
        "efConstruction": 160,
        "efSearch": 100,
        "nlinks": 32,
        "nb_bits": nb_bits,
        "verbose": False,
    }
    benchmark(index_type, store_type, index_params, xb, xq, gt)
