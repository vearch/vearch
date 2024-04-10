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
import logging
import faiss
from utils.vearch_utils import *
from utils.data_utils import *

logging.basicConfig()
logger = logging.getLogger(__name__)

__description__ = """ test case for index recall compare to baseline of faiss """


index_params = {
    "metric_type": "L2",
    "ncentroids": 1024,
    "training_threshold": 1,
    "nprobe": 80,
    "efConstruction": 160,
    "efSearch": 100,
    "nlinks": 32    
}


def create(router_url, embedding_size, index_type="FLAT", store_type="MemoryOnly", metric_type="L2"):
    index_params["metric_type"] = metric_type
    if index_type == "IVFFLAT" or index_type == "IVFPQ":
        index_params["training_threshold"] = index_params["ncentroids"] * 39

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
                    "nlinks": index_params["nlinks"]
                }
            },
        }
    ]

    space_config = {
        "name": space_name,
        "partition_num": 1,
        "replica_num": 1,
        "fields": properties["fields"]
    }
    response = create_db(router_url, db_name)
    assert response["code"] == 200

    response = create_space(router_url, db_name, space_config)
    assert response["code"] == 200
    logger.info(response["data"]["space_properties"]["field_vector"])

def create_faiss_index(index_type, metric_type, xb):
    dimension = xb.shape[1]
    metric = None
    if metric_type == "L2":
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
        quantizer = faiss.IndexFlatL2(dimension)
        index = faiss.IndexIVFPQ(quantizer, dimension, index_params["ncentroids"], int(dimension / 2), 8, metric)
        index.nprobe = index_params["nprobe"]

        index.train(xb[index_params["training_threshold"]:])

        index.add(xb)
        return index
    elif index_type == "IVFFLAT":
        quantizer = faiss.IndexFlatL2(dimension)
        index = faiss.IndexIVFFlat(quantizer, dimension, index_params["ncentroids"], metric)
        index.nprobe = index_params["nprobe"]

        index.train(xb[index_params["training_threshold"]:])

        index.add(xb)
        return index


def evaluate_faiss_index(index_type, metric_type, xb, xq, gt, k):
    index = create_faiss_index(index_type, metric_type, xb)

    nq = xq.shape[0]
    D, I = index.search(xq, k)

    recalls = {}
    i = 1
    while i <= k:
        recalls[i] = (I[:, :i] == gt[:, :1]).sum() / float(nq)
        i *= 10

    return recalls

def benchmark(index_type, store_type, metric_type, xb, xq, gt):
    embedding_size = xb.shape[1]
    batch_size = 100
    k = 100

    total = xb.shape[0]
    total_batch = int(total / batch_size)
    logger.info("dataset num: %d, total_batch: %d, dimension: %d, search num: %d, topK: %d" % (
        total, total_batch, embedding_size, xq.shape[0], k))

    create(router_url, embedding_size, index_type, store_type, metric_type)

    add(total_batch, batch_size, xb)
    if total - total_batch * batch_size:
        add(total - total_batch * batch_size, 1, xb[total_batch * batch_size:])

    waiting_index_finish(logger, total, 15)

    query_dict = {
        "vectors": [],
        "vector_value": False,
        "fields": ["field_int"],
        "limit": k,
        "db_name": db_name,
        "space_name": space_name,
    }

    batch = True
    _, recalls = evaluate(xq, gt, k, batch, query_dict, logger)
    result = "vearch %s: " %(index_type)
    for recall in recalls:
        result += "recall@%d = %.2f%% " % (recall, recalls[recall] * 100)
    logger.info(result)

    # assert recalls[100] >= 0.95
    assert recalls[10] >= 0.8
    assert recalls[1] >= 0.5

    recalls = evaluate_faiss_index(index_type, metric_type, xb, xq, gt, k)
    result = "faiss  %s: " %(index_type)
    for recall in recalls:
        result += "recall@%d = %.2f%% " % (recall, recalls[recall] * 100)
    logger.info(result)

    destroy(router_url, db_name, space_name)


@ pytest.mark.parametrize(["dataset_name", "metric_type"], [
    ["sift", "L2"],
    ["glove", "InnerProduct"],
    ["nytimes", "InnerProduct"],
])
def test_vearch_index_dataset_recall(dataset_name, metric_type):
    xb, xq, gt = get_dataset_by_name(logger, dataset_name)

    index_infos = [
        ["HNSW", "MemoryOnly"],
        ["IVFPQ", "RocksDB"],
        ["IVFFLAT", "RocksDB"],
    ]
    for index_type, store_type in index_infos:
        benchmark(index_type, store_type, metric_type, xb, xq, gt)
