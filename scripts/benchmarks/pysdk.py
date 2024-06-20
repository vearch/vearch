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


from multiprocessing import Pool
import logging
import time
import random
import json
import sys
import numpy as np
import argparse
import math
from vearch.core.vearch import Vearch
from vearch.config import Config
from vearch.schema.field import Field
from vearch.schema.space import SpaceSchema
from vearch.utils import DataType, MetricType, VectorInfo
from vearch.schema.index import (
    FlatIndex,
    ScalarIndex,
    HNSWIndex,
    IvfFlatIndex,
    IvfPQIndex,
)

from utils import parse_arguments, get_dataset_by_name, evaluate


__description__ = """ benchmark for pysdk"""


def str2MetricType(metric_type: str):
    if metric_type == "L2":
        return MetricType.L2
    else:
        return MetricType.Inner_product


def parseParams(args: argparse.Namespace):
    if args.index_type == "FLAT":
        return FlatIndex(
            "field_vector", str2MetricType(args.index_params["metric_type"])
        )

    elif args.index_type == "IVFFLAT":
        return IvfFlatIndex(
            "field_vector",
            str2MetricType(args.index_params["metric_type"]),
            args.index_params["ncentroids"],
        )
    elif args.index_type == "IVFPQ":
        return IvfPQIndex(
            "field_vector",
            int(args.index_params["ncentroids"] * 39),
            str2MetricType(args.index_params["metric_type"]),
            args.index_params["ncentroids"],
            args.index_params["nsubvector"],
        )
    elif args.index_type == "HNSW":
        return HNSWIndex(
            "field_vector",
            str2MetricType(args.index_params["metric_type"]),
            args.index_params["nlinks"],
            args.index_params["efConstruction"],
        )


def create_vearch_client(args: argparse.Namespace):
    config = Config(host=args.url, token=args.password)
    vc = Vearch(config)
    return vc


def create_db_and_space(args: argparse.Namespace):
    ret = vc.create_database(args.db)
    assert ret.code == 0

    field_int = Field(
        "field_int",
        DataType.INTEGER,
        desc="the integer type field",
        index=ScalarIndex("field_int"),
    )
    field_long = Field(
        "field_long",
        DataType.LONG,
        desc="the long type field",
        index=ScalarIndex("field_long"),
    )
    field_float = Field(
        "field_float",
        DataType.FLOAT,
        desc="the float type field",
        index=ScalarIndex("field_float"),
    )
    field_double = Field(
        "field_double",
        DataType.DOUBLE,
        desc="the double type field",
        index=ScalarIndex("field_double"),
    )
    field_string = Field(
        "field_string",
        DataType.STRING,
        desc="the string type field",
        index=ScalarIndex("field_string"),
    )
    field_vector = Field(
        "field_vector",
        DataType.VECTOR,
        parseParams(args),
        dimension=args.dimension,
    )
    space_schema = SpaceSchema(
        args.space,
        fields=[
            field_int,
            field_long,
            field_float,
            field_double,
            field_string,
            field_vector,
        ],
        partition_num=args.partition_num,
        replication_num=args.replica_num,
    )

    ret = vc.create_space(args.db, space_schema)
    assert ret.code == 0


def waiting_train_finish(args: argparse.Namespace, timewait: int = 5):
    if args.index_type == "FLAT" or args.index_type == "HNSW":
        return
    num = 0

    while num < args.partition_num:
        num = 0
        _, _, space = vc.is_space_exist(args.db, args.space)
        space = json.loads(space)
        partitions = space["partitions"]
        for p in partitions:
            num += p["index_status"]
        logger.debug("index status: %d" % (num))
        time.sleep(timewait)


def waiting_index_finish(args: argparse.Namespace, timewait: int = 5):
    if args.index_type == "FLAT":
        return
    num = 0
    while num < args.nb:
        num = 0
        _, _, space = vc.is_space_exist(args.db, args.space)
        space = json.loads(space)
        partitions = space["partitions"]
        for p in partitions:
            num += p["index_num"]
        logger.debug("index num: %d" % (num))
        time.sleep(timewait)


def process_upsert_data(items: tuple):
    args, index, size, features = items
    data = []
    for j in range(size):
        param_dict = {}
        param_dict["_id"] = str(index * args.batch_size + j)
        param_dict["field_int"] = index * args.batch_size + j
        if features is not None:
            param_dict["field_vector"] = features[j]
        else:
            param_dict["field_vector"] = [
                random.uniform(0, 1) for _ in range(args.dimension)
            ]
        param_dict["field_long"] = param_dict["field_int"]
        param_dict["field_float"] = float(param_dict["field_int"])
        param_dict["field_double"] = float(param_dict["field_int"])
        param_dict["field_string"] = str(param_dict["field_int"])
        data.append(param_dict)

    rs = vc.upsert(args.db, args.space, data)
    if rs.code != 0:
        logger.error(rs.msg)
    if len(rs.get_document_ids()) != size:
        logger.debug(rs.get_document_ids())
    assert len(rs.get_document_ids()) == size


def upsert(args: argparse.Namespace, xb: np.ndarray = None):
    pool = Pool(args.pool_size)
    total_data = []
    total_batch = int(args.nb / args.batch_size)

    if xb is not None:
        for i in range(total_batch):
            total_data.append(
                (
                    args,
                    i,
                    args.batch_size,
                    xb[i * args.batch_size : (i + 1) * args.batch_size].tolist(),
                )
            )

        remain = args.nb % args.batch_size
        if remain != 0:
            total_data.append(
                (
                    args,
                    total_batch,
                    remain,
                    xb[total_batch * args.batch_size :].tolist(),
                )
            )
    else:
        for i in range(total_batch):
            total_data.append((args, i, args.batch_size, None))

        remain = args.nb % args.batch_size
        if remain != 0:
            total_data.append((args, total_batch, remain, None))

    start = time.time()
    results = pool.map(process_upsert_data, total_data)
    pool.close()
    pool.join()
    end = time.time()

    _, _, space = vc.is_space_exist(args.db, args.space)
    total = json.loads(space)["doc_num"]
    logger.info(
        "nb: %d, batch size:%d, upsert cost: %.4f seconds, QPS: %.4f, pool size: %d"
        % (
            total,
            args.batch_size,
            end - start,
            total / (end - start),
            args.pool_size,
        )
    )


def get_timewait(args: argparse.Namespace):
    if args.nb <= 100 * 10000:
        return 1
    elif args.nb <= 1000 * 10000:
        return 5
    else:
        return 10


def train_and_build_index(args: argparse.Namespace):
    timewait = get_timewait(args)
    start = time.time()
    waiting_train_finish(args, timewait)
    end = time.time()

    logger.info(
        "nb: %d, batch size:%d, train index cost: %.4f seconds, QPS: %.4f"
        % (
            args.nb,
            args.batch_size,
            end - start,
            args.nb / (end - start) if (end - start) >= 0.001 else 0,
        )
    )

    start = time.time()
    waiting_index_finish(args, timewait)
    end = time.time()

    logger.info(
        "nb: %d, batch size:%d, build index cost: %.4f seconds, QPS: %.4f"
        % (
            args.nb,
            args.batch_size,
            end - start,
            args.nb / (end - start) if (end - start) >= 0.001 else 0,
        )
    )


def process_query_data(items: tuple):
    args, unique_keys = items
    rs = vc.query(
        args.db, args.space, document_ids=unique_keys, vector=args.vector_value
    )

    if len(rs.documents) != args.batch_size:
        logger.debug(rs.documents)
    assert len(rs.documents) == args.batch_size


def query(args: argparse.Namespace):
    pool = Pool(args.pool_size)
    total_data = []
    # There may be some left, but won't deal with it
    total_batch = int(args.nq / args.batch_size)
    unique_ids = random.sample(range(0, args.nb), args.nq)
    unique_keys = [str(i) for i in unique_ids]
    for i in range(total_batch):
        total_data.append(
            (args, unique_keys[i * args.batch_size : (i + 1) * args.batch_size])
        )

    start = time.time()
    results = pool.map(process_query_data, total_data)
    pool.close()
    pool.join()
    end = time.time()

    logger.info(
        "nq: %d, batch size:%d, query cost: %.4f seconds, QPS: %.4f, pool size: %d, vector value: %d"
        % (
            args.nq,
            args.batch_size,
            end - start,
            args.nq / (end - start),
            args.pool_size,
            args.vector_value,
        )
    )


def process_delete_data(items: tuple):
    args, unique_keys = items
    rs = vc.delete(args.db, args.space, unique_keys)

    if len(rs.document_ids) != args.batch_size:
        logger.debug(rs.document_ids)
    assert len(rs.document_ids) == args.batch_size


def delete(args: argparse.Namespace):
    pool = Pool(args.pool_size)
    total_data = []
    # There may be some left, but won't deal with it
    total_batch = int(args.nq / args.batch_size)
    unique_ids = random.sample(range(0, args.nb), args.nq)
    unique_keys = [str(i) for i in unique_ids]
    for i in range(total_batch):
        total_data.append(
            (args, unique_keys[i * args.batch_size : (i + 1) * args.batch_size])
        )

    start = time.time()
    results = pool.map(process_delete_data, total_data)
    pool.close()
    pool.join()
    end = time.time()

    logger.info(
        "nq: %d, batch size:%d, delete cost: %.4f seconds, QPS: %.4f, pool size: %d"
        % (
            args.nq,
            args.batch_size,
            end - start,
            args.nq / (end - start),
            args.pool_size,
        )
    )


def process_search_data(items: tuple):
    args, index, features = items
    vector_info = VectorInfo("field_vector", features)
    rs = vc.search(
        args.db,
        args.space,
        vector_infos=[vector_info],
        vector=args.vector_value,
        limit=args.limit,
    )
    if rs.code != 0:
        logger.error(rs.msg)
    if len(rs.documents) != args.batch_size:
        logger.error(
            "search result length should be %d, but is %d"
            % (args.batch_size, len(rs.documents))
        )
    assert len(rs.documents) == args.batch_size

    return index, rs.documents


def search(args: argparse.Namespace, xq: np.ndarray, gt: np.ndarray):
    pool = Pool(args.pool_size)
    total_data = []
    total_batch = int(args.nq / args.batch_size)
    for i in range(total_batch):
        total_data.append(
            (
                args,
                i,
                xq[i * args.batch_size : (i + 1) * args.batch_size].flatten().tolist(),
            )
        )

    start = time.time()
    results = pool.map(process_search_data, total_data)
    pool.close()
    pool.join()
    end = time.time()

    recall_str = ""
    if args.recall:
        search_results = np.empty(gt.shape)
        for result in results:
            batch_index, documents = result
            for i in range(len(documents)):
                for j in range(len(documents[i])):
                    search_results[i + batch_index * args.batch_size][j] = documents[i][
                        j
                    ]["_id"]

        recalls = evaluate(search_results, gt, args.limit)
        for recall in recalls:
            recall_str += "Recall@" + str(recall) + "=" + str(recalls[recall]) + ", "

    logger.info(
        "nq: %d, batch size:%d, search cost: %.4f seconds, QPS: %.4f, %spool size: %d"
        % (
            args.nq,
            args.batch_size,
            end - start,
            args.nq / (end - start),
            recall_str,
            args.pool_size,
        )
    )


def run_normal(args: argparse.Namespace):
    """crud and search for specify dataset"""

    xb, xq, gt = get_dataset_by_name(logger, args)

    args_str = ", ".join(f"{key}={value}" for key, value in vars(args).items())
    logger.info(f"args: {args_str}")

    create_db_and_space(args)

    upsert(args, xb)

    train_and_build_index(args)

    query(args)

    batch_size = args.batch_size
    args.batch_size = 1
    search(args, xq, gt)

    args.batch_size = batch_size
    delete(args)

    vc.drop_space(args.db, args.space)
    vc.drop_database(args.db)


def run_similar_search(
    args: argparse.Namespace, xb: np.ndarray, xq: np.ndarray, gt: np.ndarray
):
    """vector search"""

    xb, xq, gt = get_dataset_by_name(logger, args)

    args_str = ", ".join(f"{key}={value}" for key, value in vars(args).items())
    logger.info(f"args: {args_str}")

    create_db_and_space(args)

    upsert(args, xb)

    train_and_build_index(args)

    query(args)

    batch_size = args.batch_size
    args.batch_size = 1
    args.trace = True
    search(args, xq, gt)

    args.batch_size = batch_size
    args.trace = False
    delete(args)

    vc.drop_space(args.db, args.space)
    vc.drop_database(args.db)


def run_crud(args: argparse.Namespace):
    """crud means create read update delete"""
    args_str = ", ".join(f"{key}={value}" for key, value in vars(args).items())
    logger.info(f"args: {args_str}")

    create_db_and_space(args)

    upsert(args)

    query(args)

    batch_size = args.batch_size
    args.batch_size = 1
    query(args)
    args.batch_size = batch_size

    args.vector_value = False
    query(args)

    batch_size = args.batch_size
    args.batch_size = 1
    query(args)
    args.batch_size = batch_size
    args.vector_value = True

    delete(args)

    vc.drop_space(args.db, args.space)
    vc.drop_database(args.db)


def run_task(args: argparse.Namespace):
    if args.task == "CRUD":
        args.dataset = "random"
        dimensions = [128, 756, 1536]
        args.index_type = "IVFPQ"
        ncentroids = int(4 * math.sqrt(args.nb))
        args.index_params = {
            "metric_type": "L2",
            "training_threshold": args.nb + 1,
            "ncentroids": ncentroids,
        }
        for dimension in dimensions:
            args.dimension = dimension
            run_crud(args)

    if args.task == "SEARCH":
        index_types = ["IVFFLAT", "IVFPQ", "HNSW"]
        xb, xq, gt = get_dataset_by_name(logger, args)

        for index_type in index_types:
            args.index_type = index_type
            run_similar_search(args, xb, xq, gt)

    if args.task == "NORMAL":
        run_normal(args)


if __name__ == "__main__":
    args = parse_arguments()

    logger = logging.getLogger(__name__)
    logger.setLevel(args.log_level)
    formatter = logging.Formatter(
        "%(asctime)s %(name)s:%(lineno)s %(levelname)s %(message)s"
    )
    if args.output != "":
        handler = logging.FileHandler(args.output, "a")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    else:
        handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    vc = create_vearch_client(args)
    run_task(args)
