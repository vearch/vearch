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
from vearch.core.vearch import Vearch
from vearch.config import Config
from vearch.schema.field import Field
from vearch.schema.space import SpaceSchema
from vearch.utils import DataType, MetricType, VectorInfo
from vearch.schema.index import FlatIndex, ScalarIndex

from utils import parse_arguments

logging.basicConfig()
logger = logging.getLogger(__name__)


__description__ = """ benchmark for pysdk"""


def create_db_and_space(args):
    config = Config(host=args.url, token=args.password)
    vc = Vearch(config)

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
        FlatIndex("field_vector", MetricType.L2),
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
        partition_num=args.partition,
        replication_num=args.replica,
    )

    ret = vc.create_space(args.db, space_schema)
    assert ret.code == 0
    return vc


def process_upsert_data(items):
    args, index = items
    data = []
    for j in range(args.batch):
        param_dict = {}
        param_dict["_id"] = str(index * args.batch + j)
        param_dict["field_int"] = index * args.batch + j
        param_dict["field_vector"] = [
            random.uniform(0, 1) for _ in range(args.dimension)
        ]
        param_dict["field_long"] = param_dict["field_int"]
        param_dict["field_float"] = float(param_dict["field_int"])
        param_dict["field_double"] = float(param_dict["field_int"])
        param_dict["field_string"] = str(param_dict["field_int"])
        data.append(param_dict)

    ret = vc.upsert(args.db, args.space, data)
    assert len(ret.get_document_ids()) != 0


def upsert(args):
    pool = Pool(args.pool)
    total_data = []
    total_batch = int(args.nb / args.batch)
    for i in range(total_batch):
        total_data.append((args, i))

    start = time.time()
    results = pool.map(process_upsert_data, total_data)
    pool.close()
    pool.join()

    end = time.time()
    if args.verbose:
        exist, result, space = vc.is_space_exist(args.db, args.space)
        total = json.loads(space)["doc_num"]
        logger.info(
            "nb: %d, batch size:%d, upsert cost: %.4f seconds, QPS: %.4f, pool size: %d, partition num: %d, replica num: %d"
            % (
                total,
                args.batch,
                end - start,
                total / (end - start),
                args.pool,
                args.partition,
                args.replica,
            )
        )


def process_query_data(items):
    args, unique_keys = items
    rs = vc.query(args.db, args.space, unique_keys)

    if len(rs.documents) != args.batch:
        logger.debug(rs.documents)
    assert len(rs.documents) == args.batch


def query(args):
    pool = Pool(args.pool)
    total_data = []
    total_batch = int(args.nq / args.batch)
    unique_ids = random.sample(range(0, args.nb), args.nq)
    unique_keys = [str(i) for i in unique_ids]
    for i in range(total_batch):
        total_data.append((args, unique_keys[i * args.batch : (i + 1) * args.batch]))

    start = time.time()
    results = pool.map(process_query_data, total_data)
    pool.close()
    pool.join()

    end = time.time()
    if args.verbose:
        logger.info(
            "nq: %d, batch size:%d, query cost: %.4f seconds, QPS: %.4f, pool size: %d, partition num: %d, replica num: %d"
            % (
                args.nq,
                args.batch,
                end - start,
                args.nq / (end - start),
                args.pool,
                args.partition,
                args.replica,
            )
        )


def process_delete_data(items):
    args, unique_keys = items
    rs = vc.delete(args.db, args.space, unique_keys)

    if len(rs.document_ids) != args.batch:
        logger.debug(rs.document_ids)
    assert len(rs.document_ids) == args.batch


def delete(args):
    pool = Pool(args.pool)
    total_data = []
    total_batch = int(args.nq / args.batch)
    unique_ids = random.sample(range(0, args.nb), args.nq)
    unique_keys = [str(i) for i in unique_ids]
    for i in range(total_batch):
        total_data.append((args, unique_keys[i * args.batch : (i + 1) * args.batch]))

    start = time.time()
    results = pool.map(process_delete_data, total_data)
    pool.close()
    pool.join()
    end = time.time()
    if args.verbose:
        logger.info(
            "nq: %d, batch size:%d, delete cost: %.4f seconds, QPS: %.4f, pool size: %d, partition num: %d, replica num: %d"
            % (
                args.nq,
                args.batch,
                end - start,
                args.nq / (end - start),
                args.pool,
                args.partition,
                args.replica,
            )
        )


if __name__ == "__main__":
    args = parse_arguments()
    logger.setLevel(args.log)

    vc = create_db_and_space(args)

    upsert(args)

    query(args)

    delete(args)

    vc.drop_space(args.db, args.space)
    vc.drop_database(args.db)
