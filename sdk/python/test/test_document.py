import logging
from typing import List
import json
import pytest
import time
from vearch.config import Config
from vearch.core.vearch import Vearch
from vearch.schema.field import Field
from vearch.schema.space import SpaceSchema
from vearch.utils import DataType, MetricType, VectorInfo
from vearch.schema.index import FlatIndex, Index, ScalarIndex
from vearch.filter import Filter, Condition, FieldValue, Conditions
from vearch.exception import (
    DatabaseException,
    VearchException,
    SpaceException,
    DocumentException,
)
from vearch.core.client import RestClient
from config import test_host_url

logger = logging.getLogger("vearch_document_test")

database_name = "database_document_test"
space_name = "space_document_test"

vc = Vearch(Config(host=test_host_url))


def create_space_schema(space_name) -> SpaceSchema:
    book_name = Field(
        "book_name",
        DataType.STRING,
        desc="the name of book",
        index=ScalarIndex("book_name_idx"),
    )
    book_num = Field(
        "book_num",
        DataType.INTEGER,
        desc="the num of book",
        index=ScalarIndex("book_num_idx"),
    )
    book_vector = Field(
        "book_character",
        DataType.VECTOR,
        FlatIndex("book_vec_idx", MetricType.Inner_product),
        dimension=512,
    )
    ractor_address = Field(
        "ractor_address", DataType.STRING, desc="the place of the book put"
    )
    space_schema = SpaceSchema(
        space_name, fields=[book_name, book_num, book_vector, ractor_address]
    )
    return space_schema


def test_create_database():
    logger.debug(vc.client.host)
    ret = vc.create_database(database_name)
    logger.debug(ret.dict_str())
    assert ret.__dict__["code"] == 0


def test_create_space():
    ret = vc.create_space(database_name, create_space_schema(space_name))
    assert ret.data["name"] == space_name


def test_upsert_doc_data_field_missing():
    import random

    ractor = ["ractor_logical", "ractor_industry", "ractor_philosophy"]
    book_name_template = "abcdefghijklmnopqrstuvwxyz0123456789"
    data = []
    num = [12, 34, 56, 74, 53, 11, 14, 9]
    for i in range(8):
        book_item = [
            "".join(random.choices(book_name_template, k=8)),
            num[i],
            ractor[random.randint(0, 2)],
        ]
        data.append(book_item)

    ur = vc.upsert(database_name, space_name, data)
    assert ur.code != 0
    logger.debug(ur.msg)


def test_upsert_doc() -> List:
    import random

    ractor = ["ractor_logical", "ractor_industry", "ractor_philosophy"]
    book_name_template = "abcdefghijklmnopqrstuvwxyz0123456789"
    data = []
    num = [12, 34, 56, 74, 53, 11, 14, 9]
    for i in range(8):
        book_item = [
            "".join(random.choices(book_name_template, k=8)),
            num[i],
            [random.uniform(0, 1) for _ in range(512)],
            ractor[random.randint(0, 2)],
        ]
        data.append(book_item)
        logger.debug(book_item)
    ret = vc.upsert(database_name, space_name, data)
    assert len(ret.get_document_ids()) == 8


def test_delete_doc():
    time.sleep(1)
    conditons = [
        Condition(operator="<", fv=FieldValue(field="book_num", value=25)),
        Condition(operator=">", fv=FieldValue(field="book_num", value=12)),
    ]
    filters = Filter(operator="AND", conditions=conditons)
    ret = vc.delete(database_name, space_name, filter=filters)
    logger.debug(ret.document_ids)
    assert ret is None or len(ret.document_ids) >= 0


def test_delete_doc_no_result():
    conditons = [
        Condition(operator="<", fv=FieldValue(field="book_num", value=25)),
        Condition(operator=">", fv=FieldValue(field="book_num", value=12)),
    ]
    filters = Filter(operator="AND", conditions=conditons)
    ret = vc.delete(database_name, space_name, filter=filters)
    assert len(ret.document_ids) == 0


def test_query():
    conditons = [
        Condition(operator=">", fv=FieldValue(field="book_num", value=0)),
        Condition(
            operator="IN",
            fv=FieldValue(
                field="book_name", value=["bpww57nu", "sykboivx", "edjn9542"]
            ),
        ),
    ]
    filters = Filter(operator="AND", conditions=conditons)
    ret = vc.query(database_name, space_name, filter=filters)
    logger.debug(ret.documents)
    assert len(ret.documents) >= 0


def test_query_no_result():
    conditons = [
        Condition(operator=">", fv=FieldValue(field="book_num", value=0)),
        Condition(
            operator="IN",
            fv=FieldValue(
                field="book_name", value=["bpww57nu", "sykboivx", "edjn9542"]
            ),
        ),
    ]
    filters = Filter(operator="AND", conditions=conditons)
    ret = vc.query(database_name, space_name, filter=filters)
    assert ret.code == 0 and len(ret.documents) == 0


def test_search():
    import random

    feature = [random.uniform(0, 1) for _ in range(512)]
    vi = VectorInfo("book_character", feature)
    conditons = [Condition(operator=">", fv=FieldValue(field="book_num", value=0))]
    ret = vc.search(
        database_name,
        space_name,
        vector_infos=[
            vi,
        ],
        limit=7,
    )
    assert len(ret.documents[0]) >= 7


def test_multi_search():
    import random

    limit = 7
    feature = [random.uniform(0, 1) for _ in range(512 * 2)]
    vi = VectorInfo("book_character", feature)
    conditons = [Condition(operator=">", fv=FieldValue(field="book_num", value=0))]
    ret = vc.search(
        database_name,
        space_name,
        vector_infos=[
            vi,
        ],
        limit=limit,
    )
    assert len(ret.documents) == 2
    logger.debug(ret.documents)
    for document in ret.documents:
        assert len(document) == limit


def test_search_no_result():
    import random

    feature = [random.uniform(0, 1) for _ in range(512)]
    vi = VectorInfo("bad_name", feature)
    conditons = [Condition(operator=">", fv=FieldValue(field="book_num", value=0))]
    filters = Filter(operator="AND", conditions=conditons)

    ret = vc.search(
        database_name,
        space_name,
        vector_infos=[
            vi,
        ],
        filter=filters,
        limit=7,
    )
    logger.debug(ret.msg)
    assert not ret.is_success()

def test_drop_space():
    ret = vc.drop_space(database_name, space_name)
    assert ret.__dict__["code"] == 0


def test_drop_database():
    logger.debug(vc.client.host)
    ret = vc.drop_database(database_name)
    assert ret.__dict__["code"] == 0
