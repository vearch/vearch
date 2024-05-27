from vearch.config import Config
from vearch.core.db import Database
from vearch.core.space import Space
from vearch.schema.field import Field
from vearch.schema.space import SpaceSchema
from vearch.utils import DataType, MetricType, VectorInfo
from vearch.schema.index import IvfPQIndex, Index, ScalarIndex
from vearch.filter import Filter, Condition, FieldValue, Conditions
from vearch.exception import DatabaseException, VearchException, SpaceException, DocumentException

import logging
from typing import List
import json
import pytest
logger = logging.getLogger("db_test")

database_name = "database_test_db1"
database_name1 = "database_test_db_not_exist"
space_name = "book_info"
space_name1 = "book_info1"

db = Database(database_name)
db_not = Database(database_name1)


def test_is_database_not_exist():
    ret = db_not.exist()
    assert ret == False


def test_create_database():
    ret = db.create()
    logger.debug(ret)
    assert ret.__dict__["code"] == 0


def test_is_database_exist():
    ret = db.exist()
    assert ret == True


def create_space_schema(space_name) -> SpaceSchema:

    book_name = Field("book_name", DataType.STRING,
                      desc="the name of book", index=ScalarIndex("book_name_idx"))
    book_num = Field("book_num", DataType.INTEGER,
                     desc="the num of book", index=ScalarIndex("book_num_idx"))
    book_vector = Field("book_character", DataType.VECTOR,
                        IvfPQIndex("book_vec_idx", 10000, MetricType.Inner_product, 2048, 8), dimension=512)
    ractor_address = Field("ractor_address", DataType.STRING,
                           desc="the place of the book put")
    space_schema = SpaceSchema(
        space_name, fields=[book_name, book_num, book_vector, ractor_address])
    return space_schema


def test_create_space():
    ret = db.create_space(create_space_schema("book_info"))
    assert ret.data["name"] == "book_info"


def test_list_spaces():
    ret = db.list_spaces()
    logger.debug(ret)
    assert ret.__dict__["code"] == 0


def test_drop_db_not_empty():
    ret = db.drop()
    assert ret.code != 0


def test_drop_db():
    space = Space(database_name, space_name)
    ret = space.drop()
    assert ret.__dict__["code"] == 0

    ret = db.drop()
    assert ret.__dict__["code"] == 0
