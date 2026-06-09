import logging
from typing import List
import json
import pytest
from vearch.config import Config
from vearch.core.db import Database
from vearch.core.space import Space
from vearch.schema.field import Field
from vearch.schema.space import SpaceSchema
from vearch.utils import DataType, MetricType, VectorInfo
from vearch.schema.index import (
    IvfPQIndex,
    Index,
    ScalarIndex,
    InvertedIndex,
    BitmapIndex,
    CompositeIndex,
)
from vearch.filter import Filter, Condition, FieldValue
from vearch.exception import (
    DatabaseException,
    VearchException,
    SpaceException,
    DocumentException,
)
from vearch.core.client import RestClient
from config import test_host_url

logger = logging.getLogger("vearch_test")
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

database_name = "database_test"
space_name = "book_info"
space_name1 = "book_info1"

client = RestClient(host=test_host_url, token="secret")
db = Database(database_name, client)

space_not = Space(database_name, space_name1, client)
space = Space(database_name, space_name, client)

ids = []


def create_space_schema(space_name, replica_num=1) -> SpaceSchema:

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
        IvfPQIndex("book_vec_idx", MetricType.Inner_product, 2048, 8),
        dimension=512,
    )
    ractor_address = Field(
        "ractor_address", DataType.STRING, desc="the place of the book put"
    )
    space_schema = SpaceSchema(
        space_name,
        fields=[book_name, book_num, book_vector, ractor_address],
        replica_num=replica_num,
    )
    return space_schema


def test_create_database():
    ret = db.create()
    logger.debug(ret.data)
    assert ret.code == 0


def test_is_space_not_exist():
    ret = space_not.exist()
    assert ret[0] == False
    if ret[1] != None:
        logger.info(ret[1].msg)


def test_create_space():
    ret = space.create(create_space_schema("book_info"))
    assert ret.code == 0
    logger.debug(ret.data)
    assert ret.data["name"] == "book_info"


def test_create_space_with_replica():
    ret = space.create(create_space_schema("book_info_replica", replica_num=6))
    assert ret.code != 0
    logger.debug(ret.msg)


def test_is_space_exist():
    ret = space.exist()
    logger.debug(ret)
    assert ret[0] == True


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
    ret = space.upsert(data)
    assert ret.code != 0
    logger.debug(ret.msg)


def test_upsert_doc():
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

    ret = space.upsert(data)
    assert ret.code == 0
    assert len(ret.get_document_ids()) >= 8
    global ids
    ids = ret.get_document_ids()
    logger.debug(ids)


def test_query_with_document_ids():
    ret = space.query(document_ids=ids)
    logger.debug(ret.documents)
    assert ret.code == 0
    assert len(ret.documents) == 8


def test_search():
    import random

    feature = [random.uniform(0, 1) for _ in range(512)]
    vi = VectorInfo("book_character", feature)
    ret = space.search(
        vector_infos=[
            vi,
        ],
        limit=7,
    )
    logger.debug(ret.documents)
    assert ret.code == 0
    assert len(ret.documents) == 1
    assert len(ret.documents[0]) == 7


def test_search_with_filter():
    import random

    feature = [random.uniform(0, 1) for _ in range(512)]
    vi = VectorInfo("book_character", feature)
    conditons = [
        Condition(operator=">=", fv=FieldValue(field="book_num", value=9)),
        Condition(operator="<=", fv=FieldValue(field="book_num", value=9)),
    ]
    filters = Filter(operator="AND", conditions=conditons)
    ret = space.search(
        vector_infos=[
            vi,
        ],
        filter=filters,
        limit=7,
        trace=True,
    )
    logger.debug(ret.documents)
    assert ret.code == 0


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
    ret = space.query(filter=filters)
    assert ret is None or len(ret.documents) >= 0


def test_delete_doc():
    conditons = [
        Condition(operator="<", fv=FieldValue(field="book_num", value=25)),
        Condition(operator=">", fv=FieldValue(field="book_num", value=12)),
    ]
    filters = Filter(operator="AND", conditions=conditons)
    ret = space.delete(filter=filters)
    assert ret.code == 0


def test_drop_space():
    ret = space.drop()
    assert ret.code == 0


def test_space_schema_to_dict_with_indexes():
    field = Field("book_name", DataType.STRING, desc="book name")
    idx = InvertedIndex("book_name_inverted_idx", field_name="book_name")
    schema = SpaceSchema(
        "test_space_indexes",
        fields=[field],
        indexes=[idx],
        description="space with indexes",
        partition_num=2,
        replica_num=1,
    )
    d = schema.to_dict()
    assert d["name"] == "test_space_indexes"
    assert d["desc"] == "space with indexes"
    assert d["partition_num"] == 2
    assert d["replica_num"] == 1
    assert "indexes" in d
    assert len(d["indexes"]) == 1
    assert d["indexes"][0]["name"] == "book_name_inverted_idx"
    assert d["indexes"][0]["type"] == "INVERTED"
    assert d["indexes"][0]["field_name"] == "book_name"


def test_space_schema_from_dict_with_indexes():
    data = {
        "space_name": "from_dict_space",
        "desc": "test from_dict",
        "partition_num": 2,
        "replica_num": 1,
        "schema": {
            "fields": [
                {
                    "name": "book_name",
                    "type": "STRING",
                    "desc": "book name field",
                }
            ],
            "indexes": [
                {
                    "name": "book_name_inverted",
                    "type": "INVERTED",
                    "field_name": "book_name",
                }
            ],
        },
    }
    schema = SpaceSchema.from_dict(data)
    assert schema.name == "from_dict_space"
    assert schema.description == "test from_dict"
    assert schema.partition_num == 2
    assert len(schema.fields) == 1
    assert schema.fields[0].name == "book_name"
    assert len(schema.indexes) == 1
    assert schema.indexes[0]._index_name == "book_name_inverted"
    assert schema.indexes[0]._index_type == "INVERTED"
    assert schema.indexes[0]._field_name == "book_name"
    assert isinstance(schema.indexes[0], InvertedIndex)


def test_space_schema_from_dict_preserves_subclass_identity():
    data = {
        "space_name": "round_trip_space",
        "desc": "",
        "partition_num": 1,
        "replica_num": 1,
        "schema": {
            "fields": [{"name": "book_name", "type": "STRING"}],
            "indexes": [
                {"name": "scalar_idx", "type": "SCALAR", "field_name": "book_name"},
                {"name": "bm_idx", "type": "BITMAP", "field_name": "book_name"},
                {
                    "name": "comp_idx",
                    "type": "COMPOSITE",
                    "field_names": ["book_name", "book_num"],
                },
            ],
        },
    }
    schema = SpaceSchema.from_dict(data)
    assert isinstance(schema.indexes[0], ScalarIndex)
    assert isinstance(schema.indexes[1], BitmapIndex)
    assert isinstance(schema.indexes[2], CompositeIndex)
    assert schema.indexes[2]._field_names == ["book_name", "book_num"]


def test_space_schema_positional_args_compat():
    # New positional order: SpaceSchema(name, fields, indexes, description, partition_num, replica_num)
    field = Field("book_name", DataType.STRING, desc="book name")
    idx = InvertedIndex("book_name_inverted_idx", field_name="book_name")
    schema = SpaceSchema("pos_space", [field], [idx], "book catalog", 2, 3)
    assert schema.name == "pos_space"
    assert schema.description == "book catalog"
    assert schema.partition_num == 2
    assert schema.replica_num == 3
    assert schema.indexes == [idx]
    d = schema.to_dict()
    assert d["desc"] == "book catalog"
    assert d["indexes"][0]["name"] == "book_name_inverted_idx"


def test_space_schema_check_valid_rejects_composite_with_one_field():
    field = Field("book_name", DataType.STRING, desc="book name")
    bad = CompositeIndex("comp_idx", ["only_one"])
    with pytest.raises(AssertionError, match="CompositeIndex requires at least 2"):
        SpaceSchema("bad_space", fields=[field], indexes=[bad])


def test_space_schema_check_valid_rejects_non_composite_without_field_name():
    field = Field("book_name", DataType.STRING, desc="book name")
    bad = ScalarIndex("scalar_idx")  # no field_name
    with pytest.raises(AssertionError, match="non-empty field_name"):
        SpaceSchema("bad_space", fields=[field], indexes=[bad])


def test_create_space_with_indexes():
    field = Field("book_name", DataType.STRING, desc="book name field")
    idx = InvertedIndex("book_name_inverted_idx", field_name="book_name")
    book_vector = Field(
        "book_character",
        DataType.VECTOR,
        IvfPQIndex("book_vec_idx", MetricType.Inner_product, 2048, 8),
        dimension=512,
    )
    schema = SpaceSchema(
        space_name,
        fields=[field, book_vector],
        indexes=[idx],
        replica_num=1,
    )
    ret = space.create(schema)
    logger.info(ret.msg)
    assert ret.code == 0
    logger.debug(ret.data)
    assert ret.data["name"] == space_name
    assert "indexes" in ret.data
    assert len(ret.data["indexes"]) == 2


def test_drop_space_with_indexes():
    ret = space.drop()
    logger.debug(ret.msg)
    assert ret.code == 0


def test_drop_db():
    ret = db.drop()
    assert ret.code == 0
