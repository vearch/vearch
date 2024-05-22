from vearch.config import Config
from vearch.core.db import Database
from vearch.core.space import Space
from vearch.schema.field import Field
from vearch.schema.space import SpaceSchema
from vearch.utils import DataType, MetricType, VectorInfo
from vearch.schema.index import IvfPQIndex, Index, ScalarIndex
from vearch.filter import Filter,Condition,FieldValue,Conditions
from vearch.exception import DatabaseException, VearchException, SpaceException, DocumentException

import logging
from typing import List
import json
import pytest
logger = logging.getLogger("vearch_test")

database_name = "database_test"
space_name = "book_info"
space_name1 = "book_info1"

db = Database(database_name)

space_not = Space(database_name, space_name1)
space = Space(database_name, space_name)

def create_space_schema(space_name) -> SpaceSchema:
    
    book_name = Field("book_name", DataType.STRING, desc="the name of book", index=ScalarIndex("book_name_idx"))
    book_num=Field("book_num", DataType.INTEGER, desc="the num of book",index=ScalarIndex("book_num_idx"))
    book_vector = Field("book_character", DataType.VECTOR,
                        IvfPQIndex("book_vec_idx", 10000, MetricType.Inner_product, 2048, 8), dimension=512)
    ractor_address = Field("ractor_address", DataType.STRING, desc="the place of the book put")
    space_schema = SpaceSchema(space_name, fields=[book_name,book_num, book_vector, ractor_address])
    return space_schema

def test_create_database():
    ret = db.create()
    logger.debug(ret)
    assert ret.__dict__["code"] == 0

def test_is_space_not_exist():    
    ret = space_not.exist()
    assert ret[0] == False
    

def test_create_space():
    ret = space.create(create_space_schema("book_info"))
    assert ret.data["name"] == "book_info"

def test_is_space_exist():
    ret = space.exist()
    logger.debug(ret)
    assert ret[0] ==True 
    
def test_upsert_doc_data_field_missing():
    import random
    ractor = ["ractor_logical", "ractor_industry", "ractor_philosophy"]
    book_name_template = "abcdefghijklmnopqrstuvwxyz0123456789"
    data = []
    num=[12,34,56,74,53,11,14,9]
    for i in range(8):
        book_item = ["".join(random.choices(book_name_template, k=8)),
                     num[i],
                     ractor[random.randint(0, 2)]]
        data.append(book_item)
        logger.debug(book_item)
    with pytest.raises(DocumentException) as excinfo:  
        space.upsert(data)
    assert str(excinfo.value) == ""
        
def test_upsert_doc():
    import random
    ractor = ["ractor_logical", "ractor_industry", "ractor_philosophy"]
    book_name_template = "abcdefghijklmnopqrstuvwxyz0123456789"
    data = []
    num=[12,34,56,74,53,11,14,9]
    for i in range(8):
        book_item = ["".join(random.choices(book_name_template, k=8)),
                     num[i],
                     [random.uniform(0, 1) for _ in range(512)],
                     ractor[random.randint(0, 2)]]
        data.append(book_item)
        logger.debug(book_item)
    ret = space.upsert(data)
    assert len(ret.get_document_ids()) >= 0

def test_search():
    import random
    feature = [random.uniform(0, 1) for _ in range(512)]
    vi = VectorInfo("book_character", feature)
    conditons = [Condition(operator = '>', fv = FieldValue(field = "book_num",value = 0)),
              ]
    filters = Filter(operator = "AND",conditions = conditons)
    ret = space.search(vector_infos=[vi, ], filter=filters, limit=7)
    assert ret is None or len(ret.documents) >=0
    

def test_query():
    conditons = [Condition(operator = '>', fv = FieldValue(field = "book_num",value = 0)),
                 Condition(operator = 'IN', fv = FieldValue(field = "book_name",value = ["bpww57nu","sykboivx","edjn9542"]))
              ]
    filters = Filter(operator = "AND",conditions = conditons)
    ret = space.query(filter = filters)
    assert ret is None or len(ret.documents) >=0

    
    
def test_delete_doc():
    conditons = [Condition(operator = '<', fv = FieldValue(field = "book_num",value = 25)),
                 Condition(operator = '>', fv = FieldValue(field = "book_num",value = 12))
              ]
    filters = Filter(operator = "AND",conditions = conditons)
    ret = space.delete(filter = filters)
    assert ret.code == 0
    
    
def test_drop_space():
    ret = space.drop()
    assert ret.__dict__["code"] == 0


def test_drop_db():
    ret = db.drop()
    assert ret.__dict__["code"] == 0