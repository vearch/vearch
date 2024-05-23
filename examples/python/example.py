from vearch.config import Config
from vearch.core.db import Database, Space
from vearch.core.vearch import Vearch
from vearch.schema.field import Field
from vearch.schema.space import SpaceSchema
from vearch.utils import DataType, MetricType, VectorInfo
from vearch.schema.index import IvfPQIndex, Index, ScalarIndex, HNSWIndex
from vearch.filter import Filter,Condition,FieldValue,Conditions
import logging
from typing import List
import json

logger = logging.getLogger("vearch")


def create_space_schema() -> SpaceSchema:
    book_name = Field("book_name", DataType.STRING, desc="the name of book", index=ScalarIndex("book_name_idx"))
    book_num=Field("book_num", DataType.INTEGER, desc="the num of book",index=ScalarIndex("book_num_idx"))
    book_vector = Field("book_character", DataType.VECTOR,
                        FlatIndex("book_vec_idx", MetricType.Inner_product), dimension=512)
    ractor_address = Field("ractor_address", DataType.STRING, desc="the place of the book put")
    space_schema = SpaceSchema("book_info", fields=[book_name,book_num, book_vector, ractor_address])
    return space_schema


def create_database(vc: Vearch):
    logger.debug(vc.client.host)
    ret = vc.create_database("database_test")
    logger.debug(ret.dict_str())

def list_databases(vc: Vearch):
    logger.debug(vc.client.host)
    ret = vc.list_databases()
    logger.debug(ret)

    
def list_spaces(vc: Vearch):
    logger.debug(vc.client.host)
    ret = vc.list_spaces("database_test")
    logger.debug(ret)

def create_space(vc: Vearch):
    space_schema = create_space_schema()
    ret = vc.create_space("database_test", space_schema)
    print("######",ret.data, ret.msg)
    
def upsert_document(vc: Vearch) -> List:
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
    space = Space("database_test", "book_info")
    ret = space.upsert(data)
    assert len(ret.get_document_ids()) >= 0
    if ret:
        logger.debug("upsert result:" + str(ret.get_document_ids()))
        return ret.get_document_ids()
    return []


def upsert_document_from_vearch(vc: Vearch) -> List:
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
    ret = vc.upsert("database_test", "book_info", data)
    if ret:
        logger.debug("upsert result:" + str(ret.get_document_ids()))
        return ret.get_document_ids()
    return []

def query_documents_from_vearch(vc: Vearch,ids: List):
    ret = vc.query("database_test", "book_info",ids)
    print("query document",ret.documents)


def query_documents(ids: List):
    space = Space("database_test", "book_info")
    ret = space.query(ids)
    print("query document",ret.documents)


def search_documets():
    import random
    space = Space("database_test", "book_info")
    
    feature = [random.uniform(0, 1) for _ in range(512)]
    vi = VectorInfo("book_character", feature)
    ret = space.search(vector_infos=[vi, ],limit=7)
    print("search document",ret.documents)


def search_documets_from_vearch(vc: Vearch):
    import random
    feature = [random.uniform(0, 1) for _ in range(512)]
    vi = VectorInfo("book_character", feature)
    ret = vc.search("database_test", "book_info",vector_infos=[vi, ],limit=7)
    print("search document",ret.documents)

def query_documnet_by_filter_of_vearch(vc: Vearch,filters):
    ret = vc.query("database_test", "book_info",filter=filters,limit=2)
    print("search document",ret.documents)


def search_doc_by_filter_of_vearch(vc: Vearch, filters):
    import random
    feature = [random.uniform(0, 1) for _ in range(512)]
    vi = VectorInfo("book_character", feature)
    
    ret = vc.search("database_test", "book_info",vector_infos=[vi, ],filter=filters,limit=3)
    print("search document",ret.documents)

            
def is_database_exist(vc: Vearch):
    ret = vc.is_database_exist("database_test")
    return ret


def is_space_exist(vc: Vearch):
    ret = vc.is_space_exist("database_test", "book_info")
    logger.debug(ret)
    return ret


def delete_space(vc: Vearch):
    ret = vc.drop_space("database_test", "book_info")
    print(ret.data, ret.msg)


def drop_database(vc: Vearch):
    logger.debug(vc.client.host)
    ret = vc.drop_database("database_test")
    logger.debug(ret.dict_str())

def query_documnet_by_filter(filters):
    
    space = Space("database_test", "book_info")
    ret = space.query(filter=filters,limit=2)
    print("query document",ret.documents)


def search_doc_by_filter(filters):
    import random
    space = Space("database_test", "book_info")
    feature = [random.uniform(0, 1) for _ in range(512)]
    vi = VectorInfo("book_character", feature)
    
    ret = space.search(vector_infos=[vi, ],filter=filters,limit=3)
    if ret is not None:
        print("search document",ret.documents)


if __name__ == "__main__":
    # should set your host url
    config = Config(host="http://localhost:9001", token="secret")
    vc = Vearch(config)
    print("is_database_exist",is_database_exist(vc))
    if not is_database_exist(vc):
        create_database(vc)
    list_databases(vc)
    space_exist, _ = is_space_exist(vc)
    print(" is space exist:::",space_exist)
    if not space_exist:
        create_space(vc)
    list_spaces(vc)
    ids=upsert_document_from_vearch(vc)
    query_documents_from_vearch(vc, ids[:4])
    query_documents(ids[:3])
       
    conditons = [Condition(operator = '>', fv = FieldValue(field = "book_num",value = 18)),
                 Condition(operator = 'IN', fv = FieldValue(field = "book_name",value = ["bpww57nu","sykboivx","edjn9542"]))
              ]
    filters = Filter(operator = "AND",conditions = conditons)
    
    query_documnet_by_filter(filters)
    # query_documnet_by_filter_of_vearch(vc, filters)
    
    search_documets()
#     search_documets_from_vearch(vc)
#     search_doc_by_filter(filters)
#     search_doc_by_filter_of_vearch(vc, filters)
    
    delete_space(vc)
    drop_database(vc)
