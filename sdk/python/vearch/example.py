from vearch.config import Config
from vearch.core.db import Database, Space
from vearch.core.vearch import Vearch
from vearch.schema.field import Field
from vearch.schema.space import SpaceSchema
from vearch.utils import DataType, MetricType
from vearch.schema.index import IvfPQIndex, Index, ScalarIndex
import logging

logger = logging.getLogger("vearch")


def create_space_schema() -> SpaceSchema:
    book_name = Field("book_name", DataType.STRING, desc="the name of book", index=ScalarIndex("book_name_idx"))
    book_vector = Field("book_character", DataType.VECTOR,
                        IvfPQIndex("book_vec_idx", 10000, MetricType.Inner_product, 2048, 40), dimension=512)
    ractor_address = Field("ractor_address", DataType.STRING, desc="the place of the book put")
    space_schema = SpaceSchema("book_info", fields=[book_name, book_vector, ractor_address])
    return space_schema


def create_database(vc: Vearch):
    logger.debug(vc.client.host)
    ret = vc.create_database("database1")
    logger.debug(ret.dict_str())


def list_databases(vc: Vearch):
    logger.debug(vc.client.host)
    ret = vc.list_databases()
    logger.debug(ret)


def create_space(vc: Vearch):
    space_schema = create_space_schema()
    ret = vc.create_space("database1", space_schema)
    print(ret.text, ret.err_msg)


def upsert_document(vc: Vearch):
    import random
    ractor = ["ractor_logical", "ractor_industry", "ractor_philosophy"]
    book_name_template = "abcdefghijklmnopqrstuvwxyz0123456789"
    data = []
    for i in range(5):
        book_item = ["".join(random.choices(book_name_template, k=5)),
                     [random.uniform(0, 1) for _ in range(512)],
                     ractor[random.randint(0, 2)]]
        data.append(book_item)
        logger.debug(data)
    space = Space("database1", "book_info")
    ret = space.upsert_doc(data)
    if ret:
        print(ret.get_document_ids())
    pass


def is_database_exist(vc: Vearch):
    ret = vc.is_database_exist("database1")
    return ret


def is_space_exist(vc: Vearch):
    ret = vc.is_space_exist("database1", "book_info")
    logger.debug(ret)
    return ret


def delete_space(vc: Vearch):
    ret = vc.drop_space("database1", "book_info")
    print(ret.text, ret.err_msg)


def drop_database(vc: Vearch):
    logger.debug(vc.client.host)
    ret = vc.drop_database("database1")
    logger.debug(ret.dict_str())


if __name__ == "__main__":
    """
    curl --location 'http://test-api-interface-1-router.vectorbase.svc.sq01.n.jd.local/cluster/stats' \
--header 'Authorization: secret' \
--data ''"""

    config = Config(host="http://test-api-interface-1-router.vectorbase.svc.sq01.n.jd.local", token="secret")
    vc = Vearch(config)
    print(is_database_exist(vc))
    if not is_database_exist(vc):
        create_database(vc)
    list_databases(vc)
    space_exist, _ = is_space_exist(vc)
    print(space_exist)
    if not space_exist:
        create_space(vc)
    upsert_document(vc)

    delete_space(vc)
    drop_database(vc)
# vc.drop_database("database1")
# db = Database(name="fjakjfks")
# db.create()
# space_schema = create_space_schema()
# print(space_schema.dict())
# inv_pq = IvfPQIndex("book_vec_idx", 7000, MetricType.Inner_product, 40, 700)
# print(isinstance(inv_pq, Index))

data = "/ vearch / space / {sapce_name} / field / {field}"
# / vearch / space / {sapce_name} / index / {index}
