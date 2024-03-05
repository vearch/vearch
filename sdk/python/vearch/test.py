from vearch.config import Config
from vearch.core.db import Database
from vearch.core.vearch import Vearch
from vearch.schema.field import Field
from vearch.schema.space import SpaceSchema
from vearch.utils import DataType, MetricType
from vearch.schema.index import IvfPQIndex,Index

if __name__ == "__main__":
    # config = Config(host="http://127.0.0.1")
    # vc = Vearch(config)
    # print(vc.client.host)
    # vc.create_database("database1")
    # db = Database(name="fjakjfks")
    # db.create()
    book_name = Field("book_name", DataType.VARCHAR, desc="the name of book")
    book_vector = Field("book_character", DataType.VECTOR,
                        IvfPQIndex("book_vec_idx", 7000, MetricType.inner_product, 40, 700), dim=700)
    ractor_address = Field("ractor_address", DataType.VARCHAR, index=True, desc="the place of the book put")
    space_schema = SpaceSchema("book_info", fields=[book_name, book_vector, ractor_address])
    print(space_schema.dict())
    inv_pq=IvfPQIndex("book_vec_idx", 7000, MetricType.inner_product, 40, 700)
    print(isinstance(inv_pq,Index))

