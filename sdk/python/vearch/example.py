from vearch.config import Config
from vearch.core.db import Database
from vearch.core.vearch import Vearch
from vearch.schema.field import Field
from vearch.schema.space import SpaceSchema
from vearch.utils import DataType, MetricType
from vearch.schema.index import IvfPQIndex, Index, ScalarIndex


def create_space_schema() -> SpaceSchema:
    book_name = Field("book_name", DataType.VARCHAR, desc="the name of book", index=ScalarIndex("book_name_idx"))
    book_vector = Field("book_character", DataType.VECTOR,
                        IvfPQIndex("book_vec_idx", 10000, MetricType.Inner_product, 2048, 40), dim=512)
    ractor_address = Field("ractor_address", DataType.VARCHAR, desc="the place of the book put")
    space_schema = SpaceSchema("book_info", fields=[book_name, book_vector, ractor_address])
    return space_schema


if __name__ == "__main__":
    # config = Config(host="http://127.0.0.1")
    # vc = Vearch(config)
    # print(vc.client.host)
    # vc.create_database("database1")
    # db = Database(name="fjakjfks")
    # db.create()
    space_schema = create_space_schema()
    print(space_schema.dict())
    inv_pq = IvfPQIndex("book_vec_idx", 7000, MetricType.Inner_product, 40, 700)
    print(isinstance(inv_pq, Index))

# / vearch / space / {sapce_name} / field / {field}
# / vearch / space / {sapce_name} / index / {index}
