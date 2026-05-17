import pytest
from vearch.schema.field import Field
from vearch.schema.index import ScalarIndex, IvfPQIndex
from vearch.utils import DataType, MetricType


class TestFieldInit:
    def test_string_field(self):
        f = Field("book_name", DataType.STRING)
        assert f.name == "book_name"
        assert f.data_type == DataType.STRING
        assert f.desc == ""
        assert f.index is None

    def test_string_field_with_desc(self):
        f = Field("book_name", DataType.STRING, desc="the name of book")
        assert f.desc == "the name of book"

    def test_integer_field(self):
        f = Field("book_num", DataType.INTEGER)
        assert f.name == "book_num"
        assert f.data_type == DataType.INTEGER

    def test_float_field(self):
        f = Field("price", DataType.FLOAT)
        assert f.name == "price"
        assert f.data_type == DataType.FLOAT

    def test_long_field(self):
        f = Field("book_count", DataType.LONG)
        assert f.name == "book_count"
        assert f.data_type == DataType.LONG

    def test_double_field(self):
        f = Field("score", DataType.DOUBLE)
        assert f.name == "score"
        assert f.data_type == DataType.DOUBLE

    def test_vector_field_with_dimension(self):
        f = Field("embedding", DataType.VECTOR, dimension=512)
        assert f.name == "embedding"
        assert f.data_type == DataType.VECTOR
        assert f.dim == 512

    def test_vector_field_dimension_must_be_int(self):
        with pytest.raises(AssertionError, match="vector field must set dimention"):
            Field("embedding", DataType.VECTOR)

    def test_vector_field_dimension_must_be_positive(self):
        with pytest.raises(AssertionError, match="the vector field's dimention must above zero"):
            Field("embedding", DataType.VECTOR, dimension=0)

    def test_vector_field_dimension_negative(self):
        with pytest.raises(AssertionError, match="the vector field's dimention must above zero"):
            Field("embedding", DataType.VECTOR, dimension=-1)

    def test_string_field_array_true(self):
        f = Field("tags", DataType.STRING, array=True)
        assert f.array is True

    def test_string_field_array_default_false(self):
        f = Field("tags", DataType.STRING)
        assert f.array is False

    def test_field_with_index(self):
        idx = ScalarIndex("book_name_idx")
        f = Field("book_name", DataType.STRING, index=idx)
        assert f.index == idx


class TestFieldToDict:
    def test_string_field_to_dict(self):
        f = Field("book_name", DataType.STRING, desc="the name of book")
        d = f.to_dict()
        assert d == {
            "name": "book_name",
            "type": DataType.STRING,
            "desc": "the name of book",
        }

    def test_integer_field_to_dict(self):
        f = Field("book_num", DataType.INTEGER, desc="book number")
        d = f.to_dict()
        assert d == {
            "name": "book_num",
            "type": DataType.INTEGER,
            "desc": "book number",
        }

    def test_vector_field_to_dict(self):
        f = Field("embedding", DataType.VECTOR, dimension=256)
        d = f.to_dict()
        assert d == {
            "name": "embedding",
            "type": DataType.VECTOR,
            "desc": "",
            "dimension": 256,
        }

    def test_field_to_dict_with_index(self):
        idx = ScalarIndex("book_name_idx")
        f = Field("book_name", DataType.STRING, desc="the name of book", index=idx)
        d = f.to_dict()
        assert d["name"] == "book_name"
        assert d["type"] == DataType.STRING
        assert d["desc"] == "the name of book"
        assert d["index"] == {"name": "book_name_idx", "type": "SCALAR"}

    def test_dict_alias(self):
        f = Field("price", DataType.FLOAT)
        assert f.dict() == f.to_dict()


class TestFieldFromDict:
    def test_from_dict_string_field(self):
        data = {"name": "book_name", "type": DataType.STRING, "desc": "book name field"}
        f = Field.from_dict(data)
        assert f.name == "book_name"
        assert f.data_type == DataType.STRING
        assert f.desc == "book name field"
        assert f.index is None

    def test_from_dict_integer_field(self):
        data = {"name": "book_num", "type": DataType.INTEGER, "desc": ""}
        f = Field.from_dict(data)
        assert f.name == "book_num"
        assert f.data_type == DataType.INTEGER

    def test_from_dict_vector_field(self):
        data = {
            "name": "embedding",
            "type": DataType.VECTOR,
            "desc": "",
            "dimension": 128,
        }
        f = Field.from_dict(data)
        assert f.name == "embedding"
        assert f.data_type == DataType.VECTOR
        assert f.dim == 128

    def test_from_dict_with_scalar_index(self):
        data = {
            "name": "book_name",
            "type": DataType.STRING,
            "desc": "book name",
            "index": {"name": "book_name_idx", "type": "SCALAR"},
        }
        f = Field.from_dict(data)
        assert f.name == "book_name"
        assert f.index is not None
        assert f.index._index_name == "book_name_idx"
        assert f.index._index_type == "SCALAR"

    def test_from_dict_with_ivfpq_index(self):
        data = {
            "name": "embedding",
            "type": DataType.VECTOR,
            "dimension": 512,
            "index": {
                "name": "vec_idx",
                "type": "IVFPQ",
                "params": {
                    "metric_type": "Inner_product",
                    "ncentroids": 2048,
                    "nsubvector": 8,
                    "nprobe": 80,
                },
            },
        }
        f = Field.from_dict(data)
        assert f.index is not None
        assert f.index._index_name == "vec_idx"
        assert f.index._index_type == "IVFPQ"

    def test_from_dict_with_hnsw_index(self):
        data = {
            "name": "embedding",
            "type": DataType.VECTOR,
            "dimension": 128,
            "index": {
                "name": "hnsw_idx",
                "type": "HNSW",
                "params": {
                    "metric_type": "Cosine",
                    "nlinks": 32,
                    "efConstruction": 40,
                    "efSearch": 64,
                },
            },
        }
        f = Field.from_dict(data)
        assert f.index is not None
        assert f.index._index_name == "hnsw_idx"
        assert f.index._index_type == "HNSW"

    def test_from_dict_without_desc(self):
        data = {"name": "book_name", "type": DataType.STRING}
        f = Field.from_dict(data)
        assert f.desc == ""


class TestFieldChinese:
    def test_字段名称为中文(self):
        f = Field("书名", DataType.STRING, desc="书籍的名称")
        assert f.name == "书名"
        assert f.data_type == DataType.STRING
        assert f.desc == "书籍的名称"

    def test_字段描述为中文(self):
        f = Field("价格", DataType.FLOAT, desc="商品价格，单位元")
        d = f.to_dict()
        assert d["desc"] == "商品价格，单位元"

    def test_中文字段名序列化与反序列化(self):
        original = Field("书名", DataType.STRING, desc="书籍的名称")
        restored = Field.from_dict(original.to_dict())
        assert restored.name == original.name
        assert restored.data_type == original.data_type
        assert restored.desc == original.desc

    def test_中文向量字段(self):
        f = Field("文本向量", DataType.VECTOR, dimension=128, desc="文本的128维向量表示")
        assert f.name == "文本向量"
        assert f.dim == 128
        assert f.desc == "文本的128维向量表示"
        d = f.to_dict()
        assert d["name"] == "文本向量"
        assert d["dimension"] == 128

    def test_中文标签数组字段(self):
        f = Field("标签", DataType.STRING, array=True, desc="文章标签列表")
        assert f.array is True
        assert f.desc == "文章标签列表"
        d = f.to_dict()
        assert d["desc"] == "文章标签列表"


class TestFieldRoundTrip:
    def test_string_field_roundtrip(self):
        original = Field("book_name", DataType.STRING, desc="the name of book")
        restored = Field.from_dict(original.to_dict())
        assert restored.name == original.name
        assert restored.data_type == original.data_type
        assert restored.desc == original.desc
        assert restored.index == original.index

    def test_vector_field_roundtrip(self):
        original = Field("embedding", DataType.VECTOR, dimension=512)
        restored = Field.from_dict(original.to_dict())
        assert restored.name == original.name
        assert restored.data_type == original.data_type
        assert restored.dim == original.dim
        assert restored.desc == original.desc

    def test_field_with_index_roundtrip(self):
        idx = IvfPQIndex("vec_idx", MetricType.L2, 2048, 8)
        original = Field("embedding", DataType.VECTOR, dimension=256, index=idx)
        restored = Field.from_dict(original.to_dict())
        assert restored.name == original.name
        assert restored.dim == original.dim
        assert restored.index is not None
        assert restored.index._index_name == idx._index_name
        assert restored.index._index_type == idx._index_type
