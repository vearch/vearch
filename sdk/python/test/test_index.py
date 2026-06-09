import pytest
from vearch.schema.index import (
    Index,
    ScalarIndex,
    InvertedIndex,
    BitmapIndex,
    CompositeIndex,
    IvfPQIndex,
    IvfFlatIndex,
    BinaryIvfIndex,
    FlatIndex,
    HNSWIndex,
    HNSWParams,
    GPUIvfPQIndex,
    GPUIvfFlatIndex,
    NPUIvfRaBitQIndex,
    IvfRaBitQIndex,
)
from vearch.utils import IndexType, MetricType


# ---------------------------------------------------------------------------
# TestIndexBase — base constructor, to_dict, dict, from_dict
# ---------------------------------------------------------------------------

class TestIndexBase:
    def test_index_name_and_type(self):
        idx = Index("my_idx", IndexType.HNSW)
        assert idx._index_name == "my_idx"
        assert idx._index_type == IndexType.HNSW

    def test_index_with_params(self):
        idx = Index("my_idx", IndexType.HNSW, {"nlinks": 64})
        assert idx._params == {"nlinks": 64}

    def test_index_to_dict_name_and_type(self):
        idx = Index("my_idx", IndexType.FLAT)
        d = idx.to_dict()
        assert d["name"] == "my_idx"
        assert d["type"] == IndexType.FLAT
        assert "params" not in d
        assert "field_name" not in d
        assert "field_names" not in d

    def test_index_to_dict_with_params(self):
        idx = Index("my_idx", IndexType.HNSW, {"nlinks": 64, "efConstruction": 160})
        d = idx.to_dict()
        assert d["name"] == "my_idx"
        assert d["type"] == IndexType.HNSW
        assert d["params"] == {"nlinks": 64, "efConstruction": 160}

    def test_index_to_dict_with_field_name(self):
        idx = Index("my_idx", IndexType.SCALAR, field_name="book_name")
        d = idx.to_dict()
        assert d["field_name"] == "book_name"

    def test_index_to_dict_with_field_names(self):
        idx = Index("my_idx", IndexType.COMPOSITE, field_names=["field_a", "field_b"])
        d = idx.to_dict()
        assert d["field_names"] == ["field_a", "field_b"]

    def test_index_dict_alias(self):
        idx = Index("my_idx", IndexType.FLAT)
        assert idx.dict() == idx.to_dict()

    def test_index_from_dict_name_and_type(self):
        data = {"name": "vec_idx", "type": IndexType.IVFPQ}
        idx = Index.from_dict(data)
        assert idx._index_name == "vec_idx"
        assert idx._index_type == IndexType.IVFPQ
        assert idx._params is None

    def test_index_from_dict_with_params(self):
        data = {
            "name": "vec_idx",
            "type": IndexType.IVFPQ,
            "params": {"metric_type": MetricType.L2, "ncentroids": 2048},
        }
        idx = Index.from_dict(data)
        assert idx._params["metric_type"] == MetricType.L2
        assert idx._params["ncentroids"] == 2048

    def test_index_from_dict_with_field_name(self):
        data = {"name": "scalar_idx", "type": IndexType.SCALAR, "field_name": "book_name"}
        idx = Index.from_dict(data)
        assert idx._field_name == "book_name"

    def test_index_from_dict_with_field_names(self):
        data = {
            "name": "composite_idx",
            "type": IndexType.COMPOSITE,
            "field_names": ["field_a", "field_b"],
        }
        idx = Index.from_dict(data)
        assert idx._field_names == ["field_a", "field_b"]

    def test_index_roundtrip(self):
        original = Index("my_idx", IndexType.HNSW, {"nlinks": 32, "efConstruction": 160})
        restored = Index.from_dict(original.to_dict())
        assert restored._index_name == original._index_name
        assert restored._index_type == original._index_type
        assert restored._params == original._params


# ---------------------------------------------------------------------------
# TestScalarIndex
# ---------------------------------------------------------------------------

class TestScalarIndex:
    def test_scalar_index_name(self):
        idx = ScalarIndex("book_name_idx")
        assert idx._index_name == "book_name_idx"
        assert idx._index_type == IndexType.SCALAR

    def test_scalar_index_with_field_name(self):
        idx = ScalarIndex("book_name_idx", field_name="book_name")
        assert idx._field_name == "book_name"

    def test_scalar_index_to_dict(self):
        idx = ScalarIndex("book_name_idx", field_name="book_name")
        d = idx.to_dict()
        assert d["name"] == "book_name_idx"
        assert d["type"] == IndexType.SCALAR
        assert d["field_name"] == "book_name"


# ---------------------------------------------------------------------------
# TestInvertedIndex
# ---------------------------------------------------------------------------

class TestInvertedIndex:
    def test_inverted_index_name(self):
        idx = InvertedIndex("tag_idx")
        assert idx._index_name == "tag_idx"
        assert idx._index_type == IndexType.INVERTED

    def test_inverted_index_with_field_name(self):
        idx = InvertedIndex("tag_idx", field_name="tags")
        d = idx.to_dict()
        assert d["field_name"] == "tags"


# ---------------------------------------------------------------------------
# TestBitmapIndex
# ---------------------------------------------------------------------------

class TestBitmapIndex:
    def test_bitmap_index_name(self):
        idx = BitmapIndex("status_idx")
        assert idx._index_name == "status_idx"
        assert idx._index_type == IndexType.BITMAP

    def test_bitmap_index_with_field_name(self):
        idx = BitmapIndex("status_idx", field_name="status")
        d = idx.to_dict()
        assert d["field_name"] == "status"


# ---------------------------------------------------------------------------
# TestCompositeIndex
# ---------------------------------------------------------------------------

class TestCompositeIndex:
    def test_composite_index_name_and_fields(self):
        idx = CompositeIndex("composite_idx", ["field_a", "field_b"])
        assert idx._index_name == "composite_idx"
        assert idx._index_type == IndexType.COMPOSITE
        assert idx._field_names == ["field_a", "field_b"]

    def test_composite_index_to_dict(self):
        idx = CompositeIndex("composite_idx", ["field_a", "field_b"])
        d = idx.to_dict()
        assert d["name"] == "composite_idx"
        assert d["type"] == IndexType.COMPOSITE
        assert d["field_names"] == ["field_a", "field_b"]

    def test_composite_index_roundtrip(self):
        original = CompositeIndex("composite_idx", ["field_a", "field_b"])
        restored = Index.from_dict(original.to_dict())
        assert restored._index_name == original._index_name
        assert restored._index_type == original._index_type
        assert restored._field_names == original._field_names


# ---------------------------------------------------------------------------
# TestIvfPQIndex
# ---------------------------------------------------------------------------

class TestIvfPQIndex:
    def test_ivfpq_basic(self):
        idx = IvfPQIndex("vec_idx", MetricType.L2, 2048, 8)
        assert idx._index_name == "vec_idx"
        assert idx._index_type == IndexType.IVFPQ
        assert idx._params["metric_type"] == MetricType.L2
        assert idx._params["ncentroids"] == 2048
        assert idx._params["nsubvector"] == 8

    def test_ivfpq_default_training_threshold(self):
        idx = IvfPQIndex("vec_idx", MetricType.L2, 2048, 8)
        assert idx._params["training_threshold"] == 2048 * 200

    def test_ivfpq_explicit_training_threshold(self):
        idx = IvfPQIndex("vec_idx", MetricType.L2, 2048, 8, training_threshold=100000)
        assert idx._params["training_threshold"] == 100000

    def test_ivfpq_custom_bucket_sizes(self):
        idx = IvfPQIndex(
            "vec_idx", MetricType.L2, 2048, 8,
            bucket_init_size=2000, bucket_max_size=2560000
        )
        assert idx._params["bucket_init_size"] == 2000
        assert idx._params["bucket_max_size"] == 2560000

    def test_ivfpq_nprobe(self):
        idx = IvfPQIndex("vec_idx", MetricType.L2, 2048, 8, nprobe=100)
        assert idx._params["nprobe"] == 100

    def test_ivfpq_with_field_name(self):
        idx = IvfPQIndex("vec_idx", MetricType.L2, 2048, 8, field_name="embedding")
        d = idx.to_dict()
        assert d["field_name"] == "embedding"

    def test_ivfpq_nsubvector_method(self):
        idx = IvfPQIndex("vec_idx", MetricType.L2, 2048, 8)
        assert idx.nsubvector() == 8

    def test_ivfpq_to_dict(self):
        idx = IvfPQIndex("vec_idx", MetricType.Inner_product, 2048, 8, nprobe=50)
        d = idx.to_dict()
        assert d["name"] == "vec_idx"
        assert d["type"] == IndexType.IVFPQ
        assert d["params"]["metric_type"] == MetricType.Inner_product
        assert d["params"]["ncentroids"] == 2048
        assert d["params"]["nsubvector"] == 8
        assert d["params"]["nprobe"] == 50

    def test_ivfpq_with_hnsw(self):
        hnsw = HNSWParams(nlinks=16, efConstruction=80, efSearch=32)
        idx = IvfPQIndex("vec_idx", MetricType.L2, 2048, 8, hnsw=hnsw)
        assert idx._params["hnsw"]["nlinks"] == 16
        assert idx._params["hnsw"]["efConstruction"] == 80
        assert idx._params["hnsw"]["efSearch"] == 32

    def test_ivfpq_roundtrip(self):
        original = IvfPQIndex(
            "vec_idx", MetricType.L2, 2048, 8,
            training_threshold=500000, nprobe=100
        )
        restored = Index.from_dict(original.to_dict())
        assert restored._index_name == original._index_name
        assert restored._index_type == original._index_type
        assert restored._params["ncentroids"] == original._params["ncentroids"]
        assert restored._params["nsubvector"] == original._params["nsubvector"]


# ---------------------------------------------------------------------------
# TestIvfFlatIndex
# ---------------------------------------------------------------------------

class TestIvfFlatIndex:
    def test_ivfflat_basic(self):
        idx = IvfFlatIndex("vec_idx", MetricType.L2, 2048)
        assert idx._index_name == "vec_idx"
        assert idx._index_type == IndexType.IVFFLAT
        assert idx._params["metric_type"] == MetricType.L2
        assert idx._params["ncentroids"] == 2048

    def test_ivfflat_default_training_threshold(self):
        idx = IvfFlatIndex("vec_idx", MetricType.L2, 2048)
        assert idx._params["training_threshold"] == 2048 * 200

    def test_ivfflat_explicit_training_threshold(self):
        idx = IvfFlatIndex("vec_idx", MetricType.L2, 2048, training_threshold=500000)
        assert idx._params["training_threshold"] == 500000

    def test_ivfflat_nprobe(self):
        idx = IvfFlatIndex("vec_idx", MetricType.L2, 2048, nprobe=50)
        assert idx._params["nprobe"] == 50

    def test_ivfflat_with_hnsw(self):
        hnsw = HNSWParams(nlinks=16, efConstruction=80, efSearch=32)
        idx = IvfFlatIndex("vec_idx", MetricType.L2, 2048, hnsw=hnsw)
        assert idx._params["hnsw"]["nlinks"] == 16
        assert idx._params["hnsw"]["efConstruction"] == 80
        assert idx._params["hnsw"]["efSearch"] == 32

    def test_ivfflat_to_dict(self):
        idx = IvfFlatIndex("vec_idx", MetricType.Inner_product, 2048, nprobe=100)
        d = idx.to_dict()
        assert d["name"] == "vec_idx"
        assert d["type"] == IndexType.IVFFLAT
        assert d["params"]["metric_type"] == MetricType.Inner_product
        assert d["params"]["ncentroids"] == 2048
        assert d["params"]["nprobe"] == 100

    def test_ivfflat_roundtrip(self):
        original = IvfFlatIndex("vec_idx", MetricType.L2, 2048, nprobe=100)
        restored = Index.from_dict(original.to_dict())
        assert restored._index_name == original._index_name
        assert restored._params["ncentroids"] == original._params["ncentroids"]


# ---------------------------------------------------------------------------
# TestBinaryIvfIndex
# ---------------------------------------------------------------------------

class TestBinaryIvfIndex:
    def test_binaryivf_basic(self):
        idx = BinaryIvfIndex("vec_idx", 256)
        assert idx._index_name == "vec_idx"
        assert idx._index_type == IndexType.BINARYIVF
        assert idx._params["ncentroids"] == 256

    def test_binaryivf_with_field_name(self):
        idx = BinaryIvfIndex("vec_idx", 256, field_name="binary_vec")
        d = idx.to_dict()
        assert d["field_name"] == "binary_vec"

    def test_binaryivf_to_dict(self):
        idx = BinaryIvfIndex("vec_idx", 512)
        d = idx.to_dict()
        assert d["name"] == "vec_idx"
        assert d["type"] == IndexType.BINARYIVF
        assert d["params"]["ncentroids"] == 512

    def test_binaryivf_roundtrip(self):
        original = BinaryIvfIndex("vec_idx", 256, field_name="binary_vec")
        restored = Index.from_dict(original.to_dict())
        assert restored._index_name == original._index_name
        assert restored._params["ncentroids"] == original._params["ncentroids"]


# ---------------------------------------------------------------------------
# TestFlatIndex
# ---------------------------------------------------------------------------

class TestFlatIndex:
    def test_flat_index_with_inner_product(self):
        idx = FlatIndex("vec_idx", MetricType.Inner_product)
        assert idx._index_name == "vec_idx"
        assert idx._index_type == IndexType.FLAT
        assert idx._params["metric_type"] == MetricType.Inner_product

    def test_flat_index_with_l2(self):
        idx = FlatIndex("vec_idx", MetricType.L2)
        assert idx._params["metric_type"] == MetricType.L2

    def test_flat_index_with_field_name(self):
        idx = FlatIndex("vec_idx", MetricType.L2, field_name="embedding")
        d = idx.to_dict()
        assert d["field_name"] == "embedding"

    def test_flat_index_to_dict(self):
        idx = FlatIndex("vec_idx", MetricType.L2)
        d = idx.to_dict()
        assert d["name"] == "vec_idx"
        assert d["type"] == IndexType.FLAT
        assert d["params"]["metric_type"] == MetricType.L2

    def test_flat_index_roundtrip(self):
        original = FlatIndex("vec_idx", MetricType.L2, field_name="embedding")
        restored = Index.from_dict(original.to_dict())
        assert restored._index_name == original._index_name
        assert restored._params["metric_type"] == original._params["metric_type"]


# ---------------------------------------------------------------------------
# TestHNSWIndex
# ---------------------------------------------------------------------------

class TestHNSWIndex:
    def test_hnsw_basic(self):
        idx = HNSWIndex("hnsw_idx", MetricType.L2)
        assert idx._index_name == "hnsw_idx"
        assert idx._index_type == IndexType.HNSW
        assert idx._params["metric_type"] == MetricType.L2
        assert idx._params["nlinks"] == 32
        assert idx._params["efConstruction"] == 160
        assert idx._params["efSearch"] == 64

    def test_hnsw_custom_params(self):
        idx = HNSWIndex(
            "hnsw_idx", MetricType.L2,
            nlinks=64, efConstruction=200, efSearch=128
        )
        assert idx._params["nlinks"] == 64
        assert idx._params["efConstruction"] == 200
        assert idx._params["efSearch"] == 128

    def test_hnsw_to_dict(self):
        idx = HNSWIndex("hnsw_idx", MetricType.L2, nlinks=48, efConstruction=100, efSearch=80)
        d = idx.to_dict()
        assert d["name"] == "hnsw_idx"
        assert d["type"] == IndexType.HNSW
        assert d["params"]["metric_type"] == MetricType.L2
        assert d["params"]["nlinks"] == 48
        assert d["params"]["efConstruction"] == 100
        assert d["params"]["efSearch"] == 80

    def test_hnsw_roundtrip(self):
        original = HNSWIndex(
            "hnsw_idx", MetricType.L2,
            nlinks=64, efConstruction=200, efSearch=128
        )
        restored = Index.from_dict(original.to_dict())
        assert restored._index_name == original._index_name
        assert restored._params["nlinks"] == original._params["nlinks"]
        assert restored._params["efConstruction"] == original._params["efConstruction"]


# ---------------------------------------------------------------------------
# TestGPUIvfPQIndex
# ---------------------------------------------------------------------------

class TestGPUIvfPQIndex:
    def test_gpu_ivfpq_basic(self):
        idx = GPUIvfPQIndex("vec_idx", MetricType.L2, 2048, 8)
        assert idx._index_name == "vec_idx"
        assert idx._index_type == IndexType.GPU_IVFPQ
        assert idx._params["metric_type"] == MetricType.L2
        assert idx._params["ncentroids"] == 2048
        assert idx._params["nsubvector"] == 8

    def test_gpu_ivfpq_default_training_threshold(self):
        idx = GPUIvfPQIndex("vec_idx", MetricType.L2, 2048, 8)
        assert idx._params["training_threshold"] == 2048 * 200

    def test_gpu_ivfpq_explicit_training_threshold(self):
        idx = GPUIvfPQIndex("vec_idx", MetricType.L2, 2048, 8, training_threshold=500000)
        assert idx._params["training_threshold"] == 500000

    def test_gpu_ivfpq_nprobe(self):
        idx = GPUIvfPQIndex("vec_idx", MetricType.L2, 2048, 8, nprobe=100)
        assert idx._params["nprobe"] == 100

    def test_gpu_ivfpq_to_dict(self):
        idx = GPUIvfPQIndex("vec_idx", MetricType.L2, 2048, 8, nprobe=100)
        d = idx.to_dict()
        assert d["name"] == "vec_idx"
        assert d["type"] == IndexType.GPU_IVFPQ
        assert d["params"]["ncentroids"] == 2048
        assert d["params"]["nprobe"] == 100

    def test_gpu_ivfpq_roundtrip(self):
        original = GPUIvfPQIndex("vec_idx", MetricType.L2, 2048, 8, nprobe=100)
        restored = Index.from_dict(original.to_dict())
        assert restored._index_name == original._index_name
        assert restored._params["ncentroids"] == original._params["ncentroids"]


# ---------------------------------------------------------------------------
# TestGPUIvfFlatIndex
# ---------------------------------------------------------------------------

class TestGPUIvfFlatIndex:
    def test_gpu_ivfflat_basic(self):
        idx = GPUIvfFlatIndex("vec_idx", MetricType.L2, 2048)
        assert idx._index_name == "vec_idx"
        assert idx._index_type == IndexType.GPU_IVFFLAT
        assert idx._params["metric_type"] == MetricType.L2
        assert idx._params["ncentroids"] == 2048

    def test_gpu_ivfflat_default_training_threshold(self):
        idx = GPUIvfFlatIndex("vec_idx", MetricType.L2, 2048)
        assert idx._params["training_threshold"] == 2048 * 200

    def test_gpu_ivfflat_custom_bucket_sizes(self):
        idx = GPUIvfFlatIndex(
            "vec_idx", MetricType.L2, 2048,
            bucket_init_size=2000, bucket_max_size=2560000
        )
        assert idx._params["bucket_init_size"] == 2000
        assert idx._params["bucket_max_size"] == 2560000

    def test_gpu_ivfflat_to_dict(self):
        idx = GPUIvfFlatIndex("vec_idx", MetricType.L2, 2048, nprobe=100)
        d = idx.to_dict()
        assert d["name"] == "vec_idx"
        assert d["type"] == IndexType.GPU_IVFFLAT
        assert d["params"]["ncentroids"] == 2048
        assert d["params"]["nprobe"] == 100

    def test_gpu_ivfflat_roundtrip(self):
        original = GPUIvfFlatIndex("vec_idx", MetricType.L2, 2048, nprobe=100)
        restored = Index.from_dict(original.to_dict())
        assert restored._index_name == original._index_name
        assert restored._params["ncentroids"] == original._params["ncentroids"]


# ---------------------------------------------------------------------------
# TestNPUIvfRaBitQIndex
# ---------------------------------------------------------------------------

class TestNPUIvfRaBitQIndex:
    def test_npu_ivfrabitq_basic(self):
        idx = NPUIvfRaBitQIndex("vec_idx")
        assert idx._index_name == "vec_idx"
        assert idx._index_type == IndexType.NPU_IVFRABITQ
        assert idx._params["metric_type"] == MetricType.L2
        assert idx._params["ncentroids"] == 4096
        assert idx._params["nb_bits"] == 1

    def test_npu_ivfrabitq_custom_metric_type(self):
        idx = NPUIvfRaBitQIndex("vec_idx", metric_type=MetricType.Inner_product)
        assert idx._params["metric_type"] == MetricType.Inner_product

    def test_npu_ivfrabitq_custom_ncentroids(self):
        idx = NPUIvfRaBitQIndex("vec_idx", ncentroids=8192)
        assert idx._params["ncentroids"] == 8192

    def test_npu_ivfrabitq_custom_nb_bits(self):
        idx = NPUIvfRaBitQIndex("vec_idx", nb_bits=4)
        assert idx._params["nb_bits"] == 4

    def test_npu_ivfrabitq_nprobe(self):
        idx = NPUIvfRaBitQIndex("vec_idx", nprobe=50)
        assert idx._params["nprobe"] == 50

    def test_npu_ivfrabitq_nb_bits_method(self):
        idx = NPUIvfRaBitQIndex("vec_idx", nb_bits=4)
        assert idx.get_nb_bits() == 4

    def test_npu_ivfrabitq_to_dict(self):
        idx = NPUIvfRaBitQIndex("vec_idx", nb_bits=2, nprobe=60)
        d = idx.to_dict()
        assert d["name"] == "vec_idx"
        assert d["type"] == IndexType.NPU_IVFRABITQ
        assert d["params"]["nb_bits"] == 2
        assert d["params"]["nprobe"] == 60

    def test_npu_ivfrabitq_roundtrip(self):
        original = NPUIvfRaBitQIndex("vec_idx", nb_bits=2, nprobe=60)
        restored = Index.from_dict(original.to_dict())
        assert restored._index_name == original._index_name
        assert restored._params["nb_bits"] == original._params["nb_bits"]


# ---------------------------------------------------------------------------
# TestIvfRaBitQIndex
# ---------------------------------------------------------------------------

class TestIvfRaBitQIndex:
    def test_ivfrabitq_basic(self):
        idx = IvfRaBitQIndex("vec_idx", MetricType.L2, 2048, 4)
        assert idx._index_name == "vec_idx"
        assert idx._index_type == IndexType.IVFRABITQ
        assert idx._params["metric_type"] == MetricType.L2
        assert idx._params["ncentroids"] == 2048
        assert idx._params["nb_bits"] == 4

    def test_ivfrabitq_default_training_threshold(self):
        idx = IvfRaBitQIndex("vec_idx", MetricType.L2, 2048, 4)
        assert idx._params["training_threshold"] == 2048 * 200

    def test_ivfrabitq_custom_bucket_sizes(self):
        idx = IvfRaBitQIndex(
            "vec_idx", MetricType.L2, 2048, 4,
            bucket_init_size=2000, bucket_max_size=2560000
        )
        assert idx._params["bucket_init_size"] == 2000
        assert idx._params["bucket_max_size"] == 2560000

    def test_ivfrabitq_nprobe(self):
        idx = IvfRaBitQIndex("vec_idx", MetricType.L2, 2048, 4, nprobe=50)
        assert idx._params["nprobe"] == 50

    def test_ivfrabitq_with_hnsw(self):
        hnsw = HNSWIndex("hnsw_inner", MetricType.L2, nlinks=16, efConstruction=80, efSearch=32)
        idx = IvfRaBitQIndex("vec_idx", MetricType.L2, 2048, 4, hnsw=hnsw)
        assert idx._params["hnsw"]["nlinks"] == 16
        assert idx._params["hnsw"]["efConstruction"] == 80
        assert idx._params["hnsw"]["efSearch"] == 32

    def test_ivfrabitq_nb_bits_method(self):
        idx = IvfRaBitQIndex("vec_idx", MetricType.L2, 2048, 8)
        assert idx.get_nb_bits() == 8

    def test_ivfrabitq_to_dict(self):
        idx = IvfRaBitQIndex("vec_idx", MetricType.Inner_product, 2048, 8, nprobe=100)
        d = idx.to_dict()
        assert d["name"] == "vec_idx"
        assert d["type"] == IndexType.IVFRABITQ
        assert d["params"]["nb_bits"] == 8
        assert d["params"]["nprobe"] == 100

    def test_ivfrabitq_roundtrip(self):
        original = IvfRaBitQIndex("vec_idx", MetricType.L2, 2048, 8, nprobe=100)
        restored = Index.from_dict(original.to_dict())
        assert restored._index_name == original._index_name
        assert restored._params["nb_bits"] == original._params["nb_bits"]
        assert restored._params["ncentroids"] == original._params["ncentroids"]


# ---------------------------------------------------------------------------
# TestIndexChinese — Chinese name support
# ---------------------------------------------------------------------------

class TestIndexChinese:
    def test_index_name_chinese(self):
        idx = Index("向量索引", IndexType.HNSW)
        assert idx._index_name == "向量索引"
        assert idx._index_type == IndexType.HNSW

    def test_scalar_index_chinese_name(self):
        idx = ScalarIndex("标量索引", field_name="书名")
        d = idx.to_dict()
        assert d["name"] == "标量索引"
        assert d["field_name"] == "书名"

    def test_hnsw_index_chinese_name(self):
        idx = HNSWIndex("向量索引", MetricType.L2)
        d = idx.to_dict()
        assert d["name"] == "向量索引"
        assert d["type"] == IndexType.HNSW

    def test_chinese_name_roundtrip(self):
        original = Index("向量索引", IndexType.IVFPQ, {"ncentroids": 1024})
        restored = Index.from_dict(original.to_dict())
        assert restored._index_name == original._index_name
        assert restored._index_type == original._index_type


# ---------------------------------------------------------------------------
# TestFromDictDispatch — Index.from_dict returns the right subclass
# ---------------------------------------------------------------------------

class TestFromDictDispatch:
    def test_scalar_subclass_from_dict(self):
        original = ScalarIndex("scalar_idx", field_name="book_name")
        restored = Index.from_dict(original.to_dict())
        assert isinstance(restored, ScalarIndex)
        assert restored._field_name == "book_name"

    def test_inverted_subclass_from_dict(self):
        restored = Index.from_dict(InvertedIndex("inv_idx", field_name="tags").to_dict())
        assert isinstance(restored, InvertedIndex)
        assert restored._field_name == "tags"

    def test_bitmap_subclass_from_dict(self):
        restored = Index.from_dict(BitmapIndex("bm_idx", field_name="status").to_dict())
        assert isinstance(restored, BitmapIndex)
        assert restored._field_name == "status"

    def test_composite_subclass_from_dict(self):
        restored = Index.from_dict(CompositeIndex("comp_idx", ["a", "b"]).to_dict())
        assert isinstance(restored, CompositeIndex)
        assert restored._field_names == ["a", "b"]

    def test_ivfpq_subclass_from_dict(self):
        restored = Index.from_dict(IvfPQIndex("vec", MetricType.L2, 2048, 8).to_dict())
        assert isinstance(restored, IvfPQIndex)
        assert restored._params["ncentroids"] == 2048

    def test_hnsw_subclass_from_dict(self):
        restored = Index.from_dict(
            HNSWIndex("hnsw_idx", MetricType.L2, nlinks=16, efConstruction=80).to_dict()
        )
        assert isinstance(restored, HNSWIndex)
        assert restored._params["nlinks"] == 16
        assert restored._params["efConstruction"] == 80

    def test_subclass_classmethod_dispatches_by_type(self):
        # CompositeIndex.from_dict honors the payload "type", not the class it's called on.
        restored = CompositeIndex.from_dict(
            ScalarIndex("scalar_idx", field_name="book_name").to_dict()
        )
        assert isinstance(restored, ScalarIndex)

    def test_unknown_type_falls_back_to_base(self):
        restored = Index.from_dict({"name": "x", "type": "FUTURE_INDEX_TYPE"})
        assert type(restored) is Index
        assert restored._index_type == "FUTURE_INDEX_TYPE"


# ---------------------------------------------------------------------------
# TestHNSWSubparamsWarning — _hnsw_subparams warns on metric_type mismatch
# ---------------------------------------------------------------------------

class TestHNSWSubparamsWarning:
    def test_warn_on_metric_type_mismatch(self):
        import warnings as _warnings
        inner = HNSWIndex("inner", MetricType.Inner_product, nlinks=16, efConstruction=80)
        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            idx = IvfPQIndex("vec", MetricType.L2, 2048, 8, hnsw=inner)
        assert any("ignored" in str(w.message) for w in caught)
        # outer metric_type wins in the serialized payload
        assert idx._params["metric_type"] == MetricType.L2
        assert "metric_type" not in idx._params["hnsw"]

    def test_no_warn_on_matching_metric_type(self):
        import warnings as _warnings
        inner = HNSWIndex("inner", MetricType.L2, nlinks=16, efConstruction=80)
        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            IvfPQIndex("vec", MetricType.L2, 2048, 8, hnsw=inner)
        assert not any("ignored" in str(w.message) for w in caught)

    def test_no_warn_with_hnswparams(self):
        import warnings as _warnings
        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            IvfPQIndex("vec", MetricType.L2, 2048, 8, hnsw=HNSWParams())
        assert not any("ignored" in str(w.message) for w in caught)
