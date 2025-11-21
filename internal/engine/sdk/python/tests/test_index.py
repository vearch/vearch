import pytest
from vearch.schema.index import (
    Index, ScalarIndex, IvfPQIndex, IvfFlatIndex, BinaryIvfIndex, FlatIndex,
    HNSWIndex, GPUIvfPQIndex, GPUIvfFlatIndex, MetricType, IndexType
)

def test_scalar_index():
    index = ScalarIndex(index_name="scalar_index")
    assert index._index_name == "scalar_index"
    assert index._index_type == IndexType.SCALAR
    assert index._params is None

def test_ivfpq_index():
    index = IvfPQIndex(
        index_name="ivfpq_index",
        metric_type=MetricType.L2,
        ncentroids=1024,
        nsubvector=64
    )
    assert index._index_name == "ivfpq_index"
    assert index._index_type == IndexType.IVFPQ
    assert index._params["metric_type"] == MetricType.L2
    assert index._params["ncentroids"] == 1024
    assert index._params["nsubvector"] == 64

def test_ivfflat_index():
    index = IvfFlatIndex(
        index_name="ivfflat_index",
        metric_type=MetricType.Inner_product,
        ncentroids=512
    )
    assert index._index_name == "ivfflat_index"
    assert index._index_type == IndexType.IVFFLAT
    assert index._params["metric_type"] == MetricType.Inner_product
    assert index._params["ncentroids"] == 512

def test_binaryivf_index():
    index = BinaryIvfIndex(index_name="binaryivf_index", ncentroids=256)
    assert index._index_name == "binaryivf_index"
    assert index._index_type == IndexType.BINARYIVF
    assert index._params["ncentroids"] == 256

def test_flat_index():
    index = FlatIndex(index_name="flat_index", metric_type=MetricType.L2)
    assert index._index_name == "flat_index"
    assert index._index_type == IndexType.FLAT
    assert index._params["metric_type"] == MetricType.L2

def test_hnsw_index():
    index = HNSWIndex(
        index_name="hnsw_index",
        metric_type=MetricType.Inner_product,
        nlinks=32,
        efConstruction=120
    )
    assert index._index_name == "hnsw_index"
    assert index._index_type == IndexType.HNSW
    assert index._params["metric_type"] == MetricType.Inner_product
    assert index._params["nlinks"] == 32
    assert index._params["efConstruction"] == 120

def test_gpu_ivfpq_index():
    index = GPUIvfPQIndex(
        index_name="gpu_ivfpq_index",
        metric_type=MetricType.L2,
        ncentroids=2048,
        nsubvector=128
    )
    assert index._index_name == "gpu_ivfpq_index"
    assert index._index_type == IndexType.GPU_IVFPQ
    assert index._params["metric_type"] == MetricType.L2
    assert index._params["ncentroids"] == 2048
    assert index._params["nsubvector"] == 128

def test_gpu_ivfflat_index():
    index = GPUIvfFlatIndex(
        index_name="gpu_ivfflat_index",
        metric_type=MetricType.Inner_product,
        ncentroids=1024
    )
    assert index._index_name == "gpu_ivfflat_index"
    assert index._index_type == IndexType.GPU_IVFFLAT
    assert index._params["metric_type"] == MetricType.Inner_product
    assert index._params["ncentroids"] == 1024
