import numpy as np
import pytest
from vearch import VectorQuery, Table, VectorInfo
from vearch.schema.index import FlatIndex, MetricType

def mock_table():
    vector_infos = [VectorInfo(name="field_vector", dimension=64)]
    return Table(name="mock_table", field_infos=[], vector_infos=vector_infos, index=FlatIndex("flat_index", metric_type=MetricType.L2))

def test_vector_query_single_vector():
    vector = np.random.rand(64).astype('float32')
    vector_query = VectorQuery(name="field_vector", value=vector)

    assert vector_query.name == "field_vector"
    assert vector_query.value.shape == (64,)
    assert vector_query.min_score == -3.4028235e+38
    assert vector_query.max_score == 3.4028235e+38

    table = mock_table()
    assert vector_query.size(table) == 1

def test_vector_query_multiple_vectors():
    vectors = np.random.rand(3, 64).astype('float32')
    vector_query = VectorQuery(name="field_vector", value=vectors)

    assert vector_query.name == "field_vector"
    assert vector_query.value.shape == (3, 64)
    assert vector_query.min_score == -3.4028235e+38
    assert vector_query.max_score == 3.4028235e+38

    table = mock_table()
    assert vector_query.size(table) == 3

def test_vector_query_invalid_shape():
    with pytest.raises(ValueError):
        invalid_vector = np.random.rand(3, 32).astype('float32')
        table = mock_table()
        VectorQuery(name="field_vector", value=invalid_vector).size(table)

def test_vector_query_min_max_score():
    vector = np.random.rand(64).astype('float32')
    vector_query = VectorQuery(name="field_vector", value=vector, min_score=0.5, max_score=0.9)

    assert vector_query.name == "field_vector"
    assert vector_query.value.shape == (64,)
    assert vector_query.min_score == 0.5  # 验证 min_score
    assert vector_query.max_score == 0.9  # 验证 max_score