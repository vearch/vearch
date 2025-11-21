import numpy as np
import pytest
from vearch import Config, Engine, Table, FieldInfo, VectorInfo, dataType, Document, SearchRequest, VectorQuery, RangeFilter, TermFilter
from vearch.schema.index import FlatIndex, MetricType
from .utils.vearch_log import logger

def test_document_search():
    config = Config(path="data", log_dir="logs")
    engine = Engine(config)
    assert engine is not None

    # Define table schema
    field_infos = [
        FieldInfo("field_long", dataType.LONG, True),
        FieldInfo("field_int", dataType.INT, True),
        FieldInfo("field_float", dataType.FLOAT, True),
        FieldInfo("field_double", dataType.DOUBLE, True),
        FieldInfo("field_string", dataType.STRING, True),
        FieldInfo("field_date", dataType.DATE, True),
        FieldInfo("field_string_array", dataType.STRINGARRAY, True),
    ]
    vector_infos = [VectorInfo(name="field_vector", dimension=64)]
    table = Table(name="test_table", field_infos=field_infos, vector_infos=vector_infos, index=FlatIndex("flat_index", metric_type=MetricType.L2))
    response = engine.create_table(table)
    assert response.code == 0

    # Insert documents
    features = np.random.rand(64).astype('float32')
    documents = []
    for i in range(5):
        datas = {
            "field_long": i,
            "field_int": i * 10,
            "field_float": i * 1.1,
            "field_double": i * 2.2,
            "field_string": f"test_{i}",
            "field_date": f"2023-01-0{i+1} 12:00:00",
            "field_string_array": [f"a{i}", f"b{i}", f"c{i}"],
            "field_vector": features
        }
        doc = Document(datas=datas)
        doc.to_fields(table)
        documents.append(doc)

    upsert_response = engine.upsert(documents)
    assert len(upsert_response.document_ids) == 5

    # Test vector search with term filter
    query_vector = np.random.rand(64).astype('float32')
    vector_query = VectorQuery(name="field_vector", value=query_vector)
    term_filter = TermFilter(field_name="field_string", value="test_1")
    search_request = SearchRequest(vec_fields=[vector_query], term_filters=[term_filter])
    search_response = engine.search(search_request)
    assert search_response.code == 0
    assert len(search_response.documents) == 1
    queried_doc = search_response.documents[0][0].to_dict()
    assert queried_doc["field_string"] == "test_1"

    # Test vector search with range filter
    range_filter = RangeFilter(field="field_int", lower_value=10, upper_value=30, include_lower=True, include_upper=False)
    search_request = SearchRequest(vec_fields=[vector_query], range_filters=[range_filter])
    search_response = engine.search(search_request)
    assert search_response.code == 0
    assert len(search_response.documents) == 1
    for doc in search_response.documents:
        assert len(doc) == 2
        doc_data = doc[0].to_dict()
        assert 10 <= doc_data["field_int"] < 30

    # Test limit
    search_request = SearchRequest(vec_fields=[vector_query], limit=3)
    search_response = engine.search(search_request)
    assert search_response.code == 0
    assert len(search_response.documents) == 1  # Ensure limit is respected
    for doc in search_response.documents:
        assert len(doc) == 3  # Ensure limit is respected

    # Test batch vector search
    batch_vectors = np.random.rand(3, 64).astype('float32')  # 3 query vectors
    vector_query = VectorQuery(name="field_vector", value=batch_vectors)
    search_request = SearchRequest(vec_fields=[vector_query], limit=2)
    search_response = engine.search(search_request)
    assert search_response.code == 0
    assert len(search_response.documents) == 3  # Ensure batch size is respected
    for result in search_response.documents:
        assert len(result) == 2  # Ensure limit is respected for each query

    # Test vector search with min_score and max_score
    query_vector = np.random.rand(64).astype('float32')
    vector_query = VectorQuery(name="field_vector", value=query_vector, min_score=100000)
    search_request = SearchRequest(vec_fields=[vector_query], limit=2)
    search_response = engine.search(search_request)
    assert search_response.code == 0
    assert len(search_response.documents) == 1
    for result in search_response.documents:
        assert len(result) == 0

    vector_query = VectorQuery(name="field_vector", value=query_vector, max_score=-100000)
    search_request = SearchRequest(vec_fields=[vector_query], limit=2)
    search_response = engine.search(search_request)
    assert search_response.code == 0
    assert len(search_response.documents) == 1
    for result in search_response.documents:
        assert len(result) == 0

    # Exception test: SearchRequest without vec_fields
    with pytest.raises(ValueError, match="vec_fields must be a non-empty list of VectorQuery objects."):
        SearchRequest(vec_fields=[])

    engine.close()
    engine.clear()
