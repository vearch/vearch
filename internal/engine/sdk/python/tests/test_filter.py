import numpy as np
from vearch import Config, Engine, Table, FieldInfo, VectorInfo, dataType, Document, QueryRequest, RangeFilter, TermFilter
from vearch.schema.index import FlatIndex, MetricType
from .utils.vearch_log import logger

def test_term_filter():
    config = Config(path="data", log_dir="logs")
    engine = Engine(config)
    assert engine is not None

    field_infos = [
        FieldInfo("field_string", dataType.STRING, True),
        FieldInfo("field_string_array", dataType.STRINGARRAY, True),
    ]
    vector_infos = [VectorInfo(name="field_vector", dimension=64)]
    table = Table(name="test_table", field_infos=field_infos, vector_infos=vector_infos, index=FlatIndex("flat_index", metric_type=MetricType.L2))
    response = engine.create_table(table)
    assert response.code == 0

    # Insert documents
    features = np.random.rand(64).astype('float32')
    documents = []
    for i in range(3):
        datas = {
            "field_string": f"test_{i}",
            "field_string_array": [f"a{i}", f"b{i}", f"c{i}"],
            "field_vector": features
        }
        doc = Document(datas=datas)
        doc.to_fields(table)
        documents.append(doc)

    upsert_response = engine.upsert(documents)
    assert len(upsert_response.document_ids) == 3

    # Test TermFilter for field_string
    term_filter = TermFilter(
        field_name="field_string",
        value="test_1"
    )
    query_request = QueryRequest(term_filters=[term_filter])
    query_response = engine.query(query_request)
    assert query_response.code == 0
    assert len(query_response.documents) == 1
    queried_doc = query_response.documents[0].to_dict()
    assert queried_doc["field_string"] == "test_1"

    # Test TermFilter for field_string_array
    term_filter = TermFilter(
        field_name="field_string_array",
        value="a2"
    )
    query_request = QueryRequest(term_filters=[term_filter])
    query_response = engine.query(query_request)
    assert query_response.code == 0
    assert len(query_response.documents) == 1
    queried_doc = query_response.documents[0].to_dict()
    assert "a2" in queried_doc["field_string_array"]

    logger.info("TermFilter tests passed.")
    engine.close()
    engine.clear

def test_range_filter():
    config = Config(path="data", log_dir="logs")
    engine = Engine(config)
    assert engine is not None

    field_infos = [
        FieldInfo("field_int", dataType.INT, True),
        FieldInfo("field_float", dataType.FLOAT, True),
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
            "field_int": i * 10,
            "field_float": i * 1.1,
            "field_vector": features
        }
        doc = Document(datas=datas)
        doc.to_fields(table)
        documents.append(doc)

    upsert_response = engine.upsert(documents)
    assert len(upsert_response.document_ids) == 5

    # Test RangeFilter for field_int
    range_filter = RangeFilter(
        field="field_int",
        lower_value=10,
        upper_value=30,
        include_lower=True,
        include_upper=False
    )
    query_request = QueryRequest(range_filters=[range_filter])
    query_response = engine.query(query_request)
    assert query_response.code == 0
    assert len(query_response.documents) == 2
    for doc in query_response.documents:
        doc_data = doc.to_dict()
        assert 10 <= doc_data["field_int"] < 30

    # Test RangeFilter for field_float
    range_filter = RangeFilter(
        field="field_float",
        lower_value=2.2,
        upper_value=4.4,
        include_lower=False,
        include_upper=True
    )
    query_request = QueryRequest(range_filters=[range_filter])
    query_response = engine.query(query_request)
    assert query_response.code == 0
    assert len(query_response.documents) == 2
    for doc in query_response.documents:
        doc_data = doc.to_dict()
        assert 2.2 < doc_data["field_float"] <= 4.4

    engine.close()
    engine.clear()