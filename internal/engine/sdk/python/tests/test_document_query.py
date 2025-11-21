import numpy as np
import pytest
from vearch import Config, Engine, Table, FieldInfo, VectorInfo, dataType, Document, QueryRequest, RangeFilter, TermFilter
from vearch.schema.index import FlatIndex, MetricType
from .utils.vearch_log import logger

def test_document_query():
    config = Config(path="data", log_dir="logs")
    engine = Engine(config)
    assert engine is not None

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

    # Insert multiple documents
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

    # Query with limit
    query_request = QueryRequest(
        range_filters=[
            RangeFilter(
                field="field_int",
                lower_value=0,
                upper_value=50,
                include_lower=True,
                include_upper=True
            )
        ],
        limit=3
    )
    query_response = engine.query(query_request)
    assert query_response.code == 0
    assert len(query_response.documents) == 3  # Ensure limit is respected

    # Verify the returned documents
    for doc in query_response.documents:
        doc_data = doc.to_dict()
        assert doc_data["field_int"] in [0, 10, 20, 30, 40]  # Ensure the range filter is applied

    logger.info("Limit test passed.")

    # Query and verify specific fields
    query_request = QueryRequest(
        document_ids=[upsert_response.document_ids[0][0]],
        fields=["field_long", "field_string", "field_vector"]
    )
    query_response = engine.query(query_request)
    assert query_response.code == 0
    assert len(query_response.documents) == 1
    queried_doc = query_response.documents[0].to_dict()
    assert queried_doc["field_long"] == documents[0].datas["field_long"]
    assert queried_doc["field_string"] == documents[0].datas["field_string"]
    assert np.allclose(queried_doc["field_vector"], documents[0].datas["field_vector"])

    # Query with range filter for numeric fields
    numeric_fields = ["field_long", "field_int", "field_float", "field_double"]
    for field in numeric_fields:
        range_filter = RangeFilter(
            field=field,
            lower_value=0,
            upper_value=200,
            include_lower=True,
            include_upper=True
        )
        query_request = QueryRequest(range_filters=[range_filter])
        query_response = engine.query(query_request)
        assert query_response.code == 0
        assert len(query_response.documents) == 5
        queried_doc = query_response.documents[0].to_dict()
        assert queried_doc[field] == documents[0].datas[field]

    # Query with term filter for string fields
    term_filter = TermFilter(
        field_name="field_string",
        value="test_0"
    )
    query_request = QueryRequest(term_filters=[term_filter])
    query_response = engine.query(query_request)
    assert query_response.code == 0
    assert len(query_response.documents) == 1
    queried_doc = query_response.documents[0].to_dict()
    assert queried_doc["field_string"] == documents[0].datas["field_string"]

    # Query with term filter for string array fields
    term_filter = TermFilter(
        field_name="field_string_array",
        value="a0"
    )
    query_request = QueryRequest(term_filters=[term_filter])
    query_response = engine.query(query_request)
    assert query_response.code == 0
    assert len(query_response.documents) == 1
    queried_doc = query_response.documents[0].to_dict()
    assert "a0" in queried_doc["field_string_array"]

    # Exception test: QueryRequest with no filters or document_ids
    with pytest.raises(ValueError, match="At least one of 'document_ids', 'range_filters', or 'term_filters' must be provided."):
        QueryRequest()

    engine.close()
    engine.clear()