import numpy as np
from vearch import Config, Engine, Table, FieldInfo, VectorInfo, dataType, Document
from vearch.schema.index import FlatIndex, MetricType
from .utils.vearch_log import logger

def test_document_upsert():
    config = Config(path="data", log_dir="logs")
    engine = Engine(config)
    assert engine != None

    field_infos = [
        FieldInfo("field_long", dataType.LONG),
        FieldInfo("field_int", dataType.INT),
        FieldInfo("field_float", dataType.FLOAT),
        FieldInfo("field_double", dataType.DOUBLE),
        FieldInfo("field_string", dataType.STRING, True),
        FieldInfo("field_date", dataType.DATE),
        FieldInfo("field_string_array", dataType.STRINGARRAY),
    ]
    vector_infos = [VectorInfo(name="field_vector", dimension=64)]
    table = Table(name="test_table", field_infos=field_infos, vector_infos=vector_infos, index=FlatIndex("flat_index", metric_type=MetricType.L2))
    response = engine.create_table(table)
    assert response.code == 0

    # Insert a document
    features = np.random.rand(64).astype('float32')
    datas = {
        "field_long": 1,
        "field_int": 100,
        "field_float": 1.23,
        "field_double": 3.14159,
        "field_string": "test",
        "field_date": "2023-01-01 12:00:00",
        "field_string_array": ["a", "b", "c"],
        "field_vector": features
    }
    doc = Document(datas=datas)
    doc.to_fields(table)

    upsert_response = engine.upsert([doc])
    assert len(upsert_response.document_ids) == 1
    document_ids = upsert_response.document_ids

    status = engine.status().status
    assert status["max_docid"] == 0

    # Verify the inserted document
    inserted_doc = engine.get(document_ids[0][0]).document
    assert inserted_doc is not None
    inserted_data = inserted_doc.to_dict()
    for key, value in datas.items():
        if key == "field_vector":
            assert np.allclose(inserted_data[key], value)
        else:
            assert inserted_data[key] == value

    # Update the document
    updated_datas = {
        "_id": document_ids[0][0],
        "field_long": 2,
        "field_int": 200,
        "field_float": 2.34,
        "field_double": 6.28318,
        "field_string": "updated_test",
        "field_date": "2023-01-02 12:00:00",
        "field_string_array": ["x", "y", "z"],
        "field_vector": features
    }
    updated_doc = Document(datas=updated_datas)
    updated_doc.to_fields(table)

    updated_response = engine.upsert([updated_doc])
    updated_document_ids = updated_response.document_ids
    assert len(updated_document_ids) == 1
    assert updated_document_ids[0] == document_ids[0]

    # Retrieve and verify the updated document
    retrieved_doc = engine.get(document_ids[0][0]).document
    assert retrieved_doc is not None
    retrieved_data = retrieved_doc.to_dict()
    for key, value in updated_datas.items():
        if key == "field_vector":
            assert np.allclose(retrieved_data[key], value)
        else:
            assert retrieved_data[key] == value

    # Check engine status
    status = engine.status().status
    assert status["max_docid"] == 0

    engine.close()
    engine.clear()
