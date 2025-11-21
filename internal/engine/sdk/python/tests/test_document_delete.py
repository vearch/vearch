import numpy as np
from vearch import Config, Engine, Table, FieldInfo, VectorInfo, dataType, Document, Field
from vearch.schema.index import FlatIndex, MetricType
from .utils.vearch_log import logger

def test_document_delete():
    config = Config(path="data", log_dir="logs")
    engine = Engine(config)
    assert engine is not None

    # Define table schema
    field_infos = [
        FieldInfo("field_long", dataType.LONG),
        FieldInfo("field_int", dataType.INT),
        FieldInfo("field_string", dataType.STRING, True),
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
        "field_string": "test",
        "field_vector": features
    }
    doc = Document(datas=datas)
    doc.to_fields(table)

    upsert_response = engine.upsert([doc])
    assert len(upsert_response.document_ids) == 1
    document_id = upsert_response.document_ids[0][0]

    # Verify the inserted document
    retrieved_doc = engine.get(document_id).document
    assert retrieved_doc is not None
    retrieved_data = retrieved_doc.to_dict()
    for key, value in datas.items():
        if key == "field_vector":
            assert np.allclose(retrieved_data[key], value)
        else:
            assert retrieved_data[key] == value

    # Delete the document
    delete_response = engine.delete(document_id)
    assert delete_response.code == 0

    # Verify the document is deleted
    retrieved_doc = engine.get(document_id).document
    assert retrieved_doc is None

    logger.info(engine.status().to_dict())

    # Re-insert the same document
    datas = {
        "_id": document_id,
        "field_long": 1,
        "field_int": 100,
        "field_string": "test",
        "field_vector": features
    }
    doc = Document(datas=datas)
    doc.to_fields(table)
    upsert_response = engine.upsert([doc])
    assert len(upsert_response.document_ids) == 1
    reinserted_document_id = upsert_response.document_ids[0][0]
    assert reinserted_document_id == document_id  # Ensure the same document ID is reused

    logger.info(engine.status().to_dict())

    # Verify the re-inserted document
    retrieved_doc = engine.get(reinserted_document_id).document
    assert retrieved_doc is not None
    retrieved_data = retrieved_doc.to_dict()
    for key, value in datas.items():
        if key == "field_vector":
            assert np.allclose(retrieved_data[key], value)
        else:
            assert retrieved_data[key] == value

    # Delete the document again
    delete_response = engine.delete(reinserted_document_id)
    assert delete_response.code == 0

    # Verify the document is deleted again
    retrieved_doc = engine.get(reinserted_document_id).document
    assert retrieved_doc is None

    logger.info(engine.status().to_dict())

    engine.close()
    engine.clear()
