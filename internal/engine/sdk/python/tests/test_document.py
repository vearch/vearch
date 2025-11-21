from vearch import Document, Field, dataType, Table, FieldInfo, VectorInfo
import numpy as np
from vearch.schema.index import HNSWIndex, MetricType
import pytest

def mock_table():
    field_infos = [
        FieldInfo("field_string", dataType.STRING),
        FieldInfo("field_int", dataType.INT),
        FieldInfo("field_float", dataType.FLOAT),
        FieldInfo("field_date", dataType.DATE),
        FieldInfo("field_long", dataType.LONG),
        FieldInfo("field_double", dataType.DOUBLE),
        FieldInfo("field_string_array", dataType.STRINGARRAY),
    ]
    vector_infos = [
        VectorInfo(name="field_vector", dimension=64)
    ]
    index = HNSWIndex("vec_index", nlinks=32, efConstruction=120, metric_type=MetricType.L2)
    return Table(name="test_table", field_infos=field_infos, vector_infos=vector_infos, index=index)

def test_document_initialization():
    # Test initialization with fields
    fields = [
        Field(name="field_string", value="test", data_type=dataType.STRING),
        Field(name="field_int", value=123, data_type=dataType.INT)
    ]
    doc = Document(fields=fields)
    assert len(doc.fields) == 2
    assert doc.fields[0].name == "field_string"
    assert doc.fields[1].value == 123

    # Test initialization with datas
    datas = {"field_string": "test", "field_int": 123}
    doc = Document(datas=datas)
    assert doc.datas["field_string"] == "test"
    assert doc.datas["field_int"] == 123

def test_document_add_field():
    doc = Document()
    field = Field(name="field_string", value="test", data_type=dataType.STRING)
    doc.add_field(field)
    assert len(doc.fields) == 1
    assert doc.fields[0].name == "field_string"

def test_document_to_dict():
    datas = {"field_string": "test", "field_int": 123}
    doc = Document(datas=datas)
    doc_dict = doc.to_dict()
    assert doc_dict["field_string"] == "test"
    assert doc_dict["field_int"] == 123

def test_document_to_fields():
    table = mock_table()
    datas = {
        "field_string": "test",
        "field_int": 123,
        "field_float": 1.23,
        "field_date": "2023-10-01",
        "field_long": 1234567890123,
        "field_double": 123.456,
        "field_string_array": ["a", "b", "c"],
        "field_vector": np.random.rand(64).astype('float32')
    }
    doc = Document(datas=datas)
    doc.to_fields(table)
    assert len(doc.fields) == 8
    assert doc.fields[0].name == "field_string"
    assert doc.fields[1].name == "field_int"
    assert doc.fields[2].name == "field_float"
    assert doc.fields[3].name == "field_date"
    assert doc.fields[4].name == "field_long"
    assert doc.fields[5].name == "field_double"
    assert doc.fields[6].name == "field_string_array"
    assert doc.fields[7].name == "field_vector"

    # Check field values
    assert doc.fields[0].value == "test"
    assert doc.fields[1].value == 123
    assert doc.fields[2].value == 1.23
    assert doc.fields[3].value == "2023-10-01"
    assert doc.fields[4].value == 1234567890123
    assert doc.fields[5].value == 123.456
    assert doc.fields[6].value == ["a", "b", "c"]
    assert len(doc.fields[7].value) == 64

    # Test with missing field in table schema
    datas_invalid = {
        "unknown_field": "value"
    }
    doc_invalid = Document(datas=datas_invalid)
    with pytest.raises(ValueError):
        doc_invalid.to_fields(table)

def test_document_to_fields_with_stringarray():
    table = mock_table()

    # Test with a valid STRINGARRAY field (multiple values)
    datas = {
        "field_string_array": ["value1", "value2", "value3"]
    }
    doc = Document(datas=datas)
    doc.to_fields(table)
    assert len(doc.fields) == 1
    assert doc.fields[0].name == "field_string_array"
    assert doc.fields[0].value == ["value1", "value2", "value3"]

    # Test with a valid STRINGARRAY field (single value)
    datas = {
        "field_string_array": ["value1"]
    }
    doc = Document(datas=datas)
    doc.to_fields(table)
    assert len(doc.fields) == 1
    assert doc.fields[0].name == "field_string_array"
    assert doc.fields[0].value == ["value1"]

    # Test with an invalid STRINGARRAY field (not a list)
    datas_invalid = {
        "field_string_array": "not_a_list"
    }
    doc_invalid = Document(datas=datas_invalid)
    with pytest.raises(ValueError):
        doc_invalid.to_fields(table)

    # Test with an invalid STRINGARRAY field (list with non-string values)
    datas_invalid = {
        "field_string_array": [1, 2, 3]
    }
    doc_invalid = Document(datas=datas_invalid)
    with pytest.raises(ValueError):
        doc_invalid.to_fields(table)

def test_document_has_id_field():
    doc = Document(datas={"_id": "123", "field_string": "test"})
    assert doc.has_id_field() is True

    doc = Document(datas={"field_string": "test"})
    assert doc.has_id_field() is False

def test_document_get_id_field():
    doc = Document(datas={"_id": "123", "field_string": "test"})
    id_field = doc.get_id_field()
    assert id_field.name == "_id"
    assert id_field.value == "123"

    doc = Document(datas={"field_string": "test"})
    assert doc.get_id_field() is None

def test_document_serialize_deserialize():
    table = mock_table()
    datas = {
        "field_string": "test",
        "field_int": 123,
        "field_float": 1.23,
        "field_date": "2023-10-01",
        "field_vector": np.random.rand(64).astype('float32')
    }
    doc = Document(datas=datas)
    doc.to_fields(table)
    serialized = doc.serialize()
    assert serialized is not None

    new_doc = Document()
    new_doc.deserialize(serialized, table)
    deserialized_dict = new_doc.to_dict()
    assert deserialized_dict["field_string"] == "test"
    assert deserialized_dict["field_int"] == 123
    assert deserialized_dict["field_float"] == 1.23
    assert deserialized_dict["field_date"] == "2023-10-01 00:00:00"
    assert len(deserialized_dict["field_vector"]) == 64
