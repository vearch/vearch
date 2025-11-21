from vearch import FieldInfo, dataType
import pytest

def test_field_info_initialization():
    field_info = FieldInfo(name="field1", data_type=dataType.STRING, is_index=False)
    assert field_info.name == "field1"
    assert field_info.data_type == dataType.STRING
    assert field_info.is_index is False

def test_field_info_to_dict():
    field_info = FieldInfo(name="field1", data_type=dataType.STRING, is_index=False)
    field_dict = field_info.to_dict()
    assert field_dict == {
        "name": "field1",
        "data_type": dataType.STRING,
        "is_index": False
    }

def test_field_info_with_double():
    field_info = FieldInfo(name="field_double", data_type=dataType.DOUBLE, is_index=True)
    assert field_info.name == "field_double"
    assert field_info.data_type == dataType.DOUBLE
    assert field_info.is_index is True

def test_field_info_with_long():
    field_info = FieldInfo(name="field_long", data_type=dataType.LONG, is_index=True)
    assert field_info.name == "field_long"
    assert field_info.data_type == dataType.LONG
    assert field_info.is_index is True

def test_field_info_with_int():
    field_info = FieldInfo(name="field_int", data_type=dataType.INT, is_index=True)
    assert field_info.name == "field_int"
    assert field_info.data_type == dataType.INT
    assert field_info.is_index is True

def test_field_info_with_date():
    field_info = FieldInfo(name="field_date", data_type=dataType.DATE, is_index=True)
    assert field_info.name == "field_date"
    assert field_info.data_type == dataType.DATE
    assert field_info.is_index is True

def test_field_info_with_stringarray():
    field_info = FieldInfo(name="field_stringarray", data_type=dataType.STRINGARRAY, is_index=False)
    assert field_info.name == "field_stringarray"
    assert field_info.data_type == dataType.STRINGARRAY
    assert field_info.is_index is False

def test_field_info_with_invalid_data_type():
    with pytest.raises(ValueError, match="Unsupported data type: INVALID_TYPE"):
        FieldInfo(name="field_invalid", data_type="INVALID_TYPE", is_index=False)

def test_field_info_with_empty_name():
    with pytest.raises(ValueError, match="FieldInfo name cannot be empty"):
        FieldInfo(name="", data_type=dataType.STRING, is_index=False)
