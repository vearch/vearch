from vearch import Field, dataType, convert_field_value_to_numpy_array
import numpy as np
import pytest
import time

def test_field_initialization():
    field = Field(name="field1", value="value1", data_type=dataType.STRING)
    assert field.name == "field1"
    assert field.value == "value1"
    assert field.type == dataType.STRING

def test_field_to_dict():
    field = Field(name="field1", value="value1", data_type=dataType.STRING)
    field_dict = field.to_dict()
    assert field_dict == {
        "name": "field1",
        "value": "value1",
        "type": dataType.STRING
    }

def test_field_type_validation():
    with pytest.raises(ValueError, match="Unsupported data type"):
        Field(name="field1", value="value1", data_type=999)

def test_field_value_validation():
    with pytest.raises(ValueError, match="Value for STRING type must be a string"):
        Field(name="field1", value=123, data_type=dataType.STRING)
    with pytest.raises(ValueError, match="Value for INT type must be an integer"):
        Field(name="field1", value="123", data_type=dataType.INT)
    with pytest.raises(ValueError, match="Value for FLOAT type must be a float"):
        Field(name="field1", value=123, data_type=dataType.FLOAT)
    with pytest.raises(ValueError, match="Value for STRINGARRAY type must be a list of strings"):
        Field(name="field1", value="not a list", data_type=dataType.STRINGARRAY)
    with pytest.raises(ValueError, match="Value for VECTOR type must be a numpy array"):
        Field(name="field1", value=1.0, data_type=dataType.VECTOR)
    with pytest.raises(ValueError, match="Value for VECTOR type must have dtype float32"):
        Field(name="field1", value=np.array([1.0, 2.0], dtype=np.float64), data_type=dataType.VECTOR)

def test_field_date_type():
    field = Field(name="date_field", value="2023-10-01", data_type=dataType.DATE)
    assert field.name == "date_field"
    assert field.value == "2023-10-01"
    assert field.type == dataType.DATE

    with pytest.raises(ValueError):
        Field(name="date_field", value=123.0, data_type=dataType.DATE)
    with pytest.raises(ValueError):
        Field(name="date_field", value="01-10-2023", data_type=dataType.DATE)

def test_field_vector_list_support():
    # Test with list[float]
    field_list = Field(name="vector_field", value=[1.0, 2.0, 3.0], data_type=dataType.VECTOR)
    assert field_list.name == "vector_field"
    assert isinstance(field_list.value, list)
    assert field_list.value == [1.0, 2.0, 3.0]

    # Test with np.ndarray (1D)
    field_ndarray = Field(name="vector_field", value=np.array([1.0, 2.0, 3.0], dtype=np.float32), data_type=dataType.VECTOR)
    assert field_ndarray.name == "vector_field"
    assert isinstance(field_ndarray.value, np.ndarray)
    assert field_ndarray.value.dtype == np.float32
    assert field_ndarray.value.tolist() == [1.0, 2.0, 3.0]

    # Test with invalid np.ndarray (not 1D)
    with pytest.raises(ValueError):
        Field(name="vector_field", value=np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]], dtype=np.float32), data_type=dataType.VECTOR)

def test_convert_field_value_to_numpy_array():
    # Test valid conversions
    assert convert_field_value_to_numpy_array("test", dataType.STRING).tobytes() == b"test"
    assert convert_field_value_to_numpy_array(["a", "b"], dataType.STRINGARRAY).tobytes() == b"a\001b"
    assert convert_field_value_to_numpy_array(123, dataType.INT).tobytes() == np.array([123], dtype=np.int32).tobytes()
    assert convert_field_value_to_numpy_array(123.45, dataType.FLOAT).tobytes() == np.array([123.45], dtype=np.float32).tobytes()
    assert convert_field_value_to_numpy_array(1234567890123, dataType.LONG).tobytes() == np.array([1234567890123], dtype=np.int64).tobytes()
    assert convert_field_value_to_numpy_array(123.456789, dataType.DOUBLE).tobytes() == np.array([123.456789], dtype=np.float64).tobytes()
    assert convert_field_value_to_numpy_array([1.0, 2.0, 3.0], dataType.VECTOR).tobytes() == np.array([1.0, 2.0, 3.0], dtype=np.float32).tobytes()
    assert convert_field_value_to_numpy_array(np.array([1.0, 2.0, 3.0], dtype=np.float32), dataType.VECTOR).tobytes() == np.array([1.0, 2.0, 3.0], dtype=np.float32).tobytes()

    # Test DATE with string input
    expected_timestamp = int(time.mktime(time.strptime("2023-01-01", "%Y-%m-%d")))
    assert convert_field_value_to_numpy_array("2023-01-01", dataType.DATE).tobytes() == np.array([expected_timestamp], dtype=np.int64).tobytes()

    expected_timestamp = int(time.mktime(time.strptime("2023-01-01 12:00:00", "%Y-%m-%d %H:%M:%S")))
    assert convert_field_value_to_numpy_array("2023-01-01 12:00:00", dataType.DATE).tobytes() == np.array([expected_timestamp], dtype=np.int64).tobytes()

    # Test invalid conversions
    with pytest.raises(ValueError, match="STRING type requires a str value."):
        convert_field_value_to_numpy_array(123, dataType.STRING)

    with pytest.raises(ValueError, match="STRINGARRAY type requires a list of strings."):
        convert_field_value_to_numpy_array("not a list", dataType.STRINGARRAY)

    with pytest.raises(ValueError):
        convert_field_value_to_numpy_array("not a vector", dataType.VECTOR)

    with pytest.raises(ValueError):
        convert_field_value_to_numpy_array(1, dataType.VECTOR)

    with pytest.raises(ValueError):
        convert_field_value_to_numpy_array(np.array([1, 2, 3], dtype=np.int32), dataType.VECTOR)

    with pytest.raises(ValueError, match="DATE type requires a str or int value."):
        convert_field_value_to_numpy_array(123.45, dataType.DATE)

    with pytest.raises(ValueError, match="Invalid date string format: .*"):
        convert_field_value_to_numpy_array("invalid-date", dataType.DATE)

    with pytest.raises(ValueError):
        convert_field_value_to_numpy_array("not an int", dataType.INT)

    with pytest.raises(ValueError, match="Unsupported data type: .*"):
        convert_field_value_to_numpy_array("unsupported", -1)

def test_field_value_validation():
    # Test DATE type with invalid string format
    with pytest.raises(ValueError):
        convert_field_value_to_numpy_array("01-01-2023", dataType.DATE)

    # Test DATE type with unsupported value type
    with pytest.raises(ValueError, match="DATE type requires a str or int value."):
        convert_field_value_to_numpy_array([2023, 1, 1], dataType.DATE)

    # Test DATE type with invalid numeric value
    with pytest.raises(ValueError, match="DATE type requires a str or int value."):
        convert_field_value_to_numpy_array(123.456, dataType.DATE)

def test_field_stringarray():
    # Test with a valid STRINGARRAY field (multiple values)
    field = Field(name="field_string_array", value=["value1", "value2", "value3"], data_type=dataType.STRINGARRAY)
    assert field.name == "field_string_array"
    assert field.value == ["value1", "value2", "value3"]
    assert field.type == dataType.STRINGARRAY

    # Test with a valid STRINGARRAY field (single value)
    field = Field(name="field_string_array", value=["value1"], data_type=dataType.STRINGARRAY)
    assert field.name == "field_string_array"
    assert field.value == ["value1"]
    assert field.type == dataType.STRINGARRAY

    # Test with an invalid STRINGARRAY field (not a list)
    with pytest.raises(ValueError, match="STRINGARRAY type requires a list of strings."):
        Field(name="field_string_array", value="not_a_list", data_type=dataType.STRINGARRAY)

    # Test with an invalid STRINGARRAY field (list with non-string values)
    with pytest.raises(ValueError, match="STRINGARRAY type requires a list of strings."):
        Field(name="field_string_array", value=[1, 2, 3], data_type=dataType.STRINGARRAY)
