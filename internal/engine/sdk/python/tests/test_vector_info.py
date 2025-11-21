from vearch import VectorInfo
import pytest

def test_vector_info_initialization():
    vector_info = VectorInfo(name="vec_field", dimension=128, store_type="MemoryOnly")
    assert vector_info.name == "vec_field"
    assert vector_info.dimension == 128
    assert vector_info.store_type == "MemoryOnly"
    assert vector_info.data_type == 5  # VECTOR
    assert vector_info.is_index is True  # Always True for vector fields

def test_vector_info_to_dict():
    vector_info = VectorInfo(name="vec_field", dimension=128, store_type="RocksDB")
    vector_dict = vector_info.to_dict()
    assert vector_dict == {
        "name": "vec_field",
        "data_type": 5,  # VECTOR
        "is_index": True,
        "dimension": 128,
        "store_type": "RocksDB",
    }

def test_vector_info_with_default_store_type():
    vector_info = VectorInfo(name="vec_field", dimension=64)
    assert vector_info.name == "vec_field"
    assert vector_info.dimension == 64
    assert vector_info.store_type == ""  # Default value
    assert vector_info.data_type == 5  # VECTOR
    assert vector_info.is_index is True  # Always True for vector fields

def test_vector_info_with_invalid_store_type():
    with pytest.raises(ValueError, match="Unsupported store_type: InvalidStoreType"):
        VectorInfo(name="vec_field", dimension=128, store_type="InvalidStoreType")

def test_vector_info_with_invalid_dimension():
    with pytest.raises(ValueError, match="VectorInfo dimension must be a positive integer"):
        VectorInfo(name="vec_field", dimension=-1)

def test_vector_info_with_empty_name():
    with pytest.raises(ValueError, match="VectorInfo name cannot be empty"):
        VectorInfo(name="", dimension=128)
