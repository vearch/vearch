import pytest
from vearch import Table, FieldInfo, VectorInfo, Index, dataType

@pytest.fixture
def table():
    field_infos = [
        FieldInfo(name="field1", data_type=dataType.STRING, is_index=True),
        FieldInfo(name="field2", data_type=dataType.INT, is_index=False)
    ]
    vector_infos = [
        VectorInfo(name="vector1", dimension=128, store_type="RocksDB")
    ]
    index = Index(index_name="test_index", index_type="IVFPQ", params={"nlist": 100})
    return Table(
        name="test_table",
        field_infos=field_infos,
        vector_infos=vector_infos,
        index=index,
        refresh_interval=500,
        enable_id_cache=True
    )

def test_table_initialization(table):
    assert table.name == "test_table"
    assert len(table.field_infos) == 3  # Includes "_id" field
    assert len(table.vector_infos) == 1
    assert table.index_type == "IVFPQ"
    assert table.enable_id_cache is True

def test_table_serialization(table):
    serialized_data = table.serialize()
    assert isinstance(serialized_data, bytearray)

def test_table_with_empty_name():
    vector_infos = [
        VectorInfo(name="vector1", dimension=128, store_type="MemoryOnly")
    ]
    index = Index(index_name="test_index", index_type="IVFPQ", params={"nlist": 100})
    with pytest.raises(ValueError, match="Table name cannot be empty"):
        Table(name="", field_infos=[], vector_infos=vector_infos, index=index)

def test_table_with_no_fields_or_vectors():
    index = Index(index_name="test_index", index_type="IVFPQ", params={"nlist": 100})
    with pytest.raises(ValueError, match="Table must have at least one vector"):
        Table(name="test_table", field_infos=[], vector_infos=[], index=index)

def test_table_with_invalid_field_infos():
    vector_infos = [
        VectorInfo(name="vector1", dimension=128, store_type="MemoryOnly")
    ]
    index = Index(index_name="test_index", index_type="IVFPQ", params={"nlist": 100})
    with pytest.raises(ValueError, match="field_infos must be a list of FieldInfo objects"):
        Table(name="test_table", field_infos="invalid", vector_infos=vector_infos, index=index)

def test_table_with_invalid_vector_infos():
    field_infos = [
        FieldInfo(name="field1", data_type=dataType.STRING, is_index=True)
    ]
    index = Index(index_name="test_index", index_type="IVFPQ", params={"nlist": 100})
    with pytest.raises(ValueError, match="vector_infos must be a list of VectorInfo objects"):
        Table(name="test_table", field_infos=field_infos, vector_infos="invalid", index=index)

def test_table_with_empty_field_infos():
    vector_infos = [
        VectorInfo(name="vector1", dimension=128, store_type="MemoryOnly")
    ]
    index = Index(index_name="test_index", index_type="IVFPQ", params={"nlist": 100})
    table = Table(name="test_table", field_infos=[], vector_infos=vector_infos, index=index)
    assert table.name == "test_table"
    assert len(table.field_infos) == 1  # Only "_id" field
    assert len(table.vector_infos) == 1

def test_table_with_no_vectors():
    field_infos = [
        FieldInfo(name="field1", data_type=dataType.STRING, is_index=True)
    ]
    index = Index(index_name="test_index", index_type="IVFPQ", params={"nlist": 100})
    with pytest.raises(ValueError, match="Table must have at least one vector"):
        Table(name="test_table", field_infos=field_infos, vector_infos=[], index=index)

def test_table_with_invalid_index():
    field_infos = [
        FieldInfo(name="field1", data_type=dataType.STRING, is_index=True)
    ]
    vector_infos = [
        VectorInfo(name="vector1", dimension=128, store_type="MemoryOnly")
    ]
    with pytest.raises(ValueError, match="index must be an instance of Index and cannot be None"):
        Table(name="test_table", field_infos=field_infos, vector_infos=vector_infos, index="invalid_index")

def test_table_initialization_with_vectors():
    vector_infos = [
        VectorInfo(name="vector1", dimension=128, store_type="MemoryOnly"),
        VectorInfo(name="vector2", dimension=256, store_type="RocksDB"),
    ]
    index = Index(index_name="test_index", index_type="IVFPQ", params={"nlist": 100})
    table = Table(name="test_table", field_infos=[], vector_infos=vector_infos, index=index)
    assert table.name == "test_table"
    assert len(table.vector_infos) == 2
    assert table.vector_infos[0].name == "vector1"
    assert table.vector_infos[1].name == "vector2"

def test_table_initialization_with_mixed_fields():
    field_infos = [
        FieldInfo(name="field1", data_type=dataType.STRING, is_index=True),
    ]
    vector_infos = [
        VectorInfo(name="vector1", dimension=128, store_type="MemoryOnly"),
    ]
    index = Index(index_name="test_index", index_type="IVFPQ", params={"nlist": 100})
    table = Table(name="test_table", field_infos=field_infos, vector_infos=vector_infos, index=index)
    assert table.name == "test_table"
    assert len(table.field_infos) == 2  # Includes the default "_id" field
    assert len(table.vector_infos) == 1
    assert table.field_infos[0].name == "field1"
    assert table.vector_infos[0].name == "vector1"

def test_table_to_dict():
    field_infos = [
        FieldInfo(name="field1", data_type=dataType.STRING, is_index=True),
    ]
    vector_infos = [
        VectorInfo(name="vector1", dimension=128, store_type="MemoryOnly"),
    ]
    index = Index(index_name="test_index", index_type="IVFPQ", params={"nlist": 100})
    table = Table(name="test_table", field_infos=field_infos, vector_infos=vector_infos, index=index)
    table_dict = table.to_dict()
    assert table_dict["name"] == "test_table"
    assert len(table_dict["field_infos"]) == 2  # Includes the default "_id" field
    assert len(table_dict["vector_infos"]) == 1

def test_table_with_duplicate_field_names():
    field_infos = [
        FieldInfo(name="field1", data_type=dataType.STRING, is_index=True),
        FieldInfo(name="field1", data_type=dataType.INT, is_index=False),  # Duplicate field name
    ]
    vector_infos = [
        VectorInfo(name="vector1", dimension=128, store_type="MemoryOnly"),
    ]
    index = Index(index_name="test_index", index_type="IVFPQ", params={"nlist": 100})
    with pytest.raises(ValueError, match="Field names must be unique"):
        Table(name="test_table", field_infos=field_infos, vector_infos=vector_infos, index=index)

def test_table_with_reserved_field_names():
    field_infos = [
        FieldInfo(name="_id", data_type=dataType.STRING, is_index=True),  # Reserved field name
    ]
    vector_infos = [
        VectorInfo(name="vector1", dimension=128, store_type="MemoryOnly"),
    ]
    index = Index(index_name="test_index", index_type="IVFPQ", params={"nlist": 100})
    with pytest.raises(ValueError, match="Field names cannot include reserved names '_id' or '_score'"):
        Table(name="test_table", field_infos=field_infos, vector_infos=vector_infos, index=index)

    field_infos = [
        FieldInfo(name="field1", data_type=dataType.STRING, is_index=True),
    ]
    vector_infos = [
        VectorInfo(name="_score", dimension=128, store_type="MemoryOnly"),  # Reserved field name
    ]
    with pytest.raises(ValueError, match="Field names cannot include reserved names '_id' or '_score'"):
        Table(name="test_table", field_infos=field_infos, vector_infos=vector_infos, index=index)


