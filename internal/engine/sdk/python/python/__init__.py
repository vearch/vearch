"""
Vearch
======
Vearch is the vector search infrastructure for deeping 
learning and AI applications. The Python's implementation 
allows vearch to be used locally.

Provides
  1. vector storage
  2. vector similarity search
  3. use like a database

use help(vearch) to get more detail infomations.
"""
import json
import time
import uuid
import os
import subprocess
import shutil
from typing import List, Union
from enum import Enum

import flatbuffers
import numpy as np

from . import DataType as GammaDataType
from .gamma_api import Doc as GammaDoc
from .gamma_api import Field as GammaField
from .gamma_api import FieldInfo as GammaFieldInfo
from .gamma_api import Table as GammaTable
from .gamma_api import VectorInfo as GammaVectorInfo

from .request_api.router_grpc_pb2 import QueryRequest as GammaQueryRequest
from .request_api.router_grpc_pb2 import SearchRequest as GammaSearchRequest
from .request_api.router_grpc_pb2 import SearchResponse as GammaSearchResponse

from .swigvearch import *

from .schema.index import Index


class FilterBooleanOperator(Enum):
    AND = 0
    OR = 1
    NOT = 2

class FilterRelationOperator(Enum):
    IN = 1
    NOT_IN = 2

###########################################
# vearch core
###########################################

dataType = GammaDataType.DataType
type_map = {
    dataType.INT: "int32",
    dataType.LONG: "int64",
    dataType.FLOAT: "float32",
    dataType.DOUBLE: "float64",
    dataType.STRING: "string",
    dataType.VECTOR: "vector",
    dataType.DATE: "date",
}

DELIMITER = "\001"

np_datatype_map = {np.uint8: dataType.INT, np.float32: dataType.FLOAT}

np_dtype_map = {
    dataType.INT: np.int32,
    dataType.DOUBLE: np.float64,
    dataType.FLOAT: np.float32,
    dataType.LONG: np.int64,
}

field_type_map = {
    "string": dataType.STRING,
    "integer": dataType.LONG,
    "float": dataType.FLOAT,
    "vector": dataType.VECTOR,
    "double": dataType.DOUBLE,
    "int": dataType.INT,
    "keyword": dataType.STRING,
}

vector_name_map = {
    "Float": "float32",
    "Byte": "uint8",
    "UChar": "int8",
    "Uint64": "uint64",
    "Long": "int64",
    "Int": "int32",
    "Double": "float64",
    "string": "String",
}


def normalize_numpy_array(numpy_array):
    array_size = len(numpy_array.shape)
    if (array_size == 1):
        norm = np.linalg.norm(numpy_array)
        numpy_array = numpy_array / norm
    elif (array_size == 2):
        norm = np.linalg.norm(numpy_array, axis=1)
        for i in range(len(norm)):
            numpy_array[i, :] = numpy_array[i, :] / norm[i]
    else:
        e = Exception("Wrong array, array shape' size should be 1 or 2")
        raise e
    return (numpy_array, norm)


class Config:
    def __init__(
            self,
            path,
            log_dir,
            cluster_name='default',
            db_name='default',
            space_name='default',
            backup_id=1,
            is_backup_import: bool = False
        ):
        if not path:
            raise ValueError("Config path cannot be empty")
        if not log_dir:
            raise ValueError("Config log_dir cannot be empty")
        self.path = path
        self.log_dir = log_dir
        self.cluster_name = cluster_name
        self.space_name = space_name
        self.db_name = db_name
        self.backup_id = backup_id
        self.is_backup_import = is_backup_import

        if self.is_backup_import:
            self.path = f"{self.path}/{self.cluster_name}/backup/{self.db_name}/{self.space_name}/{self.backup_id}"

    def to_dict(self):
        buf = {
            "path": self.path,
            "log_dir": self.log_dir,
            "cluster_name": self.cluster_name,
            "space_name": self.space_name,
            "db_name": self.db_name,
            "backup_id": self.backup_id,
            "is_backup_import": self.is_backup_import
        }
        return buf

class VectorInfo:
    '''vector field info'''
    SUPPORTED_STORE_TYPES = {"MemoryOnly", "RocksDB"}

    def __init__(self, name: str, dimension: int, store_type: str = ""):
        if not name:
            raise ValueError("VectorInfo name cannot be empty")
        if not isinstance(dimension, int) or dimension <= 0:
            raise ValueError("VectorInfo dimension must be a positive integer")
        if store_type and store_type not in self.SUPPORTED_STORE_TYPES:
            raise ValueError(f"Unsupported store_type: {store_type}. Must be one of {self.SUPPORTED_STORE_TYPES}")

        self.name = name
        self.data_type = dataType.VECTOR
        self.dimension = dimension
        self.store_type = store_type
        self.is_index = True  # Always True for vector fields

    def to_dict(self):
        buf = {
            "name": self.name,
            "data_type": self.data_type,
            "is_index": self.is_index,
            "dimension": self.dimension,
            "store_type": self.store_type
        }
        return buf

class FieldInfo:
    '''scalar field info'''
    SUPPORTED_DATA_TYPES = {
        dataType.STRING,
        dataType.INT,
        dataType.LONG,
        dataType.FLOAT,
        dataType.DOUBLE,
        dataType.DATE,
        dataType.STRINGARRAY,
    }

    def __init__(self, name: str, data_type: str, is_index: bool = False):
        if not name:
            raise ValueError("FieldInfo name cannot be empty")
        if data_type not in self.SUPPORTED_DATA_TYPES:
            raise ValueError(f"Unsupported data type: {data_type}")
        self.name = name
        self.data_type = data_type
        self.is_index = is_index

    def to_dict(self):
        buf = {
            "name": self.name,
            "data_type": self.data_type,
            "is_index": self.is_index
        }
        return buf


class Table:
    def __init__(
        self,
        name: str,
        field_infos: List[FieldInfo],
        vector_infos: List[VectorInfo],
        index: Index,
        refresh_interval: int = 1000,
        enable_id_cache: bool = False
    ):
        if not name:
            raise ValueError("Table name cannot be empty")
        if not vector_infos:
            raise ValueError("Table must have at least one vector")
        if not isinstance(field_infos, list) or not all(isinstance(f, FieldInfo) for f in field_infos):
            raise ValueError("field_infos must be a list of FieldInfo objects")
        if not isinstance(vector_infos, list) or not all(isinstance(v, VectorInfo) for v in vector_infos):
            raise ValueError("vector_infos must be a list of VectorInfo objects")
        if index is None or not isinstance(index, Index):
            raise ValueError("index must be an instance of Index and cannot be None")

        # Check for duplicate field names and reserved names
        all_field_names = [f.name for f in field_infos] + [v.name for v in vector_infos]
        if len(all_field_names) != len(set(all_field_names)):
            raise ValueError("Field names must be unique")
        if "_id" in all_field_names or "_score" in all_field_names:
            raise ValueError("Field names cannot include reserved names '_id' or '_score'")

        self.name = name
        self.field_infos = field_infos
        self.field_infos.append(FieldInfo("_id", dataType.STRING, False))
        self.vector_infos = vector_infos
        self.training_threshold = 0
        self.index_type = index._index_type
        self.is_binaryivf_type = self.index_type == "BINARYIVF"
        self.index_params = index._params
        self.refresh_interval = refresh_interval
        self.enable_id_cache = enable_id_cache

    def ser_vector_infos(self, builder, vec_infos):
        lst_VecInfos = []
        for vec_info in vec_infos:
            fb_str_name = builder.CreateString(vec_info.name)
            fb_str_store_type = builder.CreateString(vec_info.store_type)
            fb_str_store_param = builder.CreateString("{}")  # Default empty JSON for store_param

            GammaVectorInfo.VectorInfoStart(builder)
            GammaVectorInfo.VectorInfoAddName(builder, fb_str_name)
            GammaVectorInfo.VectorInfoAddDimension(builder, vec_info.dimension)
            GammaVectorInfo.VectorInfoAddIsIndex(builder, vec_info.is_index)
            GammaVectorInfo.VectorInfoAddDataType(builder, vec_info.data_type)
            GammaVectorInfo.VectorInfoAddStoreType(builder, fb_str_store_type)
            GammaVectorInfo.VectorInfoAddStoreParam(builder, fb_str_store_param)

            lst_VecInfos.append(GammaVectorInfo.VectorInfoEnd(builder))

        GammaTable.TableStartVectorsInfoVector(builder, len(lst_VecInfos))
        for vec in lst_VecInfos:
            builder.PrependUOffsetTRelative(vec)
        return builder.EndVector(len(lst_VecInfos))

    def ser_field_infos(self, builder, field_infos):
        lst_fieldInfos = []
        for field_info in field_infos:
            fb_str_name = builder.CreateString(field_info.name)
            GammaFieldInfo.FieldInfoStart(builder)
            GammaFieldInfo.FieldInfoAddName(builder, fb_str_name)
            # now engine treat date as long
            if field_info.data_type == dataType.DATE:
                GammaFieldInfo.FieldInfoAddDataType(builder, dataType.LONG)
            else:
                GammaFieldInfo.FieldInfoAddDataType(builder, field_info.data_type)
            GammaFieldInfo.FieldInfoAddIsIndex(builder, field_info.is_index)
            lst_fieldInfos.append(GammaFieldInfo.FieldInfoEnd(builder))
        GammaTable.TableStartFieldsVector(builder, len(field_infos))
        for i in lst_fieldInfos:
            builder.PrependUOffsetTRelative(i)
        return builder.EndVector(len(field_infos))

    def serialize(self) -> bytearray:
        builder = flatbuffers.Builder(1024)
        name = builder.CreateString(self.name)
        index_type = builder.CreateString(self.index_type)
        index_params = builder.CreateString(
            json.dumps(self.index_params)
        )
        ser_fields = self.ser_field_infos(builder, self.field_infos)
        ser_vectors = self.ser_vector_infos(builder, self.vector_infos)

        GammaTable.TableStart(builder)
        GammaTable.TableAddName(builder, name)
        GammaTable.TableAddFields(builder, ser_fields)
        GammaTable.TableAddVectorsInfo(builder, ser_vectors)
        GammaTable.TableAddRefreshInterval(builder, self.refresh_interval)
        GammaTable.TableAddEnableIdCache(builder, self.enable_id_cache)
        GammaTable.TableAddIndexType(builder, index_type)
        GammaTable.TableAddIndexParams(builder, index_params)
        builder.Finish(GammaTable.TableEnd(builder))
        self.table_buf = builder.Output()
        return self.table_buf

    def deserialize(self, buf):
        table = GammaTable.Table.GetRootAsTable(buf, 0)
        self.name = table.Name().decode("utf-8")
        self.field_infos = {}
        fields_length = table.FieldsLength()
        for i in range(fields_length):
            field_info = GammaFieldInfo(
                table.Fields(i).Name().decode("utf-8"),
                table.Fields(i).DataType(),
                table.Fields(i).IsIndex(),
            )
            self.field_infos[field_info.name] = field_info
        self.vec_infos = {}
        vec_infos_length = table.VectorsInfoLength()
        for i in range(vec_infos_length):
            vec_info = GammaVectorInfo(
                name = table.VectorsInfo(i).Name().decode("utf-8"),
                dimension = table.VectorsInfo(i).Dimension(),
                is_index = table.VectorsInfo(i).IsIndex(),
                type = table.VectorsInfo(i).DataType(),
                store_type = table.VectorsInfo(i).StoreType().decode("utf-8"),
                store_param = json.loads(table.VectorsInfo(i).StoreParam()),
            )
            self.vec_infos[vec_info.name] = vec_info

        self.index_type = table.IndexType().decode("utf-8")
        self.index_params = json.loads(table.IndexParams())
        self.is_binaryivf = (
            True if self.index_type == "BINARYIVF" else False
        )

    def to_dict(self) -> dict:
        buf = {
            "name": self.name,
            "field_infos": [field_info.to_dict() for field_info in self.field_infos],
            "vector_infos": [vector_info.to_dict() for vector_info in self.vector_infos],
            "index_type": self.index_type,
            "index_params": self.index_params,
            "refresh_interval": self.refresh_interval,
            "enable_id_cache": self.enable_id_cache,
            "is_binaryivf_type": self.is_binaryivf_type
        }
        return buf


def convert_numpy_array_to_field_value(value: bytes, data_type: int, table: Table = None):
    """
    Convert field value from bytes to a Python-readable type based on data_type.
    """
    try:
        if data_type == dataType.STRING:
            return value.decode("utf-8")
        elif data_type == dataType.STRINGARRAY:
            return value.decode("utf-8").split(DELIMITER)
        elif data_type == dataType.DATE:
            timestamp = np.frombuffer(value, dtype=type_map[dataType.LONG])[0]
            return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
        elif data_type == dataType.VECTOR:
            if table and table.is_binaryivf_type:
                return np.frombuffer(value, dtype=np.uint8).copy()
            else:
                return np.frombuffer(value, dtype=np.float32).copy()
        elif data_type in type_map:
            return np.frombuffer(value, dtype=type_map[data_type])[0]
        else:
            return value  # Default to raw bytes if type is unknown
    except OverflowError:
        raise ValueError(f"Value too large to convert for data type {data_type}")


def convert_field_value_to_numpy_array(value, data_type):
    """
    Convert a field value to its numpy ndarray representation based on data_type.
    """
    # Check if value matches the expected data_type
    if data_type == dataType.VECTOR:
        if isinstance(value, list):
            value = np.array(value, dtype=np.float32)
        elif not isinstance(value, np.ndarray) or value.dtype != np.float32 or value.ndim != 1:
            raise ValueError("VECTOR type requires a list[float] or 1D np.ndarray with dtype=np.float32.")
        return value.view(np.uint8)
    elif data_type == dataType.STRING:
        if not isinstance(value, str):
            raise ValueError("STRING type requires a str value.")
        return np.frombuffer(value.encode("utf-8"), dtype=np.uint8)
    elif data_type == dataType.STRINGARRAY:
        if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
            raise ValueError("STRINGARRAY type requires a list of strings.")
        str_concat = DELIMITER.join(value)
        return np.frombuffer(str_concat.encode("utf-8"), dtype=np.uint8)
    elif data_type == dataType.DATE:
        if isinstance(value, str):
            try:
                # Try parsing as full datetime with seconds
                value = int(time.mktime(time.strptime(value, "%Y-%m-%d %H:%M:%S")))
            except ValueError:
                try:
                    # Try parsing as date only
                    value = int(time.mktime(time.strptime(value, "%Y-%m-%d")))
                except ValueError:
                    raise ValueError(
                        f"Invalid date string format: {value}. "
                        f"Expected formats: 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DD'"
                    )
        elif not isinstance(value, int):
            raise ValueError("DATE type requires a str or int value.")
        np_value = np.asarray([value], dtype=type_map[dataType.LONG])
        return np_value.view(np.uint8)
    elif data_type in type_map:
        expected_type = np_dtype_map[data_type]
        if not isinstance(value, (int, float, np.number)):
            raise ValueError(f"{type_map[data_type]} type requires a numeric value.")
        np_value = np.asarray([value], dtype=expected_type)
        return np_value.view(np.uint8)
    else:
        raise ValueError(f"Unsupported data type: {data_type}")


class Field:
    ''' document field value'''
    def __init__(self, name, value, data_type):
        if not name:
            raise ValueError("Field name cannot be empty")

        self.name = name
        self.type = data_type
        self.value = value
        self.value_view = convert_field_value_to_numpy_array(self.value, data_type)

    def to_dict(self):
        return {
            "name": self.name,
            "value": self.value,
            "type": self.type
        }


python_type_to_data_type = {
    str: dataType.STRING,
    int: dataType.INT,
    float: dataType.FLOAT,
    list: dataType.STRINGARRAY
}

class Document:
    def __init__(self, fields:List[Field]=None, datas:dict=None):
        """
        Initialize a Document.
        :param fields: List of Field objects to initialize the document.
        :param datas: Dictionary to initialize the document.
        """
        self.fields = []
        self.datas = {}
        self.key = None
        self.score = None

        if (fields):
            self.fields = fields
        elif (datas):
            self.datas = datas

    def add_field(self, field: Field):
        """
        Add a Field object to the document.
        :param field: Field object to add.
        """
        self.fields.append(field)

    def serialize(self):
        builder = flatbuffers.Builder(1024)
        lstFieldData = []
        if self.fields is None or len(self.fields) == 0:
            return None
        for i in range(len(self.fields)):
            nameData = builder.CreateString(self.fields[i].name)

            bytesOfValue = self.fields[i].value_view.tobytes()
            GammaField.FieldStartValueVector(builder, len(bytesOfValue))
            builder.head = builder.head - len(bytesOfValue)
            builder.Bytes[
                builder.head : (builder.head + len(bytesOfValue))
            ] = bytesOfValue
            valueData = builder.EndVector(len(bytesOfValue))

            GammaField.FieldStart(builder)
            GammaField.FieldAddName(builder, nameData)
            GammaField.FieldAddValue(builder, valueData)
            GammaField.FieldAddDataType(builder, self.fields[i].type)
            lstFieldData.append(GammaField.FieldEnd(builder))

        GammaDoc.DocStartFieldsVector(builder, len(lstFieldData))
        # print(dir(builder))
        for j in reversed(range(len(lstFieldData))):
            builder.PrependUOffsetTRelative(lstFieldData[j])
        fields = builder.EndVector(len(lstFieldData))

        GammaDoc.DocStart(builder)
        GammaDoc.DocAddFields(builder, fields)
        builder.Finish(GammaDoc.DocEnd(builder))
        return builder.Output()

    def deserialize(self, buf, table: Table):
        doc = GammaDoc.Doc.GetRootAsDoc(buf, 0)
        self.fields = []
        for i in range(0, doc.FieldsLength()):
            name = doc.Fields(i).Name().decode("utf-8")
            value = doc.Fields(i).ValueAsNumpy()
            data_type = doc.Fields(i).DataType()
            if data_type == dataType.VECTOR:
                value = convert_numpy_array_to_field_value(value, data_type, table)
            else:
                # check whether hava date type field
                field_info = next((field for field in table.field_infos if field.name == name), None)
                if field_info:
                    data_type = field_info.data_type
                value = convert_numpy_array_to_field_value(value.tobytes(), data_type)
            self.fields.append(Field(name, value, data_type))

    def to_dict(self):
        """
        Convert the document to a dictionary.
        :return: Dictionary representation of the document.
        """
        if self.datas:
            return self.datas
        data = {}
        for field in self.fields:
            data[field.name] = field.value
        if self.score:
            data["_score"] = self.score
        return data

    def to_fields(self, table: Table):
        """
        Convert self.datas into self.fields based on the table's field definitions.
        :param table: Table object containing field type definitions.
        """
        self.key = self.datas.get("_id")
        for key, value in self.datas.items():
            if key == "_id":
                field = Field(name=key, value=value, data_type=dataType.STRING)
                self.add_field(field)
                continue

            # Determine the data type from the table's field definitions
            field_info = next((field for field in table.field_infos if field.name == key), None)
            vector_info = next((vector for vector in table.vector_infos if vector.name == key), None)
            if field_info:
                data_type = field_info.data_type
            elif vector_info:
                data_type = dataType.VECTOR
            else:
                raise ValueError(f"Unknown field '{key}' not found in table schema.")
            # Create a Field object and add it to self.fields
            field = Field(name=key, value=value, data_type=data_type)
            self.add_field(field)

    def has_id_field(self):
        """
        Check if the '_id' field exists in the document.
        :return: True if '_id' field exists, False otherwise.
        """
        if self.datas:
            return '_id' in self.datas
        return any(field.name == '_id' for field in self.fields)

    def get_id_field(self):
        """
        Get the '_id' field from the document.
        :return: Field object representing the '_id' field.
        """
        if self.datas and '_id' in self.datas:
            id_value = self.datas.get('_id')
            return Field(name='_id', value=id_value, data_type=dataType.STRING)
        for field in self.fields:
            if field.name == '_id':
                return field
        return None


class RangeFilter:
    ''' for numeric data type '''
    def __init__(
        self,
        field: str,
        lower_value: Union[int, float],
        upper_value: Union[int, float],
        include_lower: bool = True,
        include_upper: bool = True,
        filter_operator: FilterRelationOperator = FilterRelationOperator.IN
    ):
        self.field = field
        self.lower_value = lower_value
        self.upper_value = upper_value
        self.include_lower = include_lower
        self.include_upper = include_upper
        self.filter_operator = filter_operator

    @staticmethod
    def _convert_to_bytes(value, table, field_name):
        """
        Convert a value to bytes based on its type from the table schema.
        :param value: The value to be converted.
        :param table: The table object containing field definitions.
        :param field_name: The name of the field to determine its type.
        """
        # Determine the field type from the table schema
        field_info = next((field for field in table.field_infos if field.name == field_name), None)
        if not field_info:
            raise ValueError(f"Field '{field_name}' not found in table schema.")

        if field_info.data_type == dataType.INT:
            return np.asarray([value], dtype=np.int32).tobytes()
        elif field_info.data_type == dataType.LONG:
            return np.asarray([value], dtype=np.int64).tobytes()
        elif field_info.data_type == dataType.FLOAT:
            return np.asarray([value], dtype=np.float32).tobytes()
        elif field_info.data_type == dataType.DOUBLE:
            return np.asarray([value], dtype=np.float64).tobytes()
        else:
            raise ValueError(f"Unsupported data type for field '{field_name}': {field_info.data_type}")

    @staticmethod
    def _convert_from_bytes(value_bytes, table, field_name):
        """
        Convert bytes back to the corresponding value based on the type from the table schema.
        :param value_bytes: The byte representation of the value.
        :param table: The table object containing field definitions.
        :param field_name: The name of the field to determine its type.
        """
        # Determine the field type from the table schema
        field_info = next((field for field in table.field_infos if field.name == field_name), None)
        if not field_info:
            raise ValueError(f"Field '{field_name}' not found in table schema.")

        if field_info.data_type == dataType.INT:
            return np.frombuffer(value_bytes, dtype=np.int32)[0]
        elif field_info.data_type == dataType.LONG:
            return np.frombuffer(value_bytes, dtype=np.int64)[0]
        elif field_info.data_type == dataType.FLOAT:
            return np.frombuffer(value_bytes, dtype=np.float32)[0]
        elif field_info.data_type == dataType.DOUBLE:
            return np.frombuffer(value_bytes, dtype=np.float64)[0]
        else:
            raise ValueError(f"Unsupported data type for field '{field_name}': {field_info.data_type}")

    def to_dict(self):
        """
        Convert the RangeFilter object to a dictionary.
        """
        return {
            "field": self.field,
            "lower_value": self.lower_value,
            "upper_value": self.upper_value,
            "include_lower": self.include_lower,
            "include_upper": self.include_upper,
            "filter_operator": self.filter_operator
        }


class TermFilter:
    ''' for string and string array data type '''
    def __init__(
        self,
        field_name: str,
        value: Union[str, List[str]],
        filter_operator: FilterRelationOperator = FilterRelationOperator.IN
    ):
        self.field = field_name
        self.value = value
        self.filter_operator = filter_operator

    @staticmethod
    def _convert_to_bytes(value):
        """
        Convert a value to bytes based on its type.
        :param value: The value to be converted (str or List[str]).
        """
        if isinstance(value, str):
            return np.frombuffer(value.encode("utf-8"), np.uint8).tobytes()
        elif isinstance(value, list) and all(isinstance(v, str) for v in value):
            concatenated = DELIMITER.join(value)
            return np.frombuffer(concatenated.encode("utf-8"), np.uint8).tobytes()
        else:
            raise ValueError("TermFilter value must be a string or a list of strings.")

    @staticmethod
    def _convert_from_bytes(value_bytes):
        """
        Convert bytes back to the corresponding value.
        :param value_bytes: The byte representation of the value.
        """
        decoded_value = value_bytes.decode("utf-8")
        if DELIMITER in decoded_value:
            return decoded_value.split(DELIMITER)
        return decoded_value

    def to_dict(self):
        return {
            "field": self.field,
            "value": self.value,
            "filter_operator": self.filter_operator
        }


class VectorQuery:
    def __init__(
        self,
        name: str,
        value: Union[List[float], np.ndarray],
        min_score: float = -3.4028235e+38,  # Smallest float32 value
        max_score: float = 3.4028235e+38   # Largest float32 value
    ):
        self.name = name
        self.value = value
        self.min_score = min_score
        self.max_score = max_score

    @staticmethod
    def _convert_to_bytes(value: Union[List[float], np.ndarray]) -> bytes:
        """
        Convert a value (float32) to bytes.
        :param value: The value to be converted, either a list of floats or a numpy array.
        """
        if isinstance(value, list):
            value = np.array(value, dtype=np.float32)
        elif isinstance(value, np.ndarray) and value.dtype != np.float32:
            value = value.astype(np.float32)
        return value.tobytes()

    @staticmethod
    def _convert_from_bytes(value_bytes: bytes) -> np.ndarray:
        """
        Convert bytes back to a numpy array of float32.
        :param value_bytes: The byte representation of the value.
        """
        return np.frombuffer(value_bytes, dtype=np.float32)

    def size(self, table: Table) -> int:
        """
        Get the dimension of the vector from the table schema.
        :param table: The table object containing vector definitions.
        :return: Dimension of the vector.
        """
        vector_info = next((vec for vec in table.vector_infos if vec.name == self.name), None)
        if not vector_info:
            raise ValueError(f"Vector field '{self.name}' not found in table schema.")

        if isinstance(self.value, np.ndarray):
            if len(self.value.shape) == 1 and self.value.shape[0] == vector_info.dimension:
                return 1
            if len(self.value.shape) == 2 and self.value.shape[1] == vector_info.dimension:
                return self.value.shape[0]
            else:
                raise ValueError(
                    f"Invalid shape for vector '{self.name}'. Expected shape (?, {vector_info.dimension}), "
                    f"but got {self.value.shape}."
                )
        elif isinstance(self.value, list):
            if table.is_binaryivf_type:
                return len(self.value) * 8 // vector_info.dimension
            else:
                return len(self.value) // vector_info.dimension
        else:
            raise ValueError("VectorQuery value must be a list or numpy array.")

    def to_dict(self):
        return {
            "name": self.name,
            "value": self.value,
            "min_score": self.min_score,
            "max_score": self.max_score,
        }


class QueryRequest:
    def __init__(
        self,
        document_ids: List[str] = None,
        range_filters: List[RangeFilter] = None,
        term_filters: List[TermFilter] = None,
        fields: List[str] = None,
        is_vector_value: bool = False,
        limit: int = 10,
        next: bool = False,
        trace: bool = False,
        operator: FilterBooleanOperator = FilterBooleanOperator.AND
    ):
        self.document_ids = document_ids if document_ids is not None else []
        self.range_filters = range_filters if range_filters is not None else []
        self.term_filters = term_filters if term_filters is not None else []
        self.fields = fields if fields is not None else []
        self.is_vector_value = is_vector_value
        self.limit = limit
        self.next = next
        self.trace = trace
        self.operator = operator

        # Ensure at least one of document_ids, range_filters, or term_filters is not empty
        if not self.document_ids and not self.range_filters and not self.term_filters:
            raise ValueError("At least one of 'document_ids', 'range_filters', or 'term_filters' must be provided.")

    def to_dict(self):
        return {
            "document_ids": self.document_ids,
            "next": self.next,
            "range_filters": [rf.to_dict() for rf in self.range_filters],
            "term_filters": [tf.to_dict() for tf in self.term_filters],
            "fields": self.fields,
            "is_vector_value": self.is_vector_value,
            "limit": self.limit,
            "trace": self.trace,
            "operator": self.operator
        }

    def serialize(self, table: Table):
        """
        Serialize the QueryRequest object into a protobuf binary format.
        :param table: The table object containing field definitions.
        :return: Serialized binary data.
        """
        proto_request = GammaQueryRequest()
        proto_request.limit = self.limit
        proto_request.next = self.next
        proto_request.trace = self.trace
        proto_request.operator = self.operator.value
        proto_request.is_vector_value = self.is_vector_value
        proto_request.document_ids.extend(self.document_ids)
        proto_request.fields.extend(self.fields)

        for range_filter in self.range_filters:
            proto_range_filter = proto_request.range_filters.add()
            proto_range_filter.field = range_filter.field
            proto_range_filter.lower_value = RangeFilter._convert_to_bytes(
                range_filter.lower_value, table, range_filter.field
            )
            proto_range_filter.upper_value = RangeFilter._convert_to_bytes(
                range_filter.upper_value, table, range_filter.field
            )
            proto_range_filter.include_lower = range_filter.include_lower
            proto_range_filter.include_upper = range_filter.include_upper
            proto_range_filter.is_union = range_filter.filter_operator.value

        for term_filter in self.term_filters:
            proto_term_filter = proto_request.term_filters.add()
            proto_term_filter.field = term_filter.field
            proto_term_filter.value = TermFilter._convert_to_bytes(term_filter.value)
            proto_term_filter.is_union = term_filter.filter_operator.value

        return proto_request.SerializeToString()
    
    def deserialize(self, buf, table: Table):
        """
        Deserialize the QueryRequest object from a protobuf binary format.
        :param buf: Serialized binary data.
        :param table: The table object containing field definitions.
        """
        proto_request = GammaQueryRequest.FromString(buf)
        self.limit = proto_request.limit
        self.next = proto_request.next
        self.trace = proto_request.trace
        self.operator = FilterBooleanOperator(proto_request.operator)
        self.is_vector_value = proto_request.is_vector_value
        self.document_ids = list(proto_request.document_ids)
        self.fields = list(proto_request.fields)

        self.range_filters = []
        for proto_range_filter in proto_request.range_filters:
            lower_value = RangeFilter._convert_from_bytes(
                proto_range_filter.lower_value, table, proto_range_filter.field
            )
            upper_value = RangeFilter._convert_from_bytes(
                proto_range_filter.upper_value, table, proto_range_filter.field
            )
            range_filter = RangeFilter(
                field=proto_range_filter.field,
                lower_value=lower_value,
                upper_value=upper_value,
                include_lower=proto_range_filter.include_lower,
                include_upper=proto_range_filter.include_upper,
                filter_operator=FilterRelationOperator(proto_range_filter.is_union)
            )
            self.range_filters.append(range_filter)

        self.term_filters = []
        for proto_term_filter in proto_request.term_filters:
            term_filter = TermFilter(
                field_name=proto_term_filter.field,
                value=TermFilter._convert_from_bytes(proto_term_filter.value),
                filter_operator=FilterRelationOperator(proto_term_filter.is_union)
            )
            self.term_filters.append(term_filter)


class SearchRequest:
    def __init__(
        self,
        vec_fields: List[VectorQuery],
        limit: int = 10,
        fields: List[str] = None,
        range_filters: List[RangeFilter] = None,
        term_filters: List[TermFilter] = None,
        index_params: str = "",
        is_brute_search: int = 0,
        is_vector_value: bool = False,
        ranker: str = "",
        trace: bool = False,
        operator: FilterBooleanOperator = FilterBooleanOperator.AND
    ):
        if not vec_fields or not isinstance(vec_fields, list):
            raise ValueError("vec_fields must be a non-empty list of VectorQuery objects.")
        self.vec_fields = vec_fields
        for vec_field in self.vec_fields:
            if not isinstance(vec_field, VectorQuery):
                raise ValueError("vec_fields must be a list of VectorQuery objects.")
        self.req_num = 0
        self.limit = limit
        self.fields = fields if fields is not None else []
        self.range_filters = range_filters if range_filters is not None else []
        self.term_filters = term_filters if term_filters is not None else []
        self.index_params = index_params
        self.is_brute_search = is_brute_search
        self.is_vector_value = is_vector_value
        self.ranker = ranker
        self.trace = trace
        self.operator = operator

    def to_dict(self):
        return {
            "req_num": self.req_num,
            "limit": self.limit,
            "vec_fields": [vf.to_dict() for vf in self.vec_fields],
            "fields": self.fields,
            "range_filters": [rf.to_dict() for rf in self.range_filters],
            "term_filters": [tf.to_dict() for tf in self.term_filters],
            "index_params": self.index_params,
            "is_vector_value": self.is_vector_value,
            "ranker": self.ranker,
            "trace": self.trace,
            "operator": self.operator.value,
        }

    def serialize(self, table: Table):
        """
        Serialize the SearchRequest object into a protobuf binary format.
        """
        proto_request = GammaSearchRequest()
        vec_sizes = [vec_field.size(table) for vec_field in self.vec_fields]

        if len(set(vec_sizes)) > 1:
            raise ValueError("All VectorQuery objects must have the same size.")

        self.req_num = vec_sizes[0]
        if self.req_num < 1:
            raise ValueError("VectorQuery size must be greater than 0.")

        proto_request.req_num = self.req_num
        proto_request.topN = self.limit
        proto_request.is_brute_search = self.is_brute_search
        proto_request.index_params = self.index_params
        proto_request.is_vector_value = self.is_vector_value
        proto_request.ranker = self.ranker
        proto_request.trace = self.trace
        proto_request.operator = self.operator.value

        for vec_field in self.vec_fields:
            proto_vec_field = proto_request.vec_fields.add()
            proto_vec_field.name = vec_field.name
            proto_vec_field.value = VectorQuery._convert_to_bytes(vec_field.value)
            proto_vec_field.min_score = vec_field.min_score
            proto_vec_field.max_score = vec_field.max_score
            proto_vec_field.index_type = ""  # Placeholder for index_type if needed

        proto_request.fields.extend(self.fields)

        for range_filter in self.range_filters:
            proto_range_filter = proto_request.range_filters.add()
            proto_range_filter.field = range_filter.field
            proto_range_filter.lower_value = RangeFilter._convert_to_bytes(
                range_filter.lower_value, table, range_filter.field
            )
            proto_range_filter.upper_value = RangeFilter._convert_to_bytes(
                range_filter.upper_value, table, range_filter.field
            )
            proto_range_filter.include_lower = range_filter.include_lower
            proto_range_filter.include_upper = range_filter.include_upper
            proto_range_filter.is_union = range_filter.filter_operator.value

        for term_filter in self.term_filters:
            proto_term_filter = proto_request.term_filters.add()
            proto_term_filter.field = term_filter.field
            proto_term_filter.value = TermFilter._convert_to_bytes(term_filter.value)
            proto_term_filter.is_union = term_filter.filter_operator.value

        return proto_request.SerializeToString()

    def deserialize(self, buf: bytes, table: Table):
        """
        Deserialize the SearchRequest object from a protobuf binary format.
        :param buf: Serialized binary data.
        """
        proto_request = GammaSearchRequest.FromString(buf)
        self.req_num = proto_request.req_num
        self.limit = proto_request.topN
        self.is_brute_search = proto_request.is_brute_search
        self.index_params = proto_request.index_params
        self.is_vector_value = proto_request.is_vector_value
        self.ranker = proto_request.ranker
        self.trace = proto_request.trace
        self.operator = FilterBooleanOperator(proto_request.operator)

        self.vec_fields = []
        for proto_vec_field in proto_request.vec_fields:
            vec_field = VectorQuery(
                name=proto_vec_field.name,
                value=VectorQuery._convert_from_bytes(proto_vec_field.value),
                min_score=proto_vec_field.min_score,
                max_score=proto_vec_field.max_score
            )
            self.vec_fields.append(vec_field)

        self.fields = list(proto_request.fields)

        self.range_filters = []
        for proto_range_filter in proto_request.range_filters:
            range_filter = RangeFilter(
                field=proto_range_filter.field,
                lower_value=RangeFilter._convert_from_bytes(proto_range_filter.lower_value, table, proto_range_filter.field),
                upper_value=RangeFilter._convert_from_bytes(proto_range_filter.upper_value, table, proto_range_filter.field),
                include_lower=proto_range_filter.include_lower,
                include_upper=proto_range_filter.include_upper,
                filter_operator=FilterRelationOperator(proto_range_filter.is_union),
            )
            self.range_filters.append(range_filter)

        self.term_filters = []
        for proto_term_filter in proto_request.term_filters:
            term_filter = TermFilter(
                field_name=proto_term_filter.field,
                value=TermFilter._convert_from_bytes(proto_term_filter.value),
                filter_operator=FilterRelationOperator(proto_term_filter.is_union),
            )
            self.term_filters.append(term_filter)


class Response:
    def __init__(self, code: int = 0, msg: str = ""):
        """
        Initialize the Response object.
        :param code: Response code.
        :param msg: Response message.
        :param data: Serialized binary data (optional).
        :param table: Table object containing schema information (optional).
        """
        self.code = code
        self.msg = msg

    def to_dict(self) -> dict:
        """
        Convert the Response object to a dictionary.
        :return: Dictionary representation of the Response.
        """
        return {
            "code": self.code,
            "msg": self.msg
        }


class UpsertResponse(Response):
    def __init__(self, code: int = 0, msg: str = "", document_ids: list = None):
        """
        Initialize the UpsertResponse object.
        :param code: Response code.
        :param msg: Response message.
        :param document_ids: List of document IDs.
        """
        super().__init__(code, msg)
        self.document_ids = document_ids if document_ids is not None else []

    def to_dict(self) -> dict:
        """
        Convert the UpsertResponse object to a dictionary.
        :return: Dictionary representation of the UpsertResponse.
        """
        base_dict = super().to_dict()
        base_dict["document_ids"] = self.document_ids
        return base_dict


class QueryResponse(Response):
    def __init__(self, data: dict = None, table: Table = None):
        """
        Initialize the QueryResponse object.
        :param code: Response code.
        :param msg: Response message.
        :param documents: List of Document objects.
        """
        self.code = data.get("code", 0) if data else 0
        self.msg = data.get("msg", "") if data else ""
        buf = data.get("data", None) if data else None
        self.table = table
        self.deserialize(buf)

    def deserialize(self, buf: bytes):
        """
        Deserialize the buffer into the Response object.
        :param buf: Serialized binary data.
        """
        if buf is None:
            return
        self.data = GammaSearchResponse.FromString(buf)
        self.documents = self.parse_documents(self.data.results)

    def _convert_field_value(self, value: bytes, data_type: int):
        return convert_numpy_array_to_field_value(value, data_type, self.table)

    def parse_documents(self, results):
        """
        Parse documents for QueryResponse.
        Assumes results length is 1 and extracts documents as a flat list.
        """
        if len(results) != 1:
            raise ValueError("QueryResponse expects exactly one result in GammaSearchResponse.")
        documents = []
        for item in results[0].result_items:
            fields = []
            for field in item.fields:
                # Determine the field type from the table schema
                table_field = next((f for f in self.table.field_infos if f.name == field.name), None)
                vector_field = next((v for v in self.table.vector_infos if v.name == field.name), None)
                if vector_field:
                    field_type = dataType.VECTOR
                elif table_field:
                    field_type = table_field.data_type
                else:
                    field_type = field.type
                fields.append(
                    Field(
                        name=field.name,
                        value=self._convert_field_value(field.value, field_type),
                        data_type=field_type
                    )
                )
            doc = Document(fields=fields)
            documents.append(doc)
        return documents

    def to_dict(self):
        """
        Convert the QueryResponse object to a dictionary.
        :return: Dictionary representation of the QueryResponse.
        """
        base_dict = super().to_dict()
        base_dict["documents"] = [doc.to_dict() for doc in self.documents]
        return base_dict

class SearchResponse(Response):
    def __init__(self, data: dict = None, table: Table = None):
        """
        Initialize the SearchResponse object.
        :param code: Response code.
        :param msg: Response message.
        :param documents: List of lists of Document objects (each inner list corresponds to a result).
        """
        self.code = data.get("code", 0) if data else 0
        self.msg = data.get("msg", "") if data else ""
        buf = data.get("data", None) if data else None
        self.table = table
        self.deserialize(buf)

    def deserialize(self, buf: bytes):
        """
        Deserialize the buffer into the Response object.
        :param buf: Serialized binary data.
        """
        if buf is None:
            return
        self.data = GammaSearchResponse.FromString(buf)
        self.documents = self.parse_documents(self.data.results)

    def _convert_field_value(self, value: np.ndarray, data_type: int):
        return convert_numpy_array_to_field_value(value, data_type, self.table)

    def parse_documents(self, results):
        """
        Parse documents for SearchResponse.
        Extracts documents as a list of lists, where each inner list corresponds to a result.
        Includes score for each document.
        """
        documents = []
        for result in results:
            result_documents = []
            for item in result.result_items:
                fields = []
                for field in item.fields:
                    # Determine the field type from the table schema
                    table_field = next((f for f in self.table.field_infos if f.name == field.name), None)
                    vector_field = next((v for v in self.table.vector_infos if v.name == field.name), None)
                    if vector_field:
                        field_type = dataType.VECTOR
                    elif table_field:
                        field_type = table_field.data_type
                    else:
                        field_type = field.type
                    fields.append(
                        Field(
                            name=field.name,
                            value=self._convert_field_value(field.value, field_type),
                            data_type=field_type
                        )
                    )
                doc = Document(fields=fields)
                doc.score = item.score
                result_documents.append(doc)
            documents.append(result_documents)
        return documents

    def to_dict(self):
        """
        Convert the SearchResponse object to a dictionary.
        :return: Dictionary representation of the SearchResponse.
        """
        base_dict = super().to_dict()
        base_dict["documents"] = [
            [doc.to_dict() for doc in doc_list] for doc_list in self.documents
        ]
        return base_dict

class GetResponse(Response):
    def __init__(self, code: int = 0, msg: str = "", document: Union[Document, None] = None):
        """
        Initialize the GetResponse object.
        :param code: Response code.
        :param msg: Response message.
        :param document: The retrieved document, if any.
        """
        super().__init__(code, msg)
        self.document = document

    def to_dict(self) -> dict:
        """
        Convert the GetResponse object to a dictionary.
        :return: Dictionary representation of the GetResponse.
        """
        base_dict = super().to_dict()
        base_dict["document"] = self.document.to_dict() if self.document else None
        return base_dict

class StatusResponse(Response):
    def __init__(self, code: int = 0, msg: str = "", status: dict = None):
        """
        Initialize the StatusResponse object.
        :param code: Response code.
        :param msg: Response message.
        :param status: Dictionary containing engine status information.
        """
        super().__init__(code, msg)
        self.status = status if status is not None else {}

    def to_dict(self) -> dict:
        """
        Convert the StatusResponse object to a dictionary.
        :return: Dictionary representation of the StatusResponse.
        """
        base_dict = super().to_dict()
        base_dict["status"] = self.status
        return base_dict


class Engine:
    """vearch core
    It is used to store, update and delete vectors,
    build indexes for stored vectors,
    and find the nearest neighbor of vectors.
    """

    def __init__(self, config: Config):
        """Initialize the Vearch engine."""
        self.config = config
        self.init()

    def init(self):
        buf = json.dumps(self.config.to_dict())
        buf = np.frombuffer(buf.encode('utf-8'), dtype=np.uint8)  # Convert to bytes and then to NumPy array
        ptr_buf = swig_ptr(buf)
        self.c_engine = swigInitEngine(ptr_buf, buf.shape[0])
        if not self.c_engine:
            raise RuntimeError(
                "Failed to initialize the engine, Maybe insufficient permissions for the specified paths. "
            )

    def close(self):
        """close engine
        return: 0 successed, 1 failed
        """
        response_code = swigClose(self.c_engine)
        return response_code

    def config_info(self) -> Config:
        """get config info
        return: config info
        """
        return self.config

    def create_table(
        self,
        table: Table
    ) -> Response:
        """create table for engine
        table_info: table detail info
        return: Response object with code and message
        """
        self.table = table
        table_buf = self.table.serialize()
        self.table_buf = table_buf
        np_table_buf = np.array(table_buf)
        ptableBuf = swig_ptr(np_table_buf)
        response = swigCreateTable(self.c_engine, ptableBuf, np_table_buf.shape[0])
        return Response(code =response["code"], msg = response["msg"])

    def upsert(self, documents: List[Union[dict, Document]]) -> UpsertResponse:
        """add or update docs
        documents: documents
        return: UpsertResponse object with document IDs
        """
        if not isinstance(documents, list):
            ex = Exception(
                "The add function takes an incorrect argument; it must be of a list type."
            )
            raise ex
        doc_ids = []

        for document in documents:
            if isinstance(document, dict):
                id_str = ""
                doc = Document(datas=document)
                if not doc.has_id_field():
                    id_str = self.create_id()
                    doc.add_field(Field("_id", id_str, dataType.STRING))
                else:
                    id_str = doc.get_id_field().value
                doc.to_fields(self.table)
                buf = doc.serialize()
                if buf is None:
                    ex = Exception("Document serialize failed.")
                    raise ex
                np_buf = np.array(buf)
                response_code = swigAddOrUpdateDoc(self.c_engine, swig_ptr(np_buf), len(np_buf))
                doc_ids.append((id_str, response_code))
            elif isinstance(document, Document):
                id_str = ""
                if not document.has_id_field():
                    id_str = self.create_id()
                    document.add_field(Field("_id", id_str, dataType.STRING))
                else:
                    id_str = document.get_id_field().value
                buf = document.serialize()
                np_buf = np.array(buf)
                response_code = swigAddOrUpdateDoc(self.c_engine, swig_ptr(np_buf), len(np_buf))
                doc_ids.append((id_str, response_code))
            else:
                ex = Exception(
                    "The add function takes an incorrect argument; it must be of a dict or Document type."
                )
                raise ex
        return UpsertResponse(code=0, msg="Upsert completed", document_ids=doc_ids)

    def get(self, doc_id: str) -> GetResponse:
        """Get document details by its ID.
        :param doc_id: Document ID.
        :return: GetResponse object with the document details.
        """
        if not isinstance(doc_id, str) and not isinstance(doc_id, int):
            raise ValueError("doc_id type should be string or int")

        if isinstance(doc_id, int):
            swig_buf = swigGetDocByDocID(self.c_engine, doc_id)
        elif isinstance(doc_id, str):
            value = np.frombuffer(doc_id.encode("utf-8"), dtype=np.uint8)
            np_buf = np.array(value)
            swig_buf = swigGetDocByID(self.c_engine, swig_ptr(np_buf), len(np_buf))

        if swig_buf is None:
            return GetResponse(code=1, msg="Document not found", document=None)

        np_buf = np.frombuffer(swig_buf, dtype=np.uint8)
        buf = np_buf.tobytes()
        doc = Document()
        doc.deserialize(buf, self.table)
        return GetResponse(code=0, msg="Document retrieved successfully", document=doc)

    def query(self, request: QueryRequest) -> QueryResponse:
        """Query documents based on the request."""
        buf = request.serialize(self.table)
        value = np.frombuffer(buf, dtype=np.uint8)
        np_buf = np.array(value)
        res = swigQuery(self.c_engine, swig_ptr(np_buf), len(np_buf))
        if res is None:
            return None

        response = QueryResponse(res, self.table)
        return response

    def search(self, request: SearchRequest) -> SearchResponse:
        """Search documents based on the request."""
        buf = request.serialize(self.table)
        value = np.frombuffer(buf, dtype=np.uint8)
        np_buf = np.array(value)
        res = swigSearch(self.c_engine, swig_ptr(np_buf), len(np_buf))
        if res is None:
            return None

        response = SearchResponse(res, self.table)
        return response

    def delete(self, doc_id: str) -> Response:
        """Delete a document by its ID.
        :param doc_id: Document ID to delete.
        :return: Response object with code and message.
        """
        if not isinstance(doc_id, str):
            raise ValueError("doc_id type should be string")

        id_len = len(doc_id)
        doc_id = np.frombuffer(doc_id.encode("utf-8"), dtype="uint8")
        doc_id_ptr = swig_ptr(doc_id)
        response_code = swigDeleteDoc(self.c_engine, doc_id_ptr, id_len)

        msg = "Document deleted successfully" if response_code == 0 else "Failed to delete document"
        return Response(code=response_code, msg=msg)

    def status(self) -> StatusResponse:
        """Get engine status information.
        :return: StatusResponse object with engine status details.
        """
        status_json = swigGetEngineStatus(self.c_engine)
        if status_json is None:
            return StatusResponse(code=1, msg="Failed to retrieve engine status", status={})

        status = json.loads(status_json)
        return StatusResponse(code=0, msg="Engine status retrieved successfully", status=status)

    def create_id(self):
        uid = str(uuid.uuid4())
        doc_id = "".join(uid.split("-"))
        return doc_id

    def clear(self):
        """Delete the path directory."""
        try:
            shutil.rmtree(self.config.path)
        except FileNotFoundError:
            raise(f"Directory not found: {self.config.path}")
        except Exception as e:
            raise(f"Failed to delete directory {self.config.path}: {e}")
