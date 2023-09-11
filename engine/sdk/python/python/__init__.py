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

Vearch have four builtins.object
    Engine
    EngineTable
    Item
    Query
use help(vearch.Object) to get more detail infomations.
"""
import time
import sys
import numpy as np
import copy
import pickle
import uuid
import json
import flatbuffers
from typing import List
from .swigvearch import *
from . import DataType as PDataType
from . import gamma_api

# from . gamma_api import *
from .gamma_api import Attribute
from .gamma_api import Doc as PDoc
from .gamma_api import Response as PResponse
from .gamma_api import SearchResultCode
from .gamma_api import Table
from .gamma_api import VectorInfo
from .gamma_api import Config
from .gamma_api import CacheInfo
from .gamma_api import EngineStatus
from .gamma_api import MemoryInfo
from .gamma_api import Field as PField
from .gamma_api import Request as PRequest
from .gamma_api import SearchResult as PSearchResult
from .gamma_api import TermFilter as PTermFilter
from .gamma_api import VectorQuery as PVectorQuery
from .gamma_api import FieldInfo
from .gamma_api import RangeFilter as PRangeFilter

###########################################
# vearch core
###########################################

dataType = PDataType.DataType
type_map = {
    dataType.INT: "int32",
    dataType.LONG: "int64",
    dataType.FLOAT: "float32",
    dataType.DOUBLE: "float64",
    dataType.STRING: "string",
    dataType.VECTOR: "vector",
}

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
}

dtype_name_map = {
    "float32": "Float",
    "uint8": "Byte",
    "int8": "UChar",
    "uint64": "UChar",
    "int64": "Long",
    "int32": "Int",
    "float64": "Double",
    "string": "String",
}


def vector_to_array(v):
    """convert a C++ vector to a numpy array"""
    classname = v.__class__.__name__
    assert classname.endswith("Vector")
    dtype = np.dtype(vector_name_map[classname[:-6]])
    a = np.empty(v.size(), dtype=dtype)
    if v.size() > 0:
        memcpy(swig_ptr(a), swigGetVectorPtr(v), a.nbytes)
    return a


def normalize_numpy_array(numpy_array):
    array_size = len(numpy_array.shape)
    if array_size == 1:
        norm = np.linalg.norm(numpy_array)
        numpy_array = numpy_array / norm
    elif array_size == 2:
        norm = np.linalg.norm(numpy_array, axis=1)
        for i in range(len(norm)):
            numpy_array[i, :] = numpy_array[i, :] / norm[i]
    else:
        e = Exception("Wrong array, array shape' size should be 1 or 2")
        raise e
    return (numpy_array, norm)


class GammaCacheInfo:
    def __init__(self, field_name, cache_size):
        self.field_name = field_name
        self.cache_size = cache_size


class GammaConfig:
    def __init__(self, path, log_dir):
        self.path = path
        self.log_dir = log_dir
        self.cache_infos = []

    def add_cache_info(self, cache_info):
        self.cache_infos.append(cache_info)

    def serialize(self):
        builder = flatbuffers.Builder(1024)
        path = builder.CreateString(self.path)
        log_dir = builder.CreateString(self.log_dir)
        cache_infos = []
        for cache_info in self.cache_infos:
            CacheInfo.CacheInfoStart(builder)
            field_name = builder.CreateString(cache_info.field_name)
            CacheInfo.CacheInfoAddFieldName(builder, field_name)
            CacheInfo.CacheInfoAddCacheSize(builder, cache_info.cache_size)
            cache_infos.append(CacheInfo.CacheInfoEnd(builder))
        Config.ConfigStartCacheInfosVector(builder, len(cache_infos))
        for cache_info in cache_infos:
            builder.PrependUOffsetTRelative(cache_info)
        config_cache_infos = builder.EndVector(len(cache_infos))

        Config.ConfigStart(builder)
        Config.ConfigAddPath(builder, path)
        Config.ConfigAddLogDir(builder, log_dir)
        Config.ConfigAddCacheInfos(builder, config_cache_infos)
        engine = Config.ConfigEnd(builder)
        builder.Finish(engine)
        buf = builder.Output()
        return buf

    def deserialize(self, buf):
        engine = Config.Config.GetRootAsConfig(buf, 0)
        self.path = engine.Path()
        self.log_dir = engine.LogDir()


class GammaFieldInfo:
    def __init__(self, name: str, data_type: int, is_index: bool = False):
        self.name = name
        self.type = data_type
        self.is_index = is_index

    def print_self(self):
        print("name:", self.name)
        print("type:", self.type)
        print("index:", self.is_index)


class GammaVectorInfo:
    def __init__(
        self,
        name: str,
        dimension: int,
        is_index: bool = True,
        type: int = dataType.VECTOR,
        model_id: str = "",
        store_type: str = "MemoryOnly",
        store_param: dict = {},
        has_source: bool = False,
    ):
        self.name = name
        self.type = type
        self.is_index = is_index
        self.dimension = dimension
        self.model_id = model_id
        self.store_type = store_type
        self.store_param = store_param
        self.has_source = has_source

    def print_self(self):
        print("name:", self.name)
        print("type:", self.type)
        print("index:", self.is_index)
        print("dimension:", self.dimension)
        print("model_id:", self.model_id)
        print("store_type:", self.store_type)
        print("store_param:", self.store_param)
        print("has_source:", self.has_source)


class ParseTable:
    def __init__(self, engine_info: dict):
        self.engine_info = engine_info

    def parse_field(self, fields: List[GammaFieldInfo]):
        field_infos = {}
        is_long_type_id = False
        for field in fields:
            name = field.name
            if name == "_id":
                if field.type == dataType.LONG:
                    is_long_type_id = True
                if field.type != dataType.LONG and field.type != dataType.STRING:
                    ex = Exception(
                        'The "type" of "_id" fields must is "string" or "integer"'
                    )
                    raise ex
            field_infos[name] = field
        return field_infos, is_long_type_id

    def parse_other_info(self):
        engine = self.engine_info
        if engine.get("index_size") == None:
            engine["index_size"] = 100000

        if engine.get("retrieval_type") == None:
            engine["retrieval_type"] = "IVFPQ"

        is_binaryivf = False
        if engine["retrieval_type"] == "BINARYIVF":
            is_binaryivf = True
            ex = Exception(
                "Now don't support binary ivf, will support in later version"
            )
            raise ex
        # retrieval_param of engine parse

        if engine.get("retrieval_param") == None:
            engine["retrieval_param"] = ""
        engine["compress_mode"] = 0
        engine["retrieval_types"] = []
        engine["retrieval_params"] = []

        return engine, is_binaryivf

    def parse_vector(self, vector_field: GammaVectorInfo):
        vec_infos = {}
        vec_infos[vector_field.name] = vector_field
        return vec_infos


class GammaTable:
    def __init__(self):
        self.norms = {}
        self.engine = {}
        self.vec_infos = {}
        self.field_infos = {}
        self.name = None
        self.is_binaryivf = False
        self.is_long_type_id = False

    def init(
        self,
        engine_info: dict,
        fields: List[GammaFieldInfo],
        vector_field: GammaVectorInfo,
    ):
        parseTable = ParseTable(engine_info)
        self.engine, self.is_binaryivf = parseTable.parse_other_info()
        self.field_infos, self.is_long_type_id = parseTable.parse_field(fields)
        self.vec_infos = parseTable.parse_vector(vector_field)
        for key in self.vec_infos:
            self.norms[key] = {}
        if "_id" not in self.field_infos:
            self.field_infos["_id"] = GammaFieldInfo("_id", dataType.STRING, False)
        if len(self.vec_infos) == 0:
            ex = Exception("There are no vector fields")
            raise ex

    def check_dimension(self, input_dimension, field_name):
        if field_name not in self.vec_infos:
            ex = Exception(
                "The {} field is not a field that was set when the table was built.".format(
                    field_name
                )
            )
            raise ex
        if self.is_binaryivf:
            if int(self.vec_infos[field_name].dimension / 8) != input_dimension:
                ex = Exception(
                    "dimension of add data is not correct. Since the model is BINARYIVF, a vector is {}*uint8.".format(
                        int(self.vec_infos[field_name].dimension / 8)
                    )
                )
                raise ex
        else:
            if self.vec_infos[field_name].dimension != input_dimension:
                ex = Exception("dimension of add data is not correct.")
                raise ex
        return True

    def is_binaryivf_type(self):
        return self.is_binaryivf

    def ser_vector_infos(self, builder, vec_infos):
        lst_VecInfos = []
        for key in sorted(vec_infos.keys(), reverse=True):
            fb_str_name = builder.CreateString(vec_infos[key].name)
            fb_str_store_type = builder.CreateString(vec_infos[key].store_type)
            fb_str_store_param = builder.CreateString(
                json.dumps(vec_infos[key].store_param)
            )
            fb_str_model_id = builder.CreateString(vec_infos[key].model_id)
            VectorInfo.VectorInfoStart(builder)
            VectorInfo.VectorInfoAddName(builder, fb_str_name)
            VectorInfo.VectorInfoAddDimension(builder, vec_infos[key].dimension)
            VectorInfo.VectorInfoAddIsIndex(builder, vec_infos[key].is_index)
            VectorInfo.VectorInfoAddDataType(builder, vec_infos[key].type)
            VectorInfo.VectorInfoAddModelId(builder, fb_str_model_id)
            VectorInfo.VectorInfoAddStoreType(builder, fb_str_store_type)
            VectorInfo.VectorInfoAddStoreParam(builder, fb_str_store_param)
            VectorInfo.VectorInfoAddHasSource(builder, vec_infos[key].has_source)
            lst_VecInfos.append(VectorInfo.VectorInfoEnd(builder))
        Table.TableStartVectorsInfoVector(builder, len(vec_infos))
        for i in lst_VecInfos:
            builder.PrependUOffsetTRelative(i)
        return builder.EndVector(len(vec_infos))

    def ser_field_infos(self, builder, field_infos):
        lst_fieldInfos = []
        for key in sorted(field_infos.keys(), reverse=True):
            fb_str_name = builder.CreateString(field_infos[key].name)
            FieldInfo.FieldInfoStart(builder)
            FieldInfo.FieldInfoAddName(builder, fb_str_name)
            FieldInfo.FieldInfoAddDataType(builder, field_infos[key].type)
            FieldInfo.FieldInfoAddIsIndex(builder, field_infos[key].is_index)
            lst_fieldInfos.append(FieldInfo.FieldInfoEnd(builder))
        Table.TableStartFieldsVector(builder, len(field_infos))
        for i in lst_fieldInfos:
            builder.PrependUOffsetTRelative(i)
        return builder.EndVector(len(field_infos))

    def serialize(self):
        builder = flatbuffers.Builder(1024)
        name = builder.CreateString(self.name)
        retrieval_type = builder.CreateString(self.engine["retrieval_type"])
        retrieval_param = builder.CreateString(
            json.dumps(self.engine["retrieval_param"])
        )
        ser_fields = self.ser_field_infos(builder, self.field_infos)
        ser_vectors = self.ser_vector_infos(builder, self.vec_infos)
        lst_types = []
        for val in self.engine["retrieval_types"]:
            fb_str = builder.CreateString(val)
            lst_types.append(fb_str)
        VectorInfo.VectorInfoStart(builder)
        for val in lst_types:
            builder.PrependUOffsetTRelative(val)
        retrieval_types = builder.EndVector(len(lst_types))

        lst_params = []
        for val in self.engine["retrieval_params"]:
            fb_str = builder.CreateString(val)
            lst_params.append(fb_str)
        VectorInfo.VectorInfoStart(builder)
        for val in lst_params:
            builder.PrependUOffsetTRelative(val)
        retrieval_params = builder.EndVector(len(lst_params))

        Table.TableStart(builder)
        Table.TableAddName(builder, name)
        Table.TableAddFields(builder, ser_fields)
        Table.TableAddVectorsInfo(builder, ser_vectors)
        Table.TableAddIndexingSize(builder, self.engine["index_size"])
        Table.TableAddCompressMode(builder, self.engine["compress_mode"])
        Table.TableAddRetrievalType(builder, retrieval_type)
        Table.TableAddRetrievalParam(builder, retrieval_param)
        Table.TableAddRetrievalTypes(builder, retrieval_types)
        Table.TableAddRetrievalParams(builder, retrieval_params)
        builder.Finish(Table.TableEnd(builder))
        self.table_buf = builder.Output()
        return self.table_buf

    def deserialize(self, buf):
        table = Table.Table.GetRootAsTable(buf, 0)
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
                model_id = table.VectorsInfo(i).ModelId().decode("utf-8"),
                store_type = table.VectorsInfo(i).StoreType().decode("utf-8"),
                store_param = json.loads(table.VectorsInfo(i).StoreParam()),
                has_source = table.VectorsInfo(i).HasSource(),
            )
            self.vec_infos[vec_info.name] = vec_info

        self.engine["index_size"] = table.IndexingSize()
        self.engine["retrieval_type"] = table.RetrievalType().decode("utf-8")
        self.engine["retrieval_param"] = json.loads(table.RetrievalParam())
        self.is_binaryivf = (
            True if self.engine["retrieval_type"] == "BINARYIVF" else False
        )
        self.is_long_type_id = (
            True if self.field_infos["_id"].type == dataType.LONG else False
        )

    def print_table_detail_infor(self):
        print("-------------table name-------------")
        print(self.name)
        print("---------engine information----------")
        print(self.engine)
        for vec_name in self.vec_infos:
            print("---------vector field information----------")
            self.vec_infos[vec_name].print_self()
        for field_name in self.field_infos:
            print("---------doc field information----------")
            self.field_infos[field_name].print_self()


class GammaField:
    def __init__(self, name, value, source: str, data_type):
        self.name = name
        self.origin_value = value
        if data_type == dataType.VECTOR:
            if value.dtype == np.float32:
                self.value = value.view(np.uint8)
            else:
                self.value = value
        elif data_type == dataType.STRING:
            value = value.encode("utf-8")
            self.value = np.frombuffer(value, dtype=np.uint8)
        else:
            np_value = np.asarray([value], dtype=type_map[data_type])
            self.value = np_value.view(np.uint8)
        self.source = source
        self.type = data_type

    def print_self(self):
        print("name:", self.name)
        print("value:", self.origin_value)
        print("source:", self.source)
        print("type:", self.type)

    def get_field_info(self):
        return self.name, self.origin_value


class GammaDoc:
    def __init__(self):
        self.fields = []

    def create_doc(self):
        self.doc = Doc()

    def delete_doc(self):
        swigDeleteDoc(self.doc)

    def get_vecfield_vector(self, table, field_name, vector):
        if not isinstance(vector, (np.ndarray, list)):
            ex = Exception("Vector type have error,  Vector type is numpy or list")
            raise ex
        if isinstance(vector, list):
            vector = np.asarray(vector)
        if table.is_binaryivf_type():
            vector = vector.astype(np.uint8)
        else:
            vector = vector.astype(np.float32)
            # vector = vector[:table.vec_infos[field_name].dimension]
        table.check_dimension(vector.shape[0], field_name)
        return vector

    def check_scalar_field_type(self, variate, field_name, data_type):
        if isinstance(variate, int) and (
            data_type == dataType.INT or data_type == dataType.LONG
        ):
            return True
        elif isinstance(variate, str) and data_type == dataType.STRING:
            return True
        elif isinstance(variate, float) and (
            data_type == dataType.DOUBLE or data_type == dataType.FLOAT
        ):
            return True
        ex = Exception(
            'The "{}" field type have error, field type should be (string, float, int) but is type {}'.format(
                field_name, type(variate)
            )
        )
        raise ex
        return False

    def parse_doc(self, table: GammaTable, doc_info: dict, doc_id):
        for key in doc_info.keys():
            if key in table.vec_infos:  # is vector fields
                vector = self.get_vecfield_vector(table, key, doc_info[key])
                # if not table.is_binaryivf_type():
                #    vector, norm = normalize_numpy_array(vector)
                #    table.norms[key][doc_id] = norm
                fieldNode = GammaField(key, vector, "source", dataType.VECTOR)
            elif key in table.field_infos:  # is fields
                self.check_scalar_field_type(
                    doc_info[key], key, table.field_infos[key].type
                )
                fieldNode = GammaField(
                    key, doc_info[key], "source", table.field_infos[key].type
                )
            else:
                ex = Exception("Item have error, " + key + " not in table properties")
                raise ex
            self.fields.append(fieldNode)
        if len(self.fields) != (len(table.field_infos) + len(table.vec_infos)):
            ex = Exception("There are fields with no values.")
            raise ex

    def create_item(self, table: GammaTable, doc_id: str, doc_info: dict):
        if "_id" not in doc_info:
            doc_info["_id"] = doc_id
        self.parse_doc(table, doc_info, doc_info["_id"])
        self.doc = Doc()
        self.set_doc()
        self.doc.SetKey(doc_id)
        return doc_info["_id"]

    def set_doc(self):
        for field in self.fields:
            if field.type == dataType.VECTOR:
                field_type = CreateVectorField(
                    field.name,
                    swig_ptr(field.value),
                    field.value.shape[0],
                    field.source,
                    field.type,
                )
            else:
                field_type = eval(
                    "Create" + dtype_name_map[type_map[field.type]] + "ScalarField"
                )(
                    field.name,
                    field.origin_value,
                    field.value.shape[0],
                    field.source,
                    field.type,
                )
            self.doc.AddField(field_type)

    def create_doc_item(self, table, doc_id, doc_info):
        if "_id" not in doc_info:
            doc_info["_id"] = doc_id
        self.parse_doc(table, doc_info, doc_info["_id"])
        buf = self.serialize()
        np_buf = np.array(buf)
        return np_buf, doc_info["_id"]

    def get_fields_dict(self):
        fields_dict = {}
        for node in self.fields:
            name, value = node.get_field_info()
            fields_dict[name] = value
        return fields_dict

    def serialize(self):
        builder = flatbuffers.Builder(1024)
        lstFieldData = []
        for i in range(len(self.fields)):
            nameData = builder.CreateString(self.fields[i].name)
            sourceData = builder.CreateString(self.fields[i].source)

            bytesOfValue = self.fields[i].value.tobytes()
            PField.FieldStartValueVector(builder, len(bytesOfValue))
            builder.head = builder.head - len(bytesOfValue)
            builder.Bytes[
                builder.head : (builder.head + len(bytesOfValue))
            ] = bytesOfValue
            valueData = builder.EndVector(len(bytesOfValue))

            PField.FieldStart(builder)
            PField.FieldAddName(builder, nameData)
            PField.FieldAddSource(builder, sourceData)
            PField.FieldAddValue(builder, valueData)
            PField.FieldAddDataType(builder, self.fields[i].type)
            lstFieldData.append(PField.FieldEnd(builder))

        PDoc.DocStartFieldsVector(builder, len(lstFieldData))
        # print(dir(builder))
        for j in reversed(range(len(lstFieldData))):
            builder.PrependUOffsetTRelative(lstFieldData[j])
        fields = builder.EndVector(len(lstFieldData))

        PDoc.DocStart(builder)
        PDoc.DocAddFields(builder, fields)
        builder.Finish(PDoc.DocEnd(builder))
        return builder.Output()

    def deserialize(self, buf, table, _id=None):
        doc = PDoc.Doc.GetRootAsDoc(buf, 0)
        # return doc
        self.fields = []
        for i in range(0, doc.FieldsLength()):
            name = doc.Fields(i).Name().decode("utf-8")
            value = doc.Fields(i).ValueAsNumpy()
            data_type = doc.Fields(i).DataType()
            if data_type == dataType.STRING:
                value = value.tobytes().decode("utf-8")
            elif data_type == dataType.VECTOR or data_type < 0 or data_type > 5:
                if table.is_binaryivf_type():
                    value = value.view(dtype=np.uint8)[4:].copy()
                else:
                    value = value.view(dtype=np.float32)[1:].copy()
                    if _id in table.norms[name]:
                        value *= table.norms[name][_id]
                data_type = dataType.VECTOR
            else:
                value = value.view(dtype=type_map[data_type])[0]
            self.fields.append(
                GammaField(
                    name, value, doc.Fields(i).Source().decode("utf-8"), data_type
                )
            )


class GammaRangeFilter:
    def __init__(self, field, lower_value, upper_value, include_lower, include_upper):
        self.field = field
        self.lower_value = lower_value.view(np.uint8)
        self.upper_value = upper_value.view(np.uint8)
        self.lower_value_str = lower_value
        self.upper_value_str = upper_value
        self.include_lower = include_lower
        self.include_upper = include_upper

    def print_self(self):
        print("RangeFilter")
        print(self.lower_value)
        print(self.upper_value)
        print(self.include_upper)
        print(self.include_lower)
        print("RangFilter end")


class GammaTermFilter:
    def __init__(self, field_name, value, is_union):
        self.field = field_name
        try:
            bytes_value = value.encode("utf-8")
            self.np_value = np.frombuffer(bytes_value, np.uint8)
        except:
            ex = Exception(
                'For TermFilter, Please use characters in "ASCII", other characters are not supported for the time being.'
            )
            raise ex
        self.value = value
        self.is_union = is_union

    def print_self(self):
        print("TermFilter")
        print(self.value)
        print(self.field)
        print(self.is_union)
        print("TermFilter end")


class GammaVectorQuery:
    def __init__(
        self, name, value, min_score, max_score, boost, has_boost, retrieval_type
    ):
        self.name = name
        self.value = value
        self.min_score = min_score
        self.max_score = max_score
        self.boost = boost
        self.has_boost = has_boost
        self.retrieval_type = retrieval_type


class GammaRequest:
    def __init__(self):
        self.log_level_map = {"DEBUG": 0, "INFO": 1, "WARM": 2, "ERROR": 3}
        self.req_num = 0
        self.topn = 100
        self.brute_force_search = 0
        self.retrieval_params = ""
        self.has_rank = True
        self.online_log_level = "DEBUG"
        self.multi_vector_rank = 0
        self.l2_sqrt = False

        self.fields = []
        self.range_filters = []
        self.term_filters = []
        self.vec_fields = []

    def create_request(self, querys, table):
        self.parse_base_info(querys, table)
        (
            self.req_num,
            self.vec_fields,
            self.multi_vector_rank,
        ) = self.parse_vector_querys(querys, table)
        if "filter" in querys:
            for ft in querys["filter"]:
                if "range" in ft:
                    self.range_filters.append(
                        self.parse_range_filter(ft["range"], table)
                    )
                elif "term" in ft:
                    self.term_filters.append(self.parse_term_filter(ft["term"], table))
        self.request = swigCreateRequest()
        self.set_request(self.request)

    def set_request(self, request):
        request.SetReqNum(self.req_num)
        request.SetTopN(self.topn)
        request.SetBruteForceSearch(self.brute_force_search)
        request.SetRetrievalParams(self.retrieval_params)
        request.SetOnlineLogLevel(self.online_log_level)
        request.SetHasRank(self.has_rank)
        request.SetMultiVectorRank(self.multi_vector_rank)
        request.SetL2Sqrt(self.l2_sqrt)

        for field in self.fields:
            request.AddField(field)

        for range_filter in self.range_filters:
            range_filter_type = CreateRangeFilter(
                range_filter.field,
                swig_ptr(range_filter.lower_value),
                range_filter.lower_value.shape[0],
                swig_ptr(range_filter.upper_value),
                range_filter.upper_value.shape[0],
                range_filter.include_lower,
                range_filter.include_upper,
            )
            request.AddRangeFilter(range_filter_type)

        for term_filter in self.term_filters:
            term_filter_type = CreateTermFilter(
                term_filter.field, term_filter.value, term_filter.is_union
            )
            request.AddTermFilter(term_filter_type)

        for vector_query_p in self.vec_fields:
            vector_query = CreateVectorQuery(
                vector_query_p.name,
                swig_ptr(vector_query_p.value),
                vector_query_p.value.shape[0],
                vector_query_p.min_score,
                vector_query_p.max_score,
                vector_query_p.boost,
                vector_query_p.has_boost,
                vector_query_p.retrieval_type,
            )
            request.AddVectorQuery(vector_query)

    def parse_base_info(self, querys, table):
        if "retrieval_param" in querys and isinstance(querys["retrieval_param"], dict):
            self.retrieval_params = json.dumps(querys["retrieval_param"])

        if "topn" in querys and isinstance(querys["topn"], int) and querys["topn"] > 0:
            self.topn = querys["topn"]
        if "is_brute_search" in querys and isinstance(querys["is_brute_search"], int):
            self.brute_force_search = 1 if querys["is_brute_search"] == 1 else 0
        if "has_rank" in querys and isinstance(querys["has_rank"], bool):
            self.has_rank = querys["has_rank"]
        if (
            "online_log_level" in querys
            and querys["online_log_level"].upper() in self.log_level_map
        ):
            self.onlin_log_leval = querys["online_log_level"]
        if "l2_sqrt" in querys and isinstance(querys["l2_sqrt"], bool):
            self.l2_sqrt = querys["l2_sqrt"]
        # if 'multi_vector_rank' in querys and isinstance(querys['multi_vector_rank'], int):
        #    self.multi_vector_rank = 0 if querys['multi_vector_rank'] == 0 else 1
        if "fields" in querys:
            self.parse_return_fields(querys["fields"], table)
        else:
            self.parse_return_fields([], table)

    def parse_return_fields(self, fie, table):
        if not isinstance(fie, list):
            ex = Exception('The "fields" in the query parameter is of type list.')
            raise ex

        if len(fie) == 0:  # return all fields
            for key in table.field_infos.keys():
                self.fields.append(key)
            for key in table.vec_infos.keys():
                self.fields.append(key)
        else:
            for node in fie:
                if node not in table.field_infos and node not in table.vec_infos:
                    ex = Exception(node + " field does not exist.")
                    raise ex
                self.fields.append(node)
        if "_id" not in self.fields:
            self.fields.append("_id")

    def parse_term_filter(self, termFilters, table):
        field_name = ""
        for key in termFilters.keys():
            if key in table.field_infos:
                field_name = key

        if field_name == "":
            ex = Exception("The field name is not exist.")
            raise ex
        is_union = 1
        if "operator" in termFilters and isinstance(termFilters["operator"], str):
            if termFilters["operator"] == "and":
                is_union = 0
            if termFilters["operator"] == "not in":
                is_union = 2
        value = ""
        for str_value in termFilters[field_name]:
            if isinstance(str_value, str) == False:
                ex = Exception("The type of term is not string.")
                raise ex
            if value != "":
                value += "\001"
            value += str_value
        return GammaTermFilter(field_name, value, is_union)

    def parse_range_filter(self, rangeFileters, table):
        if len(rangeFileters) <= 0:
            return False, ""
        fieldName = ""
        for key in rangeFileters.keys():
            if key in table.field_infos:
                fieldName = key
        if fieldName != "":
            if "gte" in rangeFileters[fieldName] and isinstance(
                rangeFileters[fieldName]["gte"], (int, float)
            ):
                gte = np.asarray(
                    [rangeFileters[fieldName]["gte"]],
                    dtype=type_map[table.field_infos[fieldName].type],
                )
            else:
                ex = Exception(
                    "The gte of rangeFilter has error. Check the data type or whether it is null."
                )
                raise ex
            if "lte" in rangeFileters[fieldName] and isinstance(
                rangeFileters[fieldName]["lte"], (int, float)
            ):
                lte = np.asarray(
                    [rangeFileters[fieldName]["lte"]],
                    dtype=type_map[table.field_infos[fieldName].type],
                )
            else:
                ex = Exception(
                    "The lte of rangeFilter has error. Check the data type or whether it is null."
                )
                raise ex
            include_upper = True
            include_lower = True
            if "include_lower" in rangeFileters[fieldName] and isinstance(
                rangeFileters[fieldName]["include_lower"], bool
            ):
                include_lower = rangeFileters[fieldName]["include_lower"]
            if "include_upper" in rangeFileters[fieldName] and isinstance(
                rangeFileters[fieldName]["include_upper"], bool
            ):
                include_upper = rangeFileters[fieldName]["include_upper"]
            return GammaRangeFilter(fieldName, gte, lte, include_lower, include_upper)

    def parse_vector_querys(self, querys, table):
        if "vector" not in querys:
            return 1, [], 0
        lstVectorQuerys = querys["vector"]
        vec_fields = []
        req_num = 0
        for node in lstVectorQuerys:
            if "field" not in node or node["field"] not in table.vec_infos:
                ex = Exception("The " + node["field"] + " field is not table infor.")
                raise ex
            if "feature" not in node:
                ex = Exception("feature field is not existed.")
                raise ex
            if isinstance(node["feature"], np.ndarray) == False:
                ex = Exception("feature type is error. it is numpy")
                raise ex

            boost = 1
            has_boost = 0
            retrieval_type = ""
            min_score = -10000000
            max_score = 10000000
            tmpValue = node["feature"]
            query_num = node["feature"].shape[0]
            if tmpValue.ndim == 1:
                query_num = 1
            if req_num != 0 and req_num != query_num:
                ex = Exception("Multiple vector searches, different number of vectors.")
                raise ex
            req_num = query_num
            if table.is_binaryivf_type():
                tmpValue = tmpValue.astype(np.uint8)
            else:
                tmpValue = tmpValue.astype(np.float32)
                # tmpValue, _ = normalize_numpy_array(tmpValue)
            table.check_dimension(tmpValue.shape[tmpValue.ndim - 1], node["field"])
            tmpValue = tmpValue.flatten()
            if "min_score" in node and isinstance(node["min_score"], (int, float)):
                min_score = node["min_score"]
            if "max_score" in node and isinstance(node["max_score"], (int, float)):
                max_score = node["max_score"]
            if "boost" in node:
                boost = node["boost"]
                has_boost = 1
            if "retrieval_type" in node:
                retrieval_type = node["retrieval_type"]

            vec_fields.append(
                GammaVectorQuery(
                    node["field"],
                    tmpValue,
                    min_score,
                    max_score,
                    boost,
                    has_boost,
                    retrieval_type,
                )
            )
        req_num = 1 if req_num == 0 else req_num
        multi_vector_rank = 1 if len(vec_fields) > 1 else 0
        return req_num, vec_fields, multi_vector_rank

    def get_vecQuerys_seria(self, vecQuerys, builder):
        lst_seria = []
        for vecQue in vecQuerys:
            name = builder.CreateString(vecQue.name)
            fb_retrieval_type = builder.CreateString(vecQue.retrieval_type)

            bytesOfValue = vecQue.value.tobytes()
            PVectorQuery.VectorQueryStartValueVector(builder, len(bytesOfValue))
            builder.head = builder.head - len(bytesOfValue)
            builder.Bytes[
                builder.head : (builder.head + len(bytesOfValue))
            ] = bytesOfValue
            valueData = builder.EndVector(len(bytesOfValue))

            PVectorQuery.VectorQueryStart(builder)
            PVectorQuery.VectorQueryAddName(builder, name)
            PVectorQuery.VectorQueryAddValue(builder, valueData)
            PVectorQuery.VectorQueryAddMinScore(builder, vecQue.min_score)
            PVectorQuery.VectorQueryAddMaxScore(builder, vecQue.max_score)
            PVectorQuery.VectorQueryAddBoost(builder, vecQue.boost)
            PVectorQuery.VectorQueryAddHasBoost(builder, vecQue.has_boost)
            PVectorQuery.VectorQueryAddRetrievalType(builder, fb_retrieval_type)
            lst_seria.append(PVectorQuery.VectorQueryEnd(builder))

        PRequest.RequestStartVecFieldsVector(builder, len(lst_seria))
        for j in reversed(range(len(lst_seria))):
            builder.PrependUOffsetTRelative(lst_seria[j])
        return builder.EndVector(len(lst_seria))

    def get_range_filters_seria(self, rangeFilter, builder):
        lst_rangeFilterSera = []
        for rfNode in rangeFilter:
            fieldName = builder.CreateString(rfNode.field)
            bytesOfValue = rfNode.lower_value.tobytes()
            PRangeFilter.RangeFilterStartLowerValueVector(builder, len(bytesOfValue))
            builder.head = builder.head - len(bytesOfValue)
            builder.Bytes[
                builder.head : (builder.head + len(bytesOfValue))
            ] = bytesOfValue
            lowerValue = builder.EndVector(len(bytesOfValue))

            bytesOfValue = rfNode.upper_value.tobytes()
            PRangeFilter.RangeFilterStartUpperValueVector(builder, len(bytesOfValue))
            builder.head = builder.head - len(bytesOfValue)
            builder.Bytes[
                builder.head : (builder.head + len(bytesOfValue))
            ] = bytesOfValue
            upperValue = builder.EndVector(len(bytesOfValue))

            PRangeFilter.RangeFilterStart(builder)
            PRangeFilter.RangeFilterAddField(builder, fieldName)
            PRangeFilter.RangeFilterAddLowerValue(builder, lowerValue)
            PRangeFilter.RangeFilterAddUpperValue(builder, upperValue)
            PRangeFilter.RangeFilterAddIncludeLower(builder, rfNode.include_lower)
            PRangeFilter.RangeFilterAddIncludeUpper(builder, rfNode.include_upper)
            lst_rangeFilterSera.append(PRangeFilter.RangeFilterEnd(builder))

        PRequest.RequestStartRangeFiltersVector(builder, len(lst_rangeFilterSera))
        for j in reversed(range(len(lst_rangeFilterSera))):
            builder.PrependUOffsetTRelative(lst_rangeFilterSera[j])
        return builder.EndVector(len(lst_rangeFilterSera))

    def get_term_filters_seria(self, termFilters, builder):
        lstTermFilterseria = []
        for tf in termFilters:
            fieldName = builder.CreateString(tf.field)

            bytesOfValue = tf.np_value.tobytes()
            PTermFilter.TermFilterStartValueVector(builder, len(bytesOfValue))
            builder.head = builder.head - len(bytesOfValue)
            builder.Bytes[
                builder.head : (builder.head + len(bytesOfValue))
            ] = bytesOfValue
            valueData = builder.EndVector(len(bytesOfValue))

            PTermFilter.TermFilterStart(builder)
            PTermFilter.TermFilterAddField(builder, fieldName)
            PTermFilter.TermFilterAddValue(builder, valueData)
            PTermFilter.TermFilterAddIsUnion(builder, tf.is_union)

            lstTermFilterseria.append(PTermFilter.TermFilterEnd(builder))

        PRequest.RequestStartTermFiltersVector(builder, len(lstTermFilterseria))
        for j in reversed(range(len(lstTermFilterseria))):
            builder.PrependUOffsetTRelative(lstTermFilterseria[j])
        return builder.EndVector(len(lstTermFilterseria))

    def get_fields_seria(self, fields, builder):
        lstFields = []
        for f in fields:
            lstFields.append(builder.CreateString(f))

        PRequest.RequestStartFieldsVector(builder, len(lstFields))
        for j in reversed(range(len(lstFields))):
            builder.PrependUOffsetTRelative(lstFields[j])
        return builder.EndVector(len(lstFields))

    def serialize(self):
        builder = flatbuffers.Builder(1024)

        VqSeria = self.get_vecQuerys_seria(self.vec_fields, builder)
        RfSeria = self.get_range_filters_seria(self.range_filters, builder)
        TfSeria = self.get_term_filters_seria(self.term_filters, builder)
        FdSeria = self.get_fields_seria(self.fields, builder)
        RpSeria = builder.CreateString(self.retrieval_params)
        OllSeria = builder.CreateString(self.online_log_level)

        PRequest.RequestStart(builder)
        PRequest.RequestAddReqNum(builder, self.req_num)
        PRequest.RequestAddTopn(builder, self.topn)
        PRequest.RequestAddBruteForceSearch(builder, self.brute_force_search)
        PRequest.RequestAddFields(builder, FdSeria)
        PRequest.RequestAddVecFields(builder, VqSeria)
        PRequest.RequestAddRangeFilters(builder, RfSeria)
        PRequest.RequestAddTermFilters(builder, TfSeria)
        PRequest.RequestAddOnlineLogLevel(builder, OllSeria)
        PRequest.RequestAddRetrievalParams(builder, RpSeria)
        PRequest.RequestAddHasRank(builder, self.has_rank)
        PRequest.RequestAddMultiVectorRank(builder, self.multi_vector_rank)
        PRequest.RequestAddL2Sqrt(builder, self.l2_sqrt)

        builder.Finish(PRequest.RequestEnd(builder))
        return builder.Output()


class GammaEngineStatus:
    def __init__(self):
        self.index_status = None
        self.table_mem = None
        self.index_mem = None
        self.vector_mem = None
        self.field_range_mem = None
        self.bitmap_mem = None
        self.doc_num = None
        self.max_docid = None
        self.min_indexed_num = None

    def serialize(self):
        builder = flatbuffers.Builder(1024)
        EngineStatus.EngineStatusAddIndexStatus(builder, self.index_status)
        EngineStatus.EngineStatusAddTableMem(builder, self.table_mem)
        EngineStatus.EngineStatusAddIndexMem(builder, self.index_mem)
        EngineStatus.EngineStatusAddVectorMem(builder, self.vector_mem)
        EngineStatus.EngineStatusAddFieldRangeMem(builder, self.field_range_mem)
        EngineStatus.EngineStatusAddBitmapMem(builder, self.bitmap_mem)
        EngineStatus.EngineStatusAddDocNum(builder, self.doc_num)
        EngineStatus.EngineStatusAddMaxDocid(builder, self.max_docid)
        EngineStatus.EngineStatusAddMinIndexedNum(builder, self.min_indexed_num)
        builder.Finish(EngineStatus.EngineStatusEnd(builder))
        return builder.Output()

    def deserialize(self, buf):
        engine_status = EngineStatus.EngineStatus.GetRootAsEngineStatus(buf, 0)
        self.index_status = engine_status.IndexStatus()
        self.table_mem = engine_status.TableMem()
        self.index_mem = engine_status.IndexMem()
        self.vector_mem = engine_status.VectorMem()
        self.field_range_mem = engine_status.FieldRangeMem()
        self.bitmap_mem = engine_status.BitmapMem()
        self.doc_num = engine_status.DocNum()
        self.max_docid = engine_status.MaxDocid()
        self.min_indexed_num = engine_status.MinIndexedNum()

    def get_status_dict(self):
        status = {}
        status["index_status"] = self.index_status
        status["table_mem"] = self.table_mem
        status["vector_mem"] = self.vector_mem
        status["vector_mem"] = self.vector_mem
        status["field_range_mem"] = self.field_range_mem
        status["bitmap_mem"] = self.bitmap_mem
        status["doc_num"] = self.doc_num
        status["max_docid"] = self.max_docid
        status["min_indexed_num"] = self.min_indexed_num
        return status

class GammaMemoryInfo:
    def __init__(self):
        self.table_mem = None
        self.index_mem = None
        self.vector_mem = None
        self.field_range_mem = None
        self.bitmap_mem = None

    def serialize(self):
        builder = flatbuffers.Builder(1024)
        MemoryInfo.MemoryInfoAddTableMem(builder, self.table_mem)
        MemoryInfo.MemoryInfoAddIndexMem(builder, self.index_mem)
        MemoryInfo.MemoryInfoAddVectorMem(builder, self.vector_mem)
        MemoryInfo.MemoryInfoAddFieldRangeMem(builder, self.field_range_mem)
        MemoryInfo.MemoryInfoAddBitmapMem(builder, self.bitmap_mem)
        builder.Finish(MemoryInfo.MemoryInfoEnd(builder))
        return builder.Output()

    def deserialize(self, buf):
        memory_info = MemoryInfo.MemoryInfo.GetRootAsMemoryInfo(buf, 0)
        self.table_mem = memory_info.TableMem()
        self.index_mem = memory_info.IndexMem()
        self.vector_mem = memory_info.VectorMem()
        self.field_range_mem = memory_info.FieldRangeMem()
        self.bitmap_mem = memory_info.BitmapMem()

    def get_status_dict(self):
        status = {}
        status["table_mem"] = self.table_mem
        status["vector_mem"] = self.vector_mem
        status["vector_mem"] = self.vector_mem
        status["field_range_mem"] = self.field_range_mem
        status["bitmap_mem"] = self.bitmap_mem
        return status


class GammaResponse:
    def __init__(self, results=None, online_log_message=None):
        self.search_res = []
        self.online_log_message = online_log_message

    def npValue_to_value(self, table, name, np_value):
        if type(np_value) is not np.ndarray:
            return None
        if name in table.field_infos:
            if table.field_infos[name].type == dataType.STRING:
                value = np_value.tobytes().decode("utf-8")
            else:
                value = np_value.view(dtype=type_map[table.field_infos[name].type])[0]
        if name in table.vec_infos:
            if not table.is_binaryivf_type():
                value = np_value.view(np.float32)[1:].copy()
            else:
                value = np_value[4:].copy()
        return value

    def norm_to_origin(self, table, doc_id, _source, is_binary_ivf):
        for key in _source:
            if key in table.vec_infos:
                if not is_binary_ivf:
                    _source[key] *= table.norms[key][doc_id]
                _source[key] = _source[key].tolist()
        return _source

    def deserialize(self, table, buf):
        response = PResponse.Response.GetRootAsResponse(buf, 0)
        online_log_message = response.OnlineLogMessage().decode("utf-8")
        query_results = []
        for i in range(response.ResultsLength()):
            query_result = {}
            search_res = response.Results(i)
            item_res = []
            for j in range(search_res.ResultItemsLength()):
                detail = {}
                res_item = search_res.ResultItems(j)
                _source = {}
                _id = ""
                for k in range(res_item.AttributesLength()):
                    attri = res_item.Attributes(k)
                    np_value = attri.ValueAsNumpy()
                    name = attri.Name().decode("utf-8")
                    if name == "_id":
                        _id = self.npValue_to_value(table, name, np_value)
                    else:
                        _source[name] = self.npValue_to_value(table, name, np_value)
                _source = self.norm_to_origin(
                    table, _id, _source, table.is_binaryivf_type()
                )
                detail["_score"] = res_item.Score()
                detail["_id"] = _id
                detail["_source"] = _source
                item_res.append(detail)

            query_result["total"] = search_res.Total()
            query_result["results_msg"] = search_res.Msg().decode("utf-8")
            query_result["results"] = item_res
            query_results.append(query_result)
        self.query_results = query_results


class Engine:
    """vearch core
    It is used to store, update and delete feature vectors,
    build indexes for stored vectors,
    and find the nearest neighbor of vectors.
    """

    def __init__(self, path: str = "files", log_dir: str = "logs"):
        """init vearch engine
        path: engine config path to save dump file or
        something else engine will create.
        log_dir: the log_dir is where the logs are stored.
        """
        self.path = path
        self.log_dir = log_dir
        self.init()
        self.total_added_num = 0
        self.doc_ids = []
        self.verbose = False

    def init(self):
        config = GammaConfig(self.path, self.log_dir)
        buf = config.serialize()
        buf = np.array(buf)
        ptr_buf = swig_ptr(buf)
        self.c_engine = swigInitEngine(ptr_buf, buf.shape[0])

    def create_table(
        self,
        table_info,
        name: str,
        fields: List[GammaFieldInfo],
        vector_field: GammaVectorInfo,
    ):
        """create table for engine
        table_info: table detail info
        return: 0 successed, 1 failed
        """
        self.gamma_table = GammaTable()
        self.gamma_table.init(table_info, fields=fields, vector_field=vector_field)
        self.gamma_table.name = name
        table_buf = self.gamma_table.serialize()
        self.table_buf = table_buf
        # self.gamma_table.deserialize(table_buf)
        np_table_buf = np.array(table_buf)
        ptableBuf = swig_ptr(np_table_buf)
        response_code = swigCreateTable(self.c_engine, ptableBuf, np_table_buf.shape[0])
        return response_code

    def close(self):
        """close engine
        return: 0 successed, 1 failed
        """
        response_code = swigClose(self.c_engine)
        return response_code

    def add(self, docs_info: List):
        """add docs into table
        docs_info: docs' detail info
        return: unique docs' id for docs
        """
        if self.verbose:
            start = time.time()
        if not isinstance(docs_info, list):
            ex = Exception(
                "The add function takes an incorrect argument; it must be of a list type."
            )
            raise ex
        doc_ids = []
        docs = Docs()
        for doc_info in docs_info:
            id_str = self.create_id()
            doc = GammaDoc()
            doc_id = doc.create_item(self.gamma_table, id_str, doc_info)
            docs.AddDoc(doc.doc)
            doc_ids.append(doc_id)
        results = swigCreateBatchResult(len(doc_info))
        if self.verbose:
            print("prepare add cost %.4f s" % (time.time() - start))
            start = time.time()
        if swigAddOrUpdateDocsCPP(self.c_engine, docs, results) == 0:
            if self.verbose:
                print("gamma add cost %.4f s" % (time.time() - start))
                start = time.time()
            for i in range(len(docs_info)):
                if results.Code(i) == 0:
                    self.total_added_num += 1
        swigDeleteBatchResult(results)
        if self.verbose:
            print("finish add cost %.4f s" % (time.time() - start))
        return doc_ids

    def update_doc(self, doc_info, doc_id):
        """update doc's info. The docs_info must contain "_id" information.
        doc_info: doc's new info.
        """
        if not isinstance(doc_info, dict):
            ex = Exception(
                'The parameter doc_info of "update_doc" funtion is dict type.'
            )
            raise ex
        doc_info["_id"] = doc_id
        doc_item = GammaDoc()
        np_buf, _id = doc_item.create_doc_item(
            self.gamma_table, doc_info["_id"], doc_info
        )
        doc = swig_ptr(np_buf)
        response_code = swigAddOrUpdateDoc(self.c_engine, doc, np_buf.shape[0])
        return response_code

    def del_doc(self, doc_id):
        """delete doc
        doc_id: delete doc' id
        """
        id_len = 0
        if self.gamma_table.is_long_type_id:
            if not isinstance(doc_id, int):
                ex = Exception('"_id" type should is int.')
                raise ex
            doc_id = np.array([doc_id]).astype("int64")
            doc_id = doc_id.view(dtype=np.uint8)
            id_len = 8
        else:
            if not isinstance(doc_id, str):
                ex = Exception('"_id" type should is str.')
                raise ex
            id_len = len(doc_id)
            doc_id = doc_id.encode("utf-8")
            doc_id = np.frombuffer(doc_id, dtype="uint8")
        doc_id = swig_ptr(doc_id)
        response_code = swigDeleteDoc(self.c_engine, doc_id, id_len)
        return response_code

    def get_status(self):
        """get engine status information
        return: a dict containing status information
        """
        status_buf = swigGetEngineStatus(self.c_engine)
        np_status_buf = np.asarray(status_buf, dtype=np.uint8)
        buf = np_status_buf.tobytes()
        status = GammaEngineStatus()
        status.deserialize(buf)
        status_dict = status.get_status_dict()
        return status_dict
    
    def get_mempory_info(self):
        """get engine memory information
        return: a dict containing memory information
        """
        status_buf = swigGetMemoryInfo(self.c_engine)
        np_status_buf = np.asarray(status_buf, dtype=np.uint8)
        buf = np_status_buf.tobytes()
        status = GammaMemoryInfo()
        status.deserialize(buf)
        status_dict = status.get_status_dict()
        return status_dict
    

    def get_doc_by_id(self, doc_id):
        """get doc's detail info by its' id
        doc_id: doc's id
        """
        if isinstance(doc_id, int):
            swig_buf = swigGetDocByDocID(self.c_engine, doc_id)
        elif isinstance(doc_id, str):
            swig_buf = swigGetDocByID(self.c_engine, doc_id, len(doc_id))
        else:
            return {}
        np_buf = np.asarray(swig_buf, dtype=np.uint8)
        buf = np_buf.tobytes()
        if len(np_buf) == 1:
            return {}
        doc = GammaDoc()
        doc.deserialize(buf, self.gamma_table, doc_id)
        return doc.get_fields_dict()

    def dump(self):
        """dump all info to disk"""
        save_table_path = self.path + "/table.pickle"
        table_buf = self.table_buf
        with open(save_table_path, "wb") as handle:
            pickle.dump(table_buf, handle, protocol=pickle.HIGHEST_PROTOCOL)
        save_norm_path = self.path + "/norm.pickle"
        with open(save_norm_path, "wb") as handle:
            pickle.dump(
                self.gamma_table.norms, handle, protocol=pickle.HIGHEST_PROTOCOL
            )
        response_code = swigDump(self.c_engine)
        return response_code

    def load(self):
        """load info from disk
        when load, need't to create table
        table info will load from dump file
        """
        load_table_path = self.path + "/table.pickle"
        with open(load_table_path, "rb") as handle:
            self.table_buf = pickle.load(handle)

        self.gamma_table = GammaTable()
        self.gamma_table.deserialize(self.table_buf)
        np_table_buf = np.array(self.table_buf)
        ptableBuf = swig_ptr(np_table_buf)
        swigCreateTable(self.c_engine, ptableBuf, np_table_buf.shape[0])

        load_norm_path = self.path + "/norm.pickle"
        with open(load_norm_path, "rb") as handle:
            self.gamma_table.norms = pickle.load(handle)
        response_code = swigLoad(self.c_engine)
        return response_code

    def search(self, query_info):
        """search in table
        query_info: search info
        """
        start = time.time()
        req = GammaRequest()
        req.create_request(query_info, self.gamma_table)
        response = swigCreateResponse()
        if self.verbose:
            print("prepare search cost %f ms" % ((time.time() - start) * 1000))
            start = time.time()
        swigSearchCPP(self.c_engine, req.request, response)
        swigDeleteRequest(req.request)
        if self.verbose:
            print("gamma search cost %f ms" % ((time.time() - start) * 1000))
            start = time.time()
        results = self.get_results(response)
        swigDeleteResponse(response)
        if self.verbose:
            print("get results cost %f ms" % ((time.time() - start) * 1000))
        return results

    # def del_doc_by_query(self, query_info):
    #     ''' delete docs by query
    #         query_info: what kind docs want to delete
    #     '''
    #     request = GammaRequest()
    #     request.create_request(query_info, self.gamma_table)
    #     buf = request.serialize()
    #     np_buf = np.array(buf)
    #     p_buf = swig_ptr(np_buf)
    #     response_code = swigDelDocByQuery(self.c_engine, p_buf, np_buf.shape[0])
    #     return response_code

    def create_id(self):
        uid = str(uuid.uuid4())
        doc_id = "".join(uid.split("-"))
        return doc_id

    def get_results(self, response):
        search_results = response.Results()
        results = []
        for search_result in search_results:
            result = {}
            result["total"] = search_result.total
            result["result_code"] = search_result.result_code
            result["msg"] = search_result.msg
            result["result_items"] = []
            for result_item in search_result.result_items:
                result_item_info = {}
                result_item_info["score"] = result_item.score
                result_item_info["extra"] = result_item.extra
                for i in range(len(result_item.names)):
                    if result_item.names[i] in self.gamma_table.field_infos:
                        data_type = self.gamma_table.field_infos[
                            result_item.names[i]
                        ].type
                        if data_type == dataType.STRING:
                            value = result_item.values[i]
                        else:
                            value = eval(
                                "Get"
                                + dtype_name_map[type_map[data_type]]
                                + "FromStringVector"
                            )(result_item.values, i, data_type)
                    if result_item.names[i] in self.gamma_table.vec_infos:
                        # data_type = self.gamma_table.vec_infos[result_item.names[i]].type
                        value = GetFloatVectorFromStringVector(result_item.values, i, 0)
                        value = np.asarray(value, dtype="float32")
                    result_item_info[result_item.names[i]] = value
                result["result_items"].append(result_item_info)
            results.append(result)
        return results
