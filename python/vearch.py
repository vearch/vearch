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

import numpy as np
import copy 
import pickle
import uuid

from .swigvearch import *

###########################################
# vearch core
###########################################

numpy_dtype_map = {
    np.dtype('float32'): 'Float',
    np.dtype('uint8'): 'Byte',
    np.dtype('int8'): 'Char',
    np.dtype('uint64'): 'Uint64',
    np.dtype('int64'): 'Long',
    np.dtype('int32'): 'Int',
    np.dtype('float64'): 'Double'   
}

def byte_array_to_numpy(ba):
    ''' convert byte array to numpy array
        ba: byte array
    '''
    dtype = np.dtype('float32')
    vector = ByteArrayToFloatVector(ba)
    numpy_array = np.asarray(vector, dtype)
    return numpy_array

def normalize_numpy_array(numpy_array):
    array_size = len(numpy_array.shape)
    if array_size == 1:
        norm = np.linalg.norm(numpy_array)
        numpy_array = numpy_array / norm
    elif array_size == 2:
        norm = np.linalg.norm(numpy_array, axis=1)
        for i in range(len(norm)):
            numpy_array[i,:] = numpy_array[i,:] / norm[i]
    else:
        e = Exception("Wrong array, array shape' size should be 1 or 2")
        raise e
    return (numpy_array, norm)

def numpy_to_byte_array(numpy_array):
    ''' convert numpy array to byte array
        numpy_array: numpy array
    '''
    dtype = numpy_array.dtype
    dimension = 1

    for d in numpy_array.shape:
        dimension *= d
    ba = eval(numpy_dtype_map[dtype] + "sToByteArray")(swig_ptr(numpy_array), dimension)
    return ba

data_type_map = {
    DataTypes.INT: 'Int',
    DataTypes.LONG: 'Long',    
    DataTypes.FLOAT: 'Float',
    DataTypes.DOUBLE: 'Double',    
    DataTypes.STRING: 'String',
    DataTypes.VECTOR: 'Vector'   
}

metric_type_map = {
    "InnerProduct": 0,
    "L2" : 1
}

metric_types = [0, 1]

field_type = [
    "string",
    "integer", 
    "float",
    "vector"
]

field_type_map = {
    "string": DataTypes.STRING,
    "integer": DataTypes.LONG,
    "float": DataTypes.FLOAT,
    "vector": DataTypes.VECTOR  
}

index_status_map = {
    IndexStatuses.UNINDEXED : "UNINDEXED",
    IndexStatuses.INDEXING : "INDEXING",
    IndexStatuses.INDEXED : "INDEXED"
}

true_false_map = {
    "true": TRUE,
    "false": FALSE
}

response_code_map = {
    0: "SUCCESSED",
    1: "FAILED"
}

online_log_level = [
    "DEBUG", 
    "INFO", 
    "WARN", 
    "ERROR"
]

reserved_field = "_id"

class EngineTable:
    ''' engine table show how to initialize a table for 
        engine. It convert table info to the 
        way engine can accept. 
    '''
    def __init__(self, table, load_table=False):
        ''' initialize table
            table: engine table detail info, its name, its
            model info, its properties
            load_table: whether load table from dumped file
        '''
        self.load_table = load_table
        self.norms = {}
        self.init_name(table["name"])
        self.init_model(table["model"])
        self.init_properties(table["properties"])

    def valid_key(self, dict_object, *keys, **message):
        '''check dict whether have those key in keys'''
        for key in keys:
            if key not in dict_object:
                ex = Exception("Wrong engine table, " + message["message"] + " should have a key of " + key)
                raise ex
        return True            

    def valid_name(self):
        '''check table name
        '''
        if type(self.name) != str and self.name == "":
            ex = Exception("Wrong engine table, " + "should have a name which is't empty")
            raise ex
        return True

    def init_name(self, name):
        '''init table name
        '''
        self.name = name
        self.valid_name()

    def valid_model(self):
        '''check table model
        '''
        message = {}
        message["message"] = "table model"

        self.valid_key(self.model, "name", **message)

        if not self.model["name"] == "IVFPQ":
            ex = Exception("Unsupported model, now only support IVFPQ")
            raise ex

        self.valid_key(self.model, "ncentroids", \
                "nprobe", "nsubvector", "metric_type", **message)

        if not (1 <= self.model["nprobe"] and \
            self.model["nprobe"] <= self.model["ncentroids"]):
                ex = Exception("Wrong parameters of nprobe or ncentroids, nprobe should less than ncentroids")
                raise ex 

        if not self.model["metric_type"] in metric_types:
            ex = Exception("Wrong parameters of metric_type, should be 0 or 1")
            raise ex

        if not self.model["nsubvector"] % 4 == 0:
            ex = Exception("Wrong parameters of nsubvector, should be a multiple of 4")
            raise ex

        return True
    
    def init_model(self, model):
        ''' init table model,
            model: detail model info
        '''
        self.model = copy.deepcopy(model)
        self.model["nprobe"] = 20
        if "nprobe" in model and model["nprobe"] != -1:
            self.model["nprobe"] = model["nprobe"]

        self.model["ncentroids"] = 256
        if "ncentroids" in model and model["ncentroids"] != -1:
            self.model["ncentroids"] = model["ncentroids"]

        self.model["nsubvector"] = 64
        if "nsubvector" in model and model["nsubvector"] != -1:
            self.model["nsubvector"] = model["nsubvector"]            

        self.model["metric_type"] = 1
        if "metric_type" in model:
            self.model["metric_type"] = metric_type_map[model["metric_type"]]
        
        self.valid_model()

    def get_metric_type(self):
        return self.model["metric_type"]

    def valid_properties(self):
        ''' check table properties
        '''
        message = {}
        message["message"] = "table properties"

        if len(self.properties) == 0:
            ex = Exception("Empty table properties")
            raise ex
        for key in self.properties:
            if key == reserved_field and self.load_table == False:
                ex = Exception("Wrong table properties, _id is a reserved field")
                raise ex 
            if "type" not in self.properties[key]:
                ex = Exception("Wrong table properties, should have a key of type")
                raise ex         
            if self.properties[key]["type"] not in field_type:
                ex = Exception("Wrong table properties type")
                raise ex
            if self.properties[key]["type"] == "vector":
                message["message"] += (" " + key)
                self.valid_key(self.properties[key], "dimension", **message)
        return True

    def init_properties(self, properties):
        ''' check table properties
        '''
        self.properties = properties
        self.valid_properties()

        self.properties[reserved_field] = {
            "type":"string"
        }

    def get_field_type(self, key):
        ''' get table properties' field type
            key: table properties' field
        '''
        return field_type_map[self.properties[key]["type"]]

    def get_field_type_size(self):
        ''' get table properties' field type size
            return size of Non vector field type and 
            size of vector field type
        '''
        vector_type = 0
        for key in self.properties:
            if self.get_field_type(key) == DataTypes.VECTOR:
                vector_type += 1

        return (len(self.properties) - vector_type, vector_type)

    def get_vector_field_name(self):
        ''' get all vector fields' name
        '''
        vector_field_names = []
        for key in self.properties:
            if self.get_field_type(key) == DataTypes.VECTOR:
                vector_field_names.append(key)

        return vector_field_names 

    def get_return_fields(self, fields_name=None):
        ''' get return fields for query result
        '''
        if fields_name == None:
            fields_size, _ = self.get_field_type_size()
            fields_num = 0
            fields = MakeByteArrays(fields_size)

            for key in self.properties:
                if self.get_field_type(key) != DataTypes.VECTOR:
                    field = StringToByteArray(key)
                    SetByteArray(fields, fields_num, field)
                    fields_num += 1
        else:
            fields_size = len(fields_name)
            fields_num = 0
            fields = MakeByteArrays(fields_size)

            for field_name in fields_name:
                if field_name not in self.properties:
                    print("Wrong field name, %s not in table" %(field_name))
                    continue
                field = StringToByteArray(field_name)
                SetByteArray(fields, fields_num, field)
                fields_num += 1

        return (fields, fields_size)

    def get_vector_info(self, key):
        ''' get table properties' vector field detail info,
            and set default value for it
        '''
        value = self.properties[key]
        
        vector_info = {}

        vector_info["type"] = DataTypes.FLOAT #now just support float 

        vector_info["index"] = TRUE
        if "index" in value:
            vector_info["index"] = true_false_map[value["index"]]            
        
        vector_info["dimension"] = value["dimension"]

        vector_info["model_id"] = ""
        if "model_id" in value:
            vector_info["model_id"] = value["model_id"] 
        
        vector_info["retrieval_type"] =  "IVFPQ"
        if "retrieval_type" in value:
            vector_info["retrieval_type"] = value["retrieval_type"] 

        vector_info["store_type"] = "Mmap"
        if "store_type" in value:
            vector_info["store_type"] = value["store_type"]
        
        vector_info["store_param"] = ""
        if "store_param" in value:
            store_param = '{"cache_size": '
            store_param += str(value["store_param"]["cache_size"])
            store_param += "}"
            vector_info["store_param"] = store_param

        return vector_info

    def get_field_info(self, key):
        ''' get table properties' non vector field detail info,
            and set default value for it
        '''
        value = self.properties[key]
        
        field_info = {}

        field_info["type"] = DataTypes.STRING
        if "type" in value:
            field_info["type"] = field_type_map[value["type"]]           
        
        field_info["index"] = FALSE
        if "index" in value:
            field_info["index"] = true_false_map[value["index"]] 

        return field_info
    
    def parse_field_info(self):
        ''' parse table properties' field info, because
            it need to be tanslated to the way that vearch
            engine can accept.
        '''
        fields_info_size, vectors_info_size = self.get_field_type_size()
        fields_info = MakeFieldInfos(fields_info_size)
        vectors_info = MakeVectorInfos(vectors_info_size)
        field_num = 0
        vector_num = 0
        for key in self.properties:
            if self.get_field_type(key) == DataTypes.VECTOR:
                info = self.get_vector_info(key)

                vector_info = MakeVectorInfo( \
                    key, info["type"],\
                    info["index"], info["dimension"], \
                    info["model_id"], info["retrieval_type"], \
                    info["store_type"], info["store_param"])

                SetVectorInfo(vectors_info, vector_num, vector_info)
                vector_num += 1
            else:
                info = self.get_field_info(key)
                
                field_info = MakeFieldInfo( \
                    StringToByteArray(key), \
                    info["type"], \
                    info["index"])
                SetFieldInfo(fields_info, field_num, field_info)
                field_num += 1
        return (fields_info, fields_info_size, vectors_info, vectors_info_size)

    def parse_model_param(self):
        ''' parse table model' info, because
            it need to be tanslated to the way that vearch
            engine can accept. now just support ivfpq
        '''
        model_param = MakeIVFPQParameters( \
            self.model["metric_type"], \
            self.model["nprobe"], \
            self.model["ncentroids"], \
            self.model["nsubvector"], 8) 
        return (model_param, "IVFPQ")

    def destory_model_param(self, model_param, model_type):
        ''' when model info send to engine, it will be destoried
            model_param: model parameters
            model_type: model type
        '''
        eval("Destroy" + model_type + "Parameters")(model_param)

    def create_engine_table(self):
        ''' parse table info, because
            it need to be tanslated to the way that vearch
            engine can accept.
        '''
        table_name = StringToByteArray(self.name)

        model_param, model_type = self.parse_model_param()

        fields_info, fields_info_size, vectors_info, vectors_info_size = self.parse_field_info()

        table_info = MakeTable(table_name, fields_info, \
            fields_info_size, vectors_info, \
            vectors_info_size, model_param)

        return table_info

class Item:
    ''' Document Item will be added to engine, 
        containing some fields which defined 
        in engine table, and it tanslate document
        info to the way engine can accept.
    '''
    def __init__(self, info):
        ''' init document item
            info: document info
        '''
        self.info = info

    def valid(self, table):
        ''' check document item
            table: need auxiliary table info to check 
            its validation
        '''
        properties = table["properties"]
        for key in self.info:
            if key not in properties:
                except_info = "Item have error, " + key + " not in table properties"
                ex = Exception(except_info)
                raise ex
            if properties[key]["type"] == "vector" and "feature" not in self.info[key]:
                except_info = "Item's " + key + " should have key of feature"
                ex = Exception(except_info)
                raise ex

        #should also check item's value
        return True        

    def create_doc_item(self, table, doc_id):
        ''' tanslate document info to the way 
            engine can accept.
            table: table info
            doc_id: this document item's id
        '''
        #_id need extra one field
        fields_size = len(self.info) + 1
        fields_num = 0
        fields = MakeFields(fields_size)

        for key in self.info:
            name = StringToByteArray(key)

            data_type = table.get_field_type(key)

            if data_type == DataTypes.VECTOR:
                if isinstance(self.info[key], list):
                    self.info[key] = np.asarray(self.info[key], dtype="float32")
                metric_type = table.get_metric_type()
                if metric_type == 0:
                    self.info[key], norm = normalize_numpy_array(self.info[key])
                    table.norms[doc_id] = norm
                value = numpy_to_byte_array(self.info[key])
            elif data_type == DataTypes.STRING:
                value = StringToByteArray(self.info[key])
            else:
                value = eval(data_type_map[data_type] + "ToByteArray")(self.info[key])

            field = MakeField(name, value, data_type)
            SetField(fields, fields_num, field)
            fields_num += 1

        #_id should extra add
        name = StringToByteArray(reserved_field)
        value = StringToByteArray(doc_id)
        data_type = DataTypes.STRING
        field = MakeField(name, value, data_type)

        SetField(fields, fields_num, field)

        item = MakeDoc(fields, fields_size)
        return item

class Query:
    ''' Query for search in table, basiclly it have
        three parts: vector, filter, condition, fields.
        vector: used to search query results.
        filter: used to filter query results,
        get detail info by help(vearch.Query.init_filter()).
        condition: query condition, for example, 
        how many result will returned for query, 
        whethre to rank search result by its' id.
        fields: what field result will return.
        And it tanslate query info to the way 
        engine can accept.
    '''    
    def __init__(self, query):
        ''' init query
            query: query info
        '''
        self.init(query)

    def init(self, query):
        ''' init query info
        '''
        if "vector" in query:
            self.init_vector(query["vector"])
        else:
            self.init_vector()
        if "filter" in query:
            self.init_filter(query["filter"])
        else:
            self.init_filter()
        self.init_condition(query)

        if "fields" in query:
            self.init_return_fields(query["fields"])
        else:
            self.init_return_fields()

    def init_vector(self, vector=None):
        ''' init query vector info and set default
            value for it.
            vector: vector info, it can be none,
            now whole query only filter.
        '''
        if vector == None:
            self.vector = None
            return
        self.vector = copy.deepcopy(vector)

        for i in range(0, len(vector)):
            self.vector[i]["field"] = ""
            if "field" in vector[i]:
                self.vector[i]["field"] = vector[i]["field"]

            self.vector[i]["feature"] = None
            if "feature" in vector[i]:
                self.vector[i]["feature"] = vector[i]["feature"]

            self.vector[i]["min_score"] = 0.0
            if "min_score" in vector[i]:
                self.vector[i]["min_score"] = vector[i]["min_score"]

            self.vector[i]["max_score"] = 10000.0
            if "max_score" in vector[i]:
                self.vector[i]["max_score"] = vector[i]["max_score"]

            self.vector[i]["boost"] = 1
            if "boost" in vector[i]:
                self.vector[i]["boost"] = vector[i]["boost"]

            self.vector[i]["has_boost"] = 0
            if "has_boost" in vector[i]:
                self.vector[i]["has_boost"] = vector[i]["has_boost"]

    def init_filter(self, filters=None):
        ''' init query filter info. Before using 
            filter, should set its index value as true 
            in table. Because filter only works when 
            it's indexed. And these are two type filter,
            one is range which field type should be 
            integer; one is term which field type should 
            be string.
            filters: filter info, 
        '''
        self.filter = filters

    def init_condition(self, condition):
        ''' init query condition info and set default
            value for it.
            condition: condition info.
        '''
        self.condition = {}
        self.condition["direct_search_type"] = 0
        if condition != None and "direct_search_type" in condition:
            self.condition["direct_search_type"] = condition["direct_search_type"]

        self.condition["online_log_level"] = "debug"
        if condition != None and "online_log_level" in condition:
            self.condition["online_log_level"] = condition["online_log_level"]

        self.condition["topn"] = 10
        if condition != None and "topn" in condition:
            self.condition["topn"] = condition["topn"]

        self.condition["has_rank"] = 1
        if condition != None and "has_rank" in condition:
            self.condition["has_rank"] = condition["has_rank"]

        self.condition["multi_vector_rank"] = 1
        if condition != None and "multi_vector_rank" in condition:
            self.condition["multi_vector_rank"] = condition["multi_vector_rank"]

    def init_return_fields(self, fields=None):
        ''' init query return fields info
            fields: what field will return in search result.
        '''
        self.fields = fields     

    def parse_vector_query(self, table):
        ''' parse query vector and tanslate it to
            the way engine can accept. req_num shows
            how many query vectors
            table: engine table need to parse vector
            query
        '''
        req_num = 1
        if self.vector == None:
            vector_querys = MakeVectorQuerys(0)
            return (vector_querys, 0, req_num)
        vector_querys = MakeVectorQuerys(len(self.vector))

        # In this situation, it's searching with
        # only one vector field, but maybe one or multi
        # vector
        if len(self.vector) == 1:
            if isinstance(self.vector[0]["feature"], list):
                self.vector[0]["feature"] = np.asarray(self.vector[0]["feature"], dtype="float32")
            shape = self.vector[0]["feature"].shape
            if len(shape) != 1:
                req_num = shape[0]

        for i in range(0, len(self.vector)):
            metric_type = table.get_metric_type()
            if isinstance(self.vector[i]["feature"], list):
                self.vector[i]["feature"] = np.asarray(self.vector[i]["feature"], dtype="float32")
            if metric_type == 0:
                self.vector[i]["feature"], _ = normalize_numpy_array(self.vector[i]["feature"])
            
            vector_query = MakeVectorQuery( \
                StringToByteArray(self.vector[i]["field"]), \
                numpy_to_byte_array(self.vector[i]["feature"]), \
                self.vector[i]["min_score"], \
                self.vector[i]["max_score"], \
                self.vector[i]["boost"], \
                self.vector[i]["has_boost"])
            SetVectorQuery(vector_querys, i, vector_query)

        return (vector_querys, len(self.vector), req_num)

    def get_filter_type_size(self):
        ''' get the size of range filter and
            the size of term filter
        '''
        range_size = 0
        term_size = 0
        if self.filter == None:
            return (range_size, term_size)

        for filter_type in self.filter:
            for key in filter_type:
                if key == "range":
                    range_size += 1
                elif key == "term":
                    term_size += 1
                else:
                    except_info = "filter have error, " + key + \
                    " not in [range, term]"
                    ex = Exception(except_info)
                    raise ex

        return (range_size, term_size)

    def parse_filter(self, table):
        ''' parse query filter and tanslate it to
            the way engine can accept.
        '''
        range_size, term_size = self.get_filter_type_size()
        
        range_num = 0
        term_num = 0
        
        range_filters = MakeRangeFilters(range_size)
        term_filters = MakeTermFilters(term_size)

        if self.filter == None:
            return (range_filters, range_size, term_filters, term_size)

        for filter_type in self.filter:
            for key in filter_type:
                if key == "range":
                    for field in filter_type[key]:
                        data_type = table.get_field_type(field)
                        if data_type != DataTypes.LONG:
                            print("field type error, range filter type should be integer")
                            return

                        name = StringToByteArray(field)
                        
                        if "lte" in filter_type[key][field]:
                            lower = filter_type[key][field]["lte"]
                            include_lower = 1
                        else:
                            lower = filter_type[key][field]["lt"]
                            include_lower = 0

                        if "gte" in filter_type[key][field]:
                            upper = filter_type[key][field]["gte"]
                            include_upper = 1
                        else:
                            upper = filter_type[key][field]["gt"]
                            include_upper = 0                        

                    ba_lower = eval(data_type_map[data_type] + "ToByteArray")(lower)
                    ba_upper = eval(data_type_map[data_type] + "ToByteArray")(upper)
                    range_filter = MakeRangeFilter(name, \
                                    ba_lower, \
                                    ba_upper, \
                                    include_lower, \
                                    include_upper)

                    SetRangeFilter(range_filters, range_num, range_filter)

                    range_num += 1
                else:
                    if len(filter_type[key]) != 2:
                        print("key error, term filter should have two key.")
                        return
                    for field in filter_type[key]:
                        if field == "operator":
                            operator = filter_type[key][field]
                            if operator == "and":
                                is_union = 0
                            if operator == "or":
                                is_union = 1                        
                        else:
                            data_type = table.get_field_type(field)
                            if data_type != DataTypes.STRING:
                                print("field type error, term filter type should be string")
                                return
                            name = StringToByteArray(field)
                            values = filter_type[key][field]
                            term_value = ""
                            for i in range(0, len(values)):
                                term_value += values[i]
                                if i != len(values) - 1:
                                    term_value += "\001"

                    term_filter = MakeTermFilter(name, \
                                    StringToByteArray(term_value), is_union)
                    SetTermFilter(term_filters, term_num, term_filter)
                    term_num += 1

        return (range_filters, range_size, term_filters, term_size)

    def create_query_request(self, table):
        '''convert query dict to engine query request
        '''
        vector_querys, vector_querys_size, req_num = self.parse_vector_query(table)
        
        fields, fields_size = table.get_return_fields(self.fields)

        range_filters, range_size, term_filters, term_size = self.parse_filter(table)
        
        request = MakeRequest(self.condition["topn"],
                    vector_querys, vector_querys_size, \
                    fields, fields_size, \
                    range_filters, range_size, \
                    term_filters, term_size, req_num, \
                    self.condition["direct_search_type"], \
                    StringToByteArray(self.condition["online_log_level"]), \
                    self.condition["has_rank"], \
                    self.condition["multi_vector_rank"]);

        return request

class Engine:
    ''' vearch core
        It is used to store, update and delete feature vectors, 
        build indexes for stored vectors,
        and find the nearest neighbor of vectors. 
    '''
    def __init__(self, path, max_doc_size):
        self.path = path
        self.max_doc_size = max_doc_size
        self.init()

    def init(self):
        ''' init vearch engine
            path: engine config path to save dump file or 
                something else engine will create
            max_doc_size: max doc size engine can afford
        '''
        config = MakeConfig(self.path, self.max_doc_size)
        self.engine = InitEngine(config)
        DestroyConfig(config)
    
    def init_log_dir(self, path):
        ''' init engine log dir
            path: engine to save log
        '''
        reponse_code = SetLogDictionary(StringToByteArray(path))
        return reponse_code

    def create_id(self):
        ''' create id for add doc
        '''
        uid = str(uuid.uuid4())
        doc_id = ''.join(uid.split('-'))
        return doc_id

    def close(self):
        ''' close engine
        '''
        response_code = CloseEngine(self.engine)
        return response_code

    def create_table(self, table_info):
        ''' create table for engine
            table_info: table detail info
        '''
        table = EngineTable(table_info)
        self.table = table
        engine_table = table.create_engine_table()

        response_code = CreateTable(self.engine, engine_table)

        DestroyTable(engine_table)

        return response_code

    def add(self, docs_info):
        ''' add docs into table
            docs_info: docs' detail info
            return: unique docs' id for docs
        '''
        #first add doc to table to save
        docs_id = []
        for doc_info in docs_info:
            doc_item = Item(doc_info)
            doc_id = self.create_id()
            doc = doc_item.create_doc_item(self.table, doc_id)
            AddOrUpdateDoc(self.engine, doc)
            docs_id.append(doc_id)
            DestroyDoc(doc)
        #then build index for them
        self.build_index()
        return docs_id

    def build_index(self):
        ''' build index for added docs
        '''
        response_code = BuildIndex(self.engine)
        return response_code

    def get_index_status(self):
        ''' get index status
            return  "UNINDEXED",
            or      "INDEXING",
            or      "INDEXED"
        '''
        index_status = GetIndexStatus(self.engine)
        return index_status_map[index_status]

    def del_doc(self, id):
        ''' delete doc
            id: delete doc' id
        '''
        response_code = DelDoc(self.engine, StringToByteArray(id))
        return response_code

    def del_doc_by_query(self, query_info):
        ''' delete docs by query
            query_info: what kind docs want to delete
        '''
        query = Query(query_info)
        request = query.create_query_request(self.table)
        response_code = DelDocByQuery(self.engine, request)
        DestroyRequest(request)
        return response_code

    def update_doc(self, doc_info, id):
        ''' update doc's info, now don't support to update
            string.
            doc_info: doc's new info
            id: which doc want to be updated
        '''
        doc_item = Item(doc_info)
        doc = doc_item.create_doc_item(self.table, id)
        response_code = UpdateDoc(self.engine, doc)
        DestroyDoc(doc)
        return response_code

    def search(self, query_info):
        ''' search in table
            query_info: search info
        '''
        query = Query(query_info)
        request = query.create_query_request(self.table)
        response = Search(self.engine, request)
        result = self.get_query_result(response, query.fields)

        DestroyRequest(request)
        DestroyResponse(response)

        return result

    def get_doc_by_id(self, doc_id):
        ''' get doc's detail info by its' id
            doc_id: doc's id
            info
        '''
        doc = GetDocByID(self.engine, StringToByteArray(doc_id))
        doc_info = self.get_doc_field(doc)
        return doc_info

    def get_doc_num(self):
        ''' get how many docs in table
        '''
        num = GetDocsNum(self.engine)
        return num

    def get_memory_bytes(self):
        ''' get how much memory used
        '''
        mem_bytes = GetMemoryBytes(self.engine)
        return mem_bytes

    def dump(self):
        ''' dump all info to disk
        '''
        # store table
        save_table_path = self.path + "/table.pickle"
        table = {}
        table["name"] = self.table.name
        table["model"] = self.table.model
        if table["model"]["metric_type"] == 0:
            table["model"]["metric_type"] = "InnerProduct"
        else:
            table["model"]["metric_type"] = "L2"

        table["properties"] = self.table.properties

        with open(save_table_path, 'wb') as handle:
            pickle.dump(table, handle, protocol=pickle.HIGHEST_PROTOCOL)
        
        save_norm_path = self.path + "/norm.pickle"
        with open(save_norm_path, 'wb') as handle:
            pickle.dump(self.table.norms, handle, protocol=pickle.HIGHEST_PROTOCOL)

        response_code = Dump(self.engine)

        return response_code

    def load(self):
        ''' load info from disk
            when load, need't to create table     
            table info will load from dump file
        '''

        # Load table
        load_table_path = self.path + "/table.pickle"
        with open(load_table_path, 'rb') as handle:
            table = pickle.load(handle)

        self.table = EngineTable(table, True)

        load_norm_path = self.path + "/norm.pickle"
        with open(load_norm_path, 'rb') as handle:
            self.table.norms = pickle.load(handle)

        response_code = Load(self.engine)

        return response_code     

    def get_doc_field(self, doc):
        ''' get doc's field detail info
        '''
        if doc == None:
            return None

        fields_info = {}
        for i in range(0, doc.fields_num):
            field = GetField(doc, i)
            name = ByteArrayToString(field.name)

            if field.data_type == DataTypes.VECTOR:
                fields_info[name] = byte_array_to_numpy(field.value)
            else:
                data_type = data_type_map[field.data_type]
                value = eval("ByteArrayTo" + data_type)(field.value)
                fields_info[name] = value

        return fields_info

    def get_query_result(self, response, vector_fields=None):
        '''convert search result that get from 
            table to python dict
            reponse: search result get from table
        '''
        query_results = []
        for i in range(0, response.req_num):
            query_result = {}

            results = GetSearchResult(response, i)
            query_result["total"] = results.total
            query_result["result_num"] = results.result_num
            query_result["results"] = []

            for j in range(0, results.result_num):
                result_item = GetResultItem(results, j)
                
                detail = {}
                detail["score"] = result_item.score
                if vector_fields != None:
                    fields_name = self.table.get_vector_field_name()

                detail["detail_info"] = self.get_doc_field(result_item.doc)
                #InnerProduct
                if vector_fields != None:
                    if self.table.get_metric_type() == 0:
                        doc_id = detail["detail_info"]["_id"]
                        for vector in vector_fields:
                            if vector in fields_name:
                                detail["detail_info"][vector] *= self.table.norms[doc_id] 

                query_result["results"].append(detail)

            query_results.append(query_result)

        return query_results
