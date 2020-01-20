# API for Vearch Python SDK

This document shows how to use vearch python sdk.

A simple usage
-------------------------

1. Create a vearch engine.
    
    ```
    engine_path = "files"
    max_doc_size = 100000
    engine = vearch.Engine(path, max_doc_size)
    log_path = "logs"
    engine.init_log_dir(log_path)
    ```
    
2. Create a table for engine.
    
    ```
    table = {
        "name" : "test_table",
        "model" : {
            "name": "IVFPQ",
            "nprobe": -1,
            "metric_type": "L2",
            "ncentroids": -1,
            "nsubvector": -1
        },
        "properties" : {
            "key": {
                "type": "integer"
            },
            "feature": {
                "type": "vector",
                "dimension": 128,
                "store_param": {
                    "cache_size": 2000
                }
            },
        },
    }
    engine.create_table(table)
    ```
    
3. Add vector into table.
    
    ```
    add_num = 10000
    features = np.random.rand(add_num, 128)
    doc_items = []
    for i in range(add_num):
        profiles["key"] = i
       profiles["feature"] = features[i,:]
        doc_items.append(profiles)
    
    #pass list to it, even only add one doc item
    engine.add(doc_items)
    ```
    
4. Search vector nearest neighbors.
    
    ```
    query =  {
        "vector": [{
            "field": "feature",
            "feature": features[0,:],
        }],
    }
    result = engine.search(query)
    print(result)
    ```
    
5. See other detail info for deeper use.

Create Engine
=========================

create vearch engine:

```
engine = vearch.Engine(path, max_doc_size)
```

- path : engine config path to save dump file or something else engine will create
- max_doc_size : max documents for vearch engine

init log path:

```
engine.init_log_dir(log_path)
```

- log_path : engine log file will saved in the dir

Create table
========================

ceate vearch table:

```
table = {
    "name": "space1",
    "model": {
        "nprobe": 10,
        "metric_type": "InnerProduct",
        "ncentroids": 256,
        "nsubvector": 64,
        "nbits_per_idx": 8
    },
    "properties": {
        "field1": {
            "type": "keyword"
        },
        "field2": {
            "type": "integer"
        },
        "field3": {
            "type": "float",
            "index": "true"
        },
        "field4": {
            "type": "keyword",
            "index": "true"
        },
        "field5": {
            "type": "integer",
            "index": "true"
        },
        "field6": {
            "type": "vector",
            "dimension": 128
        },
        "field7": {
            "type": "vector",
            "dimension": 256,
            "store_type": "Mmap",
            "store_param": {
                "cache_size": 2000
            }
        }
    }
}
engine.create_table(table)
```

- name :  table' s name, when dump or load will use table's name.
- model : now only support IVFPQ
- nprobe : scan clustered buckets, default 10, it should be less than ncentroids
- metric_type : default `L2`, `InnerProduct` or `L2`
- ncentroids : coarse cluster center number, default 256
- nsubvector : the number of sub vector, default 32, only the value which is multiple of 4 is supported now
- nbits_per_idx : bit number of sub cluster center, default 8, and 8 is the only value now
- properties : define what field are in the table.
- type : There are four types (that is, the value of type) supported by the field defined by the table space structure: keyword, integer, float, vector (keyword is equivalent to string).
- index : supporting numeric field filter default `false`
- Vector field params
    - - dimension: feature dimension, should be integer
    - - retrieval_type: default "IVFPQ"
    - - model_id: shows feature vector' s type, like vgg16
    - - store_type : "RocksDB" or "Mmap" default "Mmap"
    - - store_param : example {"cache_size":2592}

Add
=======================

add item into vearch table:

```
item = {
    "field1": "value1",
    "field2": "value2",
    "field3": {
        "feature": [0.1, 0.2]
    }
    "field4": {
        "feature": [0.2, 0.3]
    }
}
doc_items = []
doc_items.append(item)
doc_ids = engine.add(item)
```

field1 and field2 are scalar field and field3 is feature field. All field names, value types, and table structures are consistent. As you can see, one item can have multiple feature vectors. And vearch will return a unique id for every added item. The unique identification needs to be used for data modification and deletion, or just get added item's detail info.

### Query

Vearch supports flexible search. 
Single query:

```python
query = {
    "vector": [{
        "field": "field_name",
        "feature": [0.1, 0.2, 0.3, 0.4, 0.5],
        "min_score": 0.9,
        "boost": 0.5
    }],
    "filter": [{
        "range": {
            "field_name": {
                "gte": 160,
                "lte": 180
            }
        }
    },

    {
         "term": {
             "field_name": ["100", "200", "300"],
             "operator": "or"
         }
    }],

    "direct_search_type": 0,
    "online_log_level": "debug",
    "topn": 10,
    "fields": ["field1", "field2"]
}
result = engine.search(query)
print(result)
```

Batch query:

```
query = {
        "vector": [{
            "field": "field_name",
            "feature": [[0.1, 0.2, 0.3, 0.4, 0.5],
                        [0.6, 0.7, 0.8, 0.9, 1.0]]
        }],
}
result = engine.search(query)
print(result)
```

Multi vector query:

```
query = {
        "vector": [
            "field1": {
                "type": "vector",
                "dimension": 128
            },
            "field2": {
                "type": "vector",
                "dimension": 256
    }],
}
result = engine.search(query)
print(result)
```

result will be their intersection.

Query only with filter:

```
query = {
    "filter": [{
            "range": {
                "field_name": {
                    "gte": 160,
                    "lte": 180
                }
            }
        },
        {
             "term": {
                 "field_name": ["100", "200", "300"],
                 "operator": "or"
             }
        }]
    },   
}
result = engine.search(query)
print(result)
```

- vector :  Support multiple (including multiple feature fields when defining table structure correspondingly).
- field : Specifies the name of the feature field when the table is created.
- feature : vector feature, dimension must be the same when defining table structure
- min_score: Specify the minimum score of the returned result, the similarity between the two vector 
    calculation results is between 0-1, min_score can specify the minimum score of the returned result, and max_score can specify the maximum score. For example, set “min_score”: 0.8, “max_score”: 0.95 to filter the result of 0.8 <= score <= 0.95.
- boost : Specify the weight of similarity. For example, if the similarity score of two vectors is 0.7 and boost is set to 0.5, the returned result will multiply the score 0.7 * 0.5, which is 0.35.
- filter : Multiple conditions are supported. Multiple conditions are intersecting. There are two kind of filters, range and term.
-  range : Specify to use the numeric field integer / float filtering, the file name is the numeric field name, gte and lte specify the range, lte is less than or equal to, gte is greater than or equal to, if equivalent filtering  is used, lte and gte settings are the same value. The above example shows that the query field_name field is greater than or equal to 160 but less than or equal to 180.
-  term : With label filtering, field_name is a defined label field, which allows multiple value filtering. You can intersect “operator”: “or”, merge: “operator”: “and”. The above example indicates that the query field name segment value is “100”, “200” or “300”.
- direct_search_type : Specify the query type. 0 means to use index if the feature has been created, and violent search if it has not been created; and 1 means not to use index only for violent search.The default value is 0.
- online_log_level : debug|info|warn|error|none . Set “debug” to specify to print more detailed logs on the server, which is convenient for troubleshooting in the development and test phase.
- topn : Specifies the maximum number of results to return.
- has_rank : whether it needs reranking after recalling from PQ index. default 0, has not rank; 1, has rank
- multi_vector_rank : whether it needs reranking after merging the searching result of multi-vectors. default 0, has not rank; 1, has rank
- fields : what field you want get from query result. If you don't set any field in profile fields(here means none feature vector  field), all profile fields will return; And when you want to get feature vectors then specify the feature vector field.

Update
===============================

Here you can update doc's info by its unique id, now don't support to update string and feature vector.

```
item = {
    "field1": "value1",
    "field2": "value2",
}
doc_id = "its unique id"
engine.update_doc(item, doc_id)
```

field1 and field2 are scalar field. All field names, value types, and table structures should be consistented.

Delete
=============================

1. you can delete a document by its unique id.

  ```
  doc_id = "its unique id"
  engine.del_doc(doc_id)
  ```

2. you can delete documents by query, and now only support range filter.

  ```
  del_query =  {
   "filter": [{
       "range": {
           "field2": {
               "gte": 1,
               "lte": 20
           }
       },
   }],
  }
  engine.del_doc_by_query(del_query)
  ```

  All documents that meet the conditions will be deleted


Data persistent
================================

dump data:

```
engine.dump()
```

Vearch support dump table into disk. When you dump vearch engine,  data in table will dump into disk. Data in table will store in the path you set for engine.

load data:

```
engine.load()
```

engine will auto to load file in the path you set for engine, so the path should be the same. When load, need't to create table and auto load data from dump files, so you just create engine and init log for it.  
