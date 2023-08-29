# API for Vearch Python SDK

This document shows how to use vearch python sdk.

A simple usage
-------------------------

1. Create a vearch engine.
   
    ```python
    import vearch
    from vearch import GammaFieldInfo, GammaVectorInfo
    import numpy as np
    
    engine = vearch.Engine()
    ```
    
2. Create a table for engine.
   
    ```python
    engine_info = {
        "index_size": 10000,
        "retrieval_type": "IVFPQ",       
        "retrieval_param": {               
            "ncentroids": 256,          
            "nsubvector": 16
        }
    }
    
    fields = [GammaFieldInfo("field1", vearch.dataType.STRING, True),
              GammaFieldInfo("field2", vearch.dataType.INT, True),
              GammaFieldInfo("field3", vearch.dataType.FLOAT, True)]
    
    vector_field = GammaVectorInfo(name="feature", dimension=128)
    response_code = engine.create_table(engine_info, name="test_table", fields=fields, vector_field=vector_field)
    if response_code == 0:
        print("create table success")
    else:
        print("create table failed")
    ```
    
3. Add vector into table.
   
    ```python
    add_num = 10000
    features = np.random.rand(add_num, 128).astype('float32')
    doc_items = []
    for i in range(add_num):
        profiles = {}
        profiles["field1"] = str(i)
        profiles["field2"] = i
        profiles["field3"] = float(i)
        #The feature type supports numpy only.
        profiles["feature"] = features[i,:]
        doc_items.append(profiles)
    
    engine.add(doc_items)
    ```
    
4. Search vector nearest neighbors.
   
    ```python
    query =  {
        "vector": [{
            "field": "feature",
            "feature": features[0,:],
        }],
        "retrieval_param":{
            "metric_type": "InnerProduct", "nprobe":20
        }
    }
    result = engine.search(query)
    print(result)
    ```
    
5. See other detail info for deeper use.

Create and Close Engine
=========================

create vearch engine:

```
engine = vearch.Engine(path, log_path)
engine.close()
```

- path : engine config path to save dump file or something else engine will create
- log_path : engine log file will saved in the dir

Create table
========================

ceate vearch table:

```python
engine_info = {
    "index_size": 10000,
    "retrieval_type": "IVFPQ",       
    "retrieval_param": {               
        "ncentroids": 256,          
        "nsubvector": 16
    }
}

fields = [GammaFieldInfo("field1", vearch.dataType.STRING, True),
          GammaFieldInfo("field2", vearch.dataType.INT, True),
          GammaFieldInfo("field3", vearch.dataType.FLOAT, True)]

vector_field = GammaVectorInfo(name="feature", dimension=5, store_type="MemoryOnly", store_param={"cache_size": 10000})
response_code = engine.create_table(engine_info, name="test_table", fields=fields, vector_field=vector_field)
if response_code == 0:
    print("create table success")
else:
    print("create table failed")
```

- index_size :  training data size in IVFPQ model. You don't need to specify it in the HNSW model.
- retrieval_type : now support IVFPQ , HNSW , BINARYIVF , IVFFLAT and FLAT in python sdk.
- retrieval_param : 
    - nlinks : It is used in the HNSW model. Number of node neighbors, default 32; 
    
    - efConstruction: It is used in the HNSW model. The depth to be traversed in order to find the neighbors of nodes during building graph, default 40. 
    
    - ncentroids : It is used in the IVFPQ , BINARYIVF and IVFFLAT model. coarse cluster center number, default 2048.
    
    - nsubvector : It is used in the IVFPQ model. the number of sub vector, default 32, only the value which is multiple of 4 is supported now.
    
    - If you want to use the IVFHNSWPQ or IVFHNSWOPQ model, retrieval_param is as follows:
    
      ```shell
      "retrieval_type": "IVFPQ",
      "retrieval_param": {
          "metric_type": "InnerProduct",
          "ncentroids": 65536,
          "nsubvector": 64,
          "hnsw" : {
              "nlinks": 32,
              "efConstruction": 200,
              "efSearch": 64
          },
          "opq": {                #With "opq" it is IVFHNSWOPQ,  
              "nsubvector": 64    #Without "opq" it is IVFHNSWPQ
          }
      }
      ```
- GammaFieldInfo: Scalar field params

    - name: field name.
    - type : There are five types (that is, the value of type) supported by the
      field defined by the table space structure: string, int, long, float, double.
    - is_index : When it is True, numerical field filtering is supported , default `False`.

- GammaVectorInfo: Vector field params
    - dimension: feature dimension, should be integer
    - type: "VECTOR" represents vector field
    - store_type : "Mmap", "MemoryOnly" and "RocksDB", default "Mmap". HNSW only supports MemoryOnly. FLAT only supports MemoryOnly.  IVFFLAT only supports RocksDB. 
    - store_param : example {"cache_size":2592}

Add
=======================

add item into vearch table:

```python
item = {
    "field1": "value1",
    "field2": 1,
    "field3": 100.0,
    "feature": np.array([0.1, 0.2, 0.3, 0.4, 0.5])
}
doc_items = []
doc_items.append(item)
doc_ids = engine.add(item)
```

Field1 and field2 are scalar field. feature and feature1 is feature field. feature data type is only numpy. All field names, value types, and table structures are consistent. As you can see, one item can have multiple feature vectors. And vearch will return a unique id for every added item. You can also specify the ID field, as shown above. The unique identification needs to be used for data modification and deletion, or just get added item's detail info.

Get
=======================
get item info from vearch table:
'''
item_id = "item's id"
item_info = engine.get_doc_by_ID(item_id)
'''
use unique item id to get item's detail info.

### Query

Vearch supports flexible search. 
Single query:

```python
query = {
    "vector": [{
        "field": "field_name",
        "feature": np.array([0.1, 0.2, 0.3, 0.4, 0.5]),
        "min_score": 0.9,
        "boost": 1
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
    "retrieval_param":{"metric_type": "InnerProduct", "nprobe":20},
    "direct_search_type": 0,
    "online_log_level": "debug",
    "topn": 10,
    "fields": ["field1", "field2"]
}
result = engine.search(query)
print(result)
```

Batch query:

```python
query = {
        "vector": [{
            "field": "field_name",
            "feature": np.array([[0.1, 0.2, 0.3, 0.4, 0.5],
                        [0.6, 0.7, 0.8, 0.9, 1.0]])
        }],
}
result = engine.search(query)
print(result)
```

Query by ID:

```python
result = engine.get_doc_by_ID(id)
```

Query only with filter:

```python
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
- "retrieval_param":{"metric_type": "InnerProduct", "nprobe":20},

    - metric_type : default `L2`, `InnerProduct` or `L2`
    - nprobe :  It is used in the IVFPQ , BINARYIVF and IVFFLAT model. Scan clustered buckets, default 10, it should be less than ncentroids.
    - efSearch: It is used in the HNSW model.  defaul 64.
- direct_search_type : Specify the query type. 0 means to use index if the feature has been created, and violent search if it has not been created; and 1 means not to use index only for violent search. The default value is 0.
- online_log_level : debug|info|warn|error|none . Set “debug” to specify to print more detailed logs on the server, which is convenient for troubleshooting in the development and test phase.
- topn : Specifies the maximum number of results to return.
- has_rank : whether it needs reranking after recalling from PQ index. default 0, has not rank; 1, has rank
- multi_vector_rank : whether it needs reranking after merging the searching result of multi-vectors. default 0, has not rank; 1, has rank
- fields : what field you want get from query result. If you don't set any field in profile fields(here means none feature vector  field), all profile fields will return; And when you want to get feature vectors then specify the feature vector field.

Update
===============================

Here you can update doc's info by its unique id, now don't support to update string and feature vector.

```python
item = {
    "field1": "value1",
    "field2": "value2",
}
doc_id = "its unique id"
engine.update_doc(item, doc_id)
```

Field1 and field2 are scalar field. All field names, value types, and table structures should be consistented.

Delete
=============================

1. you can delete a document by its unique id.

  ```python
  doc_id = "its unique id"
  engine.del_doc(doc_id)
  ```

2. you can delete documents by query, and now only support range filter.

  ```python
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

Vearch status
================================

Get the status information:

```python
engine.get_status()
```

Data persistent
================================

It is supported in IVFPQ, FLAT, IVFFLAT and HNSW models.

dump data:

```python
engine.dump()
```

Vearch support dump table into disk. When you dump vearch engine,  data in table will dump into disk. Data in table will store in the path you set for engine.

load data:

```
engine.load()
```

Engine will auto to load file in the path you set for engine, so the path should be the same. When load, need't to create table and auto load data from dump files, so you just create engine and init log for it.
