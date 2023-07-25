import numpy as np
import vearch
import sys
import time
import json
import os

#The following error occurred on the MAC platform:
#Initializing libiomp.dylib, but found libiomp.dylib already initialized OMP.
#The following code can be opened to solve the following problem.
#os.environ['KMP_DUPLICATE_LIB_OK']='True'



def test_create_engine():
    print("######    test create engine    ######")
    engine = vearch.Engine("files", 'logs')
    return engine

def test_create_table(engine):
    print("######     test create table    ######")
    table = {
        "name" : "test_table",
        "engine" : {
            "index_size": 10000,
            "retrieval_type": "IVFPQ",       
            "retrieval_param": {               
                "ncentroids": 256,          
                "nsubvector": 16
            }
            # this is for very large dataset and not suitable for random data
            #"retrieval_param": {
            #    "metric_type": "InnerProduct",
            #    "ncentroids": 1024,
            #    "nsubvector": 64,
            #    "hnsw" : {
            #        "nlinks": 32,
            #        "efConstruction": 200,
            #        "efSearch": 64
            #    },
            #    "opq": {
            #        "nsubvector": 64
            #    }
            #}
        },
        "properties" : {
            #"_id":{                        #You usually don't need to specify. Vearch is automatically specified.
            #    "type": "integer",         
            #    "is_index": True
            #},
            "feature":{
                "type": "vector",
                "index": True,
                "dimension": 64,
                "store_type": "MemoryOnly", 
                "store_param": {
                    "cache_size": 10000
                }
            },
            "key": {
                "type": "integer"
            },
            "url": {
                "type": "string"
            },
            "field1": {
                "type": "string",
                "index": True
            },
            "field2": {
                "type": "integer",
                "index": True
            },
            "field3": {
                "type": "integer",
                "index": True
            },
            "feature1": {
                "type": "vector",
                "index": True,
                "dimension": 64,
                "store_type": "memoryonly",
                "store_param": {
                    "cache_size": 10000
                }
            }
        }
    }
    response_code = engine.create_table(table)
    if response_code == 0:                    #response_code: 0, success; 1 failed.
        print("create table success")
    else:
        print("create table failed")


def test_add(engine, add_num=100000):
    print("######        test add          ######")    
    doc_items = []
    features = np.random.rand(add_num, 64).astype('float32')

    for i in range(add_num):
        profiles = {}
        profiles["key"] = i
        profiles["url"] = str(i) + ".jpg"
        profiles["field1"] = str(i%5)
        profiles["field2"] = i
        profiles["field3"] = i * 2

        #The feature type supports numpy only.
        profiles["feature"] = features[i,:]
        profiles["feature1"] = features[i,:]

        doc_items.append(profiles)
    
    docs_id = engine.add(doc_items)
    print("add complete, success num:", len(docs_id))
    time.sleep(5)

    #'min_indexed_num' = features.shape[0]. Indexing complete.
    indexed_num = 0
    while indexed_num != features.shape[0]:
        indexed_num = engine.get_status()['min_indexed_num']
        time.sleep(0.5)
    print("engine status:",engine.get_status())
    for i in range(2):
        print(doc_items[i])
        print("   ")
        print(engine.get_doc_by_id(docs_id[i]))
    return (doc_items, docs_id)



def test_search(engine):
    print("######        test search       ######")
    query_features = np.random.rand(64).astype('float32')
    # range filter should be integer
    # term filter should be string
    # if filter type is wrong, maybe you cannot
    # get any result
    query =  {
        "vector": [{
            "field": "feature",
            "feature": query_features                                    # data type is numpy
        }],
        "fields":["feature",'key'],
        "retrieval_param":{"metric_type": "InnerProduct", "nprobe":20},  # HNSW: {"efSearch": 64, "metric_type": "L2" }
        "topn":1
    }
    result = engine.search(query)
    print(result)
    return result

def test_violent_search(engine):
    print("######    test violent search     ######")
    query_features = np.random.rand(10, 64).astype('float32')
    # range filter should be integer
    # term filter should be string
    # if filter type is wrong, maybe you cannot
    # get any result
    query =  {
        "vector": [{
            "field": "feature",
            "feature": query_features,
        }],
        "direct_search_type": 1,
        "fields":["url", "key", "feature"],
        "topn":2
    }
    result = engine.search(query)
    print(result)



def test_search_return_fields(engine):
    print("###### test search return fields######")
    query_features = np.random.rand(1, 64).astype('float32')
    # range filter should be integer
    # term filter should be string
    # if filter type is wrong, maybe you cannot
    # get any result
    query =  {
        "vector": [{
            "field": "feature",
            "feature": query_features,
        }],
        "retrieval_param":{"metric_type": "InnerProduct", "nprobe":20},
        "topn": 2,
        "fields": ["key","url"]
    }

    result = engine.search(query)
    print(result)

def test_search_with_range(engine):
    print("######  test search with range ######")
    query_features = np.random.rand(1, 64).astype('float32')

    # range filter should be integer
    # term filter should be string
    # if filter type is wrong, maybe you cannot
    # get any result
    query =  {
        "filter": [{
            "range": {
                "field2": {     #When the table is built, the field "is index": True
                    "gte": 10,
                    "lte": 80
                }
            }
        }],
        "fields":["key","url"],  
        "topn": 5
    }

    result = engine.search(query)
    print(result)

def test_search_with_term(engine):
    print("######  test search with term  ######")
    query_features = np.random.rand(1, 64).astype('float32')

    # range filter should be integer
    # term filter should be string
    # if filter type is wrong, maybe you cannot
    # get any result
    query =  {
        "filter": [{        
            "term": {
                "field1": ["1", "2", "3"], #When the table is built, the field "is index": True
                "operator": "or"
            },
        }],
        "fields":["field1","url"],
        "topn": 5
    }
    result = engine.search(query)
    print(result)

def test_search_with_filter(engine):
    print("###### test search with filter ######")
    query_features = np.random.rand(1, 64).astype('float32')

    # range filter should be integer
    # term filter should be string
    # if filter type is wrong, maybe you cannot
    # get any result
    query =  {
        "filter": [{
            "range": {
                "field2": {
                    "gte": 10,
                    "lte": 17
                }
            }},        
            {"term": {
                "field1": ["1", "2"],
                "operator": "or"
            },
        }],
        "fields":["field1","field2"],
    }

    result = engine.search(query)
    print(result)

def test_batch_search(engine):
    print("######     test batch search    ######")
    query_features = np.random.rand(3, 64).astype('float32')

    # now feature is two feature vector
    query =  {
        "vector": [{
            "field": "feature",
            "feature": query_features[0:2,:],
        }],
        "filter": [{        
            "term": {
                "field1": ["1", "2", "3"],
                "operator": "not in"
            },
        }],
        "retrieval_param":{"nprobe":20, "metric_type": "L2"},
        "fields":["key", "url", 'field1'],
        'topn': 2
    }
    result = engine.search(query)
    print(result)

def test_multi_vector_search(engine):
    print("###### test multi vector search ######")
    query_features = np.random.rand(2, 64).astype('float32')

    # now vector have two feature field
    # it can be different feature field
    # result will be their intersection

    print("Different field's result:")
    query =  {
        "vector": [{
                "field": "feature",
                "feature": query_features[1,:],
            },
            {
                "field": "feature1",
                "feature": query_features[1,:],
        }],
        "fields":["key", "url", 'field1'],
        "topn":5
    }

    result = engine.search(query)
    print(result)

    print("Same field's result:")
    query1 =  {
        "vector": [{
            "field": "feature",
            "feature": query_features[0,:],
            },
            {
            "field": "feature",
            "feature": query_features[1,:],
        }],
        "fields":["key", "url", 'field1'],
    }

    result = engine.search(query1)
    print(result)


def test_update(engine, doc_items, id):
    print("######        test update       ######")
    print(engine.get_doc_by_id(id))
    update_item = doc_items[0]
    update_item["key"] = 2021
    #print(update_item)
    response_code = engine.update_doc(update_item, id)
    print(engine.get_doc_by_id(id))
    if response_code == 0:              #response_code: 0, success; 1 failed.
        print("update_doc success")
    else:
        print("update_doc failed")

def test_del_doc_by_id(engine, id):
    print("######  test delete doc by id   ######")
    print("engine status",engine.get_status())
    print(engine.get_doc_by_id(id))
    engine.del_doc(id)
    print(engine.get_doc_by_id(id))
    print("engine status", engine.get_status())

def test_del_doc_by_range(engine):
    print("###### test delete doc by range ######")
    #del_doc_by_query
    del_query =  {
        "filter": [{
            "range": {
                "field2": {
                    "gte": 1,
                    "lte": 10
                }
            },
        }],
    }

    print("engine status", engine.get_status())
    engine.del_doc_by_query(del_query)
    print("engine status", engine.get_status())

def test_del_doc_by_term(engine):
    #only support del doc by range filter, nothing will happen
    print("######  test delete doc by term ######")
    #del_doc_by_query
    del_query =  {
        "filter": [{
            "term": {
                "field1": ["1", "2"],
                "operator": "or"
            },
        }],
    }

    print("engine status", engine.get_status())
    #response_code: 0, success.  
    #response_code: 1, failed.
    print('response_code:', engine.del_doc_by_query(del_query))
    print("engine status", engine.get_status())

def test_dump(engine):                              
    #HNSW does not support dump and load. 
    print("######         test dump        ######")    
    response_code = engine.dump()
    return response_code

def test_load(doc_id):
    print("######         test load        ######")
    engine = test_create_engine()
    # when load, need't to create table
    # and auto load data from dump files   
    response_code = engine.load()
   
    print("engine status:", engine.get_status())

    test_search(engine)

    test_batch_search(engine)

    test_multi_vector_search(engine)
    
    print("get_doc_by_id", engine.get_doc_by_id(doc_id))

    time.sleep(5)

    engine.close()

    return response_code

def main():
    engine = test_create_engine()
    test_create_table(engine)
    
    if len(sys.argv) == 2:
        add_num = int(sys.argv[1])
        doc_items, docs_id = test_add(engine, add_num)
    else:
        doc_items, docs_id = test_add(engine)

    result = test_search(engine)
    
    test_violent_search(engine)

    test_search_return_fields(engine)

    test_search_with_range(engine)

    test_search_with_term(engine)

    test_search_with_filter(engine)

    test_batch_search(engine)

    test_multi_vector_search(engine)
    
    test_update(engine, doc_items, docs_id[0])

    test_del_doc_by_id(engine, docs_id[0])

    test_del_doc_by_range(engine)

    test_search(engine)

    test_dump(engine)

    engine.close()

    test_load(docs_id[0])

if __name__ == '__main__':
    main()
