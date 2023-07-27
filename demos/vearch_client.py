# -*- coding: UTF-8 -*-

import requests
import json
import random
import sys
import time

def get_cluster_stats(master_url):
    url = f'{master_url}/_cluster/stats'
    resp = requests.get(url)
    return resp.json()
 
def get_cluster_health(master_url):
    url = f'{master_url}/_cluster/health'
    resp = requests.get(url)
    return resp.json()

def get_servers_status(master_url):
    url = f'{master_url}/list/server'
    resp = requests.get(url)
    return resp.json()

def list_dbs(master_url):
    url = f'{master_url}/list/db'
    resp = requests.get(url)
    return resp.json()

def create_db(master_url, db_name):
    url = f'{master_url}/db/_create'
    data = {'name': db_name}
    resp = requests.put(url, json=data)
    return resp.json()

def get_db(master_url, db_name):
    url = f'{master_url}/db/{db_name}' 
    resp = requests.get(url)
    return resp.json()

def drop_db(master_url, db_name):
    url = f'{master_url}/db/{db_name}'
    resp = requests.delete(url)
    return resp.text

def list_spaces(master_url, db_name):
    url = f'{master_url}/list/space?db={db_name}'
    resp = requests.get(url)
    return resp.json()

def create_space(master_url, db_name, space_config):
    url = f'{master_url}/space/{db_name}/_create'
    resp = requests.put(url, json=space_config)
    return resp.json()
  
def get_space(master_url, db_name, space_name):
    url = f'{master_url}/space/{db_name}/{space_name}'
    resp = requests.get(url)
    return resp.json()

def drop_space(master_url, db_name, space_name):
    url = f'{master_url}/space/{db_name}/{space_name}'
    resp = requests.delete(url)
    return resp.text

def insert_one(router_url, db_name, space_name, data, doc_id=None):
    if doc_id:
        url = f'{router_url}/{db_name}/{space_name}/{doc_id}'
    else:
        url = f'{router_url}/{db_name}/{space_name}'
    resp = requests.post(url, json=data)
    return resp.json()

def insert_batch(router_url, db_name, space_name, data_list):
    url = f'{router_url}/{db_name}/{space_name}/_bulk'
    resp = requests.post(url, data=data_list)
    return resp.text
 
def update(router_url, db_name, space_name, data, doc_id):
    url = f'{router_url}/{db_name}/{space_name}/{doc_id}/_update'
    resp = requests.post(url, json=data)
    return resp.text
    
def delete(router_url, db_name, space_name, doc_id):
    url = f'{router_url}/{db_name}/{space_name}/{doc_id}'
    resp = requests.delete(url) 
    return resp.text
    
def search(router_url, db_name, space_name, query):
    url = f'{router_url}/{db_name}/{space_name}/_search'
    resp = requests.post(url, json=query)
    return resp.json()
    
def get_by_id(router_url, db_name, space_name, doc_id):
    url = f'{router_url}/{db_name}/{space_name}/{doc_id}'
    resp = requests.get(url)
    return resp.json()
    
def mget_by_ids(router_url, db_name, space_name, query):
    url = f'{router_url}/{db_name}/{space_name}/_query_byids'
    resp = requests.post(url, json=query)
    return resp.json()

def bulk_search(router_url, db_name, space_name, queries):
    url = f'{router_url}/{db_name}/{space_name}/_bulk_search'  
    resp = requests.post(url, json=queries)
    return resp.json()
 
def msearch(router_url, db_name, space_name, query):
    url = f'{router_url}/{db_name}/{space_name}/_msearch'
    resp = requests.post(url, json=query)
    return resp.json()
    
def search_by_id_feature(router_url, db_name, space_name, query):
    url = f'{router_url}/{db_name}/{space_name}/_query_byids_feature' 
    resp = requests.post(url, json=query)
    return resp.json()

def delete_by_query(router_url, db_name, space_name, query):
    url = f'{router_url}/{db_name}/{space_name}/_delete_by_query'
    resp = requests.post(url, json=query)
    return resp.text

def operate_cluster(master_url):
    print("****** this show how to operate cluster ******")
    print(get_cluster_stats(master_url))
    print(get_cluster_health(master_url))
    print(get_servers_status(master_url))    

def operate_db(master_url, db_name):
    print("\n****** this show how to operate db ******")
    print(list_dbs(master_url))
    print(create_db(master_url, db_name))
    print(list_dbs(master_url))
    print(get_db(master_url, db_name))
    print(drop_db(master_url, db_name))
    print(create_db(master_url, db_name))

def operate_space(master_url, db_name, space_name, space_config):
    print("\n****** this show how to operate space ******")
    print(list_spaces(master_url, db_name))
    print(create_space(master_url, db_name, space_config))
    print(get_space(master_url, db_name, space_name))
    print(drop_space(master_url, db_name, space_name))
    print(get_space(master_url, db_name, space_name))
    print(create_space(master_url, db_name, space_config))

def gen_bulk_data(embedding_dimension):
    doc_id2 = "2"
    doc_id3 = "3"
    data_list = ""
    data_id = {"index": {"_id": doc_id2}}
    data_properties = {"field1": "2", "field2": 2, "field3": 2, "field4": 2, "field5": {"feature": [random.random() for i in range(embedding_dimension)]}}
    data_list += json.dumps(data_id) + "\n" + json.dumps(data_properties) + "\n"

    data_id = {"index": {"_id": doc_id3}}
    data_properties = {"field1": "3", "field2": 3, "field3": 3, "field4": 3, "field5": {"feature": [random.random() for i in range(embedding_dimension)]}}
    data_list += json.dumps(data_id) + "\n" + json.dumps(data_properties) + "\n"

    return data_list

def operate_document_add(router_url, db_name, space_name, embedding_dimension):
    # add
    print("\n****** this show how to add ******")
    data = {
        "field1": "1",
        "field2": 1,
        "field3": 1,
        "field4": 1,
        "field5": {
            "feature": [random.random() for i in range(embedding_dimension)]
        }
    }
    doc_id1 = "1"
    print(insert_one(router_url, db_name, space_name, data, doc_id1))
    print(get_by_id(router_url, db_name, space_name, doc_id1))
    
    data_list = gen_bulk_data(embedding_dimension)
    print(insert_batch(router_url, db_name, space_name, data_list))

    get_query = {
        "query": {
            "ids": ["2", "3"]
        }
    }
    print(mget_by_ids(router_url, db_name, space_name, get_query)) 

def operate_document_update(router_url, db_name, space_name, embedding_dimension, doc_id):
    #update
    print("\n****** this show how to update ******")
    data = {
        "field1": "4",
        "field2": 4,
        "field3": 4,
        "field4": 4,
        "field5": {
            "feature": [random.random() for i in range(embedding_dimension)]
        }
    }
    print(get_by_id(router_url, db_name, space_name, doc_id))
    update(router_url, db_name, space_name, data, doc_id)
    print(get_by_id(router_url, db_name, space_name, doc_id))

def operate_document_search(router_url, db_name, space_name, embedding_dimension):
    #search
    print("\n****** this show how to search ******")
    search_query = {
        "query": {
            "sum": [{
                "field": "field5",
                "feature": [random.random() for i in range(embedding_dimension)]
            }],
            "filter": [{
                "range": {
                    "field4": {
                        "gte": 0.0,
                        "lte": 10.0
                    }
                }
            },
            {
                 "term": {
                     "field1": ["1", "2", "3", "4"],
                 }
            }]
        },
        "size": 3
    }
    print(search(router_url, db_name, space_name, search_query))
    bulk_queries = [{
        "query": {
            "sum": [{
                "field": "field5",
                "feature": [random.random() for i in range(embedding_dimension)]
            }]
        },
        "size": 3
    },
    {
        "query": {
            "sum": [{
                "field": "field5",
                "feature": [random.random() for i in range(embedding_dimension)]
            }]
        },
        "size": 1
    }]
    print(bulk_search(router_url, db_name, space_name, bulk_queries))
    msearch_size = 2
    msearch_query = {
        "query": {
            "sum": [{
                "field": "field5",
                "feature": [random.random() for i in range(embedding_dimension * msearch_size)]
            }],
            "filter": [{
                "range": {
                    "field3": {
                        "gte": 0,
                        "lte": 5
                    }
                }
            },
            {
                 "term": {
                     "field1": ["1", "2", "3", "4"],
                     "operator": "or"
                 }
            }]
        },
        "size": 3
    }
    print(msearch(router_url, db_name, space_name, msearch_query))

def operate_document_delete(router_url, db_name, space_name, embedding_dimension, doc_id):
    #delete
    print("\n****** this show how to delete ******")
    print(get_by_id(router_url, db_name, space_name, doc_id))
    delete(router_url, db_name, space_name, doc_id)
    print(get_by_id(router_url, db_name, space_name, doc_id))
    delete_query = {
        "query": {
            "filter": [{
                "range": {
                    "field3": {
                        "gte": 0,
                        "lte": 5
                    }
                }
            },
            {
                "term": {
                     "field1": ["1", "2", "3", "4"],
                     "operator": "or"
                }
            }]
        },
    }
    print(delete_by_query(router_url, db_name, space_name, delete_query))

def operate_document(router_url, db_name, space_name, embedding_dimension):
    operate_document_add(router_url, db_name, space_name, embedding_dimension)

    doc_id = "1"
    operate_document_update(router_url, db_name, space_name, embedding_dimension, doc_id)

    operate_document_search(router_url, db_name, space_name, embedding_dimension)
    
    operate_document_delete(router_url, db_name, space_name, embedding_dimension, doc_id)

def destroy(master_url, db_name, space_name):
    drop_space(master_url, db_name, space_name)
    drop_db(master_url, db_name)

def more_usage(master_url, router_url, db_name, space_name, space_config, embedding_dimension):
    operate_cluster(master_url)

    operate_db(master_url, db_name)

    operate_space(master_url, db_name, space_name, space_config)

    operate_document(router_url, db_name, space_name, embedding_dimension)

    destroy(master_url, db_name, space_name)

def simple_usage(master_url, router_url, db_name, space_name, space_config, embedding_dimension):
    print("step 1: create db")
    print(create_db(master_url, db_name))
    print("\nstep 2: create space")
    print(create_space(master_url, db_name, space_config))
    data = {
        "field1": "1",
        "field2": 1,
        "field3": 1,
        "field4": 1,
        "field5": {
            "feature": [random.random() for i in range(embedding_dimension)]
        }
    }
    doc_id1 = "1"
    print("\nstep 3: add document and get")
    print(insert_one(router_url, db_name, space_name, data, doc_id1))
    print(get_by_id(router_url, db_name, space_name, doc_id1))
    
    search_query = {
        "query": {
            "sum": [{
                "field": "field5",
                "feature": [random.random() for i in range(embedding_dimension)]
            }],
            "filter": [{
                "range": {
                    "field4": {
                        "gte": 0.0,
                        "lte": 10.0
                    }
                }
            },
            {
                 "term": {
                     "field1": ["1", "2", "3", "4"],
                 }
            }]
        },
        "size": 3
    }
    print("\nstep 4: search document")
    print(search(router_url, db_name, space_name, search_query))
    
    # Delete to the previous db and space after the demonstration
    destroy(master_url, db_name, space_name)
    
def usage(usage_type, master_url, router_url):
    timestamp = time.time()
    db_name = "test_python_client_db_" + str(timestamp)
    space_name = "test_python_client_space_" + str(timestamp)
    embedding_dimension = 128
    space_config = {
        "name" : space_name,
        "partition_num": 1,
        "replica_num": 1,
        "engine" : {
            "name": "gamma",
            "index_size": 1,
            "retrieval_type": "FLAT",       
            "retrieval_param": {               
                "metric_type": "L2", # or "InnerProduct"          
            }
        },
        "properties" : {
            "field1": {
                "type": "string",
                "index": True
            },
            "field2": {
                "type": "float",
                "index": True
            },
            "field3": {
                "type": "integer",
                "index": True
            },
            "field4": {
                "type": "double",
                "index": True
            },
            "field5":{
                "type": "vector",
                "index": True,
                "dimension": embedding_dimension,
                "store_type": "MemoryOnly"
            }
        }
    }

    if usage_type == "simple":
        simple_usage(master_url, router_url, db_name, space_name, space_config, embedding_dimension)
    else:
        more_usage(master_url, router_url, db_name, space_name, space_config, embedding_dimension)

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("Usage: python %s [simple|more] [master_url] [router_url]" % (sys.argv[0]))
        desc_simple = '''Option of simple will show how to use vearch in four simple steps:
            1. create db
            2. create space
            3. add document
            4. search
            '''
        print("\t%s" %(desc_simple))
        desc_more = '''Option of more will show the four types of operations of vearch:
            1. operate cluster
            2. operate db
            3. operate space
            4. operate document
        '''
        print("\t%s" %(desc_more))
        exit(0)
    if sys.argv[1] != "simple" and sys.argv[1] != "more":
        print("Usage: python %s [simple|more] [master_url] [router_url]" % (sys.argv[0]))
        exit(-1)
    usage_type = sys.argv[1]
    master_url = sys.argv[2]
    router_url = sys.argv[3]
    usage(usage_type, master_url, router_url)
