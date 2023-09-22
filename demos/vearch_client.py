#
# Copyright 2019 The Vearch Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.

# -*- coding: UTF-8 -*-

import requests
import json
import random
import sys
import time


def get_cluster_stats(router_url: str):
    url = f'{router_url}/_cluster/stats'
    resp = requests.get(url)
    return resp.json()


def get_cluster_health(router_url: str):
    url = f'{router_url}/_cluster/health'
    resp = requests.get(url)
    return resp.json()


def get_servers_status(router_url: str):
    url = f'{router_url}/list/server'
    resp = requests.get(url)
    return resp.json()


def list_dbs(router_url: str):
    url = f'{router_url}/list/db'
    resp = requests.get(url)
    return resp.json()


def create_db(router_url: str, db_name: str):
    url = f'{router_url}/db/_create'
    data = {'name': db_name}
    resp = requests.put(url, json=data)
    return resp.json()


def get_db(router_url: str, db_name: str):
    url = f'{router_url}/db/{db_name}'
    resp = requests.get(url)
    return resp.json()


def drop_db(router_url: str, db_name: str):
    url = f'{router_url}/db/{db_name}'
    resp = requests.delete(url)
    return resp.text


def list_spaces(router_url: str, db_name: str):
    url = f'{router_url}/list/space?db={db_name}'
    resp = requests.get(url)
    return resp.json()


def create_space(router_url: str, db_name: str, space_config: dict):
    url = f'{router_url}/space/{db_name}/_create'
    resp = requests.put(url, json=space_config)
    return resp.json()


def get_space(router_url: str, db_name: str, space_name: str):
    url = f'{router_url}/space/{db_name}/{space_name}'
    resp = requests.get(url)
    return resp.json()


def drop_space(router_url: str, db_name: str, space_name: str):
    url = f'{router_url}/space/{db_name}/{space_name}'
    resp = requests.delete(url)
    return resp.text


def insert_one(router_url: str, db_name: str, space_name: str, data: dict, doc_id=None):
    if doc_id:
        url = f'{router_url}/{db_name}/{space_name}/{doc_id}'
    else:
        url = f'{router_url}/{db_name}/{space_name}'
    resp = requests.post(url, json=data)
    return resp.json()


def insert_batch(router_url: str, db_name: str, space_name: str, data_list: str):
    url = f'{router_url}/{db_name}/{space_name}/_bulk'
    resp = requests.post(url, data=data_list)
    return resp.text


def update(router_url: str, db_name: str, space_name: str, data: dict, doc_id):
    url = f'{router_url}/{db_name}/{space_name}/{doc_id}/_update'
    resp = requests.post(url, json=data)
    return resp.text


def delete(router_url: str, db_name: str, space_name: str, doc_id: str):
    url = f'{router_url}/{db_name}/{space_name}/{doc_id}'
    resp = requests.delete(url)
    return resp.text


def search(router_url: str, db_name: str, space_name: str, query: dict):
    url = f'{router_url}/{db_name}/{space_name}/_search'
    resp = requests.post(url, json=query)
    return resp.json()


def get_by_id(router_url: str, db_name: str, space_name: str, doc_id: str):
    url = f'{router_url}/{db_name}/{space_name}/{doc_id}'
    resp = requests.get(url)
    return resp.json()


def mget_by_ids(router_url: str, db_name: str, space_name: str, query: dict):
    url = f'{router_url}/{db_name}/{space_name}/_query_byids'
    resp = requests.post(url, json=query)
    return resp.json()


def bulk_search(router_url: str, db_name: str, space_name: str, queries: dict):
    url = f'{router_url}/{db_name}/{space_name}/_bulk_search'
    resp = requests.post(url, json=queries)
    return resp.json()


def msearch(router_url: str, db_name: str, space_name: str, query: dict):
    url = f'{router_url}/{db_name}/{space_name}/_msearch'
    resp = requests.post(url, json=query)
    return resp.json()


def search_by_id_feature(router_url: str, db_name: str, space_name: str, query: dict):
    url = f'{router_url}/{db_name}/{space_name}/_query_byids_feature'
    resp = requests.post(url, json=query)
    return resp.json()


def delete_by_query(router_url: str, db_name: str, space_name: str, query: dict):
    url = f'{router_url}/{db_name}/{space_name}/_delete_by_query'
    resp = requests.post(url, json=query)
    return resp.text


def operate_cluster(router_url: str):
    print("****** this show how to operate cluster ******")
    print(get_cluster_stats(router_url))
    print(get_cluster_health(router_url))
    print(get_servers_status(router_url))


def operate_db(router_url: str, db_name: str):
    print("\n****** this show how to operate db ******")
    print(list_dbs(router_url))
    print(create_db(router_url, db_name))
    print(list_dbs(router_url))
    print(get_db(router_url, db_name))
    print(drop_db(router_url, db_name))
    print(create_db(router_url, db_name))


def operate_space(router_url: str, db_name: str, space_name: str, space_config: dict):
    print("\n****** this show how to operate space ******")
    print(list_spaces(router_url, db_name))
    print(create_space(router_url, db_name, space_config))
    print(get_space(router_url, db_name, space_name))
    print(drop_space(router_url, db_name, space_name))
    print(get_space(router_url, db_name, space_name))
    print(create_space(router_url, db_name, space_config))


def gen_bulk_data(embedding_dimension: int) -> str:
    doc_id2 = "2"
    doc_id3 = "3"
    data_list = ""
    data_id = {"index": {"_id": doc_id2}}
    data_properties = {"field1": "2", "field2": 2, "field3": 2, "field4": 2, "field5": {
        "feature": [random.random() for i in range(embedding_dimension)]}}
    data_list += json.dumps(data_id) + "\n" + \
        json.dumps(data_properties) + "\n"

    data_id = {"index": {"_id": doc_id3}}
    data_properties = {"field1": "3", "field2": 3, "field3": 3, "field4": 3, "field5": {
        "feature": [random.random() for i in range(embedding_dimension)]}}
    data_list += json.dumps(data_id) + "\n" + \
        json.dumps(data_properties) + "\n"

    return data_list


def operate_document_add(router_url: str, db_name: str, space_name: str, embedding_dimension: int):
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


def operate_document_update(router_url: str, db_name: str, space_name: str, embedding_dimension: int, doc_id: str):
    # update
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


def operate_document_search(router_url: str, db_name: str, space_name: str, embedding_dimension: int):
    # search
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


def operate_document_delete(router_url: str, db_name: str, space_name: str, doc_id: str):
    # delete
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


def operate_document(router_url: str, db_name: str, space_name: str, embedding_dimension: int):
    operate_document_add(router_url, db_name, space_name, embedding_dimension)

    doc_id = "1"
    operate_document_update(router_url, db_name,
                            space_name, embedding_dimension, doc_id)

    operate_document_search(router_url, db_name,
                            space_name, embedding_dimension)

    operate_document_delete(router_url, db_name, space_name, doc_id)


def destroy(router_url: str, db_name: str, space_name: str):
    drop_space(router_url, db_name, space_name)
    drop_db(router_url, db_name)


def more_usage(router_url: str, db_name: str, space_name: str, space_config: dict, embedding_dimension):
    operate_cluster(router_url)

    operate_db(router_url, db_name)

    operate_space(router_url, db_name, space_name, space_config)

    operate_document(router_url, db_name, space_name, embedding_dimension)

    destroy(router_url, db_name, space_name)


def simple_usage(router_url: str, db_name: str, space_name: str, space_config: dict, embedding_dimension: int):
    print("step 1: create db")
    print(create_db(router_url, db_name))
    print("\nstep 2: create space")
    print(create_space(router_url, db_name, space_config))
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
    time.sleep(3)
    print("\nstep 4: search document")
    print(search(router_url, db_name, space_name, search_query))

    # Delete to the previous db and space after the demonstration
    destroy(router_url, db_name, space_name)


def usage(usage_type: str, router_url: str):
    timestamp = time.time()
    db_name = "test_python_client_db_" + str(timestamp)
    space_name = "test_python_client_space_" + str(timestamp)
    embedding_dimension = 128
    space_config = {
        "name": space_name,
        "partition_num": 1,
        "replica_num": 1,
        "engine": {
            "name": "gamma",
            "index_size": 1,
            "retrieval_type": "FLAT",
            "retrieval_param": {
                "metric_type": "L2",  # or "InnerProduct"
            }
        },
        "properties": {
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
            "field5": {
                "type": "vector",
                "index": True,
                "dimension": embedding_dimension,
                "store_type": "MemoryOnly"
            }
        }
    }

    if usage_type == "simple":
        simple_usage(router_url, db_name, space_name,
                     space_config, embedding_dimension)
    else:
        more_usage(router_url, db_name, space_name,
                   space_config, embedding_dimension)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python %s [simple|more] [router_url]" % (
            sys.argv[0]))
        desc_simple = '''Option of simple will show how to use vearch in four simple steps:
            1. create db
            2. create space
            3. add document
            4. search
            '''
        print("\t%s" % (desc_simple))
        desc_more = '''Option of more will show the four types of operations of vearch:
            1. operate cluster
            2. operate db
            3. operate space
            4. operate document
        '''
        print("\t%s" % (desc_more))
        exit(0)
    if sys.argv[1] != "simple" and sys.argv[1] != "more":
        print("Usage: python %s [simple|more] [router_url]" % (
            sys.argv[0]))
        exit(-1)
    usage_type = sys.argv[1]
    router_url = sys.argv[2]
    usage(usage_type, router_url)
