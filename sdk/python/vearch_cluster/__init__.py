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
import json

import requests


class VearchCluster:
    def __init__(self,router_url):
        self.router_url=router_url

    """
    operation about cluster
    """
    def get_cluster_stats(self):
        url = f'{self.router_url}/_cluster/stats'
        resp = requests.get(url)
        return resp.json()

    def get_cluster_health(self):
        url = f'{self.router_url}/_cluster/health'
        resp = requests.get(url)
        return resp.json() 

    def get_servers_status(self):
        url = f'{self.router_url}/list/server'
        resp = requests.get(url)
        return resp.json()

    """
     this show how to operate db
    """ 
    def create_db(self,db_name):
        url = f'{self.router_url}/db/_create'
        data = {'name': db_name}
        response=requests.put(url,json=data)
        res=response.json()
        if res["code"]==200:
            return 1
        else:
            return 0

    def drop_db(self,db_name):
        url = f'{self.router_url}/db/{db_name}'
        resp = requests.delete(url)
        return resp.text

    def list_dbs(self):
        url = f'{self.router_url}/list/db'
        db_list=[]
        try:
            resp = requests.get(url)
            res=resp.json()
            if res["code"]==200:
                for i in res["data"]:
                    db_list.append(i["name"])
        except Exception as e:
            raise ValueError("query db list failed!!!")
        return list(set(db_list))

    def get_db(self, db_name):
        url = f'{self.router_url}/db/{db_name}' 
        resp = requests.get(url)
        return resp.json()

    """
     this show how to operate space
    """ 
    def create_space(self,db_name, space_config):
        url = f'{self.router_url}/space/{db_name}/_create'
        resp = requests.put(url, json=space_config)
        res=json.loads(resp.content)
        if res["code"]==200:
            return 1
        else:
            return 0

    def drop_space(self,db_name, space_name):
        url = f'{self.router_url}/space/{db_name}/{space_name}'
        resp = requests.delete(url)
        return resp.text

    def list_spaces(self, db_name):
        url = f'{self.router_url}/list/space?db={db_name}'
        space_list=[]
        try:
            resp = requests.get(url)
            res=resp.json()
            if res["code"]==200:
                for i in res["data"]:
                    space_list.append(i["name"])
        except Exception as e:
            raise ValueError("query space list failed!!!")
        return list(set(space_list))
    def get_space(self, db_name, space_name):
        url = f'{self.router_url}/space/{db_name}/{space_name}'
        field_list=[]
        try:
            resp = requests.get(url)
            res=resp.json()
            if res["code"]==200:
                for k,v in res["data"]["properties"].items():
                    field_list.append(k)
        except Exception as e:
            raise ValueError("query field list failed!!!")

        return list(set(field_list))
    
    """
    add,delete,search,update
    """
    def insert_one(self, db_name, space_name, data, doc_id=None):
        if doc_id:
            url = f'{self.router_url}/{db_name}/{space_name}/{doc_id}'
        else:
            url = f'{self.router_url}/{db_name}/{space_name}'
        resp = requests.post(url, json=data)
        return resp.json()

    def insert_batch(self, db_name, space_name, data_list):
        url = f'{self.router_url}/{db_name}/{space_name}/_bulk'
        resp = requests.post(url, data=data_list)
        return resp.text

    def delete(self, db_name, space_name, doc_id):
        
        url = f'{self.router_url}/{db_name}/{space_name}/{doc_id}'
        try:
            resp = requests.delete(url) 
            res=json.loads(resp.content)
            if res[0]["status"]==200:
                return 0
            else:
                return 1
        except Exception as e:
            raise ValueError("delete doc by id failed!!!")

    def delete_by_query(self, db_name, space_name, query):
        url = f'{self.router_url}/{db_name}/{space_name}/_delete_by_query'
        resp = requests.post(url, json=query)

        return resp.text

    def search(self, db_name, space_name, query):
        url = f'{self.router_url}/{db_name}/{space_name}/_search'
        resp = requests.post(url, json=query)
        return resp.json()

    def msearch(self, db_name, space_name, query):
        url = f'{self.router_url}/{db_name}/{space_name}/_msearch'
        resp = requests.post(url, json=query)
        return resp.json()

    def search_by_id_feature(self, db_name, space_name, query):
        url = f'{self.router_url}/{db_name}/{space_name}/_query_byids_feature' 
        resp = requests.post(url, json=query)
        return resp.json()

    def bulk_search(self, db_name, space_name, queries):
        url = f'{self.router_url}/{db_name}/{space_name}/_bulk_search'  
        resp = requests.post(url, json=queries)
        return resp.json()
        
    def update_by_id(self, db_name, space_name, data, doc_id):
        url = f'{self.router_url}/{db_name}/{space_name}/{doc_id}/_update'
        resp = requests.post(url, json=data)
        return resp.text

    def update(self, db_name, space_name, data, doc_id):
        url = f'{self.router_url}/{db_name}/{space_name}/{doc_id}/_update'
        resp = requests.post(url, json=data)
        return resp.text

    def get_by_id(self, db_name, space_name, doc_id):
        url = f'{self.router_url}/{db_name}/{space_name}/{doc_id}'
        resp = requests.get(url)
        return resp.json()

    def mget_by_ids(self, db_name, space_name, query):
        url = f'{self.router_url}/{db_name}/{space_name}/_query_byids'
        resp = requests.post(url, json=query)
        return resp.json()

    
