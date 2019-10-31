# Low-level API for vector search

### create database

````$xslt
curl -v --user "root:secret" -H "content-type: application/json" -XPUT -d'
{
    "name":"tpy"
}
' http://$IP:8817/db/_create
````
## create schema

````$xslt
curl -v --user "root:secret" -H "content-type: application/json" -XPUT -d'
  {
      "name": "tpy",
      "dynamic_schema": "strict",
      "partition_num": 2,
      "replica_num": 1,
      "engine": {"name":"gamma", "max_size":1000000,"nprobe":10,"metric_type":-1,"ncentroids":-1,"nsubvector":-1,"nbits_per_idx":-1},
      "properties": {
          "image_type": {
              "type": "keyword"
          },
          "fku": {
              "type": "integer",
              "index":"false"
          },
          "tags": {
              "type": "keyword",
              "array":true,
              "index":"true"
          },
          "image_vec": {
              "type": "vector",
              "model_id": "img",
              "dimension": 5000
          }
      }
  }
' http://$IP:8817/space/tpy/_create  
````

* dynamic_schema :[`true`, `false`, `strict`]
* partition_num : how many partition to slot,  default is `1`
* replica_num: how many replica has , recommend `3`
* engine
* max_size : max documents for each partition 
* nprobe : scan clustered buckets, default 10, it should be less than ncentroids
* metric_type : inner product or L2 
* ncentroids : coarse cluster center number, default 256
* nsubvector : the number of sub vector, default 32, only the value which is multiple of 4 is supported now
* nbits_per_idx : bit number of sub cluster center, default 8, and 8 is the only value now
* keyword
* array : whether the tags for each document is multi-valued, `true` or `false` default is false
* index : supporting numeric field filter default `false`


### insert data

````$xslt
curl -H "content-type: application/json" -XPOST -d'
{
    "area_code":"tpy",
    "product_code":"tpy",
    "image_type":"tpy",
    "image_name":"tpy",
    "image_vec": {
        "source":"test",
        "feature":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    }
}
' http://11.3.149.73/tpy/tpy/1

````

> url: [ip]:[port]/[dbName]/[tableName]/[documentID]

### Search

````$xslt
# search
curl -H "content-type: application/json" -XPOST -d'

{
  "query": {
      "and":[
        {
          "field": "feature2",
          "feature": [0,0,0,0,0],
          "symbol":">=",
          "value":0.9
        }
      ],
      "filter":[
          {
              "range":{
                  "product_code":{
                      "gte":1,
                      "lte":3
                  }
              }
          },
          {
              "term":{
                "tags":["t1","t2"],
                "operator":"and"
              }
          }
       ],
      "direct_search_type":0,
      "online_log_level":"debug" 
  },
  "size":10,
   "sort" : [
       { "_score" : {"order" : "asc"} }
   ]
}
' http://11.3.149.73/tpy/tpy/_search?size=10
````

````$xslt
# search
curl -H "content-type: application/json" -XPOST -d'

{
  "query": {
      "sum": [
        {
          "field": "feature1",
          "feature": [0,0,0,0,0],
          "boost":0.8
        }
      ],
      "filter":[
          {
              "range":{
                  "product_code":{
                      "gte":1,
                      "lte":3
                  }
              }
          },
          {
              "term":{
                "tags":["t1","t2"],
                "operator":"and"
              }
          }
       ],
      "direct_search_type":0,
      "online_log_level":"debug" 
  },
  "size":10,
   "sort" : [
       { "_score" : {"order" : "asc"} }
   ]
}
' http://11.3.149.73/tpy/tpy/_search?size=10
````

> url: [ip]:[port]/[dbName]/[tableName]/_search
* filter->term-> operator [`and`, `or`] default `or` 
* direct_search_type : default 0 ; -1: no direct search, 0: auto, 1: always direct
 


### get Document
 
````$xslt
curl -XGET http://11.3.149.73/tpy/tpy/1
````
> url: [ip]:[port]/[dbName]/[tableName]/[documentID]                                                 

### delete Document
 
````$xslt
curl -XDELETE http://11.3.149.73/tpy/tpy/1
````
> url: [ip]:[port]/[dbName]/[tableName]/[documentID]
