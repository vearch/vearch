Space Operation
=================


Create Space
--------
::
   
  curl -XPUT -H "content-type: application/json" -d'
  {
      "name": "tpy",
      "dynamic_schema": "strict",
      "partition_num": 2,
      "replica_num": 1,
      "engine": {"name":"gamma", "max_size":1000000,"nprobe":10,"metric_type":"L2","ncentroids":-1,"nsubvector":-1,"nbits_per_idx":-1},
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

View Space
--------
::
  
  curl -XGET http://xxxxxx/space/$db_name/$space_name


Delete Space
--------
::
 
  curl -XDELETE http://xxxxxx/space/$db_name/$space_name
