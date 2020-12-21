# Low-level API for vector search

> define MASTER = http://127.0.0.1:8817
> define ROUTER = http://127.0.0.1:9001
> `MASTER` is cluster manager , `ROUTER` is data manage

## database 

----

### create database

````$xslt
curl -v --user "root:secret" -H "content-type: application/json" -XPUT -d'
{
	"name":"test_vector_db"
}
' {{MASTER}}/db/_create
````

### get database

````$xslt
curl -XGET {{MASTER}}/db/test_vector_db
````

### list database

````$xslt
curl -XGET {{MASTER}}/list/db
````

### delete database

````$xslt
curl -XDELETE {{MASTER}}/db/test_vector_db
````

## space

----

### create space
Create Table for IVFPQ, and here are the matters needing attention:

Now ivfpq can be used in combination with hnsw and opq. If you want to use hnsw, it is recommended to set ncentroids to a larger value. At the same time, for the combination of hnsw, the limitation on the amount of training data is now released. Now you can use data not exceeding ncentroids * 256 for training. Need to pay attention to the memory used during training. Especially when opq is used in combination, the memory occupied by training is 2 * indexing_size * dimension * sizeof(float), so pay more attention to the setting of indexing_size. For the combined use of hnsw and opq, training will take up more memory and take a long time, which requires caution and attention.

index_size: For IVFPQ, it need train before building index, so you should set index_size a suitable value, such as 10000 or set larger for larger amounts of data. If used in combination with hnsw, index_size can be ncentroids * 39 - ncentroids * 256

How to use hnsw and opq in combination is controlled by retrieval_param. If you both set hnsw and opq, then you will use opq+ivf+hnsw+pq, an it is recommended to set the nsubvector of opq to be the same as the nsubvector of pq. If you just want to use ivf+hnsw+pq, then you just need to set hnsw. If you just want to use ivfpq, you don’t need to set hnsw or opq in retrieval_param. You can set hnsw or opq like this:
```
"index_size": 2600000,
"id_type": "string",
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
    "opq": {
        "nsubvector": 64
    }
}
````
````$xslt
curl -v --user "root:secret" -H "content-type: application/json" -XPUT -d'
{
	"name": "vector_space",
	"dynamic_schema": "strict",
	"partition_num": 1,
	"replica_num": 1,
	"engine": {
		"name": "gamma",
		"index_size": 100000,
		"id_type": "string",
		"retrieval_type": "IVFPQ",
		"retrieval_param": {
			"metric_type": "InnerProduct",
			"ncentroids": -1,
			"nsubvector": -1
		}
	},
	"properties": {
		"string": {
			"type": "keyword",
			"index": true
		},
		"int": {
			"type": "integer",
			"index": true
		},
		"float": {
			"type": "float",
			"index": true
		},
		"vector": {
			"type": "vector",
			"model_id": "img",
			"dimension": 128,
			"format": "normalization"
		},
		"string_tags": {
			"type": "string",
			"array": true,
			"index": true
		},
		"int_tags": {
			"type": "integer",
			"array": true,
			"index": true
		},
		"float_tags": {
			"type": "float",
			"array": true,
			"index": true
		}
	},
	"models": [{
		"model_id": "vgg16",
		"fields": ["string"],
		"out": "feature"
	}]
}
' {{MASTER}}/space/test_vector_db/_create
````

Create table for HNSW, and here are the matters needing attention:

index_size: For HNSW, it doesn't need to train before building index, so greater than 0 is enough.
store_type: should set it as `MemoryOnly`. For HNSW, it can only be run in MemoryOnly mode.

````$xslt
curl -v --user "root:secret" -H "content-type: application/json" -XPUT -d'
{
    "name": "vector_space",
    "dynamic_schema": "strict",
    "partition_num": 1,
    "replica_num": 1,
    "engine": {
        "name": "gamma",
        "index_size": 1,
        "retrieval_type": "HNSW",
        "retrieval_param": {
            "metric_type": "L2",
            "nlinks": -1,
            "efConstruction": -1
        }
    },
    "properties": {
        "string": {
            "type": "keyword",
            "index": true
        },
        "int": {
            "type": "integer",
            "index": true
        },
        "float": {
            "type": "float",
            "index": true
        },
        "vector": {
            "type": "vector",
            "model_id": "img",
            "dimension": 128,
            "store_type": "MemoryOnly",
            "format": "normalization"
        },
        "string_tags": {
            "type": "string",
            "array": true,
            "index": true
        },
        "int_tags": {
            "type": "integer",
            "array": true,
            "index": true
        },
        "float_tags": {
            "type": "float",
            "array": true,
            "index": true
        }
    },
    "models": [{
        "model_id": "vgg16",
        "fields": ["string"],
        "out": "feature"
    }]
}
' {{MASTER}}/space/test_vector_db/_create
````

Create table for IVFFLAT, and here are the matters needing attention:

index_size: For IVFFLAT, it need train before building index, so you should set index_size a suitable value, such as 10000 or set larger for larger amounts of data.
store_type: should set it as `RocksDB`. For IVFFLAT, it can only be run in RocksDB mode.

````$xslt
curl -v --user "root:secret" -H "content-type: application/json" -XPUT -d'
{
    "name": "vector_space",
    "dynamic_schema": "strict",
    "partition_num": 1,
    "replica_num": 1,
    "engine": {
        "name": "gamma",
        "index_size": 100000,
        "retrieval_type": "IVFFLAT",
        "retrieval_param": {
            "metric_type": "InnerProduct",
            "ncentroids": -1
        }
    },
    "properties": {
        "string": {
            "type": "keyword",
            "index": true
        },
        "int": {
            "type": "integer",
            "index": true
        },
        "float": {
            "type": "float",
            "index": true
        },
        "vector": {
            "type": "vector",
            "model_id": "img",
            "dimension": 128,
            "store_type": "RocksDB",
            "format": "normalization"
        },
        "string_tags": {
            "type": "string",
            "array": true,
            "index": true
        },
        "int_tags": {
            "type": "integer",
            "array": true,
            "index": true
        },
        "float_tags": {
            "type": "float",
            "array": true,
            "index": true
        }
    },
    "models": [{
        "model_id": "vgg16",
        "fields": ["string"],
        "out": "feature"
    }]
}
' {{MASTER}}/space/test_vector_db/_create
````

Create table for Multiple models, and here are the matters needing attention:

The parameters of each model may be different, please refer to the precautions on model parameters above.
Multi-model table creation and query statements are different.Multi-model use store_type must be supported by all models.

Old usage for create table:
````$xslt
{
    "name": "vector_space",
    "dynamic_schema": "strict",
    "partition_num": 1,
    "replica_num": 1,
    "engine": {
        "name": "gamma",
        "index_size": 100000, 
        "retrieval_type": "IVFPQ", 
        "retrieval_param": {
            "metric_type": "InnerProduct",
            "ncentroids": 256, 
            "nsubvector": 32
        }
    }, 
    "properties": {
            "string": {
                "type": "keyword",
                "index": true
            },
            "int": {
                "type": "integer",
                "index": true
            },
            "float": {
                "type": "float",
                "index": true
            },
            "vector": {
                "type": "vector",
                "model_id": "img",
                "dimension": 128,
                "store_type": "RocksDB",
                "format": "normalization"
            },
            "string_tags": {
                "type": "string",
                "array": true,
                "index": true
            },
            "int_tags": {
                "type": "integer",
                "array": true,
                "index": true
            },
            "float_tags": {
                "type": "float",
                "array": true,
                "index": true
            }
        },
        "models": [{
            "model_id": "vgg16",
            "fields": ["string"],
            "out": "feature"
        }]
}
````
New usage for create table about Multiple models:
````$xslt
{
    "name": "vector_space",
    "dynamic_schema": "strict",
    "partition_num": 1,
    "replica_num": 1,
    "engine": {
        "name": "gamma", 
        "retrieval_types": [
            "IVFPQ", 
            "HNSW"
        ], 
        "retrieval_params": [
            {
                "metric_type": "InnerProduct",
                "ncentroids": 256, 
                "nsubvector": 32
            }, 
            {
                "nlinks": 32, 
                "efconstructor": 40, 
                "efsearch": 50
            }
        ]
    }, 
    "properties": {
            "string": {
                "type": "keyword",
                "index": true
            },
            "int": {
                "type": "integer",
                "index": true
            },
            "float": {
                "type": "float",
                "index": true
            },
            "vector": {
                "type": "vector",
                "model_id": "img",
                "dimension": 128,
                "store_type": "MemoryOnly",
                "format": "normalization"
            },
            "string_tags": {
                "type": "string",
                "array": true,
                "index": true
            },
            "int_tags": {
                "type": "integer",
                "array": true,
                "index": true
            },
            "float_tags": {
                "type": "float",
                "array": true,
                "index": true
            }
        },
        "models": [{
            "model_id": "vgg16",
            "fields": ["string"],
            "out": "feature"
        }]
}
````

Old usage for _search query about Multiple models :

````$xslt
curl -H "content-type: application/json" -XPOST -d'
{
  "query": {
      "sum":[
        {
          "field": "vector",
          "feature": [0.88658684,0.9873159,0.68632215,-0.114685304,-0.45059848,0.5360963,0.9243208,0.14288005,0.9383601,0.17486687,0.3889527,0.91680753,0.6597193,0.52906346,0.5491872,-0.24706548,0.28541148,0.87731135,-0.18872026,0.28016,0.14826365,0.7217548,0.66360927,0.839685,0.29014188,-0.7303055,0.31786093,0.7611028,0.38408384,0.004707908,0.27696127,0.6069607,0.52147454,0.34435293,0.5665409,0.9676775,0.9415799,-0.95000356,-0.7441306,0.32473814,0.24417956,0.4114195,-0.15658693,0.9567978,0.91448873,0.8040493,0.7370252,0.41042542,-0.12714817,0.7344759,0.95486677,0.6752892,0.79088193,0.27843192,0.7594493,0.96637094,0.21354128,0.14667709,0.52713686,0.39803344,0.13063455,-0.26041254,0.21177465,0.0889158,0.7040157,0.9184541,0.33231667,0.109015055,0.7252709,0.85923946,0.6874303,0.9188243,0.44670975,0.6534332,0.67833525,0.40294313,0.76628596,0.722926,0.2507119,0.86939317,0.1049489,0.5707651,0.89342695,0.89022624,0.06606513,0.46363428,0.8836891,0.8416466,0.43164334,-0.059498303,0.25076458,0.91614866,0.21405962,0.07442343,0.8398273,-0.518248,0.4477598,0.54731685,0.39200985,0.2999862,0.22204888,0.9051194,0.7241311,0.9049213,0.48899868,0.11941989,0.45151904,0.9315986,0.17897557,0.759705,0.2549287,0.96008617,0.25688004,0.5925487,0.3069243,0.9171891,0.46981755,0.14557107,0.8900092,0.84537476,0.5608369,0.6909559,0.777092,0.66562796,0.6040272,0.77930593,0.59144366,0.12506102],
          "boost":0.8
        }
      ],
      "filter":[
          {
              "range":{
                  "int":{
                      "gte":1,
                      "lte":1000
                  }
              }
          },
          {
              "term":{
                "string_tags":["28","2","29"],
                "operator":"or"
              }
          }
       ]
  },
  "is_brute_search":0
  "size":10,
}
' {{ROUTER}}/test_vector_db/vector_space/_search
````
New usage for _search query about Multiple models:
add retrieval_type paramter,retrieval_type specifies the model to be queried, and the retrieval_param parameter is set according to the specified model. If retrieval_type is not set, the data will be queried according to the first model when the table is created.

```$xslt
curl -H "content-type: application/json" -XPOST -d'
{
  "query": {
      "sum":[
        {
          "field": "vector",
          "retrieval_type": "IVFPQ", 
          "feature": [0.88658684,0.9873159,0.68632215,-0.114685304,-0.45059848,0.5360963,0.9243208,0.14288005,0.9383601,0.17486687,0.3889527,0.91680753,0.6597193,0.52906346,0.5491872,-0.24706548,0.28541148,0.87731135,-0.18872026,0.28016,0.14826365,0.7217548,0.66360927,0.839685,0.29014188,-0.7303055,0.31786093,0.7611028,0.38408384,0.004707908,0.27696127,0.6069607,0.52147454,0.34435293,0.5665409,0.9676775,0.9415799,-0.95000356,-0.7441306,0.32473814,0.24417956,0.4114195,-0.15658693,0.9567978,0.91448873,0.8040493,0.7370252,0.41042542,-0.12714817,0.7344759,0.95486677,0.6752892,0.79088193,0.27843192,0.7594493,0.96637094,0.21354128,0.14667709,0.52713686,0.39803344,0.13063455,-0.26041254,0.21177465,0.0889158,0.7040157,0.9184541,0.33231667,0.109015055,0.7252709,0.85923946,0.6874303,0.9188243,0.44670975,0.6534332,0.67833525,0.40294313,0.76628596,0.722926,0.2507119,0.86939317,0.1049489,0.5707651,0.89342695,0.89022624,0.06606513,0.46363428,0.8836891,0.8416466,0.43164334,-0.059498303,0.25076458,0.91614866,0.21405962,0.07442343,0.8398273,-0.518248,0.4477598,0.54731685,0.39200985,0.2999862,0.22204888,0.9051194,0.7241311,0.9049213,0.48899868,0.11941989,0.45151904,0.9315986,0.17897557,0.759705,0.2549287,0.96008617,0.25688004,0.5925487,0.3069243,0.9171891,0.46981755,0.14557107,0.8900092,0.84537476,0.5608369,0.6909559,0.777092,0.66562796,0.6040272,0.77930593,0.59144366,0.12506102],
          "boost":0.8
        }
      ],
      "filter":[
          {
              "range":{
                  "int":{
                      "gte":1,
                      "lte":1000
                  }
              }
          },
          {
              "term":{
                "string_tags":["28","2","29"],
                "operator":"or"
              }
          }
       ]
  },
  "is_brute_search":0
  "size":10,
}
' {{ROUTER}}/test_vector_db/vector_space/_search
````


* partition_num : how many partition to slot,  default is `1`

* replica_num: how many replica has , recommend `3`

* engine

* id_type : the type of Primary key, default String, you can set it as `long` or `string`

* index_size : default 2, if insert document num >= index_size, it will start to build index automatically. For different retrieval model, it have different index size.  For HNSW and FLAT, it doesn't need to train before building index, greater than 0 is enough. For IVFPQ, GPU and BINARYIVF, it need train before building index, so you should set index_size larger, such as 100000.

* retrieval_type: the type of retrieval model, now support five kind retrieval model: IVFPQ GPU BINARYIVF HNSW FLAT. BINARYIVF is to index binary data. The Other type of retrieval models are for float32 data. And GPU is the implementation of IVFPQ on GPU, so IVFPQ and GPU have the same retrieval_param. FLAT is brute-force search. HNSW and FLAT can only work in `MemoryOnly` mode. And HNSW now uses mark deletion, and does not make corresponding changes to the hnsw graph structure after deletion or update.

* retrieval_param: parameter of retrieval model, this corresponds to the retrieval type.
For metric_type, It can be specified when building the table, if it is not set when searching, then use the parameters specified when building the table.
* IVFPQ

    * * metric_type : `InnerProduct` or `L2`.
    * * nprobe : scan clustered buckets, default 80, it should be less than ncentroids. Now it should set at search time. 
    * * ncentroids : coarse cluster center number, default 2048
    * * nsubvector : the number of sub vector, default 64, only the value which is multiple of 4 is supported now 
    * * nbits_per_idx : bit number of sub cluster center, default 8, and 8 is the only value now
    * * bucket_init_size : the original size of RTInvertIndex bucket, default 1000. You can set its value to the amount of data you just want to insert divided by ncentroids.
    * * bucket_max_size : the max size of RTInvertIndex bucket. default 1280000, if your dataset is very large, you can set it larger.

* GPU

    * * metric_type :  `InnerProduct` or `L2`.
    * * nprobe : scan clustered buckets, default 80, it should be less than ncentroids. Now it should set at search time.   
    * * ncentroids : coarse cluster center number, default 2048 
    * * nsubvector : the number of sub vector, default 64
    * * nbits_per_idx : bit number of sub cluster center, default 8, and 8 is the only value now

* IVFFLAT

    * * metric_type :  `InnerProduct` or `L2`.
    * * nprobe : scan clustered buckets, default 80, it should be less than ncentroids. Now it should set at search time. 
    * * ncentroids : coarse cluster center number, default 2048
    
* BINARYIVF

    * * nprobe : scan clustered buckets, default 20, it should be less than ncentroids. Now it should set at search time. 
    * * ncentroids : coarse cluster center number, default 256

* HNSW

    * * metric_type : `InnerProduct` or `L2` 
    * * nlinks ： neighbors number of each node, default 32
    * * efConstruction : expansion factor at construction time, default 40. The higher the value, the better the construction effect, and the longer it takes 
    * * efSearch : expansion factor at search time, default 64. The higher the value, the more accurate the search results and the longer it takes. Now it should set at search time.

* FLAT

    * * metric_type : `InnerProduct` or `L2`.

* keyword
* array : whether the tags for each document is multi-valued, `true` or `false` default is false
* index : supporting numeric field filter default `false`
* Vector field params
    * * format : default not normalized . if you set "normalization", "normal" it will normalized  
    * * store_type : "RocksDB" or "Mmap" or "MemoryOnly" default "Mmap".For HNSW and IVFFLAT and FLAT, it can only be run in MemoryOnly mode.   
    * * store_param : example {"cache_size":2592}. default value is 1024. It means you will use so much memory, the excess will be kept to disk. For MemoryOnly, this parameter is invalid.

### get space

````$xslt
curl -XGET {{MASTER}}/space/test_vector_db/vector_space
````

### list space

````$xslt
curl -XGET {{MASTER}}/list/space?db=test_vector_db
````

### delete space

````$xslt
curl -XDELETE {{MASTER}}/space/test_vector_db/vector_space
````

### change member

````$xslt
curl -v --user "root:secret" -H "content-type: application/json" -XPOST -d'
{
	"partition_id":1,
	"node_id":1,
	"method":0
}
' {{MASTER}}/partition/change_member
````

> this api is add or del partition in ds ,
> method : method=0 add partition:1 to node:1, method=1 delete partition:1 from node:1


## document 

----

### insert document

````$xslt
curl -H "content-type: application/json" -XPOST -d'
{"string":"14AW1mK_j19FyJvn5NR4Ep","int":14,"float":3.7416573867739413,"vector":{"feature":[0.88658684,0.9873159,0.68632215,-0.114685304,-0.45059848,0.5360963,0.9243208,0.14288005,0.9383601,0.17486687,0.3889527,0.91680753,0.6597193,0.52906346,0.5491872,-0.24706548,0.28541148,0.87731135,-0.18872026,0.28016,0.14826365,0.7217548,0.66360927,0.839685,0.29014188,-0.7303055,0.31786093,0.7611028,0.38408384,0.004707908,0.27696127,0.6069607,0.52147454,0.34435293,0.5665409,0.9676775,0.9415799,-0.95000356,-0.7441306,0.32473814,0.24417956,0.4114195,-0.15658693,0.9567978,0.91448873,0.8040493,0.7370252,0.41042542,-0.12714817,0.7344759,0.95486677,0.6752892,0.79088193,0.27843192,0.7594493,0.96637094,0.21354128,0.14667709,0.52713686,0.39803344,0.13063455,-0.26041254,0.21177465,0.0889158,0.7040157,0.9184541,0.33231667,0.109015055,0.7252709,0.85923946,0.6874303,0.9188243,0.44670975,0.6534332,0.67833525,0.40294313,0.76628596,0.722926,0.2507119,0.86939317,0.1049489,0.5707651,0.89342695,0.89022624,0.06606513,0.46363428,0.8836891,0.8416466,0.43164334,-0.059498303,0.25076458,0.91614866,0.21405962,0.07442343,0.8398273,-0.518248,0.4477598,0.54731685,0.39200985,0.2999862,0.22204888,0.9051194,0.7241311,0.9049213,0.48899868,0.11941989,0.45151904,0.9315986,0.17897557,0.759705,0.2549287,0.96008617,0.25688004,0.5925487,0.3069243,0.9171891,0.46981755,0.14557107,0.8900092,0.84537476,0.5608369,0.6909559,0.777092,0.66562796,0.6040272,0.77930593,0.59144366,0.12506102],"source":"14AW1mK_j19FyJvn5NR4Ep"},"string_tags":["14","10","15"],"int_tags":[14,10,15],"float_tags":[0.88658684,0.9873159,0.68632215,-0.114685304,-0.45059848]}
' {{ROUTER}}/test_vector_db/vector_space/1

````
> url: [ip]:[port]/[dbName]/[tableName]/[documentID]


### insert document without id
````$xslt
curl -H "content-type: application/json" -XPOST -d'
{"string":"14AW1mK_j19FyJvn5NR4Ep","int":14,"float":3.7416573867739413,"vector":{"feature":[0.88658684,0.9873159,0.68632215,-0.114685304,-0.45059848,0.5360963,0.9243208,0.14288005,0.9383601,0.17486687,0.3889527,0.91680753,0.6597193,0.52906346,0.5491872,-0.24706548,0.28541148,0.87731135,-0.18872026,0.28016,0.14826365,0.7217548,0.66360927,0.839685,0.29014188,-0.7303055,0.31786093,0.7611028,0.38408384,0.004707908,0.27696127,0.6069607,0.52147454,0.34435293,0.5665409,0.9676775,0.9415799,-0.95000356,-0.7441306,0.32473814,0.24417956,0.4114195,-0.15658693,0.9567978,0.91448873,0.8040493,0.7370252,0.41042542,-0.12714817,0.7344759,0.95486677,0.6752892,0.79088193,0.27843192,0.7594493,0.96637094,0.21354128,0.14667709,0.52713686,0.39803344,0.13063455,-0.26041254,0.21177465,0.0889158,0.7040157,0.9184541,0.33231667,0.109015055,0.7252709,0.85923946,0.6874303,0.9188243,0.44670975,0.6534332,0.67833525,0.40294313,0.76628596,0.722926,0.2507119,0.86939317,0.1049489,0.5707651,0.89342695,0.89022624,0.06606513,0.46363428,0.8836891,0.8416466,0.43164334,-0.059498303,0.25076458,0.91614866,0.21405962,0.07442343,0.8398273,-0.518248,0.4477598,0.54731685,0.39200985,0.2999862,0.22204888,0.9051194,0.7241311,0.9049213,0.48899868,0.11941989,0.45151904,0.9315986,0.17897557,0.759705,0.2549287,0.96008617,0.25688004,0.5925487,0.3069243,0.9171891,0.46981755,0.14557107,0.8900092,0.84537476,0.5608369,0.6909559,0.777092,0.66562796,0.6040272,0.77930593,0.59144366,0.12506102],"source":"14AW1mK_j19FyJvn5NR4Ep"},"string_tags":["14","10","15"],"int_tags":[14,10,15],"float_tags":[0.88658684,0.9873159,0.68632215,-0.114685304,-0.45059848]}
' {{ROUTER}}/test_vector_db/vector_space/
````

### search document by id 
````$xslt
curl -XGET {{ROUTER}}/test_vector_db/vector_space/id
````

### bulk search document by ids
````$xslt
curl -H "content-type: application/json" -XPOST -d'
{
  "query": {
	"ids": ["3", "1"],
	"fields": ["int"]
  }
}
' {{ROUTER}}/test_vector_db/vector_space/_query_byids
````
* `fields` : search results return field


### search document by query and vector score limit
````$xslt
curl -H "content-type: application/json" -XPOST -d'
{
  "query": {
      "and":[
        {
          "field": "vector",
          "feature": [0.88658684,0.9873159,0.68632215,-0.114685304,-0.45059848,0.5360963,0.9243208,0.14288005,0.9383601,0.17486687,0.3889527,0.91680753,0.6597193,0.52906346,0.5491872,-0.24706548,0.28541148,0.87731135,-0.18872026,0.28016,0.14826365,0.7217548,0.66360927,0.839685,0.29014188,-0.7303055,0.31786093,0.7611028,0.38408384,0.004707908,0.27696127,0.6069607,0.52147454,0.34435293,0.5665409,0.9676775,0.9415799,-0.95000356,-0.7441306,0.32473814,0.24417956,0.4114195,-0.15658693,0.9567978,0.91448873,0.8040493,0.7370252,0.41042542,-0.12714817,0.7344759,0.95486677,0.6752892,0.79088193,0.27843192,0.7594493,0.96637094,0.21354128,0.14667709,0.52713686,0.39803344,0.13063455,-0.26041254,0.21177465,0.0889158,0.7040157,0.9184541,0.33231667,0.109015055,0.7252709,0.85923946,0.6874303,0.9188243,0.44670975,0.6534332,0.67833525,0.40294313,0.76628596,0.722926,0.2507119,0.86939317,0.1049489,0.5707651,0.89342695,0.89022624,0.06606513,0.46363428,0.8836891,0.8416466,0.43164334,-0.059498303,0.25076458,0.91614866,0.21405962,0.07442343,0.8398273,-0.518248,0.4477598,0.54731685,0.39200985,0.2999862,0.22204888,0.9051194,0.7241311,0.9049213,0.48899868,0.11941989,0.45151904,0.9315986,0.17897557,0.759705,0.2549287,0.96008617,0.25688004,0.5925487,0.3069243,0.9171891,0.46981755,0.14557107,0.8900092,0.84537476,0.5608369,0.6909559,0.777092,0.66562796,0.6040272,0.77930593,0.59144366,0.12506102],
          "symbol":">=",
          "value":0.9
        }
      ],
      "filter":[
          {
              "range":{
                  "int":{
                      "gte":1,
                      "lte":1000
                  }
              }
          },
          {
              "term":{
                "string_tags":["28","2","29"],
                "operator":"or"
              }
          }
       ]
  },
  "size":10,
   "quick":false, 
   "vector_value":false,
    "sort" : [
       { "_score" : {"order" : "asc"} }
   ],
   "fileds":["name","age"]
}
' {{ROUTER}}/test_vector_db/vector_space/_search
````

### search document by query and vector score boost
````$xslt
curl -H "content-type: application/json" -XPOST -d'
{
  "query": {
      "sum":[
        {
          "field": "vector",
          "feature": [0.88658684,0.9873159,0.68632215,-0.114685304,-0.45059848,0.5360963,0.9243208,0.14288005,0.9383601,0.17486687,0.3889527,0.91680753,0.6597193,0.52906346,0.5491872,-0.24706548,0.28541148,0.87731135,-0.18872026,0.28016,0.14826365,0.7217548,0.66360927,0.839685,0.29014188,-0.7303055,0.31786093,0.7611028,0.38408384,0.004707908,0.27696127,0.6069607,0.52147454,0.34435293,0.5665409,0.9676775,0.9415799,-0.95000356,-0.7441306,0.32473814,0.24417956,0.4114195,-0.15658693,0.9567978,0.91448873,0.8040493,0.7370252,0.41042542,-0.12714817,0.7344759,0.95486677,0.6752892,0.79088193,0.27843192,0.7594493,0.96637094,0.21354128,0.14667709,0.52713686,0.39803344,0.13063455,-0.26041254,0.21177465,0.0889158,0.7040157,0.9184541,0.33231667,0.109015055,0.7252709,0.85923946,0.6874303,0.9188243,0.44670975,0.6534332,0.67833525,0.40294313,0.76628596,0.722926,0.2507119,0.86939317,0.1049489,0.5707651,0.89342695,0.89022624,0.06606513,0.46363428,0.8836891,0.8416466,0.43164334,-0.059498303,0.25076458,0.91614866,0.21405962,0.07442343,0.8398273,-0.518248,0.4477598,0.54731685,0.39200985,0.2999862,0.22204888,0.9051194,0.7241311,0.9049213,0.48899868,0.11941989,0.45151904,0.9315986,0.17897557,0.759705,0.2549287,0.96008617,0.25688004,0.5925487,0.3069243,0.9171891,0.46981755,0.14557107,0.8900092,0.84537476,0.5608369,0.6909559,0.777092,0.66562796,0.6040272,0.77930593,0.59144366,0.12506102],
          "boost":0.8
        }
      ],
      "filter":[
          {
              "range":{
                  "int":{
                      "gte":1,
                      "lte":1000
                  }
              }
          },
          {
              "term":{
                "string_tags":["28","2","29"],
                "operator":"or"
              }
          }
       ]
  },
  "is_brute_search":0
  "size":10,
}
' {{ROUTER}}/test_vector_db/vector_space/_search
````

### search document by query and vector for ivfpq
````$xslt
curl -H "content-type: application/json" -XPOST -d'
{
  "query": {
      "sum":[
        {
          "field": "vector",
          "feature": [0.88658684,0.9873159,0.68632215,-0.114685304,-0.45059848,0.5360963,0.9243208,0.14288005,0.9383601,0.17486687,0.3889527,0.91680753,0.6597193,0.52906346,0.5491872,-0.24706548,0.28541148,0.87731135,-0.18872026,0.28016,0.14826365,0.7217548,0.66360927,0.839685,0.29014188,-0.7303055,0.31786093,0.7611028,0.38408384,0.004707908,0.27696127,0.6069607,0.52147454,0.34435293,0.5665409,0.9676775,0.9415799,-0.95000356,-0.7441306,0.32473814,0.24417956,0.4114195,-0.15658693,0.9567978,0.91448873,0.8040493,0.7370252,0.41042542,-0.12714817,0.7344759,0.95486677,0.6752892,0.79088193,0.27843192,0.7594493,0.96637094,0.21354128,0.14667709,0.52713686,0.39803344,0.13063455,-0.26041254,0.21177465,0.0889158,0.7040157,0.9184541,0.33231667,0.109015055,0.7252709,0.85923946,0.6874303,0.9188243,0.44670975,0.6534332,0.67833525,0.40294313,0.76628596,0.722926,0.2507119,0.86939317,0.1049489,0.5707651,0.89342695,0.89022624,0.06606513,0.46363428,0.8836891,0.8416466,0.43164334,-0.059498303,0.25076458,0.91614866,0.21405962,0.07442343,0.8398273,-0.518248,0.4477598,0.54731685,0.39200985,0.2999862,0.22204888,0.9051194,0.7241311,0.9049213,0.48899868,0.11941989,0.45151904,0.9315986,0.17897557,0.759705,0.2549287,0.96008617,0.25688004,0.5925487,0.3069243,0.9171891,0.46981755,0.14557107,0.8900092,0.84537476,0.5608369,0.6909559,0.777092,0.66562796,0.6040272,0.77930593,0.59144366,0.12506102],          "boost":0.8
        }
      ],
      "filter":[
          {
              "range":{
                  "int":{
                      "gte":1,
                      "lte":1000
                  }
              }
          },
          {
              "term":{
                "string_tags":["28","2","29"],
                "operator":"or"
              }
          }
       ]
  },
  "retrieval_param": {
      "parallel_on_queries": 1,
      "recall_num" : 100,
      "nprobe": 80,
      "metric_type": "L2" 
  }
}
' {{ROUTER}}/test_vector_db/vector_space/_search
````
* `parallel_on_queries` : defalut 1, it will parallel on queries. 0: parallel on invert lists
* `recall_num` : default value is equal to size. It determines how many vectors to return when searching the index and finally returns the n most similar results.
* `nprobe` : how many invert list will be visited when search.
* `metric_type` : L2 or InnerProduct.

### search document by query and vector for GPU
````$xslt
curl -H "content-type: application/json" -XPOST -d'
{
  "query": {
      "sum":[
        {
          "field": "vector",
          "feature": [0.88658684,0.9873159,0.68632215,-0.114685304,-0.45059848,0.5360963,0.9243208,0.14288005,0.9383601,0.17486687,0.3889527,0.91680753,0.6597193,0.52906346,0.5491872,-0.24706548,0.28541148,0.87731135,-0.18872026,0.28016,0.14826365,0.7217548,0.66360927,0.839685,0.29014188,-0.7303055,0.31786093,0.7611028,0.38408384,0.004707908,0.27696127,0.6069607,0.52147454,0.34435293,0.5665409,0.9676775,0.9415799,-0.95000356,-0.7441306,0.32473814,0.24417956,0.4114195,-0.15658693,0.9567978,0.91448873,0.8040493,0.7370252,0.41042542,-0.12714817,0.7344759,0.95486677,0.6752892,0.79088193,0.27843192,0.7594493,0.96637094,0.21354128,0.14667709,0.52713686,0.39803344,0.13063455,-0.26041254,0.21177465,0.0889158,0.7040157,0.9184541,0.33231667,0.109015055,0.7252709,0.85923946,0.6874303,0.9188243,0.44670975,0.6534332,0.67833525,0.40294313,0.76628596,0.722926,0.2507119,0.86939317,0.1049489,0.5707651,0.89342695,0.89022624,0.06606513,0.46363428,0.8836891,0.8416466,0.43164334,-0.059498303,0.25076458,0.91614866,0.21405962,0.07442343,0.8398273,-0.518248,0.4477598,0.54731685,0.39200985,0.2999862,0.22204888,0.9051194,0.7241311,0.9049213,0.48899868,0.11941989,0.45151904,0.9315986,0.17897557,0.759705,0.2549287,0.96008617,0.25688004,0.5925487,0.3069243,0.9171891,0.46981755,0.14557107,0.8900092,0.84537476,0.5608369,0.6909559,0.777092,0.66562796,0.6040272,0.77930593,0.59144366,0.12506102],          "boost":0.8
        }
      ],
      "filter":[
          {
              "range":{
                  "int":{
                      "gte":1,
                      "lte":1000
                  }
              }
          },
          {
              "term":{
                "string_tags":["28","2","29"],
                "operator":"or"
              }
          }
       ]
  },
  "retrieval_param": {
      "recall_num" : 100,
      "nprobe": 80,
      "metric_type": "L2"
  }
}
' {{ROUTER}}/test_vector_db/vector_space/_search
````
* `recall_num` : default value is equal to size. It determines how many results to return when searching the index and finally returns the topn most similar results. And you should set it larger if you have filter in your query.Because for gpu, it will filter after searching the index.
* `nprobe` : how many invert list will be visited when search.
* `metric_type` : L2 or InnerProduct.

### search document by query and vector for hnsw
````$xslt
curl -H "content-type: application/json" -XPOST -d'
{
  "query": {
      "sum":[
        {
          "field": "vector",
          "feature": [0.88658684,0.9873159,0.68632215,-0.114685304,-0.45059848,0.5360963,0.9243208,0.14288005,0.9383601,0.17486687,0.3889527,0.91680753,0.6597193,0.52906346,0.5491872,-0.24706548,0.28541148,0.87731135,-0.18872026,0.28016,0.14826365,0.7217548,0.66360927,0.839685,0.29014188,-0.7303055,0.31786093,0.7611028,0.38408384,0.004707908,0.27696127,0.6069607,0.52147454,0.34435293,0.5665409,0.9676775,0.9415799,-0.95000356,-0.7441306,0.32473814,0.24417956,0.4114195,-0.15658693,0.9567978,0.91448873,0.8040493,0.7370252,0.41042542,-0.12714817,0.7344759,0.95486677,0.6752892,0.79088193,0.27843192,0.7594493,0.96637094,0.21354128,0.14667709,0.52713686,0.39803344,0.13063455,-0.26041254,0.21177465,0.0889158,0.7040157,0.9184541,0.33231667,0.109015055,0.7252709,0.85923946,0.6874303,0.9188243,0.44670975,0.6534332,0.67833525,0.40294313,0.76628596,0.722926,0.2507119,0.86939317,0.1049489,0.5707651,0.89342695,0.89022624,0.06606513,0.46363428,0.8836891,0.8416466,0.43164334,-0.059498303,0.25076458,0.91614866,0.21405962,0.07442343,0.8398273,-0.518248,0.4477598,0.54731685,0.39200985,0.2999862,0.22204888,0.9051194,0.7241311,0.9049213,0.48899868,0.11941989,0.45151904,0.9315986,0.17897557,0.759705,0.2549287,0.96008617,0.25688004,0.5925487,0.3069243,0.9171891,0.46981755,0.14557107,0.8900092,0.84537476,0.5608369,0.6909559,0.777092,0.66562796,0.6040272,0.77930593,0.59144366,0.12506102],          "boost":0.8
        }
      ],
      "filter":[
          {
              "range":{
                  "int":{
                      "gte":1,
                      "lte":1000
                  }
              }
          },
          {
              "term":{
                "string_tags":["28","2","29"],
                "operator":"or"
              }
          }
       ]
  },
  "retrieval_param": {
      "efSearch": 64,
      "metric_type": "L2"
  }
}
' {{ROUTER}}/test_vector_db/vector_space/_search
````
* `efSearch` : It determines how far to traverse in the graph.
* `metric_type` : L2 or InnerProduct.

### search document by query and vector for ivfflat
````$xslt
curl -H "content-type: application/json" -XPOST -d'
{
  "query": {
      "sum":[
        {
          "field": "vector",
          "feature": [0.88658684,0.9873159,0.68632215,-0.114685304,-0.45059848,0.5360963,0.9243208,0.14288005,0.9383601,0.17486687,0.3889527,0.91680753,0.6597193,0.52906346,0.5491872,-0.24706548,0.28541148,0.87731135,-0.18872026,0.28016,0.14826365,0.7217548,0.66360927,0.839685,0.29014188,-0.7303055,0.31786093,0.7611028,0.38408384,0.004707908,0.27696127,0.6069607,0.52147454,0.34435293,0.5665409,0.9676775,0.9415799,-0.95000356,-0.7441306,0.32473814,0.24417956,0.4114195,-0.15658693,0.9567978,0.91448873,0.8040493,0.7370252,0.41042542,-0.12714817,0.7344759,0.95486677,0.6752892,0.79088193,0.27843192,0.7594493,0.96637094,0.21354128,0.14667709,0.52713686,0.39803344,0.13063455,-0.26041254,0.21177465,0.0889158,0.7040157,0.9184541,0.33231667,0.109015055,0.7252709,0.85923946,0.6874303,0.9188243,0.44670975,0.6534332,0.67833525,0.40294313,0.76628596,0.722926,0.2507119,0.86939317,0.1049489,0.5707651,0.89342695,0.89022624,0.06606513,0.46363428,0.8836891,0.8416466,0.43164334,-0.059498303,0.25076458,0.91614866,0.21405962,0.07442343,0.8398273,-0.518248,0.4477598,0.54731685,0.39200985,0.2999862,0.22204888,0.9051194,0.7241311,0.9049213,0.48899868,0.11941989,0.45151904,0.9315986,0.17897557,0.759705,0.2549287,0.96008617,0.25688004,0.5925487,0.3069243,0.9171891,0.46981755,0.14557107,0.8900092,0.84537476,0.5608369,0.6909559,0.777092,0.66562796,0.6040272,0.77930593,0.59144366,0.12506102],          "boost":0.8
        }
      ],
      "filter":[
          {
              "range":{
                  "int":{
                      "gte":1,
                      "lte":1000
                  }
              }
          },
          {
              "term":{
                "string_tags":["28","2","29"],
                "operator":"or"
              }
          }
       ]
  },
  "retrieval_param": {
      "parallel_on_queries": 1,
      "nprobe": 80,
      "metric_type": "L2"
  }
}
' {{ROUTER}}/test_vector_db/vector_space/_search
````
* `parallel_on_queries` : defalut 1, it will parallel on queries. 0: parallel on invert lists
* `nprobe` : how many invert list will be visited when search.
* `metric_type` : L2 or InnerProduct.

### search document by query and vector for flat
````$xslt
curl -H "content-type: application/json" -XPOST -d'
{
  "query": {
      "sum":[
        {
          "field": "vector",
          "feature": [0.88658684,0.9873159,0.68632215,-0.114685304,-0.45059848,0.5360963,0.9243208,0.14288005,0.9383601,0.17486687,0.3889527,0.91680753,0.6597193,0.52906346,0.5491872,-0.24706548,0.28541148,0.87731135,-0.18872026,0.28016,0.14826365,0.7217548,0.66360927,0.839685,0.29014188,-0.7303055,0.31786093,0.7611028,0.38408384,0.004707908,0.27696127,0.6069607,0.52147454,0.34435293,0.5665409,0.9676775,0.9415799,-0.95000356,-0.7441306,0.32473814,0.24417956,0.4114195,-0.15658693,0.9567978,0.91448873,0.8040493,0.7370252,0.41042542,-0.12714817,0.7344759,0.95486677,0.6752892,0.79088193,0.27843192,0.7594493,0.96637094,0.21354128,0.14667709,0.52713686,0.39803344,0.13063455,-0.26041254,0.21177465,0.0889158,0.7040157,0.9184541,0.33231667,0.109015055,0.7252709,0.85923946,0.6874303,0.9188243,0.44670975,0.6534332,0.67833525,0.40294313,0.76628596,0.722926,0.2507119,0.86939317,0.1049489,0.5707651,0.89342695,0.89022624,0.06606513,0.46363428,0.8836891,0.8416466,0.43164334,-0.059498303,0.25076458,0.91614866,0.21405962,0.07442343,0.8398273,-0.518248,0.4477598,0.54731685,0.39200985,0.2999862,0.22204888,0.9051194,0.7241311,0.9049213,0.48899868,0.11941989,0.45151904,0.9315986,0.17897557,0.759705,0.2549287,0.96008617,0.25688004,0.5925487,0.3069243,0.9171891,0.46981755,0.14557107,0.8900092,0.84537476,0.5608369,0.6909559,0.777092,0.66562796,0.6040272,0.77930593,0.59144366,0.12506102],          "boost":0.8
        }
      ],
      "filter":[
          {
              "range":{
                  "int":{
                      "gte":1,
                      "lte":1000
                  }
              }
          },
          {
              "term":{
                "string_tags":["28","2","29"],
                "operator":"or"
              }
          }
       ]
  },
  "retrieval_param": {
      "metric_type": "L2"
  }
}
' {{ROUTER}}/test_vector_db/vector_space/_search
````
* `metric_type` : L2 or InnerProduct.

> url: [ip]:[port]/[dbName]/[tableName]/_search
* filter->term-> operator [`and`, `or`, `not`] default `or` 
* `is_brute_search` : default 0 ; -1: no brute force search, 0: auto, 1: always brute force search
* `online_log_level` : "debug" , is print debug info 
* `quick` : default is false, if quick=true it not use precision sorting
* `vector_value` : default is false, is return vector value
* `client_type` : search partition type, default is leader , `random` or `no_leader`
* `l2_sqrt` : default FALSE, don't do sqrt; TRUE, do sqrt

### delete Document

````$xslt
curl -XDELETE {{ROUTER}}/test_vector_db/vector_space/1
````
> url: [ip]:[port]/[dbName]/[tableName]/[documentID]


### document update by merge 
````$xslt
curl -H "content-type: application/json" -XPOST -d'
{
	"_id":"1",
	"doc":{
	    "int": 32
	}
}
' {{ROUTER}}/test_vector_db/vector_space/2/_update
````

### document bulk insert
````$xslt
curl -XPOST -d'
{ "index" : {"_id" : "1" } }
{"string":"14AW1mK_j19FyJvn5NR4Ep","int":14,"float":3.7416573867739413,"vector":{"feature":[0.88658684,0.9873159,0.68632215,-0.114685304,-0.45059848,0.5360963,0.9243208,0.14288005,0.9383601,0.17486687,0.3889527,0.91680753,0.6597193,0.52906346,0.5491872,-0.24706548,0.28541148,0.87731135,-0.18872026,0.28016,0.14826365,0.7217548,0.66360927,0.839685,0.29014188,-0.7303055,0.31786093,0.7611028,0.38408384,0.004707908,0.27696127,0.6069607,0.52147454,0.34435293,0.5665409,0.9676775,0.9415799,-0.95000356,-0.7441306,0.32473814,0.24417956,0.4114195,-0.15658693,0.9567978,0.91448873,0.8040493,0.7370252,0.41042542,-0.12714817,0.7344759,0.95486677,0.6752892,0.79088193,0.27843192,0.7594493,0.96637094,0.21354128,0.14667709,0.52713686,0.39803344,0.13063455,-0.26041254,0.21177465,0.0889158,0.7040157,0.9184541,0.33231667,0.109015055,0.7252709,0.85923946,0.6874303,0.9188243,0.44670975,0.6534332,0.67833525,0.40294313,0.76628596,0.722926,0.2507119,0.86939317,0.1049489,0.5707651,0.89342695,0.89022624,0.06606513,0.46363428,0.8836891,0.8416466,0.43164334,-0.059498303,0.25076458,0.91614866,0.21405962,0.07442343,0.8398273,-0.518248,0.4477598,0.54731685,0.39200985,0.2999862,0.22204888,0.9051194,0.7241311,0.9049213,0.48899868,0.11941989,0.45151904,0.9315986,0.17897557,0.759705,0.2549287,0.96008617,0.25688004,0.5925487,0.3069243,0.9171891,0.46981755,0.14557107,0.8900092,0.84537476,0.5608369,0.6909559,0.777092,0.66562796,0.6040272,0.77930593,0.59144366,0.12506102],"source":"14AW1mK_j19FyJvn5NR4Ep"},"string_tags":["14","10","15"],"int_tags":[14,10,15],"float_tags":[0.88658684,0.9873159,0.68632215,-0.114685304,-0.45059848]}
{ "index" : {"_id" : "2" } }
{"string":"15AW1mK_j19FyJvn5NR4Eq","int":15,"float":3.872983346207417,"vector":{"feature":[-0.0038810808,0.50270164,0.46236226,0.8368957,0.8815242,0.78809184,0.89055073,-0.51614517,0.54325134,0.6719224,0.13672091,0.6404796,0.9224157,-0.7740983,0.52720326,0.74872464,0.8260708,0.9365769,0.42292675,0.16674419,0.56215894,0.089285985,0.88623554,0.7564934,-0.83398277,0.6405787,0.2872994,0.12818569,0.39748654,0.64588255,0.56034446,0.086507045,0.6948602,0.520057,0.25022814,0.41388723,0.033272985,0.7404135,0.64617085,0.3767786,0.6626121,0.6251477,0.032421302,0.59941626,0.16301283,0.40923816,0.09709679,0.43523893,0.27910972,0.8172492,0.005106152,0.6954244,0.16049409,0.05720525,0.82384783,0.28723553,0.8000356,0.6657377,0.94619673,0.41396338,0.7705339,0.20907669,0.45554107,0.93863463,0.21363859,0.34517393,0.5998529,0.6238152,0.48901105,0.29416743,0.12389764,0.35137305,0.40488324,0.87247574,0.6704096,0.49591425,0.9041601,0.0914733,-0.33062342,0.26721698,0.98811746,0.3249821,0.32736206,0.4463985,0.6514083,0.43671924,0.8226718,0.24561033,0.802273,0.7555299,0.42211258,0.4470421,0.4500975,0.5951412,0.022136642,0.6884238,0.05849208,0.91610074,0.019590367,-0.5526502,0.12399096,0.15212315,0.048176214,0.8750968,0.8719288,0.4471701,0.96245474,0.9566114,-0.92479396,0.7930607,0.22384964,0.9283645,0.75434643,0.6996443,0.14241579,0.21770845,0.022004746,-0.8546021,0.5877476,-0.7832402,-0.2893,-0.9363224,0.91134095,0.079473376,0.9525028,0.56223595,0.77623546,0.9966859],"source":"15AW1mK_j19FyJvn5NR4Eq"},"string_tags":["15","4","16"],"int_tags":[15,4,16],"float_tags":[-0.0038810808,0.50270164,0.46236226,0.8368957,0.8815242,0.78809184]}
' {{ROUTER}}/test_vector_db/vector_space/_bulk
````
or
````$xslt
curl -H "content-type: application/json" -XPOST -d $'
{ "index" : {"_id" : "1" } }\n{"string":"14AW1mK_j19FyJvn5NR4Ep","integer":14,"float":3.7416573867739413,"vector":{"feature":[0.88658684,0.9873159,0.68632215,-0.114685304,-0.45059848,0.5360963,0.9243208,0.14288005,0.9383601,0.17486687,0.3889527,0.91680753,0.6597193,0.52906346,0.5491872,-0.24706548,0.28541148,0.87731135,-0.18872026,0.28016,0.14826365,0.7217548,0.66360927,0.839685,0.29014188,-0.7303055,0.31786093,0.7611028,0.38408384,0.004707908,0.27696127,0.6069607,0.52147454,0.34435293,0.5665409,0.9676775,0.9415799,-0.95000356,-0.7441306,0.32473814,0.24417956,0.4114195,-0.15658693,0.9567978,0.91448873,0.8040493,0.7370252,0.41042542,-0.12714817,0.7344759,0.95486677,0.6752892,0.79088193,0.27843192,0.7594493,0.96637094,0.21354128,0.14667709,0.52713686,0.39803344,0.13063455,-0.26041254,0.21177465,0.0889158,0.7040157,0.9184541,0.33231667,0.109015055,0.7252709,0.85923946,0.6874303,0.9188243,0.44670975,0.6534332,0.67833525,0.40294313,0.76628596,0.722926,0.2507119,0.86939317,0.1049489,0.5707651,0.89342695,0.89022624,0.06606513,0.46363428,0.8836891,0.8416466,0.43164334,-0.059498303,0.25076458,0.91614866,0.21405962,0.07442343,0.8398273,-0.518248,0.4477598,0.54731685,0.39200985,0.2999862,0.22204888,0.9051194,0.7241311,0.9049213,0.48899868,0.11941989,0.45151904,0.9315986,0.17897557,0.759705,0.2549287,0.96008617,0.25688004,0.5925487,0.3069243,0.9171891,0.46981755,0.14557107,0.8900092,0.84537476,0.5608369,0.6909559,0.777092,0.66562796,0.6040272,0.77930593,0.59144366,0.12506102],"source":"14AW1mK_j19FyJvn5NR4Ep"},"string_tags":["14","10","15"],"int_tags":[14,10,15],"float_tags":[0.88658684,0.9873159,0.68632215,-0.114685304,-0.45059848]}\n{ "index" : {"_id" : "2" } }\n{"string":"14AW1mK_j19FyJvn5NR4Ep","integer":14,"float":3.7416573867739413,"vector":{"feature":[0.88658684,0.9873159,0.68632215,-0.114685304,-0.45059848,0.5360963,0.9243208,0.14288005,0.9383601,0.17486687,0.3889527,0.91680753,0.6597193,0.52906346,0.5491872,-0.24706548,0.28541148,0.87731135,-0.18872026,0.28016,0.14826365,0.7217548,0.66360927,0.839685,0.29014188,-0.7303055,0.31786093,0.7611028,0.38408384,0.004707908,0.27696127,0.6069607,0.52147454,0.34435293,0.5665409,0.9676775,0.9415799,-0.95000356,-0.7441306,0.32473814,0.24417956,0.4114195,-0.15658693,0.9567978,0.91448873,0.8040493,0.7370252,0.41042542,-0.12714817,0.7344759,0.95486677,0.6752892,0.79088193,0.27843192,0.7594493,0.96637094,0.21354128,0.14667709,0.52713686,0.39803344,0.13063455,-0.26041254,0.21177465,0.0889158,0.7040157,0.9184541,0.33231667,0.109015055,0.7252709,0.85923946,0.6874303,0.9188243,0.44670975,0.6534332,0.67833525,0.40294313,0.76628596,0.722926,0.2507119,0.86939317,0.1049489,0.5707651,0.89342695,0.89022624,0.06606513,0.46363428,0.8836891,0.8416466,0.43164334,-0.059498303,0.25076458,0.91614866,0.21405962,0.07442343,0.8398273,-0.518248,0.4477598,0.54731685,0.39200985,0.2999862,0.22204888,0.9051194,0.7241311,0.9049213,0.48899868,0.11941989,0.45151904,0.9315986,0.17897557,0.759705,0.2549287,0.96008617,0.25688004,0.5925487,0.3069243,0.9171891,0.46981755,0.14557107,0.8900092,0.84537476,0.5608369,0.6909559,0.777092,0.66562796,0.6040272,0.77930593,0.59144366,0.12506102],"source":"14AW1mK_j19FyJvn5NR4Ep"},"string_tags":["14","10","15"],"int_tags":[14,10,15],"float_tags":[0.88658684,0.9873159,0.68632215,-0.114685304,-0.45059848]}
' http://172.20.189.96:80/ts_db/ts_space/_bulk
````

### document multiple vectors bulk search
````$xslt
curl -H "content-type: application/json" -XPOST -d'

{
  "query": {
      "and":[
        {
          "field": "vector",
          "feature": [0.88658684,0.9873159,0.68632215,-0.114685304,-0.45059848,0.5360963,0.9243208,0.14288005,0.9383601,0.17486687,0.3889527,0.91680753,0.6597193,0.52906346,0.5491872,-0.24706548,0.28541148,0.87731135,-0.18872026,0.28016,0.14826365,0.7217548,0.66360927,0.839685,0.29014188,-0.7303055,0.31786093,0.7611028,0.38408384,0.004707908,0.27696127,0.6069607,0.52147454,0.34435293,0.5665409,0.9676775,0.9415799,-0.95000356,-0.7441306,0.32473814,0.24417956,0.4114195,-0.15658693,0.9567978,0.91448873,0.8040493,0.7370252,0.41042542,-0.12714817,0.7344759,0.95486677,0.6752892,0.79088193,0.27843192,0.7594493,0.96637094,0.21354128,0.14667709,0.52713686,0.39803344,0.13063455,-0.26041254,0.21177465,0.0889158,0.7040157,0.9184541,0.33231667,0.109015055,0.7252709,0.85923946,0.6874303,0.9188243,0.44670975,0.6534332,0.67833525,0.40294313,0.76628596,0.722926,0.2507119,0.86939317,0.1049489,0.5707651,0.89342695,0.89022624,0.06606513,0.46363428,0.8836891,0.8416466,0.43164334,-0.059498303,0.25076458,0.91614866,0.21405962,0.07442343,0.8398273,-0.518248,0.4477598,0.54731685,0.39200985,0.2999862,0.22204888,0.9051194,0.7241311,0.9049213,0.48899868,0.11941989,0.45151904,0.9315986,0.17897557,0.759705,0.2549287,0.96008617,0.25688004,0.5925487,0.3069243,0.9171891,0.46981755,0.14557107,0.8900092,0.84537476,0.5608369,0.6909559,0.777092,0.66562796,0.6040272,0.77930593,0.59144366,0.12506102,0.88658684,0.9873159,0.68632215,-0.114685304,-0.45059848,0.5360963,0.9243208,0.14288005,0.9383601,0.17486687,0.3889527,0.91680753,0.6597193,0.52906346,0.5491872,-0.24706548,0.28541148,0.87731135,-0.18872026,0.28016,0.14826365,0.7217548,0.66360927,0.839685,0.29014188,-0.7303055,0.31786093,0.7611028,0.38408384,0.004707908,0.27696127,0.6069607,0.52147454,0.34435293,0.5665409,0.9676775,0.9415799,-0.95000356,-0.7441306,0.32473814,0.24417956,0.4114195,-0.15658693,0.9567978,0.91448873,0.8040493,0.7370252,0.41042542,-0.12714817,0.7344759,0.95486677,0.6752892,0.79088193,0.27843192,0.7594493,0.96637094,0.21354128,0.14667709,0.52713686,0.39803344,0.13063455,-0.26041254,0.21177465,0.0889158,0.7040157,0.9184541,0.33231667,0.109015055,0.7252709,0.85923946,0.6874303,0.9188243,0.44670975,0.6534332,0.67833525,0.40294313,0.76628596,0.722926,0.2507119,0.86939317,0.1049489,0.5707651,0.89342695,0.89022624,0.06606513,0.46363428,0.8836891,0.8416466,0.43164334,-0.059498303,0.25076458,0.91614866,0.21405962,0.07442343,0.8398273,-0.518248,0.4477598,0.54731685,0.39200985,0.2999862,0.22204888,0.9051194,0.7241311,0.9049213,0.48899868,0.11941989,0.45151904,0.9315986,0.17897557,0.759705,0.2549287,0.96008617,0.25688004,0.5925487,0.3069243,0.9171891,0.46981755,0.14557107,0.8900092,0.84537476,0.5608369,0.6909559,0.777092,0.66562796,0.6040272,0.77930593,0.59144366,0.12506102],
          "symbol":">=",
          "value":0.9
        }
      ],
      "filter":[
          {
              "range":{
                  "int":{
                      "gte":1,
                      "lte":1000
                  }
              }
          },
          {
              "term":{
                "string_tags":["28","2","29"],
                "operator":"or"
              }
          }
       ]
  },
  "size":10
}
' {{ROUTER}}/test_vector_db/vector_space/_msearch
````

### document multiple param bulk search
````$xslt
curl -H "content-type: application/json" -XPOST -d'
[{
		"size": 6,
		"query": {
			"filter": [{
					"term": {
						"string_tags": ["28", "2", "29"],
						"operator": "or"
					}
				},
				{
					"term": {
						"string_tags": ["30"],
						"operator": "or"
					}
				},
				{
					"term": {
						"string_tags": ["10"],
						"operator": "or"
					}
				}
			],
			"sum": [{
				"field": "vector",
				"feature": [0.88658684, 0.9873159, 0.68632215, -0.114685304, -0.45059848, 0.5360963, 0.9243208, 0.14288005, 0.9383601, 0.17486687, 0.3889527, 0.91680753, 0.6597193, 0.52906346, 0.5491872, -0.24706548, 0.28541148, 0.87731135, -0.18872026, 0.28016, 0.14826365, 0.7217548, 0.66360927, 0.839685, 0.29014188, -0.7303055, 0.31786093, 0.7611028, 0.38408384, 0.004707908, 0.27696127, 0.6069607, 0.52147454, 0.34435293, 0.5665409, 0.9676775, 0.9415799, -0.95000356, -0.7441306, 0.32473814, 0.24417956, 0.4114195, -0.15658693, 0.9567978, 0.91448873, 0.8040493, 0.7370252, 0.41042542, -0.12714817, 0.7344759, 0.95486677, 0.6752892, 0.79088193, 0.27843192, 0.7594493, 0.96637094, 0.21354128, 0.14667709, 0.52713686, 0.39803344, 0.13063455, -0.26041254, 0.21177465, 0.0889158, 0.7040157, 0.9184541, 0.33231667, 0.109015055, 0.7252709, 0.85923946, 0.6874303, 0.9188243, 0.44670975, 0.6534332, 0.67833525, 0.40294313, 0.76628596, 0.722926, 0.2507119, 0.86939317, 0.1049489, 0.5707651, 0.89342695, 0.89022624, 0.06606513, 0.46363428, 0.8836891, 0.8416466, 0.43164334, -0.059498303, 0.25076458, 0.91614866, 0.21405962, 0.07442343, 0.8398273, -0.518248, 0.4477598, 0.54731685, 0.39200985, 0.2999862, 0.22204888, 0.9051194, 0.7241311, 0.9049213, 0.48899868, 0.11941989, 0.45151904, 0.9315986, 0.17897557, 0.759705, 0.2549287, 0.96008617, 0.25688004, 0.5925487, 0.3069243, 0.9171891, 0.46981755, 0.14557107, 0.8900092, 0.84537476, 0.5608369, 0.6909559, 0.777092, 0.66562796, 0.6040272, 0.77930593, 0.59144366, 0.12506102, 0.88658684, 0.9873159, 0.68632215, -0.114685304, -0.45059848, 0.5360963, 0.9243208, 0.14288005, 0.9383601, 0.17486687, 0.3889527, 0.91680753, 0.6597193, 0.52906346, 0.5491872, -0.24706548, 0.28541148, 0.87731135, -0.18872026, 0.28016, 0.14826365, 0.7217548, 0.66360927, 0.839685, 0.29014188, -0.7303055, 0.31786093, 0.7611028, 0.38408384, 0.004707908, 0.27696127, 0.6069607, 0.52147454, 0.34435293, 0.5665409, 0.9676775, 0.9415799, -0.95000356, -0.7441306, 0.32473814, 0.24417956, 0.4114195, -0.15658693, 0.9567978, 0.91448873, 0.8040493, 0.7370252, 0.41042542, -0.12714817, 0.7344759, 0.95486677, 0.6752892, 0.79088193, 0.27843192, 0.7594493, 0.96637094, 0.21354128, 0.14667709, 0.52713686, 0.39803344, 0.13063455, -0.26041254, 0.21177465, 0.0889158, 0.7040157, 0.9184541, 0.33231667, 0.109015055, 0.7252709, 0.85923946, 0.6874303, 0.9188243, 0.44670975, 0.6534332, 0.67833525, 0.40294313, 0.76628596, 0.722926, 0.2507119, 0.86939317, 0.1049489, 0.5707651, 0.89342695, 0.89022624, 0.06606513, 0.46363428, 0.8836891, 0.8416466, 0.43164334, -0.059498303, 0.25076458, 0.91614866, 0.21405962, 0.07442343, 0.8398273, -0.518248, 0.4477598, 0.54731685, 0.39200985, 0.2999862, 0.22204888, 0.9051194, 0.7241311, 0.9049213, 0.48899868, 0.11941989, 0.45151904, 0.9315986, 0.17897557, 0.759705, 0.2549287, 0.96008617, 0.25688004, 0.5925487, 0.3069243, 0.9171891, 0.46981755, 0.14557107, 0.8900092, 0.84537476, 0.5608369, 0.6909559, 0.777092, 0.66562796, 0.6040272, 0.77930593, 0.59144366, 0.12506102],
				"boost": 1,
				"min_score": 0.7
			}]
		},
		"sort": [{
			"int": {
				"order": "asc"
			}
		}],
		"fields": [
			"int", "float"
		]
	},
	{
		"size": 3,
		"query": {
			"filter": [{
					"term": {
						"string_tags": ["10", "2"],
						"operator": "or"
					}
				},
				{
					"term": {
						"string_tags": ["60"],
						"operator": "or"
					}
				},
				{
					"term": {
						"string_tags": ["11"],
						"operator": "or"
					}
				}
			],
			"sum": [{
				"field": "vector",
				"feature": [0.88658684, 0.9873159, 0.68632215, -0.114685304, -0.45059848, 0.5360963, 0.9243208, 0.14288005, 0.9383601, 0.17486687, 0.3889527, 0.91680753, 0.6597193, 0.52906346, 0.5491872, -0.24706548, 0.28541148, 0.87731135, -0.18872026, 0.28016, 0.14826365, 0.7217548, 0.66360927, 0.839685, 0.29014188, -0.7303055, 0.31786093, 0.7611028, 0.38408384, 0.004707908, 0.27696127, 0.6069607, 0.52147454, 0.34435293, 0.5665409, 0.9676775, 0.9415799, -0.95000356, -0.7441306, 0.32473814, 0.24417956, 0.4114195, -0.15658693, 0.9567978, 0.91448873, 0.8040493, 0.7370252, 0.41042542, -0.12714817, 0.7344759, 0.95486677, 0.6752892, 0.79088193, 0.27843192, 0.7594493, 0.96637094, 0.21354128, 0.14667709, 0.52713686, 0.39803344, 0.13063455, -0.26041254, 0.21177465, 0.0889158, 0.7040157, 0.9184541, 0.33231667, 0.109015055, 0.7252709, 0.85923946, 0.6874303, 0.9188243, 0.44670975, 0.6534332, 0.67833525, 0.40294313, 0.76628596, 0.722926, 0.2507119, 0.86939317, 0.1049489, 0.5707651, 0.89342695, 0.89022624, 0.06606513, 0.46363428, 0.8836891, 0.8416466, 0.43164334, -0.059498303, 0.25076458, 0.91614866, 0.21405962, 0.07442343, 0.8398273, -0.518248, 0.4477598, 0.54731685, 0.39200985, 0.2999862, 0.22204888, 0.9051194, 0.7241311, 0.9049213, 0.48899868, 0.11941989, 0.45151904, 0.9315986, 0.17897557, 0.759705, 0.2549287, 0.96008617, 0.25688004, 0.5925487, 0.3069243, 0.9171891, 0.46981755, 0.14557107, 0.8900092, 0.84537476, 0.5608369, 0.6909559, 0.777092, 0.66562796, 0.6040272, 0.77930593, 0.59144366, 0.12506102, 0.88658684, 0.9873159, 0.68632215, -0.114685304, -0.45059848, 0.5360963, 0.9243208, 0.14288005, 0.9383601, 0.17486687, 0.3889527, 0.91680753, 0.6597193, 0.52906346, 0.5491872, -0.24706548, 0.28541148, 0.87731135, -0.18872026, 0.28016, 0.14826365, 0.7217548, 0.66360927, 0.839685, 0.29014188, -0.7303055, 0.31786093, 0.7611028, 0.38408384, 0.004707908, 0.27696127, 0.6069607, 0.52147454, 0.34435293, 0.5665409, 0.9676775, 0.9415799, -0.95000356, -0.7441306, 0.32473814, 0.24417956, 0.4114195, -0.15658693, 0.9567978, 0.91448873, 0.8040493, 0.7370252, 0.41042542, -0.12714817, 0.7344759, 0.95486677, 0.6752892, 0.79088193, 0.27843192, 0.7594493, 0.96637094, 0.21354128, 0.14667709, 0.52713686, 0.39803344, 0.13063455, -0.26041254, 0.21177465, 0.0889158, 0.7040157, 0.9184541, 0.33231667, 0.109015055, 0.7252709, 0.85923946, 0.6874303, 0.9188243, 0.44670975, 0.6534332, 0.67833525, 0.40294313, 0.76628596, 0.722926, 0.2507119, 0.86939317, 0.1049489, 0.5707651, 0.89342695, 0.89022624, 0.06606513, 0.46363428, 0.8836891, 0.8416466, 0.43164334, -0.059498303, 0.25076458, 0.91614866, 0.21405962, 0.07442343, 0.8398273, -0.518248, 0.4477598, 0.54731685, 0.39200985, 0.2999862, 0.22204888, 0.9051194, 0.7241311, 0.9049213, 0.48899868, 0.11941989, 0.45151904, 0.9315986, 0.17897557, 0.759705, 0.2549287, 0.96008617, 0.25688004, 0.5925487, 0.3069243, 0.9171891, 0.46981755, 0.14557107, 0.8900092, 0.84537476, 0.5608369, 0.6909559, 0.777092, 0.66562796, 0.6040272, 0.77930593, 0.59144366, 0.12506102],
				"boost": 1,
				"min_score": 0.9
			}]
		},
		"sort": [{
			"float": {
				"order": "desc"
			}
		}],
		"fields": [
			"int", "float"
		]
	},
]
' {{ROUTER}}/test_vector_db/vector_space/_bulk_search
````
### get vectors by ids and param search
````$xslt
curl -H "content-type: application/json" -XPOST -d'
{
  "size":50,
  "query":{
    "filter":[
      {
        "term":{
          "operator":"and",
          "string_tags":["10","2"]
        }
      },
      {
        "term":{
          "operator":"or",
          "string_tags":["50","12"]
        }
      },
      {
        "term":{
          "operator":"not",
           "string_tags":["100"]
        }
      },
      {
        "term":{
          "operator":"and",
          "string_tags":["101"]
        }
      },
      {
        "term":{
          "operator":"or",
           "string_tags":["99","98"]
        }
      }
    ],
    "ids":[
      "123"
    ],
    "sum":[
      {
        "field":"vector",
         "feature": [0.88658684,0.9873159,0.68632215,-0.114685304,-0.45059848,0.5360963,0.9243208,0.14288005,0.9383601,0.17486687,0.3889527,0.91680753,0.6597193,0.52906346,0.5491872,-0.24706548,0.28541148,0.87731135,-0.18872026,0.28016,0.14826365,0.7217548,0.66360927,0.839685,0.29014188,-0.7303055,0.31786093,0.7611028,0.38408384,0.004707908,0.27696127,0.6069607,0.52147454,0.34435293,0.5665409,0.9676775,0.9415799,-0.95000356,-0.7441306,0.32473814,0.24417956,0.4114195,-0.15658693,0.9567978,0.91448873,0.8040493,0.7370252,0.41042542,-0.12714817,0.7344759,0.95486677,0.6752892,0.79088193,0.27843192,0.7594493,0.96637094,0.21354128,0.14667709,0.52713686,0.39803344,0.13063455,-0.26041254,0.21177465,0.0889158,0.7040157,0.9184541,0.33231667,0.109015055,0.7252709,0.85923946,0.6874303,0.9188243,0.44670975,0.6534332,0.67833525,0.40294313,0.76628596,0.722926,0.2507119,0.86939317,0.1049489,0.5707651,0.89342695,0.89022624,0.06606513,0.46363428,0.8836891,0.8416466,0.43164334,-0.059498303,0.25076458,0.91614866,0.21405962,0.07442343,0.8398273,-0.518248,0.4477598,0.54731685,0.39200985,0.2999862,0.22204888,0.9051194,0.7241311,0.9049213,0.48899868,0.11941989,0.45151904,0.9315986,0.17897557,0.759705,0.2549287,0.96008617,0.25688004,0.5925487,0.3069243,0.9171891,0.46981755,0.14557107,0.8900092,0.84537476,0.5608369,0.6909559,0.777092,0.66562796,0.6040272,0.77930593,0.59144366,0.12506102,0.88658684,0.9873159,0.68632215,-0.114685304,-0.45059848,0.5360963,0.9243208,0.14288005,0.9383601,0.17486687,0.3889527,0.91680753,0.6597193,0.52906346,0.5491872,-0.24706548,0.28541148,0.87731135,-0.18872026,0.28016,0.14826365,0.7217548,0.66360927,0.839685,0.29014188,-0.7303055,0.31786093,0.7611028,0.38408384,0.004707908,0.27696127,0.6069607,0.52147454,0.34435293,0.5665409,0.9676775,0.9415799,-0.95000356,-0.7441306,0.32473814,0.24417956,0.4114195,-0.15658693,0.9567978,0.91448873,0.8040493,0.7370252,0.41042542,-0.12714817,0.7344759,0.95486677,0.6752892,0.79088193,0.27843192,0.7594493,0.96637094,0.21354128,0.14667709,0.52713686,0.39803344,0.13063455,-0.26041254,0.21177465,0.0889158,0.7040157,0.9184541,0.33231667,0.109015055,0.7252709,0.85923946,0.6874303,0.9188243,0.44670975,0.6534332,0.67833525,0.40294313,0.76628596,0.722926,0.2507119,0.86939317,0.1049489,0.5707651,0.89342695,0.89022624,0.06606513,0.46363428,0.8836891,0.8416466,0.43164334,-0.059498303,0.25076458,0.91614866,0.21405962,0.07442343,0.8398273,-0.518248,0.4477598,0.54731685,0.39200985,0.2999862,0.22204888,0.9051194,0.7241311,0.9049213,0.48899868,0.11941989,0.45151904,0.9315986,0.17897557,0.759705,0.2549287,0.96008617,0.25688004,0.5925487,0.3069243,0.9171891,0.46981755,0.14557107,0.8900092,0.84537476,0.5608369,0.6909559,0.777092,0.66562796,0.6040272,0.77930593,0.59144366,0.12506102],
        "boost":1,
        "min_score":0.7
      }
    ]
  },
  "fields":[
    "int"
  ]
}
' {{ROUTER}}/test_vector_db/vector_space/_query_byids_feature
````

### delete by query
````$xslt
# search
curl -H "content-type: application/json" -XPOST -d'
{
  "query": {
      "sum":[
        {
          "field": "vector",
          "feature": [0.88658684,0.9873159,0.68632215,-0.114685304,-0.45059848,0.5360963,0.9243208,0.14288005,0.9383601,0.17486687,0.3889527,0.91680753,0.6597193,0.52906346,0.5491872,-0.24706548,0.28541148,0.87731135,-0.18872026,0.28016,0.14826365,0.7217548,0.66360927,0.839685,0.29014188,-0.7303055,0.31786093,0.7611028,0.38408384,0.004707908,0.27696127,0.6069607,0.52147454,0.34435293,0.5665409,0.9676775,0.9415799,-0.95000356,-0.7441306,0.32473814,0.24417956,0.4114195,-0.15658693,0.9567978,0.91448873,0.8040493,0.7370252,0.41042542,-0.12714817,0.7344759,0.95486677,0.6752892,0.79088193,0.27843192,0.7594493,0.96637094,0.21354128,0.14667709,0.52713686,0.39803344,0.13063455,-0.26041254,0.21177465,0.0889158,0.7040157,0.9184541,0.33231667,0.109015055,0.7252709,0.85923946,0.6874303,0.9188243,0.44670975,0.6534332,0.67833525,0.40294313,0.76628596,0.722926,0.2507119,0.86939317,0.1049489,0.5707651,0.89342695,0.89022624,0.06606513,0.46363428,0.8836891,0.8416466,0.43164334,-0.059498303,0.25076458,0.91614866,0.21405962,0.07442343,0.8398273,-0.518248,0.4477598,0.54731685,0.39200985,0.2999862,0.22204888,0.9051194,0.7241311,0.9049213,0.48899868,0.11941989,0.45151904,0.9315986,0.17897557,0.759705,0.2549287,0.96008617,0.25688004,0.5925487,0.3069243,0.9171891,0.46981755,0.14557107,0.8900092,0.84537476,0.5608369,0.6909559,0.777092,0.66562796,0.6040272,0.77930593,0.59144366,0.12506102],
          "boost":0.8
        }
      ],
      "filter":[
          {
              "range":{
                  "int":{
                      "gte":1,
                      "lte":1000
                  }
              }
          },
          {
              "term":{
                "string_tags":["28","2","29"],
                "operator":"or"
              }
          }
       ]
  },
  "size":10
}
' {{ROUTER}}/test_vector_db/vector_space/_delete_by_query
````


## space in router

----

### get space mapping
````$xslt
curl -XGET {{ROUTER}}/test_vector_db/_mapping/vector_space
````

### flush space
````$xslt
curl -XPOST {{ROUTER}}/test_vector_db/vector_space/_flush
````

### create index to space
````$xslt
curl -XPOST {{ROUTER}}/test_vector_db/vector_space/_forcemerge
````

## cluster API

----

### password_encrypt
````$xslt
curl -XGET {{ROUTER}}/_encrypt?name=cb&password=1234
````

### clean lock
````$xslt
curl -XGET {{MASTER}}/clean_lock
````
> if cluster table creating is crashed, it will has lock , now you need clean lock use this api

### server list
````$xslt
curl -XGET {{MASTER}}/list/server
````

### server stats
````$xslt
curl -XGET {{MASTER}}/_cluster/stats
````


### server health
````$xslt
curl -XGET {{MASTER}}/_cluster/health
````

#### router cache info
````$xslt
curl -XGET {{ROUTER}}/_cache_info?db_name=test_vector_db&space_name=vector_space
````
