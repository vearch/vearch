# Low-level API for vector search

> define MASTER = http://127.0.0.1:8817
> define ROUTER = http://127.0.0.1:9001
> `MASTER` is cluster manager, `ROUTER` is data manager

## database 

----

### create database

````$xslt
curl -v --user "root:secret" -H "content-type: application/json" -XPUT -d'
{
	"name":"test_vector_db"
}
' {{ROUTER}}/db/_create
````

### get database

````$xslt
curl -XGET {{ROUTER}}/db/test_vector_db
````

### list database

````$xslt
curl -XGET {{ROUTER}}/list/db
````

### delete database

````$xslt
curl -XDELETE {{ROUTER}}/db/test_vector_db
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
  "partition_num": 1,
  "replica_num": 1,
  "engine": {
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
    }
  },
  "models": [
    {
      "model_id": "vgg16",
      "fields": [
        "string"
      ],
      "out": "feature"
    }
  ]
}
' {{ROUTER}}/space/test_vector_db/_create
````

Create table for HNSW, and here are the matters needing attention:

index_size: For HNSW, it doesn't need to train before building index, so greater than 0 is enough.
store_type: should set it as `MemoryOnly`. For HNSW, it can only be run in MemoryOnly mode.

````$xslt
curl -v --user "root:secret" -H "content-type: application/json" -XPUT -d'
{
  "name": "vector_space",
  "partition_num": 1,
  "replica_num": 1,
  "engine": {
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
    }
  },
  "models": [
    {
      "model_id": "vgg16",
      "fields": [
        "string"
      ],
      "out": "feature"
    }
  ]
}
' {{ROUTER}}/space/test_vector_db/_create
````

Create table for IVFFLAT, and here are the matters needing attention:

index_size: For IVFFLAT, it need train before building index, so you should set index_size a suitable value, such as 10000 or set larger for larger amounts of data.
store_type: should set it as `RocksDB`. For IVFFLAT, it can only be run in RocksDB mode.

````$xslt
curl -v --user "root:secret" -H "content-type: application/json" -XPUT -d'
{
    "name": "vector_space",
    "partition_num": 1,
    "replica_num": 1,
    "engine": {
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
        }
    },
    "models": [{
        "model_id": "vgg16",
        "fields": ["string"],
        "out": "feature"
    }]
}
' {{ROUTER}}/space/test_vector_db/_create
````

Create table for Multiple models, and here are the matters needing attention:

The parameters of each model may be different, please refer to the precautions on model parameters above.
Multi-model table creation and query statements are different.Multi-model use store_type must be supported by all models.

Old usage for create table:
````$xslt
{
  "name": "vector_space",
  "partition_num": 1,
  "replica_num": 1,
  "engine": {
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
    }
  },
  "models": [
    {
      "model_id": "vgg16",
      "fields": [
        "string"
      ],
      "out": "feature"
    }
  ]
}
````
New usage for create table about Multiple models:
````$xslt
{
  "name": "vector_space",
  "partition_num": 1,
  "replica_num": 1,
  "engine": {
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
    }
  },
  "models": [
    {
      "model_id": "vgg16",
      "fields": [
        "string"
      ],
      "out": "feature"
    }
  ]
}
````

Old usage for _search query about Multiple models :

````$xslt
curl -H "content-type: application/json" -XPOST -d'
{
  "query": {
    "sum": [
      {
        "field": "vector",
        "feature": [
          "features..."
        ]
      }
    ],
    "filter": [
      {
        "range": {
          "int": {
            "gte": 1,
            "lte": 1000
          }
        }
      },
      {
        "term": {
          "string_tags": [
            "28",
            "2",
            "29"
          ],
          "operator": "or"
        }
      }
    ]
  },
  "is_brute_search": 0,
  "size": 10
}
' {{ROUTER}}/test_vector_db/vector_space/_search
````
New usage for _search query about Multiple models:
add retrieval_type paramter,retrieval_type specifies the model to be queried, and the retrieval_param parameter is set according to the specified model. If retrieval_type is not set, the data will be queried according to the first model when the table is created.

```$xslt
curl -H "content-type: application/json" -XPOST -d'
{
  "query": {
    "sum": [
      {
        "field": "vector",
        "retrieval_type": "IVFPQ",
        "feature": [
          "features..."
        ],
        "boost": 0.8
      }
    ],
    "filter": [
      {
        "range": {
          "int": {
            "gte": 1,
            "lte": 1000
          }
        }
      },
      {
        "term": {
          "string_tags": [
            "28",
            "2",
            "29"
          ],
          "operator": "or"
        }
      }
    ]
  },
  "is_brute_search": 0,
  "size": 10
}
' {{ROUTER}}/test_vector_db/vector_space/_search
````


* partition_num : how many partition to slot,  default is `1`

* replica_num: how many replica has, recommend `3`

* engine

* id_type : the type of Primary key, default String, you can set it as `long` or `string`

* index_size : default 2, if insert document num >= index_size, it will start to build index automatically. For different retrieval model, it have different index size.  For HNSW and FLAT, it doesn't need to train before building index, greater than 0 is enough. For IVFPQ, GPU and BINARYIVF, it need train before building index, so you should set index_size larger, such as 100000.

* retrieval_type: the type of retrieval model, now support five kind retrieval model: IVFPQ GPU BINARYIVF HNSW FLAT. BINARYIVF is to index binary data. The Other type of retrieval models are for float32 data. And GPU is the implementation of IVFPQ on GPU, so IVFPQ and GPU have the same retrieval_param. FLAT is brute-force search. HNSW and FLAT can only work in `MemoryOnly` mode. And HNSW now uses mark deletion, and does not make corresponding changes to the hnsw graph structure after deletion or update.

* retrieval_param: parameter of retrieval model, this corresponds to the retrieval type.
For metric_type, It can be specified when building the table, if it is not set when searching, then use the parameters specified when building the table.
* IVFPQ

    * metric_type : `InnerProduct` or `L2`.
    * nprobe : scan clustered buckets, default 80, it should be less than ncentroids. Now it should set at search time. 
    * ncentroids : coarse cluster center number, default 2048
    * nsubvector : the number of sub vector, default 64, only the value which is multiple of 4 is supported now 
    * nbits_per_idx : bit number of sub cluster center, default 8, and 8 is the only value now
    * bucket_init_size : the original size of RTInvertIndex bucket, default 1000. You can set its value to the amount of data you just want to insert divided by ncentroids.
    * bucket_max_size : the max size of RTInvertIndex bucket. default 1280000, if your dataset is very large, you can set it larger.

* GPU

    * metric_type :  `InnerProduct` or `L2`.
    * nprobe : scan clustered buckets, default 80, it should be less than ncentroids. Now it should set at search time.   
    * ncentroids : coarse cluster center number, default 2048 
    * nsubvector : the number of sub vector, default 64
    * nbits_per_idx : bit number of sub cluster center, default 8, and 8 is the only value now

* IVFFLAT

    * metric_type :  `InnerProduct` or `L2`.
    * nprobe : scan clustered buckets, default 80, it should be less than ncentroids. Now it should set at search time. 
    * ncentroids : coarse cluster center number, default 2048
    
* BINARYIVF

    * nprobe : scan clustered buckets, default 20, it should be less than ncentroids. Now it should set at search time. 
    * ncentroids : coarse cluster center number, default 256

* HNSW

    * metric_type : `InnerProduct` or `L2` 
    * nlinks ： neighbors number of each node, default 32
    * efConstruction : expansion factor at construction time, default 40. The higher the value, the better the construction effect, and the longer it takes 
    * efSearch : expansion factor at search time, default 64. The higher the value, the more accurate the search results and the longer it takes. Now it should set at search time.

* FLAT

    * metric_type : `InnerProduct` or `L2`.

* keyword
* array : whether the tags for each document is multi-valued, `true` or `false` default is false
* index : supporting numeric field filter default `false`
* Vector field params
    * format : default not normalized . if you set "normalization", "normal" it will normalized  
    * store_type : "RocksDB" or "Mmap" or "MemoryOnly" default "Mmap".For HNSW and IVFFLAT and FLAT, it can only be run in MemoryOnly mode.   
    * store_param : example {"cache_size":2592}. default value is 1024. It means you will use so much memory, the excess will be kept to disk. For MemoryOnly, this parameter is invalid.

### get space

````$xslt
curl -XGET {{ROUTER}}/space/test_vector_db/vector_space
````

### list space

````$xslt
curl -XGET {{ROUTER}}/list/space?db=test_vector_db
````

### delete space

````$xslt
curl -XDELETE {{ROUTER}}/space/test_vector_db/vector_space
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

> this api is add or del partition in ds
> method : method=0 add partition:1 to node:1, method=1 delete partition:1 from node:1


## document 

Document operations will be redefined as the following 4 interfaces: 
  /document/upsert
  /document/query
  /document/search
  /document/delete

---

### document upsert
If the primary key _id is set, the specified primary key will be used. If it is not set, it will be generated by Vearch.
If the specified _id already exists when inserting, the existing data will be updated; otherwise, it will be inserted.

request format:
````$xslt
Without id
curl -H "content-type: application/json" -XPOST -d'
{
	"db_name": "ts_db",
	"space_name": "ts_space",
	"documents": [{
		"field_int": 90399,
		"field_float": 90399,
		"field_double": 90399,
		"field_string": "111399",
		"field_vector": {
			"feature": [...]
		}
	}, {
		"field_int": 45085,
		"field_float": 45085,
		"field_double": 45085,
		"field_string": "106085",
		"field_vector": {
			"feature": [...]
		}
	}, {
		"field_int": 52968,
		"field_float": 52968,
		"field_double": 52968,
		"field_string": "113968",
		"field_vector": {
			"feature": [...]
		}
	}]
}
' http://router_server/document/upsert

With id
curl -H "content-type: application/json" -XPOST -d'
{
	"db_name": "ts_db",
	"space_name": "ts_space",
	"documents": [{
		"_id": 1000000,
		"field_int": 90399,
		"field_float": 90399,
		"field_double": 90399,
		"field_string": "111399",
		"field_vector": {
			"feature": [...]
		}
	}, {
		"_id": 1000001,
		"field_int": 45085,
		"field_float": 45085,
		"field_double": 45085,
		"field_string": "106085",
		"field_vector": {
			"feature": [...]
		}
	}, {
		"_id": 1000002,
		"field_int": 52968,
		"field_float": 52968,
		"field_double": 52968,
		"field_string": "113968",
		"field_vector": {
			"feature": [...]
		}
	}]
}
' http://router_server/document/upsert
````

response format:
````$xslt
{
	'code': 0,
	'msg': 'success',
	'total': 3,
	'document_ids': [{
		'_id': '-526059949411103803',
		'status': 200,
		'error': 'success'
	}, {
		'_id': '1287805132970120733',
		'status': 200,
		'error': 'success'
	}, {
		'_id': '-1948185285365684656',
		'status': 200,
		'error': 'success'
	}]
}
````

### document query
The interface is used to accurately find documents that exactly match the query conditions.
Usually the query statement does not contain the vector part.
Two methods are supported: one is to obtain documents directly through primary keys,
and the other is to obtain corresponding documents based on filter conditions.
If partition_id is set, obtain the corresponding document on the specified partition.
At this time, the meaning of document_id is the document number on this partition.
Usually this is used to obtain the full data of the cluster.

request format:
````$xslt
By document_ids
curl -H "content-type: application/json" -XPOST -d'
{
	"db_name": "ts_db",
	"space_name": "ts_space",
	"query": {
		"document_ids": ["6560995651113580768", "-5621139761924822824", "-104688682735192253"]
	}
}
' http://router_server/document/query

By document_ids on specify parition
curl -H "content-type: application/json" -XPOST -d'
{
  "db_name": "ts_db",
  "space_name": "ts_space",
  "query": {
    "document_ids": [
      "10000",
      "10001",
      "10002"
    ],
    "partition_id": "1"
  }
}
' http://router_server/document/query

By filter:
curl -H "content-type: application/json" -XPOST -d'
{
  "db_name": "ts_db",
  "space_name": "ts_space",
  "query": {
    "filter": [
      {
        "range": {
          "field_int": {
            "gte": 1000,
            "lte": 100000
          }
        }
      },
      {
        "term": {
          "field_string": [
            "322"
          ]
        }
      }
    ]
  }
}
' http://router_server/document/query
````

response format:
````$xslt
{
	'code': 0,
	'msg': 'success',
	'total': 3,
	'documents': [{
		'_id': '6560995651113580768',
		'_source': {
			'field_double': 202558,
			'field_float': 102558,
			'field_int': 1558,
			'field_string': '1558'
		}
	}, {
		'_id': '-5621139761924822824',
		'_source': {
			'field_double': 210887,
			'field_float': 110887,
			'field_int': 89887,
			'field_string': '89887'
		}
	}, {
		'_id': '-104688682735192253',
		'_source': {
			'field_double': 207588,
			'field_float': 107588,
			'field_int': 46588,
			'field_string': '46588'
		}
	}]
}
````

### document search
A method based on similarity matching. This interface is used to find vectors similar to a given query vector.
This interface supports retrieval based on primary key id and vector.
For vector retrieval, it supports passing in multiple vectors and returning results in batches.

request format:
````$xslt
By document_ids
curl -H "content-type: application/json" -XPOST -d'
{
  "query": {
    "document_ids": [
      "3646866681750952826"
    ],
    "filter": [
      {
        "range": {
          "field_int": {
            "gte": 1000,
            "lte": 100000
          }
        }
      }
    ]
  },
  "retrieval_param": {
    "metric_type": "L2"
  },
  "size": 3,
  "db_name": "ts_db",
  "space_name": "ts_space"
}
' http://router_server/document/search

By vector
curl -H "content-type: application/json" -XPOST -d'
{
  "query": {
    "vector": [
      {
        "field": "field_vector",
        "feature": [
          "..."
        ]
      }
    ],
    "filter": [
      {
        "range": {
          "field_int": {
            "gte": 1000,
            "lte": 100000
          }
        }
      }
    ]
  },
  "retrieval_param": {
    "metric_type": "L2"
  },
  "size": 3,
  "db_name": "ts_db",
  "space_name": "ts_space"
}
' http://router_server/document/search
````

response format:
````$xslt
{
	'code': 0,
	'msg': 'success',
	'documents': [
		[{
			'_id': '6979025510302030694',
			'_score': 16.55717658996582,
			'_source': {
				'field_double': 207598,
				'field_float': 107598,
				'field_int': 6598,
				'field_string': '6598'
			}
		}, {
			'_id': '-104688682735192253',
			'_score': 17.663991928100586,
			'_source': {
				'field_double': 207588,
				'field_float': 107588,
				'field_int': 46588,
				'field_string': '46588'
			}
		}, {
			'_id': '8549822044854277588',
			'_score': 17.88829803466797,
			'_source': {
				'field_double': 220413,
				'field_float': 120413,
				'field_int': 99413,
				'field_string': '99413'
			}
		}]
	]
}
````
### document delete
Delete also supports two methods: document_ids and filter conditions.

request format:
````$xslt
By document_ids
curl -H "content-type: application/json" -XPOST -d'
{
	"db_name": "ts_db",
	"space_name": "ts_space",
	"query": {
		'document_ids': ['4501743250723073467', '616335952940335471', '-2422965400649882823']
	}
}
' http://router_server/document/delete

By filter:
curl -H "content-type: application/json" -XPOST -d'
{
  "db_name": "ts_db",
  "space_name": "ts_space",
  "query": {
    "filter": [
      {
        "range": {
          "field_int": {
            "gte": 1000,
            "lte": 100000
          }
        }
      },
      {
        "term": {
          "field_string": [
            "322"
          ]
        }
      }
    ]
  },
  "size": 3
}
' http://router_server/document/delete
````

response format:
````$xslt
{
	'code': 0,
	'msg': 'success',
	'total': 3,
	'document_ids': ['4501743250723073467', '616335952940335471', '-2422965400649882823']
}
````

The following interfaces will remain compatible for a period of time and will be completely removed in the future.

----

### insert document

````$xslt
curl -H "content-type: application/json" -XPOST -d'
{
  "string": "14AW1mK_j19FyJvn5NR4Ep",
  "int": 14,
  "float": 3.7416573867739413,
  "vector": {
    "feature": [
      "features..."
    ],
    "source": "14AW1mK_j19FyJvn5NR4Ep"
  },
  "string_tags": [
    "14",
    "10",
    "15"
  ]
}
' {{ROUTER}}/test_vector_db/vector_space/1

````
> url: [ip]:[port]/[dbName]/[tableName]/[documentID]


### insert document without id
````$xslt
curl -H "content-type: application/json" -XPOST -d'
{
  "string": "14AW1mK_j19FyJvn5NR4Ep",
  "int": 14,
  "float": 3.7416573867739413,
  "vector": {
    "feature": [
      "features..."
    ],
    "source": "14AW1mK_j19FyJvn5NR4Ep"
  },
  "string_tags": [
    "14",
    "10",
    "15"
  ]
}
' {{ROUTER}}/test_vector_db/vector_space/
````

### search document by id 
````$xslt
curl -XGET {{ROUTER}}/test_vector_db/vector_space/id
````

### search document by id on specify partition
use this to get all data of the cluster, id can be [0, max_doc_id of the specify partition]
````$xslt
curl -XGET {{ROUTER}}/test_vector_db/vector_space/$partition_id/id
````

### bulk search document by ids
````$xslt
curl -H "content-type: application/json" -XPOST -d'
{
  "query": {
    "ids": [
      "3",
      "1"
    ],
    "fields": [
      "int"
    ]
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
    "and": [
      {
        "field": "vector",
        "feature": [
          "features..."
        ],
        "symbol": ">=",
        "value": 0.9
      }
    ],
    "filter": [
      {
        "range": {
          "int": {
            "gte": 1,
            "lte": 1000
          }
        }
      },
      {
        "term": {
          "string_tags": [
            "28",
            "2",
            "29"
          ],
          "operator": "or"
        }
      }
    ]
  },
  "size": 10,
  "quick": false,
  "vector_value": false,
  "sort": [
    {
      "_score": {
        "order": "asc"
      }
    }
  ],
  "fileds": [
    "name",
    "age"
  ]
}
' {{ROUTER}}/test_vector_db/vector_space/_search
````

### search document by query and vector score boost
````$xslt
curl -H "content-type: application/json" -XPOST -d'
{
  "query": {
    "sum": [
      {
        "field": "vector",
        "feature": [
          "features..."
        ],
        "boost": 0.8
      }
    ],
    "filter": [
      {
        "range": {
          "int": {
            "gte": 1,
            "lte": 1000
          }
        }
      },
      {
        "term": {
          "string_tags": [
            "28",
            "2",
            "29"
          ],
          "operator": "or"
        }
      }
    ]
  },
  "is_brute_search": 0,
  "size": 10
}
' {{ROUTER}}/test_vector_db/vector_space/_search
````

### search document by query and vector for ivfpq
````$xslt
curl -H "content-type: application/json" -XPOST -d'
{
  "query": {
    "sum": [
      {
        "field": "vector",
        "feature": [
          "features..."
        ]
      }
    ],
    "filter": [
      {
        "range": {
          "int": {
            "gte": 1,
            "lte": 1000
          }
        }
      },
      {
        "term": {
          "string_tags": [
            "28",
            "2",
            "29"
          ],
          "operator": "or"
        }
      }
    ]
  },
  "retrieval_param": {
    "parallel_on_queries": 1,
    "recall_num": 100,
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
    "sum": [
      {
        "field": "vector",
        "feature": [
          "features..."
        ]
      }
    ],
    "filter": [
      {
        "range": {
          "int": {
            "gte": 1,
            "lte": 1000
          }
        }
      },
      {
        "term": {
          "string_tags": [
            "28",
            "2",
            "29"
          ],
          "operator": "or"
        }
      }
    ]
  },
  "retrieval_param": {
    "recall_num": 100,
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
    "sum": [
      {
        "field": "vector",
        "feature": [
          "features..."
        ],
        "boost": 0.8
      }
    ],
    "filter": [
      {
        "range": {
          "int": {
            "gte": 1,
            "lte": 1000
          }
        }
      },
      {
        "term": {
          "string_tags": [
            "28",
            "2",
            "29"
          ],
          "operator": "or"
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
    "sum": [
      {
        "field": "vector",
        "feature": [
          "features..."
        ],
        "boost": 0.8
      }
    ],
    "filter": [
      {
        "range": {
          "int": {
            "gte": 1,
            "lte": 1000
          }
        }
      },
      {
        "term": {
          "string_tags": [
            "28",
            "2",
            "29"
          ],
          "operator": "or"
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
    "sum": [
      {
        "field": "vector",
        "feature": [
          "features..."
        ],
        "boost": 0.8
      }
    ],
    "filter": [
      {
        "range": {
          "int": {
            "gte": 1,
            "lte": 1000
          }
        }
      },
      {
        "term": {
          "string_tags": [
            "28",
            "2",
            "29"
          ],
          "operator": "or"
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
* `online_log_level` : "debug", is print debug info 
* `quick` : default is false, if quick=true it not use precision sorting
* `vector_value` : default is false, is return vector value
* `load_balance` : load balance type, include `random`, `least_connection`, `no_leader`, `leader`, default is `random`
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
  "_id": "1",
  "doc": {
    "int": 32
  }
}
' {{ROUTER}}/test_vector_db/vector_space/2/_update
````

### document bulk insert
````$xslt
curl -H "content-type: application/json" -XPOST -d'
{"index":{"_id":"1"}}
{"string":"14AW1mK_j19FyJvn5NR4Ep","int":14,"float":3.7416573867739413,"vector":{"feature":["features..."],"source":"14AW1mK_j19FyJvn5NR4Ep"},"string_tags":["14","10","15"]}
{"index":{"_id":"2"}}
{"string":"15AW1mK_j19FyJvn5NR4Eq","int":15,"float":3.872983346207417,"vector":{"feature":["features..."],"source":"15AW1mK_j19FyJvn5NR4Eq"},"string_tags":["15","4","16"]}
' {{ROUTER}}/test_vector_db/vector_space/_bulk
````

### document multiple vectors bulk search
````$xslt
curl -H "content-type: application/json" -XPOST -d'
{
  "query": {
    "and": [
      {
        "field": "vector",
        "feature": [
          "features..."
        ],
        "symbol": ">=",
        "value": 0.9
      }
    ],
    "filter": [
      {
        "range": {
          "int": {
            "gte": 1,
            "lte": 1000
          }
        }
      },
      {
        "term": {
          "string_tags": [
            "28",
            "2",
            "29"
          ],
          "operator": "or"
        }
      }
    ]
  },
  "size": 10
}
' {{ROUTER}}/test_vector_db/vector_space/_msearch
````

### document multiple param bulk search
````$xslt
curl -H "content-type: application/json" -XPOST -d'
[
  {
    "size": 6,
    "query": {
      "filter": [
        {
          "term": {
            "string_tags": [
              "28",
              "2",
              "29"
            ],
            "operator": "or"
          }
        },
        {
          "term": {
            "string_tags": [
              "30"
            ],
            "operator": "or"
          }
        },
        {
          "term": {
            "string_tags": [
              "10"
            ],
            "operator": "or"
          }
        }
      ],
      "sum": [
        {
          "field": "vector",
          "feature": [
            "features..."
          ],
          "boost": 1,
          "min_score": 0.7
        }
      ]
    },
    "sort": [
      {
        "int": {
          "order": "asc"
        }
      }
    ],
    "fields": [
      "int",
      "float"
    ]
  },
  {
    "size": 3,
    "query": {
      "filter": [
        {
          "term": {
            "string_tags": [
              "10",
              "2"
            ],
            "operator": "or"
          }
        },
        {
          "term": {
            "string_tags": [
              "60"
            ],
            "operator": "or"
          }
        },
        {
          "term": {
            "string_tags": [
              "11"
            ],
            "operator": "or"
          }
        }
      ],
      "sum": [
        {
          "field": "vector",
          "feature": [
            "features..."
          ],
          "boost": 1,
          "min_score": 0.9
        }
      ]
    },
    "sort": [
      {
        "float": {
          "order": "desc"
        }
      }
    ],
    "fields": [
      "int",
      "float"
    ]
  }
]
' {{ROUTER}}/test_vector_db/vector_space/_bulk_search
````
### get vectors by ids and param search
````$xslt
curl -H "content-type: application/json" -XPOST -d'
{
  "size": 50,
  "query": {
    "filter": [
      {
        "term": {
          "operator": "and",
          "string_tags": [
            "10",
            "2"
          ]
        }
      },
      {
        "term": {
          "operator": "or",
          "string_tags": [
            "50",
            "12"
          ]
        }
      },
      {
        "term": {
          "operator": "not",
          "string_tags": [
            "100"
          ]
        }
      },
      {
        "term": {
          "operator": "and",
          "string_tags": [
            "101"
          ]
        }
      },
      {
        "term": {
          "operator": "or",
          "string_tags": [
            "99",
            "98"
          ]
        }
      }
    ],
    "ids": [
      "123"
    ],
    "sum": [
      {
        "field": "vector",
        "feature": [
          "features..."
        ],
        "boost": 1,
        "min_score": 0.7
      }
    ]
  },
  "fields": [
    "int"
  ]
}
' {{ROUTER}}/test_vector_db/vector_space/_query_byids_feature
````

### delete by query
````$xslt
# search
curl -H "content-type: application/json" -XPOST -d'
[
  {
    "size": 6,
    "query": {
      "filter": [
        {
          "term": {
            "string_tags": [
              "28",
              "2",
              "29"
            ],
            "operator": "or"
          }
        },
        {
          "term": {
            "string_tags": [
              "30"
            ],
            "operator": "or"
          }
        },
        {
          "term": {
            "string_tags": [
              "10"
            ],
            "operator": "or"
          }
        }
      ],
      "sum": [
        {
          "field": "vector",
          "feature": [
            "features..."
          ],
          "boost": 1,
          "min_score": 0.7
        }
      ]
    },
    "sort": [
      {
        "int": {
          "order": "asc"
        }
      }
    ],
    "fields": [
      "int",
      "float"
    ]
  },
  {
    "size": 3,
    "query": {
      "filter": [
        {
          "term": {
            "string_tags": [
              "10",
              "2"
            ],
            "operator": "or"
          }
        },
        {
          "term": {
            "string_tags": [
              "60"
            ],
            "operator": "or"
          }
        },
        {
          "term": {
            "string_tags": [
              "11"
            ],
            "operator": "or"
          }
        }
      ],
      "sum": [
        {
          "field": "vector",
          "feature": [
            "features..."
          ],
          "boost": 1,
          "min_score": 0.9
        }
      ]
    },
    "sort": [
      {
        "float": {
          "order": "desc"
        }
      }
    ],
    "fields": [
      "int",
      "float"
    ]
  }
]
' {{ROUTER}}/test_vector_db/vector_space/_delete_by_query

Versions prior to v3.3.0 support passing in vector

curl -H "content-type: application/json" -XPOST -d'
{
  "query": {
    "sum": [
      {
        "field": "vector",
        "feature": [
          "features..."
        ]
      }
    ],
    "filter": [
      {
        "range": {
          "int": {
            "gte": 1,
            "lte": 1000
          }
        }
      },
      {
        "term": {
          "string_tags": [
            "28",
            "2",
            "29"
          ],
          "operator": "or"
        }
      }
    ]
  },
  "size": 10
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
> if cluster table creating is crashed, it will has lock, now you need clean lock use this api

### server list
````$xslt
curl -XGET {{ROUTER}}/list/server
````

### server stats
````$xslt
curl -XGET {{ROUTER}}/_cluster/stats
````


### server health
````$xslt
curl -XGET {{ROUTER}}/_cluster/health
````

#### router cache info
````$xslt
curl -XGET {{ROUTER}}/_cache_info?db_name=test_vector_db&space_name=vector_space
````
