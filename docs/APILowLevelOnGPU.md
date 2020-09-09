# Low-level API for vector search on GPU

Most API are same to APILowLevel.md, here only list the different places.
* **search size** and **nprobe** should not larger than 1024(CUDA >= 9.0) or 2048(CUDA >= 9.2).
* Since GPU index does not support real time indexing, index_size should **set 0** to prevent auto indexing. After add documents, you should call `create index` `curl -XPOST {{ROUTER}}/test_vector_db/vector_space/_forcemerge` to build index.

* Search is not supported while add or indexing.
* GPU memory need 2GiB at least.

## space

----

### create space

You should set `retrieval_type : "GPU"`.

````$xslt
curl -v --user "root:secret" -H "content-type: application/json" -XPUT -d'
{
	"name": "vector_space",
	"dynamic_schema": "strict",
	"partition_num": 1,
	"replica_num": 1,
	"engine": {
		"name": "gamma",
		"index_size": 0,
		"max_size": 100000,
        "retrieval_type": "GPU",
		"retrieval_param": {
			"ncentroids": 1024,
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

* engine
* max_size : max documents for each partition
* index_size : **index_size should set 0**
* nprobe : should not larger than 1024(CUDA >= 9.0) or 2048(CUDA >= 9.2), you can set it when at search time
* keyword

* Vector field params
