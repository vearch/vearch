# Low-level API for vector search on GPU

Most API are same to [Vearch Documents](https://vearch.readthedocs.io/en/latest/), here only list the different places.
* **search size** and **nprobe** should not larger than 2048(CUDA >= 9.2).
* Since GPU index does not support real time indexing, index_size should **set 0** to prevent auto indexing. After add documents, you should call `curl -XPOST -H "content-type: application/json" -d '{"db_name":"test_vecot_db", "space_name":"vector_space"}' '${VEARCH_HOST}:${VEARCH_PORT}/index/forcemerge` to build index.

* Search is not supported while add or indexing.
* GPU memory need 2GiB at least.

## space

----

### create space

You should set `index_type : "GPU"`.

````$xslt
curl -v --user "root:secret" -H "content-type: application/json" -XPUT -d'
{
  "name": "vector_space",
  "partition_num": 1,
  "replica_num": 1,
  "index": {
    "index_type": "GPU",
    "index_params": {
      "metric_type": "L2",
      "ncentroids": 1024,
      "nsubvector": -1
    }
  },
  "fields": {
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
      "dimension": 128,
      "format": "normalization"
    },
    "string_tags": {
      "type": "string",
      "array": true,
      "index": true
    }
  }
}
' ${VEARCH_HOST}:${VEARCH_PORT}/dbs/test_vector_db/spaces
````

* engine
* index_size : **index_size should set 0**
* nprobe : should not larger than 2048(CUDA >= 9.2), you can set it when at search time
* keyword

* Vector field params
