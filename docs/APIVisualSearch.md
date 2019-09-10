# Vearch Plugin API

Vearch Plugin is aimed to build a simple and fast image retrieval system. Through this system, you can easily build your own image retrieval system, including image object detection,  feature extraction and similarity search. This API demonstrates how to use it.


## Create a database and space
The first step is to create a database and space, a database can have many spaces. A space is just like a table in database, you can define your data structure. The name of database and space are defined in the request path. In the path of request, `ip` is the address of your service, `port` is the port your service run on, default value is 4101, you can change this  by modifying the value of port defined in config/config.py. `database` and `space` are simply defined as "test" in the API. In the body of request,the parameter of db is used to distinguish whether to create a new database and space or only create a new space in this database. The value of `db` is true or false; `columns` is a dict mapping the fields in this space, `imageurl`, `boundingbox`  and `label` are required in each space, you can add other filed such as `sku` in `columns` ; `feature` is defined to assign a model to extract feature. Specifically as follows:

**HTTP request**

```shell
POST http://{ip}:{port}/{database}/{space}/_create
```

path parameters:

| Parameter | Type   | Explain                           |
| --------- | ------ | --------------------------------- |
| ip        | string | the address of service            |
| port      | string | the port of service               |
| database  | string | the name of db you want create    |
| space     | string | the name of space you want create |



**Request body**

| Parameter | Type   | Required | Default | Explain                                          |
| --------- | ------ | -------- | ------- | ------------------------------------------------ |
| db        | bool   | no       | true    | whether to create DB                             |
| columns   | dict   | yes      |         | the filed in the space                           |
| feature   | dict   | yes      |         | define  a model to extract features from a field |

columns parameters:
The columns is a dictionary, mapping the type of each field in your data. The value of each type should be one of ["keyword", "int", "float", "vector", "null", "text", "bool", "date"]. `imageurl`,`boundingbox` and `label` are required, if you only have the url of image, You can delete this field and the system will generate one by default.


| Parameter   | Type | Required | Explain                                                      |
| ----------- | ---- | -------- | ------------------------------------------------------------ |
| imageurl    | dict | yes      | image URI for an publicly accessible online image or image path stored in images folders |
| boundingbox | dict | yes      | the boundingbox of the subject of the image.                 |
| label       | dict | yes      | the categories of  image                                     |
| sku         | dict | no       | other field in your data                                     |


feature parameters


| Parameter | Type    | Required | Explain                        |
| --------- | ------- | -------- | ------------------------------ |
| type      | string  | yes      | the type of feature            |
| filed     | string  | yes      | the filed need extract feature |
| model_id  | string  | yes      | the model to extract feature   |
| dimension | integer | yes      | the dimension of  feature      |

**Response body**

| Parameter | Type    | Explain                           |
| --------- | ------- | --------------------------------- |
| code      | integer | the code of result,200 is success |
| db_msg    | string  | the message of creating database  |
| space_msg | string  | the message of creating space     |

**Example**

In this example, we create a database and space ,both name of them are "test". 
```shell
curl -XPOST -H "content-type:application/json" -d '{
    "db": true,
    "method": "innerproduct",
    "columns": {
        "imageurl": {
            "type": "keyword"
        },
        "boundingbox": {
            "type": "keyword"
        },
        "label": {
            "type": "keyword"
        }
    },
    "feature": {
        "type": "vector",
        "filed": "imageurl",
        "model_id": "vgg16",
        "dimension": 512
    }
}' http://{ip}:{port}/test/test/_create
```

A successful response like this:

```shell
{"code": 200, "db_msg": "success", "space_msg": "success"}
```



## Delete a database and space

If your database or space is created incorrectly, or you just don't want it, you can delete it by this way. The parameters in request path is same as above. You can delete both database and space or just delete space. The values of `db` and `space` are true or false.

**HTTP request**

```shell
POST http://{ip}:{port}/{database}/{space}/_delete
```

`path` parameters same like above.

**Request body**

| Parameter | Type | Required | Explain                 |
| --------- | ---- | -------- | ----------------------- |
| db        | bool | yes      | whether to delete DB    |
| space     | bool | yes      | whether to delete space |

**Response body**

| Parameter | Type    | Explain                           |
| --------- | ------- | --------------------------------- |
| code      | integer | the code of result,200 is success |
| db_msg    | string  | the message of deleting database  |
| space_msg | string  | the message of deleting space     |

**Example**

In this example, we only delete space.
```shell
curl -XPOST -H "content-type:application/json" -d '{
    "db": false,
    "space": true
}' http://{ip}:{port}/test/test/_delete
```

A successful response like this:

```shell
{"code": 200, "db_msg": "success", "space_msg": "success"}
```



## Insert data into space

After creating space, you can import data into space. There are two ways to import,single or batch. You can control single import or batch import by parameter `method`. `imageurl` is an image URI for an publicly accessible online image or an image path stored in images local folder ,when `method` is "single". Otherwise, `imageurl` is the local path of csv file which including lots of image URIs. If you want to detect the subject of the image, you can set the parameter `detection`  true, otherwise false. If you set it true, the bounding box and label of image will be added when inserting records.

**HTTP request**

```shell
POST http://{ip}:{port}/{database}/{space}/_insert
```

`path` parameters same like above.

**Request body**

| Parameter   | Type   | Required | Explain                                      |
| ----------- | ------ | -------- | -------------------------------------------- |
| method      | string | yes      | single or batch import                       |
| imageurl    | string | yes      | image URI or csv file path, required if method is single      |
| detection   | bool   | no       | whether object detection is required         |
| boundingbox | string | no       | the boundingbox of the subject of the image. |
| label       | string | no       | the categories of  image                     |

`boundingbox` and `label` should exist together, `detection` should be false at the same time. If `detection` is true, `boundingbox` and `label` will be overrided.  If you do not need object detect or crop image,you can set `boundingbox` and `label` to None, and  `detection` to false .

**Response body**

| Parameter  | Type    | Explain                                       |
| ---------- | ------- | --------------------------------------------- |
| db         | string  | the name of database                          |
| space      | string  | the name of space                             |
| ids        | dict    | a dict mapping index is successful or failed. |
| successful | integer | the number of successful record               |

**Example**

 The method of single import demo:
```shell
# single insert
curl -XPOST -H "content-type:application/json" -d '{
    "imageurl": "images/test/COCO_val2014_000000123599.jpg",
    "detection": false,
    "boundingbox": "10,10,290,290",
    "label": "coat"
}' http://{ip}:{port}/test/test/_insert

```

The method of batch import demo:

```shell
# batch insert
curl -XPOST -H "content-type:application/json" -d '{
    "method": "batch",
    "imageurl": "./images/test.csv",
    "detection": true
}' http://{ip}:{port}/test/test/_insert

```

A successful response like this:

```shell
# response
{
    "db": "test",
    "space": "test",
    "ids": [
        {
            "AWz2IRAvJG6WicwQVToi": "successful"
        },
        {
            "AWz2IRFVJG6WicwQVTom": "successful"
        },
        {
            "AWz2IRJyJG6WicwQVToq": "successful"
        },
        {
            "AWz2IRDEJG6WicwQVTok": "successful"
        },
        {
            "AWz2IRHlJG6WicwQVToo": "successful"
        },
        {
            "AWz2IRMGJG6WicwQVTos": "successful"
        },
        {
            "AWz2IRQpJG6WicwQVTow": "successful"
        },
        {
            "AWz2IROaJG6WicwQVTou": "successful"
        },
        {
            "AWz2IRTQJG6WicwQVToy": "successful"
        },
        {
            "AWz2IRVhJG6WicwQVTo0": "successful"
        }
    ],
    "successful": 10
}
```


## Get record by ID

You can use the ids returned above to check the status of the bulk import operation.

**HTTP request**

```shell
GET http://{ip}:{port}/{database}/{space}/${id}
```

`path` parameters same like above, `id` is unique id of record  .

**Response body**

| Parameter | Type    | Explain                              |
| --------- | ------- | ------------------------------------ |
| _index    | string  | the name of db                       |
| _type     | string  | the name of space                    |
| _id       | string  | record unique ID                     |
| found     | bool    | true or false                        |
| _version  | integer | the version of this record           |
| _source   | dict    | a dict mapping all fields in a space |

**Example**

```shell
# request
curl -XGET http://{ip}:{port}/test/test/AWz2IFBSJG6WicwQVTog

```

A successful response like this:

```shell
{
    "_index": "test",
    "_type": "test",
    "_id": "AWz2IFBSJG6WicwQVTog",
    "found": true,
    "_version": 1,
    "_source": {
        "boundingbox": '232,204,436,406',
        "imageurl": "images/test/COCO_val2014_000000123599.jpg",
        "label": "zebra"
    }
}
```

## Delete record by ID

If you want delete a record and you know the unique id of this record, you can delete this record by this method.

**HTTP request**

```shell
DELETE http://{ip}:{port}/{database}/{space}/${id}
```

`path` parameters same like above, `id` is unique id of record  .

**Response body**

| Parameter | Type    | Explain                      |
| --------- | ------- | ---------------------------- |
| code      | integer | the status code of result    |
| msg       | string  | the status message of reuslt |

**Example**

In this example, we delete the record whose id is AWz2IFBSJG6WicwQVTog in test space.

```shell
# request
curl -XDELETE http://{ip}:{port}/test/test/AWz2IFBSJG6WicwQVTog

```

A successful response like this:

```shell
# response
{
    "code": 200,
    "msg": "success"
}
```


## Update record by ID

If you want update a record and you know the unique id of this record, you can update this record by this method. The body of request is same to body of request when inserting a record.

**HTTP request**

```shell
POST http://{ip}:{port}/{database}/{space}/_update?id=${id}
```

`path` parameters same like above, `id` is unique id of record  .

**Request body**

same as Insert data into space

**Response body**

same as Insert data into space

**Example**

```shell
curl -XPOST -H "content-type:application/json" -d '{
    "imageurl": "images/test/COCO_val2014_000000123599.jpg",
    "detection": true
}' http://{ip}:{port}/test/test/_update?id=AWz2IFBSJG6WicwQVTog
```

A successful response like this:

```shell
# response
{
    "db": "test",
    "space": "test",
    "ids": [
        {
            "AWz2IFBSJG6WicwQVTog": "successful"
        }
    ],
    "successful": 1
}
```


## Search similar result from space 

You can search using a base64 encoded local image, or use an image URI for an publicly accessible online image or an image stored in a local path such as the image below.

![images/COCO_val2014_000000123599.jpg](plugin/images/COCO_val2014_000000123599.jpg)

**HTTP request**

```shell
POST http://{ip}:{port}/{database}/{space}/_search
```

`path` parameters same like above.

**Request body**

| Parameter   | Type    | Required | Explain                              |
| ----------- | ------- | -------- | ------------------------------------ |
| imageurl    | string  | yes      | image URI or base64 encoded image    |
| detection   | string  | no       | whether object detection is required |
| boundingbox | string  | no       | the boundingbox of the image.        |
| size        | integer | no       | the num of  search result            |
| filter      | list    | no       | the fileds required filter           |
| label       | string  | no       | the categories of  image             |

**Response body**

just like following

**Example**

Search using an image stored in images folders or image URI on Internet 

```shell
# request
curl -XPOST -H "content-type:application/json" -d '{
    "imageurl": "images/test/COCO_val2014_000000123599.jpg",
    "detection": true,
    "score": 0.5,
    "filter": [
        {
            "term": {
                "label": {
                    "value": "zebra"
                }
            }
        }
    ],
    "size": 5
}' http://{ip}:{port}/test/test/_search


```

Search using an base64 encoded image 

```shell
curl -XPOST -H "content-type:application/json" -d '{
    "imageurl": "$(base64 -w 0 images/test/COCO_val2014_000000123599.jpg)"
}'http://{ip}:{port}/test/test/_search
```

A successful response

```shell
{
    'took': 9,
    'timed_out': False,
    '_shards': {
        'total': 1,
        'failed': 0,
        'successful': 1
    },
    'hits': {
        'total': 14068,
        'max_score': 0.9999999403953552,
        'hits': [
            {
                '_index': 'test',
                '_type': 'test',
                '_id': 'AWz_ciUvJG6WicwQQgXP',
                '_score': 0.9999999403953552,
                '_extra': {
                    'vector_result': [
                        {
                            'field': 'feature',
                            'source': 'images/test/COCO_val2014_000000123599.jpg',
                            'score': 0.9999999403953552
                        }
                    ]
                },
                '_version': 1,
                '_source': {
                    'boundingbox': '35,
                    67,
                    546,
                    556',
                    'imageurl': 'images/test/COCO_val2014_000000123599.jpg',
                    'label': 'zebra'
                }
            },
            {
                '_index': 'test',
                '_type': 'test',
                '_id': 'AWz_bVB0JG6WicwQQfbh',
                '_score': 0.9116669297218323,
                '_extra': {
                    'vector_result': [
                        {
                            'field': 'feature',
                            'source': 'images/test/COCO_val2014_000000095375.jpg',
                            'score': 0.9116669297218323
                        }
                    ]
                },
                '_version': 1,
                '_source': {
                    'boundingbox': '34,
                    320,
                    208,
                    427',
                    'imageurl': 'images/test/COCO_val2014_000000095375.jpg',
                    'label': 'zebra'
                }
            },
            {
                '_index': 'test',
                '_type': 'test',
                '_id': 'AWz_ZKBpJG6WicwQQdxL',
                '_score': 0.9110268354415894,
                '_extra': {
                    'vector_result': [
                        {
                            'field': 'feature',
                            'source': 'images/test/COCO_val2014_000000045535.jpg',
                            'score': 0.9110268354415894
                        }
                    ]
                },
                '_version': 1,
                '_source': {
                    'boundingbox': '218,
                    171,
                    494,
                    346',
                    'imageurl': 'images/test/COCO_val2014_000000045535.jpg',
                    'label': 'zebra'
                }
            },
            {
                '_index': 'test',
                '_type': 'test',
                '_id': 'AWz_XbS-JG6WicwQQccF',
                '_score': 0.9091571569442749,
                '_extra': {
                    'vector_result': [
                        {
                            'field': 'feature',
                            'source': 'images/test/COCO_val2014_000000007522.jpg',
                            'score': 0.9091571569442749
                        }
                    ]
                },
                '_version': 1,
                '_source': {
                    'boundingbox': '232,
                    204,
                    436,
                    406',
                    'imageurl': 'images/test/COCO_val2014_000000007522.jpg',
                    'label': 'zebra'
                }
            },
            {
                '_index': 'test',
                '_type': 'test',
                '_id': 'AWz_dFBwJG6WicwQQgyR',
                '_score': 0.9059789180755615,
                '_extra': {
                    'vector_result': [
                        {
                            'field': 'feature',
                            'source': 'images/test/COCO_val2014_000000136077.jpg',
                            'score': 0.9059789180755615
                        }
                    ]
                },
                '_version': 1,
                '_source': {
                    'boundingbox': '288,
                    75,
                    624,
                    285',
                    'imageurl': 'images/test/COCO_val2014_000000136077.jpg',
                    'label': 'zebra'
                }
            },
            {
                '_index': 'test',
                '_type': 'test',
                '_id': 'AWz_YQzVJG6WicwQQdFf',
                '_score': 0.9054344296455383,
                '_extra': {
                    'vector_result': [
                        {
                            'field': 'feature',
                            'source': 'images/test/COCO_val2014_000000026174.jpg',
                            'score': 0.9054344296455383
                        }
                    ]
                },
                '_version': 1,
                '_source': {
                    'boundingbox': '191,
                    89,
                    506,
                    388',
                    'imageurl': 'images/test/COCO_val2014_000000026174.jpg',
                    'label': 'zebra'
                }
            },
            {
                '_index': 'test',
                '_type': 'test',
                '_id': 'AWz_ajGhJG6WicwQQe0v',
                '_score': 0.9029231071472168,
                '_extra': {
                    'vector_result': [
                        {
                            'field': 'feature',
                            'source': 'images/test/COCO_val2014_000000077479.jpg',
                            'score': 0.9029231071472168
                        }
                    ]
                },
                '_version': 1,
                '_source': {
                    'boundingbox': '110,
                    82,
                    525,
                    391',
                    'imageurl': 'images/test/COCO_val2014_000000077479.jpg',
                    'label': 'zebra'
                }
            },
            {
                '_index': 'test',
                '_type': 'test',
                '_id': 'AWz_Y3-bJG6WicwQQdjf',
                '_score': 0.9010977745056152,
                '_extra': {
                    'vector_result': [
                        {
                            'field': 'feature',
                            'source': 'images/test/COCO_val2014_000000039390.jpg',
                            'score': 0.9010977745056152
                        }
                    ]
                },
                '_version': 1,
                '_source': {
                    'boundingbox': '0,
                    141,
                    187,
                    290',
                    'imageurl': 'images/test/COCO_val2014_000000039390.jpg',
                    'label': 'zebra'
                }
            },
            {
                '_index': 'test',
                '_type': 'test',
                '_id': 'AWz_YJpVJG6WicwQQc_t',
                '_score': 0.8993719816207886,
                '_extra': {
                    'vector_result': [
                        {
                            'field': 'feature',
                            'source': 'images/test/COCO_val2014_000000023411.jpg',
                            'score': 0.8993719816207886
                        }
                    ]
                },
                '_version': 1,
                '_source': {
                    'boundingbox': '238,
                    89,
                    538,
                    390',
                    'imageurl': 'images/test/COCO_val2014_000000023411.jpg',
                    'label': 'zebra'
                }
            },
            {
                '_index': 'test',
                '_type': 'test',
                '_id': 'AWz_fDU9JG6WicwQQiTB',
                '_score': 0.8990296721458435,
                '_extra': {
                    'vector_result': [
                        {
                            'field': 'feature',
                            'source': 'images/test/COCO_val2014_000000180363.jpg',
                            'score': 0.8990296721458435
                        }
                    ]
                },
                '_version': 1,
                '_source': {
                    'boundingbox': '36,
                    88,
                    335,
                    310',
                    'imageurl': 'images/test/COCO_val2014_000000180363.jpg',
                    'label': 'zebra'
                }
            }
        ]
    }
}
```

search result look like this

![docs/plugin/images/COCO_val2014_000000123599.jpg](docs/plugin/images/COCO_val2014_000000123599.jpg)

![docs/plugin/images/result.jpg](docs/plugin/images/result.jpg)
