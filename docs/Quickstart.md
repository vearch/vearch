# Quickstart

Vearch is aimed to build a simple and fast image retrieval system. Through this system, you can easily build your own image retrieval system, including image object detection,  feature extraction and similarity search. This quickstart demonstrates how to use it.




## Before you begin

1. Deploy Vearch system referred to [Deploy.md](Deploy.md).
2. Download the [weight](https://pjreddie.com/media/files/yolov3.weights) of object detect model in model/image_detect folder.

 And you can download  [coco data](https://pjreddie.com/media/files/val2014.zip) for testing, or  use the images in images folder we choose from [coco data](https://pjreddie.com/media/files/val2014.zip).


## Deploy your own plugin service

This requires only two operations:

1. Modify parameters `ip_address` in `config/config.py`;
2. Execution script `run.sh`;



## Create a database and space

Before inserting and searching, you should create a database and space. Use the following `curl` command to create a new database and space.

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

A successful response looks like this:

```shell
{"code": 200, "db_msg": "success", "space_msg": "success"}
```



## Delete a database and space

If you want delete a database and space. Use the following `curl` command to delete a database and space

```shell
curl -XPOST -H "content-type:application/json" -d '{
    "db": false,
    "space": true
}' http://{ip}:{port}/test/test/_delete
```

A successful response looks like this:

```shell
{"code": 200, "db_msg": "success", "space_msg": "success"}
```



## Insert data into space

We support both single and bulk imports. Use the following `curl` command to insert single data into space.

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

The method of bulk import demo:

```shell
# bulk insert
curl -XPOST -H "content-type:application/json" -d '{
    "method": "bulk",
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

Use the following `curl` command to get a record by ID

```shell
# request
curl -XGET http://{ip}:{port}/test/test/AWz2IFBSJG6WicwQVTog

# response
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

Use the following `curl` command to delete a record by ID

```shell
# request
curl -XDELETE http://{ip}:{port}/test/test/AWz2IFBSJG6WicwQVTog

# response
{
    "code": 200,
    "msg": "success"
}
```



## Update record by ID

Use the following `curl` command to update a record by ID

```shell
# request
curl -XPOST -H "content-type:application/json" -d '{
    "imageurl": "images/test/COCO_val2014_000000123599.jpg",
    "detection": true
}' http://{ip}:{port}/test/test/_update?id=AWz2IFBSJG6WicwQVTog
    
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

You can search using a base64 encoded local image, or use an image URI for an publicly accessible online image or an image stored in images folders.

Search using an image stored in images folders or image URI on Internet. Use the following `curl` command to search  similar result from space

```shell
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

or you can simply use the following `curl` command:

```shell
curl -XPOST -H "content-type:application/json" -d '{
    "imageurl": "images/test/COCO_val2014_000000123599.jpg"
}' http://{ip}:{port}/test/test/_search
```

A successful response looks like this:

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

![docs/img/plugin/COCO_val2014_000000123599.jpg](img/plugin/COCO_val2014_000000123599.jpg)

![docs/img/plugin/result.jpg](img/plugin/result.jpg)

