# VisualSearch API

VisualSearch API is dedicated to building a simple and fast image retrieval system. Through this system, you can easily build your own image retrieval system, including object detection, feature extraction and similarity search.This API demonstrates how to use it.



[TOC]



## Create a database and space
The first step is to create a database and space, each database can have a lot of space, space is where you store data.So you should determine which fields are needed in the space by your own data format. The name of database and space are defined in request uri. In the path of request,`ip` is the address of your service, `port` is the port your service run on, default value is 4101, you can change this  by modifying the value of port defined in config/config.py.`database` and `space` are simply defined as "test" in the API.In the body of request,the parameter of db is used to distinguish whether to create a new database and space or create a new space in this database,the value of `db` is "yes" or "no";`columns` is a dict mapping the fields in this space,`imageurl`,`boundingbox` and `label` are required in each space,you can add other filed such as `sku` in `columns` ; `feature` is defined to assign a model to extract feature.Specifically as follows:

#### HTTP request

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



#### Request body

| Parameter | Type   | Required | Default | Explain                                          |
| --------- | ------ | -------- | ------- | ------------------------------------------------ |
| db        | string | no       | yes     | whether to create DB                             |
| columns   | dict   | yes      |         | the filed in the space                           |
| feature   | dict   | yes      |         | define  a model to extract features from a field |


columns parameters


| Parameter   | Type   | Required | Explain                                                      |
| ----------- | ------ | -------- | ------------------------------------------------------------ |
| imageurl    | string | yes      | image URI for an publicly accessible online image or image path stored in images folders |
| boundingbox | string | yes      | the boundingbox of the subject of the image.                 |
| label       | string | yes      | the categories of  image                                     |
| id          | string | no       | other field in the space                                     |


feature parameters


| Parameter | Type    | Required | Explain                        |
| --------- | ------- | -------- | ------------------------------ |
| filed     | string  | yes      | the filed need extract feature |
| model_id  | string  | yes      | the model to extract feature   |
| dimension | integer | yes      | the dimension of  feature      |

#### Response body

| Parameter | Type    | Explain                           |
| --------- | ------- | --------------------------------- |
| code      | integer | the code of result,200 is success |
| db_msg    | string  | the message of creating database  |
| space_msg | string  | the message of creating space     |

#### Example
In this example, we create a database and space ,both name of them are "test". Defined the method of indexing is innerproduct. There are three fileds in this space,imageurl,boundingbox and label. The type of fileds are all string.And assign vgg16 model to extract feature.
```shell
curl  -X  POST \
    -H "Content-Type: application/json" \
    -d '{
            "db":"yes",
            "method":"innerproduct",
            "columns":{
                    "imageurl": "str",
                    "boundingbox": "str",
                    "label": "str"
            },
            "feature":{
                    "filed": "imageurl",
                    "model_id": "vgg16",
                    "dimension": 512
            }
        }' \
    http://{ip}:{port}/test/test/_create
```

A successful response like this:

```shell
{"code": 200, "db_msg": "success", "space_msg": "success"}
```



## Delete a database and space

If your database or space is created incorrectly, or you just don't want it, you can delete it in this way.The parameters in request path is same as above.You can delete both database and space or just delete space.The values of `db` and `space` are "yes" or "no".

#### HTTP request

```shell
POST http://{ip}:{port}/{database}/{space}/_delete
```

`path` parameters same like above.

#### Request body

| Parameter | Type   | Required | Explain                 |
| --------- | ------ | -------- | ----------------------- |
| db        | string | yes      | whether to delete DB    |
| space     | string | yes      | whether to delete space |

#### Response body

| Parameter | Type    | Explain                           |
| --------- | ------- | --------------------------------- |
| code      | integer | the code of result,200 is success |
| db_msg    | string  | the message of deleting database  |
| space_msg | string  | the message of deleting space     |

#### Example

In this example, we only delete space.
```shell
curl  -X  POST \
    -H "Content-Type: application/json" \
    -d '{
        "db":"no",
        "space":"yes"
        }' \
    http://{ip}:{port}/{database}/{space}/_delete
```

A successful response like this:

```shell
{"code": 200, "db_msg": "success", "space_msg": "success"}
```



## Insert data into space

After creating space, you can import data into space.There are two ways to import,single or batch.You can control single import or batch import by parameter `method`. `imageurl` is an image URI for an publicly accessible online image or an image stored in images local folder ,when `method` is "single". Otherwise, `imageurl` is the local path of csv file which including lots of image URIs. If you want to detect the subject of the image, you can set the parameter `detection` to "yes", otherwise set it to "no".If you set it to "yes", the checked boxes and categories of image will be added when inserting records.

#### HTTP request

```shell
POST http://{ip}:{port}/{database}/{space}/_insert
```

`path` parameters same like above.

#### Request body

| Parameter   | Type   | Required | Explain                                      |
| ----------- | ------ | -------- | -------------------------------------------- |
| method      | string | yes      | single or batch import                       |
| imageurl    | string | no       | image URI or csv file path, required if method is single      |
| detection   | string | no       | whether object detection is required         |
| boundingbox | string | no       | the boundingbox of the subject of the image. |
| label       | string | no       | the categories of  image                     |

`boundingbox` and `label` should exist together, `detection` should be "no" at the same time.If `detection` is "yes", `boundingbox` and `label` will be overrided.If you do not want object detect or crop image,you can set `boundingbox` and `label` to None, and  `detection` to "no" .

#### Response body

| Parameter  | Type    | Explain                                                      |
| ---------- | ------- | ------------------------------------------------------------ |
| db         | string  | the name of database                                         |
| space      | string  | the name of space                                            |
| ids        | dict    | the index of each record,key of dict is index,value of dict is 0 or 1. |
| successful | integer | the num of successful record                                 |

#### Example

 The method of single import demo:
```shell
# single insert
curl  -X  POST \
    -H "Content-Type: application/json" \
    -d '{
            "method":"single",
            "imageurl":"./images/test/89552151.jpg",
            "detection":"no",
            "boundingbox":"10,10,290,290",
            "label":"shoes"
        }' \
    http://{ip}:{port}/{database}/{space}/_insert

```

The method of batch import demo:

```shell
# batch insert
curl  -X  POST \
    -H "Content-Type: application/json" \
    -d '{
            "method":"batch",
            "imagefile":"./images/test.csv",
            "detection":"yes"
        }' \
    http://{ip}:{port}/{database}/{space}/_insert

```

A successful response like this:

```shell
# response
{
    "db":"test",
    "space":"test",
    "ids":["AWzWk_fmJG6WicwQJoiS"],
    "successful":1
}
```


## Get record by ID

You can use the ids returned above to check the status of the bulk import operation.

#### HTTP request

```shell
GET http://{ip}:{port}/{database}/{space}/${id}
```

`path` parameters same like above, `id` is unique id of record  .

#### Response body

| Parameter | Type   | Explain                                 |
| --------- | ------ | --------------------------------------- |
| id        | string | record unique ID                        |
| db        | string | the name of db                          |
| space     | string | the name of space                       |
| content   | dict   | a dict mapping all fields in a database |

#### Example

```shell
# request
curl  -X  GET \
    -H "Content-Type: application/json"  \
    http://{ip}:{port}/test/test/AWzXSaP6JG6WicwQiO9X

```

A successful response like this:

```shell
# response
{
    "db": "test",
    "space": "test",
    "id": "AWzXSaP6JG6WicwQiO9X",
    "content": {
        "imageurl": "./images/test/89552151.jpg",
        "label": "bag",
        "loc": "4,53,300,145"
    }
}
```

## Delete record by ID

If you want delete a record and you know the unique id of this record, you can delete this record by this method.

#### HTTP request

```shell
DELETE http://{ip}:{port}/{database}/{space}/${id}
```

`path` parameters same like above, `id` is unique id of record  .

#### Response body

| Parameter | Type    | Explain                      |
| --------- | ------- | ---------------------------- |
| code      | integer | the status code of result    |
| msg       | string  | the status message of reuslt |

#### Example

In this example, we delete the record whose id is AWzXSaP6JG6WicwQiO9X in test space.

```shell
# request
curl  -X  DELETE \
    -H "Content-Type: application/json" \
    http://{ip}:{port}/test/test/AWzXSaP6JG6WicwQiO9X

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

#### HTTP request

```shell
POST http://{ip}:{port}/{database}/{space}/_update?id=${id}
```

`path` parameters same like above, `id` is unique id of record  .

#### Request body

same as Insert data into space

#### Response body

same as Insert data into space

#### Example

```shell
# request
## single insert
curl  -X  POST \
    -H "Content-Type: application/json" \
    -d '{
            "method":"single",
            "imageurl":"./images/test/89552151.jpg",
            "detection":"no",
            "boundingbox":"10,10,290,290",
            "label":"shoes"
        }' \
    http://{ip}:{port}/{database}/{space}/_update?id=AWzWk_fmJG6WicwQJoiS
```

A successful response like this:

```shell
# response
{
    "db":"test",
    "space":"test",
    "ids":["AWzWk_fmJG6WicwQJoiS"],
    "successful":1,
}
```


## Search similar result from space 

You can search using a base64 encoded local image, or use an image URI for an publicly accessible online image or an image stored in a local path such as the image below.

![docs/img/93396423.jpg](docs/img/93396423.jpg)

#### HTTP request

```shell
POST http://{ip}:{port}/{database}/{space}/_search
```

`path` parameters same like above.

#### Request body

| Parameter   | Type    | Required | Explain                              |
| ----------- | ------- | -------- | ------------------------------------ |
| imageurl    | string  | yes      | image URI or base64 encoded image    |
| detection   | string  | no       | whether object detection is required |
| boundingbox | string  | no       | the boundingbox of the image.        |
| size        | integer | no       | the num of  search result            |
| filter      | dict    | no       | the fileds required filter           |
| label       | string  | no       | the categories of  image             |

#### Response body

just like following

#### Example

Search using an image stored in images folders or image URI on Internet 

```shell
# request
curl  -X  POST \
    -H "Content-Type: application/json" \
    -d '{
            "imageurl":"./images/89552151.jpg"
            "boundingbox":"0,0,300,300",
            "detection":"no",
            "size":10,
            "filter":{
                    "label":"bag",
                    "score":0.6
                    }
        }' \
    http://{ip}:{port}/{database}/{space}/_search


```

Search using an base64 encoded image 

```shell
curl -X POST \
    -H "Content-Type: application/json" \
    -d '{
            "image":"data:image/jpeg;base64,$(base64 -w 0 ./images/test/89552151.jpg)",
        }' \
    http://{ip}:{port}/{database}/{space}/_search
```

A successful response

```shell
{
    "took": 5,
    "timed_out": false,
    "_shards": {
        "total": 1,
        "failed": 0,
        "successful": 1
    },
    "hits": {
        "total": 67,
        "max_score": 1,
        "hits": [
            {
                "_index": "test",
                "_type": "test",
                "_id": "AWzXSZqLJG6WicwQiO8Z",
                "_score": 1,
                "_extra": {
                    "vector_result": [
                        {
                            "field": "feature",
                            "source": "./images/1132898.jpg",
                            "score": 1
                        }
                    ]
                },
                "_version": 1,
                "_source": {
                    "imageurl": "./images/1132898.jpg",
                    "label": "all",
                    "loc": "0,0,300,300"
                }
            },
            {
                "_index": "test",
                "_type": "test",
                "_id": "AWzXSZ61JG6WicwQiO81",
                "_score": 0.6554051637649536,
                "_extra": {
                    "vector_result": [
                        {
                            "field": "feature",
                            "source": "./images/77586349.jpg",
                            "score": 0.65540516376495361
                        }
                    ]
                },
                "_version": 1,
                "_source": {
                    "imageurl": "./images/77586349.jpg",
                    "label": "surfboard",
                    "loc": "14,44,283,146"
                }
            },
            {
                "_index": "test",
                "_type": "test",
                "_id": "AWzXSaljJG6WicwQiO95",
                "_score": 0.646151602268219,
                "_extra": {
                    "vector_result": [
                        {
                            "field": "feature",
                            "source": "./images/92833573.jpg",
                            "score": 0.646151602268219
                        }
                    ]
                },
                "_version": 1,
                "_source": {
                    "imageurl": "./images/92833573.jpg",
                    "label": "all",
                    "loc": "0,0,300,300"
                }
            },
            {
                "_index": "test",
                "_type": "test",
                "_id": "AWzXSaAgJG6WicwQiO89",
                "_score": 0.6207960844039917,
                "_extra": {
                    "vector_result": [
                        {
                            "field": "feature",
                            "source": "./images/83866084.jpg",
                            "score": 0.6207960844039917
                        }
                    ]
                },
                "_version": 1,
                "_source": {
                    "imageurl": "./images/83866084.jpg",
                    "label": "bottle",
                    "loc": "7,19,293,155"
                }
            },
            {
                "_index": "test",
                "_type": "test",
                "_id": "AWzXSZ8NJG6WicwQiO83",
                "_score": 0.603010892868042,
                "_extra": {
                    "vector_result": [
                        {
                            "field": "feature",
                            "source": "./images/78486959.jpg",
                            "score": 0.603010892868042
                        }
                    ]
                },
                "_version": 1,
                "_source": {
                    "imageurl": "./images/78486959.jpg",
                    "label": "suitcase",
                    "loc": "16,27,240,278"
                }
            },
            {
                "_index": "test",
                "_type": "test",
                "_id": "AWzXSaYhJG6WicwQiO9l",
                "_score": 0.6007768511772156,
                "_extra": {
                    "vector_result": [
                        {
                            "field": "feature",
                            "source": "./images/91939703.jpg",
                            "score": 0.60077685117721558
                        }
                    ]
                },
                "_version": 1,
                "_source": {
                    "imageurl": "./images/91939703.jpg",
                    "label": "suitcase",
                    "loc": "17,34,283,276"
                }
            },
            {
                "_index": "test",
                "_type": "test",
                "_id": "AWzXSarCJG6WicwQiO-B",
                "_score": 0.5973396301269531,
                "_extra": {
                    "vector_result": [
                        {
                            "field": "feature",
                            "source": "./images/93396423.jpg",
                            "score": 0.59733963012695312
                        }
                    ]
                },
                "_version": 1,
                "_source": {
                    "imageurl": "./images/93396423.jpg",
                    "label": "all",
                    "loc": "0,0,300,300"
                }
            },
            {
                "_index": "test",
                "_type": "test",
                "_id": "AWzXSaUGJG6WicwQiO9f",
                "_score": 0.5798431038856506,
                "_extra": {
                    "vector_result": [
                        {
                            "field": "feature",
                            "source": "./images/91116998.jpg",
                            "score": 0.57984310388565063
                        }
                    ]
                },
                "_version": 1,
                "_source": {
                    "imageurl": "./images/91116998.jpg",
                    "label": "all",
                    "loc": "0,0,300,300"
                }
            },
            {
                "_index": "test",
                "_type": "test",
                "_id": "AWzXSa3dJG6WicwQiO-T",
                "_score": 0.577214241027832,
                "_extra": {
                    "vector_result": [
                        {
                            "field": "feature",
                            "source": "./images/94254873.jpg",
                            "score": 0.577214241027832
                        }
                    ]
                },
                "_version": 1,
                "_source": {
                    "imageurl": "./images/94254873.jpg",
                    "label": "all",
                    "loc": "0,0,300,300"
                }
            },
            {
                "_index": "test",
                "_type": "test",
                "_id": "AWzXSaP6JG6WicwQiO9X",
                "_score": 0.5653480887413025,
                "_extra": {
                    "vector_result": [
                        {
                            "field": "feature",
                            "source": "./images/89552151.jpg",
                            "score": 0.56534808874130249
                        }
                    ]
                },
                "_version": 1,
                "_source": {
                    "imageurl": "./images/89552151.jpg",
                    "label": "surfboard",
                    "loc": "4,53,300,145"
                }
            }
        ]
    }
}
```

