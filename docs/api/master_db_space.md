## API 



#### list db 

Get http://11.3.170.164:443/list/db

* ok

````
{
	"code": 200,
	"msg": "success",
	"data": [{
		"id": 1,
		"name": "vector_db"
	}]
}
````

#### list space

Get http://11.3.170.164:443/list/space?db=vector_db

* ok

````
{
	"code": 200,
	"msg": "success",
	"data": [{
		"id": 1,
		"name": "vector_space",
		"version": 2,
		"db_id": 1,
		"enabled": true,
		"partitions": [{
			"id": 1,
			"space_id": 1,
			"db_id": 1,
			"partition_slot": 0,
			"replicas": [29]
		}, {
			"id": 2,
			"space_id": 1,
			"db_id": 1,
			"partition_slot": 33294320,
			"replicas": [36]
		},....]
	}]
}
````


#### Create Databases

PUT 127.0.0.1:8817/db/_create

````
{
	"name":"ansj"
}
````



- Result ok

```
{
    "code": 200,
    "msg": "success"
}
```

* Result error

````
{
    "code": 500,
    "msg": "dbname vearch is exists"
}
````



#### Create Databases

GET 127.0.0.1:8817/db/_create

- Result ok

```
{
    "id": 5,
    "name": "ansj"
}
```

- Result error

```
{
    "code": 500,
    "msg": "db:[ansj] not exist"
}
```



#### Delete Databases

DELETE 127.0.0.1:8817/db/ansj

- Result ok

```
{
    "code": 200,
    "msg": "success"
}
```

- Result error

```
{
    "code": 500,
    "msg": "delete db has err"
}
```



#### Create Space

PUT 127.0.0.1:8817/space/ansj/_create

```
{
	"name":"ansj"
}
```



- Result ok

```
{
    "code": 200,
    "msg": "success"
}
```

- Result error

```
{
    "code": 500,
    "msg": "dbname baud1 is exists"
}
```


#### Delete Space

DELETE 127.0.0.1:8817/space/ansj/ansj_table
