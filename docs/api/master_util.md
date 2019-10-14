### Get Cluster stats

> http://11.3.170.164:443/_cluster/stats

````$json

[{
	"status": 200,
	"ip": "11.3.240.138",
	"labels": [{
		"name": "models",
		"value": "ps"
	}],
	"mem": {
		"total_in_bytes": 67132641280,
		"free_in_bytes": 57638010880,
		"used_in_bytes": 4693512192,
		"used_percent": 6.99140105693753
	},
	"swap": {
		"total_in_bytes": 17179865088,
		"free_in_bytes": 16711159808,
		"used_in_bytes": 468705280,
		"used_percent": 2.7282244511185767
	},
	"fs": {
		"total_in_bytes": 1199051247616,
		"free_in_bytes": 1194054438912,
		"used_in_bytes": 4996808704,
		"used_percent": 239,
		"paths": ["/export/Data/vearch/datas/", "/export/Logs/vearch/logs/"]
	},
	"cpu": {
		"total_in_bytes": 32,
		"user_percent": 0.396455253025388,
		"sys_percent": 0.5861961176374272,
		"io_wait_percent": 0.00012503516613878,
		"idle_percent": 0.012909880935221114
	},
	"net": {
		"in_pre_second": 54760803,
		"out_pre_second": 36932496,
		"connect": 1639
	},
	"gc": {
		"calls": 17325762,
		"routines": 674
	}
} 
......

]
````


----

### Get Cluster health

> http://11.3.170.164:443/_cluster/health

```$json
[{
	"db_name": "vector_db",
	"doc_num": 2138324,
	"errors": null,
	"size": 0,
	"space_num": 1,
	"spaces": [{
		"doc_num": 2138324,
		"name": "vector_space",
		"partition_num": 129,
		"partitions": [{
				"pid": 1,
				"doc_num": 16474,
				"replica_num": 1,
				"path": "/export/Data/vearch/datas/",
				"status": 4,
				"color": "green",
				"ip": "11.3.238.42",
				"node_id": 29
			},
			{
				"pid": 2,
				"doc_num": 16527,
				"replica_num": 1,
				"path": "/export/Data/vearch/datas/",
				"status": 4,
				"color": "green",
				"ip": "11.3.240.162",
				"node_id": 36
			},
			....
			{
				"pid": 128,
				"doc_num": 16608,
				"replica_num": 1,
				"path": "/export/Data/vearch/datas/",
				"status": 4,
				"color": "green",
				"ip": "11.3.230.105",
				"node_id": 21
			},
			{
				"pid": 129,
				"doc_num": 16488,
				"replica_num": 1,
				"path": "/export/Data/vearch/datas/",
				"status": 4,
				"color": "green",
				"ip": "11.3.238.42",
				"node_id": 29
			}
		],
		"replica_num": 129,
		"size": 0,
		"status": "green"
	}],
	"status": "green"
}]
```



### list server

> http://11.3.170.164:443/list/server

```$json
{
	"code": 200,
	"msg": "success",
	"data": {
		"servers": [{
			"name": 1,
			"rpc_port": 8081,
			"raft_heartbeat_port": 8898,
			"raft_replicate_port": 8899,
			"ip": "11.3.170.167",
			"p_ids": [73, 97, 41]
		},.....
		{
			"name": 9,
			"rpc_port": 8081,
			"raft_heartbeat_port": 8898,
			"raft_replicate_port": 8899,
			"ip": "11.3.214.233",
			"p_ids": [111, 67, 4]
		}],
		"count": 42
	}
}
```


#### clean cluster lock

> Get 127.0.0.1:8817/clean_lock

````$xslt
when you create table is hang or restart in creating table
{
    "code": 200,
    "msg": "success",
    "data": []
}
````