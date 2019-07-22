# Get BaudEngine release version

* first you can need download application for baudengine

````
wget http://xxxxxx
````

> you can run it , to get config param

````$xslt
./baudengine -f

  -conf string
    	baud config path (default "/Users/sunjian/go/src/github.com/tiglabs/baudengine/config/config.toml")
  -master string
    	baud config for master name , is on local start two master must use it
````  
* -conf is your config file path 
* -master is your master name, in this example like `m1`,`m2` . Usually you don't have to set it，it found master name by your local IP ,  but when you one machine startd two master you must tell it the master name like `-master m1`                                                                

# Setting up a Local Model

* make config file conf.toml

````
[global]
    # the name will validate join cluster by same name
    name = "baudengine"
    # you data save to disk path ,If you are in a production environment, You'd better set absolute paths
    data = "datas/"
    # log path , If you are in a production environment, You'd better set absolute paths
    log = "logs/"
    # default log type for any model
    level = "debug"
    # master <-> ps <-> router will use this key to send or receive data
    signkey = "baudengine"

# if you are master you'd better set all config for router and ps and router and ps use default config it so cool
[[masters]]
    #name machine name for cluster
    name = "m1"
    #ip or domain
    address = "127.0.0.1"
    # api port for http server
    api_port = 8817
    # port for etcd server
    etcd_port = 2378
    # listen_peer_urls List of comma separated URLs to listen on for peer traffic.
    # advertise_peer_urls List of this member's peer URLs to advertise to the rest of the cluster. The URLs needed to be a comma-separated list.
    etcd_peer_port = 2390
    # List of this member's client URLs to advertise to the public.
    # The URLs needed to be a comma-separated list.
    # advertise_client_urls AND listen_client_urls
    etcd_client_port = 2370

[router]
    # port for server
    port = 9001
    # skip auth for client visit data
    skip_auth = false

[ps]
    # port for server
    rpc_port = 8081
    #raft config begin
    raft_heartbeat_port = 8898
    raft_replicate_port = 8899
    heartbeat-interval = 200 #ms
    raft_retain_logs = 10000
    raft_replica_concurrency = 1
    raft_snap_concurrency = 1

```` 

* start it

````
./baudengine -conf conf.toml
````

> you will see like this ,it means you are got it 

````
2019-02-27 14:21:59.534520 I | etcdserver: published {Name:m1 ClientURLs:[http://127.0.0.1:2370]} to cluster 64c4741bb8d03782
2019/02/27 14:21:59 server.go:64: [ERROR] Server is ready!
2019-02-27 14:21:59.534562 I | embed: ready to serve client requests
2019-02-27 14:21:59.535699 N | embed: serving insecure client requests on 127.0.0.1:2370, this is strongly discouraged!
[GIN] 2019/02/27 - 14:21:59 | 200 |  2.621834973s |       127.0.0.1 | GET      /register?clusterName=cbdb&nodeId=1
2019/02/27 14:21:59 server.go:154: [INFO] register master ok
2019/02/27 14:21:59 server.go:116: [INFO] Baud server successful startup...
2019/02/27 14:21:59 server.go:169: INFO : server pid:94731
````

# Setting up a Cluster Model

> In BaudEngine it has three module `ps`(PartitionServer) , `master`, `router` you can run `./baudengine -f config.toml router master` only start router and master.

> Now we have five machine , two master , two ps and one router

* master 
    * 192.168.1.1
    * 192.168.1.2
* ps
    * 192.168.1.3
    * 192.168.1.4
* router
    * 192.168.1.5


* make your config  to conf.tml

````
[global]
    name = "baudengine"
    data = "datas/"
    log = "logs/"
    level = "debug"
    signkey = "baudengine"

# if you are master you'd better set all config for router and ps and router and ps use default config it so cool
[[masters]]
    name = "m1"
    address = "127.0.0.1"
    api_port = 8817
    etcd_port = 2378
    etcd_peer_port = 2390
    etcd_client_port = 2370
[[masters]]
    name = "m2"
    address = "192.168.1.2"
    api_port = 8817
    etcd_port = 2378
    etcd_peer_port = 2390
    etcd_client_port = 2370
        

[router]
    port = 9001
    skip_auth = false

[ps]
    rpc_port = 8081
    raft_heartbeat_port = 8898
    raft_replicate_port = 8899
    heartbeat-interval = 200 #ms
    raft_retain_logs = 10000
    raft_replica_concurrency = 1
    raft_snap_concurrency = 1
````

* on 192.168.1.1 , 192.168.1.2

````
./baudengine -conf conf.toml master
````

* on 192.168.1.3 , 192.168.1.4

````
./baudengine -conf conf.toml ps
````

* 192.168.1.5

````
./baudengine -conf conf.toml router
````

> Good Luck you will see like this ,it means you are got it 

````
2019-02-27 14:21:59.534520 I | etcdserver: published {Name:m1 ClientURLs:[http://127.0.0.1:2370]} to cluster 64c4741bb8d03782
2019/02/27 14:21:59 server.go:64: [ERROR] Server is ready!
2019-02-27 14:21:59.534562 I | embed: ready to serve client requests
2019-02-27 14:21:59.535699 N | embed: serving insecure client requests on 127.0.0.1:2370, this is strongly discouraged!
[GIN] 2019/02/27 - 14:21:59 | 200 |  2.621834973s |       127.0.0.1 | GET      /register?clusterName=baudengine&nodeId=1
2019/02/27 14:21:59 server.go:154: [INFO] register master ok
2019/02/27 14:21:59 server.go:116: [INFO] Baud server successful startup...
2019/02/27 14:21:59 server.go:169: INFO : server pid:94731
````
