# Vearch Compile and Deploy

[TOC]

## Compile

#### Dependent Environment 

   1. CentOS, Ubuntu and Mac OS are all OK (recommend CentOS >= 7.2)，cmake required
   2. Go >= 1.11.2 required
   3. Gcc >= 5 required
   4. [Faiss](https://github.com/facebookresearch/faiss)

#### Compile 
   * Enter the `GOPATH` directory, `cd $GOPATH/src` `mkdir -p github/vearch` `cd github/vearch`
   * Download the source code: `git clone https://xxxxxx/vearch.git` ($vearch denotes the absolute path of vearch code)
   * Compile gamma
       1. `cd $vearch/engine/gamma`
       2. `mkdir build && cd build`
       3. `export FAISS_HOME=the installed path of faiss`
       4. `cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=$vearch/ps/engine/gammacb/lib  ..`
       5. `make && make install`
      
   * Compile vearch
      1. `cd $vearch`
      2. `export FAISS_HOME=the installed path of faiss`
      3. `export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$vearch/ps/engine/gammacb/lib/lib:$FAISS_HOME/lib`
      4. `go build -o vearch`
      when `vearch` file generated, it is ok.
      
      
## Docker

#### Build compile Environment 
* go to $vearch/cloud dir
* run `./compile_env.sh` you will got a image named `vearch_env`
#### Compile Vearch
* go to $vearch/cloud dir
* run `./compile.sh` you will compile Vearch in `$vearch/build/bin` , `$vearch/build/lib`
#### Make Vearch Image
* go to $vearch/cloud dir
* run `./build.sh` you will got a image named `vearch` good luck
#### How to use it 
> you can use `docker run -it -v config.toml:/vearch/config.toml vearch all` to start vearch by local model the last param has four type[`ps`, `router` ,`master`, `all`] all means tree type to start
       
## Deploy
   Before run vearch, you shuld set `LD_LIBRARY_PATH`, Ensure that system can find faiss and gamma dynamic libraries (like $vearch/ps/engine/gammacb/lib/lib and $FAISS_HOME/lib directory files) .
   #### 1 Local Model
   * generate config file conf.toml
      
```
[global]
    # the name will validate join cluster by same name
    name = "vearch"
    # you data save to disk path ,If you are in a production environment, You'd better set absolute paths
    data = ["datas/"]
    # log path , If you are in a production environment, You'd better set absolute paths
    log = "logs/"
    # default log type for any model
    level = "debug"
    # master <-> ps <-> router will use this key to send or receive data
    signkey = "vearch"
    skip_auth = true

# if you are master you'd better set all config for router and ps and router and ps use default config it so cool
[[masters]]
    # name machine name for cluster
    name = "m1"
    # ip or domain
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
    skip_auth = true

[router]
    # port for server
    port = 9001
    # skip auth for client visit data
    skip_auth = true

[ps]
    # port for server
    rpc_port = 8081
    # raft config begin
    raft_heartbeat_port = 8898
    raft_replicate_port = 8899
    heartbeat-interval = 200 #ms
    raft_retain_logs = 10000
    raft_replica_concurrency = 1
    raft_snap_concurrency = 1 
```
   * start

````
./vearch -conf conf.toml
````
   
   #### 2 Cluster Model
   > vearch has three module: `ps`(PartitionServer) , `master`, `router`, run `./vearch -f conf.toml ps/router/master` start ps/router/master module

   > Now we have five machine, two master, two ps and one router

* master
    * 192.168.1.1
    * 192.168.1.2
* ps
    * 192.168.1.3
    * 192.168.1.4
* router
    * 192.168.1.5
* generate config file conf.toml

````
[global]
    name = "vearch"
    data = ["datas/"]
    log = "logs/"
    level = "debug"
    signkey = "vearch"
    skip_auth = true

# if you are master you'd better set all config for router and ps and router and ps use default config it so cool
[[masters]]
    name = "m1"
    address = "192.168.1.1"
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
    skip_auth = true
[ps]
    rpc_port = 8081
    raft_heartbeat_port = 8898
    raft_replicate_port = 8899
    heartbeat-interval = 200 #ms
    raft_retain_logs = 10000
    raft_replica_concurrency = 1
    raft_snap_concurrency = 1
````
* on 192.168.1.1 , 192.168.1.2  run master

````
./vearch -conf conf.toml master
````

* on 192.168.1.3 , 192.168.1.4 run ps

````
./vearch -conf conf.toml ps
````

* on 192.168.1.5 run router

````
./vearch -conf conf.toml router
````
