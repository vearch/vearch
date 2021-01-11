# Vearch Compile and Deploy

## Docker Deploy

#### Docker Hub Image Center 
 1. vearch base compile environment image address: https://hub.docker.com/r/vearch/vearch/tags
 2. vearch deploy image address: https://hub.docker.com/r/vearch/vearch/tags

#### Use Vearch Image Deploy
 1. take vearch:3.2.0 as an example
 2. docker pull vearch/vearch:3.2.0
 3. one docker deploy or distributed deployment
    1. ```If deploy a docker start vearch,master,ps,router start together: cat vearch/config/config.toml.example > config.toml nohup docker run -p 8817:8817 -p 9001:9001 -v $PWD/config.toml:/vearch/config.toml  vearch/vearch:3.2.0 all &```
    
    2. ```If distributed deploy ,modify vearch/config/config.toml and start separately```
    3. ```Modify vearch/config/config.toml ,refer the step 'Local Model'```
    4. ```Start separately image, modify step i 'all' to 'master' and 'ps' and 'router' ,master image must first start```

#### Use Base Image Compile And Deploy
 1. take vearch_env:3.2.0 as an example
 2. docker pull vearch/vearch_env:3.2.0
 3. sh vearch/cloud/complile.sh
 4. sh build.sh
 5. reference "User vearch image deploy" step 3

#### Use Script Create Base Image And Vearch Image
 1. build compile base environment image 
    1. go to $vearch/cloud dir
    2. run ./compile_env.sh you will got a image named vearch_env
 2. compile vearch
    1. go to $vearch/cloud dir
    2. run ./compile.sh you will compile Vearch in $vearch/build/bin , $vearch/build/lib
 3. make vearch image
    1. go to $vearch/cloud dir
    2. run ./build.sh you will got a image named vearch good luck
 4. how to use it 
    1. you can use docker run -it -v config.toml:/vearch/config.toml vearch all to start vearch by local model the last param has four type[ps, router ,master, all] all means tree type to start
 5. One-click build vearch image
    1. go to $vearch/cloud dir
    2. you can run ./run_docker.sh

## No Image Compile And Deploy

#### Dependent Environment 

   1. CentOS, Ubuntu and Mac OS are all OK (recommend CentOS >= 7.2).
   2. Go >= 1.11.2 required.
   3. Gcc >= 5 required.
   4. Cmake >= 3.17 required.
   5. OpenBLAS.
   6. [Faiss](https://github.com/facebookresearch/faiss) >= v1.6.4, You don't need to install it manually, the script installs it automatically. 
   7. [RocksDB](https://github.com/facebook/rocksdb) == 6.2.2 ***(optional)***. You don't need to install it manually, the script installs it automatically. But you need to manually install the dependencies of rocksdb. Please refer to the installation method: https://github.com/facebook/rocksdb/blob/master/INSTALL.md
   8. [Zfp](https://github.com/LLNL/zfp) == v0.5.5 ***(optional)***, You don't need to install it manually, the script installs it automatically.
   9. CUDA >= 9.0, if you want GPU support.
#### Compile 
   * Enter the `GOPATH` directory, `cd $GOPATH/src` `mkdir -p github.com/vearch` `cd github.com/vearch`
   * Download the source code: `git clone https://xxxxxx/vearch.git` ($vearch denotes the absolute path of vearch code)
   * Download the source code of subprojects gamma: ``cd vearch``  `git submodule update`
   * To add GPU Index support: change `BUILD_WITH_GPU` from `"off"` to `"on"` in `$vearch/engine/CMakeLists.txt` 
   * Compile vearch and gamma
      1. `cd build`
      5. `sh build.sh`
      when `vearch` file generated, it is ok.
      
#### Deploy
   Before run vearch, you shuld set `LD_LIBRARY_PATH`, Ensure that system can find gamma dynamic libraries. The gamma dynamic library that has been compiled is in the $vearch/build/gamma_build folder.
   ##### 1 Local Model
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
./vearch -conf conf.toml all
````

   ##### 2 Cluster Model
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

