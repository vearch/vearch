# Vearch Compile and Deploy

## Docker Deploy

#### Docker Hub Image Center 
 1. vearch base compile environment image address: https://hub.docker.com/r/vearch/vearch_env/tags
 2. vearch deploy image address: https://hub.docker.com/r/vearch/vearch/tags

#### Use Vearch Image Deploy
 1. docker pull vearch/vearch:latest
 2. one docker deploy or distributed deployment
    1. ```If deploy a docker start vearch,master,ps,router start together: cat vearch/config/config.toml.example > config.toml nohup docker run -p 8817:8817 -p 9001:9001 -v $PWD/config.toml:/vearch/config.toml  vearch/vearch:latest all &```
    
    2. ```If distributed deploy ,modify vearch/config/config.toml and start separately```
    3. ```Modify vearch/config/config.toml ,refer the step 'Local Model'```
    4. ```Start separately image, modify step i 'all' to 'master' and 'ps' and 'router' ,master image must first start```

#### Use Base Image Compile And Deploy
 1. take vearch_env:latest as an example
 2. docker pull vearch/vearch_env:latest
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
   2. go >= 1.19 required.
   3. gcc >= 7 required.
   4. cmake >= 3.17 required.
   5. OpenBLAS.
   6. tbb，In CentOS it can be installed by yum. Such as: yum install tbb-devel.x86_64.
   7. [RocksDB](https://github.com/facebook/rocksdb) == 6.6.4 ***(optional)***. You don't need to install it manually, the script installs it automatically. But you need to manually install the dependencies of rocksdb. Please refer to the installation method: https://github.com/facebook/rocksdb/blob/master/INSTALL.md
   8. CUDA >= 9.2, if you want GPU support.
#### Compile 
   * Enter the `GOPATH` directory, `cd $GOPATH/src` `mkdir -p github.com/vearch` `cd github.com/vearch`
   * Download the source code: `git clone https://github.com/vearch/vearch.git` ($vearch denotes the absolute path of vearch code)
   * To add GPU Index support: change `BUILD_WITH_GPU` from `"off"` to `"on"` in `$vearch/engine/CMakeLists.txt` 
   * Compile vearch and gamma
      1. `cd build`
      2. `sh build.sh`
      when `vearch` file generated, it is ok.
      
#### Deploy
   Before run vearch, you shuld set `LD_LIBRARY_PATH`, Ensure that system can find gamma dynamic libraries. The gamma dynamic library that has been compiled is in the $vearch/build/gamma_build folder.
   ##### 1 Local Model
   * generate config file conf.toml
     
```
cp config/config.toml.example conf.toml
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
