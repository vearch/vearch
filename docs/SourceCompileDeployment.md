# Vearch Compile and Deploy

## No Docker Image Compile And Deploy

#### Dependent Environment 

   1. CentOS, Ubuntu and Mac OS are all OK (recommend CentOS >= 7.2).
   2. go >= 1.22 required.
   3. gcc >= 10 required.
   4. cmake >= 3.17 required.
   5. OpenBLAS.
   6. tbb，In CentOS it can be installed by yum. Such as: yum install tbb-devel.x86_64.
   7. [RocksDB](https://github.com/facebook/rocksdb) == 6.6.4. You don't need to install it manually, the script installs it automatically. But you need to manually install the dependencies of rocksdb. Please refer to the installation method: https://github.com/facebook/rocksdb/blob/master/INSTALL.md
   8. CUDA >= 9.2, if you want GPU support.
#### Compile 
   * Enter the `GOPATH` directory, `cd $GOPATH/src` `mkdir -p github.com/vearch` `cd github.com/vearch`
   * Download the source code: `git clone https://github.com/vearch/vearch.git` ($vearch denotes the absolute path of vearch code)
   * To add GPU Index support: change `BUILD_WITH_GPU` from `"off"` to `"on"` in `$vearch/internal/engine/CMakeLists.txt` 
   * Compile vearch and gamma
      1. `cd build`
      2. `sh build.sh`
      when `vearch` file generated, it is ok.
      
#### Deploy
   Before run vearch, you shuld set `LD_LIBRARY_PATH`, Ensure that system can find gamma dynamic libraries. The gamma dynamic library that has been compiled is in the $vearch/build/gamma_build folder.
   ##### 1 Local Model
   * generate config file conf.toml
     
```
cp config/config.toml conf.toml
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
