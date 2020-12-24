# Vearch编译和部署

## Docker部署

#### Docker Hub Image Center 
 1. vearch基础编译环境镜像地址： https://hub.docker.com/r/vearch/vearch/tags
 2. vearch部署镜像地址: https://hub.docker.com/r/vearch/vearch/tags

#### 使用Vearch镜像部署
 1. 以vearch:3.2.0为例
 2. docker pull vearch/vearch:3.2.0
 3. 一个docker部署或分布式部署
    1. ```If deploy a docker start vearch,master,ps,router start together: cat vearch/config/config.toml.example > config.toml nohup docker run -p 8817:8817 -p 9001:9001 -v $PWD/config.toml:/vearch/config.toml  vearch/vearch:3.2.0 all &```
    
    2. ```If distributed deploy ,modify vearch/config/config.toml and start separately```
    3. ```Modify vearch/config/config.toml ,refer the step 'Local Model'```
    4. ```Start separately image, modify step i 'all' to 'master' and 'ps' and 'router' ,master image must first start```

#### 使用基础镜像编译和部署
 1. 以vearch_env:3.2.0为例
 2. docker pull vearch/vearch_env:3.2.0
 3. sh vearch/cloud/complile.sh
 4. sh build.sh
 5. 参考“使用Vearch镜像部署”步骤3

#### 使用脚本创建基础镜像和vearch镜像
 1. 构建编译基础环境映像
    1. 进入$vearch/cloud目录
    2. 执行./compile_env.sh，你将得到一个名为vearch_env的映像
 2. 编译vearch
    1. 进入$vearch/cloud目录
    2. 执行./compile.sh，编译结果在$vearch/build/bin , $vearch/build/lib中
 3. 制作vearch镜像
    1. 进入$vearch/cloud目录
    2. 执行./build.sh， 你将得到一个vearch的镜像
 4. 使用方法 
    1. 执行 `docker run -it -v config.toml:/vearch/config.toml vearch all`  all表示master、router、ps同时启动，也可以使用master\router\ps分开启动
 5. 一键构建vearch镜像
    1. 进入$vearch/cloud目录
    2. 执行./run_docker.sh

## 源码编译和部署

#### 依赖环境

   1. CentOS、ubuntu和Mac OS都支持（推荐CentOS >= 7.2）
   2. Go >= 1.11.2
   3. Gcc >= 5
   4. Cmake >= 3.17
   5. OpenBLAS
   6. [Faiss](https://github.com/facebookresearch/faiss) >= v1.6.4，你不需要手动安装，脚本自动安装。
   7. [RocksDB](https://github.com/facebook/rocksdb) == 6.2.2 ***（可选）***，你不需要手动安装，脚本自动安装。但是你需要手动安装rocksdb的依赖。请参考如下安装方法：https://github.com/facebook/rocksdb/blob/master/INSTALL.md
   8. [Zfp](https://github.com/LLNL/zfp) == v0.5.5 ***(可选)***，你不需要手动安装，脚本自动安装。
   9. CUDA >= 9.0，如果你不使用GPU模型，可忽略。
#### 编译
   * 进入 `GOPATH` 目录, `cd $GOPATH/src` `mkdir -p github.com/vearch` `cd github.com/vearch`
   * 下载源码: `git clone https://xxxxxx/vearch.git` ($vearch表示vearch代码的绝对路径)
   * 下载子项目: `cd vearch`  `git submodule update`
   * 添加GPU索引支持: 将`$vearch/engine/CMakeLists.txt`中的 `BUILD_WITH_GPU` 从`"off"` 变为`"on"` 
   * 编译vearch和gamma
      1. `cd build`
      5. `sh build.sh`
      当' vearch '文件生成时，表示编译成功。
      
#### 部署
运行vearch前，你需要设置环境变量 `LD_LIBRARY_PATH`，确保系统能找到gamma的动态库。编译好的gamma动态库在$vearch/build/gamma_build文件夹下。
   ##### 1 单机模式
   * 配置文件conf.toml
     
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
   * 执行

````
./vearch -conf conf.toml all
````

   ##### 2 集群模式
   > vearch有3种模式: `ps`(PartitionServer) 、`master`、`router`， 执行`./vearch -f conf.toml ps/router/master` 开始 ps/router/master模式

   > 现在我们有5台机器, 2 master、2 ps 和 1 router

* master
    * 192.168.1.1
    * 192.168.1.2
* ps
    * 192.168.1.3
    * 192.168.1.4
* router
    * 192.168.1.5
* 配置文件conf.toml

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
* 在192.168.1.1 , 192.168.1.2 运行 master

````
./vearch -conf conf.toml master
````

* 在192.168.1.3 , 192.168.1.4 运行 ps

````
./vearch -conf conf.toml ps
````

* 在192.168.1.5 运行 router

````
./vearch -conf conf.toml router
````

