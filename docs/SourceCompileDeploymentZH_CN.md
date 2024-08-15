# Vearch编译和部署

## 源码编译和部署

#### 依赖环境

   1. CentOS、ubuntu和Mac OS都支持（推荐CentOS >= 7.2）
   2. go >= 1.22
   3. gcc >= 10
   4. cmake >= 3.17
   5. OpenBLAS
   6. tbb，CentOS可使用yum安装，如：yum install tbb-devel.x86_64
   7. [RocksDB](https://github.com/facebook/rocksdb) == 6.6.4，你不需要手动安装，脚本自动安装。但是你需要手动安装rocksdb的依赖。请参考如下安装方法：https://github.com/facebook/rocksdb/blob/master/INSTALL.md
   8. CUDA >= 9.2，如果你不使用GPU模型，可忽略。
#### 编译
   * 进入 `GOPATH` 目录, `cd $GOPATH/src` `mkdir -p github.com/vearch` `cd github.com/vearch`
   * 下载源码: `git clone https://github.com/vearch/vearch.git` ($vearch表示vearch代码的绝对路径)
   * 添加GPU索引支持: 将`$vearch/internal/engine/CMakeLists.txt`中的 `BUILD_WITH_GPU` 从`"off"` 变为`"on"` 
   * 编译vearch和gamma
      1. `cd build`
      2. `sh build.sh`
      当' vearch '文件生成时，表示编译成功。
      
#### 部署
运行vearch前，你需要设置环境变量 `LD_LIBRARY_PATH`，确保系统能找到gamma的动态库。编译好的gamma动态库在$vearch/build/gamma_build文件夹下。
   ##### 1 单机模式
   * 配置文件conf.toml
     
```
cp config/config.toml conf.toml
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
