Installation and Use
==================


Compile
--------

Environmental dependence

1. Linux system(recommend CentOS 7.2 or more), supporting cmake and make commands.
2. Go version 1.11.2 or more
3. Gcc version 5 or more
4. `Faiss <https://github.com/facebookresearch/faiss>`_

Compile

-  download source code: git clone https://github.com/vearch/vearch.git (Follow-up use of $vearch to represent the absolute path of the vearch directory)

-  compile gamma

   1. ``cd $vearch/engine/src``
   2. ``mkdir build && cd build``
   3. ``export Faiss_HOME=faiss installed path``
   4. ``cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=$vearch/ps/engine/gammacb/lib ..``
   5. ``make && make install``

-  compile vearch

   1. ``cd $vearch``
   2. ``export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$vearch/ps/engine/gammacb/lib/lib``
   3. ``export Faiss_HOME=faiss installed path``
   4. ``go build -a --tags=vector -o  vearch``
   
   generate \ ``vearch``\ file compile success

Deploy
--------

stand-alone mode:

-  generate configuration file conf.toml
::

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
       
   [router]
       # port for server
       port = 9001
   
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

-  start

::

   ./vearch -conf conf.toml

Use Examples
--------

