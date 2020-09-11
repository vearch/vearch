# Vearch Compile and Deploy

## Check config

#### modify config 

   1. cd config 
   2. modify config.toml.example file
   3. such as deploy two master pod,get master pod ip
   4. modify address to new ip 
   5. IP such as "192.168.0.1","192.168.0.2":
```
[global]
    # the name will validate join cluster by same name
    name = "cbdb"
    # you data save to disk path ,If you are in a production environment, You'd better set absolute paths
    data = [
                "/export/vdb/baud/datas/",
            ] 
    # log path , If you are in a production environment, You'd better set absolute paths
    log = "/export/vdb/baud/logs/"
    # default log type for any model
    level = "debug"
    # master <-> ps <-> router will use this key to send or receive data
    signkey = "secret"
    # skip auth for master and router
    skip_auth = true

# if you are master you'd better set all config for router and ps and router and ps use default config it so cool
[[masters]]
    #name machine name for cluster
    name = "m1"
    #ip or domain
    address = "192.168.0.1"
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
    pprof_port = 6062
    # monitor
    monitor_port = 8818
    data = ["/export/vdb/vdb-master/datas/"]
    
[[masters]]
    #name machine name for cluster
    name = "m2"
    #ip or domain
    address = "192.168.0.2"
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
    pprof_port = 6062
    # monitor
    monitor_port = 8818
    data = ["/export/vdb/vdb-master/datas/"]

[router]
    # port for server
    port = 9001
    # rpc_port = 9002
    pprof_port = 6061
    plugin_path = "plugin"

[ps]
    # port for server
    rpc_port = 8081
    #raft config begin
    raft_heartbeat_port = 8898
    raft_replicate_port = 8899
    heartbeat-interval = 200 #ms
    raft_retain_logs = 20000000
    raft_replica_concurrency = 1
    raft_snap_concurrency = 1
    raft_truncate_count = 20000
    # engine config
    engine_dwpt_num = 8
    # max size byte
    # max_size = 50000000
    pprof_port = 6060
    # if set true , this ps only use in db meta config
    private = false



```         
      
##  Docker install

####  delete old docker 
* sudo yum remove -y docker \
                  docker-client \
                  docker-client-latest \
                  docker-common \
                  docker-latest \
                  docker-latest-logrotate \
                  docker-logrotate \
                  docker-selinux \
                  docker-engine-selinux \
                  docker-engine

#### install docker
* sudo yum install -y yum-utils device-mapper-persistent-data lvm2
* sudo yum-config-manager --add-repo  https://download.docker.com/linux/centos/docker-ce.repo
* sudo yum install docker -y

#### start docker
* modify /etc/sysconfig/docker file
* OPTIONS='--selinux-enabled=false --log-driver=json-file --signature-verification=false'
#### modify systemd start param
* mv /etc/systemd/system/docker.service.d/execstart.conf /etc/systemd/system/docker.service.d/execstart.conf.cp
* systemctl daemon-reload
* systemctl enable docker && systemctl start docker
      

## Make Docker Image
* go to $vearch/cloud dir
* you can run `./run_docker.sh`  
* docker push image center,such as  docker push xx.xx.local/ai/vearch:3.2.0
       
## Deploy
* new master.yaml start two pod
      
```
apiVersion: extensions/v1beta1
kind: Deployment
metadata:
  name: vearchmaster
  namespace: vearch
spec:
  replicas: 2
  selector:
    matchLabels:
      app: vearchmaster
  revisionHistoryLimit: 3
  template:
    metadata:
      labels:
        app: vearchmaster
    spec:
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
            - matchExpressions:
              - key: node.bcc.XX.com/dedicated
                operator: In
                values:
                - vearch
      terminationGracePeriodSeconds: 0
      tolerations:
      - effect: NoSchedule
        key: nvidia.com/gpu
        operator: Exists
      - effect: NoSchedule
        key: node.bcc.XX.com/dedicated
        operator: Equal
        value: vearch
      containers:
      - name: vearchmaster
        command: ["./bin/start.sh master"]
        image: xx.xx.local/ai/vearch:3.2.0
        resources:
          limits:
            cpu: 16
            memory: 16Gi
          requests:
            cpu: 16
            memory: 16Gi
        env:
        - name: TZ
          value: Asia/Shanghai

```
* new ps.yaml start four pod
```
apiVersion: extensions/v1beta1
kind: Deployment
metadata:
  name: vearchps
  namespace: vearch
spec:
  replicas: 4
  selector:
    matchLabels:
      app: vearchps
  revisionHistoryLimit: 3
  template:
    metadata:
      labels:
        app: vearchps
    spec:
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
            - matchExpressions:
              - key: node.bcc.XX.com/dedicated
                operator: In
                values:
                - vearch
      terminationGracePeriodSeconds: 0
      tolerations:
      - effect: NoSchedule
        key: nvidia.com/gpu
        operator: Exists
      - effect: NoSchedule
        key: node.bcc.XX.com/dedicated
        operator: Equal
        value: vearch
      containers:
      - name: vearchps
        command: ["./bin/start.sh ps"]
        image: xx.xx.local/ai/vearch:3.2.0
        resources:
          limits:
            cpu: 16
            memory: 16Gi
          requests:
            cpu: 16
            memory: 16Gi
        env:
        - name: TZ
          value: Asia/Shanghai

```
* new router.yaml start three pod
```
apiVersion: extensions/v1beta1
kind: Deployment
metadata:
  name: vearchrouter
  namespace: vearch
spec:
  replicas: 3
  selector:
    matchLabels:
      app: vearchrouter
  revisionHistoryLimit: 3
  template:
    metadata:
      labels:
        app: vearchrouter
    spec:
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
            - matchExpressions:
              - key: node.bcc.XX.com/dedicated
                operator: In
                values:
                - vearch
      terminationGracePeriodSeconds: 0
      tolerations:
      - effect: NoSchedule
        key: nvidia.com/gpu
        operator: Exists
      - effect: NoSchedule
        key: node.bcc.XX.com/dedicated
        operator: Equal
        value: vearch
      containers:
      - name: vearchrouter
        command: ["./bin/start.sh router"]
        image: xx.xx.local/ai/vearch:3.2.0
        resources:
          limits:
            cpu: 16
            memory: 16Gi
          requests:
            cpu: 16
            memory: 16Gi
        env:
        - name: TZ
          value: Asia/Shanghai

```