# Fashion image search
A simple image neural search demo for Fashion-MNIST. No extra dependencies needed, simply run:
```shell
docker run -it --rm  -p 80:80 --name vearch-demo gdream/vearch-demo:latest
```

```shell
$ docker run -it --rm  -p 80:80 --name vearch-demo gdream/vearch-demo:latest 


██╗   ██╗███████╗ █████╗ ██████╗  ██████╗██╗  ██╗
██║   ██║██╔════╝██╔══██╗██╔══██╗██╔════╝██║  ██║
██║   ██║█████╗  ███████║██████╔╝██║     ███████║
╚██╗ ██╔╝██╔══╝  ██╔══██║██╔══██╗██║     ██╔══██║
 ╚████╔╝ ███████╗██║  ██║██║  ██║╚██████╗██║  ██║
  ╚═══╝  ╚══════╝╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝╚═╝  ╚═╝
                                                 

[vearch]: starting
[vearch]: start success
------------ create db[fashion-minst] success, resp: {"code":200,"msg":"success","data":{"id":1,"name":"fashionmnist"}} 
------------ create space[fashion-minst] success, resp: {"code":200,"msg":"success","data":{"id":1,"name":"fashionmnist","version":2,"db_id":1,"enabled":true,"partitions":[{"id":1,"space_id":1,"db_id":1,"partition_slot":0,"replicas":[1]}],"partition_num":1,"replica_num":1,"properties":{"target": {"type": "keyword"}, "feature": {"type": "vector", "dimension": 512, "format": "normalization"}},"engine":{"name":"gamma","index_size":10000,"metric_type":"InnerProduct","retrieval_type":"IVFPQ","retrieval_param":{"metric_type": "InnerProduct", "ncentroids": 256, "nsubvector": 32}},"space_properties":{"target":{"field_type":4,"type":"keyword"},"feature":{"field_type":5,"type":"vector","format":"normalization","dimension":512,"option":1}}}} 
------------ download dataset from http://fashion-mnist.s3-website.eu-central-1.amazonaws.com 
------------ Begin load vgg model... 
------------ Begin insert data 
------------ insert complete num: 0, resp: <Response [200]> 
------------ insert complete num: 1000, resp: <Response [200]> 
App running at:
  - Local:   http://localhost:80/ 
```

This downloads the Fashion-MNIST training and test dataset, choose 1000 train data extract feature and insert into vearch, then you can retrieve relevant results by http://localhost:80/ . 

![vearch-image-demo](../img/vearch-image-demo.jpg)