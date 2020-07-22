
# Vearch Third Party Dependencies

Vearch depends on third party libraries to implement some functionality. This document describes which libraries are depended upon, and how. It is maintained by and for humans, and so while it is a best effort attempt to describe the server's dependencies, it is subject to change as libraries are added or removed.

## Gamma Engine's Dependencies

The gamma engine of vearch is mainly implemented based on faiss. The following list shows all the dependencies of the gamma engine.

| name         | License                                  | dependency type | modify  |
|---------------|------------------------------------------|-----------------|------------------------|
| faiss       | MIT                       | so              | modify the the source code of IVFPQ index  |
| RocksDB       | GPLv2, Apache 2\.0                        | so              | N                      |
| Btree-source-code       | public                        | source code        | N                      |
| cJSON         | MIT                                      | source code     | N                      |
| easyloggingpp | MIT                                      | source code     | N                      |
| libcuckoo     | Apache 2\.0                              | source code     | N                      |
| googletest    | BSD 3\-Clause "New" or "Revised" License | so              | N                      |


## Server's Dependencies

Servers include master, router and ps are implemented by golang. These following libraries are packages imported by these servers through golang.

| name          | License          | dependency type | modify |
|---------------|------------------|-----------------|------------------------|
| gopsutil      | BSD\-3\-Clause   | import          | N                      |
| geohash       | MIT              | import          | N                      |
| fastjson      | MIT              | import          | N                      |
| hdrhistogram  | MIT              | import          | N                      |
| gojson        | BSD\-3\-Clause   | import          | N                      |
| go            | MIT              | import          | N                      |
| ratelimit     | LGPL\-3\.0\-only | so              | N                      |
| msgpack       | BSD\-2\-Clause   | import          | N                      |
| crypto        | BSD\-3\-Clause   | import          | N                      |
| sftp          | BSD\-2\-Clause   | import          | N                      |
| protobuf      | BSD\-3\-Clause   | import          | N                      |
| mux           | BSD\-3\-Clause   | import          | N                      |
| protobuf      | BSD\-3\-Clause   | import          | N                      |
| httprouter    | BSD\-3\-Clause   | import          | N                      |
| atomic        | MIT              | import          | N                      |
| errors        | BSD\-2\-Clause   | import          | N                      |
| gocron        | BSD\-2\-Clause   | import          | N                      |
| gin           | MIT              | import          | N                      |
| etcd          | Apache\-2\.0     | import          | N                      |
| murmur3       | BSD\-3\-Clause   | import          | N                      |
| rpcx          | Apache\-2\.0     | import          | N                      |
| go\-cache     | MIT              | import          | N                      |
| gotest\.tools | Apache\-2\.0     | import          | N                      |
| grpc\-go      | Apache\-2\.0     | import          | N                      |
| net           | BSD\-3\-Clause   | import          | N                      |
| cast          | MIT              | import          | N                      |


## Plugin Server's Dependencies

Plugin server is a python server. These following libraries are packages imported by plugin server through python.

| name           | License      | dependency type | modify                            |
| -------------- | ------------ | --------------- | --------------------------------- |
| requests       | Apache\-2\.0 | import          | N                                 |
| tornado        | Apache\-2\.0 | import          | N                                 |
| shortuuid      | BSD          | import          | N                                 |
| opencv\-python | MIT          | import          | N                                 |
| tensorflow     | Apache\-2\.0 | import          | N                                 |
| mtcnn          | MIT          | import          | N                                 |
| facenet        | MIT          | import          | modify the source code of predict |
| torch          | BSD          | import          | N                                 |
| torchvision    | BSD          | import          | N                                 |
| keras-bert     | MIT          | import          | N                                 |
| keras          | MIT          | import          | N                                 |




