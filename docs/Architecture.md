# Architecture of Vearch

![arc](docs/img/VearchArch.jpg)

* Components

   `Master`, `Router` and `PartitionServer` 

* Master 

   When you crate database or space you must use this service , default port is `8817` when you create dabase is only create a scope associate user permissions.

   When you create space ,the master will select relatively idel machine to create partition , when you delete space the master notice the related machines to delete local partition.

   Responsible for the management of distributed configurations.
* Router

   Supports restful api.`create`  , `delete`  `search` and `update` ， also when write document it routing function to related machine , to save it , you can define your routing args default is `_id` , and merge multiple searching results to one result.

* PartitionServer (PS)

   Hosts document partitions, raft-based replication.

   Gamma`is the core vector search engine. It provides the ability of storing, indexing and retrieving the vectors and scalars.
