<div align="center">
  <img src="docs/img/vearch_logo.png">
</div>
Vearch is a scalable system for deep learning vector search, and particularly it can works as an open source visual search engine.

## Architecture

![arc](docs/img/VearchArch.jpg)

* Components

  > `Master`, `Router` and `PartitionServer` 

* Master 

  > When you crate database or space you must use this service , default port is `8817` when you create dabase is only create a scope associate user permissions.

  > When you create space ,the master will select relatively idel machine to create partition , when you delete space the master notice the related machines to delete local partition.

  > Responsible for the management of distributed configurations.
* Router

  > Supports restful api.`create`  , `delete`  `search` and `update` ， also when write document it routing function to related machine , to save it , you can define your routing args default is `_id` , and merge multiple searching results to one result.

* PartitionServer (PS)

  > Hosts document partitions, raft-based replication.

  > Gamma`is the core vector search engine. It provides the ability of storing, indexing and retrieving the vectors and scalars.


## Quick start

* Quickly build a distributed vector search system with Restful api, please see [docs/Deploy.md](docs/Deploy.md).


* Quickly build a complete visual search system, which can support billion-scale images. The image retrieval plugin about object detection and feature extraction should be extra required, For more information, please refer to [plugin/README.md](plugin/README.md).

## API

### VisualSearchAPI
* [docs/APIVisualSearch.md](docs/APIVisualSearch.md)

### LowLevelAPI
* [docs/APILowLevel.md](docs/APILowLevel.md)

## License
Licensed under the Apache License, Version 2.0. For detail see LICENSE and NOTICE.
