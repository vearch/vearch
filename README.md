<div align="center">
  <img src="docs/img/vearch_logo.png">
</div>

[![master Build Status](https://travis-ci.org/vearch/vearch.svg?branch=master)](https://travis-ci.org/vearch/vearch)
[![dev Build Status](https://travis-ci.org/vearch/vearch.svg?branch=dev)](https://travis-ci.org/vearch/vearch)
## Overview

Vearch is a scalable distributed system for efficient similarity search of deep learning vectors. 


## Architecture

![arc](docs/img/VearchArch.jpg)

* Data Model

  space, documents, vectors, scalars

* Components

  `Master`, `Router` and `PartitionServer` 

* Master 

  Responsible for schema mananagement, cluster-level metadata, and resource coordination. 
  
* Router

  Provides RESTful API: `create`  , `delete`  `search` and `update` ; request routing, and result merging. 

* PartitionServer (PS)

  Hosts document partitions with raft-based replication.

  Gamma`is the core vector search engine. It provides the ability of storing, indexing and retrieving the vectors and scalars.


## Quick start

* Quickly build a distributed vector search system with RESTful API, please see [docs/Deploy.md](docs/Deploy.md).


* Vearch can be leveraged to build a complete visual search system to index billions of images. The image retrieval plugin for object detection and feature extraction is also required. For more information, please refer to [docs/Quickstart.md](docs/Quickstart.md).


## APIs and Use Cases


### LowLevelAPI
* [docs/APILowLevel.md](docs/APILowLevel.md)


### VisualSearchAPI
* [docs/APIVisualSearch.md](docs/APIVisualSearch.md)



## License
Licensed under the Apache License, Version 2.0. For detail see LICENSE and NOTICE.
