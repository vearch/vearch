<div align="center">
  <img src="docs/img/vearch_logo.png">
</div>

[![Build Status](https://travis-ci.org/vearch/vearch.svg?branch=master)](https://travis-ci.org/vearch/vearch)  &nbsp;&nbsp;&nbsp; [![Gitter](https://badges.gitter.im/vector_search/community.svg)](https://gitter.im/vector_search/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)
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

  Gamma is the core vector search engine implemented based on [faiss](https://github.com/facebookresearch/faiss). It provides the ability of storing, indexing and retrieving the vectors and scalars.


## Quick start
![docs/img/plugin/main_process.gif](docs/img/plugin/main_process.gif)

* One-click binary installation, please see [docs/BinaryInstallation.md](docs/BinaryInstallation.md)([中文版](docs/BinaryInstallationZH_CN.md)).

* Quickly compile the source codes to build a distributed vector search system with RESTful API, please see [docs/SourceCompileDeployment.md](docs/SourceCompileDeployment.md).

* Vearch can be leveraged to build a complete visual search system to index billions of images. The image retrieval plugin for object detection and feature extraction is also required. For more information, please refer to [docs/Quickstart.md](docs/Quickstart.md).

* Vearch Python SDK enables vearch to use locally. Vearch python sdk can be installed easily by pip install vearch. For more information, please refer to [docs/APIPythonSDK.md](docs/APIPythonSDK.md).

## APIs and Use Cases


### LowLevelAPI
* [docs/APILowLevel.md](docs/APILowLevel.md)
* For GPU [docs/APILowLevel.md](docs/APILowLevelOnGPU.md)


### VisualSearchAPI
* [docs/APIVisualSearch.md](docs/APIVisualSearch.md)

### PythonSDKAPI
* [docs/APIPythonSDK.md](docs/APIPythonSDK.md)

## Document

* https://vearch.readthedocs.io/en/latest
* https://vearch.readthedocs.io/zh_CN/latest
* [Common QA](https://github.com/vearch/vearch/wiki/Vearch-QA)

## Benchmarks

* [benchmarks](/engine/benchs/README.md)

## Publication
Jie Li, Haifeng Liu, Chuanghua Gui, Jianyu chen, Zhenyun Ni, Ning Wang, Yuan Chen, Qiang Liu. The Design and Implementation of a Real Time Visual Search System on JD E-commerce Platform. In the 19th International ACM Middleware Conference, December 10–14, 2018, Rennes, France. https://arxiv.org/abs/1908.07389

## Community
You can report bugs or ask questions in the [issues page](https://github.com/vearch/vearch/issues) of the repository.

For public discussion of Vearch or for questions, you can also send email to vearch-maintainers@groups.io.

Our slack : https://vearchwrokspace.slack.com

## Known Users
Welcome to register the company name in this issue: https://github.com/vearch/vearch/issues/221 (in order of registration)

欢迎在此 issue https://github.com/vearch/vearch/issues/221 中登记公司名称

## License
Licensed under the Apache License, Version 2.0. For detail see [LICENSE and NOTICE](https://github.com/vearch/vearch/blob/master/LICENSE).
