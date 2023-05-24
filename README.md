<div align="center">
  <img src="docs/img/vearch_logo.png">
</div>

[![Build Status](https://travis-ci.com/wxingda/vearch.svg?branch=master)](https://travis-ci.com/wxingda/vearch)  &nbsp;&nbsp;&nbsp; [![Gitter](https://badges.gitter.im/vector_search/community.svg)](https://gitter.im/vector_search/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)
## Overview

Here we build a cloud native vector database on top of CubeFS, It supports failover function and separates storage and computation by design.

## Architecture

* Data Model

  space, documents, vectors, scalars

* Components

  `Master`, `Router` and `PartitionServer` 

* Master 

  Responsible for schema mananagement, cluster-level metadata, and resource coordination. 
  
* Router

  Provides RESTful API: `create`  , `delete`  `search` and `update` ; request routing, and result merging. 

* PartitionServer (PS)

  Hosts space partitions; each partition has document store, inverted index and vector index. 

  Gamma is the core vector search engine implemented based on [faiss](https://github.com/facebookresearch/faiss). It provides the ability of storing, indexing and retrieving the vectors and scalars.


## Quick start

* One-click binary installation, please see [docs/BinaryInstallation.md](docs/BinaryInstallation.md)([中文版](docs/BinaryInstallationZH_CN.md)).

* Quickly compile the source codes to build a distributed vector search system with RESTful API, please see [docs/SourceCompileDeployment.md](docs/SourceCompileDeployment.md).

* Vearch Python SDK enables vearch to use locally. Vearch python sdk can be installed easily by pip install vearch. For more information, please refer to [docs/APIPythonSDK.md](docs/APIPythonSDK.md).

## APIs and Use Cases


### LowLevelAPI
* [docs/APILowLevel.md](docs/APILowLevel.md)
* For GPU [docs/APILowLevel.md](docs/APILowLevelOnGPU.md)

### PythonSDKAPI
* [docs/APIPythonSDK.md](docs/APIPythonSDK.md)

## Document

* https://vearch.readthedocs.io/en/latest
* https://vearch.readthedocs.io/zh_CN/latest
* [Common QA](https://github.com/vearch/vearch/wiki/Vearch-QA)

## Benchmarks

* [benchmarks](/engine/benchs/README.md)

## Publication
Jie Li, Haifeng Liu, Chuanghua Gui, Jianyu Chen, Zhenyun Ni, Ning Wang, Yuan Chen. The Design and Implementation of a Real Time Visual Search System on JD E-commerce Platform. In the 19th International ACM Middleware Conference, December 10–14, 2018, Rennes, France. https://arxiv.org/abs/1908.07389

## Community
You can report bugs or ask questions in the [issues page](https://github.com/vearch/vearch/issues) of the repository.

For public discussion of Vearch or for questions, you can also send email to vearch-maintainers@groups.io.

Our slack : https://vearchwrokspace.slack.com

## Known Users
Welcome to register the company name in this issue: https://github.com/vearch/vearch/issues/230 (in order of registration)

欢迎在此 issue https://github.com/vearch/vearch/issues/230 中登记公司名称

![科大讯飞](static/kedaxunfei.png)
![飞搜科技](static/faceall.png)
![君库科技](static/bigbigwork.png)
![爱奇艺](static/iqiyi.png)
![人民科技](static/peopletech.png)
![趣头条](static/qutoutiao.png)
![网易严选](static/wangyiyanxuan.png)
![咸唐科技](static/sunthang.png)
![华为技术](static/huawei.png)
![OPPO](static/oppo.png)
![汽车之家](static/autohome.png)
![芯翌智能](static/xforwardai.png)
![图灵机器人](static/turingapi.png)
![金山云](static/ksyun.png)
![汇智通信](static/teligen.png)
![小红书](static/xiaohongshu.png)
![VIVO](static/vivo.png)
![JD](static/jd.png)


## Wechat official communication group

![Official Accounts](docs/img/251120CB-8A61-4124-9336-B549D8D10607.png)


## License

Licensed under the Apache License, Version 2.0. For detail see [LICENSE and NOTICE](https://github.com/vearch/vearch/blob/master/LICENSE).
