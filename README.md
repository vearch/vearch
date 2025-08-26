<div align="center">
  <img src="assets/vearch_logo.png">
  <p>
    <a href="https://github.com/vearch/vearch/blob/master/README_ZH_CN.md">简体中文</a> | <a href="https://github.com/vearch/vearch/blob/master/README.md">English</a>
  </p>
</div>

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](./LICENSE)
[![Build Status](https://github.com/vearch/vearch/actions/workflows/CI.yml/badge.svg)](https://github.com/vearch/vearch/actions/workflows/CI.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/vearch/vearch/v3)](https://goreportcard.com/report/github.com/vearch/vearch/v3)
[![Gitter](https://badges.gitter.im/vector_search/community.svg)](https://gitter.im/vector_search/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

## Overview

Vearch is a cloud-native distributed vector database for efficient similarity search of embedding vectors in your AI applications.

## Key features

- **Hybrid search**: Both vector search and scalar filtering.

- **Performance**: Fast vector retrieval - search from millions of objects in milliseconds.

- **Scalability & Reliability**: Replication and elastic scaling out.

## Document

### Restful APIs

- [Tutorial](https://vearch.readthedocs.io/en/latest) | [参考文档](https://vearch.readthedocs.io/zh_CN/latest)

### OpenAPIs

- [API Documentation](https://vearch.github.io/tools#/)

### SDK

| SDK                                              | Description                    |
|--------------------------------------------------|--------------------------------|
| [**Python SDK**](sdk/python/README.md)           | Python client for Vearch       |
| [**Go SDK**](sdk/go/README.md)                   | Go client for Vearch           |
| [**Java SDK**](sdk/java/README.md)               | Java client for Vearch |
| [**Rust SDK**](sdk/rust/vearch-sdk-rs/README.md) | Rust client for Vearch         |

## Usage Cases

### Use Vearch as a Memory Backend

Vearch integrates with popular AI frameworks:

| Framework | Integration |
|-----------|-------------|
| [**Langchain**](sdk/integrations/langchain/README.md) | Use Vearch as vector store in Langchain |
| [**LlamaIndex**](sdk/integrations/llama-index/README.md) | Integrate with LlamaIndex for knowledge bases |
| [**Langchaingo**](sdk/integrations/langchaingo/vearch/README.md) | Go implementation of Langchain with Vearch support |
| [**LangChain4j**](sdk/integrations/langchain4j/README.md) | Java implementation with Vearch integration |

### Real world Demos

- **[VisualSearch](docs/Quickstart.md)**: Vearch can be leveraged to build a complete visual search system to index billions of images. The image retrieval plugin for object detection and feature extraction is also required.

## Quick start

**[Kubernetes Deployment](https://vearch.github.io/vearch-helm/)**

```
# Via Helm Repository
$ helm repo add vearch https://vearch.github.io/vearch-helm
$ helm repo update && helm install my-release vearch/vearch

# Or from Local Charts
$ git clone https://github.com/vearch/vearch-helm.git && cd vearch-helm
$ helm install my-release ./charts -f ./charts/values.yaml
```

**Docker Compose Deployment**

```
# Standalone Mode
$ cd cloud && cp ../config/config.toml .
$ docker-compose --profile standalone up -d

# Cluster Mode
$ cd cloud && cp ../config/config_cluster.toml .
$ docker-compose --profile cluster up -d
```

**Other Deployment Methods**
- **[DeployByDocker](docs/DeployByDocker.md)**: Deploy Vearch by Docker
- **[SourceCompileDeployment](docs/SourceCompileDeployment.md)**: Compile Vearch from source code

## Components

**Vearch Architecture**

![arc](assets/architecture.excalidraw.png)

**Master**: Responsible for schema management, cluster-level metadata, and resource coordination.

**Router**: Provides RESTful API: `upsert`, `delete`, `search` and `query`; request routing, and result merging.

**PartitionServer (PS)**: Hosts document partitions with raft-based replication. Gamma is the core vector search engine implemented based on [faiss](https://github.com/facebookresearch/faiss). It provides the ability of storing, indexing and retrieving the vectors and scalars.

## Technical Reference

### Academic Citation
When using Vearch in academic or research projects, please cite our paper:
```
@misc{li2019design,
      title={The Design and Implementation of a Real Time Visual Search System on JD E-commerce Platform},
      author={Jie Li and Haifeng Liu and Chuanghua Gui and Jianyu Chen and Zhenyun Ni and Ning Wang},
      year={2019},
      eprint={1908.07389},
      archivePrefix={arXiv},
      primaryClass={cs.IR}
}
```

## Community Support

### Connect With Us
Connect with the Vearch community through multiple channels:

- **GitHub Issues**: Report bugs or request features on our [issues page](https://github.com/vearch/vearch/issues)
- **Email Discussion**: For public discussion or questions, contact us at [vearch-maintainers@groups.io](mailto:vearch-maintainers@groups.io)
- **Slack Channel**: Join our community on [Slack](https://vearchworkspace.slack.com) for real-time discussions

### Contribution
We welcome contributions from the community! Check our contribution guidelines to get started.

## License

Vearch is licensed under the [Apache License, Version 2.0](./LICENSE).

For complete licensing details, please see [LICENSE and NOTICE](https://github.com/vearch/vearch/blob/master/LICENSE) in our repository.

---

<div align="center">
  <small>© 2019 Vearch Contributors. All Rights Reserved.</small>
</div>
