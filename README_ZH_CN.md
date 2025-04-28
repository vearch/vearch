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

## 简介

Vearch 是一个云原生分布式向量数据库，用于在 AI 应用程序中对向量进行高效的相似性搜索。

## 主要特性

- **混合检索**: 向量搜索和标量过滤。

- **性能**: 快速矢量检索 - 在几毫秒内搜索数百万个对象。

- **可扩展性和可靠性**: 弹性扩展和高可用。

## 文档

### Restful 接口

- [参考文档](https://vearch.readthedocs.io/zh_CN/latest)

### OpenAPIS

- [API文档](https://vearch.github.io/tools#/)

### SDK

| SDK | 描述 |
|-----|-------------|
| [**Python SDK**](sdk/python/README.md) | Vearch的Python客户端 |
| [**Go SDK**](sdk/go/README.md) | Vearch的Go客户端 |
| [**Java SDK**](sdk/java/README.md) | Vearch的Java客户端（开发中） |

## 使用案例

### 大模型记忆后端

Vearch与流行的AI框架集成：

| 框架 | 集成 |
|-----------|-------------|
| [**Langchain**](sdk/integrations/langchain/README.md) | 在Langchain中使用Vearch作为向量存储 |
| [**LlamaIndex**](sdk/integrations/llama-index/README.md) | 与LlamaIndex集成构建知识库 |
| [**Langchaingo**](sdk/integrations/langchaingo/vearchREADME.md) | 支持Vearch的Langchain的Go实现 |
| [**LangChain4j**](sdk/integrations/langchain4j/README.md) | 支持Vearch集成的Java实现 |

### 实际场景

- **[图片检索](docs/Quickstart.md)**: Vearch 可用于构建完整的视觉搜索系统来索引数十亿张图像。还需要用于对象检测和特征提取的图像检索插件。

## 快速开始

**[k8s 部署 Vearch 集群](https://vearch.github.io/vearch-helm/)**

```
# 通过仓库添加charts
$ helm repo add vearch https://vearch.github.io/vearch-helm
$ helm repo update && helm install my-release vearch/vearch

# 或从本地添加charts
$ git clone https://github.com/vearch/vearch-helm.git && cd vearch-helm
$ helm install my-release ./charts -f ./charts/values.yaml
```

**通过 docker-compose 使用 vearch**

```
# 单节点模式
$ cd cloud && cp ../config/config.toml .
$ docker-compose --profile standalone up -d

# 集群模式
$ cd cloud && cp ../config/config_cluster.toml .
$ docker-compose --profile cluster up -d
```

**其他部署方式**
- **[Docker部署](docs/DeployByDockerZH_CN.md)**: 通过Docker部署Vearch
- **[源码编译部署](docs/SourceCompileDeploymentZH_CN.md)**: 从源代码编译Vearch

## 组件

**Vearch 架构**

![arc](assets/architecture.excalidraw.png)

**Master**: 负责模式管理、集群级元数据和资源协调。

**Router**: 提供 RESTful API：`upsert`、`delete`、`search` 和 `query` ； 请求路由和结果合并。

**PartitionServer (PS)**: 使用基于 raft 的复制托管文档分区。 Gamma 是基于[faiss](https://github.com/facebookresearch/faiss)实现的核心矢量搜索引擎，提供了存储、索引和检索向量和标量的能力。

## 技术参考

### 学术引用
在学术或研究项目中使用Vearch时，请引用我们的论文：
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

## 社区支持

### 联系我们
通过多种渠道与Vearch社区联系：

- **GitHub Issues**: 在我们的[问题页面](https://github.com/vearch/vearch/issues)中报告错误或请求功能
- **电子邮件讨论**: 如需公开讨论或提出问题，请发送电子邮件至[vearch-maintainers@groups.io](mailto:vearch-maintainers@groups.io)
- **Slack频道**: 加入我们在[Slack](https://vearchworkspace.slack.com)上的社区进行实时讨论

### 贡献
我们欢迎社区的贡献！查看我们的贡献指南开始参与。

## 开源许可

Vearch根据[Apache许可证2.0版本](./LICENSE)授权。

有关完整的许可详情，请参阅我们仓库中的[LICENSE和NOTICE](https://github.com/vearch/vearch/blob/master/LICENSE)。

---

<div align="center">
  <small>© 2019 Vearch Contributors. 保留所有权利。</small>
</div>
