# Vearch Java SDK

Vearch Java SDK 是 [Vearch](https://github.com/vearch/vearch) 向量数据库的官方 Java 客户端库。它提供了一套完整的 API，用于与 Vearch 服务进行交互，包括集群管理、数据库管理、空间管理和向量数据操作等功能。

## 目录

- [简介](#简介)
- [安装](#安装)
- [配置](#配置)
- [快速开始](#快速开始)
- [API 参考](#api-参考)
  - [VearchClient](#vearchclient)
  - [集群操作](#集群操作)
  - [数据库操作](#数据库操作)
  - [空间操作](#空间操作)
  - [数据操作](#数据操作)
  - [RAG 数据操作](#rag-数据操作)
- [高级用法](#高级用法)
  - [向量索引配置](#向量索引配置)
  - [批量操作](#批量操作)
  - [复杂查询](#复杂查询)
- [最佳实践](#最佳实践)
- [贡献指南](#贡献指南)
- [许可证](#许可证)

## 简介

Vearch 是一个高效、可扩展的向量数据库，专为大规模向量检索和相似性搜索而设计。它支持多种向量索引类型，包括 HNSW（Hierarchical Navigable Small World）等，能够处理高维向量数据，并提供快速、准确的相似性搜索功能。

Vearch Java SDK 提供了与 Vearch 服务交互的简单方式，使开发者能够轻松地在 Java 应用程序中集成向量搜索功能。SDK 支持以下主要功能：

- 集群管理：查看集群状态、健康状况和节点信息
- 数据库管理：创建、查看、删除数据库
- 空间管理：创建、查看、删除空间（类似于传统数据库中的表）
- 数据操作：插入、更新、删除和查询向量数据
- RAG（检索增强生成）数据操作：支持 RAG 相关的特定数据操作

## 安装

### Maven

在项目的 `pom.xml` 文件中添加以下依赖：

```xml
<dependency>
    <groupId>com.jd.vearch</groupId>
    <artifactId>fresh-vearch</artifactId>
    <version>1.0</version>
</dependency>
```

### Gradle

在项目的 `build.gradle` 文件中添加以下依赖：

```groovy
implementation 'com.jd.vearch:fresh-vearch:1.0'
```

## 配置

### 基本配置

创建 VearchClient 实例：

```java
// 创建 VearchClient 实例
String baseUrl = "http://vearch-server:9001";
VearchClient client = new VearchClient(baseUrl);
```

### Spring Boot 集成

虽然目前 Spring Boot 自动配置功能尚未完全实现，但您可以在 Spring Boot 应用程序中手动配置 VearchClient：

```java
@Configuration
public class VearchConfig {
    
    @Value("${vearch.baseUrl}")
    private String baseUrl;
    
    @Value("${vearch.userName:root}")
    private String userName;
    
    @Value("${vearch.token:token}")
    private String token;
    
    @Bean
    public VearchClient vearchClient() {
        return new VearchClient(baseUrl);
    }
}
```

在 `application.properties` 或 `application.yml` 中配置：

```properties
vearch.baseUrl=http://vearch-server:9001
vearch.userName=root
vearch.token=token
```

## 快速开始

以下是使用 Vearch Java SDK 的基本示例：

```java
import com.jd.vearch.VearchClient;
import com.jd.vearch.client.*;
import com.jd.vearch.model.*;

import java.util.*;

public class VearchExample {
    public static void main(String[] args) {
        // 创建 VearchClient 实例
        String baseUrl = "http://vearch-server:9001";
        VearchClient client = new VearchClient(baseUrl);
        
        // 获取数据库客户端
        DatabaseClient dbClient = client.getDatabaseClient();
        
        // 创建数据库
        String dbName = "example_db";
        Database database = dbClient.createDatabase(dbName);
        
        // 获取空间客户端
        SpaceClient spaceClient = client.getSpaceClient(dbName);
        
        // 创建空间
        Space space = new Space();
        space.setName("example_space");
        space.setPartitionNum(1);
        space.setReplicaNum(1);
        
        // 配置字段
        List<Field> fields = new ArrayList<>();
        
        // 添加 ID 字段
        Field idField = new Field();
        idField.setName("id");
        idField.setType("string");
        fields.add(idField);
        
        // 添加向量字段
        Field vectorField = new Field();
        vectorField.setName("feature");
        vectorField.setType("vector");
        vectorField.setDimension(128);
        
        // 配置索引
        Index index = new Index();
        index.setName("feature");
        index.setType("HNSW");
        
        // 配置检索参数
        RetrievalParam retrievalParam = new RetrievalParam();
        retrievalParam.setMetricType("L2");
        
        // 配置 HNSW 参数
        Hnsw hnsw = new Hnsw();
        hnsw.setNlinks(32);
        hnsw.setEfConstruction(128);
        hnsw.setEfSearch(64);
        retrievalParam.setHnsw(hnsw);
        
        index.setRetrievalParam(retrievalParam);
        vectorField.setIndex(index);
        fields.add(vectorField);
        
        space.setFields(fields);
        spaceClient.createSpace(space);
        
        // 获取数据客户端
        DataClient<String> dataClient = client.getDataClient(dbName);
        
        // 插入数据
        Map<String, Object> document = new HashMap<>();
        document.put("id", "1");
        document.put("feature", generateRandomVector(128));
        dataClient.insertDocument("example_space", document, "1");
        
        // 查询数据
        String result = dataClient.getDocumentById("example_space", "1");
        System.out.println("查询结果: " + result);
    }
    
    private static float[] generateRandomVector(int dimension) {
        float[] vector = new float[dimension];
        Random random = new Random();
        for (int i = 0; i < dimension; i++) {
            vector[i] = random.nextFloat();
        }
        return vector;
    }
}
```

## API 参考

### VearchClient

`VearchClient` 是 SDK 的主要入口点，用于获取各种客户端实例。

```java
// 创建 VearchClient 实例
VearchClient client = new VearchClient(baseUrl);

// 获取数据库客户端
DatabaseClient dbClient = client.getDatabaseClient();

// 获取集群客户端
ClusterClient clusterClient = client.getClusterClient();

// 获取空间客户端
SpaceClient spaceClient = client.getSpaceClient(databaseName);

// 获取数据客户端
DataClient<String> dataClient = client.getDataClient(databaseName);
```

### 集群操作

`ClusterClient` 提供了集群管理相关的操作。

```java
// 获取集群状态
List<ClusterStatus> statusList = clusterClient.getClusterStatus();

// 获取集群健康状态
List<ClusterHealth> healthList = clusterClient.getHealthStatus();

// 获取端口状态
PortStatus portStatus = clusterClient.getPortStatus();

// 清除锁
String result = clusterClient.clearLocks();
```

### 数据库操作

`DatabaseClient` 提供了数据库管理相关的操作。

```java
// 获取所有数据库
List<Database> databases = dbClient.getAllDatabases();

// 创建数据库
Database database = dbClient.createDatabase(databaseName);

// 查看数据库
Database database = dbClient.viewDatabase(databaseName);

// 删除数据库
boolean success = dbClient.deleteDatabase(databaseName);

// 获取数据库中的所有空间
List<Space> spaces = dbClient.getAllSpaces(databaseName);
```

### 空间操作

`SpaceClient` 提供了空间管理相关的操作。

```java
// 创建空间
String result = spaceClient.createSpace(space);

// 查看空间
String spaceInfo = spaceClient.viewSpace(spaceName);

// 删除空间
String result = spaceClient.deleteSpace(spaceName);
```

### 数据操作

`DataClient` 提供了数据操作相关的功能。

```java
// 插入文档
String result = dataClient.insertDocument(spaceName, document, documentId);

// 批量插入文档
String result = dataClient.bulkInsertDocuments(spaceName, documents);

// 更新文档
String result = dataClient.updateDocument(spaceName, documentId, updatedDocument);

// 删除文档
String result = dataClient.deleteDocument(spaceName, documentId);

// 根据 ID 获取文档
String document = dataClient.getDocumentById(spaceName, documentId);

// 批量根据 ID 获取文档
List<Object> documents = dataClient.bulkGetDocumentsByIds(spaceName, documentIds, fields);

// 根据特征向量搜索文档
Object results = dataClient.msearchDocumentsByFeatures(spaceName, featureVector, queryParam);

// 批量特征向量搜索
Object results = dataClient.bulkSearchDocumentsByFeatures(spaceName, featureVectors, queryParam);

// 多向量搜索
List<String> results = dataClient.multiVectorSearch(spaceName, fieldMatches, size);

// 复杂搜索
Object results = dataClient.search(spaceName, sumMap, filterMap, queryParam);
```

### RAG 数据操作

`RagDataClient` 提供了 RAG（检索增强生成）相关的数据操作。

```java
// 获取 RAG 数据客户端
RagDataClient<String> ragDataClient = new RagDataOperation<>(baseUrl, databaseName);

// 批量插入或更新文档
String result = ragDataClient.upsertDocuments(spaceName, documents);

// 根据 ID 删除文档
String result = ragDataClient.deleteDocumentByIds(spaceName, documentIds);

// 根据过滤条件删除文档
String result = ragDataClient.deleteDocumentByFilter(spaceName, filters);

// 根据 ID 查询文档
List<Object> documents = ragDataClient.queryDocumentsByIds(spaceName, documentIds);

// 根据特征向量搜索文档
String results = ragDataClient.searchDocumentByFeatures(spaceName, featureVectors, searchParam);

// 根据过滤条件和特征向量搜索文档
List<Object> results = ragDataClient.searchDocumentsByFilterAndFeatures(spaceName, featureVectors, filters, searchMap);
```

## 高级用法

### 向量索引配置

Vearch 支持多种向量索引类型，最常用的是 HNSW（Hierarchical Navigable Small World）。以下是配置 HNSW 索引的示例：

```java
// 创建索引
Index index = new Index();
index.setName("feature");
index.setType("HNSW");

// 配置检索参数
RetrievalParam retrievalParam = new RetrievalParam();
retrievalParam.setMetricType("L2"); // 可选值: L2, IP (内积), COSINE 等

// 配置 HNSW 参数
Hnsw hnsw = new Hnsw();
hnsw.setNlinks(32);           // 每个节点的链接数量
hnsw.setEfConstruction(128);  // 构建时的扩展因子
hnsw.setEfSearch(64);         // 搜索时的扩展因子
retrievalParam.setHnsw(hnsw);

index.setRetrievalParam(retrievalParam);
```

### 批量操作

对于大量数据的处理，可以使用批量操作来提高效率：

```java
// 批量插入文档
Map<Long, Map<String, Object>> documents = new HashMap<>();
for (int i = 0; i < 1000; i++) {
    Map<String, Object> doc = new HashMap<>();
    doc.put("id", String.valueOf(i));
    doc.put("feature", generateRandomVector(128));
    documents.put((long) i, doc);
}
String result = dataClient.bulkInsertDocuments(spaceName, documents);

// 批量获取文档
List<String> ids = Arrays.asList("1", "2", "3", "4", "5");
List<String> fields = Arrays.asList("id", "feature");
List<Object> results = dataClient.bulkGetDocumentsByIds(spaceName, ids, fields);
```

### 复杂查询

Vearch 支持复杂的查询条件，包括向量相似性搜索和过滤条件的组合：

```java
// 创建向量查询条件
List<Map<String, Object>> sumMap = new ArrayList<>();
Map<String, Object> vectorQuery = new HashMap<>();
vectorQuery.put("field", "feature");
vectorQuery.put("feature", generateRandomVector(128));
sumMap.add(vectorQuery);

// 创建过滤条件
List<Map<String, Object>> filterMap = new ArrayList<>();
Map<String, Object> filter = new HashMap<>();
filter.put("range", Map.of("age", Map.of("gte", 18, "lte", 30)));
filterMap.add(filter);

// 设置查询参数
Map<String, Object> queryParam = new HashMap<>();
queryParam.put("size", 10);
queryParam.put("quick", true);

// 执行查询
Object results = dataClient.search(spaceName, sumMap, filterMap, queryParam);
```

## 最佳实践

### 性能优化

1. **批量操作**：对于大量数据的插入或查询，使用批量操作可以显著提高性能。

2. **索引参数调优**：
   - 对于 HNSW 索引，增加 `nlinks` 可以提高搜索精度，但会增加内存使用和构建时间。
   - 增加 `efConstruction` 可以提高索引质量，但会增加构建时间。
   - 增加 `efSearch` 可以提高搜索精度，但会增加搜索时间。

3. **合理设置分区和副本**：
   - 分区数（`partitionNum`）影响数据分布和并行处理能力。
   - 副本数（`replicaNum`）影响可用性和读取性能。

### 错误处理

SDK 使用 `VearchException` 来处理异常情况。建议在使用 SDK 时进行适当的异常处理：

```java
try {
    Database database = dbClient.createDatabase(databaseName);
} catch (VearchException e) {
    System.err.println("创建数据库失败: " + e.getMessage());
    // 处理异常
}
```

### 连接管理

SDK 内部使用 HttpURLConnection 进行 HTTP 通信。为了避免连接泄漏，建议在不再需要时关闭连接。SDK 已经在内部处理了连接的关闭，但在高并发场景下，可能需要考虑使用连接池。

## 贡献指南

我们欢迎并感谢社区对 Vearch Java SDK 的贡献。以下是贡献的一般步骤：

1. Fork 项目仓库
2. 创建您的特性分支 (`git checkout -b feature/amazing-feature`)
3. 提交您的更改 (`git commit -m 'Add some amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 打开一个 Pull Request

### 开发环境设置

1. 克隆仓库：
   ```
   git clone https://github.com/vearch/vearch-java.git
   cd vearch-java
   ```

2. 使用 Maven 构建项目：
   ```
   mvn clean install
   ```

3. 运行测试：
   ```
   mvn test
   ```

## 许可证

Vearch Java SDK 使用 Apache 2.0 许可证。详情请参阅 [LICENSE](LICENSE) 文件。

---

© 2025 Vearch 团队。保留所有权利。