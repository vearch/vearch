# Vearch Rust SDK

[![Crates.io](https://img.shields.io/crates/v/vearch-sdk-rs)](https://crates.io/crates/vearch-sdk-rs)
[![Documentation](https://docs.rs/vearch-sdk-rs/badge.svg)](https://docs.rs/vearch-sdk-rs)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

一个功能完整的 Rust SDK，用于连接和操作 Vearch 向量数据库。支持异步操作、类型安全、构建器模式，以及完整的 RBAC 鉴权系统。

## ✨ 特性

- 🚀 **异步支持** - 基于 Tokio 的高性能异步操作
- 🔒 **类型安全** - 完整的 Rust 类型安全性和错误处理
- 🏗️ **构建器模式** - 流畅的 API 用于构建复杂查询和过滤器
- 📚 **完整覆盖** - 支持所有主要的 Vearch 操作
- 🗄️ **数据库管理** - 创建、删除和管理数据库
- 🏗️ **表空间管理** - 创建、配置和管理表空间
- 🔐 **RBAC 鉴权** - 基于角色的访问控制，支持用户和角色管理
- 🧪 **充分测试** - 全面的测试覆盖和示例

## 🚀 快速开始

### 安装

在 `Cargo.toml` 中添加依赖：

```toml
[dependencies]
vearch-sdk-rs = "3.5.0"
tokio = { version = "1.0", features = ["full"] }
```

### 基本使用

```rust
use vearch_sdk_rs::{VearchClient, Document, VectorField};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建客户端
    let client = VearchClient::new("http://localhost:8817")?;
    
    // 创建文档
    let doc = Document::new()
        .with_id("doc1")
        .with_field("title", "示例文档")
        .with_field("content", "这是一个示例文档")
        .with_vector("embedding", vec![0.1, 0.2, 0.3]);
    
    // 插入文档
    let result = client.upsert("my_db", "my_space", vec![doc]).await?;
    println!("插入结果: {:?}", result);
    
    Ok(())
}
```

## 🏗️ 核心概念

### 文档 (Documents)

文档是 Vearch 中的基本数据单元，包含标量字段和向量字段：

```rust
use vearch_sdk_rs::{Document, VectorField, FieldValue};

let doc = Document::new()
    .with_id("unique_id")
    .with_field("title", "文档标题")
    .with_field("tags", vec!["标签1", "标签2"])
    .with_field("score", 95.5)
    .with_vector("embedding", vec![0.1, 0.2, 0.3, 0.4]);
```

### 向量字段 (Vector Fields)

向量字段用于存储高维向量数据：

```rust
let vector = VectorField::new("embedding", vec![0.1, 0.2, 0.3, 0.4])
    .with_dimension(4)
    .with_index_type("HNSW")
    .with_metric_type("L2");
```

### 过滤器 (Filters)

过滤器用于对查询结果进行标量字段过滤：

```rust
use vearch_sdk_rs::{FilterBuilder, FilterOperator};

let filter = FilterBuilder::new()
    .and()
        .field("category").equals("技术")
        .field("score").greater_than(80.0)
        .field("tags").contains("AI")
    .end()
    .build();
```

### 索引参数 (Index Parameters)

索引参数控制向量搜索的性能和准确性：

```rust
use vearch_sdk_rs::IndexParamsBuilder;

let index_params = IndexParamsBuilder::new("HNSW")
    .with_m(16)
    .with_ef_construction(200)
    .with_ef_search(100)
    .build();
```

### 数据库和表空间

#### 创建数据库

```rust
use vearch_sdk_rs::{DatabaseBuilder, FieldBuilder, IndexBuilder};

let db = DatabaseBuilder::new("my_database")
    .with_space("my_space", |space| {
        space
            .with_field(FieldBuilder::new("title", FieldType::String))
            .with_field(FieldBuilder::new("content", FieldType::String))
            .with_field(FieldBuilder::new("embedding", FieldType::Vector).with_dimension(384))
            .with_index(IndexBuilder::new("HNSW").with_m(16).with_ef_construction(200))
    })
    .build();

client.create_database(db).await?;
```

#### 创建表空间

```rust
let space = SpaceBuilder::new("my_space")
    .with_field(FieldBuilder::new("title", FieldType::String))
    .with_field(FieldBuilder::new("content", FieldType::String))
    .with_field(FieldBuilder::new("embedding", FieldType::Vector).with_dimension(384))
    .with_index(IndexBuilder::new("HNSW").with_m(16).with_ef_construction(200))
    .build();

client.create_space("my_db", space).await?;
```

## 🔐 鉴权和授权

SDK 支持 Vearch 的 RBAC（基于角色的访问控制）系统，用于安全的访问管理。

### 设置鉴权

```rust
use vearch_sdk_rs::{VearchClient, AuthConfigBuilder};

// 创建带鉴权的客户端
let auth_config = AuthConfigBuilder::new("用户名", "密码")
    .with_role("管理员")
    .build();

let mut client = VearchClient::new("http://localhost:8817")?
    .with_auth(auth_config);

// 登录获取认证令牌
client.login().await?;
```

### 管理角色和权限

```rust
use vearch_sdk_rs::{PrivilegeBuilder, RoleBuilder, PrivilegeType};

// 创建具有特定权限的角色
let privileges = PrivilegeBuilder::new()
    .document(PrivilegeType::ReadOnly)
    .space(PrivilegeType::WriteRead)
    .database(PrivilegeType::ReadOnly)
    .build();

let role = RoleBuilder::new("数据科学家")
    .with_privileges(privileges)
    .build();

client.create_role(role).await?;
```

### 管理用户

```rust
use vearch_sdk_rs::UserBuilder;

let user = UserBuilder::new("john_doe", "安全密码")
    .with_role("数据科学家")
    .build();

client.create_user(user).await?;
```

### 支持的资源类型

- `ResourceAll` - 所有接口资源（root 角色权限）
- `ResourceCluster` - 集群接口
- `ResourceDB` - 数据库操作
- `ResourceSpace` - 表空间操作
- `ResourceDocument` - 文档操作
- `ResourceIndex` - 索引管理
- 以及更多...

### 权限级别

- `ReadOnly` - 只读访问权限
- `WriteOnly` - 只写访问权限
- `WriteRead` - 读写访问权限

## 📚 API 参考

### 文档操作

#### 插入/更新文档

```rust
// 单个文档
let result = client.upsert("db_name", "space_name", vec![document]).await?;

// 批量文档
let documents = vec![doc1, doc2, doc3];
let result = client.upsert("db_name", "space_name", documents).await?;
```

#### 查询文档

```rust
// 精确查询
let docs = client.query("db_name", "space_name", filter).await?;

// 向量搜索
let result = client.search("db_name", "space_name", vector, 10).await?;

// 复杂搜索
let search_request = client.search_builder("db_name", "space_name")
    .with_vector(vector)
    .with_filter(filter)
    .with_limit(20)
    .with_ranker(Ranker::weighted_ranker(vec![0.7, 0.3]))
    .build();

let result = client.search_with_request(search_request).await?;
```

#### 删除文档

```rust
// 按 ID 删除
client.delete_by_ids("db_name", "space_name", vec!["id1", "id2"]).await?;

// 按过滤器删除
client.delete_by_filter("db_name", "space_name", filter).await?;
```

### 数据库管理

```rust
// 创建数据库
let db_request = DatabaseBuilder::new("my_db").build();
client.create_database(db_request).await?;

// 删除数据库
client.delete_database("my_db").await?;

// 列出所有数据库
let databases = client.list_databases().await?;

// 获取数据库信息
let db_info = client.get_database("my_db").await?;
```

### 表空间管理

```rust
// 创建表空间
let space_request = SpaceBuilder::new("my_space")
    .with_field(FieldBuilder::new("title", FieldType::String))
    .with_field(FieldBuilder::new("embedding", FieldType::Vector).with_dimension(384))
    .build();

client.create_space("my_db", space_request).await?;

// 删除表空间
client.delete_space("my_db", "my_space").await?;

// 获取表空间信息
let space_info = client.get_space("my_db", "my_space").await?;

// 获取表空间统计信息
let stats = client.get_space_stats("my_db", "my_space").await?;

// 重建索引
client.rebuild_index("my_db", "my_space").await?;
```

### 鉴权管理

```rust
// 角色管理
client.create_role(role_request).await?;
client.delete_role("role_name").await?;
client.get_role("role_name").await?;
client.modify_role(modify_request).await?;

// 用户管理
client.create_user(user_request).await?;
client.delete_user("user_name").await?;
client.get_user("user_name").await?;
client.modify_user(modify_request).await?;

// 认证状态
client.is_authenticated();
client.get_auth_token();
client.clear_auth();
```

## 🔧 配置

### 客户端选项

```rust
use std::time::Duration;

// 自定义超时
let client = VearchClient::with_timeout(
    "http://localhost:8817",
    Duration::from_secs(60)
)?;

// 克隆客户端用于并发
let client2 = client.clone();
```

### 负载均衡

```rust
let result = client.search_with_request(
    client.search_builder("db", "space")
        .with_vector(vector)
        .load_balance("leader") // 选项: leader, random, not_leader, least_connection
        .build()
).await?;
```

## 📖 示例

查看 `examples/` 目录中的完整工作示例：

- `basic_operations.rs` - 基本 CRUD 操作
- `vector_search.rs` - 向量相似性搜索
- `database_operations.rs` - 数据库和表空间管理
- `authentication.rs` - RBAC 鉴权和授权
- `filters.rs` - 复杂过滤示例
- `batch_operations.rs` - 批量文档操作

## 🧪 测试

运行测试套件：

```bash
cargo test
```

运行详细输出：

```bash
cargo test -- --nocapture
```

## 🚀 性能优化

### 批量操作

```rust
// 批量插入文档
let documents = vec![doc1, doc2, doc3, doc4, doc5];
client.upsert("db", "space", documents).await?;

// 批量删除文档
client.delete_by_ids("db", "space", vec!["id1", "id2", "id3"]).await?;
```

### 向量搜索优化

```rust
// 使用 HNSW 索引进行快速搜索
let index_params = IndexParamsBuilder::new("HNSW")
    .with_m(16)              // 每个节点的邻居数
    .with_ef_construction(200) // 构建时的搜索深度
    .with_ef_search(100)     // 搜索时的深度
    .build();

// 使用 FLAT 索引进行精确搜索
let flat_params = IndexParamsBuilder::new("FLAT")
    .with_metric_type("L2")
    .build();
```

## 🔍 错误处理

SDK 提供全面的错误处理：

```rust
use vearch_sdk_rs::{VearchError, VearchResult};

match client.upsert("db", "space", docs).await {
    Ok(result) => println!("成功: {:?}", result),
    Err(VearchError::ApiError { code, message }) => {
        eprintln!("API 错误 {}: {}", code, message);
    }
    Err(VearchError::HttpError(e)) => {
        eprintln!("HTTP 错误: {}", e);
    }
    Err(e) => eprintln!("其他错误: {}", e),
}
```

## 🌟 高级功能

### 多向量搜索

```rust
let ranker = Ranker::weighted_ranker(vec![0.7, 0.3]);

let result = client.search_with_request(
    client.search_builder("my_db", "my_space")
        .with_vector(VectorField::new("embedding1", vec1))
        .with_vector(VectorField::new("embedding2", vec2))
        .with_ranker(ranker)
        .build()
).await?;
```

### 复杂过滤器

```rust
let filter = FilterBuilder::new()
    .or()
        .and()
            .field("category").equals("技术")
            .field("score").greater_than(80.0)
        .end()
        .and()
            .field("category").equals("科学")
            .field("score").greater_than(90.0)
        .end()
    .end()
    .build();
```

### 字段工具函数

```rust
use vearch_sdk_rs::field_utils;

let space = SpaceBuilder::new("my_space")
    .with_field(field_utils::string_field("title"))
    .with_field(field_utils::int_field("age"))
    .with_field(field_utils::float_field("score"))
    .with_field(field_utils::vector_field("embedding", 384))
    .with_field(field_utils::array_field("tags", FieldType::String))
    .build();
```

## 🤝 贡献

欢迎贡献！请随时提交 Pull Request。

## 📄 许可证

本项目采用 Apache 2.0 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

## 🙏 致谢

- [Vearch](https://github.com/vearch/vearch) - 本 SDK 连接的向量数据库
- [Tokio](https://tokio.rs/) - 异步运行时
- [Serde](https://serde.rs/) - 序列化框架
- [Reqwest](https://github.com/seanmonstar/reqwest) - HTTP 客户端

## 📞 支持

如果您遇到问题或有建议，请：

1. 查看 [Issues](https://github.com/your-username/vearch-sdk-rs/issues)
2. 提交新的 Issue
3. 参与讨论

---

**Vearch Rust SDK** - 让向量数据库操作变得简单而强大！ 🚀🔍
