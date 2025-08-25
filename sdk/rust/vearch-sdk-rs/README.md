# Vearch Rust SDK

A high-level Rust client for the [Vearch](https://vearch.readthedocs.io/) vector database, providing a type-safe and ergonomic API for vector similarity search and document operations.

## Features

- 🚀 **Async/await support** - Built with Tokio for high-performance async operations
- 🔒 **Type-safe API** - Full Rust type safety with comprehensive error handling
- 🏗️ **Builder pattern** - Fluent API for building complex queries and filters
- 📚 **Complete coverage** - All major Vearch operations supported
- 🗄️ **Database management** - Create, delete, and manage databases
- 🏗️ **Space management** - Create, configure, and manage table spaces
- 🔐 **RBAC Authentication** - Role-based access control with user and role management
- 🧪 **Well tested** - Comprehensive test coverage and examples

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
vearch-sdk-rs = "3.5.0"
tokio = { version = "1.0", features = ["full"] }
```

## Quick Start

```rust
use vearch_sdk_rs::{VearchClient, Document, VectorField};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a client
    let client = VearchClient::new("http://localhost:8817")?;
    
    // Insert a document
    let doc = Document::new()
        .with_field("title", "Sample Document")
        .with_vector("embedding", vec![0.1, 0.2, 0.3]);
    
    let result = client.upsert("my_db", "my_space", vec![doc]).await?;
    println!("Inserted {} documents", result.total);
    
    // Search for similar vectors
    let search_result = client.search(
        "my_db",
        "my_space",
        vec![VectorField::new("embedding", vec![0.1, 0.2, 0.3])],
    ).await?;
    
    for doc in search_result.documents {
        println!("Found document: {:?}", doc);
    }
    
    Ok(())
}
```

## Core Concepts

### Documents

Documents are the primary data structure in Vearch, containing both scalar fields and vector fields:

```rust
use vearch_sdk_rs::Document;

let doc = Document::new()
    .with_field("title", "My Document")
    .with_field("category", "technology")
    .with_field("score", 95.5)
    .with_vector("embedding", vec![0.1, 0.2, 0.3, 0.4, 0.5]);
```

### Vector Fields

Vector fields store high-dimensional vectors for similarity search:

```rust
use vearch_sdk_rs::VectorField;

let vector = VectorField::new("embedding", vec![0.1, 0.2, 0.3])
    .with_min_score(0.8)
    .with_max_score(0.95);
```

### Filters

Filters allow you to query documents based on scalar field values:

```rust
use vearch_sdk_rs::{FilterBuilder, FilterOperator};

let filter = FilterBuilder::new()
    .gte("score", 90.0)
    .in_values("category", vec!["technology", "science"])
    .lt("age", 100)
    .build();
```

### Index Parameters

Configure search behavior with index-specific parameters:

```rust
use vearch_sdk_rs::IndexParamsBuilder;

let params = IndexParamsBuilder::new()
    .metric_type("L2")
    .ef_search(64)  // For HNSW index
    .nprobe(80)     // For IVFFLAT index
    .build();
```

### Databases

Databases are the top-level containers in Vearch:

```rust
use vearch_sdk_rs::DatabaseBuilder;

let db_request = DatabaseBuilder::new("my_database")
    .with_description("My vector database")
    .build();
```

### Table Spaces

Table spaces define the schema and indexing for documents:

```rust
use vearch_sdk_rs::{SpaceBuilder, field_utils, IndexBuilder};

let space_request = SpaceBuilder::new("my_space")
    .with_field(field_utils::string_field("title").with_index(true).build())
    .with_field(field_utils::vector_field("embedding", 128).with_index(true).build())
    .with_index(IndexBuilder::new("HNSW").with_metric_type("L2").build())
    .with_partition_num(8)
    .with_replica_num(1)
    .build();
```

## API Reference

### Document Operations

#### Insert/Update Documents

```rust
// Insert new documents
let docs = vec![
    Document::new().with_field("name", "Doc 1"),
    Document::new().with_field("name", "Doc 2"),
];

let result = client.upsert("my_db", "my_space", docs).await?;
println!("Inserted {} documents", result.total);
```

#### Query Documents

```rust
// Query by document IDs
let docs = client.query(
    "my_db",
    "my_space",
    client.query_builder("my_db", "my_space")
        .with_ids(vec!["doc1", "doc2"])
        .build()
).await?;

// Query by filters
let docs = client.query(
    "my_db",
    "my_space",
    client.query_builder("my_db", "my_space")
        .with_filters(filter)
        .limit(100)
        .build()
).await?;
```

#### Search Documents

```rust
// Simple vector search
let result = client.search(
    "my_db",
    "my_space",
    vec![VectorField::new("embedding", query_vector)],
).await?;

// Advanced search with filters and parameters
let result = client.search_with_request(
    client.search_builder("my_db", "my_space")
        .with_vector(search_vector)
        .with_filters(filter)
        .with_index_params(index_params)
        .limit(20)
        .build()
).await?;
```

#### Delete Documents

```rust
// Delete by IDs
let result = client.delete_by_ids(
    "my_db",
    "my_space",
    vec!["doc1", "doc2"],
).await?;

// Delete by filter
let result = client.delete_by_filter(
    "my_db",
    "my_space",
    filter,
    Some(100), // limit
).await?;
```

### Database Operations

#### Create Database

```rust
let db_request = DatabaseBuilder::new("my_database")
    .with_description("My vector database")
    .build();

let result = client.create_database(db_request).await?;
```

#### Manage Database

```rust
// List all databases
let databases = client.list_databases().await?;

// Get database info
let db_info = client.get_database("my_database").await?;

// Delete database
let result = client.delete_database("my_database").await?;
```

### Table Space Operations

#### Create Table Space

```rust
let space_request = SpaceBuilder::new("my_space")
    .with_field(field_utils::string_field("title").with_index(true).build())
    .with_field(field_utils::vector_field("embedding", 128).with_index(true).build())
    .with_index(IndexBuilder::new("HNSW").with_metric_type("L2").build())
    .with_partition_num(8)
    .with_replica_num(1)
    .build();

let result = client.create_space("my_database", space_request).await?;
```

#### Manage Table Space

```rust
// List spaces in database
let spaces = client.list_spaces("my_database").await?;

// Get space info
let space_info = client.get_space("my_database", "my_space").await?;

// Get space statistics
let stats = client.get_space_stats("my_database", "my_space").await?;

// Rebuild index
let result = client.rebuild_index("my_database", "my_space").await?;

// Delete space
let result = client.delete_space("my_database", "my_space").await?;
```

### Filter Operations

#### Building Complex Filters

```rust
use vearch_sdk_rs::{FilterBuilder, FilterOperator, utils};

// Using the builder pattern
let filter = FilterBuilder::new()
    .gte("score", 80.0)
    .lt("age", 50)
    .in_values("category", vec!["A", "B", "C"])
    .not_in("status", vec!["deleted", "archived"])
    .build();

// Using utility functions
let range_filter = utils::range_filter("price", Some(10.0), Some(100.0));
let text_filter = utils::text_filter("tags", vec!["rust", "vearch"]);
```

#### Filter Operators

Supported operators:
- `>` - Greater than
- `>=` - Greater than or equal
- `<` - Less than
- `<=` - Less than or equal
- `IN` - Value in list
- `NOT IN` - Value not in list

### Search Configuration

#### Index Types

**HNSW (Hierarchical Navigable Small World)**
```rust
let params = IndexParamsBuilder::new()
    .metric_type("L2")
    .ef_search(64)
    .build();
```

**IVFFLAT (Inverted File with Flat compression)**
```rust
let params = IndexParamsBuilder::new()
    .metric_type("L2")
    .nprobe(80)
    .parallel_on_queries(1)
    .build();
```

**FLAT (Exhaustive search)**
```rust
let params = IndexParamsBuilder::new()
    .metric_type("L2")
    .build();
```

#### Multi-Vector Search

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

### Authentication and Authorization

The SDK supports Vearch's RBAC (Role-Based Access Control) system for secure access management.

#### Setting up Authentication

```rust
use vearch_sdk_rs::{VearchClient, AuthConfigBuilder};

// Create client with authentication
let auth_config = AuthConfigBuilder::new("username", "password")
    .with_role("admin")
    .build();

let mut client = VearchClient::new("http://localhost:8817")?
    .with_auth(auth_config);

// Login to get authentication token
client.login().await?;
```

#### Managing Roles and Permissions

```rust
use vearch_sdk_rs::{PrivilegeBuilder, RoleBuilder, PrivilegeType};

// Create role with specific permissions
let privileges = PrivilegeBuilder::new()
    .document(PrivilegeType::ReadOnly)
    .space(PrivilegeType::WriteRead)
    .database(PrivilegeType::ReadOnly)
    .build();

let role = RoleBuilder::new("data_scientist")
    .with_privileges(privileges)
    .build();

client.create_role(role).await?;
```

#### Managing Users

```rust
use vearch_sdk_rs::UserBuilder;

let user = UserBuilder::new("john_doe", "secure_password")
    .with_role("data_scientist")
    .build();

client.create_user(user).await?;
```

#### Supported Resource Types

- `ResourceAll` - All resources (root access)
- `ResourceCluster` - Cluster management
- `ResourceDB` - Database operations
- `ResourceSpace` - Table space operations
- `ResourceDocument` - Document operations
- `ResourceIndex` - Index management
- And more...

#### Permission Levels

- `ReadOnly` - Read-only access
- `WriteOnly` - Write-only access
- `WriteRead` - Full read-write access

## Error Handling

The SDK provides comprehensive error handling:

```rust
use vearch_sdk_rs::{VearchError, VearchResult};

match client.upsert("db", "space", docs).await {
    Ok(result) => println!("Success: {:?}", result),
    Err(VearchError::ApiError { code, message }) => {
        eprintln!("API error {}: {}", code, message);
    }
    Err(VearchError::HttpError(e)) => {
        eprintln!("HTTP error: {}", e);
    }
    Err(e) => eprintln!("Other error: {}", e),
}
```

## Configuration

### Client Options

```rust
use std::time::Duration;

// Custom timeout
let client = VearchClient::with_timeout(
    "http://localhost:8817",
    Duration::from_secs(60)
)?;

// Clone client for concurrent use
let client2 = client.clone();
```

### Load Balancing

```rust
let result = client.search_with_request(
    client.search_builder("db", "space")
        .with_vector(vector)
        .load_balance("leader") // Options: leader, random, not_leader, least_connection
        .build()
).await?;
```

## Examples

See the `examples/` directory for complete working examples:

- `basic_operations.rs` - Basic CRUD operations
- `vector_search.rs` - Vector similarity search
- `database_operations.rs` - Database and table space management
- `authentication.rs` - RBAC authentication and authorization
- `filters.rs` - Complex filtering examples
- `batch_operations.rs` - Batch document operations

## Testing

Run the test suite:

```bash
cargo test
```

Run with verbose output:

```bash
cargo test -- --nocapture
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Vearch](https://github.com/vearch/vearch) - The vector database this SDK connects to
- [Tokio](https://tokio.rs/) - Async runtime
- [Serde](https://serde.rs/) - Serialization framework
- [Reqwest](https://github.com/seanmonstar/reqwest) - HTTP client
