//! Vearch Rust SDK
//! 
//! A high-level Rust client for the Vearch vector database.
//! 
//! ## Features
//! 
//! - Document operations (insert, update, delete, query, search)
//! - Vector similarity search with filters
//! - Batch operations
//! - Async/await support
//! - Type-safe API
//! 
//! ## Example
//! 
//! ```rust,no_run
//! use vearch_sdk_rs::{VearchClient, Document, VectorField};
//! 
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = VearchClient::new("http://localhost:8817")?;
//!     
//!     // Insert a document
//!     let doc = Document::new()
//!         .with_field("title", "Sample Document")
//!         .with_vector("embedding", vec![0.1, 0.2, 0.3]);
//!     
//!     let result = client.upsert("my_db", "my_space", vec![doc]).await?;
//!     println!("Inserted {} documents", result.total);
//!     
//!     // Search for similar vectors
//!     let search_result = client.search(
//!         "my_db",
//!         "my_space",
//!         vec![VectorField::new("embedding", vec![0.1, 0.2, 0.3])],
//!     ).await?;
//!     
//!     for doc in search_result.documents {
//!         println!("Found document: {:?}", doc);
//!     }
//!     
//!     Ok(())
//! }
//! ```

pub mod client;
pub mod models;
pub mod error;
pub mod operations;

pub use client::VearchClient;
pub use models::*;
pub use error::*;
pub use operations::*;

// 重新导出库和表空间操作相关的类型
pub use operations::{
    DatabaseBuilder, SpaceBuilder, FieldBuilder, IndexBuilder,
    field_utils,
};

// 重新导出鉴权相关的类型
pub use operations::{
    PrivilegeBuilder, RoleBuilder, UserBuilder, AuthConfigBuilder,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_document_creation() {
        let doc = Document::new()
            .with_field("title", "Test Document")
            .with_field("score", 95.5)
            .with_vector("embedding", vec![0.1, 0.2, 0.3]);

        assert_eq!(doc._id, None);
        assert!(matches!(doc.get_field("title"), Some(FieldValue::String(_))));
        assert!(matches!(doc.get_field("score"), Some(FieldValue::Float(_))));
        assert_eq!(doc.get_vector("embedding"), Some(&vec![0.1, 0.2, 0.3]));
    }

    #[test]
    fn test_document_with_id() {
        let doc = Document::with_id("test_001")
            .with_field("name", "Test");

        assert_eq!(doc._id, Some("test_001".to_string()));
        assert!(matches!(doc.get_field("name"), Some(FieldValue::String(_))));
    }

    #[test]
    fn test_filter_builder() {
        let filter = FilterBuilder::new()
            .gte("score", 80.0)
            .lt("age", 50)
            .in_values("category", vec!["A".to_string(), "B".to_string()])
            .build();

        assert_eq!(filter.operator, "AND");
        assert_eq!(filter.conditions.len(), 3);
    }

    #[test]
    fn test_index_params_builder() {
        let params = IndexParamsBuilder::new()
            .metric_type("L2")
            .ef_search(64)
            .nprobe(80)
            .build();

        assert_eq!(params.metric_type, Some("L2".to_string()));
        assert_eq!(params.ef_search, Some(64));
        assert_eq!(params.nprobe, Some(80));
    }

    #[test]
    fn test_vector_field() {
        let vector = VectorField::new("embedding", vec![0.1, 0.2, 0.3])
            .with_min_score(0.8)
            .with_max_score(0.95);

        assert_eq!(vector.field, "embedding");
        assert_eq!(vector.feature, vec![0.1, 0.2, 0.3]);
        assert_eq!(vector.min_score, Some(0.8));
        assert_eq!(vector.max_score, Some(0.95));
    }

    #[test]
    fn test_ranker() {
        let ranker = Ranker::weighted_ranker(vec![0.7, 0.3]);

        assert_eq!(ranker.ranker_type, "WeightedRanker");
        assert_eq!(ranker.params, vec![0.7, 0.3]);
    }

    #[test]
    fn test_utils_range_filter() {
        let filter = operations::utils::range_filter("price", Some(10.0), Some(100.0));
        
        assert_eq!(filter.operator, "AND");
        assert_eq!(filter.conditions.len(), 2);
    }

    #[test]
    fn test_utils_text_filter() {
        let filter = operations::utils::text_filter("tags", vec!["rust".to_string(), "vearch".to_string()]);
        
        assert_eq!(filter.operator, "AND");
        assert_eq!(filter.conditions.len(), 1);
    }
}
