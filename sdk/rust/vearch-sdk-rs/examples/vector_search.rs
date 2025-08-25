use vearch_sdk_rs::{
    VearchClient, Document, VectorField, FilterBuilder, IndexParamsBuilder, Ranker,
    operations::utils,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Vector Search Example");
    println!("====================");

    // Create a client
    let client = VearchClient::new("http://127.0.0.1:8817")?;
    println!("✓ Client created");

    // Example 1: Basic vector search
    println!("\n1. Basic vector search...");
    
    let query_vector = VectorField::new("embedding", vec![0.1, 0.2, 0.3, 0.4, 0.5]);
    println!("✓ Query vector created: {:?}", query_vector);

    // Example 2: Vector search with score thresholds
    println!("\n2. Vector search with score thresholds...");
    
    let threshold_vector = VectorField::new("embedding", vec![0.2, 0.3, 0.4, 0.5, 0.6])
        .with_min_score(0.8)
        .with_max_score(0.95);
    
    println!("✓ Threshold vector created: {:?}", threshold_vector);

    // Example 3: Building filters for search
    println!("\n3. Building search filters...");
    
    let filter = FilterBuilder::new()
        .gte("rating", 4.5)
        .in_values("category", vec!["programming".to_string(), "technology".to_string()])
        .lt("year", 2023)
        .build();
    
    println!("✓ Filter created: {:?}", filter);

    // Example 4: Index parameters for different index types
    println!("\n4. Index parameters...");
    
    // HNSW parameters
    let hnsw_params = IndexParamsBuilder::new()
        .metric_type("L2")
        .ef_search(64)
        .build();
    println!("✓ HNSW parameters: {:?}", hnsw_params);
    
    // IVFFLAT parameters
    let ivfflat_params = IndexParamsBuilder::new()
        .metric_type("L2")
        .nprobe(80)
        .parallel_on_queries(1)
        .build();
    println!("✓ IVFFLAT parameters: {:?}", ivfflat_params);
    
    // FLAT parameters
    let flat_params = IndexParamsBuilder::new()
        .metric_type("L2")
        .build();
    println!("✓ FLAT parameters: {:?}", flat_params);

    // Example 5: Multi-vector search
    println!("\n5. Multi-vector search...");
    
    let vector1 = VectorField::new("title_embedding", vec![0.1, 0.2, 0.3, 0.4, 0.5]);
    let vector2 = VectorField::new("content_embedding", vec![0.2, 0.3, 0.4, 0.5, 0.6]);
    
    let ranker = Ranker::weighted_ranker(vec![0.7, 0.3]);
    println!("✓ Multi-vector setup created");
    println!("  - Title embedding: {:?}", vector1);
    println!("  - Content embedding: {:?}", vector2);
    println!("  - Ranker: {:?}", ranker);

    // Example 6: Building complete search requests
    println!("\n6. Building search requests...");
    
    // Simple search
    let simple_search = client
        .search_builder("books_db", "books_space")
        .with_vector(query_vector)
        .limit(10)
        .build();
    println!("✓ Simple search request: {:?}", simple_search);
    
    // Advanced search with filters and parameters
    let advanced_search = client
        .search_builder("books_db", "books_space")
        .with_vector(threshold_vector)
        .with_filters(filter)
        .with_index_params(hnsw_params)
        .limit(20)
        .include_vectors(true)
        .load_balance("random")
        .build();
    println!("✓ Advanced search request: {:?}", advanced_search);
    
    // Multi-vector search
    let multi_search = client
        .search_builder("books_db", "books_space")
        .with_vector(vector1)
        .with_vector(vector2)
        .with_ranker(ranker)
        .with_index_params(ivfflat_params)
        .limit(15)
        .build();
    println!("✓ Multi-vector search request: {:?}", multi_search);

    // Example 7: Utility filter functions
    println!("\n7. Utility filter functions...");
    
    let range_filter = utils::range_filter("price", Some(10.0), Some(100.0));
    println!("✓ Range filter: {:?}", range_filter);
    
    let text_filter = utils::text_filter("tags", vec!["rust".to_string(), "programming".to_string()]);
    println!("✓ Text filter: {:?}", text_filter);
    
    let single_filter = utils::filter("status", vearch_sdk_rs::models::FilterOperator::In, "active");
    println!("✓ Single filter: {:?}", single_filter);

    // Example 8: Search with brute force
    println!("\n8. Brute force search...");
    
    let brute_search = client
        .search_builder("books_db", "books_space")
        .with_vector(VectorField::new("embedding", vec![0.1, 0.2, 0.3, 0.4, 0.5]))
        .brute_search(true)
        .limit(50)
        .build();
    println!("✓ Brute force search request: {:?}", brute_search);

    println!("\n✓ Vector search example completed!");
    println!("To test actual searches, start a Vearch server on localhost:8817");
    println!("and ensure you have documents with matching vector fields.");

    Ok(())
}
