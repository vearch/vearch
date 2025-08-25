use vearch_sdk_rs::{VearchClient, Document, VectorField, FilterBuilder, IndexParamsBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Vearch Rust SDK Example");
    println!("========================");

    // Create a new client
    let client = VearchClient::new("http://localhost:8817")?;
    println!("✓ Client created successfully");

    // Health check
    match client.health_check().await {
        Ok(true) => println!("✓ Vearch server is healthy"),
        Ok(false) => println!("⚠ Vearch server health check failed"),
        Err(e) => println!("✗ Health check error: {}", e),
    }

    // Example: Create a document
    let doc = Document::new()
        .with_field("title", "Sample Document")
        .with_field("content", "This is a sample document for testing")
        .with_field("category", "test")
        .with_field("score", 95.5)
        .with_vector("embedding", vec![0.1, 0.2, 0.3, 0.4, 0.5]);

    println!("✓ Document created: {:?}", doc);

    // Example: Create a filter
    let filter = FilterBuilder::new()
        .gte("score", 90.0)
        .in_values("category", vec!["test", "demo"])
        .build();

    println!("✓ Filter created: {:?}", filter);

    // Example: Create index parameters
    let index_params = IndexParamsBuilder::new()
        .metric_type("L2")
        .ef_search(64)
        .build();

    println!("✓ Index parameters created: {:?}", index_params);

    // Example: Create a vector field for search
    let search_vector = VectorField::new("embedding", vec![0.1, 0.2, 0.3, 0.4, 0.5])
        .with_min_score(0.8);

    println!("✓ Search vector created: {:?}", search_vector);

    // Example: Build a search request
    let search_request = client
        .search_builder("my_db", "my_space")
        .with_vector(search_vector)
        .with_filters(filter)
        .with_index_params(index_params)
        .limit(10)
        .build();

    println!("✓ Search request built: {:?}", search_request);

    println!("\nSDK is ready to use! 🚀");
    println!("Note: This example shows the SDK structure without making actual API calls.");
    println!("To test with a real Vearch server, ensure it's running on localhost:8817");

    Ok(())
}
