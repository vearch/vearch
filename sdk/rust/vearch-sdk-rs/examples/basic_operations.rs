use vearch_sdk_rs::{VearchClient, Document};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Basic Operations Example");
    println!("=======================");

    // Create a client
    let client = VearchClient::new("http://127.0.0.1:8817")?;
    println!("✓ Client created");

    // Example 1: Create and insert documents
    println!("\n1. Creating and inserting documents...");
    
    let doc1 = Document::new()
        .with_field("title", "Introduction to Rust")
        .with_field("author", "Steve Klabnik")
        .with_field("year", 2018)
        .with_field("category", "programming")
        .with_field("rating", 4.8)
        .with_vector("embedding", vec![0.1, 0.2, 0.3, 0.4, 0.5]);

    let doc2 = Document::new()
        .with_field("title", "The Rust Programming Language")
        .with_field("author", "Carol Nichols")
        .with_field("year", 2020)
        .with_field("category", "programming")
        .with_field("rating", 4.9)
        .with_vector("embedding", vec![0.2, 0.3, 0.4, 0.5, 0.6]);

    let doc3 = Document::new()
        .with_field("title", "Zero to Production in Rust")
        .with_field("author", "Luca Palmieri")
        .with_field("year", 2022)
        .with_field("category", "programming")
        .with_field("rating", 4.7)
        .with_vector("embedding", vec![0.3, 0.4, 0.5, 0.6, 0.7]);

    let documents = vec![doc1, doc2, doc3];
    
    // Note: This would fail without a running Vearch server
    // Uncomment the following lines when you have a server running:
    
    match client.upsert("books_db", "books_space", documents).await {
        Ok(result) => println!("✓ Inserted {} documents", result.total),
        Err(e) => println!("✗ Insert failed: {}", e),
    }
    

    println!("✓ Documents prepared (insertion skipped - no server)");

    // Example 2: Create documents with specific IDs
    println!("\n2. Creating documents with specific IDs...");
    
    let doc_with_id = Document::with_id("book_001")
        .with_field("title", "Rust in Action")
        .with_field("author", "Tim McNamara")
        .with_field("year", 2021)
        .with_field("category", "programming")
        .with_field("rating", 4.6)
        .with_vector("embedding", vec![0.4, 0.5, 0.6, 0.7, 0.8]);

    println!("✓ Document with ID created: {:?}", doc_with_id);

    // Example 3: Demonstrate field access
    println!("\n3. Accessing document fields...");
    
    if let Some(title) = doc_with_id.get_field("title") {
        println!("✓ Title: {:?}", title);
    }
    
    if let Some(rating) = doc_with_id.get_field("rating") {
        println!("✓ Rating: {:?}", rating);
    }
    
    if let Some(embedding) = doc_with_id.get_vector("embedding") {
        println!("✓ Embedding dimension: {}", embedding.len());
    }

    // Example 4: Different field types
    println!("\n4. Different field types...");
    
    let mixed_doc = Document::new()
        .with_field("string_field", "Hello World")
        .with_field("int_field", 42)
        .with_field("float_field", 3.14159)
        .with_field("bool_field", true)
        .with_field("string_array", vec!["tag1".to_string(), "tag2".to_string(), "tag3".to_string()])
        .with_field("int_array", vec![1, 2, 3, 4, 5])
        .with_vector("vector_field", vec![0.1, 0.2, 0.3]);

    println!("✓ Mixed type document created");
    println!("  - String: {:?}", mixed_doc.get_field("string_field"));
    println!("  - Integer: {:?}", mixed_doc.get_field("int_field"));
    println!("  - Float: {:?}", mixed_doc.get_field("float_field"));
    println!("  - Boolean: {:?}", mixed_doc.get_field("bool_field"));
    println!("  - String Array: {:?}", mixed_doc.get_field("string_array"));
    println!("  - Integer Array: {:?}", mixed_doc.get_field("int_array"));
    println!("  - Vector: {:?}", mixed_doc.get_vector("vector_field"));

    println!("\n✓ Basic operations example completed!");
    println!("To test actual operations, start a Vearch server on localhost:8817");

    Ok(())
}
