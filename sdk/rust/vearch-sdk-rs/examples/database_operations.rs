use vearch_sdk_rs::{
    VearchClient, 
    operations::{field_utils, DatabaseBuilder, SpaceBuilder, IndexBuilder},
    models::FieldType,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Database and Space Operations Example");
    println!("====================================");

    // Create a client
    let client = VearchClient::new("http://localhost:8817")?;
    println!("✓ Client created");

    // Example 1: 创建数据库
    println!("\n1. 创建数据库...");
    
    let db_request = DatabaseBuilder::new("books_db")
        .with_description("图书数据库")
        .build();
    
    println!("✓ 数据库创建请求: {:?}", db_request);
    
    // Note: 这需要运行中的Vearch服务器
    // match client.create_database(db_request).await {
    //     Ok(result) => println!("✓ 数据库创建成功: {:?}", result),
    //     Err(e) => println!("✗ 数据库创建失败: {}", e),
    // }

    // Example 2: 创建表空间 - 使用构建器模式
    println!("\n2. 创建表空间...");
    
    // 创建字段定义
    let title_field = field_utils::string_field("title")
        .with_index(true)
        .with_store(true)
        .build();
    
    let author_field = field_utils::string_field("author")
        .with_index(true)
        .with_store(true)
        .build();
    
    let year_field = field_utils::int_field("year")
        .with_index(true)
        .with_store(true)
        .build();
    
    let rating_field = field_utils::float_field("rating")
        .with_index(true)
        .with_store(true)
        .build();
    
    let category_field = field_utils::string_field("category")
        .with_index(true)
        .with_store(true)
        .build();
    
    let embedding_field = field_utils::vector_field("embedding", 128)
        .with_index(true)
        .with_store(false)
        .build();
    
    let tags_field = field_utils::array_field("tags", FieldType::String)
        .with_max_length(10)
        .with_index(false)
        .with_store(true)
        .build();
    
    // 创建索引配置
    let index_config = IndexBuilder::new("HNSW")
        .with_m(16)
        .with_ef_construction(200)
        .with_ef_search(64)
        .with_metric_type("L2")
        .build();
    
    // 创建表空间
    let space_request = SpaceBuilder::new("books_space")
        .with_field(title_field)
        .with_field(author_field)
        .with_field(year_field)
        .with_field(rating_field)
        .with_field(category_field)
        .with_field(embedding_field)
        .with_field(tags_field)
        .with_index(index_config)
        .with_partition_num(8)
        .with_replica_num(1)
        .with_description("图书向量搜索表空间")
        .build();
    
    println!("✓ 表空间创建请求: {:?}", space_request);
    
    // Note: 这需要运行中的Vearch服务器
    // match client.create_space("books_db", space_request).await {
    //     Ok(result) => println!("✓ 表空间创建成功: {:?}", result),
    //     Err(e) => println!("✗ 表空间创建失败: {}", e),
    // }

    // Example 3: 不同的索引类型配置
    println!("\n3. 不同索引类型配置...");
    
    // HNSW索引
    let hnsw_index = IndexBuilder::new("HNSW")
        .with_m(16)
        .with_ef_construction(200)
        .with_ef_search(64)
        .with_metric_type("L2")
        .build();
    println!("✓ HNSW索引配置: {:?}", hnsw_index);
    
    // IVFFLAT索引
    let ivfflat_index = IndexBuilder::new("IVFFLAT")
        .with_nlist(1000)
        .with_nprobe(80)
        .with_metric_type("L2")
        .with_parallel_on_queries(1)
        .build();
    println!("✓ IVFFLAT索引配置: {:?}", ivfflat_index);
    
    // FLAT索引
    let flat_index = IndexBuilder::new("FLAT")
        .with_metric_type("L2")
        .build();
    println!("✓ FLAT索引配置: {:?}", flat_index);

    // Example 4: 复杂的表空间结构
    println!("\n4. 复杂表空间结构...");
    
    let complex_space = SpaceBuilder::new("complex_space")
        .with_field(field_utils::string_field("id").with_index(true).build())
        .with_field(field_utils::string_field("name").with_index(true).build())
        .with_field(field_utils::int_field("age").with_index(true).build())
        .with_field(field_utils::float_field("score").with_index(true).build())
        .with_field(field_utils::bool_field("active").with_index(true).build())
        .with_field(field_utils::vector_field("image_embedding", 512).with_index(true).build())
        .with_field(field_utils::vector_field("text_embedding", 256).with_index(true).build())
        .with_field(field_utils::array_field("categories", FieldType::String).with_max_length(20).build())
        .with_field(field_utils::date_field("created_at").with_index(true).build())
        .with_field(field_utils::geo_field("location").with_index(true).build())
        .with_index(hnsw_index)
        .with_partition_num(16)
        .with_replica_num(2)
        .with_description("复杂多模态搜索表空间")
        .build();
    
    println!("✓ 复杂表空间创建请求: {:?}", complex_space);

    // Example 5: 字段类型示例
    println!("\n5. 字段类型示例...");
    
    let field_examples = vec![
        ("string_field", field_utils::string_field("name").build()),
        ("int_field", field_utils::int_field("age").build()),
        ("float_field", field_utils::float_field("score").build()),
        ("double_field", field_utils::double_field("price").build()),
        ("bool_field", field_utils::bool_field("is_active").build()),
        ("vector_field", field_utils::vector_field("embedding", 128).build()),
        ("array_field", field_utils::array_field("tags", FieldType::String).build()),
        ("date_field", field_utils::date_field("created_at").build()),
        ("geo_field", field_utils::geo_field("location").build()),
    ];
    
    for (field_type, (name, field_def)) in field_examples {
        println!("  - {}: {} -> {:?}", field_type, name, field_def.field_type);
    }

    // Example 6: 数据库和表空间管理操作
    println!("\n6. 数据库和表空间管理操作...");
    
    println!("✓ 可用的管理操作:");
    println!("  - client.create_database(request) - 创建数据库");
    println!("  - client.delete_database(name) - 删除数据库");
    println!("  - client.list_databases() - 列出所有数据库");
    println!("  - client.get_database(name) - 获取数据库信息");
    println!("  - client.create_space(db_name, request) - 创建表空间");
    println!("  - client.delete_space(db_name, space_name) - 删除表空间");
    println!("  - client.list_spaces(db_name) - 列出数据库中的所有表空间");
    println!("  - client.get_space(db_name, space_name) - 获取表空间信息");
    println!("  - client.get_space_stats(db_name, space_name) - 获取表空间统计信息");
    println!("  - client.rebuild_index(db_name, space_name) - 重建表空间索引");

    println!("\n✓ 数据库和表空间操作示例完成!");
    println!("要测试实际操作，请启动Vearch服务器在localhost:8817");
    println!("并确保有适当的权限来创建数据库和表空间。");

    Ok(())
}
