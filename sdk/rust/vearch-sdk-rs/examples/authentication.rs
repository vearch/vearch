use vearch_sdk_rs::{
    VearchClient, 
    operations::{PrivilegeBuilder, RoleBuilder, UserBuilder, AuthConfigBuilder},
    models::{PrivilegeType, Operator},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Authentication and Authorization Example");
    println!("=====================================");

    // Example 1: 创建带鉴权的客户端
    println!("\n1. 创建带鉴权的客户端...");
    
    let auth_config = AuthConfigBuilder::new("admin", "password123")
        .with_role("root")
        .build();
    
    let mut client = VearchClient::new("http://127.0.0.1:8817")?
        .with_auth(auth_config);
    
    println!("✓ 客户端创建成功，已配置鉴权");

    // Example 2: 登录认证
    println!("\n2. 登录认证...");
    
    // Note: 这需要运行中的Vearch服务器
    // match client.login().await {
    //     Ok(_) => println!("✓ 登录成功，认证令牌: {:?}", client.get_auth_token()),
    //     Err(e) => println!("✗ 登录失败: {}", e),
    // }
    
    println!("✓ 登录流程演示完成");

    // Example 3: 创建角色和权限
    println!("\n3. 创建角色和权限...");
    
    // 创建只读权限
    let read_only_privileges = PrivilegeBuilder::new()
        .document(PrivilegeType::ReadOnly)
        .space(PrivilegeType::ReadOnly)
        .database(PrivilegeType::ReadOnly)
        .build();
    
    let read_only_role = RoleBuilder::new("read_only_user")
        .with_privileges(read_only_privileges)
        .build();
    
    println!("✓ 只读角色创建请求: {:?}", read_only_role);
    
    // 创建读写权限
    let read_write_privileges = PrivilegeBuilder::new()
        .document(PrivilegeType::WriteRead)
        .space(PrivilegeType::WriteRead)
        .database(PrivilegeType::WriteRead)
        .build();
    
    let read_write_role = RoleBuilder::new("read_write_user")
        .with_privileges(read_write_privileges)
        .build();
    
    println!("✓ 读写角色创建请求: {:?}", read_write_role);
    
    // 创建管理员权限
    let admin_privileges = PrivilegeBuilder::new()
        .all_resources(PrivilegeType::WriteRead)
        .build();
    
    let admin_role = RoleBuilder::new("admin_user")
        .with_privileges(admin_privileges)
        .build();
    
    println!("✓ 管理员角色创建请求: {:?}", admin_role);

    // Example 4: 创建用户
    println!("\n4. 创建用户...");
    
    let user1 = UserBuilder::new("john_doe", "password123")
        .with_role("read_only_user")
        .build();
    
    let user2 = UserBuilder::new("jane_smith", "password456")
        .with_role("read_write_user")
        .build();
    
    let admin_user = UserBuilder::new("admin", "admin123")
        .with_role("admin_user")
        .build();
    
    println!("✓ 用户创建请求:");
    println!("  - 只读用户: {:?}", user1);
    println!("  - 读写用户: {:?}", user2);
    println!("  - 管理员用户: {:?}", admin_user);

    // Example 5: 权限管理操作
    println!("\n5. 权限管理操作...");
    
    println!("✓ 可用的权限管理操作:");
    println!("  - client.create_role(request) - 创建角色");
    println!("  - client.delete_role(name) - 删除角色");
    println!("  - client.get_role(name) - 获取角色信息");
    println!("  - client.modify_role(request) - 修改角色权限");
    println!("  - client.create_user(request) - 创建用户");
    println!("  - client.delete_user(name) - 删除用户");
    println!("  - client.get_user(name) - 获取用户信息");
    println!("  - client.modify_user(request) - 修改用户");

    // Example 6: 权限类型示例
    println!("\n6. 权限类型示例...");
    
    let privilege_examples = vec![
        ("WriteOnly", PrivilegeType::WriteOnly),
        ("ReadOnly", PrivilegeType::ReadOnly),
        ("WriteRead", PrivilegeType::WriteRead),
    ];
    
    for (name, privilege) in privilege_examples {
        println!("  - {}: {:?}", name, privilege);
    }

    // Example 7: 资源类型示例
    println!("\n7. 资源类型示例...");
    
    println!("✓ 支持的资源类型:");
    println!("  - ResourceAll - 所有接口资源");
    println!("  - ResourceCluster - 集群接口");
    println!("  - ResourceServer - partition server接口资源");
    println!("  - ResourcePartition - 自定义分片类型");
    println!("  - ResourceDB - 自定义分片字段");
    println!("  - ResourceSpace - 自定义分片范围");
    println!("  - ResourceDocument - 自定义分片类型");
    println!("  - ResourceIndex - 自定义分片字段");
    println!("  - ResourceAlias - 自定义分片范围");
    println!("  - ResourceUser - 自定义分片类型");
    println!("  - ResourceRole - 自定义分片字段");
    println!("  - ResourceConfig - 自定义分片范围");
    println!("  - ResourceCache - 自定义分片范围");

    // Example 8: 完整的鉴权流程
    println!("\n8. 完整的鉴权流程...");
    
    println!("✓ 鉴权流程步骤:");
    println!("  1. 创建客户端: VearchClient::new(url)");
    println!("  2. 配置鉴权: .with_auth(auth_config)");
    println!("  3. 登录认证: client.login().await");
    println!("  4. 执行操作: 所有API调用自动包含认证头");
    println!("  5. 检查状态: client.is_authenticated()");
    println!("  6. 清除认证: client.clear_auth()");

    // Example 9: 鉴权配置构建器
    println!("\n9. 鉴权配置构建器...");
    
    let auth_configs = vec![
        AuthConfigBuilder::new("user1", "pass1").build(),
        AuthConfigBuilder::new("user2", "pass2").with_role("admin").build(),
        AuthConfigBuilder::new("guest", "guest123").build(),
    ];
    
    for (i, config) in auth_configs.iter().enumerate() {
        println!("  - 配置 {}: {:?}", i + 1, config);
    }

    println!("\n✓ 鉴权操作示例完成!");
    println!("要测试实际操作，请启动Vearch服务器在localhost:8817");
    println!("并确保有适当的权限来创建角色和用户。");
    println!("注意：鉴权功能需要Vearch服务器启用RBAC模式。");

    Ok(())
}
