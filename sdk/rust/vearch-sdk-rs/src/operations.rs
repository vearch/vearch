use crate::models::*;
use serde_json::Value;
use std::collections::HashMap;

/// Builder for filter conditions
pub struct FilterBuilder {
    conditions: Vec<FilterCondition>,
}

impl FilterBuilder {
    /// Create a new filter builder
    pub fn new() -> Self {
        Self {
            conditions: Vec::new(),
        }
    }

    /// Add a greater than condition
    pub fn gt(mut self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.conditions.push(FilterCondition {
            field: field.into(),
            operator: FilterOperator::GreaterThan,
            value: value.into(),
        });
        self
    }

    /// Add a greater than or equal condition
    pub fn gte(mut self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.conditions.push(FilterCondition {
            field: field.into(),
            operator: FilterOperator::GreaterThanOrEqual,
            value: value.into(),
        });
        self
    }

    /// Add a less than condition
    pub fn lt(mut self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.conditions.push(FilterCondition {
            field: field.into(),
            operator: FilterOperator::LessThan,
            value: value.into(),
        });
        self
    }

    /// Add a less than or equal condition
    pub fn lte(mut self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.conditions.push(FilterCondition {
            field: field.into(),
            operator: FilterOperator::LessThanOrEqual,
            value: value.into(),
        });
        self
    }

    /// Add an IN condition
    pub fn in_values(mut self, field: impl Into<String>, values: Vec<impl Into<Value>>) -> Self {
        let values: Vec<Value> = values.into_iter().map(|v| v.into()).collect();
        self.conditions.push(FilterCondition {
            field: field.into(),
            operator: FilterOperator::In,
            value: Value::Array(values),
        });
        self
    }

    /// Add a NOT IN condition
    pub fn not_in(mut self, field: impl Into<String>, values: Vec<impl Into<Value>>) -> Self {
        let values: Vec<Value> = values.into_iter().map(|v| v.into()).collect();
        self.conditions.push(FilterCondition {
            field: field.into(),
            operator: FilterOperator::NotIn,
            value: Value::Array(values),
        });
        self
    }

    /// Build the filter
    pub fn build(self) -> Filter {
        Filter::and(self.conditions)
    }
}

impl Default for FilterBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for index parameters
pub struct IndexParamsBuilder {
    params: IndexParams,
}

impl IndexParamsBuilder {
    /// Create a new index params builder
    pub fn new() -> Self {
        Self {
            params: IndexParams::new(),
        }
    }

    /// Set metric type
    pub fn metric_type(mut self, metric_type: impl Into<String>) -> Self {
        self.params.metric_type = Some(metric_type.into());
        self
    }

    /// Set ef_search for HNSW
    pub fn ef_search(mut self, ef_search: i32) -> Self {
        self.params.ef_search = Some(ef_search);
        self
    }

    /// Set nprobe for IVFFLAT
    pub fn nprobe(mut self, nprobe: i32) -> Self {
        self.params.nprobe = Some(nprobe);
        self
    }

    /// Set parallel_on_queries
    pub fn parallel_on_queries(mut self, parallel: i32) -> Self {
        self.params.parallel_on_queries = Some(parallel);
        self
    }

    /// Build the index parameters
    pub fn build(self) -> IndexParams {
        self.params
    }
}

impl Default for IndexParamsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for search requests
pub struct SearchRequestBuilder {
    request: SearchRequest,
}

impl SearchRequestBuilder {
    /// Create a new search request builder
    pub fn new(db_name: impl Into<String>, space_name: impl Into<String>) -> Self {
        Self {
            request: SearchRequest {
                db_name: db_name.into(),
                space_name: space_name.into(),
                vectors: Vec::new(),
                filters: None,
                index_params: None,
                ranker: None,
                is_brute_search: None,
                vector_value: None,
                load_balance: None,
                limit: None,
            },
        }
    }

    /// Add a vector field
    pub fn with_vector(mut self, vector: VectorField) -> Self {
        self.request.vectors.push(vector);
        self
    }

    /// Set filters
    pub fn with_filters(mut self, filters: Filter) -> Self {
        self.request.filters = Some(filters);
        self
    }

    /// Set index parameters
    pub fn with_index_params(mut self, params: IndexParams) -> Self {
        self.request.index_params = Some(params);
        self
    }

    /// Set ranker
    pub fn with_ranker(mut self, ranker: Ranker) -> Self {
        self.request.ranker = Some(ranker);
        self
    }

    /// Set brute search flag
    pub fn brute_search(mut self, brute: bool) -> Self {
        self.request.is_brute_search = Some(if brute { 1 } else { 0 });
        self
    }

    /// Set vector value flag
    pub fn include_vectors(mut self, include: bool) -> Self {
        self.request.vector_value = Some(include);
        self
    }

    /// Set load balance strategy
    pub fn load_balance(mut self, strategy: impl Into<String>) -> Self {
        self.request.load_balance = Some(strategy.into());
        self
    }

    /// Set result limit
    pub fn limit(mut self, limit: i32) -> Self {
        self.request.limit = Some(limit);
        self
    }

    /// Build the search request
    pub fn build(self) -> SearchRequest {
        self.request
    }
}

/// Builder for query requests
pub struct QueryRequestBuilder {
    request: QueryRequest,
}

impl QueryRequestBuilder {
    /// Create a new query request builder
    pub fn new(db_name: impl Into<String>, space_name: impl Into<String>) -> Self {
        Self {
            request: QueryRequest {
                db_name: db_name.into(),
                space_name: space_name.into(),
                document_ids: None,
                partition_id: None,
                filters: None,
                fields: None,
                vector_value: None,
                limit: None,
            },
        }
    }

    /// Set document IDs
    pub fn with_ids(mut self, ids: Vec<String>) -> Self {
        self.request.document_ids = Some(ids);
        self
    }

    /// Set partition ID
    pub fn with_partition(mut self, partition_id: i32) -> Self {
        self.request.partition_id = Some(partition_id);
        self
    }

    /// Set filters
    pub fn with_filters(mut self, filters: Filter) -> Self {
        self.request.filters = Some(filters);
        self
    }

    /// Set fields to return
    pub fn with_fields(mut self, fields: Vec<String>) -> Self {
        self.request.fields = Some(fields);
        self
    }

    /// Set vector value flag
    pub fn include_vectors(mut self, include: bool) -> Self {
        self.request.vector_value = Some(include);
        self
    }

    /// Set result limit
    pub fn limit(mut self, limit: i32) -> Self {
        self.request.limit = Some(limit);
        self
    }

    /// Build the query request
    pub fn build(self) -> QueryRequest {
        self.request
    }
}

/// Builder for delete requests
pub struct DeleteRequestBuilder {
    request: DeleteRequest,
}

impl DeleteRequestBuilder {
    /// Create a new delete request builder
    pub fn new(db_name: impl Into<String>, space_name: impl Into<String>) -> Self {
        Self {
            request: DeleteRequest {
                db_name: db_name.into(),
                space_name: space_name.into(),
                document_ids: None,
                filters: None,
                limit: None,
            },
        }
    }

    /// Set document IDs to delete
    pub fn with_ids(mut self, ids: Vec<String>) -> Self {
        self.request.document_ids = Some(ids);
        self
    }

    /// Set filters for deletion
    pub fn with_filters(mut self, filters: Filter) -> Self {
        self.request.filters = Some(filters);
        self
    }

    /// Set deletion limit
    pub fn limit(mut self, limit: i32) -> Self {
        self.request.limit = Some(limit);
        self
    }

    /// Build the delete request
    pub fn build(self) -> DeleteRequest {
        self.request
    }
}

/// Utility functions for common operations
pub mod utils {
    use super::*;

    /// Create a simple filter for a single condition
    pub fn filter(field: impl Into<String>, operator: FilterOperator, value: impl Into<Value>) -> Filter {
        Filter::and(vec![FilterCondition {
            field: field.into(),
            operator,
            value: value.into(),
        }])
    }

    /// Create a range filter
    pub fn range_filter(
        field: impl Into<String> + Clone,
        min: Option<impl Into<Value>>,
        max: Option<impl Into<Value>>,
    ) -> Filter {
        let mut conditions = Vec::new();
        let field_string = field.into();
        
        if let Some(min_val) = min {
            conditions.push(FilterCondition {
                field: field_string.clone(),
                operator: FilterOperator::GreaterThanOrEqual,
                value: min_val.into(),
            });
        }
        
        if let Some(max_val) = max {
            conditions.push(FilterCondition {
                field: field_string,
                operator: FilterOperator::LessThanOrEqual,
                value: max_val.into(),
            });
        }
        
        Filter::and(conditions)
    }

    /// Create a text search filter
    pub fn text_filter(field: impl Into<String>, values: Vec<String>) -> Filter {
        Filter::and(vec![FilterCondition {
            field: field.into(),
            operator: FilterOperator::In,
            value: Value::Array(values.into_iter().map(Value::String).collect()),
        }])
    }
}

// ===== 库操作构建器 =====

/// 库操作构建器
pub struct DatabaseBuilder {
    name: String,
    description: Option<String>,
}

impl DatabaseBuilder {
    /// 创建新的库构建器
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: None,
        }
    }

    /// 设置库描述
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// 构建创建库请求
    pub fn build(self) -> crate::models::CreateDatabaseRequest {
        crate::models::CreateDatabaseRequest {
            name: self.name,
            description: self.description,
        }
    }
}

// ===== 表空间操作构建器 =====

/// 字段定义构建器
pub struct FieldBuilder {
    name: String,
    field_type: crate::models::FieldType,
    dimension: Option<usize>,
    model_id: Option<String>,
    format: Option<String>,
    element_type: Option<crate::models::FieldType>,
    max_length: Option<usize>,
    index: Option<bool>,
    store: Option<bool>,
}

impl FieldBuilder {
    /// 创建新的字段构建器
    pub fn new(name: impl Into<String>, field_type: crate::models::FieldType) -> Self {
        Self {
            name: name.into(),
            field_type,
            dimension: None,
            model_id: None,
            format: None,
            element_type: None,
            max_length: None,
            index: None,
            store: None,
        }
    }

    /// 设置向量维度
    pub fn with_dimension(mut self, dimension: usize) -> Self {
        self.dimension = Some(dimension);
        self
    }

    /// 设置模型ID
    pub fn with_model_id(mut self, model_id: impl Into<String>) -> Self {
        self.model_id = Some(model_id.into());
        self
    }

    /// 设置格式
    pub fn with_format(mut self, format: impl Into<String>) -> Self {
        self.format = Some(format.into());
        self
    }

    /// 设置数组元素类型
    pub fn with_element_type(mut self, element_type: crate::models::FieldType) -> Self {
        self.element_type = Some(element_type);
        self
    }

    /// 设置最大长度
    pub fn with_max_length(mut self, max_length: usize) -> Self {
        self.max_length = Some(max_length);
        self
    }

    /// 设置是否索引
    pub fn with_index(mut self, index: bool) -> Self {
        self.index = Some(index);
        self
    }

    /// 设置是否存储
    pub fn with_store(mut self, store: bool) -> Self {
        self.store = Some(store);
        self
    }

    /// 构建字段定义
    pub fn build(self) -> (String, crate::models::FieldDefinition) {
        let field_def = crate::models::FieldDefinition {
            field_type: self.field_type,
            dimension: self.dimension,
            model_id: self.model_id,
            format: self.format,
            element_type: self.element_type,
            max_length: self.max_length,
            index: self.index,
            store: self.store,
        };
        (self.name, field_def)
    }
}

/// 索引配置构建器
pub struct IndexBuilder {
    index_type: String,
    nlist: Option<i32>,
    m: Option<i32>,
    ef_construction: Option<i32>,
    ef_search: Option<i32>,
    metric_type: Option<String>,
    nprobe: Option<i32>,
    parallel_on_queries: Option<i32>,
}

impl IndexBuilder {
    /// 创建新的索引构建器
    pub fn new(index_type: impl Into<String>) -> Self {
        Self {
            index_type: index_type.into(),
            nlist: None,
            m: None,
            ef_construction: None,
            ef_search: None,
            metric_type: None,
            nprobe: None,
            parallel_on_queries: None,
        }
    }

    /// 设置nlist参数（IVFFLAT）
    pub fn with_nlist(mut self, nlist: i32) -> Self {
        self.nlist = Some(nlist);
        self
    }

    /// 设置m参数（HNSW）
    pub fn with_m(mut self, m: i32) -> Self {
        self.m = Some(m);
        self
    }

    /// 设置ef_construction参数（HNSW）
    pub fn with_ef_construction(mut self, ef_construction: i32) -> Self {
        self.ef_construction = Some(ef_construction);
        self
    }

    /// 设置ef_search参数（HNSW）
    pub fn with_ef_search(mut self, ef_search: i32) -> Self {
        self.ef_search = Some(ef_search);
        self
    }

    /// 设置度量类型
    pub fn with_metric_type(mut self, metric_type: impl Into<String>) -> Self {
        self.metric_type = Some(metric_type.into());
        self
    }

    /// 设置nprobe参数（IVFFLAT）
    pub fn with_nprobe(mut self, nprobe: i32) -> Self {
        self.nprobe = Some(nprobe);
        self
    }

    /// 设置parallel_on_queries参数
    pub fn with_parallel_on_queries(mut self, parallel: i32) -> Self {
        self.parallel_on_queries = Some(parallel);
        self
    }

    /// 构建索引配置
    pub fn build(self) -> crate::models::IndexConfig {
        crate::models::IndexConfig {
            index_type: self.index_type,
            nlist: self.nlist,
            m: self.m,
            ef_construction: self.ef_construction,
            ef_search: self.ef_search,
            metric_type: self.metric_type,
            nprobe: self.nprobe,
            parallel_on_queries: self.parallel_on_queries,
        }
    }
}

/// 表空间构建器
pub struct SpaceBuilder {
    name: String,
    fields: HashMap<String, crate::models::FieldDefinition>,
    index: Option<crate::models::IndexConfig>,
    partition_num: Option<i32>,
    replica_num: Option<i32>,
    description: Option<String>,
}

impl SpaceBuilder {
    /// 创建新的表空间构建器
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            fields: HashMap::new(),
            index: None,
            partition_num: None,
            replica_num: None,
            description: None,
        }
    }

    /// 添加字段
    pub fn with_field(mut self, field: (String, crate::models::FieldDefinition)) -> Self {
        self.fields.insert(field.0, field.1);
        self
    }

    /// 设置索引配置
    pub fn with_index(mut self, index: crate::models::IndexConfig) -> Self {
        self.index = Some(index);
        self
    }

    /// 设置分区数量
    pub fn with_partition_num(mut self, partition_num: i32) -> Self {
        self.partition_num = Some(partition_num);
        self
    }

    /// 设置副本数量
    pub fn with_replica_num(mut self, replica_num: i32) -> Self {
        self.replica_num = Some(replica_num);
        self
    }

    /// 设置描述
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// 构建创建表空间请求
    pub fn build(self) -> crate::models::CreateSpaceRequest {
        crate::models::CreateSpaceRequest {
            name: self.name,
            fields: self.fields,
            index: self.index,
            partition_num: self.partition_num,
            replica_num: self.replica_num,
            description: self.description,
        }
    }
}

/// 便捷的字段创建函数
pub mod field_utils {
    use super::*;

    /// 创建字符串字段
    pub fn string_field(name: impl Into<String>) -> FieldBuilder {
        FieldBuilder::new(name, crate::models::FieldType::String)
    }

    /// 创建整数字段
    pub fn int_field(name: impl Into<String>) -> FieldBuilder {
        FieldBuilder::new(name, crate::models::FieldType::Integer)
    }

    /// 创建浮点数字段
    pub fn float_field(name: impl Into<String>) -> FieldBuilder {
        FieldBuilder::new(name, crate::models::FieldType::Float)
    }

    /// 创建双精度浮点数字段
    pub fn double_field(name: impl Into<String>) -> FieldBuilder {
        FieldBuilder::new(name, crate::models::FieldType::Double)
    }

    /// 创建布尔字段
    pub fn bool_field(name: impl Into<String>) -> FieldBuilder {
        FieldBuilder::new(name, crate::models::FieldType::Boolean)
    }

    /// 创建向量字段
    pub fn vector_field(name: impl Into<String>, dimension: usize) -> FieldBuilder {
        FieldBuilder::new(name, crate::models::FieldType::Vector)
            .with_dimension(dimension)
    }

    /// 创建数组字段
    pub fn array_field(name: impl Into<String>, element_type: crate::models::FieldType) -> FieldBuilder {
        FieldBuilder::new(name, crate::models::FieldType::Array)
            .with_element_type(element_type)
    }

    /// 创建日期字段
    pub fn date_field(name: impl Into<String>) -> FieldBuilder {
        FieldBuilder::new(name, crate::models::FieldType::Date)
    }

    /// 创建地理字段
    pub fn geo_field(name: impl Into<String>) -> FieldBuilder {
        FieldBuilder::new(name, crate::models::FieldType::Geo)
    }
}

// ===== 鉴权操作构建器 =====

/// 权限构建器
pub struct PrivilegeBuilder {
    privileges: crate::models::Privileges,
}

impl PrivilegeBuilder {
    /// 创建新的权限构建器
    pub fn new() -> Self {
        Self {
            privileges: HashMap::new(),
        }
    }

    /// 添加所有资源权限
    pub fn all_resources(mut self, privilege: crate::models::PrivilegeType) -> Self {
        self.privileges.insert(crate::models::ResourceType::ResourceAll, privilege);
        self
    }

    /// 添加集群资源权限
    pub fn cluster(mut self, privilege: crate::models::PrivilegeType) -> Self {
        self.privileges.insert(crate::models::ResourceType::ResourceCluster, privilege);
        self
    }

    /// 添加服务器资源权限
    pub fn server(mut self, privilege: crate::models::PrivilegeType) -> Self {
        self.privileges.insert(crate::models::ResourceType::ResourceServer, privilege);
        self
    }

    /// 添加分片资源权限
    pub fn partition(mut self, privilege: crate::models::PrivilegeType) -> Self {
        self.privileges.insert(crate::models::ResourceType::ResourcePartition, privilege);
        self
    }

    /// 添加数据库资源权限
    pub fn database(mut self, privilege: crate::models::PrivilegeType) -> Self {
        self.privileges.insert(crate::models::ResourceType::ResourceDB, privilege);
        self
    }

    /// 添加表空间资源权限
    pub fn space(mut self, privilege: crate::models::PrivilegeType) -> Self {
        self.privileges.insert(crate::models::ResourceType::ResourceSpace, privilege);
        self
    }

    /// 添加文档资源权限
    pub fn document(mut self, privilege: crate::models::PrivilegeType) -> Self {
        self.privileges.insert(crate::models::ResourceType::ResourceDocument, privilege);
        self
    }

    /// 添加索引资源权限
    pub fn index(mut self, privilege: crate::models::PrivilegeType) -> Self {
        self.privileges.insert(crate::models::ResourceType::ResourceIndex, privilege);
        self
    }

    /// 添加别名资源权限
    pub fn alias(mut self, privilege: crate::models::PrivilegeType) -> Self {
        self.privileges.insert(crate::models::ResourceType::ResourceAlias, privilege);
        self
    }

    /// 添加用户资源权限
    pub fn user(mut self, privilege: crate::models::PrivilegeType) -> Self {
        self.privileges.insert(crate::models::ResourceType::ResourceUser, privilege);
        self
    }

    /// 添加角色资源权限
    pub fn role(mut self, privilege: crate::models::PrivilegeType) -> Self {
        self.privileges.insert(crate::models::ResourceType::ResourceRole, privilege);
        self
    }

    /// 添加配置资源权限
    pub fn config(mut self, privilege: crate::models::PrivilegeType) -> Self {
        self.privileges.insert(crate::models::ResourceType::ResourceConfig, privilege);
        self
    }

    /// 添加缓存资源权限
    pub fn cache(mut self, privilege: crate::models::PrivilegeType) -> Self {
        self.privileges.insert(crate::models::ResourceType::ResourceCache, privilege);
        self
    }

    /// 构建权限映射
    pub fn build(self) -> crate::models::Privileges {
        self.privileges
    }
}

impl Default for PrivilegeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// 角色构建器
pub struct RoleBuilder {
    name: String,
    privileges: crate::models::Privileges,
}

impl RoleBuilder {
    /// 创建新的角色构建器
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            privileges: HashMap::new(),
        }
    }

    /// 设置权限
    pub fn with_privileges(mut self, privileges: crate::models::Privileges) -> Self {
        self.privileges = privileges;
        self
    }

    /// 构建创建角色请求
    pub fn build(self) -> crate::models::CreateRoleRequest {
        crate::models::CreateRoleRequest {
            name: self.name,
            privileges: self.privileges,
        }
    }
}

/// 用户构建器
pub struct UserBuilder {
    name: String,
    password: String,
    role_name: String,
}

impl UserBuilder {
    /// 创建新的用户构建器
    pub fn new(name: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            password: password.into(),
            role_name: String::new(),
        }
    }

    /// 设置角色
    pub fn with_role(mut self, role_name: impl Into<String>) -> Self {
        self.role_name = role_name.into();
        self
    }

    /// 构建创建用户请求
    pub fn build(self) -> crate::models::CreateUserRequest {
        crate::models::CreateUserRequest {
            name: self.name,
            password: self.password,
            role_name: self.role_name,
        }
    }
}

/// 鉴权配置构建器
pub struct AuthConfigBuilder {
    username: String,
    password: String,
    role_name: Option<String>,
}

impl AuthConfigBuilder {
    /// 创建新的鉴权配置构建器
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
            role_name: None,
        }
    }

    /// 设置角色
    pub fn with_role(mut self, role_name: impl Into<String>) -> Self {
        self.role_name = Some(role_name.into());
        self
    }

    /// 构建鉴权配置
    pub fn build(self) -> crate::models::AuthConfig {
        crate::models::AuthConfig {
            username: self.username,
            password: self.password,
            role_name: self.role_name,
        }
    }
}
