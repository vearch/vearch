use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Document representation for Vearch
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _id: Option<String>,
    #[serde(flatten)]
    pub fields: HashMap<String, FieldValue>,
}

impl Document {
    /// Create a new empty document
    pub fn new() -> Self {
        Self {
            _id: None,
            fields: HashMap::new(),
        }
    }

    /// Create a new document with a specific ID
    pub fn with_id(id: impl Into<String>) -> Self {
        Self {
            _id: Some(id.into()),
            fields: HashMap::new(),
        }
    }

    /// Add a field to the document
    pub fn with_field(mut self, key: impl Into<String>, value: impl Into<FieldValue>) -> Self {
        self.fields.insert(key.into(), value.into());
        self
    }

    /// Add a vector field to the document
    pub fn with_vector(mut self, key: impl Into<String>, vector: Vec<f32>) -> Self {
        self.fields.insert(key.into(), FieldValue::Vector(vector));
        self
    }

    /// Get a field value
    pub fn get_field(&self, key: &str) -> Option<&FieldValue> {
        self.fields.get(key)
    }

    /// Get a vector field value
    pub fn get_vector(&self, key: &str) -> Option<&Vec<f32>> {
        if let Some(FieldValue::Vector(vector)) = self.fields.get(key) {
            Some(vector)
        } else {
            None
        }
    }
}

impl Default for Document {
    fn default() -> Self {
        Self::new()
    }
}

/// Field value types supported by Vearch
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FieldValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Vector(Vec<f32>),
    StringArray(Vec<String>),
    IntegerArray(Vec<i64>),
    FloatArray(Vec<f64>),
}

impl From<String> for FieldValue {
    fn from(value: String) -> Self {
        FieldValue::String(value)
    }
}

impl From<&str> for FieldValue {
    fn from(value: &str) -> Self {
        FieldValue::String(value.to_string())
    }
}

impl From<i32> for FieldValue {
    fn from(value: i32) -> Self {
        FieldValue::Integer(value as i64)
    }
}

impl From<i64> for FieldValue {
    fn from(value: i64) -> Self {
        FieldValue::Integer(value)
    }
}

impl From<f32> for FieldValue {
    fn from(value: f32) -> Self {
        FieldValue::Float(value as f64)
    }
}

impl From<f64> for FieldValue {
    fn from(value: f64) -> Self {
        FieldValue::Float(value)
    }
}

impl From<bool> for FieldValue {
    fn from(value: bool) -> Self {
        FieldValue::Boolean(value)
    }
}

impl From<Vec<f32>> for FieldValue {
    fn from(value: Vec<f32>) -> Self {
        FieldValue::Vector(value)
    }
}

impl From<Vec<String>> for FieldValue {
    fn from(value: Vec<String>) -> Self {
        FieldValue::StringArray(value)
    }
}

impl From<Vec<i64>> for FieldValue {
    fn from(value: Vec<i64>) -> Self {
        FieldValue::IntegerArray(value)
    }
}

impl From<Vec<f64>> for FieldValue {
    fn from(value: Vec<f64>) -> Self {
        FieldValue::FloatArray(value)
    }
}

/// Vector field for search operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorField {
    pub field: String,
    pub feature: Vec<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_score: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_score: Option<f64>,
}

impl VectorField {
    /// Create a new vector field
    pub fn new(field: impl Into<String>, feature: Vec<f32>) -> Self {
        Self {
            field: field.into(),
            feature,
            min_score: None,
            max_score: None,
        }
    }

    /// Set minimum score threshold
    pub fn with_min_score(mut self, min_score: f64) -> Self {
        self.min_score = Some(min_score);
        self
    }

    /// Set maximum score threshold
    pub fn with_max_score(mut self, max_score: f64) -> Self {
        self.max_score = Some(max_score);
        self
    }
}

/// Filter condition for queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterCondition {
    pub field: String,
    pub operator: FilterOperator,
    pub value: serde_json::Value,
}

/// Filter operators supported by Vearch
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum FilterOperator {
    #[serde(rename = ">")]
    GreaterThan,
    #[serde(rename = ">=")]
    GreaterThanOrEqual,
    #[serde(rename = "<")]
    LessThan,
    #[serde(rename = "<=")]
    LessThanOrEqual,
    #[serde(rename = "IN")]
    In,
    #[serde(rename = "NOT IN")]
    NotIn,
}

/// Filter structure for complex queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Filter {
    pub operator: String, // Currently only supports "AND"
    pub conditions: Vec<FilterCondition>,
}

impl Filter {
    /// Create a new AND filter
    pub fn and(conditions: Vec<FilterCondition>) -> Self {
        Self {
            operator: "AND".to_string(),
            conditions,
        }
    }

    /// Add a condition to the filter
    pub fn with_condition(mut self, condition: FilterCondition) -> Self {
        self.conditions.push(condition);
        self
    }
}

/// Index parameters for search operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metric_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ef_search: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nprobe: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallel_on_queries: Option<i32>,
}

impl IndexParams {
    /// Create new index parameters
    pub fn new() -> Self {
        Self {
            metric_type: None,
            ef_search: None,
            nprobe: None,
            parallel_on_queries: None,
        }
    }

    /// Set metric type (L2, IP, COSINE)
    pub fn with_metric_type(mut self, metric_type: impl Into<String>) -> Self {
        self.metric_type = Some(metric_type.into());
        self
    }

    /// Set ef_search for HNSW index
    pub fn with_ef_search(mut self, ef_search: i32) -> Self {
        self.ef_search = Some(ef_search);
        self
    }

    /// Set nprobe for IVFFLAT index
    pub fn with_nprobe(mut self, nprobe: i32) -> Self {
        self.nprobe = Some(nprobe);
        self
    }
}

impl Default for IndexParams {
    fn default() -> Self {
        Self::new()
    }
}

/// Ranker for multi-vector search
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ranker {
    #[serde(rename = "type")]
    pub ranker_type: String,
    pub params: Vec<f64>,
}

impl Ranker {
    /// Create a weighted ranker
    pub fn weighted_ranker(weights: Vec<f64>) -> Self {
        Self {
            ranker_type: "WeightedRanker".to_string(),
            params: weights,
        }
    }
}

/// Upsert request payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpsertRequest {
    pub db_name: String,
    pub space_name: String,
    pub documents: Vec<Document>,
}

/// Upsert response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpsertResponse {
    pub total: i32,
    pub document_ids: Vec<DocumentId>,
}

/// Document ID in response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentId {
    pub _id: String,
}

/// Query request payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRequest {
    pub db_name: String,
    pub space_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub document_ids: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<Filter>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_value: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<i32>,
}

/// Search request payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchRequest {
    pub db_name: String,
    pub space_name: String,
    pub vectors: Vec<VectorField>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<Filter>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_params: Option<IndexParams>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ranker: Option<Ranker>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_brute_search: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_value: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub load_balance: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<i32>,
}

/// Search response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResponse {
    pub documents: Vec<Vec<SearchResult>>,
}

/// Search result document
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub _id: String,
    pub _score: f64,
    #[serde(flatten)]
    pub fields: HashMap<String, serde_json::Value>,
}

/// Delete request payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteRequest {
    pub db_name: String,
    pub space_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub document_ids: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<Filter>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<i32>,
}

/// Delete response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteResponse {
    pub total: i32,
    pub document_ids: Vec<String>,
}

// ===== 库操作相关结构 =====

/// 创建库请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateDatabaseRequest {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// 库信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseInfo {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub space_count: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub document_count: Option<i64>,
}

/// 库列表响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListDatabasesResponse {
    pub databases: Vec<DatabaseInfo>,
}

/// 库操作响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseResponse {
    pub code: i32,
    pub msg: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

// ===== 表空间操作相关结构 =====

/// 字段类型定义
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    String,
    Integer,
    Float,
    Double,
    Boolean,
    Vector,
    Array,
    Date,
    Geo,
}

/// 向量字段配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorFieldConfig {
    pub dimension: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
}

/// 数组字段配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArrayFieldConfig {
    pub element_type: FieldType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_length: Option<usize>,
}

/// 字段定义
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDefinition {
    #[serde(rename = "type")]
    pub field_type: FieldType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dimension: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub element_type: Option<FieldType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_length: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub store: Option<bool>,
}

/// 索引配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexConfig {
    #[serde(rename = "type")]
    pub index_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nlist: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub m: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ef_construction: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ef_search: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metric_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nprobe: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallel_on_queries: Option<i32>,
}

/// 创建表空间请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSpaceRequest {
    pub name: String,
    pub fields: HashMap<String, FieldDefinition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<IndexConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_num: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replica_num: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// 表空间信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpaceInfo {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub fields: HashMap<String, FieldDefinition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<IndexConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_num: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replica_num: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub document_count: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

/// 表空间列表响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListSpacesResponse {
    pub spaces: Vec<SpaceInfo>,
}

/// 表空间操作响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpaceResponse {
    pub code: i32,
    pub msg: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

/// 表空间统计信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpaceStats {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub document_count: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_size: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_size: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_stats: Option<Vec<PartitionStats>>,
}

/// 分区统计信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionStats {
    pub partition_id: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub document_count: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_size: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_size: Option<i64>,
}

// ===== 鉴权相关结构 =====

/// 权限类型
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum PrivilegeType {
    WriteOnly,
    ReadOnly,
    WriteRead,
}

/// 资源类型
#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub enum ResourceType {
    ResourceAll,
    ResourceCluster,
    ResourceServer,
    ResourcePartition,
    ResourceDB,
    ResourceSpace,
    ResourceDocument,
    ResourceIndex,
    ResourceAlias,
    ResourceUser,
    ResourceRole,
    ResourceConfig,
    ResourceCache,
}

/// 权限映射
pub type Privileges = HashMap<ResourceType, PrivilegeType>;

/// 操作类型
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum Operator {
    Grant,
    Revoke,
}

/// 创建角色请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateRoleRequest {
    pub name: String,
    pub privileges: Privileges,
}

/// 修改角色权限请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModifyRoleRequest {
    pub name: String,
    pub operator: Operator,
    pub privileges: Privileges,
}

/// 角色信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleInfo {
    pub name: String,
    pub privileges: Privileges,
}

/// 角色列表响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListRolesResponse {
    pub roles: Vec<RoleInfo>,
}

/// 创建用户请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateUserRequest {
    pub name: String,
    pub password: String,
    pub role_name: String,
}

/// 修改用户请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModifyUserRequest {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old_password: Option<String>,
}

/// 用户信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserInfo {
    pub name: String,
    pub role_name: String,
}

/// 用户列表响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListUsersResponse {
    pub users: Vec<UserInfo>,
}

/// 鉴权响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthResponse {
    pub code: i32,
    pub msg: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

/// 鉴权配置
#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub username: String,
    pub password: String,
    pub role_name: Option<String>,
}

/// 登录响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginResponse {
    pub code: i32,
    pub msg: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
}
