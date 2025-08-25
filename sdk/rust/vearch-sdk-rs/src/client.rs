use crate::{models::*, operations::*, VearchResult, VearchError};
use reqwest::Client;
use std::time::Duration;
use url::Url;

/// Main client for interacting with Vearch
pub struct VearchClient {
    base_url: Url,
    http_client: Client,
    auth_config: Option<AuthConfig>,
    auth_token: Option<String>,
}

impl VearchClient {
    /// Helper method to parse response text as JSON with proper error handling
    fn parse_response_json(&self, response_text: &str) -> VearchResult<serde_json::Value> {
        // Trim whitespace and check if response is empty
        let trimmed_text = response_text.trim();
        if trimmed_text.is_empty() {
            return Err(VearchError::OperationFailed("Empty response from server".to_string()));
        }
        
        // Try to parse JSON, with better error handling
        match serde_json::from_str(trimmed_text) {
            Ok(value) => Ok(value),
            Err(e) => {
                // Log the response for debugging
                eprintln!("Failed to parse response as JSON: {}", e);
                eprintln!("Response text: {:?}", trimmed_text);
                Err(VearchError::SerializationError(e))
            }
        }
    }

    /// Create a new Vearch client
    pub fn new(base_url: impl Into<String>) -> VearchResult<Self> {
        let base_url = Url::parse(&base_url.into())?;
        let http_client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;

        Ok(Self {
            base_url,
            http_client,
            auth_config: None,
            auth_token: None,
        })
    }

    /// Create a new Vearch client with custom timeout
    pub fn with_timeout(base_url: impl Into<String>, timeout: Duration) -> VearchResult<Self> {
        let base_url = Url::parse(&base_url.into())?;
        let http_client = Client::builder()
            .timeout(timeout)
            .build()?;

        Ok(Self {
            base_url,
            http_client,
            auth_config: None,
            auth_token: None,
        })
    }

    /// Insert or update documents
    pub async fn upsert(
        &self,
        db_name: impl Into<String>,
        space_name: impl Into<String>,
        documents: Vec<Document>,
    ) -> VearchResult<UpsertResponse> {
        let request = UpsertRequest {
            db_name: db_name.into(),
            space_name: space_name.into(),
            documents,
        };

        let response = self
            .add_auth_headers(
                self.http_client
                    .post(self.base_url.join("document/upsert")?)
                    .json(&request)
            )
            .send()
            .await?;

        let response_text = response.text().await?;
        let json = self.parse_response_json(&response_text)?;

        if let Some(code) = json.get("code").and_then(|v| v.as_i64()) {
            if code != 0 {
                return Err(VearchError::from(json));
            }
        }

        let result: UpsertResponse = serde_json::from_value(json)?;
        Ok(result)
    }

    /// Query documents by exact match
    pub async fn query(
        &self,
        _db_name: impl Into<String>,
        _space_name: impl Into<String>,
        request: QueryRequest,
    ) -> VearchResult<Vec<Document>> {
        let response = self
            .add_auth_headers(
                self.http_client
                    .post(self.base_url.join("document/query")?)
                    .json(&request)
            )
            .send()
            .await?;

        let response_text = response.text().await?;
        let json = self.parse_response_json(&response_text)?;

        if let Some(code) = json.get("code").and_then(|v| v.as_i64()) {
            if code != 0 {
                return Err(VearchError::from(json));
            }
        }

        // Parse the response - the exact structure depends on the Vearch version
        // For now, we'll try to extract documents from the response
        if let Some(data) = json.get("data") {
            if let Some(documents) = data.get("documents") {
                let docs: Vec<Document> = serde_json::from_value(documents.clone())?;
                return Ok(docs);
            }
        }

        Ok(Vec::new())
    }

    /// Search for similar vectors
    pub async fn search(
        &self,
        db_name: impl Into<String>,
        space_name: impl Into<String>,
        vectors: Vec<VectorField>,
    ) -> VearchResult<SearchResponse> {
        let request = SearchRequest {
            db_name: db_name.into(),
            space_name: space_name.into(),
            vectors,
            filters: None,
            index_params: None,
            ranker: None,
            is_brute_search: None,
            vector_value: None,
            load_balance: None,
            limit: None,
        };

        self.search_with_request(request).await
    }

    /// Search with a complete request
    pub async fn search_with_request(&self, request: SearchRequest) -> VearchResult<SearchResponse> {
        let response = self
            .add_auth_headers(
                self.http_client
                    .post(self.base_url.join("document/search")?)
                    .json(&request)
            )
            .send()
            .await?;

        let response_text = response.text().await?;
        let json = self.parse_response_json(&response_text)?;

        if let Some(code) = json.get("code").and_then(|v| v.as_i64()) {
            if code != 0 {
                return Err(VearchError::from(json));
            }
        }

        let result: SearchResponse = serde_json::from_value(json)?;
        Ok(result)
    }

    /// Delete documents
    pub async fn delete(
        &self,
        db_name: impl Into<String>,
        space_name: impl Into<String>,
        request: DeleteRequest,
    ) -> VearchResult<DeleteResponse> {
        let response = self
            .add_auth_headers(
                self.http_client
                    .post(self.base_url.join("document/delete")?)
                    .json(&request)
            )
            .send()
            .await?;

        let response_text = response.text().await?;
        let json = self.parse_response_json(&response_text)?;

        if let Some(code) = json.get("code").and_then(|v| v.as_i64()) {
            if code != 0 {
                return Err(VearchError::from(json));
            }
        }

        let result: DeleteResponse = serde_json::from_value(json)?;
        Ok(result)
    }

    /// Convenience method to delete documents by IDs
    pub async fn delete_by_ids(
        &self,
        db_name: impl Into<String>,
        space_name: impl Into<String>,
        document_ids: Vec<String>,
    ) -> VearchResult<DeleteResponse> {
        let db_name = db_name.into();
        let space_name = space_name.into();
        let request = DeleteRequest {
            db_name: db_name.clone(),
            space_name: space_name.clone(),
            document_ids: Some(document_ids),
            filters: None,
            limit: None,
        };

        self.delete(db_name, space_name, request).await
    }

    /// Convenience method to delete documents by filter
    pub async fn delete_by_filter(
        &self,
        db_name: impl Into<String>,
        space_name: impl Into<String>,
        filters: Filter,
        limit: Option<i32>,
    ) -> VearchResult<DeleteResponse> {
        let db_name = db_name.into();
        let space_name = space_name.into();
        let request = DeleteRequest {
            db_name: db_name.clone(),
            space_name: space_name.clone(),
            document_ids: None,
            filters: Some(filters),
            limit,
        };

        self.delete(db_name, space_name, request).await
    }

    /// Get a builder for creating search requests
    pub fn search_builder(
        &self,
        db_name: impl Into<String>,
        space_name: impl Into<String>,
    ) -> SearchRequestBuilder {
        SearchRequestBuilder::new(db_name, space_name)
    }

    /// Get a builder for creating query requests
    pub fn query_builder(
        &self,
        db_name: impl Into<String>,
        space_name: impl Into<String>,
    ) -> QueryRequestBuilder {
        QueryRequestBuilder::new(db_name, space_name)
    }

    /// Get a builder for creating delete requests
    pub fn delete_builder(
        &self,
        db_name: impl Into<String>,
        space_name: impl Into<String>,
    ) -> DeleteRequestBuilder {
        DeleteRequestBuilder::new(db_name, space_name)
    }

    /// Get a builder for creating filters
    pub fn filter_builder() -> FilterBuilder {
        FilterBuilder::new()
    }

    /// Get a builder for creating index parameters
    pub fn index_params_builder() -> IndexParamsBuilder {
        IndexParamsBuilder::new()
    }

    /// Health check
    pub async fn health_check(&self) -> VearchResult<bool> {
        let response = self
            .http_client
            .get(self.base_url.join("health")?)
            .send()
            .await?;

        Ok(response.status().is_success())
    }

    // ===== 鉴权相关方法 =====

    /// 设置鉴权配置
    pub fn with_auth(mut self, auth_config: AuthConfig) -> Self {
        self.auth_config = Some(auth_config);
        self
    }

    /// 登录并获取认证令牌
    pub async fn login(&mut self) -> VearchResult<()> {
        if let Some(auth_config) = &self.auth_config {
            let login_data = serde_json::json!({
                "username": auth_config.username,
                "password": auth_config.password,
            });

            let response = self
                .http_client
                .post(self.base_url.join("login")?)
                .json(&login_data)
                .send()
                .await?;

            let response_text = response.text().await?;
            let json = self.parse_response_json(&response_text)?;

            if let Some(code) = json.get("code").and_then(|v| v.as_i64()) {
                if code != 0 {
                    return Err(VearchError::from(json));
                }
            }

            // 尝试提取token
            if let Some(token) = json.get("token").and_then(|v| v.as_str()) {
                self.auth_token = Some(token.to_string());
            }

            Ok(())
        } else {
            Err(VearchError::InvalidParameter("No auth config provided".to_string()))
        }
    }

    /// 检查是否已认证
    pub fn is_authenticated(&self) -> bool {
        self.auth_token.is_some()
    }

    /// 获取认证令牌
    pub fn get_auth_token(&self) -> Option<&String> {
        self.auth_token.as_ref()
    }

    /// 清除认证信息
    pub fn clear_auth(&mut self) {
        self.auth_token = None;
    }

    /// 为请求添加认证头
    fn add_auth_headers(&self, request_builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(token) = &self.auth_token {
            request_builder.header("Authorization", format!("Bearer {}", token))
        } else {
            request_builder
        }
    }
}

impl Clone for VearchClient {
    fn clone(&self) -> Self {
        Self {
            base_url: self.base_url.clone(),
            http_client: self.http_client.clone(),
            auth_config: self.auth_config.clone(),
            auth_token: self.auth_token.clone(),
        }
    }
}

// ===== 库操作 =====

impl VearchClient {
    /// 创建数据库
    pub async fn create_database(&self, request: crate::models::CreateDatabaseRequest) -> VearchResult<DatabaseResponse> {
        let response = self
            .add_auth_headers(
                self.http_client
                    .post(self.base_url.join("db")?)
                    .json(&request)
            )
            .send()
            .await?;

        let response_text = response.text().await?;
        let json = self.parse_response_json(&response_text)?;

        if let Some(code) = json.get("code").and_then(|v| v.as_i64()) {
            if code != 0 {
                return Err(VearchError::from(json));
            }
        }

        let result: DatabaseResponse = serde_json::from_value(json)?;
        Ok(result)
    }

    /// 删除数据库
    pub async fn delete_database(&self, db_name: impl Into<String>) -> VearchResult<DatabaseResponse> {
        let db_name = db_name.into();
        let response = self
            .add_auth_headers(
                self.http_client
                    .delete(self.base_url.join(&format!("db/{}", db_name))?)
            )
            .send()
            .await?;

        let response_text = response.text().await?;
        let json = self.parse_response_json(&response_text)?;

        if let Some(code) = json.get("code").and_then(|v| v.as_i64()) {
            if code != 0 {
                return Err(VearchError::from(json));
            }
        }

        let result: DatabaseResponse = serde_json::from_value(json)?;
        Ok(result)
    }

    /// 列出所有数据库
    pub async fn list_databases(&self) -> VearchResult<ListDatabasesResponse> {
        let response = self
            .add_auth_headers(
                self.http_client
                    .get(self.base_url.join("db")?)
            )
            .send()
            .await?;

        let response_text = response.text().await?;
        let json = self.parse_response_json(&response_text)?;

        if let Some(code) = json.get("code").and_then(|v| v.as_i64()) {
            if code != 0 {
                return Err(VearchError::from(json));
            }
        }

        let result: ListDatabasesResponse = serde_json::from_value(json)?;
        Ok(result)
    }

    /// 获取数据库信息
    pub async fn get_database(&self, db_name: impl Into<String>) -> VearchResult<DatabaseInfo> {
        let db_name = db_name.into();
        let response = self
            .add_auth_headers(
                self.http_client
                    .get(self.base_url.join(&format!("db/{}", db_name))?)
            )
            .send()
            .await?;

        let response_text = response.text().await?;
        let json = self.parse_response_json(&response_text)?;

        if let Some(code) = json.get("code").and_then(|v| v.as_i64()) {
            if code != 0 {
                return Err(VearchError::from(json));
            }
        }

        let result: DatabaseInfo = serde_json::from_value(json)?;
        Ok(result)
    }
}

// ===== 表空间操作 =====

impl VearchClient {
    /// 创建表空间
    pub async fn create_space(
        &self,
        db_name: impl Into<String>,
        request: crate::models::CreateSpaceRequest,
    ) -> VearchResult<SpaceResponse> {
        let db_name = db_name.into();
        let response = self
            .add_auth_headers(
                self.http_client
                    .post(self.base_url.join(&format!("db/{}/space", db_name))?)
                    .json(&request)
            )
            .send()
            .await?;

        let response_text = response.text().await?;
        let json = self.parse_response_json(&response_text)?;

        if let Some(code) = json.get("code").and_then(|v| v.as_i64()) {
            if code != 0 {
                return Err(VearchError::from(json));
            }
        }

        let result: SpaceResponse = serde_json::from_value(json)?;
        Ok(result)
    }

    /// 删除表空间
    pub async fn delete_space(
        &self,
        db_name: impl Into<String>,
        space_name: impl Into<String>,
    ) -> VearchResult<SpaceResponse> {
        let db_name = db_name.into();
        let space_name = space_name.into();
        let response = self
            .add_auth_headers(
                self.http_client
                    .delete(self.base_url.join(&format!("db/{}/space/{}", db_name, space_name))?)
            )
            .send()
            .await?;

        let response_text = response.text().await?;
        let json = self.parse_response_json(&response_text)?;

        if let Some(code) = json.get("code").and_then(|v| v.as_i64()) {
            if code != 0 {
                return Err(VearchError::from(json));
            }
        }

        let result: SpaceResponse = serde_json::from_value(json)?;
        Ok(result)
    }

    /// 列出数据库中的所有表空间
    pub async fn list_spaces(&self, db_name: impl Into<String>) -> VearchResult<ListSpacesResponse> {
        let db_name = db_name.into();
        let response = self
            .add_auth_headers(
                self.http_client
                    .get(self.base_url.join(&format!("db/{}/space", db_name))?)
            )
            .send()
            .await?;

        let response_text = response.text().await?;
        let json = self.parse_response_json(&response_text)?;

        if let Some(code) = json.get("code").and_then(|v| v.as_i64()) {
            if code != 0 {
                return Err(VearchError::from(json));
            }
        }

        let result: ListSpacesResponse = serde_json::from_value(json)?;
        Ok(result)
    }

    /// 获取表空间信息
    pub async fn get_space(
        &self,
        db_name: impl Into<String>,
        space_name: impl Into<String>,
    ) -> VearchResult<SpaceInfo> {
        let db_name = db_name.into();
        let space_name = space_name.into();
        let response = self
            .add_auth_headers(
                self.http_client
                    .get(self.base_url.join(&format!("db/{}/space/{}", db_name, space_name))?)
            )
            .send()
            .await?;

        let response_text = response.text().await?;
        let json = self.parse_response_json(&response_text)?;

        if let Some(code) = json.get("code").and_then(|v| v.as_i64()) {
            if code != 0 {
                return Err(VearchError::from(json));
            }
        }

        let result: SpaceInfo = serde_json::from_value(json)?;
        Ok(result)
    }

    /// 获取表空间统计信息
    pub async fn get_space_stats(
        &self,
        db_name: impl Into<String>,
        space_name: impl Into<String>,
    ) -> VearchResult<SpaceStats> {
        let db_name = db_name.into();
        let space_name = space_name.into();
        let response = self
            .add_auth_headers(
                self.http_client
                    .get(self.base_url.join(&format!("db/{}/space/{}/stats", db_name, space_name))?)
            )
            .send()
            .await?;

        let response_text = response.text().await?;
        let json = self.parse_response_json(&response_text)?;

        if let Some(code) = json.get("code").and_then(|v| v.as_i64()) {
            if code != 0 {
                return Err(VearchError::from(json));
            }
        }

        let result: SpaceStats = serde_json::from_value(json)?;
        Ok(result)
    }

    /// 重建表空间索引
    pub async fn rebuild_index(
        &self,
        db_name: impl Into<String>,
        space_name: impl Into<String>,
    ) -> VearchResult<SpaceResponse> {
        let db_name = db_name.into();
        let space_name = space_name.into();
        let response = self
            .add_auth_headers(
                self.http_client
                    .post(self.base_url.join(&format!("db/{}/space/{}/rebuild", db_name, space_name))?)
            )
            .send()
            .await?;

        let response_text = response.text().await?;
        let json = self.parse_response_json(&response_text)?;

        if let Some(code) = json.get("code").and_then(|v| v.as_i64()) {
            if code != 0 {
                return Err(VearchError::from(json));
            }
        }

        let result: SpaceResponse = serde_json::from_value(json)?;
        Ok(result)
    }

    /// 获取构建器
    pub fn database_builder(name: impl Into<String>) -> crate::operations::DatabaseBuilder {
        crate::operations::DatabaseBuilder::new(name)
    }

    pub fn space_builder(name: impl Into<String>) -> crate::operations::SpaceBuilder {
        crate::operations::SpaceBuilder::new(name)
    }

    pub fn field_builder(name: impl Into<String>, field_type: crate::models::FieldType) -> crate::operations::FieldBuilder {
        crate::operations::FieldBuilder::new(name, field_type)
    }

    pub fn index_builder(index_type: impl Into<String>) -> crate::operations::IndexBuilder {
        crate::operations::IndexBuilder::new(index_type)
    }

    // ===== 鉴权管理方法 =====

    /// 创建角色
    pub async fn create_role(&self, request: crate::models::CreateRoleRequest) -> VearchResult<AuthResponse> {
        let response = self
            .add_auth_headers(
                self.http_client
                    .post(self.base_url.join("roles")?)
                    .json(&request)
            )
            .send()
            .await?;

        let response_text = response.text().await?;
        let json = self.parse_response_json(&response_text)?;

        if let Some(code) = json.get("code").and_then(|v| v.as_i64()) {
            if code != 0 {
                return Err(VearchError::from(json));
            }
        }

        let result: AuthResponse = serde_json::from_value(json)?;
        Ok(result)
    }

    /// 删除角色
    pub async fn delete_role(&self, role_name: impl Into<String>) -> VearchResult<AuthResponse> {
        let role_name = role_name.into();
        let response = self
            .add_auth_headers(
                self.http_client
                    .delete(self.base_url.join(&format!("roles/{}", role_name))?)
            )
            .send()
            .await?;

        let response_text = response.text().await?;
        let json = self.parse_response_json(&response_text)?;

        if let Some(code) = json.get("code").and_then(|v| v.as_i64()) {
            if code != 0 {
                return Err(VearchError::from(json));
            }
        }

        let result: AuthResponse = serde_json::from_value(json)?;
        Ok(result)
    }

    /// 获取角色信息
    pub async fn get_role(&self, role_name: impl Into<String>) -> VearchResult<RoleInfo> {
        let role_name = role_name.into();
        let response = self
            .add_auth_headers(
                self.http_client
                    .get(self.base_url.join(&format!("roles/{}", role_name))?)
            )
            .send()
            .await?;

        let response_text = response.text().await?;
        let json = self.parse_response_json(&response_text)?;

        if let Some(code) = json.get("code").and_then(|v| v.as_i64()) {
            if code != 0 {
                return Err(VearchError::from(json));
            }
        }

        let result: RoleInfo = serde_json::from_value(json)?;
        Ok(result)
    }

    /// 修改角色权限
    pub async fn modify_role(&self, request: crate::models::ModifyRoleRequest) -> VearchResult<AuthResponse> {
        let response = self
            .add_auth_headers(
                self.http_client
                    .put(self.base_url.join("roles")?)
                    .json(&request)
            )
            .send()
            .await?;

        let response_text = response.text().await?;
        let json = self.parse_response_json(&response_text)?;

        if let Some(code) = json.get("code").and_then(|v| v.as_i64()) {
            if code != 0 {
                return Err(VearchError::from(json));
            }
        }

        let result: AuthResponse = serde_json::from_value(json)?;
        Ok(result)
    }

    /// 创建用户
    pub async fn create_user(&self, request: crate::models::CreateUserRequest) -> VearchResult<AuthResponse> {
        let response = self
            .add_auth_headers(
                self.http_client
                    .post(self.base_url.join("users")?)
                    .json(&request)
            )
            .send()
            .await?;

        let response_text = response.text().await?;
        let json = self.parse_response_json(&response_text)?;

        if let Some(code) = json.get("code").and_then(|v| v.as_i64()) {
            if code != 0 {
                return Err(VearchError::from(json));
            }
        }

        let result: AuthResponse = serde_json::from_value(json)?;
        Ok(result)
    }

    /// 删除用户
    pub async fn delete_user(&self, user_name: impl Into<String>) -> VearchResult<AuthResponse> {
        let user_name = user_name.into();
        let response = self
            .add_auth_headers(
                self.http_client
                    .delete(self.base_url.join(&format!("users/{}", user_name))?)
            )
            .send()
            .await?;

        Ok(AuthResponse {
            code: 0,
            msg: "User deleted successfully".to_string(),
            data: None,
        })
    }

    /// 获取用户信息
    pub async fn get_user(&self, user_name: impl Into<String>) -> VearchResult<UserInfo> {
        let user_name = user_name.into();
        let response = self
            .add_auth_headers(
                self.http_client
                    .get(self.base_url.join(&format!("users/{}", user_name))?)
            )
            .send()
            .await?;

        let response_text = response.text().await?;
        let json = self.parse_response_json(&response_text)?;

        if let Some(code) = json.get("code").and_then(|v| v.as_i64()) {
            if code != 0 {
                return Err(VearchError::from(json));
            }
        }

        let result: UserInfo = serde_json::from_value(json)?;
        Ok(result)
    }

    /// 修改用户
    pub async fn modify_user(&self, request: crate::models::ModifyUserRequest) -> VearchResult<AuthResponse> {
        let response = self
            .add_auth_headers(
                self.http_client
                    .put(self.base_url.join("users")?)
                    .json(&request)
            )
            .send()
            .await?;

        let response_text = response.text().await?;
        let json = self.parse_response_json(&response_text)?;

        if let Some(code) = json.get("code").and_then(|v| v.as_i64()) {
            if code != 0 {
                return Err(VearchError::from(json));
            }
        }

        let result: AuthResponse = serde_json::from_value(json)?;
        Ok(result)
    }

    /// 获取鉴权构建器
    pub fn privilege_builder() -> crate::operations::PrivilegeBuilder {
        crate::operations::PrivilegeBuilder::new()
    }

    pub fn role_builder(name: impl Into<String>) -> crate::operations::RoleBuilder {
        crate::operations::RoleBuilder::new(name)
    }

    pub fn user_builder(name: impl Into<String>, password: impl Into<String>) -> crate::operations::UserBuilder {
        crate::operations::UserBuilder::new(name, password)
    }

    pub fn auth_config_builder(username: impl Into<String>, password: impl Into<String>) -> crate::operations::AuthConfigBuilder {
        crate::operations::AuthConfigBuilder::new(username, password)
    }
}
