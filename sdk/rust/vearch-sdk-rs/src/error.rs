use thiserror::Error;

/// Vearch SDK error types
#[derive(Error, Debug)]
pub enum VearchError {
    #[error("HTTP request failed: {0}")]
    HttpError(#[from] reqwest::Error),
    
    #[error("JSON serialization failed: {0}")]
    SerializationError(#[from] serde_json::Error),
    
    #[error("Invalid URL: {0}")]
    InvalidUrl(#[from] url::ParseError),
    
    #[error("Vearch API error: {code} - {message}")]
    ApiError { code: i32, message: String },
    
    #[error("Invalid parameter: {0}")]
    InvalidParameter(String),
    
    #[error("Operation failed: {0}")]
    OperationFailed(String),
    
    #[error("Document not found: {0}")]
    DocumentNotFound(String),
    
    #[error("Database not found: {0}")]
    DatabaseNotFound(String),
    
    #[error("Space not found: {0}")]
    SpaceNotFound(String),
    
    #[error("Vector dimension mismatch: expected {expected}, got {actual}")]
    VectorDimensionMismatch { expected: usize, actual: usize },
}

impl From<serde_json::Value> for VearchError {
    fn from(value: serde_json::Value) -> Self {
        if let (Some(code), Some(msg)) = (
            value.get("code").and_then(|v| v.as_i64()),
            value.get("msg").and_then(|v| v.as_str()),
        ) {
            if code != 0 {
                return VearchError::ApiError {
                    code: code as i32,
                    message: msg.to_string(),
                };
            }
        }
        VearchError::OperationFailed("Unknown API error".to_string())
    }
}

/// Result type for Vearch operations
pub type VearchResult<T> = Result<T, VearchError>;
