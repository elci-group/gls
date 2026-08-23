use thiserror::Error;

#[derive(Debug, Error)]
pub enum GlsError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialisation error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("context provider error: {0}")]
    Context(String),

    #[error("persistence error: {0}")]
    Persistence(String),

    #[error("pipeline error: {0}")]
    Pipeline(String),
}

pub type Result<T> = std::result::Result<T, GlsError>;
