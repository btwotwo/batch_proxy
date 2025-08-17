pub mod reqwest_api_client;

use async_trait::async_trait;
use thiserror::Error;

use super::endpoint::embed_endpoint::EmbedApiRequest;

pub type ApiClientResult<T> = Result<T, ApiClientError>;

#[derive(Error, Debug)]
pub enum ApiClientError {
    #[error("Request failed: {0:?}")]
    Request(#[from] reqwest::Error),

    #[error("Other error: {0:?}")]
    Other(#[from] anyhow::Error),
}

#[async_trait]
pub trait ApiClient: Send + Sync + 'static {
    async fn call_embed(&self, request: &EmbedApiRequest) -> ApiClientResult<Vec<Vec<f64>>>;
}
