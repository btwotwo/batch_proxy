use anyhow::Context;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type ApiClientResult<T> = Result<T, ApiClientError>;

#[derive(Error, Debug)]
pub enum ApiClientError {
    #[error("Error calling /embed endpoint. Error = {0}")]
    EmbedEndpointError(#[from] anyhow::Error),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EmbedApiRequest {
    pub inputs: Vec<String>,
    pub dimensions: Option<usize>,
    pub normalize: bool,
    pub prompt_name: Option<String>,
    pub truncate: Option<bool>,
    pub truncation_direction: Option<String>,
}

#[async_trait]
pub trait ApiClient: Send + Sync {
    async fn call_embed(&self, request: &EmbedApiRequest) -> ApiClientResult<Vec<Vec<f64>>>;
}

#[async_trait]
impl ApiClient for reqwest::Client {
    async fn call_embed(&self, request: &EmbedApiRequest) -> ApiClientResult<Vec<Vec<f64>>> {
        let result = self
            .post("/embed")
            .json(&request)
            .send()
            .await
            .context("Failed to send request")?
            .json::<Vec<Vec<f64>>>()
            .await
            .context("Failed to deserialize response")?;

        Ok(result)
    }
}
