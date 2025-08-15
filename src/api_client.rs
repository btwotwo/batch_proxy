use std::fmt::Debug;

use anyhow::Result;
use async_trait::async_trait;
use log::error;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use thiserror::Error;

use crate::request::{EmbedRequestGroupingParams, GroupingParams};

pub type ApiClientResult<T> = Result<T, ApiClientError>;

#[derive(Error, Debug)]
pub enum ApiClientError {
    #[error("Request failed: {0:?}")]
    Request(#[from] reqwest::Error),

    #[error("Failed to deserialize JSON from response: {source}\nRaw response: {raw}")]
    Deserialize {
        #[source]
        source: serde_json::Error,
        raw: String,
    },

    #[error("Other error: {0:?}")]
    Other(#[from] anyhow::Error),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EmbedApiRequestInputs {
    Vec(Vec<String>),
    Str(String),
}

impl Default for EmbedApiRequestInputs {
    fn default() -> Self {
        EmbedApiRequestInputs::Vec(Vec::new())
    }
}

#[skip_serializing_none]
#[derive(Debug, Serialize, Deserialize)]
pub struct EmbedApiRequest {
    pub inputs: EmbedApiRequestInputs,
    pub dimensions: Option<usize>,
    pub normalize: Option<bool>,
    pub prompt_name: Option<String>,
    pub truncate: Option<bool>,
    pub truncation_direction: Option<String>,
}

pub trait ApiEndpont: 'static {
    type ApiRequest: Send + Sync + Debug;
    type ApiResponseItem: Send + Sync + Debug;
    type DataItem: Send + Sync + Debug;
    type GroupingParams: Send
        + GroupingParams<DataItem = Self::DataItem, ApiRequest = Self::ApiRequest>
        + Debug;
}

pub struct EmbedApiEndpoint;

impl ApiEndpont for EmbedApiEndpoint {
    type ApiRequest = EmbedApiRequest;
    type ApiResponseItem = Vec<f64>;
    type DataItem = String;
    type GroupingParams = EmbedRequestGroupingParams;
}

#[async_trait]
pub trait ApiClient: Send + Sync + 'static {
    async fn call_embed(&self, request: &EmbedApiRequest) -> ApiClientResult<Vec<Vec<f64>>>;
}

pub struct ReqwestApiClient {
    embed_url: String,
    pub client: reqwest::Client,
}

impl ReqwestApiClient {
    pub fn new(base_url: &str) -> anyhow::Result<Self> {
        let base_url = Url::parse(base_url)?;

        Ok(Self {
            embed_url: base_url.join("/embed")?.to_string(),
            client: reqwest::Client::new(),
        })
    }
}

#[async_trait]
impl ApiClient for ReqwestApiClient {
    async fn call_embed(&self, request: &EmbedApiRequest) -> ApiClientResult<Vec<Vec<f64>>> {
        let result_string = self
            .client
            .post(&self.embed_url)
            .json(&request)
            .send()
            .await?
            .text()
            .await?;

        match serde_json::from_str::<Vec<Vec<f64>>>(&result_string) {
            Ok(parsed) => Ok(parsed),
            Err(e) => Err(ApiClientError::Deserialize {
                source: e,
                raw: result_string,
            }),
        }
    }
}
