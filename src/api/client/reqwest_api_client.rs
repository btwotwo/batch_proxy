use async_trait::async_trait;
use reqwest::Url;

use crate::api::endpoint::embed_endpoint::EmbedApiRequest;

use super::{ApiClient, ApiClientError, ApiClientResult};

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
