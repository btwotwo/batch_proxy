use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use crate::{
    api::{api_data_provider::ApiDataProvider, client::ApiClient},
    batch::{Batch, DataProvider},
};

use super::{ApiEndpont, GroupingParams};

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

pub struct EmbedApiEndpoint;

impl ApiEndpont for EmbedApiEndpoint {
    type ApiRequest = EmbedApiRequest;
    type ApiResponseItem = Vec<f64>;
    type DataItem = String;
    type GroupingParams = EmbedRequestGroupingParams;
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct EmbedRequestGroupingParams {
    pub dimensions: Option<usize>,
    pub normalize: Option<bool>,
    pub prompt_name: Option<String>,
    pub truncate: Option<bool>,
    pub truncation_direction: Option<String>,
}

impl GroupingParams for EmbedRequestGroupingParams {
    type DataItem = String;
    type ApiRequest = EmbedApiRequest;

    fn to_request(&self, data: Vec<Self::DataItem>) -> Self::ApiRequest {
        EmbedApiRequest {
            inputs: EmbedApiRequestInputs::Vec(data),
            truncate: self.truncate,
            normalize: self.normalize,
            dimensions: self.dimensions,
            prompt_name: self.prompt_name.clone(),
            truncation_direction: self.truncation_direction.clone(),
        }
    }

    fn decompose_api_request(api_request: Self::ApiRequest) -> (Vec<Self::DataItem>, Self) {
        let EmbedApiRequest {
            inputs,
            dimensions,
            normalize,
            prompt_name,
            truncate,
            truncation_direction,
        } = api_request;

        let request_data = match inputs {
            EmbedApiRequestInputs::Str(input) => vec![input],
            EmbedApiRequestInputs::Vec(inputs) => inputs,
        };

        let request_params = EmbedRequestGroupingParams {
            truncate,
            normalize,
            dimensions,
            prompt_name,
            truncation_direction,
        };

        (request_data, request_params)
    }
}

#[async_trait]
impl<TApiClient: ApiClient> DataProvider<EmbedApiEndpoint> for ApiDataProvider<TApiClient> {
    async fn get_data_for_batch(
        &self,
        batch: &Batch<EmbedApiEndpoint>,
    ) -> anyhow::Result<Vec<Vec<f64>>> {
        let response = self.api_client.call_embed(batch.api_parameters()).await?;

        Ok(response)
    }
}
