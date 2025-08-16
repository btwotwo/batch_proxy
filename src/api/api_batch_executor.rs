use async_trait::async_trait;

use crate::{
    api::{client::ApiClient, endpoint::embed_endpoint::EmbedApiEndpoint},
    batch::request_executor::{Batch, DataProvider},
};

pub struct ApiBatchExecutor<TApiClient: ApiClient> {
    pub api_client: TApiClient,
}

#[async_trait]
impl<TApiClient: ApiClient> DataProvider<EmbedApiEndpoint> for ApiBatchExecutor<TApiClient> {
    async fn get_response(&self, batch: &Batch<EmbedApiEndpoint>) -> anyhow::Result<Vec<Vec<f64>>> {
        let response = self.api_client.call_embed(batch.api_parameters()).await?;

        Ok(response)
    }
}
