use async_trait::async_trait;

use crate::api::endpoint::ApiEndpont;

use super::batch_executor::Batch;

#[async_trait]
pub trait DataProvider<TApiEndpoint: ApiEndpont>: Send + Sync + 'static {
    async fn get_response(
        &self,
        batch: &Batch<TApiEndpoint>,
    ) -> anyhow::Result<Vec<TApiEndpoint::ApiResponseItem>>;
}
