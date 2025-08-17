use async_trait::async_trait;

use crate::api::endpoint::ApiEndpont;

use super::batch_executor::Batch;

#[async_trait]
pub trait DataProvider<TApiEndpoint: ApiEndpont>: Send + Sync + 'static {
    async fn get_data_for_batch(
        &self,
        batch: &Batch<TApiEndpoint>,
    ) -> anyhow::Result<Vec<TApiEndpoint::ApiResponseItem>>;
}
