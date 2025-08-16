use std::sync::Arc;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use log::error;

use crate::{
    api_client::{ApiClient, ApiEndpont, EmbedApiEndpoint, EmbedApiRequest, EmbedApiRequestInputs},
    request::{
        EmbedRequestClient, EmbedRequestGroupingParams, GroupingParams, RequestClient,
        RequestHandle,
    },
};

struct BatchedClient<TApiEndpoint: ApiEndpont> {
    request_size: usize,
    request_handle: RequestHandle<TApiEndpoint::ApiResponseItem>,
}

pub struct Batch<TApiEndpoint: ApiEndpont> {
    clients: Vec<BatchedClient<TApiEndpoint>>,
    api_parameters: TApiEndpoint::ApiRequest,
}

#[async_trait]
pub trait BatchExecutor<TApiEndpoint: ApiEndpont>: Send + Sync + 'static {
    async fn execute_batch(&self, batch: Batch<TApiEndpoint>);
}

pub struct ApiBatchExecutor<TApiClient: ApiClient> {
    pub api_client: TApiClient,
}

#[async_trait]
impl<TApiClient: ApiClient> BatchExecutor<EmbedApiEndpoint> for ApiBatchExecutor<TApiClient> {
    async fn execute_batch(&self, batch: Batch<EmbedApiEndpoint>) {
        let response = self
            .api_client
            .call_embed(&batch.api_parameters)
            .await.map_err(Into::into);
        
        distribute_response(response, batch.clients);
    }
}

pub fn batch_requests<TApiEndpoint>(
    current_batch_size: usize,
    request_clients: Vec<RequestClient<TApiEndpoint>>,
    grouping_params: &TApiEndpoint::GroupingParams,
) -> Batch<TApiEndpoint>
where
    TApiEndpoint: ApiEndpont,
{
    let mut inputs = Vec::with_capacity(current_batch_size);
    let mut clients = Vec::with_capacity(request_clients.len());

    for client in request_clients {
        let request_size = client.data.len();
        let request_handle = client.handle;
        inputs.extend(client.data);
        clients.push(BatchedClient {
            request_size,
            request_handle,
        });
    }

    let api_parameters = grouping_params.to_request(inputs);

    Batch {
        clients,
        api_parameters,
    }
}

fn distribute_response<TApiEndpoint: ApiEndpont>(
    response: anyhow::Result<Vec<TApiEndpoint::ApiResponseItem>>,
    batched_clients: Vec<BatchedClient<TApiEndpoint>>,
) {
    match response {
        Ok(result) => {
            let mut result_iterator = result.into_iter();
            for BatchedClient {
                request_size,
                request_handle,
            } in batched_clients
            {
                let client_data: Vec<_> = result_iterator.by_ref().take(request_size).collect();
                request_handle.reply_with_result(client_data);
            }
        }

        Err(err) => {
            error!("Embedding API call failed. Error = {0}", &err);
            for BatchedClient { request_handle, .. } in batched_clients {
                request_handle.reply_with_error(anyhow!("API call failed. Please try again"));
            }
        }
    }
}

