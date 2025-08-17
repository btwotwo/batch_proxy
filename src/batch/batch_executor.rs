use std::sync::Arc;

use anyhow::anyhow;
use log::error;

use crate::{
    api::endpoint::{ApiEndpont, GroupingParams},
    request::{RequestClient, RequestHandle},
};

use super::DataProvider;

pub struct Batch<TApiEndpoint: ApiEndpont> {
    clients: Vec<BatchedClient<TApiEndpoint>>,
    api_parameters: TApiEndpoint::ApiRequest,
}

pub struct BatchedClient<TApiEndpoint: ApiEndpont> {
    request_size: usize,
    request_handle: RequestHandle<TApiEndpoint::ApiResponseItem>,
}

impl<TApiEndpoint: ApiEndpont> Batch<TApiEndpoint> {
    pub fn api_parameters(&self) -> &TApiEndpoint::ApiRequest {
        &self.api_parameters
    }
}

pub async fn execute_batch<TApiEndpoint: ApiEndpont, TDataProvider: DataProvider<TApiEndpoint>>(
    data_provider: Arc<TDataProvider>,
    grouping_params: Arc<TApiEndpoint::GroupingParams>,
    request_clients: Vec<RequestClient<TApiEndpoint>>,
    current_batch_size: usize,
) {
    let batch = batch_requests(current_batch_size, request_clients, &grouping_params);
    let data = data_provider.get_data_for_batch(&batch).await;
    distribute_response(data, batch);
}

fn batch_requests<TApiEndpoint>(
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
    batch: Batch<TApiEndpoint>,
) {
    let batched_clients = batch.clients;
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
