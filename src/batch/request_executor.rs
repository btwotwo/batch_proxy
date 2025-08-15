use std::sync::Arc;

use anyhow::{anyhow, Context};
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

struct Batch<TApiEndpoint: ApiEndpont> {
    clients: Vec<BatchedClient<TApiEndpoint>>,
    api_parameters: TApiEndpoint::ApiRequest,
}

#[async_trait]
trait BatchExecutor<TApiEndpoint: ApiEndpont> {
    async fn execute_batch(&self, batch: Batch<TApiEndpoint>);
}

struct ApiBatchExecutor<TApiClient: ApiClient> {
    api_client: TApiClient,
}

#[async_trait]
impl<TApiClient: ApiClient> BatchExecutor<EmbedApiEndpoint> for ApiBatchExecutor<TApiClient> {
    async fn execute_batch(&self, batch: Batch<EmbedApiEndpoint>) {
        let response = self.api_client.call_embed(&batch.api_parameters).await.context("Failed call to /embed");
        distribute_response(response, batch.clients);
    }
}

fn batch_requests<TApiEndpoint>(
    current_batch_size: usize,
    request_clients: Vec<RequestClient<TApiEndpoint>>,
    grouping_params: TApiEndpoint::GroupingParams,
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

pub struct RequestExecutor<TApiClient: ApiClient, TGroupingParams> {
    request_parameters: Arc<TGroupingParams>,
    api_client: Arc<TApiClient>,
}

impl<TApiClient, TGroupingParams> RequestExecutor<TApiClient, TGroupingParams>
where
    TApiClient: ApiClient,
{
    pub fn new(api_client: Arc<TApiClient>, request_parameters: TGroupingParams) -> Self {
        Self {
            api_client,
            request_parameters: Arc::new(request_parameters),
        }
    }
}

pub trait GenericRequestExecutor<TRequestClient> {
    fn execute_request(&self, current_batch_size: usize, requests: Vec<TRequestClient>);
}

impl<TApiClient> GenericRequestExecutor<EmbedRequestClient>
    for RequestExecutor<TApiClient, EmbedRequestGroupingParams>
where
    TApiClient: ApiClient + 'static,
{
    fn execute_request(&self, current_batch_size: usize, requests: Vec<EmbedRequestClient>) {
        let request_parameters = Arc::clone(&self.request_parameters);
        let api_client = Arc::clone(&self.api_client);

        tokio::spawn(async move {
            let mut inputs = Vec::with_capacity(current_batch_size);
            let mut clients = Vec::with_capacity(requests.len());

            for request in requests {
                let input_len = request.request_data.len();
                let reply_handle = request.reply_handle;
                inputs.extend(request.request_data);
                clients.push((input_len, reply_handle));
            }

            let api_parameters = EmbedApiRequest {
                inputs: EmbedApiRequestInputs::Vec(inputs),
                truncate: request_parameters.truncate,
                normalize: request_parameters.normalize,
                dimensions: request_parameters.dimensions,
                prompt_name: request_parameters.prompt_name.clone(),
                truncation_direction: request_parameters.truncation_direction.clone(),
            };

            match api_client.call_embed(&api_parameters).await {
                Ok(result) => {
                    let mut result_iterator = result.into_iter();

                    for (data_len, client) in clients {
                        let client_data: Vec<_> = result_iterator.by_ref().take(data_len).collect();

                        client.reply_with_result(client_data);
                    }
                }

                Err(err) => {
                    error!("Embedding API call failed. Error = {0}", &err);
                    for (_, client) in clients {
                        client.reply_with_error(anyhow!("API call failed. Please try again"));
                    }
                }
            }
        });
    }
}
