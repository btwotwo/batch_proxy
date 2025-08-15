use std::sync::Arc;

use anyhow::anyhow;
use log::error;

use crate::{
    api_client::{ApiClient, EmbedApiRequest, EmbedApiRequestInputs},
    request::{
        EmbedRequestClient, EmbedRequestGroupingParams, GroupingParams, RequestClient,
        RequestHandle,
    },
};

struct BatchedClient<O> {
    request_size: usize,
    request_handle: RequestHandle<O>,
}

struct Batch<TApiRequest, O> {
    clients: Vec<BatchedClient<O>>,
    api_parameters: TApiRequest,
}

trait BatchExecutor {
    type ApiRequest;
    type ApiResponse;

    fn execute_batch(batch: Batch<Self::ApiRequest, Self::ApiResponse>);
}

fn batch_requests<I, O, G: GroupingParams<I>>(
    current_batch_size: usize,
    request_clients: RequestClient<I, O>,
    grouping_params: G
) {
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
