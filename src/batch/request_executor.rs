use std::sync::Arc;

use anyhow::anyhow;
use log::error;

use crate::{
    api_client::{self, ApiClient, EmbedApiRequest, EmbedApiRequestInputs},
    request::{EmbedRequestClient, EmbedRequestGroupingParams},
};

pub struct RequestExecutor<TApiClient: ApiClient, TGroupingParams> {
    request_parameters: Arc<TGroupingParams>,
    api_client: Arc<TApiClient>,
}

impl<TApiClient: ApiClient + 'static, TGroupingParams>
    RequestExecutor<TApiClient, TGroupingParams>
{
    pub fn new(api_client: Arc<TApiClient>, request_parameters: TGroupingParams) -> Self {
        Self {
            api_client: api_client,
            request_parameters: Arc::new(request_parameters),
        }
    }
}

impl<TApiClient: ApiClient + 'static> RequestExecutor<TApiClient, EmbedRequestGroupingParams> {
    pub fn execute_embed_request(
        &self,
        current_batch_size: usize,
        requests: Vec<EmbedRequestClient>,
    ) {
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
