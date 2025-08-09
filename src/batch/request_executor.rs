use std::sync::Arc;

use log::error;
use anyhow::anyhow;

use crate::{api_client::{ApiClient, EmbedApiRequest}, request::{EmbedRequestHandle, EmbedRequestParams}};


pub fn execute_request(
    requests: Vec<EmbedRequestHandle>,
    request_parameters: Arc<EmbedRequestParams>,
    api_client: Arc<impl ApiClient + 'static>,
    current_batch_size: usize,
) {
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
            inputs,
            truncate: request_parameters.truncate,
            normalize: request_parameters.normalize.unwrap_or(false),
            dimensions: request_parameters.dimensions,
            prompt_name: request_parameters.prompt_name.clone(),
            truncation_direction: request_parameters.truncation_direction.clone(),
        };

        match api_client.call_embed(&api_parameters).await {
            Ok(result) => {
                let mut result_iterator = result.into_iter();

                for (data_len, client) in clients {
                    let client_data: Vec<_> = result_iterator.by_ref().take(data_len).collect();

                    client.send(Ok(client_data)).unwrap_or_else(|_| {
                    error!(
                        "Could not send response to client, receiver has dropped. [ClientId = TODO]"
                    )
                });
                }
            }

            Err(err) => {
                error!("Embedding API call failed. Error = {0}", &err);
                for (_, client) in clients {
                    client.send(Err(anyhow!("API Call failed."))).unwrap_or_else(|_| {
                        error!(
                            "Could not send response to client, receiver has dropped. [ClientId = TODO]"
                        )
                    });
                }
            }
        }
    });
}
