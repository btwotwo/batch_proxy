use std::{collections::HashMap, sync::Arc};

use log::info;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    api_client::{ApiClient, EmbedApiRequest},
    config::BatchConfiguration,
    request::{EmbedRequestClient, EmbedRequestGroupingParams},
};

use super::batch_worker::{self, EmbedApiBatchWorkerHandle};

struct BatchManager<TApiClient: ApiClient> {
    workers: HashMap<EmbedRequestGroupingParams, EmbedApiBatchWorkerHandle>,
    api_client: Arc<TApiClient>,
    batch_config: BatchConfiguration,

    receiver: mpsc::Receiver<BatchManagerMessage>,
}

#[derive(Clone)]
pub struct BatchManagerHandle {
    sender: mpsc::Sender<BatchManagerMessage>,
}

impl BatchManagerHandle {
    pub async fn call_batched_embed(
        &self,
        mut api_request: EmbedApiRequest,
    ) -> anyhow::Result<Vec<Vec<f64>>> {
        let request_data = std::mem::take(&mut api_request.inputs);

        let request_params = EmbedRequestGroupingParams {
            truncate: api_request.truncate,
            normalize: api_request.normalize,
            dimensions: api_request.dimensions,
            prompt_name: api_request.prompt_name,
            truncation_direction: api_request.truncation_direction,
        };

        let client_id = Uuid::new_v4();
        info!(
            "Processing request from client. [input = {:?}, params = {:?}, client_id = {:?}]",
            request_data, request_params, client_id
        );

        let (result, client) = EmbedRequestClient::new(request_data, client_id);
        self.sender
            .send(BatchManagerMessage::NewRequest(client, request_params))
            .await?;
        result.await?
    }
}

enum BatchManagerMessage {
    NewRequest(EmbedRequestClient, EmbedRequestGroupingParams),
}

impl<TApiClient: ApiClient + 'static> BatchManager<TApiClient> {
    fn handle_messages(&mut self, message: BatchManagerMessage) {
        match message {
            BatchManagerMessage::NewRequest(client, req_params) => {
                let worker = self.workers.entry(req_params.clone()).or_insert_with(|| {
                    batch_worker::start(
                        Arc::clone(&self.api_client),
                        req_params,
                        &self.batch_config,
                        CancellationToken::new(),
                    )
                });

                worker.put_request(client);
            }
        }
    }
}

pub fn start<TApiClient: ApiClient + 'static>(
    api_client: TApiClient,
    batch_config: BatchConfiguration,
) -> BatchManagerHandle {
    let (sender, receiver) = mpsc::channel::<BatchManagerMessage>(64);
    let mut manager = BatchManager {
        workers: HashMap::new(),
        api_client: Arc::new(api_client),
        receiver,
        batch_config,
    };

    tokio::spawn(async move {
        while let Some(msg) = manager.receiver.recv().await {
            manager.handle_messages(msg);
        }
    });

    BatchManagerHandle { sender }
}
