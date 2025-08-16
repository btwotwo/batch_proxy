use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use log::info;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    api_client::{ApiClient, ApiEndpont, EmbedApiEndpoint, EmbedApiRequest},
    request::{EmbedRequestClient, EmbedRequestGroupingParams, GroupingParams, RequestClient},
    settings::BatchSettings,
};

use super::batch_worker::{self, EmbedApiBatchWorkerHandle};

#[derive(Clone)]
pub struct BatchManagerHandle {
    sender: mpsc::Sender<BatchManagerMessage>,
}

struct BatchManager<TApiClient: ApiClient> {
    workers: HashMap<EmbedRequestGroupingParams, EmbedApiBatchWorkerHandle>,
    api_client: Arc<TApiClient>,
    batch_config: BatchSettings,

    receiver: mpsc::Receiver<BatchManagerMessage>,
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
            "Adding request from the client to batcher. [input = {:?}, params = {:?}, client_id = {:?}]",
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
    // TODO: Periodical cleanup of workers?
    fn handle_messages(&mut self, message: BatchManagerMessage) {
        match message {
            BatchManagerMessage::NewRequest(client, req_params) => {
                let worker = self.workers.entry(req_params.clone()).or_insert_with(|| {
                    let worker_id = Uuid::new_v4();
                    
                    info!("Starting new worker. [parameters = {req_params:#?}, worker_id = {worker_id}");
                    
                    batch_worker::start(
                        Arc::clone(&self.api_client),
                        req_params,
                        &self.batch_config,
                        worker_id,
                        // TODO: Use cancellation token for graceful shutdown
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
    batch_config: BatchSettings,
) -> BatchManagerHandle {
    let (sender, receiver) = mpsc::channel::<BatchManagerMessage>(2048);
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
