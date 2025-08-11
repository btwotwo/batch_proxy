use std::{collections::HashMap, hash::Hash, sync::Arc};

use log::info;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    api_client::{ApiClient, EmbedApiRequest},
    config::BatchConfiguration,
    request::{Caller, EmbedRequestCaller, EmbedRequestGroupingParams},
};

use super::batch_worker::{self, BatchWorkerHandle};

struct BatchManager<TApiClient, TCaller, TGroupingParams>
where
    TApiClient: ApiClient,
    TCaller: Caller,
    TGroupingParams: Hash,
{
    workers: HashMap<TGroupingParams, BatchWorkerHandle<TCaller>>,
    api_client: Arc<TApiClient>,
    batch_config: BatchConfiguration,

    receiver: mpsc::Receiver<BatchManagerMessage<TCaller, TGroupingParams>>,
}

#[derive(Clone)]
pub struct EmbedApiBatchManagerHandler {
    sender: mpsc::Sender<BatchManagerMessage<EmbedRequestCaller, EmbedRequestGroupingParams>>,
}

impl EmbedApiBatchManagerHandler {
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

        let caller_id = Uuid::new_v4();

        info!(
            "Adding request from the caller to batcher. [input = {:?}, params = {:?}, caller_id = {:?}]",
            request_data, request_params, caller_id
        );

        let (result, caller) = EmbedRequestCaller::new(request_data, caller_id);
        self.sender
            .send(BatchManagerMessage::NewRequest(caller, request_params))
            .await?;

        result.await?
    }
}

enum BatchManagerMessage<TCaller, TGroupingParams> {
    NewRequest(TCaller, TGroupingParams),
}

impl<TApiClient, TCaller, TGroupingParams> BatchManager<TApiClient, TCaller, TGroupingParams>
where
    TApiClient: ApiClient + 'static,
    TCaller: Caller,
    TGroupingParams: Hash + Clone + Eq,
{
    // TODO: Periodical cleanup of workers?
    fn handle_messages(&mut self, message: BatchManagerMessage<TCaller, TGroupingParams>) {
        match message {
            BatchManagerMessage::NewRequest(caller, req_params) => {
                let worker = self.workers.entry(req_params.clone()).or_insert_with(|| {
                    batch_worker::start_worker(
                        Arc::clone(&self.api_client),
                        req_params,
                        &self.batch_config,
                        CancellationToken::new(),
                    )
                });

                worker.put_request(caller);
            }
        }
    }
}

pub fn start_embed_api_batch_manager<TApiClient: ApiClient + 'static>(
    api_client: TApiClient,
    batch_config: BatchConfiguration,
) -> EmbedApiBatchManagerHandler {
    let (sender, receiver) = mpsc::channel::<BatchManagerMessage<EmbedRequestCaller, EmbedRequestGroupingParams>>(64);
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

    EmbedApiBatchManagerHandler { sender }
}
