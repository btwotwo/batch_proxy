use std::{sync::Arc, time::Duration};

use anyhow::anyhow;
use log::{error, info};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    api_client::ApiClient,
    config::BatchConfiguration,
    request::{EmbedRequestClient, EmbedRequestGroupingParams},
};

use super::{request_executor::{self, RequestExecutor}, request_store::RequestStore};

enum BatchWorkerMessage {
    NewRequest(EmbedRequestClient),
}

#[derive(Clone)]
pub struct EmbedApiBatchWorkerHandle {
    sender: mpsc::Sender<BatchWorkerMessage>,
}

impl EmbedApiBatchWorkerHandle {
    pub fn put_request(&self, req: EmbedRequestClient) {
        let sender = self.sender.clone();

        tokio::spawn(async move {
            sender
                .send(BatchWorkerMessage::NewRequest(req))
                .await
                .unwrap_or_else(|err| {
                    error!("Error sending message to worker! [Error = {err}]");

                    match err.0 {
                        BatchWorkerMessage::NewRequest(req) => {
                            req.reply_handle.reply_with_error(anyhow!(
                                "Could not process request, please try again."
                            ));
                        }
                    }
                })
        });
    }
}

pub struct EmbedApiBatchWorker<TApiClient: ApiClient> {
    request_store: RequestStore<EmbedRequestClient>,
    request_executor: RequestExecutor<TApiClient>,
    receiver: mpsc::Receiver<BatchWorkerMessage>,
    // TODO: Add worker ID
}

impl<TApiClient: ApiClient + 'static> EmbedApiBatchWorker<TApiClient> {
    fn new(
        receiver: mpsc::Receiver<BatchWorkerMessage>,
        api_client: Arc<TApiClient>,
        api_parameters: EmbedRequestGroupingParams,
        batch_config: &BatchConfiguration,
    ) -> Self {
        Self {
            request_store: RequestStore::new(batch_config.max_batch_size),
            request_executor: RequestExecutor::new(api_client, api_parameters),
            receiver,
        }
    }

    fn handle_message(&mut self, message: BatchWorkerMessage) {
        match message {
            BatchWorkerMessage::NewRequest(req) => self.handle_new_request(req),
        }
    }

    fn flush_batch(&mut self) {
        if self.request_store.is_empty() {
            return;
        }

        let (current_batch_size, requests) = self.request_store.drain();

        let client_ids: Vec<_> = requests.iter().map(|r| r.client_id()).collect();

        info!(
            "Flushing requests from clients. [client_ids = {:#?}]",
            client_ids
        );

        self.request_executor.execute_embed_request(
            current_batch_size,
            requests,
        );
    }

    // Message handlers
    fn handle_new_request(&mut self, req: EmbedRequestClient) {
        info!(
            "Accepted request from client. [client_id = {}]",
            req.client_id()
        );

        if let Some(req) = self.request_store.try_store(req) {
            info!("Could not store request, max batch size was reached. Flushing current batch.");
            self.flush_batch();
            self.request_store.force_store(req);
        }
    }
}

pub fn start<TApiClient: ApiClient + 'static>(
    api_client: Arc<TApiClient>,
    api_parameters: EmbedRequestGroupingParams,
    batch_config: &BatchConfiguration,
    cancellation_token: CancellationToken,
) -> EmbedApiBatchWorkerHandle {
    let (sender, receiver) = mpsc::channel::<BatchWorkerMessage>(64);
    let mut worker = EmbedApiBatchWorker::new(receiver, api_client, api_parameters, batch_config);
    let waiting_time_duration = Duration::from_millis(batch_config.max_waiting_time_ms);

    tokio::spawn(async move {
        loop {
            tokio::select! {
                msg = worker.receiver.recv() => {
                    match msg {
                        Some(msg) => worker.handle_message(msg),
                        None => {
                            info!("Last sender was dropped, flushing batch and stopping batch worker.");
                            worker.flush_batch();
                            break;
                        }
                    }
                },
                _ = tokio::time::sleep(waiting_time_duration) => {
                    worker.flush_batch();
                },
                _ = cancellation_token.cancelled() => {
                    info!("Flusing batch and stopping worker.");
                    worker.flush_batch();
                    break;
                }
            }
        }
    });

    EmbedApiBatchWorkerHandle { sender }
}

#[cfg(test)]
mod tests {}
