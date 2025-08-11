use std::{sync::Arc, time::Duration};

use anyhow::anyhow;
use log::{error, info};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    api_client::ApiClient,
    request::{EmbedRequestClient, EmbedRequestGroupingParams},
    settings::BatchSettings,
};

use super::{
    request_executor::{self, GenericRequestExecutor, RequestExecutor},
    request_store::RequestStore,
};

enum BatchWorkerMessage {
    NewRequest(EmbedRequestClient),
}

#[derive(Clone)]
pub struct EmbedApiBatchWorkerHandle {
    sender: mpsc::Sender<BatchWorkerMessage>,
    worker_id: Uuid,
}

impl EmbedApiBatchWorkerHandle {
    pub fn put_request(&self, req: EmbedRequestClient) {
        let sender = self.sender.clone();
        let worker_id = self.worker_id;

        tokio::spawn(async move {
            sender
                .send(BatchWorkerMessage::NewRequest(req))
                .await
                .unwrap_or_else(|err| {
                    error!(
                        "Error sending message to worker! [error = {err}, worker_id = {worker_id}]"
                    );

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

pub struct EmbedApiBatchWorker<TRequestExecutor: GenericRequestExecutor<EmbedRequestClient>> {
    request_store: RequestStore<EmbedRequestClient>,
    request_executor: TRequestExecutor,
    receiver: mpsc::Receiver<BatchWorkerMessage>,
    worker_id: Uuid,
}

impl<TApiExecutor: GenericRequestExecutor<EmbedRequestClient> + 'static> EmbedApiBatchWorker<TApiExecutor>
{
    fn new(
        request_store: RequestStore<EmbedRequestClient>,
        request_executor: TApiExecutor,
        receiver: mpsc::Receiver<BatchWorkerMessage>,
        worker_id: Uuid,
    ) -> Self {
        Self {
            request_store,
            request_executor,
            worker_id,
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
            "Flushing requests from clients. [worker_id={:#?}, client_ids = {:#?}]",
            self.worker_id, client_ids
        );

        self.request_executor
            .execute_request(current_batch_size, requests);
    }

    // Message handlers
    fn handle_new_request(&mut self, req: EmbedRequestClient) {
        info!(
            "Accepted request from client. [worker_id={:#?}, client_id = {}]",
            self.worker_id,
            req.client_id()
        );

        if let Some(req) = self.request_store.try_store(req) {
            info!(
                "Could not store request, max batch size was reached. Flushing current batch. [worker_id={:#?}]",
                self.worker_id
            );
            self.flush_batch();
            self.request_store.force_store(req);
        }
    }
}

pub fn start(
    api_client: Arc<impl ApiClient + 'static>,
    api_parameters: EmbedRequestGroupingParams,
    batch_config: &BatchSettings,
    worker_id: Uuid,
    cancellation_token: CancellationToken,
) -> EmbedApiBatchWorkerHandle {
    let (sender, receiver) = mpsc::channel::<BatchWorkerMessage>(64);
    let request_executor = RequestExecutor::new(api_client, api_parameters);
    let request_store = RequestStore::new(batch_config.max_batch_size);

    let worker =
        EmbedApiBatchWorker::new(request_store, request_executor, receiver, worker_id.clone());
    let flush_wait_duration = Duration::from_millis(batch_config.max_waiting_time_ms);

    tokio::spawn(async move {
        run_worker(worker, flush_wait_duration, cancellation_token).await;
    });

    EmbedApiBatchWorkerHandle { sender, worker_id }
}

async fn run_worker(
    mut worker: EmbedApiBatchWorker<impl GenericRequestExecutor<EmbedRequestClient> + 'static>,
    flush_wait_duration: Duration,
    cancellation_token: CancellationToken,
) {
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
            _ = tokio::time::sleep(flush_wait_duration) => {
                worker.flush_batch();
            },
            _ = cancellation_token.cancelled() => {
                info!("Flusing batch and stopping worker.");
                worker.flush_batch();
                break;
            }
        }
    }
}

#[cfg(test)]
mod batch_worker_tests {}
