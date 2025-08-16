use std::{sync::Arc, time::Duration};

use anyhow::anyhow;
use log::{error, info};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{
    api_client::{ApiClient, ApiEndpont},
    batch::request_executor,
    request::{GroupingParams, RequestClient},
    settings::BatchSettings,
};

use super::{
    request_executor::{ApiBatchExecutor, BatchExecutor},
    request_store::RequestStoreV2,
};

enum BatchWorkerMessageV2<TApiEndpoint: ApiEndpont> {
    NewRequest(RequestClient<TApiEndpoint>),
}

pub struct BatchWorkerV2<TApiEndpoint: ApiEndpont, TBatchExecutor: BatchExecutor<TApiEndpoint>> {
    request_store: RequestStoreV2<TApiEndpoint>,
    batch_executor: Arc<TBatchExecutor>,
    receiver: mpsc::Receiver<BatchWorkerMessageV2<TApiEndpoint>>,
    worker_id: Uuid,
    grouping_params: Arc<TApiEndpoint::GroupingParams>,
}

pub struct BatchWorkerV2Handle<TApiEndpoint: ApiEndpont> {
    sender: mpsc::Sender<BatchWorkerMessageV2<TApiEndpoint>>,
    worker_id: Uuid,
}

impl<TApiEndpoint: ApiEndpont> BatchWorkerV2Handle<TApiEndpoint> {
    pub fn put_request(&self, req: RequestClient<TApiEndpoint>) {
        let sender = self.sender.clone();
        let worker_id = self.worker_id;

        tokio::spawn(async move {
            sender
                .send(BatchWorkerMessageV2::NewRequest(req))
                .await
                .unwrap_or_else(|err| {
                    error!(
                        "Error sending message to worker! [error = {err}, worker_id = {worker_id}]"
                    );

                    match err.0 {
                        BatchWorkerMessageV2::NewRequest(req) => {
                            req.handle.reply_with_error(anyhow!(
                                "Could not process request, please try again."
                            ));
                        }
                    }
                })
        });
    }
}

impl<TApiEndpoint: ApiEndpont, TBatchExecutor: BatchExecutor<TApiEndpoint>>
    BatchWorkerV2<TApiEndpoint, TBatchExecutor>
{
    fn flush_batch(&mut self) {
        if self.request_store.is_empty() {
            return;
        }

        let (current_batch_size, requests) = self.request_store.drain();

        let client_ids: Vec<_> = requests.iter().map(|r| r.handle.client_id).collect();

        info!(
            "Flushing requests from clients. [worker_id={:#?}, client_ids = {:#?}]",
            self.worker_id, client_ids
        );

        let batch =
            request_executor::batch_requests(current_batch_size, requests, &self.grouping_params);
        let executor = Arc::clone(&self.batch_executor);

        tokio::spawn(async move { executor.execute_batch(batch).await });
    }

    fn handle_new_request(&mut self, req: RequestClient<TApiEndpoint>) {
        info!(
            "Accepted request from client. [worker_id={:#?}, client_id = {}]",
            self.worker_id, req.handle.client_id
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

    fn handle_message(&mut self, message: BatchWorkerMessageV2<TApiEndpoint>) {
        match message {
            BatchWorkerMessageV2::NewRequest(req) => self.handle_new_request(req),
        }
    }
}

pub fn start<TApiEndpoint, TBatchExecutor>(
    grouping_params: Arc<TApiEndpoint::GroupingParams>,
    batch_config: &BatchSettings,
    worker_id: Uuid,
    batch_executor: Arc<TBatchExecutor>,
) -> BatchWorkerV2Handle<TApiEndpoint>
where
    TApiEndpoint: ApiEndpont,
    TBatchExecutor: BatchExecutor<TApiEndpoint>,
{
    let (sender, receiver) = mpsc::channel(2048);
    let flush_wait_duration = Duration::from_millis(batch_config.max_waiting_time_ms);

    let worker = BatchWorkerV2 {
        request_store: RequestStoreV2::new(batch_config.max_batch_size),
        batch_executor,
        receiver,
        worker_id,
        grouping_params,
    };

    tokio::spawn(async move { run_worker(worker, flush_wait_duration).await });

    BatchWorkerV2Handle { sender, worker_id }
}

async fn run_worker<TApiEndpoint: ApiEndpont, TBatchExecutor: BatchExecutor<TApiEndpoint>>(
    mut worker: BatchWorkerV2<TApiEndpoint, TBatchExecutor>,
    flush_wait_duration: Duration,
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
        }
    }
}
