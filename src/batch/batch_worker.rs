use std::{sync::Arc, time::Duration};

use anyhow::anyhow;
use log::{error, info};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{
    api::endpoint::ApiEndpont, batch::batch_executor, request::RequestClient,
    settings::BatchSettings,
};

use super::{DataProvider, request_store::RequestStore};

enum BatchWorkerMessage<TApiEndpoint: ApiEndpont> {
    NewRequest(RequestClient<TApiEndpoint>),
}

pub struct BatchWorker<TApiEndpoint: ApiEndpont, TDataProvider: DataProvider<TApiEndpoint>> {
    request_store: RequestStore<TApiEndpoint>,
    data_provider: Arc<TDataProvider>,
    receiver: mpsc::Receiver<BatchWorkerMessage<TApiEndpoint>>,
    worker_id: Uuid,
    grouping_params: Arc<TApiEndpoint::GroupingParams>,
}

pub struct BatchWorkerHandle<TApiEndpoint: ApiEndpont> {
    sender: mpsc::Sender<BatchWorkerMessage<TApiEndpoint>>,
    worker_id: Uuid,
}

impl<TApiEndpoint: ApiEndpont> BatchWorkerHandle<TApiEndpoint> {
    pub fn put_request(&self, req: RequestClient<TApiEndpoint>) {
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
                            req.handle.reply_with_error(anyhow!(
                                "Could not process request, please try again."
                            ));
                        }
                    }
                })
        });
    }
}

impl<TApiEndpoint: ApiEndpont, TBatchExecutor: DataProvider<TApiEndpoint>>
    BatchWorker<TApiEndpoint, TBatchExecutor>
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

        let executor = Arc::clone(&self.data_provider);
        let grouping_params = Arc::clone(&self.grouping_params);

        tokio::spawn(async move {
            batch_executor::execute_batch(executor, grouping_params, requests, current_batch_size)
                .await
        });
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

    fn handle_message(&mut self, message: BatchWorkerMessage<TApiEndpoint>) {
        match message {
            BatchWorkerMessage::NewRequest(req) => self.handle_new_request(req),
        }
    }
}

pub fn start<TApiEndpoint, TDataProvider>(
    grouping_params: Arc<TApiEndpoint::GroupingParams>,
    batch_config: &BatchSettings,
    worker_id: Uuid,
    data_provider: Arc<TDataProvider>,
) -> BatchWorkerHandle<TApiEndpoint>
where
    TApiEndpoint: ApiEndpont,
    TDataProvider: DataProvider<TApiEndpoint>,
{
    let (sender, receiver) = mpsc::channel(2048);
    let flush_wait_duration = Duration::from_millis(batch_config.max_waiting_time_ms);

    let worker = BatchWorker {
        request_store: RequestStore::new(batch_config.max_batch_size),
        data_provider,
        receiver,
        worker_id,
        grouping_params,
    };

    tokio::spawn(async move { run_worker(worker, flush_wait_duration).await });

    BatchWorkerHandle { sender, worker_id }
}

async fn run_worker<TApiEndpoint: ApiEndpont, TBatchExecutor: DataProvider<TApiEndpoint>>(
    mut worker: BatchWorker<TApiEndpoint, TBatchExecutor>,
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
