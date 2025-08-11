use std::{sync::Arc, time::Duration};

use anyhow::anyhow;
use log::{error, info};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    api_client::ApiClient,
    config::BatchConfiguration,
    request::{Caller, EmbedRequestCaller, EmbedRequestGroupingParams},
};

use super::{
    request_executor::{self, GenericRequestExecutor, RequestExecutor},
    request_store::RequestStore,
};

enum BatchWorkerMessage<TCaller> {
    NewRequest(TCaller),
}

#[derive(Clone)]
pub struct BatchWorkerHandle<TCaller> {
    sender: mpsc::Sender<BatchWorkerMessage<TCaller>>,
}

impl<TCaller: Caller> BatchWorkerHandle<TCaller> {
    pub fn put_request(&self, req: TCaller) {
        let sender = self.sender.clone();

        tokio::spawn(async move {
            sender
                .send(BatchWorkerMessage::NewRequest(req))
                .await
                .unwrap_or_else(|err| {
                    error!("Error sending message to worker! [Error = {err}]");

                    match err.0 {
                        BatchWorkerMessage::NewRequest(req) => {
                            req.reply_handle().reply_with_error(anyhow!(
                                "Could not process request, please try again."
                            ));
                        }
                    }
                })
        });
    }
}

pub struct BatchWorker<TCaller, TRequestExecutor>
where
    TCaller: Caller,
    TRequestExecutor: GenericRequestExecutor<TCaller>,
{
    request_store: RequestStore<TCaller>,
    request_executor: TRequestExecutor,
    receiver: mpsc::Receiver<BatchWorkerMessage<TCaller>>,
    // TODO: Add worker ID
}

impl<TCaller, TRequestExecutor> BatchWorker<TCaller, TRequestExecutor>
where
    TCaller: Caller,
    TRequestExecutor: GenericRequestExecutor<TCaller>,
{
    fn new(
        receiver: mpsc::Receiver<BatchWorkerMessage<TCaller>>,
        request_executor: TRequestExecutor,
        batch_config: &BatchConfiguration,
    ) -> Self {
        Self {
            request_store: RequestStore::new(batch_config.max_batch_size),
            request_executor,
            receiver,
        }
    }

    fn handle_message(&mut self, message: BatchWorkerMessage<TCaller>) {
        match message {
            BatchWorkerMessage::NewRequest(req) => self.handle_new_request(req),
        }
    }

    fn flush_batch(&mut self) {
        if self.request_store.is_empty() {
            return;
        }

        let (current_batch_size, requests) = self.request_store.drain();

        let caller_ids: Vec<_> = requests.iter().map(|r| r.caller_id()).collect();

        info!(
            "Flushing requests from callers. [caller_ids = {:#?}]",
            caller_ids
        );

        self.request_executor
            .execute_request(current_batch_size, requests);
    }

    // Message handlers
    fn handle_new_request(&mut self, req: TCaller) {
        info!(
            "Accepted request from caller. [caller_id = {}]",
            req.caller_id()
        );

        if let Some(req) = self.request_store.try_store(req) {
            info!("Could not store request, max batch size was reached. Flushing current batch.");
            self.flush_batch();
            self.request_store.force_store(req);
        }
    }
}

pub fn start_worker<TApiClient: ApiClient + 'static, TRequestGroupingParams, TCaller: Caller>(
    api_client: Arc<TApiClient>,
    api_parameters: TRequestGroupingParams,
    batch_config: &BatchConfiguration,
    cancellation_token: CancellationToken,
) -> BatchWorkerHandle<TCaller>
where
    RequestExecutor<TApiClient, TRequestGroupingParams>: GenericRequestExecutor<TCaller>,
{
    let (sender, receiver) = mpsc::channel::<BatchWorkerMessage<TCaller>>(64);
    let request_executor = RequestExecutor::new(Arc::clone(&api_client), api_parameters);
    let mut worker = BatchWorker::new(receiver, request_executor, batch_config);

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

    BatchWorkerHandle { sender }
}

#[cfg(test)]
mod tests {}
