mod request_executor;
mod request_store;
mod batch_manager;
use std::{sync::Arc, time::Duration};

use log::info;
use request_store::RequestStore;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    api_client::ApiClient,
    config::BatchConfiguration,
    request::{EmbedRequestHandle, EmbedRequestParams},
};

enum BatchMessage {
    NewRequest(EmbedRequestHandle),
}

struct EmbedApiBatchWorkerHandle {
    sender: mpsc::Sender<BatchMessage>,
}

impl EmbedApiBatchWorkerHandle {
    pub async fn put_request(&self, req: EmbedRequestHandle) {
        self.sender
            .send(BatchMessage::NewRequest(req))
            .await
            .expect("TODO: Proper error handling")
    }
}

struct EmbedApiBatchWorker<TApiClient: ApiClient> {
    request_store: RequestStore<EmbedRequestHandle>,
    receiver: mpsc::Receiver<BatchMessage>,
    api_client: Arc<TApiClient>,
    api_parameters: Arc<EmbedRequestParams>,
}

impl<TApiClient: ApiClient + 'static> EmbedApiBatchWorker<TApiClient> {
    pub fn start(
        api_client: TApiClient,
        api_parameters: EmbedRequestParams,
        batch_config: &BatchConfiguration,
        cancellation_token: CancellationToken,
    ) -> EmbedApiBatchWorkerHandle {
        let (tx, rx) = mpsc::channel::<BatchMessage>(64);
        let mut task = Self::new(rx, api_client, api_parameters, batch_config);
        let waiting_time_duration = Duration::from_millis(batch_config.max_waiting_time_ms);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    msg = task.receiver.recv() => {
                        match msg {
                            Some(msg) => task.handle_message(msg),
                            None => {
                                info!("Last sender was dropped, flushing batch and stopping batch worker with request parameters {:?}", task.api_parameters);
                                task.flush_batch();
                                break;
                            }
                        }
                    },
                    _ = tokio::time::sleep(waiting_time_duration) => {
                        task.flush_batch();
                    },
                    _ = cancellation_token.cancelled() => {
                        info!("Flusing batch and stopping worker with request parameters {:?}", task.api_parameters);
                        task.flush_batch();
                        break;
                    }
                }
            }
        });

        EmbedApiBatchWorkerHandle { sender: tx }
    }

    fn new(
        receiver: mpsc::Receiver<BatchMessage>,
        api_client: TApiClient,
        api_parameters: EmbedRequestParams,
        batch_config: &BatchConfiguration,
    ) -> Self {
        Self {
            request_store: RequestStore::new(batch_config.max_batch_size),
            api_client: Arc::new(api_client),
            api_parameters: Arc::new(api_parameters),
            receiver,
        }
    }

    fn handle_message(&mut self, message: BatchMessage) {
        match message {
            BatchMessage::NewRequest(req) => self.handle_new_request(req),
        }
    }

    fn flush_batch(&mut self) {
        if self.request_store.is_empty() {
            return;
        }

        let (current_batch_size, requests) = self.request_store.drain();

        request_executor::execute_request(
            requests,
            Arc::clone(&self.api_parameters),
            Arc::clone(&self.api_client),
            current_batch_size,
        );
    }

    // Message handlers
    fn handle_new_request(&mut self, req: EmbedRequestHandle) {
        if let Some(req) = self.request_store.try_store(req) {
            self.flush_batch();
            self.request_store.force_store(req);
        }
    }
}
#[cfg(test)]
mod tests {
    
}
