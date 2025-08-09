mod request_executor;
mod request_store;
use std::{sync::Arc, time::Duration};

use anyhow::anyhow;
use log::error;
use request_store::RequestStore;
use tokio::sync::mpsc;

use crate::{
    api_client::{ApiClient, EmbedApiRequest},
    request::{EmbedRequestHandle, EmbedRequestParams},
};

const MAX_BATCHED_COUNT: usize = 4;
const MAX_WAITING_TIME: std::time::Duration = Duration::from_secs(10);

enum BatchMessage {
    NewRequest(EmbedRequestHandle),
}

struct EmbedApiBatchTaskHandle {
    sender: mpsc::Sender<BatchMessage>,
}

impl EmbedApiBatchTaskHandle {
    pub async fn put_request(&self, req: EmbedRequestHandle) {
        self.sender
            .send(BatchMessage::NewRequest(req))
            .await
            .expect("TODO: Proper error handling")
    }
}

struct EmbedApiBatchTask<TApiClient: ApiClient> {
    request_store: RequestStore<EmbedRequestHandle>,
    receiver: mpsc::Receiver<BatchMessage>,
    api_client: Arc<TApiClient>,
    api_parameters: Arc<EmbedRequestParams>,
}

impl<TApiClient: ApiClient + 'static> EmbedApiBatchTask<TApiClient> {
    pub fn start(
        api_client: TApiClient,
        api_parameters: EmbedRequestParams,
    ) -> EmbedApiBatchTaskHandle {
        let (tx, rx) = mpsc::channel::<BatchMessage>(64);
        let mut task = Self::new(rx, api_client, api_parameters);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(msg) = task.receiver.recv() => {
                        task.handle_message(msg);
                    },
                    _ = tokio::time::sleep(MAX_WAITING_TIME) => {
                        task.flush_batch();
                    },
                    else => break,
                }
            }
        });

        EmbedApiBatchTaskHandle { sender: tx }
    }

    fn new(
        receiver: mpsc::Receiver<BatchMessage>,
        api_client: TApiClient,
        api_parameters: EmbedRequestParams,
    ) -> Self {
        Self {
            request_store: RequestStore::new(MAX_BATCHED_COUNT),
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
    use super::*;
}
