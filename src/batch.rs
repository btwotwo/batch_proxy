use std::{sync::Arc, time::Duration};

use log::error;
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
    pending_requests: Vec<EmbedRequestHandle>,
    current_batch_size: usize,
    receiver: mpsc::Receiver<BatchMessage>,
    api_client: Arc<TApiClient>,
    api_parameters: Arc<EmbedRequestParams>,
}

impl<TApiClient: ApiClient + 'static> EmbedApiBatchTask<TApiClient> {
    fn new(
        receiver: mpsc::Receiver<BatchMessage>,
        api_client: TApiClient,
        api_parameters: EmbedRequestParams,
    ) -> Self {
        Self {
            pending_requests: Vec::new(),
            current_batch_size: 0,
            api_client: Arc::new(api_client),
            api_parameters: Arc::new(api_parameters),
            receiver,
        }
    }

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

    fn handle_message(&mut self, message: BatchMessage) {
        match message {
            BatchMessage::NewRequest(req) => self.handle_new_request(req),
        }
        todo!()
    }

    fn flush_batch(&mut self) {
        if self.pending_requests.is_empty() {
            return;
        }

        let requests = std::mem::take(&mut self.pending_requests);
        let current_batch_size = std::mem::take(&mut self.current_batch_size);


        execute_request(
            requests,
            Arc::clone(&self.api_parameters),
            Arc::clone(&self.api_client),
            current_batch_size,
        );
    }

    // Message handlers
    fn handle_new_request(&mut self, req: EmbedRequestHandle) {
        let data_len = req.request_data.len();

        // TODO: Replace with config
        if self.current_batch_size + data_len >= MAX_BATCHED_COUNT {}

        self.current_batch_size += data_len;
        self.pending_requests.push(req);
    }
}

fn execute_request(
    requests: Vec<EmbedRequestHandle>,
    request_parameters: Arc<EmbedRequestParams>,
    api_client: Arc<impl ApiClient + 'static>,
    current_batch_size: usize,
) {
    tokio::spawn(async move {
        let mut inputs = Vec::with_capacity(current_batch_size);
        let mut clients = Vec::with_capacity(requests.len());

        for request in requests {
            let input_len = request.request_data.len();
            let reply_handle = request.reply_handle;
            inputs.extend(request.request_data);
            clients.push((input_len, reply_handle));
        }

        let api_parameters = EmbedApiRequest {
            inputs,
            truncate: request_parameters.truncate,
            normalize: request_parameters.normalize.unwrap_or(false),
            dimensions: request_parameters.dimensions,
            prompt_name: request_parameters.prompt_name.clone(),
            truncation_direction: request_parameters.truncation_direction.clone(),
        };

        if let Ok(result) = api_client.call_embed(&api_parameters).await {
            let mut result_iterator = result.into_iter();

            for (data_len, client) in clients {
                let client_data: Vec<_> = result_iterator.by_ref().take(data_len).collect();
                // TODO: replace expect with logging
                client.send(Ok(client_data)).unwrap_or_else(|_| {
                    error!(
                        "Could not send response to client, receiver has dropped. [ClientId = TODO]"
                    )
                });
            }
        }
    });
}

#[cfg(test)]
mod tests {}
