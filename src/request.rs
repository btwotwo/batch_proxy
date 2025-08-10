use log::error;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::api_client::EmbedApiRequestInputs;

pub trait Request {
    fn data_count(&self) -> usize;
}

/// This struct represents request parameters that are used to group similar requests together
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct EmbedRequestGroupingParams {
    pub dimensions: Option<usize>,
    pub normalize: Option<bool>,
    pub prompt_name: Option<String>,
    pub truncate: Option<bool>,
    pub truncation_direction: Option<String>,
}

pub struct EmbedRequestClient {
    pub reply_handle: EmbedRequestHandle,
    pub request_data: Vec<String>,
}

impl EmbedRequestClient {
    pub fn new(
        request_inputs: EmbedApiRequestInputs,
        client_id: Uuid
    ) -> (oneshot::Receiver<anyhow::Result<Vec<Vec<f64>>>>, Self) {
        let (sender, receiver) = oneshot::channel();
        let request_data = match request_inputs {
            EmbedApiRequestInputs::Str(input) => vec![input],
            EmbedApiRequestInputs::Vec(inputs) => inputs,
        };
        
        (
            receiver,
            EmbedRequestClient {
                request_data,
                reply_handle: EmbedRequestHandle {
                    reply_handle: sender,
                    client_id
                },
            },
        )
    }

    pub fn client_id(&self) -> Uuid {
        self.reply_handle.client_id
    }
}

pub struct EmbedRequestHandle {
    pub reply_handle: oneshot::Sender<anyhow::Result<Vec<Vec<f64>>>>,
    pub client_id: Uuid,
}

impl EmbedRequestHandle {
    pub fn reply_with_result(self, result: Vec<Vec<f64>>) {
        self.reply_handle.send(Ok(result)).unwrap_or_else(|_| {
            error!("Could not send response to client, receiver has dropped. [ClientId = {0}]", self.client_id)
        });
    }

    pub fn reply_with_error(self, error: anyhow::Error) {
        self.reply_handle.send(Err(error)).unwrap_or_else(|_| {
            error!("Could not send response to client, receiver has dropped. [ClientId = {0}]", self.client_id)
        });
    }
}

impl Request for EmbedRequestClient {
    fn data_count(&self) -> usize {
        self.request_data.len()
    }
}
