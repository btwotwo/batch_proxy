use log::error;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::api_client::EmbedApiRequestInputs;

pub trait Caller: Send {
    type ExpectedResult;
    fn data_count(&self) -> usize;
    fn caller_id(&self) -> Uuid;
    fn reply_handle(self) -> ReplyHandle<Self::ExpectedResult>;
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

pub struct EmbedRequestCaller {
    pub reply_handle: ReplyHandle<Vec<Vec<f64>>>,
    pub request_data: Vec<String>,
}

impl EmbedRequestCaller {
    pub fn new(
        request_inputs: EmbedApiRequestInputs,
        caller_id: Uuid,
    ) -> (oneshot::Receiver<anyhow::Result<Vec<Vec<f64>>>>, Self) {
        let (sender, receiver) = oneshot::channel();
        let request_data = match request_inputs {
            EmbedApiRequestInputs::Str(input) => vec![input],
            EmbedApiRequestInputs::Vec(inputs) => inputs,
        };

        (
            receiver,
            EmbedRequestCaller {
                request_data,
                reply_handle: ReplyHandle {
                    reply_handle: sender,
                    caller_id
                },
            },
        )
    }

    pub fn caller_id(&self) -> Uuid {
        self.reply_handle.caller_id
    }
}

pub struct ReplyHandle<TResult> {
    pub reply_handle: oneshot::Sender<anyhow::Result<TResult>>,
    pub caller_id: Uuid,
}

impl<TResult> ReplyHandle<TResult> {
    pub fn reply_with_result(self, result: TResult) {
        self.reply_handle.send(Ok(result)).unwrap_or_else(|_| {
            error!(
                "Could not send response to caller, receiver has dropped. [CallerId = {0}]",
                self.caller_id
            )
        });
    }

    pub fn reply_with_error(self, error: anyhow::Error) {
        self.reply_handle.send(Err(error)).unwrap_or_else(|_| {
            error!(
                "Could not send response to caller, receiver has dropped. [CallerId = {0}]",
                self.caller_id
            )
        });
    }
}

impl Caller for EmbedRequestCaller {
    type ExpectedResult = Vec<Vec<f64>>;

    fn data_count(&self) -> usize {
        self.request_data.len()
    }

    fn caller_id(&self) -> Uuid {
        self.reply_handle.caller_id
    }

    fn reply_handle(self) -> ReplyHandle<Self::ExpectedResult> {
        self.reply_handle
    }
}
