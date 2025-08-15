use std::fmt::Debug;

use log::error;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::api_client::{ApiClient, ApiEndpont, EmbedApiRequest, EmbedApiRequestInputs};

pub trait Request {
    fn data_count(&self) -> usize;
}

pub struct RequestClient<TApiEndpoint>
where
    TApiEndpoint: ApiEndpont,
{
    pub handle: RequestHandle<TApiEndpoint::ApiResponseItem>,
    pub data: Vec<TApiEndpoint::DataItem>,
}

impl<TApiEndpoint: ApiEndpont> RequestClient<TApiEndpoint> {
    pub fn new(
        data: Vec<TApiEndpoint::DataItem>,
        client_id: Uuid,
    ) -> (
        oneshot::Receiver<anyhow::Result<Vec<TApiEndpoint::ApiResponseItem>>>,
        Self,
    ) {
        let (sender, receiver) = oneshot::channel();
        let client = RequestClient {
            handle: RequestHandle {
                client_id,
                reply_handle: sender,
            },
            data,
        };

        (receiver, client)
    }
}

pub struct RequestHandle<O> {
    pub reply_handle: oneshot::Sender<anyhow::Result<Vec<O>>>,
    pub client_id: Uuid,
}

pub trait GroupingParams: Send + Sync {
    type DataItem;
    type ApiRequest;

    fn to_request(&self, data: Vec<Self::DataItem>) -> Self::ApiRequest;
    fn decompose_api_request(api_request: Self::ApiRequest) -> (Vec<Self::DataItem>, Self);
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

impl GroupingParams for EmbedRequestGroupingParams {
    type DataItem = String;
    type ApiRequest = EmbedApiRequest;

    fn to_request(&self, data: Vec<Self::DataItem>) -> Self::ApiRequest {
        EmbedApiRequest {
            inputs: EmbedApiRequestInputs::Vec(data),
            truncate: self.truncate,
            normalize: self.normalize,
            dimensions: self.dimensions,
            prompt_name: self.prompt_name.clone(),
            truncation_direction: self.truncation_direction.clone(),
        }
    }

    fn decompose_api_request(api_request: Self::ApiRequest) -> (Vec<Self::DataItem>, Self) {
        let EmbedApiRequest {
            inputs,
            dimensions,
            normalize,
            prompt_name,
            truncate,
            truncation_direction,
        } = api_request;

        let request_data = match inputs {
            EmbedApiRequestInputs::Str(input) => vec![input],
            EmbedApiRequestInputs::Vec(inputs) => inputs,
        };

        let request_params = EmbedRequestGroupingParams {
            truncate: truncate,
            normalize: normalize,
            dimensions: dimensions,
            prompt_name: prompt_name,
            truncation_direction: truncation_direction,
        };

        (request_data, request_params)
    }
}

pub struct EmbedRequestClient {
    pub reply_handle: EmbedRequestHandle,
    pub request_data: Vec<String>,
}

impl EmbedRequestClient {
    pub fn new(
        request_inputs: EmbedApiRequestInputs,
        client_id: Uuid,
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
                    client_id,
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
            error!(
                "Could not send response to client, receiver has dropped. [ClientId = {0}]",
                self.client_id
            )
        });
    }

    pub fn reply_with_error(self, error: anyhow::Error) {
        self.reply_handle.send(Err(error)).unwrap_or_else(|_| {
            error!(
                "Could not send response to client, receiver has dropped. [ClientId = {0}]",
                self.client_id
            )
        });
    }
}

impl<O> RequestHandle<O> {
    pub fn reply_with_result(self, result: Vec<O>) {
        self.reply_handle.send(Ok(result)).unwrap_or_else(|_| {
            error!(
                "Could not send response to client, receiver has dropped. [ClientId = {0}]",
                self.client_id
            )
        });
    }

    pub fn reply_with_error(self, error: anyhow::Error) {
        self.reply_handle.send(Err(error)).unwrap_or_else(|_| {
            error!(
                "Could not send response to client, receiver has dropped. [ClientId = {0}]",
                self.client_id
            )
        });
    }
}

impl Request for EmbedRequestClient {
    fn data_count(&self) -> usize {
        self.request_data.len()
    }
}
