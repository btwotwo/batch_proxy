use log::error;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::api::endpoint::ApiEndpont;

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
