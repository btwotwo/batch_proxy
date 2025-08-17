use std::{collections::HashMap, sync::Arc};

use log::info;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{
    api::endpoint::ApiEndpont, api::endpoint::GroupingParams, request::RequestClient,
    settings::BatchSettings,
};

use super::{DataProvider, batch_worker::BatchWorkerHandle};

struct BatchManager<TApiEndpoint: ApiEndpont, TExecutor: DataProvider<TApiEndpoint>> {
    workers: HashMap<TApiEndpoint::GroupingParams, BatchWorkerHandle<TApiEndpoint>>,
    batch_executor: Arc<TExecutor>,
    batch_config: BatchSettings,
}

impl<TApiEndpoint: ApiEndpont, TExecutor: DataProvider<TApiEndpoint>>
    BatchManager<TApiEndpoint, TExecutor>
{
    fn handle_messages(&mut self, message: BatchManagerMessage<TApiEndpoint>) {
        match message {
            BatchManagerMessage::NewRequest(req, grouping_params) => {
                let worker = self.workers.entry(grouping_params).or_insert_with_key(|grouping_params| {
                    let worker_id = Uuid::new_v4();

                    info!("Starting new worker. [parameters = {grouping_params:#?}, worker_id = {worker_id}");
                    super::batch_worker::start(Arc::new(grouping_params.clone()), &self.batch_config, worker_id, Arc::clone(&self.batch_executor))
                });

                worker.put_request(req);
            }
        }
    }
}

enum BatchManagerMessage<TApiEndpoint: ApiEndpont> {
    NewRequest(RequestClient<TApiEndpoint>, TApiEndpoint::GroupingParams),
}

pub struct BatchManagerHandle<TApiEndpoint: ApiEndpont> {
    sender: mpsc::UnboundedSender<BatchManagerMessage<TApiEndpoint>>,
}

impl<TApiEndpoint: ApiEndpont> BatchManagerHandle<TApiEndpoint> {
    pub async fn call_api(
        &self,
        api_request: TApiEndpoint::ApiRequest,
    ) -> anyhow::Result<Vec<TApiEndpoint::ApiResponseItem>> {
        let (data, grouping_params) =
            TApiEndpoint::GroupingParams::decompose_api_request(api_request);

        let client_id = Uuid::new_v4();

        info!(
            "Adding request from the client to batcher. [input = {:?}, params = {:?}, client_id = {:?}]",
            data, grouping_params, client_id
        );

        let (receiver, client) = RequestClient::new(data, client_id);

        self.sender
            .send(BatchManagerMessage::NewRequest(client, grouping_params));

        receiver.await?
    }
}

pub fn start<TApiEndpoint: ApiEndpont>(
    batch_executor: Arc<impl DataProvider<TApiEndpoint>>,
    batch_config: BatchSettings,
) -> BatchManagerHandle<TApiEndpoint> {
    let (sender, mut receiver) = mpsc::unbounded_channel::<BatchManagerMessage<TApiEndpoint>>();
    let mut manager = BatchManager {
        workers: HashMap::new(),
        batch_executor,
        batch_config,
    };

    tokio::spawn(async move {
        while let Some(msg) = receiver.recv().await {
            manager.handle_messages(msg);
        }
    });

    BatchManagerHandle { sender }
}
