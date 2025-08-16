use std::{collections::HashMap, sync::Arc};

use log::info;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{
    api_client::{ApiClient, ApiEndpont},
    request::{GroupingParams, RequestClient},
    settings::BatchSettings,
};

use super::{
    batch_workerv2::{BatchWorkerV2, BatchWorkerV2Handle},
    request_executor::BatchExecutor,
};

struct BatchManagerV2<TApiEndpoint: ApiEndpont, TExecutor: BatchExecutor<TApiEndpoint>> {
    workers: HashMap<TApiEndpoint::GroupingParams, BatchWorkerV2Handle<TApiEndpoint>>,
    batch_executor: Arc<TExecutor>,
    batch_config: BatchSettings,
}

impl<TApiEndpoint: ApiEndpont, TExecutor: BatchExecutor<TApiEndpoint>>
    BatchManagerV2<TApiEndpoint, TExecutor>
{
    fn handle_messages(&mut self, message: BatchManagerMessageV2<TApiEndpoint>) {
        match message {
            BatchManagerMessageV2::NewRequest(req, grouping_params) => {
                let worker = self.workers.entry(grouping_params).or_insert_with_key(|grouping_params| {
                    let worker_id = Uuid::new_v4();

                    info!("Starting new worker. [parameters = {grouping_params:#?}, worker_id = {worker_id}");
                    super::batch_workerv2::start(Arc::new(grouping_params.clone()), &self.batch_config, worker_id, Arc::clone(&self.batch_executor))
                });

                worker.put_request(req);
            }
        }
    }
}

enum BatchManagerMessageV2<TApiEndpoint: ApiEndpont> {
    NewRequest(RequestClient<TApiEndpoint>, TApiEndpoint::GroupingParams),
}

pub struct BatchManagerHandleV2<TApiEndpoint: ApiEndpont> {
    sender: mpsc::Sender<BatchManagerMessageV2<TApiEndpoint>>,
}

impl<TApiEndpoint: ApiEndpont> BatchManagerHandleV2<TApiEndpoint> {
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
            .send(BatchManagerMessageV2::NewRequest(client, grouping_params))
            .await?;

        receiver.await?
    }
}

pub fn start<TApiEndpoint: ApiEndpont>(
    batch_executor: Arc<impl BatchExecutor<TApiEndpoint>>,
    batch_config: BatchSettings,
) -> BatchManagerHandleV2<TApiEndpoint> {
    let (sender, mut receiver) = mpsc::channel::<BatchManagerMessageV2<TApiEndpoint>>(2048);
    let mut manager = BatchManagerV2 {
        workers: HashMap::new(),
        batch_executor,
        batch_config,
    };

    tokio::spawn(async move {
        while let Some(msg) = receiver.recv().await {
            manager.handle_messages(msg);
        }
    });

    BatchManagerHandleV2 { sender }
}
