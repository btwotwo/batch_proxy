use crate::{api::endpoint::ApiEndpont, request::RequestClient};

pub struct RequestStoreV2<TApiEndpoint: ApiEndpont> {
    pending_requests: Vec<RequestClient<TApiEndpoint>>,
    current_batch_size: usize,
    max_batch_size: usize,
}

impl<TApiEndpoint: ApiEndpont> RequestStoreV2<TApiEndpoint> {
    pub fn new(max_batch_size: usize) -> Self {
        Self {
            pending_requests: Vec::new(),
            current_batch_size: 0,
            max_batch_size,
        }
    }

    /// Tries to store request, returns it back if maximum batch size has been reached.
    pub fn try_store(
        &mut self,
        req: RequestClient<TApiEndpoint>,
    ) -> Option<RequestClient<TApiEndpoint>> {
        let data_count = req.data.len();

        if self.current_batch_size + data_count > self.max_batch_size {
            return Some(req);
        }

        self.current_batch_size += data_count;
        self.pending_requests.push(req);

        None
    }

    /// Stores request ignoring the maximum batch size setting.
    pub fn force_store(&mut self, req: RequestClient<TApiEndpoint>) {
        let data_count = req.data.len();

        self.current_batch_size += data_count;
        self.pending_requests.push(req);
    }

    /// Empties the store, returning stored requests and current batch size.
    pub fn drain(&mut self) -> (usize, Vec<RequestClient<TApiEndpoint>>) {
        let requests = std::mem::take(&mut self.pending_requests);
        let current_batch_size = std::mem::take(&mut self.current_batch_size);

        (current_batch_size, requests)
    }

    pub fn is_empty(&self) -> bool {
        self.pending_requests.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use crate::api::endpoint::GroupingParams;

    use super::*;
    struct TestApiEndpoint;
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct TestGroupingParams;
    impl GroupingParams for TestGroupingParams {
        type DataItem = ();

        type ApiRequest = ();

        fn to_request(&self, data: Vec<Self::DataItem>) -> Self::ApiRequest {
            todo!()
        }

        fn decompose_api_request(api_request: Self::ApiRequest) -> (Vec<Self::DataItem>, Self) {
            todo!()
        }
    }

    impl ApiEndpont for TestApiEndpoint {
        type ApiRequest = ();
        type ApiResponseItem = ();
        type DataItem = ();
        type GroupingParams = TestGroupingParams;
    }

    fn client(data_count: usize) -> RequestClient<TestApiEndpoint> {
        let (_, client) = RequestClient::new(vec![(); data_count], Uuid::new_v4());
        client
    }

    #[test]
    fn given_request__when_retrieved__should_give_correct_data_count() {
        let client = client(12);
        let mut store = RequestStoreV2::new(2);
        store.force_store(client);

        let (data_size, _) = store.drain();

        assert_eq!(data_size, 12)
    }

    #[test]
    fn given_request__when_stored__should_return_request_if_larger_than_configured_max() {
        let client = client(2);
        let mut store = RequestStoreV2::new(1);
        let result = store.try_store(client);

        assert!(result.is_some(), "Should return request back.");

        let (data_size, data) = store.drain();

        assert_eq!(data_size, 0);
        assert!(data.is_empty())
    }
}
