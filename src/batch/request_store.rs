use crate::request::Request;

pub struct RequestStore<TRequest: Request> {
    pending_requests: Vec<TRequest>,
    current_batch_size: usize,
    max_batch_size: usize,
}

impl<TRequest: Request> RequestStore<TRequest> {
    pub fn new(max_batch_size: usize) -> Self {
        Self {
            pending_requests: Vec::new(),
            current_batch_size: 0,
            max_batch_size,
        }
    }

    /// Tries to store request, returns it back if maximum batch size has been reached.
    pub fn try_store(&mut self, req: TRequest) -> Option<TRequest> {
        let data_count = req.data_count();

        if self.current_batch_size + data_count > self.max_batch_size {
            return Some(req);
        }

        self.current_batch_size += data_count;
        self.pending_requests.push(req);

        None
    }

    /// Stores request ignoring the maximum batch size setting.
    pub fn force_store(&mut self, req: TRequest) {
        let data_count = req.data_count();

        self.current_batch_size += data_count;
        self.pending_requests.push(req);
    }

    /// Empties the store, returning stored requests and current batch size.
    pub fn drain(&mut self) -> (usize, Vec<TRequest>) {
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
    use crate::request::Request;

    use super::*;
    const MOCK_DATA_COUNT: usize = 2;

    struct MockRequest;
    impl Request for MockRequest {
        fn data_count(&self) -> usize {
            MOCK_DATA_COUNT
        }
    }

    #[test]
    fn given_request__when_retrieved__should_give_correct_data_count() {
        let mut store = RequestStore::<MockRequest>::new(2);
        store.try_store(MockRequest);

        let (data_size, _) = store.drain();

        assert_eq!(data_size, MOCK_DATA_COUNT)
    }

    #[test]
    fn given_request__when_stored__should_return_request_if_larger_than_configured_max() {
        let mut store = RequestStore::<MockRequest>::new(1);
        let result = store.try_store(MockRequest);

        assert!(result.is_some(), "Should return request back.");

        let (data_size, data) = store.drain();

        assert_eq!(data_size, 0);
        assert!(data.is_empty())
    }
}
