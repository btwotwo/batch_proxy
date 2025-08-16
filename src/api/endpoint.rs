pub mod embed_endpoint;

pub trait GroupingParams: Send + Sync {
    type DataItem;
    type ApiRequest;

    fn to_request(&self, data: Vec<Self::DataItem>) -> Self::ApiRequest;
    fn decompose_api_request(api_request: Self::ApiRequest) -> (Vec<Self::DataItem>, Self);
}

pub trait ApiEndpont: 'static {
    type ApiRequest: Send + Sync + std::fmt::Debug;
    type ApiResponseItem: Send + Sync + std::fmt::Debug;
    type DataItem: Send + Sync + std::fmt::Debug;
    type GroupingParams: Send
        + GroupingParams<DataItem = Self::DataItem, ApiRequest = Self::ApiRequest>
        + std::hash::Hash
        + Eq
        + Clone
        + std::fmt::Debug;
}
