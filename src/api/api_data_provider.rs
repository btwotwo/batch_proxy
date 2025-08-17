use crate::api::client::ApiClient;

pub struct ApiDataProvider<TApiClient: ApiClient> {
    pub api_client: TApiClient,
}
