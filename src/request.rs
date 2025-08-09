use tokio::sync::oneshot;
use uuid::Uuid;

/// This struct represents request parameters that are used to batch similar requests together
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct EmbedRequestParams {
    pub dimensions: Option<usize>,
    pub normalize: Option<bool>,
    pub prompt_name: Option<String>,
    pub truncate: Option<bool>,
    pub truncation_direction: Option<String>,
}

pub struct EmbedRequestHandle {
    pub reply_handle: oneshot::Sender<anyhow::Result<Vec<Vec<f64>>>>,
    pub request_data: Vec<String>,
    pub request_id: Uuid
}

