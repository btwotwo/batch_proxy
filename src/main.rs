use std::sync::Arc;

use actix_web::{App, HttpServer, post, web};
use api_client::{EmbedApiEndpoint, EmbedApiRequest};
use batch::{batch_managerv2::{self, BatchManagerHandleV2}, request_executor::ApiBatchExecutor};
use settings::Settings;

mod api_client;
mod batch;
mod request;
mod settings;

#[post("/embed")]
async fn embed(
    batch_manager: web::Data<BatchManagerHandleV2<EmbedApiEndpoint>>,
    req: web::Json<EmbedApiRequest>,
) -> actix_web::Result<String> {
    let result = batch_manager
        .call_api(req.into_inner())
        .await
        .map_err(|e: anyhow::Error| actix_web::error::ErrorInternalServerError(e))?;

    let json = serde_json::to_string(&result)?;

    Ok(json)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();
    let settings = web::Data::new(Settings::new().unwrap());
    let target_port = settings.api.target_port;
    let api_client = api_client::ReqwestApiClient::new(&settings.inference_api.target_url).unwrap();


    let batch_executor = Arc::new(ApiBatchExecutor {
        api_client,
    });
    
    let batch_managerv2 = batch_managerv2::start(Arc::clone(&batch_executor), settings.batch.clone());
    let batch_manager_data = web::Data::new(batch_managerv2);
    
    HttpServer::new(move || {
        App::new()
            .app_data(batch_manager_data.clone())
            .app_data(settings.clone())
            .service(embed)
    })
    .bind(("0.0.0.0", target_port))?
    .run()
    .await
}
