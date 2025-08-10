use actix_web::{App, HttpServer, post, web};
use api_client::EmbedApiRequest;
use batch::batch_manager::BatchManagerHandle;
use config::BatchConfiguration;

mod api_client;
mod batch;
mod config;
mod request;

#[post("/embed")]
async fn embed(batch_manager: web::Data<BatchManagerHandle>, req: web::Json<EmbedApiRequest>) -> actix_web::Result<String> {
    let result = batch_manager
        .call_batched_embed(req.into_inner())
        .await
        .map_err(|e: anyhow::Error| actix_web::error::ErrorInternalServerError(e))?;

    let json = serde_json::to_string(&result)?;

    Ok(json)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    colog::init();
    let api_client = api_client::ReqwestApiClient::new("http://localhost:8080").unwrap();
    let batch_manager = web::Data::new(batch::batch_manager::start(
        api_client,
        BatchConfiguration {
            max_waiting_time_ms: 5000,
            max_batch_size: 10,
        },
    ));

    HttpServer::new(move || App::new().app_data(batch_manager.clone()).service(embed))
        .bind(("127.0.0.1", 8087))?
        .run()
        .await
}
