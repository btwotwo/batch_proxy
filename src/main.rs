use actix_web::{App, HttpServer, post, web};
use api_client::EmbedApiRequest;
use batch::batch_manager::BatchManagerHandle;
use settings::{BatchSettings, Settings};

mod api_client;
mod batch;
mod request;
mod settings;

#[post("/embed")]
async fn embed(
    batch_manager: web::Data<BatchManagerHandle>,
    req: web::Json<EmbedApiRequest>,
) -> actix_web::Result<String> {
    let result = batch_manager
        .call_batched_embed(req.into_inner())
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

    let batch_manager = web::Data::new(batch::batch_manager::start(
        api_client,
        settings.batch.clone(),
    ));

    HttpServer::new(move || {
        App::new()
            .app_data(batch_manager.clone())
            .app_data(settings.clone())
            .service(embed)
    })
    .bind(("0.0.0.0", target_port))?
    .run()
    .await
}

