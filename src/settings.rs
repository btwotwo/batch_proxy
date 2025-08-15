use std::env;

use config::{Config, ConfigError, Environment, File};
use log::info;
use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
#[allow(unused)]
pub struct BatchSettings {
    pub max_batch_size: usize,
    pub max_waiting_time_ms: u64,
}

#[derive(Deserialize, Debug, Clone)]
#[allow(unused)]
pub struct ApiSettings {
    pub target_port: u16,
}

#[derive(Deserialize, Debug, Clone)]
#[allow(unused)]
pub struct InferenceApiSettings {
    pub target_url: String,
}

#[derive(Deserialize, Debug, Clone)]
#[allow(unused)]
pub struct Settings {
    pub api: ApiSettings,
    pub inference_api: InferenceApiSettings,
    pub batch: BatchSettings,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let s = Config::builder()
            .add_source(File::with_name("settings"))
            .add_source(File::with_name("settings.local").required(false))
            .add_source(Environment::with_prefix("batch_proxy").separator("__"))
            .build()?;

        let settings = s.try_deserialize()?;

        info!("Loaded settings. {:#?}", settings);

        Ok(settings)
    }
}
