//! Application config initialization.
use std::{net::SocketAddr, path::PathBuf};

#[derive(Clone, Debug)]
pub struct Config {
    pub app_addr: SocketAddr,
    pub storage_dir: PathBuf,
    pub file_to_ingest: PathBuf,
    pub metadata_registry_name: String,
}

impl Config {
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        dotenvy::dotenv().ok();
        let app_host = dotenvy::var("APP_HOST")?;
        let app_port = dotenvy::var("APP_PORT")?;
        let app_addr: SocketAddr = format!("{app_host}:{app_port}").parse()?;

        let storage_dir: PathBuf = dotenvy::var("STORAGE_DIR")?.into();
        let file_to_ingest: PathBuf = dotenvy::var("FILE_TO_INGEST")?.into();
        let metadata_registry_name = dotenvy::var("METADATA_REGISTRY_NAME")?;

        Ok(Self {
            app_addr,
            storage_dir,
            file_to_ingest,
            metadata_registry_name,
        })
    }
}
