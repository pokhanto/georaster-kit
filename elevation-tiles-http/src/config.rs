//! Application config initialization.
use std::{net::SocketAddr, path::PathBuf};

#[derive(Clone, Debug)]
pub struct Config {
    pub app_addr: SocketAddr,
    pub metadata_dir: PathBuf,
    pub metadata_registry_name: String,
    pub tile_cache_max_capacity: u64,
}

impl Config {
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        dotenvy::dotenv().ok();
        let app_host = dotenvy::var("APP_HOST")?;
        let app_port = dotenvy::var("APP_PORT")?;
        let app_addr: SocketAddr = format!("{app_host}:{app_port}").parse()?;

        let metadata_dir: PathBuf = dotenvy::var("METADATA_STORAGE_DIR")?.into();
        let metadata_registry_name = dotenvy::var("METADATA_REGISTRY_NAME")?;

        let tile_cache_max_capacity: u64 = dotenvy::var("TILE_CACHE_MAX_CAPACITY")?.parse()?;

        Ok(Self {
            app_addr,
            metadata_dir,
            tile_cache_max_capacity,
            metadata_registry_name,
        })
    }
}
