use std::{net::SocketAddr, path::PathBuf};

#[derive(Clone, Debug)]
pub struct Config {
    pub grpc_addr: SocketAddr,
    pub sample_step_meters: f64,
    pub metadata_dir: PathBuf,
    pub max_samples: usize,
}

impl Config {
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        dotenvy::dotenv().ok();
        let app_host = dotenvy::var("APP_HOST")?;
        let app_port = dotenvy::var("APP_PORT")?;
        let grpc_addr: SocketAddr = format!("{app_host}:{app_port}").parse()?;

        let sample_step_meters = dotenvy::var("SAMPLE_STEP_METERS")
            .ok()
            .map(|v| v.parse())
            .transpose()?
            .unwrap_or(50.0);

        let max_samples: usize = dotenvy::var("MAX_SAMPLES")
            .ok()
            .map(|v| v.parse())
            .transpose()?
            .unwrap_or(50000);

        let metadata_dir: PathBuf = dotenvy::var("METADATA_STORAGE_DIR")?.into();
        metadata_dir.try_exists()?;

        Ok(Self {
            grpc_addr,
            sample_step_meters,
            metadata_dir,
            max_samples,
        })
    }
}
