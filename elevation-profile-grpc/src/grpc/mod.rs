mod server;
pub use server::ApiServer;
mod status;

pub mod pb {
    tonic::include_proto!("elevation_service");
}
