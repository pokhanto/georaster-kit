use tonic::Status;

use crate::application::ProfileServiceError;

pub fn profile_error_to_status(err: ProfileServiceError) -> Status {
    match err {
        ProfileServiceError::TooFewPoints => {
            Status::invalid_argument("Path must contain at least two points")
        }
        ProfileServiceError::InvalidStep => {
            Status::invalid_argument("Sample step must be greater than zero")
        }
        ProfileServiceError::InvalidCoordinate { index } => {
            Status::invalid_argument(format!("Invalid coordinate at index {index}"))
        }
        ProfileServiceError::TooManySamples => {
            Status::resource_exhausted("Too many sampled points")
        }
        ProfileServiceError::Elevation(_) => Status::internal("Elevation resolving failed"),
    }
}
