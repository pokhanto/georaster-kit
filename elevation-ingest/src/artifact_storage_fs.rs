use std::path::PathBuf;

use elevation_types::ArtifactStorage;

pub struct FsArtifactStorage {}

impl ArtifactStorage for FsArtifactStorage {
    fn save_artifact(&self, bytes: Vec<u8>) -> PathBuf {
        let path: PathBuf = "./data/cog.tif".into();
        std::fs::write(&path, &bytes).unwrap();

        path
    }

    fn load_artifact(&self) {
        todo!()
    }
}
