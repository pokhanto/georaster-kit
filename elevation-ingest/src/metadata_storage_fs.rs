use std::{fs::File, io::BufWriter};

use elevation_types::{DatasetMetadata, MetadataStorage};

pub struct FsMetadataStorage {}

const PATH: &str = "./data/dataset.json";

// TODO: rework to Result
impl MetadataStorage for FsMetadataStorage {
    fn save_metadata(&self, metadata: &DatasetMetadata) {
        let file = File::create(PATH).unwrap();
        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, metadata).unwrap();
    }

    fn load_metadata(&self) -> DatasetMetadata {
        serde_json::from_reader(std::fs::File::open(PATH).unwrap()).unwrap()
    }
}
