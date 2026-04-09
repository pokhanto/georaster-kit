use elevation_adapters::{FsArtifactStorage, FsMetadataStorage};
use elevation_domain::{Crs, MetadataStorage};
use elevation_ingest::ingest;
use std::path::PathBuf;
use tempfile::tempdir;

#[tokio::test]
async fn ingest_stores_artifact_and_metadata_for_raster_fixture() {
    let metadata_registry_name = "registry";
    let temp_dir = tempdir().unwrap();
    let base_dir = temp_dir.path().to_path_buf();

    let source_path = PathBuf::from("tests/fixtures/geo.tif");
    let dataset_id = "dataset-1".to_string();
    let target_crs = Crs::new("EPSG:4326".to_string());

    let artifact_storage = FsArtifactStorage::new(base_dir.clone());
    let metadata_storage = FsMetadataStorage::new(base_dir.clone(), metadata_registry_name.into());

    ingest(
        dataset_id.clone(),
        source_path,
        target_crs.clone(),
        artifact_storage,
        metadata_storage,
    )
    .await
    .unwrap();

    let metadata_storage = FsMetadataStorage::new(base_dir, metadata_registry_name.into());

    let mut metadata = metadata_storage.load_metadata().await.unwrap();
    let metadata = metadata.remove(0);

    assert_eq!(metadata.dataset_id, dataset_id);
    assert_eq!(metadata.raster.crs, target_crs);

    assert!(metadata.raster.width > 0);
    assert!(metadata.raster.height > 0);

    assert!(metadata.raster.bounds.min_lon() < metadata.raster.bounds.max_lon());
    assert!(metadata.raster.bounds.min_lat() < metadata.raster.bounds.max_lat());

    assert!(!metadata.artifact_path.as_ref().is_empty());

    let artifact_path = PathBuf::from(&metadata.artifact_path.as_ref());
    assert!(artifact_path.exists(), "artifact file should exist");
}
