use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, thiserror::Error)]
pub enum GdalProcessError {
    #[error("Failed to start gdalwarp: {0}")]
    WarpSpawn(#[source] std::io::Error),

    #[error("Gdalwarp failed: {0}")]
    WarpFailed(String),

    #[error("Failed to start gdal_translate: {0}")]
    TranslateSpawn(#[source] std::io::Error),

    #[error("Gdal_translate failed: {0}")]
    TranslateFailed(String),
}

fn get_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

// TODO: move to constants or config
#[tracing::instrument(fields(source = %source.display(), target_crs = %crs), skip_all)]
pub fn reproject(source: &Path, crs: &str) -> Result<PathBuf, GdalProcessError> {
    let tmp_file_name = format!("elevation-reprojected-{}.tif", get_millis());
    let tmp_file = std::env::temp_dir().join(tmp_file_name);

    tracing::debug!(tmp_file = %tmp_file.display(), "running gdalwarp");

    let warp_output = Command::new("gdalwarp")
        .arg("-t_srs")
        .arg(crs)
        .arg("-r")
        // TODO: consider settings
        .arg("bilinear")
        .arg("-of")
        .arg("GTiff")
        .arg(source)
        .arg(&tmp_file)
        .output()
        .map_err(GdalProcessError::WarpSpawn)?;

    if !warp_output.status.success() {
        return Err(GdalProcessError::WarpFailed(
            String::from_utf8_lossy(&warp_output.stderr).into_owned(),
        ));
    }

    tracing::debug!(tmp_file = %tmp_file.display(), "gdalwarp completed successfully");

    Ok(tmp_file)
}

#[tracing::instrument(fields(source = %source.display()), skip_all)]
pub fn translate_to_cog(source: &Path) -> Result<PathBuf, GdalProcessError> {
    let tmp_file_name = format!("elevation-cog-{}.tif", get_millis());
    let tmp_file = std::env::temp_dir().join(tmp_file_name);

    tracing::debug!(tmp_file = %tmp_file.display(), "running gdal_translate to COG");

    let translate_output = Command::new("gdal_translate")
        .arg("-of")
        .arg("COG")
        .arg("-co")
        .arg("COMPRESS=LZW")
        .arg(source)
        .arg(&tmp_file)
        .output()
        .map_err(GdalProcessError::TranslateSpawn)?;

    if !translate_output.status.success() {
        return Err(GdalProcessError::TranslateFailed(
            String::from_utf8_lossy(&translate_output.stderr).into_owned(),
        ));
    }

    tracing::debug!(tmp_file = %tmp_file.display(), "gdal_translate completed successfully");

    Ok(tmp_file)
}
