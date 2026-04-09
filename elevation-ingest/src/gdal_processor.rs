//! GDAL command line wrapper.
use std::path::Path;
use std::process::Command;

/// GDAL command settings used by ingest pipeline.
#[derive(Debug, Clone)]
pub struct GdalProcessSettings {
    pub warp_binary: &'static str,
    pub translate_binary: &'static str,
    pub warp_resampling: &'static str,
    pub warp_output_format: &'static str,
    pub cog_output_format: &'static str,
    pub cog_compression: &'static str,
}

impl Default for GdalProcessSettings {
    fn default() -> Self {
        Self {
            warp_binary: "gdalwarp",
            translate_binary: "gdal_translate",
            warp_resampling: "bilinear",
            warp_output_format: "GTiff",
            cog_output_format: "COG",
            cog_compression: "LZW",
        }
    }
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum GdalProcessError {
    #[error("Failed to start gdalwarp: {0}")]
    WarpSpawn(#[source] std::io::Error),

    #[error("gdalwarp failed: {stderr}")]
    WarpFailed { stderr: String },

    #[error("Failed to start gdal_translate: {0}")]
    TranslateSpawn(#[source] std::io::Error),

    #[error("gdal_translate failed: {stderr}")]
    TranslateFailed { stderr: String },
}

/// Runs GDAL-based preprocessing commands.
#[derive(Debug, Clone)]
pub struct GdalProcessor {
    settings: GdalProcessSettings,
}

impl GdalProcessor {
    pub fn new(settings: GdalProcessSettings) -> Self {
        Self { settings }
    }

    /// Reprojects geotiff to given CRS. This operation will create new geotiff file.
    #[tracing::instrument(fields(source = %source.display(), target_crs = %crs, output = %output.display()), skip_all)]
    pub fn reproject_to_path(
        &self,
        source: &Path,
        crs: &str,
        output: &Path,
    ) -> Result<(), GdalProcessError> {
        let cmd_output = Command::new(self.settings.warp_binary)
            .arg("-t_srs")
            .arg(crs)
            .arg("-r")
            .arg(self.settings.warp_resampling)
            .arg("-of")
            .arg(self.settings.warp_output_format)
            .arg(source)
            .arg(output)
            .output()
            .map_err(GdalProcessError::WarpSpawn)?;

        if !cmd_output.status.success() {
            return Err(GdalProcessError::WarpFailed {
                stderr: String::from_utf8_lossy(&cmd_output.stderr).into_owned(),
            });
        }

        Ok(())
    }

    /// Translates geotiff to COG format. This operation will create new geotiff file.
    #[tracing::instrument(fields(source = %source.display(), output = %output.display()), skip_all)]
    pub fn translate_to_cog_path(
        &self,
        source: &Path,
        output: &Path,
    ) -> Result<(), GdalProcessError> {
        let cmd_output = Command::new(self.settings.translate_binary)
            .arg("-of")
            .arg(self.settings.cog_output_format)
            .arg("-co")
            .arg(format!("COMPRESS={}", self.settings.cog_compression))
            .arg(source)
            .arg(output)
            .output()
            .map_err(GdalProcessError::TranslateSpawn)?;

        if !cmd_output.status.success() {
            return Err(GdalProcessError::TranslateFailed {
                stderr: String::from_utf8_lossy(&cmd_output.stderr).into_owned(),
            });
        }

        Ok(())
    }
}
