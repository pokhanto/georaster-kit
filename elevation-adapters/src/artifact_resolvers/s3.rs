use elevation_domain::{
    ArtifactLocator, ArtifactResolveError, ArtifactResolver, ResolvedArtifactPath,
};

/// Resolves `s3://...` locators into GDAL `/vsis3/...` paths.
/// Uses (GDAL Virtual file system)[https://gdal.org/en/stable/user/virtual_file_systems.html]
#[derive(Debug, Clone, Default)]
pub struct GdalS3ArtifactResolver;

impl ArtifactResolver for GdalS3ArtifactResolver {
    fn resolve(
        &self,
        locator: &ArtifactLocator,
    ) -> Result<ResolvedArtifactPath, ArtifactResolveError> {
        let raw = locator.as_ref();

        let rest = raw
            .strip_prefix("s3://")
            .ok_or_else(|| ArtifactResolveError::UnsupportedLocator(raw.to_string()))?;

        if rest.is_empty() {
            return Err(ArtifactResolveError::UnsupportedLocator(raw.to_string()));
        }

        Ok(ResolvedArtifactPath::new(format!("/vsis3/{rest}")))
    }
}
