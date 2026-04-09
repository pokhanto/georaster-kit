use elevation_domain::{
    ArtifactLocator, ArtifactResolveError, ArtifactResolver, ResolvedArtifactPath,
};

/// Resolves local filesystem artifact locators as is.
#[derive(Debug, Clone, Default)]
pub struct FsArtifactResolver;

impl ArtifactResolver for FsArtifactResolver {
    fn resolve(
        &self,
        locator: &ArtifactLocator,
    ) -> Result<ResolvedArtifactPath, ArtifactResolveError> {
        Ok(ResolvedArtifactPath::new(locator.to_string()))
    }
}
