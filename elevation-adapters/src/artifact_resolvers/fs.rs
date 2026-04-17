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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_returns_locator_as_resolved_path() {
        let resolver = FsArtifactResolver;
        let locator = ArtifactLocator::new("/tmp/data/dataset.tif");

        let resolved = resolver.resolve(&locator).unwrap();

        assert_eq!(resolved, ResolvedArtifactPath::new("/tmp/data/dataset.tif"));
    }
}
