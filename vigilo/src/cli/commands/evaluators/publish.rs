//! Evaluator publish command implementation.
//!
//! This module resolves the requested manifest profile, prepares the evaluator
//! Wasm artifact, and inserts it into the built-in Vigilo registry namespace.
//! Accepts source directories and portable package directories or archives.
//! Source profiles select prebuilt artifacts; portable packages are self-contained.

use super::*;

pub(super) async fn exec(
    context: Context,
    evaluator_path: PathBuf,
    release: bool,
    profile: Option<String>,
) -> anyhow::Result<()> {
    info!("publishing evaluator: {}", evaluator_path.display());

    let profile = get_manifest_profile(release, profile);
    let outcome =
        crate::evaluators::publish_vigilo_evaluator(&context, evaluator_path, profile).await?;
    let evaluator = outcome.evaluator();
    info!(
        "finished evaluator publish for {}/{}:{}",
        evaluator.namespace, evaluator.name, evaluator.version,
    );

    Ok(())
}
