//! Persistence wiring for LDK's filesystem-backed KVStore.
//!
//! This module exists for one reason: LDK's `ProbabilisticScorer` is
//! persisted under a long key name (`SCORER_PERSISTENCE_KEY`) that
//! historically collided with an older file-backed format used by this
//! node. Rather than lose the user's scoring state across an upgrade,
//! [`ScorerKeyRemappingStore`] intercepts every KVStore call and rewrites
//! the scorer key to the legacy short name while passing every other key
//! through unchanged.
//!
//! Extracting this from `main.rs` has two benefits:
//!   1. `main.rs` becomes pure orchestration; persistence semantics are
//!      discoverable in one file.
//!   2. If the remapping ever needs to evolve (e.g. a one-shot migration
//!      to the canonical key), the change is localized here.

use std::sync::Arc;

use bitcoin::io;
use lightning::util::async_poll::AsyncResult;
use lightning::util::persist::{
	KVStore, SCORER_PERSISTENCE_KEY, SCORER_PERSISTENCE_PRIMARY_NAMESPACE,
	SCORER_PERSISTENCE_SECONDARY_NAMESPACE,
};
use lightning_persister::fs_store::FilesystemStore;

/// The on-disk filename used by earlier versions of this node for the
/// `ProbabilisticScorer`. Kept short for backwards compatibility.
pub(crate) const SCORER_PERSISTENCE_FILE_NAME: &str = "scorer";

fn remap_scorer_key<'a>(
	primary_namespace: &str, secondary_namespace: &str, key: &'a str,
) -> &'a str {
	if primary_namespace == SCORER_PERSISTENCE_PRIMARY_NAMESPACE
		&& secondary_namespace == SCORER_PERSISTENCE_SECONDARY_NAMESPACE
		&& key == SCORER_PERSISTENCE_KEY
	{
		SCORER_PERSISTENCE_FILE_NAME
	} else {
		key
	}
}

/// A `KVStore` adapter that rewrites the scorer key on its way to the
/// inner `FilesystemStore`, leaving every other key unchanged.
pub(crate) struct ScorerKeyRemappingStore {
	inner: Arc<FilesystemStore>,
}

impl ScorerKeyRemappingStore {
	pub(crate) fn new(inner: Arc<FilesystemStore>) -> Self {
		Self { inner }
	}
}

impl KVStore for ScorerKeyRemappingStore {
	fn read(
		&self, primary_namespace: &str, secondary_namespace: &str, key: &str,
	) -> AsyncResult<'static, Vec<u8>, io::Error> {
		let key = remap_scorer_key(primary_namespace, secondary_namespace, key);
		self.inner.read(primary_namespace, secondary_namespace, key)
	}

	fn write(
		&self, primary_namespace: &str, secondary_namespace: &str, key: &str, buf: Vec<u8>,
	) -> AsyncResult<'static, (), io::Error> {
		let key = remap_scorer_key(primary_namespace, secondary_namespace, key);
		self.inner.write(primary_namespace, secondary_namespace, key, buf)
	}

	fn remove(
		&self, primary_namespace: &str, secondary_namespace: &str, key: &str, lazy: bool,
	) -> AsyncResult<'static, (), io::Error> {
		let key = remap_scorer_key(primary_namespace, secondary_namespace, key);
		self.inner.remove(primary_namespace, secondary_namespace, key, lazy)
	}

	fn list(
		&self, primary_namespace: &str, secondary_namespace: &str,
	) -> AsyncResult<'static, Vec<String>, io::Error> {
		self.inner.list(primary_namespace, secondary_namespace)
	}
}
