use std::io::{BufReader, Seek};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs, io};

use bitcoin::io as bitcoin_io;
use lightning::sign::{EntropySource, KeysManager, SpendableOutputDescriptor};
use lightning::util::logger::Logger;
use lightning::util::persist::KVStore;
use lightning::util::ser::{Readable, WithoutLength, Writeable};

use lightning_persister::fs_store::FilesystemStore;

use thiserror::Error;

use crate::disk::FilesystemLogger;
use crate::hex_utils;
use crate::OutputSweeper;

const DEPRECATED_PENDING_SPENDABLE_OUTPUT_DIR: &str = "pending_spendable_outputs";
/// Sentinel file that marks the migration as complete. Prevents re-running
/// (and re-failing) on every startup.
const MIGRATION_SENTINEL_FNAME: &str = ".spendable_outputs_migrated";

#[derive(Debug, Error)]
pub(crate) enum SweepMigrationError {
	#[error("I/O error while {ctx}: {source}")]
	Io {
		ctx: &'static str,
		#[source]
		source: io::Error,
	},
	/// The KVStore persist path uses bitcoin's re-exported io::Error type
	/// (distinct from `std::io::Error`), so we keep its own variant rather
	/// than lossily flattening both into one.
	#[error("failed to persist migrated outputs: {0}")]
	Persist(bitcoin_io::Error),
	#[error("failed to track migrated outputs with sweeper")]
	TrackSpendable,
}

/// Public API: returns the number of spendable outputs migrated.
///
/// Failures are logged and returned rather than panicking, so a corrupt
/// on-disk state no longer crashes the node. The migration is idempotent:
/// once it succeeds, a sentinel file prevents re-execution.
///
/// This function is `pub(crate)` and expected to be called in a spawned
/// task at startup. To preserve the existing `tokio::spawn(...)` call-site
/// contract that expects a future of `()`, we provide a thin wrapper
/// [`migrate_deprecated_spendable_outputs`] that logs the result.
pub(crate) async fn try_migrate_deprecated_spendable_outputs(
	ldk_data_dir: String, keys_manager: Arc<KeysManager>, logger: Arc<FilesystemLogger>,
	persister: Arc<FilesystemStore>, sweeper: Arc<OutputSweeper>,
) -> Result<usize, SweepMigrationError> {
	let sentinel_path = format!("{}/{}", ldk_data_dir, MIGRATION_SENTINEL_FNAME);
	if Path::new(&sentinel_path).exists() {
		lightning::log_debug!(&*logger, "Spendable-outputs migration already completed; skipping");
		return Ok(0);
	}

	lightning::log_info!(&*logger, "Beginning migration of deprecated spendable outputs");
	let pending_spendables_dir =
		format!("{}/{}", ldk_data_dir, DEPRECATED_PENDING_SPENDABLE_OUTPUT_DIR);
	let processing_spendables_dir = format!("{}/processing_spendable_outputs", ldk_data_dir);
	let spendables_dir = format!("{}/spendable_outputs", ldk_data_dir);

	if !Path::new(&pending_spendables_dir).exists()
		&& !Path::new(&processing_spendables_dir).exists()
		&& !Path::new(&spendables_dir).exists()
	{
		lightning::log_info!(&*logger, "No deprecated spendable outputs to migrate");
		write_sentinel(&sentinel_path, &logger);
		return Ok(0);
	}

	move_pending_to_processing(&pending_spendables_dir, &processing_spendables_dir, &logger)?;
	concatenate_processing_outputs(
		&processing_spendables_dir,
		&keys_manager,
		&persister,
		&logger,
	)
	.await?;

	let best_block = sweeper.current_best_block();
	let outputs = read_all_spendable_outputs(&spendables_dir, &logger);
	let migrated_count = outputs.len();

	if migrated_count > 0 {
		let spend_delay = Some(best_block.height + 2);
		sweeper
			.track_spendable_outputs(outputs, None, false, spend_delay)
			.await
			.map_err(|_| SweepMigrationError::TrackSpendable)?;
	}

	// Best-effort cleanup — surviving files are harmless now that the
	// sentinel guards re-entry.
	best_effort_remove_dir(&spendables_dir, &logger);
	best_effort_remove_dir(&pending_spendables_dir, &logger);

	write_sentinel(&sentinel_path, &logger);

	lightning::log_info!(
		&*logger,
		"Successfully migrated {} deprecated spendable outputs",
		migrated_count
	);
	Ok(migrated_count)
}

/// Call-site-compatible wrapper: spawn-friendly, returns `()`, logs on
/// failure. Replaces the previous signature that panicked on error.
pub(crate) async fn migrate_deprecated_spendable_outputs(
	ldk_data_dir: String, keys_manager: Arc<KeysManager>, logger: Arc<FilesystemLogger>,
	persister: Arc<FilesystemStore>, sweeper: Arc<OutputSweeper>,
) {
	if let Err(e) = try_migrate_deprecated_spendable_outputs(
		ldk_data_dir,
		keys_manager,
		Arc::clone(&logger),
		persister,
		sweeper,
	)
	.await
	{
		lightning::log_error!(
			&*logger,
			"Deprecated spendable outputs migration failed: {}. On-disk state is preserved; node continues.",
			e
		);
	}
}

fn write_sentinel(sentinel_path: &str, logger: &Arc<FilesystemLogger>) {
	if let Err(e) = fs::write(sentinel_path, b"ok") {
		lightning::log_warn!(
			&**logger,
			"Failed to write migration sentinel {}: {}. Migration will re-run on next startup.",
			sentinel_path,
			e
		);
	}
}

/// Move 32-byte-hex-named files out of the pending directory so that
/// newly-arrived spendables cannot race with the migration. Any rename
/// failure is logged and skipped for that entry.
fn move_pending_to_processing(
	pending_dir: &str, processing_dir: &str, logger: &Arc<FilesystemLogger>,
) -> Result<(), SweepMigrationError> {
	let dir_iter = match fs::read_dir(pending_dir) {
		Ok(iter) => iter,
		Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
		Err(e) => return Err(SweepMigrationError::Io { ctx: "reading pending dir", source: e }),
	};

	for file_res in dir_iter {
		let file = match file_res {
			Ok(f) => f,
			Err(e) => {
				lightning::log_warn!(
					&**logger,
					"Skipping unreadable pending spendable entry: {}",
					e
				);
				continue;
			},
		};
		// Only move files whose names look like 32-byte-hex (= 64 chars),
		// otherwise it's likely a temp file LDK is still writing.
		if file.file_name().len() != 64 {
			continue;
		}
		if let Err(e) = fs::create_dir_all(processing_dir) {
			return Err(SweepMigrationError::Io {
				ctx: "creating processing dir",
				source: e,
			});
		}
		let mut holding_path = PathBuf::new();
		holding_path.push(processing_dir);
		holding_path.push(file.file_name());
		if let Err(e) = fs::rename(file.path(), &holding_path) {
			lightning::log_warn!(
				&**logger,
				"Skipping spendable entry {:?} (rename failed): {}",
				file.file_name(),
				e
			);
			continue;
		}
	}
	Ok(())
}

/// Concatenate all files in the processing directory and persist them as
/// a single blob under `spendable_outputs/<random>`. Drops the processing
/// directory on success.
async fn concatenate_processing_outputs(
	processing_dir: &str, keys_manager: &KeysManager, persister: &FilesystemStore,
	logger: &Arc<FilesystemLogger>,
) -> Result<(), SweepMigrationError> {
	let dir_iter = match fs::read_dir(processing_dir) {
		Ok(iter) => iter,
		Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
		Err(e) => return Err(SweepMigrationError::Io { ctx: "reading processing dir", source: e }),
	};

	let mut outputs = Vec::new();
	for file_res in dir_iter {
		let path = match file_res {
			Ok(f) => f.path(),
			Err(e) => {
				lightning::log_warn!(
					&**logger,
					"Skipping unreadable processing spendable entry: {}",
					e
				);
				continue;
			},
		};
		match fs::read(&path) {
			Ok(mut bytes) => outputs.append(&mut bytes),
			Err(e) => {
				lightning::log_warn!(
					&**logger,
					"Skipping unreadable processing entry {}: {}",
					path.display(),
					e
				);
			},
		}
	}

	if outputs.is_empty() {
		return Ok(());
	}

	let key = hex_utils::hex_str(&keys_manager.get_secure_random_bytes());
	persister
		.write("spendable_outputs", "", &key, WithoutLength(&outputs).encode())
		.await
		.map_err(SweepMigrationError::Persist)?;
	best_effort_remove_dir(processing_dir, logger);
	Ok(())
}

/// Read all spendable descriptors from every file in `spendables_dir`.
/// Corrupt or unreadable files are logged and skipped — a single bad
/// descriptor no longer halts the node.
fn read_all_spendable_outputs(
	spendables_dir: &str, logger: &Arc<FilesystemLogger>,
) -> Vec<SpendableOutputDescriptor> {
	let mut outputs: Vec<SpendableOutputDescriptor> = Vec::new();
	let dir_iter = match fs::read_dir(spendables_dir) {
		Ok(iter) => iter,
		Err(e) if e.kind() == io::ErrorKind::NotFound => return outputs,
		Err(e) => {
			lightning::log_warn!(
				&**logger,
				"Skipping spendable_outputs directory (read failed): {}",
				e
			);
			return outputs;
		},
	};

	for file_res in dir_iter {
		let path = match file_res {
			Ok(f) => f.path(),
			Err(e) => {
				lightning::log_warn!(&**logger, "Skipping unreadable spendable entry: {}", e);
				continue;
			},
		};
		read_outputs_from_file(&path, &mut outputs, logger);
	}
	outputs
}

fn read_outputs_from_file(
	path: &Path, outputs: &mut Vec<SpendableOutputDescriptor>, logger: &Arc<FilesystemLogger>,
) {
	let file = match fs::File::open(path) {
		Ok(f) => f,
		Err(e) => {
			lightning::log_warn!(
				&**logger,
				"Skipping spendable file {}: open failed: {}",
				path.display(),
				e
			);
			return;
		},
	};
	let metadata_len = file.metadata().map(|m| m.len()).unwrap_or(0);
	let mut reader = BufReader::new(file);

	loop {
		let pos = match reader.stream_position() {
			Ok(p) => p,
			Err(e) => {
				lightning::log_warn!(
					&**logger,
					"Aborting spendable file {}: stream_position failed: {}",
					path.display(),
					e
				);
				return;
			},
		};
		if pos >= metadata_len {
			return;
		}
		match Readable::read(&mut reader) {
			Ok(descriptor) => outputs.push(descriptor),
			Err(e) => {
				lightning::log_warn!(
					&**logger,
					"Aborting spendable file {}: decode failed at offset {}: {:?}. {} descriptors recovered so far.",
					path.display(),
					pos,
					e,
					outputs.len()
				);
				return;
			},
		}
	}
}

fn best_effort_remove_dir(dir: &str, logger: &Arc<FilesystemLogger>) {
	match fs::remove_dir_all(dir) {
		Ok(()) => {},
		Err(e) if e.kind() == io::ErrorKind::NotFound => {},
		Err(e) => {
			lightning::log_warn!(&**logger, "Failed to remove directory {}: {}", dir, e);
		},
	}
}
