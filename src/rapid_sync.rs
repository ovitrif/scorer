use crate::{FilesystemLogger, NetworkGraph};
use lightning::util::logger::Logger;
use lightning_rapid_gossip_sync::{GraphSyncError, RapidGossipSync};
use reqwest::Client;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::time::sleep;

const MAX_RETRIES: u32 = 3;
const INITIAL_BACKOFF_MS: u64 = 1000; // 1 second
const MAX_BACKOFF_MS: u64 = 60000; // 1 minute
const DOWNLOAD_TIMEOUT_SECS: u64 = 300; // 5 minutes
const DEFAULT_RAPID_SYNC_URL: &str = "https://rapidsync.lightningdevkit.org/snapshot/";

pub(crate) struct RapidGossipSyncManager {
	rapid_sync: Arc<RapidGossipSync<Arc<NetworkGraph>, Arc<FilesystemLogger>>>,
	sync_url: String,
	logger: Arc<FilesystemLogger>,
	http_client: Client,
	ldk_data_dir: String,
}

#[derive(Debug, Error)]
pub(crate) enum RapidSyncError {
	#[error("download failed: {0}")]
	DownloadFailed(String),
	// GraphSyncError implements neither `Display` nor `std::error::Error`
	// upstream, so we cannot use `#[from]` (which requires `Error`) and
	// we render via Debug. Manual conversion via `From` below.
	#[error("parse failed: {0:?}")]
	ParseFailed(GraphSyncError),
	#[error("I/O error: {0}")]
	IoError(#[from] io::Error),
	#[error("retry attempts exhausted")]
	RetryExhausted,
}

impl From<GraphSyncError> for RapidSyncError {
	fn from(e: GraphSyncError) -> Self {
		RapidSyncError::ParseFailed(e)
	}
}

impl RapidGossipSyncManager {
	// The `fs_store` parameter is deliberately omitted: the rapid-sync
	// state is persisted via a small flat file (`rapid_sync_state`) rather
	// than the KVStore, so the `FilesystemStore` was never used. Dropping
	// the dead parameter cleans up the call site in `main.rs`.
	pub async fn new(
		network_graph: Arc<NetworkGraph>, sync_url: Option<String>, logger: Arc<FilesystemLogger>,
		ldk_data_dir: String,
	) -> Result<Self, RapidSyncError> {
		let rapid_sync = Arc::new(RapidGossipSync::new(network_graph, logger.clone()));

		let http_client =
			Client::builder().timeout(Duration::from_secs(DOWNLOAD_TIMEOUT_SECS)).build().map_err(
				|e| RapidSyncError::DownloadFailed(format!("Failed to create HTTP client: {}", e)),
			)?;

		let sync_url = sync_url.unwrap_or_else(|| DEFAULT_RAPID_SYNC_URL.to_string());

		let mut manager = Self { rapid_sync, sync_url, logger, http_client, ldk_data_dir };

		// Perform initial sync
		match manager.sync_network_graph().await {
			Ok(timestamp) => {
				lightning::log_info!(
					&*manager.logger,
					"Initial rapid gossip sync completed successfully. Timestamp: {}",
					timestamp
				);
				Ok(manager)
			},
			Err(e) => {
				lightning::log_error!(
					&*manager.logger,
					"Initial rapid gossip sync failed: {:?}",
					e
				);
				Err(e)
			},
		}
	}

	pub async fn sync_network_graph(&mut self) -> Result<u32, RapidSyncError> {
		let last_sync = self.load_last_sync_timestamp().await.unwrap_or(0);

		lightning::log_info!(
			&*self.logger,
			"Starting rapid gossip sync from timestamp: {}",
			last_sync
		);

		let snapshot_data = self.download_with_retry(last_sync).await?;

		lightning::log_info!(
			&*self.logger,
			"Downloaded snapshot of {} bytes, applying to network graph...",
			snapshot_data.len()
		);

		// Apply the snapshot to the network graph
		let new_timestamp = self.rapid_sync.update_network_graph(&snapshot_data)?;

		// Save the new timestamp
		self.save_last_sync_timestamp(new_timestamp).await?;

		lightning::log_info!(
			&*self.logger,
			"Rapid gossip sync completed. New timestamp: {}",
			new_timestamp
		);

		Ok(new_timestamp)
	}

	async fn download_with_retry(&self, last_sync: u32) -> Result<Vec<u8>, RapidSyncError> {
		let mut retry_count = 0;
		let mut backoff_ms = INITIAL_BACKOFF_MS;

		loop {
			match self.download_snapshot_attempt(last_sync).await {
				Ok(data) => return Ok(data),
				Err(e) => {
					retry_count += 1;
					if retry_count >= MAX_RETRIES {
						lightning::log_error!(
							&*self.logger,
							"Rapid gossip sync download failed after {} attempts",
							MAX_RETRIES
						);
						return Err(RapidSyncError::RetryExhausted);
					}

					lightning::log_warn!(
                        &*self.logger,
                        "Rapid gossip sync download failed (attempt {}/{}): {}. Retrying in {}ms...",
                        retry_count,
                        MAX_RETRIES,
                        e,
                        backoff_ms
                    );

					sleep(Duration::from_millis(backoff_ms)).await;
					backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
				},
			}
		}
	}

	async fn download_snapshot_attempt(&self, last_sync: u32) -> Result<Vec<u8>, RapidSyncError> {
		let url = format!("{}{}", self.sync_url, last_sync);

		lightning::log_info!(&*self.logger, "Downloading rapid gossip snapshot from: {}", url);

		let response = self.http_client.get(&url).send().await.map_err(|e| {
			RapidSyncError::DownloadFailed(format!("HTTP request failed: {}", e))
		})?;

		if !response.status().is_success() {
			return Err(RapidSyncError::DownloadFailed(format!(
				"HTTP {} - {}",
				response.status(),
				response.status().as_str()
			)));
		}

		let content_length = response.content_length().unwrap_or(0);
		let mut downloaded = 0u64;
		let mut data = Vec::new();
		// Report progress at most a few times; the previous code logged
		// on every chunk, which for a multi-MB snapshot produced hundreds
		// of near-identical entries and hid more important log lines.
		let progress_step_bytes: u64 =
			if content_length > 0 { (content_length / 10).max(1) } else { u64::MAX };
		let mut next_progress_threshold = progress_step_bytes;

		// Use the response as a stream to show progress
		use futures_util::StreamExt;
		let mut stream = response.bytes_stream();

		while let Some(chunk) = stream.next().await {
			let chunk = chunk.map_err(|e| {
				RapidSyncError::DownloadFailed(format!("Failed to read chunk: {}", e))
			})?;
			downloaded += chunk.len() as u64;
			data.extend_from_slice(&chunk);

			if content_length > 0 && downloaded >= next_progress_threshold {
				let progress_pct = (downloaded as f64 / content_length as f64 * 100.0) as u32;
				lightning::log_info!(
					&*self.logger,
					"Download progress: {:.1}MB / {:.1}MB ({}%)",
					downloaded as f64 / 1_048_576.0,
					content_length as f64 / 1_048_576.0,
					progress_pct
				);
				next_progress_threshold =
					next_progress_threshold.saturating_add(progress_step_bytes);
			}
		}

		lightning::log_info!(
			&*self.logger,
			"Download complete: {:.1}MB",
			data.len() as f64 / 1_048_576.0
		);

		Ok(data)
	}

	async fn load_last_sync_timestamp(&self) -> Result<u32, io::Error> {
		let timestamp_path = format!("{}/rapid_sync_state", self.ldk_data_dir);
		match tokio::fs::read_to_string(&timestamp_path).await {
			Ok(content) => content
				.trim()
				.parse::<u32>()
				.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e)),
			Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(0),
			Err(e) => Err(e),
		}
	}

	async fn save_last_sync_timestamp(&self, timestamp: u32) -> Result<(), io::Error> {
		let timestamp_path = format!("{}/rapid_sync_state", self.ldk_data_dir);
		let mut file = tokio::fs::File::create(&timestamp_path).await?;
		file.write_all(timestamp.to_string().as_bytes()).await?;
		file.sync_all().await?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use tempfile::TempDir;

	#[tokio::test]
	async fn test_rapid_sync_state_persistence() {
		let temp_dir = TempDir::new().unwrap();
		let ldk_data_dir = temp_dir.path().to_str().unwrap().to_string();

		// Create a mock RapidGossipSyncManager
		let logger = Arc::new(FilesystemLogger::new(ldk_data_dir.clone()));
		let network_graph = Arc::new(NetworkGraph::new(bitcoin::Network::Bitcoin, logger.clone()));

		// Test timestamp persistence
		let manager = RapidGossipSyncManager {
			rapid_sync: Arc::new(RapidGossipSync::new(network_graph, logger.clone())),
			sync_url: "https://test.com/".to_string(),
			logger,
			http_client: Client::new(),
			ldk_data_dir: ldk_data_dir.clone(),
		};

		// Test saving timestamp
		let test_timestamp = 1234567890u32;
		manager.save_last_sync_timestamp(test_timestamp).await.unwrap();

		// Test loading timestamp
		let loaded_timestamp = manager.load_last_sync_timestamp().await.unwrap();
		assert_eq!(loaded_timestamp, test_timestamp);

		// Test loading when file doesn't exist
		tokio::fs::remove_file(format!("{}/rapid_sync_state", ldk_data_dir)).await.unwrap();
		let default_timestamp = manager.load_last_sync_timestamp().await.unwrap();
		assert_eq!(default_timestamp, 0);
	}

	#[test]
	fn test_rapid_sync_error_display() {
		let download_error = RapidSyncError::DownloadFailed("Connection timeout".to_string());
		assert_eq!(format!("{}", download_error), "download failed: Connection timeout");

		let retry_error = RapidSyncError::RetryExhausted;
		assert_eq!(format!("{}", retry_error), "retry attempts exhausted");
	}
}
