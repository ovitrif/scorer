pub mod config;
pub mod pubkey;
pub mod query;

mod bootstrapper;

pub use bootstrapper::DnsBootstrapper;
pub use config::DnsBootstrapConfig;

use thiserror::Error;

/// Errors that can occur during DNS bootstrap operations.
#[derive(Debug, Error)]
pub enum DnsBootstrapError {
	/// SRV query failed on all seeds.
	#[error("SRV query failed: {0}")]
	SrvQueryFailed(String),

	/// A/AAAA resolution failed for a node hostname.
	#[error("host resolution failed for {hostname}: {reason}")]
	HostResolutionFailed { hostname: String, reason: String },

	/// Bech32 decoding of node ID from hostname failed.
	#[error("invalid bech32 node ID in {hostname}: {reason}")]
	InvalidBech32NodeId { hostname: String, reason: String },

	/// Decoded bytes are not a valid secp256k1 public key.
	#[error("invalid public key: {reason}")]
	InvalidPublicKey { reason: String },

	/// DNS operation timed out.
	#[error("DNS operation timed out")]
	Timeout,

	/// Internal DNS resolver error.
	#[error("resolver error: {0}")]
	ResolverError(String),
}
