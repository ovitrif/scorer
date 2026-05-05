use super::config::DnsBootstrapConfig;
use super::pubkey::decode_pubkey_from_hostname;
use super::query::SrvQueryExecutor;
use super::DnsBootstrapError;
use bitcoin::secp256k1::PublicKey;
use lightning::routing::gossip::NodeId;
use lightning::util::logger::Logger;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;

/// A discovered peer: public key + network address.
#[derive(Debug, Clone)]
pub struct BootstrappedPeer {
	/// The node's secp256k1 public key.
	pub pubkey: PublicKey,
	/// The node's LDK NodeId (derived from pubkey, retained for future use).
	pub _node_id: NodeId,
	/// The node's network address (IP + port from SRV record).
	pub addr: SocketAddr,
}

/// BOLT-0010 DNS seed bootstrapper.
///
/// Queries DNS seeds for SRV records encoding Lightning node public keys,
/// resolves their IP addresses, and returns a list of peers ready for
/// connection.
pub struct DnsBootstrapper {
	config: DnsBootstrapConfig,
	query_executor: SrvQueryExecutor,
}

impl DnsBootstrapper {
	/// Create a new bootstrapper from the given configuration.
	pub fn new(config: DnsBootstrapConfig) -> Result<Self, DnsBootstrapError> {
		let timeout = Duration::from_secs(config.timeout_secs);
		let query_executor = SrvQueryExecutor::new(timeout)?;
		Ok(Self { config, query_executor })
	}

	/// Discover peers from DNS seeds.
	///
	/// Iterates through configured seeds, queries SRV records, decodes node
	/// public keys from bech32-encoded hostnames, resolves IP addresses, and
	/// returns up to `num_addrs` peers not present in `ignore`.
	pub async fn sample_node_addrs(
		&self, num_addrs: usize, ignore: &HashSet<NodeId>, logger: &impl Logger,
	) -> Result<Vec<BootstrappedPeer>, DnsBootstrapError> {
		let mut peers = Vec::new();

		for seed in &self.config.seeds {
			if peers.len() >= num_addrs {
				break;
			}

			// Query SRV records for this seed.
			let srv_records = match self.query_executor.query_srv(seed).await {
				Ok(records) => records,
				Err(e) => {
					lightning::log_warn!(
						logger,
						"[dns_bootstrap] SRV query failed for {}: {}",
						seed,
						e
					);
					continue;
				},
			};

			if srv_records.is_empty() {
				lightning::log_info!(
					logger,
					"[dns_bootstrap] No SRV records returned from {}",
					seed
				);
				continue;
			}

			lightning::log_info!(
				logger,
				"[dns_bootstrap] Got {} SRV records from {}",
				srv_records.len(),
				seed
			);

			// Process each SRV record.
			for srv in srv_records {
				if peers.len() >= num_addrs {
					break;
				}

				// Decode node pubkey from the bech32-encoded hostname label.
				let (pubkey, node_id) = match decode_pubkey_from_hostname(&srv.target) {
					Ok(result) => result,
					Err(e) => {
						lightning::log_info!(
							logger,
							"[dns_bootstrap] Failed to decode pubkey from {}: {}",
							srv.target,
							e
						);
						continue;
					},
				};

				// Skip nodes in the ignore set.
				if ignore.contains(&node_id) {
					continue;
				}

				// Resolve the virtual hostname to an IP address.
				let ip = match self.query_executor.resolve_host(&srv.target).await {
					Ok(ip) => ip,
					Err(e) => {
						lightning::log_info!(
							logger,
							"[dns_bootstrap] Failed to resolve {}: {}",
							srv.target,
							e
						);
						continue;
					},
				};

				let addr = SocketAddr::new(ip, srv.port);
				peers.push(BootstrappedPeer { pubkey, _node_id: node_id, addr });
			}
		}

		if peers.is_empty() {
			return Err(DnsBootstrapError::SrvQueryFailed(
				"no peers discovered from any DNS seed".to_string(),
			));
		}

		Ok(peers)
	}

	/// Return a reference to the configuration.
	pub fn config(&self) -> &DnsBootstrapConfig {
		&self.config
	}

	/// Return the configured bootstrap interval.
	pub fn interval_secs(&self) -> u64 {
		self.config.interval_secs
	}

	/// Return the configured target peer count.
	pub fn num_peers(&self) -> usize {
		self.config.num_peers
	}
}
