use bitcoin::network::Network;
use lightning::ln::msgs::SocketAddress;

/// Probing configuration loaded from config.toml.
#[derive(Clone)]
pub(crate) struct ProbingConfig {
	pub(crate) interval_sec: u64,
	pub(crate) peers: Vec<String>,
	pub(crate) amount_msats: Vec<u64>,
	pub(crate) random_min_amount_msat: u64,
	pub(crate) random_nodes_per_interval: u64,
	pub(crate) timeout_sec: u64,
	pub(crate) probe_delay_sec: u64,
	pub(crate) peer_delay_sec: u64,
}

pub(crate) struct LdkUserInfo {
	pub(crate) bitcoind_rpc_username: String,
	pub(crate) bitcoind_rpc_password: String,
	pub(crate) bitcoind_rpc_port: u16,
	pub(crate) bitcoind_rpc_host: String,
	pub(crate) ldk_storage_dir_path: String,
	pub(crate) ldk_peer_listening_port: u16,
	pub(crate) ldk_announced_listen_addr: Vec<SocketAddress>,
	pub(crate) ldk_announced_node_name: [u8; 32],
	pub(crate) network: Network,
	pub(crate) rapid_gossip_sync_enabled: bool,
	pub(crate) rapid_gossip_sync_url: Option<String>,
	pub(crate) rapid_gossip_sync_interval_hours: u64,
	pub(crate) probing: Option<ProbingConfig>,
	pub(crate) dns_bootstrap: Option<crate::dns_bootstrap::DnsBootstrapConfig>,
}
