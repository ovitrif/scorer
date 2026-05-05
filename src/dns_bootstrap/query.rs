use super::DnsBootstrapError;
use hickory_resolver::TokioAsyncResolver;
use std::net::IpAddr;
use std::time::Duration;

/// A single SRV record returned from a DNS seed query.
///
/// Note: SRV `priority` and `weight` fields exist in the DNS protocol but
/// are intentionally omitted here because BOLT-0010 sampling treats every
/// record as equal-weight (we reservoir-sample peers elsewhere). If you
/// introduce priority-aware selection, reintroduce the fields rather than
/// reserving underscore-prefixed placeholders.
#[derive(Debug, Clone)]
pub struct SrvRecord {
	/// Virtual hostname (e.g. `ln1q....<seed>.`).
	pub target: String,
	/// Port the node is listening on.
	pub port: u16,
}

/// Executes DNS SRV queries and A/AAAA lookups for BOLT-0010 bootstrap.
pub struct SrvQueryExecutor {
	resolver: TokioAsyncResolver,
	timeout: Duration,
}

impl SrvQueryExecutor {
	/// Create a new executor using the system DNS resolver.
	pub fn new(timeout: Duration) -> Result<Self, DnsBootstrapError> {
		let resolver = TokioAsyncResolver::tokio_from_system_conf().map_err(|e| {
			DnsBootstrapError::ResolverError(format!("system resolver init: {}", e))
		})?;
		Ok(Self { resolver, timeout })
	}

	/// Query SRV records for `_nodes._tcp.<seed>.`
	///
	/// This is the primary BOLT-0010 query that returns virtual hostnames
	/// encoding node public keys and their listening ports.
	pub async fn query_srv(&self, seed: &str) -> Result<Vec<SrvRecord>, DnsBootstrapError> {
		let query_name = format!("_nodes._tcp.{}.", seed);

		let lookup = tokio::time::timeout(self.timeout, self.resolver.srv_lookup(&query_name))
			.await
			.map_err(|_| DnsBootstrapError::Timeout)?
			.map_err(|e| DnsBootstrapError::SrvQueryFailed(format!("{}: {}", query_name, e)))?;

		let records: Vec<SrvRecord> = lookup
			.iter()
			.map(|srv| SrvRecord { target: srv.target().to_utf8(), port: srv.port() })
			.collect();

		Ok(records)
	}

	/// Resolve a hostname to its first IP address (A or AAAA).
	///
	/// Matches LND behavior: only the first returned IP is used.
	pub async fn resolve_host(&self, hostname: &str) -> Result<IpAddr, DnsBootstrapError> {
		let lookup = tokio::time::timeout(self.timeout, self.resolver.lookup_ip(hostname))
			.await
			.map_err(|_| DnsBootstrapError::Timeout)?
			.map_err(|e| DnsBootstrapError::HostResolutionFailed {
				hostname: hostname.to_string(),
				reason: e.to_string(),
			})?;

		lookup.iter().next().ok_or_else(|| DnsBootstrapError::HostResolutionFailed {
			hostname: hostname.to_string(),
			reason: "no A/AAAA records returned".to_string(),
		})
	}
}
