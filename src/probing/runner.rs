use super::sender::{prepare_probe, send_probe, ProbeError};
use super::tracker::{ProbeOutcome, ProbeTracker};
use super::ProbingScorerLock;
use crate::disk::FilesystemLogger;
use crate::hex_utils;
use crate::runtime_config::ProbingConfig;
use crate::{ChannelManager, NetworkGraph};
use lightning::routing::gossip::NodeId;
use lightning::routing::scoring::ProbabilisticScoringFeeParameters;
use lightning::types::payment::PaymentHash;
use lightning::util::logger::Logger;
use rand::{thread_rng, Rng};
use std::convert::TryInto;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone)]
pub(crate) struct ProbingDeps {
	pub(crate) channel_manager: Arc<ChannelManager>,
	pub(crate) network_graph: Arc<NetworkGraph>,
	pub(crate) logger: Arc<FilesystemLogger>,
	pub(crate) scorer: Arc<ProbingScorerLock>,
	pub(crate) scoring_fee_params: ProbabilisticScoringFeeParameters,
	pub(crate) tracker: Arc<Mutex<ProbeTracker>>,
}

#[derive(Copy, Clone)]
struct ProbingModes {
	peer_list: bool,
	random_graph: bool,
}

enum ProbeCompletion {
	Success(PaymentHash),
	Failed(PaymentHash),
	Dropped(PaymentHash),
	Timeout(PaymentHash),
	SendError(ProbeError),
}

impl ProbeCompletion {
	fn should_break_peer_amounts(&self) -> bool {
		!matches!(self, Self::Success(_))
	}
}

pub(crate) fn spawn_probing_loop(probe_config: ProbingConfig, deps: ProbingDeps) {
	let modes = compute_modes(&probe_config);
	log_mode_warnings(&probe_config, modes, &deps.logger);

	if !modes.peer_list && !modes.random_graph {
		lightning::log_warn!(
			&*deps.logger,
			"Probing config does not enable any probing mode. Probing disabled."
		);
		return;
	}

	let mut sorted_amounts = probe_config.amount_msats.clone();
	sorted_amounts.sort();

	let probe_timeout = Duration::from_secs(probe_config.timeout_sec);
	let probe_delay = Duration::from_secs(probe_config.probe_delay_sec);
	let peer_delay = Duration::from_secs(probe_config.peer_delay_sec);

	log_startup(&deps.logger, &probe_config, modes, &sorted_amounts);

	tokio::spawn(async move {
		let ProbingDeps {
			channel_manager,
			network_graph,
			logger,
			scorer,
			scoring_fee_params,
			tracker,
		} = deps;

		let mut interval = tokio::time::interval(Duration::from_secs(probe_config.interval_sec));
		let our_node_id = NodeId::from_pubkey(&channel_manager.get_our_node_id());

		loop {
			interval.tick().await;

			if modes.peer_list {
				let peer_count = probe_config.peers.len();
				for (peer_idx, peer) in probe_config.peers.iter().enumerate() {
					let Some(peer_pubkey) = extract_peer_pubkey(peer) else {
						lightning::log_warn!(
							&*logger,
							"Probe skipped: invalid peer entry '{}' (expected pubkey@host:port or pubkey)",
							peer
						);
						continue;
					};

					for &amount in &sorted_amounts {
						let completion = run_probe_attempt(
							&tracker,
							prepare_probe(
								&channel_manager,
								&network_graph,
								&logger,
								&scorer,
								&scoring_fee_params,
								peer_pubkey,
								amount,
							),
							probe_timeout,
							&logger,
						)
						.await;

						log_probe_completed(&logger, peer_pubkey, amount, &completion);
						sleep_probe_delay(probe_delay).await;

						if completion.should_break_peer_amounts() {
							break;
						}
					}

					if peer_idx < peer_count - 1 {
						tokio::time::sleep(peer_delay).await;
					}
				}
			}

			if modes.random_graph {
				let requested_count =
					probe_config.random_nodes_per_interval.try_into().unwrap_or(usize::MAX);
				let random_recipients = {
					let graph_read_only = network_graph.read_only();
					let mut rng = thread_rng();
					reservoir_sample(
						graph_read_only.nodes().unordered_iter().filter_map(|(node_id, _)| {
							if *node_id == our_node_id {
								return None;
							}
							node_id.as_pubkey().ok()
						}),
						requested_count,
						&mut rng,
					)
				};

				if random_recipients.is_empty() {
					lightning::log_warn!(
						&*logger,
						"Random probe skipped: no graph nodes available"
					);
					continue;
				}

				for recipient in random_recipients {
					let destination_node = hex_utils::hex_str(&recipient.serialize());
					let amount = probe_config.random_min_amount_msat;
					let completion = run_probe_attempt(
						&tracker,
						send_probe(
							&channel_manager,
							recipient,
							&network_graph,
							&logger,
							amount,
							&scorer,
							&scoring_fee_params,
						),
						probe_timeout,
						&logger,
					)
					.await;

					log_probe_completed(&logger, &destination_node, amount, &completion);
					sleep_probe_delay(probe_delay).await;
				}
			}
		}
	});
}

fn compute_modes(probe_config: &ProbingConfig) -> ProbingModes {
	let has_peer_targets = !probe_config.peers.is_empty();
	let has_peer_amounts = !probe_config.amount_msats.is_empty();
	let random_amount_enabled = probe_config.random_min_amount_msat > 0;
	let random_count_enabled = probe_config.random_nodes_per_interval > 0;

	ProbingModes {
		peer_list: has_peer_targets && has_peer_amounts,
		random_graph: random_amount_enabled && random_count_enabled,
	}
}

fn log_mode_warnings(
	probe_config: &ProbingConfig, modes: ProbingModes, logger: &FilesystemLogger,
) {
	let has_peer_targets = !probe_config.peers.is_empty();
	let has_peer_amounts = !probe_config.amount_msats.is_empty();

	if !modes.peer_list {
		if !has_peer_targets && has_peer_amounts {
			lightning::log_warn!(
				logger,
				"probing.peers is empty in config.toml. Peer-list probing disabled."
			);
		} else if has_peer_targets && !has_peer_amounts {
			lightning::log_warn!(
				logger,
				"probing.amount_msats is empty in config.toml. Peer-list probing disabled."
			);
		}
	}

	if !modes.random_graph
		&& probe_config.random_min_amount_msat > 0
		&& probe_config.random_nodes_per_interval == 0
	{
		lightning::log_warn!(
			logger,
			"probing.random_nodes_per_interval is 0 in config.toml. Random-graph probing disabled."
		);
	}
}

fn log_startup(
	logger: &FilesystemLogger, probe_config: &ProbingConfig, modes: ProbingModes,
	sorted_amounts: &[u64],
) {
	lightning::log_info!(
		logger,
		"Probing started: peer_list_mode={} ({} peers, {} amounts {:?} msat), random_graph_mode={} ({} nodes/interval at {} msat), interval={}s, timeout={}s, probe_delay={}s, peer_delay={}s",
		modes.peer_list,
		probe_config.peers.len(),
		sorted_amounts.len(),
		sorted_amounts,
		modes.random_graph,
		probe_config.random_nodes_per_interval,
		probe_config.random_min_amount_msat,
		probe_config.interval_sec,
		probe_config.timeout_sec,
		probe_config.probe_delay_sec,
		probe_config.peer_delay_sec
	);
}

fn log_probe_completed(
	logger: &FilesystemLogger, destination_node: &str, amount_msat: u64,
	completion: &ProbeCompletion,
) {
	match completion {
		ProbeCompletion::Success(hash) => {
			lightning::log_info!(
				logger,
				"probe_completed destination_node={} amount_msat={} state=SUCCESS payment_hash={}",
				destination_node,
				amount_msat,
				hash
			);
		},
		ProbeCompletion::Failed(hash) => {
			lightning::log_warn!(
				logger,
				"probe_completed destination_node={} amount_msat={} state=FAILED payment_hash={}",
				destination_node,
				amount_msat,
				hash
			);
		},
		ProbeCompletion::Dropped(hash) => {
			lightning::log_warn!(
				logger,
				"probe_completed destination_node={} amount_msat={} state=DROPPED payment_hash={}",
				destination_node,
				amount_msat,
				hash
			);
		},
		ProbeCompletion::Timeout(hash) => {
			lightning::log_warn!(
				logger,
				"probe_completed destination_node={} amount_msat={} state=TIMEOUT payment_hash={}",
				destination_node,
				amount_msat,
				hash
			);
		},
		ProbeCompletion::SendError(ProbeError::NoRoute(err)) => {
			lightning::log_warn!(
				logger,
				"probe_completed destination_node={} amount_msat={} state=NO_ROUTE error={}",
				destination_node,
				amount_msat,
				err
			);
		},
		ProbeCompletion::SendError(ProbeError::SendFailed(err)) => {
			lightning::log_warn!(
				logger,
				"probe_completed destination_node={} amount_msat={} state=SEND_FAILED error={:?}",
				destination_node,
				amount_msat,
				err
			);
		},
		ProbeCompletion::SendError(ProbeError::InvalidInput(err)) => {
			lightning::log_warn!(
				logger,
				"probe_completed destination_node={} amount_msat={} state=INVALID_INPUT error={}",
				destination_node,
				amount_msat,
				err
			);
		},
	}
}

async fn run_probe_attempt(
	tracker: &Arc<Mutex<ProbeTracker>>, send_result: Result<PaymentHash, ProbeError>,
	timeout: Duration, logger: &FilesystemLogger,
) -> ProbeCompletion {
	let payment_hash = match send_result {
		Ok(payment_hash) => payment_hash,
		Err(err) => return ProbeCompletion::SendError(err),
	};

	// A poisoned tracker mutex means a different thread panicked while
	// holding it. We log and report the probe as Dropped rather than
	// propagating the panic — losing a probe result is strictly better
	// than taking down the probing task.
	let receiver = match tracker.lock() {
		Ok(mut guard) => guard.register_probe(payment_hash),
		Err(_) => {
			lightning::log_error!(
				logger,
				"Probe tracker mutex poisoned while registering probe {}; treating as dropped",
				payment_hash
			);
			return ProbeCompletion::Dropped(payment_hash);
		},
	};
	match tokio::time::timeout(timeout, receiver).await {
		Ok(Ok(ProbeOutcome::Success)) => ProbeCompletion::Success(payment_hash),
		Ok(Ok(ProbeOutcome::Failed)) => ProbeCompletion::Failed(payment_hash),
		Ok(Err(_)) => ProbeCompletion::Dropped(payment_hash),
		Err(_) => {
			match tracker.lock() {
				Ok(mut guard) => guard.mark_timed_out(&payment_hash),
				Err(_) => {
					lightning::log_error!(
						logger,
						"Probe tracker mutex poisoned while marking probe {} timed-out",
						payment_hash
					);
				},
			}
			ProbeCompletion::Timeout(payment_hash)
		},
	}
}

async fn sleep_probe_delay(probe_delay: Duration) {
	tokio::time::sleep(probe_delay).await;
}

fn extract_peer_pubkey(peer: &str) -> Option<&str> {
	let pubkey = peer.split('@').next().unwrap_or("").trim();
	if pubkey.is_empty() {
		None
	} else {
		Some(pubkey)
	}
}

fn reservoir_sample<T, I, R>(candidates: I, sample_size: usize, rng: &mut R) -> Vec<T>
where
	I: IntoIterator<Item = T>,
	R: Rng,
{
	if sample_size == 0 {
		return Vec::new();
	}

	let mut reservoir = Vec::with_capacity(sample_size);
	let mut seen = 0usize;

	for candidate in candidates {
		seen += 1;
		if reservoir.len() < sample_size {
			reservoir.push(candidate);
			continue;
		}

		let replace_idx = rng.gen_range(0, seen);
		if replace_idx < sample_size {
			reservoir[replace_idx] = candidate;
		}
	}

	reservoir
}

#[cfg(test)]
mod tests {
	use rand::thread_rng;
	use std::collections::HashSet;

	#[test]
	fn extract_peer_pubkey_from_pubkey_and_addr() {
		assert_eq!(super::extract_peer_pubkey("02abc123@1.2.3.4:9735"), Some("02abc123"));
	}

	#[test]
	fn extract_peer_pubkey_from_raw_pubkey() {
		assert_eq!(super::extract_peer_pubkey("02abc123"), Some("02abc123"));
	}

	#[test]
	fn extract_peer_pubkey_rejects_empty_pubkey() {
		assert_eq!(super::extract_peer_pubkey("@1.2.3.4:9735"), None);
		assert_eq!(super::extract_peer_pubkey("   "), None);
	}

	#[test]
	fn reservoir_sample_is_bounded_and_unique() {
		let mut rng = thread_rng();
		let sample = super::reservoir_sample(0..100usize, 8, &mut rng);

		assert_eq!(sample.len(), 8);
		assert!(sample.iter().all(|value| *value < 100));
		let unique = sample.iter().copied().collect::<HashSet<_>>();
		assert_eq!(unique.len(), sample.len());
	}

	#[test]
	fn reservoir_sample_returns_all_when_k_exceeds_population() {
		let mut rng = thread_rng();
		let sample = super::reservoir_sample(vec![1usize, 2, 3], 10, &mut rng);

		assert_eq!(sample.len(), 3);
		let values = sample.into_iter().collect::<HashSet<_>>();
		assert_eq!(values, HashSet::from([1usize, 2, 3]));
	}

	#[test]
	fn reservoir_sample_returns_empty_when_no_candidates() {
		let mut rng = thread_rng();
		let sample = super::reservoir_sample(Vec::<usize>::new(), 5, &mut rng);

		assert!(sample.is_empty());
	}
}
