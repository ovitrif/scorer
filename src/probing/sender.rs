use super::ProbingScorerLock;
use crate::disk::FilesystemLogger;
use crate::hex_utils;
use crate::{ChannelManager, NetworkGraph};
use bitcoin::secp256k1::PublicKey;
use lightning::ln::channelmanager;
use lightning::routing::router::{
	PaymentParameters, RouteParameters, ScorerAccountingForInFlightHtlcs,
};
use lightning::routing::scoring::ProbabilisticScoringFeeParameters;
use lightning::types::payment::PaymentHash;
use thiserror::Error;

#[derive(Debug, Error)]
pub(super) enum ProbeError {
	#[error("{0}")]
	NoRoute(&'static str),
	// `ProbeSendFailure` has no `Display` impl upstream, so format via Debug.
	#[error("probe send failed: {0:?}")]
	SendFailed(channelmanager::ProbeSendFailure),
	#[error("{0}")]
	InvalidInput(&'static str),
}

pub(super) fn prepare_probe(
	channel_manager: &ChannelManager, graph: &NetworkGraph, logger: &FilesystemLogger,
	scorer: &ProbingScorerLock, scoring_fee_params: &ProbabilisticScoringFeeParameters,
	pub_key_hex: &str, probe_amount: u64,
) -> Result<PaymentHash, ProbeError> {
	if probe_amount == 0 {
		return Err(ProbeError::InvalidInput("probe amount must be greater than 0"));
	}

	let pub_key_bytes = hex_utils::to_vec(pub_key_hex)
		.ok_or(ProbeError::InvalidInput("invalid peer pubkey hex"))?;
	let pubkey = PublicKey::from_slice(&pub_key_bytes)
		.map_err(|_| ProbeError::InvalidInput("invalid peer pubkey"))?;

	send_probe(channel_manager, pubkey, graph, logger, probe_amount, scorer, scoring_fee_params)
}

pub(super) fn send_probe(
	channel_manager: &ChannelManager, recipient: PublicKey, graph: &NetworkGraph,
	logger: &FilesystemLogger, amount_msat: u64, scorer: &ProbingScorerLock,
	scoring_fee_params: &ProbabilisticScoringFeeParameters,
) -> Result<PaymentHash, ProbeError> {
	let usable_channels = channel_manager.list_usable_channels();
	let channel_refs = usable_channels.iter().collect::<Vec<_>>();

	let mut payment_params = PaymentParameters::from_node_id(recipient, 144);
	payment_params.max_path_count = 1;

	let in_flight_htlcs = channel_manager.compute_inflight_htlcs();
	let scorer = scorer.read().unwrap();
	let inflight_scorer = ScorerAccountingForInFlightHtlcs::new(&scorer, &in_flight_htlcs);

	let route = lightning::routing::router::find_route(
		&channel_manager.get_our_node_id(),
		&RouteParameters::from_payment_params_and_value(payment_params, amount_msat),
		graph,
		Some(&channel_refs),
		logger,
		&inflight_scorer,
		scoring_fee_params,
		&[32; 32],
	)
	.map_err(ProbeError::NoRoute)?;

	let path = route.paths.into_iter().next().ok_or(ProbeError::NoRoute("route has no paths"))?;
	let (payment_hash, _) = channel_manager.send_probe(path).map_err(ProbeError::SendFailed)?;
	Ok(payment_hash)
}
