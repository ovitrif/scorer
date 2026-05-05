//! `Event::FundingGenerationReady` handling.
//!
//! This is the single most complex event arm: we must construct, fund,
//! sign, and submit the funding transaction via bitcoind, any step of
//! which can fail independently. Splitting it out of the main
//! dispatcher makes the error-branch tree reviewable.

use bitcoin::blockdata::transaction::Transaction;
use bitcoin::consensus::encode;
use bitcoin::network::Network;
use bitcoin_bech32::WitnessProgram;
use bitcoin::secp256k1::PublicKey;
use lightning::ln::types::ChannelId;
use lightning::util::logger::Logger;
use std::collections::HashMap as StdHashMap;
use std::sync::Arc;

use crate::bitcoind_client::BitcoindClient;
use crate::{hex_utils, prompt, user_err, ChannelManager, FilesystemLogger};

/// Drive the funding flow for a newly-ready channel.
///
/// The split return from upstream is `()` — errors are logged via the
/// dual-output `user_err!` macro and surface on both stdout and the log
/// file.
// Eight parameters reflect the genuine dependencies of the funding
// flow (three bitcoind-derived inputs, the channel manager, the network
// for address encoding, the originating event fields, and the logger).
// Bundling them into a struct would only add one layer of indirection.
#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_funding_generation_ready(
	temporary_channel_id: ChannelId, counterparty_node_id: PublicKey,
	channel_value_satoshis: u64, output_script: bitcoin::ScriptBuf, network: Network,
	bitcoind_client: &Arc<BitcoindClient>, channel_manager: &Arc<ChannelManager>,
	logger: &FilesystemLogger,
) {
	let addr = match WitnessProgram::from_scriptpubkey(
		output_script.as_bytes(),
		bech32_network(network),
	) {
		Ok(addr) => addr.to_address(),
		Err(e) => {
			user_err!(logger, "failed to derive funding address from script: {}", e);
			prompt!();
			return;
		},
	};
	let mut outputs = vec![StdHashMap::new()];
	outputs[0].insert(addr, channel_value_satoshis as f64 / 100_000_000.0);

	let raw_tx = match bitcoind_client.create_raw_transaction(outputs).await {
		Ok(tx) => tx,
		Err(e) => {
			user_err!(logger, "failed to create raw funding transaction: {}", e);
			prompt!();
			return;
		},
	};
	let funded_tx = match bitcoind_client.fund_raw_transaction(raw_tx).await {
		Ok(tx) => tx,
		Err(e) => {
			user_err!(logger, "failed to fund raw transaction: {}", e);
			prompt!();
			return;
		},
	};
	let signed_tx = match bitcoind_client.sign_raw_transaction_with_wallet(funded_tx.hex).await {
		Ok(tx) => tx,
		Err(e) => {
			user_err!(logger, "failed to sign funding transaction: {}", e);
			prompt!();
			return;
		},
	};
	if !signed_tx.complete {
		user_err!(logger, "Wallet returned incomplete funding transaction signature");
		prompt!();
		return;
	}
	let signed_tx_bytes = match hex_utils::to_vec(&signed_tx.hex) {
		Some(bytes) => bytes,
		None => {
			user_err!(logger, "Failed to decode signed funding transaction hex");
			prompt!();
			return;
		},
	};
	let final_tx: Transaction = match encode::deserialize(&signed_tx_bytes) {
		Ok(tx) => tx,
		Err(e) => {
			user_err!(logger, "Failed to deserialize signed funding transaction: {}", e);
			prompt!();
			return;
		},
	};
	if channel_manager
		.funding_transaction_generated(temporary_channel_id, counterparty_node_id, final_tx)
		.is_err()
	{
		user_err!(
			logger,
			"Channel went away before we could fund it. The peer disconnected or refused the channel."
		);
		prompt!();
	}
}

fn bech32_network(network: Network) -> bitcoin_bech32::constants::Network {
	match network {
		Network::Bitcoin => bitcoin_bech32::constants::Network::Bitcoin,
		Network::Regtest => bitcoin_bech32::constants::Network::Regtest,
		Network::Signet => bitcoin_bech32::constants::Network::Signet,
		Network::Testnet => bitcoin_bech32::constants::Network::Testnet,
		// Fall back to Testnet constants for any future LDK-supported
		// network; bitcoin_bech32 does not currently expose variants for
		// them, and Testnet's HRP is the safest default for test-only
		// deployments.
		_ => bitcoin_bech32::constants::Network::Testnet,
	}
}
