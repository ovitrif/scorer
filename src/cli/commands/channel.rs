//! `openchannel` / `closechannel` / `forceclosechannel`.

use std::sync::Arc;

use bitcoin::secp256k1::PublicKey;
use lightning::ln::types::ChannelId;
use lightning::util::config::{ChannelHandshakeConfig, ChannelHandshakeLimits, UserConfig};
// Bringing `Logger` into scope is required for the `user_*!` macros to
// resolve `.log(...)` on the logger reference.
use lightning::util::logger::Logger;

use crate::cli::CliError;
use crate::{user_err, user_out, ChannelManager, FilesystemLogger};

pub(crate) fn open_channel(
	peer_pubkey: PublicKey, channel_amt_sat: u64, announce_for_forwarding: bool,
	with_anchors: bool, channel_manager: Arc<ChannelManager>,
) -> Result<(), CliError> {
	let config = UserConfig {
		channel_handshake_limits: ChannelHandshakeLimits {
			// lnd's max to_self_delay is 2016, so we want to be compatible.
			their_to_self_delay: 2016,
			..Default::default()
		},
		channel_handshake_config: ChannelHandshakeConfig {
			announce_for_forwarding,
			negotiate_anchors_zero_fee_htlc_tx: with_anchors,
			..Default::default()
		},
		..Default::default()
	};

	match channel_manager.create_channel(peer_pubkey, channel_amt_sat, 0, 0, None, Some(config)) {
		Ok(_) => {
			println!("EVENT: initiated channel with peer {}. ", peer_pubkey);
			Ok(())
		},
		Err(e) => Err(CliError::ChannelOpenFailed(format!("{:?}", e))),
	}
}

pub(crate) fn close_channel(
	channel_id: [u8; 32], counterparty_node_id: PublicKey, channel_manager: Arc<ChannelManager>,
	logger: &FilesystemLogger,
) {
	match channel_manager.close_channel(&ChannelId(channel_id), &counterparty_node_id) {
		Ok(()) => user_out!(logger, "EVENT: initiating channel close"),
		Err(e) => user_err!(logger, "failed to close channel: {:?}", e),
	}
}

pub(crate) fn force_close_channel(
	channel_id: [u8; 32], counterparty_node_id: PublicKey, channel_manager: Arc<ChannelManager>,
	logger: &FilesystemLogger,
) {
	match channel_manager.force_close_broadcasting_latest_txn(
		&ChannelId(channel_id),
		&counterparty_node_id,
		"Manually force-closed".to_string(),
	) {
		Ok(()) => user_out!(logger, "EVENT: initiating channel force-close"),
		Err(e) => user_err!(logger, "failed to force-close channel: {:?}", e),
	}
}
