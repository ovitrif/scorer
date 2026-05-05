//! `connectpeer` / `disconnectpeer` / `parse_peer_info`.

use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;

use bitcoin::secp256k1::PublicKey;

use crate::cli::CliError;
use crate::hex_utils;
use crate::{ChannelManager, PeerManager};

pub(crate) fn parse_peer_info(
	peer_pubkey_and_ip_addr: String,
) -> Result<(PublicKey, SocketAddr), CliError> {
	let mut pubkey_and_addr = peer_pubkey_and_ip_addr.split('@');
	let pubkey_str = pubkey_and_addr.next().ok_or(CliError::InvalidPeerInfoFormat)?;
	let peer_addr_str = pubkey_and_addr.next().ok_or(CliError::InvalidPeerInfoFormat)?;

	let mut peer_addrs = peer_addr_str
		.to_socket_addrs()
		.map_err(|_| CliError::PeerAddressResolution(peer_addr_str.to_string()))?;
	let peer_addr = peer_addrs
		.next()
		.ok_or_else(|| CliError::PeerAddressResolution(peer_addr_str.to_string()))?;

	let pubkey = hex_utils::to_compressed_pubkey(pubkey_str).ok_or(CliError::InvalidPubkey)?;

	Ok((pubkey, peer_addr))
}

pub(crate) fn do_disconnect_peer(
	pubkey: PublicKey, peer_manager: Arc<PeerManager>, channel_manager: Arc<ChannelManager>,
) -> Result<(), CliError> {
	// Refuse to disconnect a peer we still have an open channel with;
	// LDK would re-connect immediately to keep the channel alive anyway.
	for channel in channel_manager.list_channels() {
		if channel.counterparty.node_id == pubkey {
			return Err(CliError::ActiveChannelWithPeer);
		}
	}

	if peer_manager.peer_by_node_id(&pubkey).is_none() {
		return Err(CliError::PeerNotConnected(pubkey));
	}

	peer_manager.disconnect_by_node_id(pubkey);
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::parse_peer_info;
	use crate::cli::CliError;

	#[test]
	fn parse_peer_info_accepts_valid_pubkey_and_socket() {
		let input =
			"02e89ca9e8da72b33d896bae51d20e7e1f0571cece852d1387b4e2f4ae48c80a85@127.0.0.1:9735"
				.to_string();
		let parsed = parse_peer_info(input);
		assert!(parsed.is_ok());
	}

	#[test]
	fn parse_peer_info_rejects_missing_separator() {
		let input =
			"02e89ca9e8da72b33d896bae51d20e7e1f0571cece852d1387b4e2f4ae48c80a85".to_string();
		assert!(matches!(parse_peer_info(input), Err(CliError::InvalidPeerInfoFormat)));
	}

	#[test]
	fn parse_peer_info_rejects_invalid_pubkey() {
		let input = "zzzz@127.0.0.1:9735".to_string();
		assert!(matches!(parse_peer_info(input), Err(CliError::InvalidPubkey)));
	}
}
