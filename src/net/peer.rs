//! Outbound peer connection helpers.
//!
//! Both the REPL (`cli::commands::peer`) and the LDK event handler
//! (`events::connection`) need to initiate outbound peer connections.
//! Putting the two primitives here means the event handler no longer
//! depends on the CLI module — the dependency now points from both to
//! this neutral helper, which is a proper strict partial order.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bitcoin::secp256k1::PublicKey;
use thiserror::Error;

use crate::types::PeerManager;

#[derive(Debug, Error)]
pub(crate) enum ConnectError {
	#[error("failed to initiate outbound connection")]
	ConnectionFailed,
	#[error("connection closed before peer handshake completed")]
	ConnectionClosed,
}

/// Connect to `pubkey@peer_addr` if not already connected. Idempotent.
pub(crate) async fn connect_peer_if_necessary(
	pubkey: PublicKey, peer_addr: SocketAddr, peer_manager: Arc<PeerManager>,
) -> Result<(), ConnectError> {
	if peer_manager.peer_by_node_id(&pubkey).is_some() {
		return Ok(());
	}
	do_connect_peer(pubkey, peer_addr, peer_manager).await
}

/// Unconditionally attempt to connect to `pubkey@peer_addr`, spinning
/// until either the connection is established or the TCP-level future
/// resolves (indicating the connection closed).
pub(crate) async fn do_connect_peer(
	pubkey: PublicKey, peer_addr: SocketAddr, peer_manager: Arc<PeerManager>,
) -> Result<(), ConnectError> {
	match lightning_net_tokio::connect_outbound(Arc::clone(&peer_manager), pubkey, peer_addr).await
	{
		Some(connection_closed_future) => {
			let mut connection_closed_future = Box::pin(connection_closed_future);
			loop {
				tokio::select! {
					_ = &mut connection_closed_future => return Err(ConnectError::ConnectionClosed),
					_ = tokio::time::sleep(Duration::from_millis(10)) => {},
				};
				if peer_manager.peer_by_node_id(&pubkey).is_some() {
					return Ok(());
				}
			}
		},
		None => Err(ConnectError::ConnectionFailed),
	}
}
