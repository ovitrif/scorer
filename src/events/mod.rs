mod funding;

use crate::bitcoind_client::BitcoindClient;
use crate::disk::{INBOUND_PAYMENTS_FNAME, OUTBOUND_PAYMENTS_FNAME};
use crate::net::peer::connect_peer_if_necessary;
use crate::probing::ProbeTracker;
use crate::{
	prompt, user_err, user_out, user_warn, BumpTxEventHandler, ChannelManager, FilesystemLogger,
	HTLCStatus, InboundPaymentInfoStorage, MillisatAmount, NetworkGraph,
	OutboundPaymentInfoStorage, PaymentInfo, PeerManager,
};
use bitcoin::network::Network;
use lightning::events::{Event, PaymentFailureReason, PaymentPurpose};
use lightning::ln::types::ChannelId;
use lightning::routing::gossip::NodeId;
use lightning::sign::EntropySource;
use lightning::util::hash_tables::hash_map::Entry;
use lightning::util::logger::Logger;
use lightning::util::persist::KVStore;
use lightning::util::ser::Writeable;
use lightning_persister::fs_store::FilesystemStore;
use std::net::ToSocketAddrs;
use std::sync::{Arc, Mutex};
use tokio::sync::Mutex as AsyncMutex;

/// All dependencies an event handler needs. Assembled once at startup in
/// `main.rs` and cheaply cloned into each handler invocation.
///
/// Note: `logger` is carried alongside the other collaborators so that
/// every error path can route through the dual-output macros in
/// `src/logging.rs` (terminal + log file).
#[derive(Clone)]
pub(crate) struct EventContext {
	pub(crate) channel_manager: Arc<ChannelManager>,
	pub(crate) bitcoind_client: Arc<BitcoindClient>,
	pub(crate) network_graph: Arc<NetworkGraph>,
	pub(crate) keys_manager: Arc<crate::KeysManager>,
	pub(crate) bump_tx_event_handler: Arc<BumpTxEventHandler>,
	pub(crate) peer_manager: Arc<PeerManager>,
	// Phase 4: payment-state mutexes are `tokio::sync::Mutex` so that the
	// `await` points inside event handlers (e.g. `fs_store.write(...).await`)
	// can be enforced by `#![deny(clippy::await_holding_lock)]` to NOT hold
	// the guard. With `std::sync::Mutex` the lint is silent and correctness
	// depends on careful block-expression scoping. The async variant makes
	// the invariant a compile-time property.
	pub(crate) inbound_payments: Arc<AsyncMutex<InboundPaymentInfoStorage>>,
	pub(crate) outbound_payments: Arc<AsyncMutex<OutboundPaymentInfoStorage>>,
	pub(crate) fs_store: Arc<FilesystemStore>,
	pub(crate) output_sweeper: Arc<crate::OutputSweeper>,
	pub(crate) network: Network,
	pub(crate) probe_tracker: Arc<Mutex<ProbeTracker>>,
	pub(crate) logger: Arc<FilesystemLogger>,
}

pub(crate) async fn handle_ldk_events(ctx: Arc<EventContext>, event: Event) {
	let channel_manager = &ctx.channel_manager;
	let bitcoind_client = &ctx.bitcoind_client;
	let network_graph = &ctx.network_graph;
	let keys_manager = &ctx.keys_manager;
	let bump_tx_event_handler = &ctx.bump_tx_event_handler;
	let peer_manager = &ctx.peer_manager;
	let inbound_payments = &ctx.inbound_payments;
	let outbound_payments = &ctx.outbound_payments;
	let fs_store = &ctx.fs_store;
	let output_sweeper = &ctx.output_sweeper;
	let network = ctx.network;
	let probe_tracker = &ctx.probe_tracker;
	let logger = &ctx.logger;

	match event {
		Event::FundingGenerationReady {
			temporary_channel_id,
			counterparty_node_id,
			channel_value_satoshis,
			output_script,
			..
		} => {
			funding::handle_funding_generation_ready(
				temporary_channel_id,
				counterparty_node_id,
				channel_value_satoshis,
				output_script,
				network,
				bitcoind_client,
				channel_manager,
				logger,
			)
			.await;
		},
		Event::FundingTxBroadcastSafe { .. } => {},
		Event::PaymentClaimable { payment_hash, purpose, amount_msat, .. } => {
			user_out!(
				&**logger,
				"\nEVENT: received payment from payment hash {} of {} millisatoshis",
				payment_hash,
				amount_msat
			);
			prompt!();
			let payment_preimage = match purpose {
				PaymentPurpose::Bolt11InvoicePayment { payment_preimage, .. } => payment_preimage,
				PaymentPurpose::Bolt12OfferPayment { payment_preimage, .. } => payment_preimage,
				PaymentPurpose::Bolt12RefundPayment { payment_preimage, .. } => payment_preimage,
				PaymentPurpose::SpontaneousPayment(preimage) => Some(preimage),
			};
			if let Some(preimage) = payment_preimage {
				channel_manager.claim_funds(preimage);
			} else {
				user_err!(&**logger, "PaymentClaimable event missing payment preimage");
				prompt!();
			}
		},
		Event::PaymentClaimed { payment_hash, purpose, amount_msat, .. } => {
			user_out!(
				&**logger,
				"\nEVENT: claimed payment from payment hash {} of {} millisatoshis",
				payment_hash,
				amount_msat
			);
			prompt!();
			let (payment_preimage, payment_secret) = match purpose {
				PaymentPurpose::Bolt11InvoicePayment {
					payment_preimage, payment_secret, ..
				} => (payment_preimage, Some(payment_secret)),
				PaymentPurpose::Bolt12OfferPayment { payment_preimage, payment_secret, .. } => {
					(payment_preimage, Some(payment_secret))
				},
				PaymentPurpose::Bolt12RefundPayment {
					payment_preimage, payment_secret, ..
				} => (payment_preimage, Some(payment_secret)),
				PaymentPurpose::SpontaneousPayment(preimage) => (Some(preimage), None),
			};
			// Build the persistence future under the guard, then drop the
			// guard *before* awaiting. This is explicit here (rather than
			// relying on a block-expression) because the `tokio::sync::Mutex`
			// guard *can* legally be held across `.await`, but we prefer
			// not to so that concurrent event handlers don't serialise on
			// the filesystem write.
			let write_future = {
				let mut inbound = inbound_payments.lock().await;
				match inbound.payments.entry(payment_hash) {
					Entry::Occupied(mut e) => {
						let payment = e.get_mut();
						payment.status = HTLCStatus::Succeeded;
						payment.preimage = payment_preimage;
						payment.secret = payment_secret;
					},
					Entry::Vacant(e) => {
						e.insert(PaymentInfo {
							preimage: payment_preimage,
							secret: payment_secret,
							status: HTLCStatus::Succeeded,
							amt_msat: MillisatAmount(Some(amount_msat)),
						});
					},
				}
				fs_store.write("", "", INBOUND_PAYMENTS_FNAME, inbound.encode())
			};
			if let Err(e) = write_future.await {
				user_err!(&**logger, "failed to persist inbound payment state: {}", e);
			}
		},
		Event::PaymentSent {
			payment_preimage, payment_hash, fee_paid_msat, payment_id, ..
		} => {
			let Some(payment_id) = payment_id else {
				user_warn!(&**logger, "PaymentSent event missing payment_id");
				return;
			};

			let write_future = {
				let mut outbound = outbound_payments.lock().await;
				for (id, payment) in outbound.payments.iter_mut() {
					if *id == payment_id {
						payment.preimage = Some(payment_preimage);
						payment.status = HTLCStatus::Succeeded;
						user_out!(
							&**logger,
							"\nEVENT: successfully sent payment of {} millisatoshis{} from \
									 payment hash {} with preimage {}",
							payment.amt_msat,
							if let Some(fee) = fee_paid_msat {
								format!(" (fee {} msat)", fee)
							} else {
								"".to_string()
							},
							payment_hash,
							payment_preimage
						);
						prompt!();
					}
				}
				fs_store.write("", "", OUTBOUND_PAYMENTS_FNAME, outbound.encode())
			};
			if let Err(e) = write_future.await {
				user_err!(&**logger, "failed to persist outbound payment state: {}", e);
			}
		},
		Event::OpenChannelRequest {
			ref temporary_channel_id, ref counterparty_node_id, ..
		} => {
			let mut random_bytes = [0u8; 16];
			random_bytes.copy_from_slice(&keys_manager.get_secure_random_bytes()[..16]);
			let user_channel_id = u128::from_be_bytes(random_bytes);
			let res = channel_manager.accept_inbound_channel(
				temporary_channel_id,
				counterparty_node_id,
				user_channel_id,
				None,
			);

			if let Err(e) = res {
				user_err!(
					&**logger,
					"Failed to accept inbound channel ({}) from {}: {:?}",
					temporary_channel_id,
					crate::hex_utils::hex_str(&counterparty_node_id.serialize()),
					e
				);
			} else {
				user_out!(
					&**logger,
					"\nEVENT: Accepted inbound channel ({}) from {}",
					temporary_channel_id,
					crate::hex_utils::hex_str(&counterparty_node_id.serialize())
				);
			}
			prompt!();
		},
		Event::PaymentPathSuccessful { .. } => {},
		Event::PaymentPathFailed { .. } => {},
		Event::ProbeSuccessful { payment_hash, .. } => match probe_tracker.lock() {
			Ok(mut tracker) => tracker.complete_success(&payment_hash),
			Err(_) => user_err!(&**logger, "probe tracker lock poisoned"),
		},
		Event::ProbeFailed { payment_hash, .. } => match probe_tracker.lock() {
			Ok(mut tracker) => tracker.complete_failed(&payment_hash),
			Err(_) => user_err!(&**logger, "probe tracker lock poisoned"),
		},
		Event::PaymentFailed { payment_hash, reason, payment_id, .. } => {
			let reason = reason.unwrap_or(PaymentFailureReason::RetriesExhausted);
			if let Some(hash) = payment_hash {
				user_out!(
					&**logger,
					"\nEVENT: Failed to send payment to payment ID {}, payment hash {}: {:?}",
					payment_id,
					hash,
					reason
				);
			} else {
				user_out!(
					&**logger,
					"\nEVENT: Failed fetch invoice for payment ID {}: {:?}",
					payment_id,
					reason
				);
			}
			prompt!();

			let write_future = {
				let mut outbound = outbound_payments.lock().await;
				if let Some(payment) = outbound.payments.get_mut(&payment_id) {
					payment.status = HTLCStatus::Failed;
				}
				fs_store.write("", "", OUTBOUND_PAYMENTS_FNAME, outbound.encode())
			};
			if let Err(e) = write_future.await {
				user_err!(&**logger, "failed to persist outbound payment failure state: {}", e);
			}
		},
		Event::InvoiceReceived { .. } => {},
		Event::PaymentForwarded {
			prev_channel_id,
			next_channel_id,
			total_fee_earned_msat,
			claim_from_onchain_tx,
			outbound_amount_forwarded_msat,
			..
		} => {
			let read_only_network_graph = network_graph.read_only();
			let nodes = read_only_network_graph.nodes();
			let channels = channel_manager.list_channels();

			let node_str = |channel_id: &Option<ChannelId>| match channel_id {
				None => String::new(),
				Some(channel_id) => match channels.iter().find(|c| c.channel_id == *channel_id) {
					None => String::new(),
					Some(channel) => {
						match nodes.get(&NodeId::from_pubkey(&channel.counterparty.node_id)) {
							None => "private node".to_string(),
							Some(node) => match &node.announcement_info {
								None => "unnamed node".to_string(),
								Some(announcement) => {
									format!("node {}", announcement.alias())
								},
							},
						}
					},
				},
			};
			let channel_str = |channel_id: &Option<ChannelId>| {
				channel_id
					.map(|channel_id| format!(" with channel {}", channel_id))
					.unwrap_or_default()
			};
			let from_prev_str =
				format!(" from {}{}", node_str(&prev_channel_id), channel_str(&prev_channel_id),);
			let to_next_str =
				format!(" to {}{}", node_str(&next_channel_id), channel_str(&next_channel_id));

			let from_onchain_str = if claim_from_onchain_tx {
				"from onchain downstream claim"
			} else {
				"from HTLC fulfill message"
			};
			let amt_args = if let Some(v) = outbound_amount_forwarded_msat {
				format!("{}", v)
			} else {
				"?".to_string()
			};
			if let Some(fee_earned) = total_fee_earned_msat {
				user_out!(
					&**logger,
					"\nEVENT: Forwarded payment for {} msat{}{}, earning {} msat {}",
					amt_args,
					from_prev_str,
					to_next_str,
					fee_earned,
					from_onchain_str
				);
			} else {
				user_out!(
					&**logger,
					"\nEVENT: Forwarded payment for {} msat{}{}, claiming onchain {}",
					amt_args,
					from_prev_str,
					to_next_str,
					from_onchain_str
				);
			}
			prompt!();
		},
		Event::HTLCHandlingFailed { .. } => {},
		Event::SpendableOutputs { outputs, channel_id } => {
			if let Err(e) =
				output_sweeper.track_spendable_outputs(outputs, channel_id, false, None).await
			{
				user_err!(&**logger, "failed to track spendable outputs: {:?}", e);
			}
		},
		Event::ChannelPending { channel_id, counterparty_node_id, .. } => {
			user_out!(
				&**logger,
				"\nEVENT: Channel {} with peer {} is pending awaiting funding lock-in!",
				channel_id,
				crate::hex_utils::hex_str(&counterparty_node_id.serialize())
			);
			prompt!();
		},
		Event::ChannelReady { ref channel_id, ref counterparty_node_id, .. } => {
			user_out!(
				&**logger,
				"\nEVENT: Channel {} with peer {} is ready to be used!",
				channel_id,
				crate::hex_utils::hex_str(&counterparty_node_id.serialize())
			);
			prompt!();
		},
		Event::ChannelClosed { channel_id, reason, counterparty_node_id, .. } => {
			user_out!(
				&**logger,
				"\nEVENT: Channel {} with counterparty {} closed due to: {:?}",
				channel_id,
				counterparty_node_id.map(|id| format!("{}", id)).unwrap_or("".to_owned()),
				reason
			);
			prompt!();
		},
		Event::DiscardFunding { .. } => {},
		Event::HTLCIntercepted { .. } => {},
		Event::OnionMessageIntercepted { .. } => {},
		Event::OnionMessagePeerConnected { .. } => {},
		Event::BumpTransaction(event) => bump_tx_event_handler.handle_event(&event).await,
		Event::ConnectionNeeded { node_id, addresses } => {
			let peer_manager = Arc::clone(peer_manager);
			tokio::spawn(async move {
				for address in addresses {
					if let Ok(sockaddrs) = address.to_socket_addrs() {
						for addr in sockaddrs {
							let pm = Arc::clone(&peer_manager);
							if connect_peer_if_necessary(node_id, addr, pm).await.is_ok() {
								return;
							}
						}
					}
				}
			});
		},
		_ => {},
	}
}
