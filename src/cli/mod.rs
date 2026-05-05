mod commands;
mod display;

use crate::disk::{INBOUND_PAYMENTS_FNAME, OUTBOUND_PAYMENTS_FNAME};
use crate::hex_utils;
use crate::{
	prompt, user_err, ChainMonitor, ChannelManager, FilesystemLogger, HTLCStatus,
	InboundPaymentInfoStorage, MillisatAmount, NetworkGraph, OutboundPaymentInfoStorage,
	OutputSweeper, PaymentInfo, PeerManager,
};
use bitcoin::secp256k1::PublicKey;
use lightning::ln::channelmanager::{OptionalOfferPaymentParams, PaymentId, Retry};
use lightning::offers::offer::{self, Offer};
use lightning::onion_message::dns_resolution::HumanReadableName;
use lightning::onion_message::messenger::Destination;
use lightning::sign::{EntropySource, KeysManager};
use lightning::util::logger::Logger;
use lightning::util::persist::KVStore;
use lightning::util::ser::Writeable;
use lightning_invoice::Bolt11Invoice;
use lightning_persister::fs_store::FilesystemStore;
use std::io::Write;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Mutex as AsyncMutex;

use tokio::io::{AsyncBufReadExt, BufReader};

// Historical helpers have been moved to the submodules below. The `use`
// statements below make them reachable under their short names so the
// huge dispatch loop in `poll_for_user_input` still reads the same.
use commands::channel::{close_channel, force_close_channel, open_channel};
use commands::payment::{get_invoice, keysend, send_payment};
use commands::peer::do_disconnect_peer;
pub(crate) use commands::peer::parse_peer_info;
use display::{
	help, list_channels, list_claimable_balances, list_payments, list_peers, list_sweeper_outputs,
	node_info,
};

pub(crate) struct CliRuntime {
	pub(crate) peer_manager: Arc<PeerManager>,
	pub(crate) channel_manager: Arc<ChannelManager>,
	pub(crate) chain_monitor: Arc<ChainMonitor>,
	pub(crate) keys_manager: Arc<KeysManager>,
	pub(crate) network_graph: Arc<NetworkGraph>,
	// See `EventContext` in `events.rs` for rationale.
	pub(crate) inbound_payments: Arc<AsyncMutex<InboundPaymentInfoStorage>>,
	pub(crate) outbound_payments: Arc<AsyncMutex<OutboundPaymentInfoStorage>>,
	pub(crate) output_sweeper: Arc<OutputSweeper>,
	pub(crate) fs_store: Arc<FilesystemStore>,
	/// Shared logger so every CLI error path mirrors to the rotating log
	/// file in addition to the terminal (see `src/logging.rs`).
	pub(crate) logger: Arc<FilesystemLogger>,
}

#[derive(Debug, Error)]
pub(crate) enum CliError {
	#[error("incorrectly formatted peer info, expected `pubkey@host:port`")]
	InvalidPeerInfoFormat,
	#[error("could not resolve `{0}` to a socket address")]
	PeerAddressResolution(String),
	#[error("unable to parse peer public key")]
	InvalidPubkey,
	#[error("failed to connect to peer")]
	PeerConnectionFailed,
	#[error("connection closed before peer handshake completed")]
	ConnectionClosed,
	#[error("node has an active channel with this peer, close channels first")]
	ActiveChannelWithPeer,
	#[error("could not find connected peer {0}")]
	PeerNotConnected(PublicKey),
	#[error("failed to open channel: {0}")]
	ChannelOpenFailed(String),
}

pub(crate) async fn poll_for_user_input(runtime: CliRuntime) {
	let CliRuntime {
		peer_manager,
		channel_manager,
		chain_monitor,
		keys_manager,
		network_graph,
		inbound_payments,
		outbound_payments,
		output_sweeper,
		fs_store,
		logger,
	} = runtime;

	println!(
		"LDK startup successful. Enter \"help\" to view available commands. Press Ctrl-D to quit."
	);
	println!("LDK logs are available at <your-supplied-ldk-data-dir-path>/.ldk/logs");
	println!("Local Node ID is {}.", channel_manager.get_our_node_id());

	let mut input = BufReader::new(tokio::io::stdin()).lines();
	'read_command: loop {
		prompt!(); // Without flushing, the `>` doesn't print
		let line = match input.next_line().await {
			Ok(Some(l)) => l,
			Err(e) => {
				break user_err!(&*logger, "{}", e);
			},
			Ok(None) => {
				break user_err!(&*logger, "End of stdin");
			},
		};

		let mut words = line.split_whitespace();
		if let Some(word) = words.next() {
			match word {
				"help" => help(),
				"openchannel" => {
					let (peer_pubkey_and_ip_addr, channel_value_sat) = match (
						words.next(),
						words.next(),
					) {
						(Some(peer), Some(amount)) => (peer, amount),
						_ => {
							user_err!(&*logger, "openchannel has 2 required arguments: `openchannel pubkey@host:port channel_amt_satoshis` [--public] [--with-anchors]");
							continue;
						},
					};

					let mut pubkey_and_addr = peer_pubkey_and_ip_addr.split("@");
					let pubkey = pubkey_and_addr.next().unwrap_or("");
					let peer_addr_str = pubkey_and_addr.next();
					let pubkey = hex_utils::to_compressed_pubkey(pubkey);
					if let Some(pubkey) = pubkey {
						if peer_addr_str.is_none() {
							if peer_manager.peer_by_node_id(&pubkey).is_none() {
								println!(
									"ERROR: Peer address not provided and peer is not connected"
								);
								continue;
							}
						} else {
							let (pubkey, peer_addr) =
								match parse_peer_info(peer_pubkey_and_ip_addr.to_string()) {
									Ok(info) => info,
									Err(e) => {
										user_err!(&*logger, "{}", e);
										continue;
									},
								};

							if let Err(e) =
								connect_peer_if_necessary(pubkey, peer_addr, peer_manager.clone())
									.await
							{
								user_err!(&*logger, "{}", e);
								continue;
							};
						}

						let chan_amt_sat: u64 = match channel_value_sat.parse() {
							Ok(amount) => amount,
							Err(_) => {
								user_err!(&*logger, "channel amount must be a number");
								continue;
							},
						};
						let (mut announce_channel, mut with_anchors) = (false, false);
						for word in words.by_ref() {
							match word {
								"--public" | "--public=true" => announce_channel = true,
								"--public=false" => announce_channel = false,
								"--with-anchors" | "--with-anchors=true" => with_anchors = true,
								"--with-anchors=false" => with_anchors = false,
								_ => {
									user_err!(&*logger, "invalid boolean flag format. Valid formats: `--option`, `--option=true` `--option=false`");
									continue;
								},
							}
						}

						if let Err(e) = open_channel(
							pubkey,
							chan_amt_sat,
							announce_channel,
							with_anchors,
							channel_manager.clone(),
						) {
							user_err!(&*logger, "{}", e);
						}
					} else {
						user_err!(&*logger, "unable to parse given pubkey for node");
						continue;
					}
				},
				"sendpayment" => {
					let invoice_str = match words.next() {
						Some(invoice) => invoice,
						None => {
							user_err!(&*logger, "sendpayment requires an invoice: `sendpayment <invoice> [amount_msat]`");
							continue;
						},
					};

					let mut user_provided_amt: Option<u64> = None;
					if let Some(amt_msat_str) = words.next() {
						match amt_msat_str.parse() {
							Ok(amt) => user_provided_amt = Some(amt),
							Err(e) => {
								user_err!(&*logger, "couldn't parse amount_msat: {}", e);
								continue;
							},
						};
					}

					if let Ok(offer) = Offer::from_str(invoice_str) {
						let random_bytes = keys_manager.get_secure_random_bytes();
						let payment_id = PaymentId(random_bytes);

						let amt_msat = match (offer.amount(), user_provided_amt) {
							(Some(offer::Amount::Bitcoin { amount_msats }), _) => amount_msats,
							(_, Some(amt)) => amt,
							(amt, _) => {
								user_err!(&*logger, "Cannot process non-Bitcoin-denominated offer value {:?}", amt);
								continue;
							},
						};
						if user_provided_amt.is_some() && user_provided_amt != Some(amt_msat) {
							println!("Amount didn't match offer of {}msat", amt_msat);
							continue;
						}

						while user_provided_amt.is_none() {
							print!("Paying offer for {} msat. Continue (Y/N)? >", amt_msat);
							let _ = std::io::stdout().flush();

							let line = match input.next_line().await {
								Ok(Some(l)) => l,
								Err(e) => {
									user_err!(&*logger, "{}", e);
									break 'read_command;
								},
								Ok(None) => {
									user_err!(&*logger, "End of stdin");
									break 'read_command;
								},
							};

							if line.starts_with("Y") {
								break;
							}
							if line.starts_with("N") {
								continue 'read_command;
							}
						}

						let write_future = {
							let mut outbound_payments = outbound_payments.lock().await;
							outbound_payments.payments.insert(
								payment_id,
								PaymentInfo {
									preimage: None,
									secret: None,
									status: HTLCStatus::Pending,
									amt_msat: MillisatAmount(Some(amt_msat)),
								},
							);
							fs_store.write(
								"",
								"",
								OUTBOUND_PAYMENTS_FNAME,
								outbound_payments.encode(),
							)
						};
						if let Err(e) = write_future.await {
							user_err!(&*logger, "failed to persist outbound payments: {}", e);
							continue;
						}

						let params = OptionalOfferPaymentParams {
							retry_strategy: Retry::Timeout(Duration::from_secs(10)),
							..Default::default()
						};
						let amt = Some(amt_msat);
						let pay = channel_manager.pay_for_offer(&offer, amt, payment_id, params);
						if pay.is_ok() {
							println!("Payment in flight");
						} else {
							user_err!(&*logger, "Failed to pay: {:?}", pay);
						}
					} else if let Ok(hrn) = HumanReadableName::from_encoded(invoice_str) {
						let random_bytes = keys_manager.get_secure_random_bytes();
						let payment_id = PaymentId(random_bytes);

						if user_provided_amt.is_none() {
							println!("Can't pay to a human-readable-name without an amount");
							continue;
						}

						// We need some nodes that will resolve DNS for us in order to pay a Human
						// Readable Name. They don't need to be trusted, but until onion message
						// forwarding is widespread we'll directly connect to them, revealing who
						// we intend to pay.
						let mut dns_resolvers = Vec::new();
						for (node_id, node) in network_graph.read_only().nodes().unordered_iter() {
							if let Some(info) = &node.announcement_info {
								// Sadly, 31 nodes currently squat on the DNS Resolver feature bit
								// without speaking it.
								// Its unclear why they're doing so, but none of them currently
								// also have the onion messaging feature bit set, so here we check
								// for both.
								let supports_dns = info.features().supports_dns_resolution();
								let supports_om = info.features().supports_onion_messages();
								if supports_dns && supports_om {
									if let Ok(pubkey) = node_id.as_pubkey() {
										dns_resolvers.push(Destination::Node(pubkey));
									}
								}
							}
							if dns_resolvers.len() > 5 {
								break;
							}
						}
						if dns_resolvers.is_empty() {
							println!(
								"Failed to find any DNS resolving nodes, check your network graph is synced"
							);
							continue;
						}

						let Some(amt_msat) = user_provided_amt else {
							println!("Can't pay to a human-readable-name without an amount");
							continue;
						};
						let write_future = {
							let mut outbound_payments = outbound_payments.lock().await;
							outbound_payments.payments.insert(
								payment_id,
								PaymentInfo {
									preimage: None,
									secret: None,
									status: HTLCStatus::Pending,
									amt_msat: MillisatAmount(Some(amt_msat)),
								},
							);
							fs_store.write(
								"",
								"",
								OUTBOUND_PAYMENTS_FNAME,
								outbound_payments.encode(),
							)
						};
						if let Err(e) = write_future.await {
							user_err!(&*logger, "failed to persist outbound payments: {}", e);
							continue;
						}

						let params = OptionalOfferPaymentParams {
							retry_strategy: Retry::Timeout(Duration::from_secs(10)),
							..Default::default()
						};
						#[allow(deprecated)]
						let pay = |a, b, c, d, e| {
							channel_manager.pay_for_offer_from_human_readable_name(a, b, c, d, e)
						};
						let pay = pay(hrn, amt_msat, payment_id, params, dns_resolvers);
						if pay.is_ok() {
							println!("Payment in flight");
						} else {
							user_err!(&*logger, "Failed to pay");
						}
					} else {
						match Bolt11Invoice::from_str(invoice_str) {
							Ok(invoice) => {
								send_payment(
									&channel_manager,
									&invoice,
									user_provided_amt,
									&outbound_payments,
									&fs_store,
									&logger,
								)
								.await
							},
							Err(e) => {
								user_err!(&*logger, "invalid invoice: {:?}", e);
							},
						}
					}
				},
				"keysend" => {
					let dest_pubkey = match words.next() {
						Some(dest) => match hex_utils::to_compressed_pubkey(dest) {
							Some(pk) => pk,
							None => {
								user_err!(&*logger, "couldn't parse destination pubkey");
								continue;
							},
						},
						None => {
							user_err!(&*logger, "keysend requires a destination pubkey: `keysend <dest_pubkey> <amt_msat>`");
							continue;
						},
					};
					let amt_msat_str = match words.next() {
						Some(amt) => amt,
						None => {
							user_err!(&*logger, "keysend requires an amount in millisatoshis: `keysend <dest_pubkey> <amt_msat>`");
							continue;
						},
					};
					let amt_msat: u64 = match amt_msat_str.parse() {
						Ok(amt) => amt,
						Err(e) => {
							user_err!(&*logger, "couldn't parse amount_msat: {}", e);
							continue;
						},
					};
					keysend(
						&channel_manager,
						dest_pubkey,
						amt_msat,
						&*keys_manager,
						&outbound_payments,
						&fs_store,
						&logger,
					)
					.await;
				},
				"getoffer" => {
					let offer_builder = match channel_manager.create_offer_builder() {
						Ok(builder) => builder,
						Err(e) => {
							user_err!(&*logger, "Failed to initiate offer building: {:?}", e);
							continue;
						},
					};

					let amt_str = words.next();
					let offer = if let Some(amt) = amt_str {
						let amt_msat: u64 = match amt.parse() {
							Ok(v) => v,
							Err(_) => {
								println!(
									"ERROR: getoffer provided payment amount was not a number"
								);
								continue;
							},
						};
						offer_builder.amount_msats(amt_msat).build()
					} else {
						offer_builder.build()
					};

					match offer {
						Err(e) => {
							user_err!(&*logger, "Failed to build offer: {:?}", e);
						},
						Ok(offer) => {
							// Note that unlike BOLT11 invoice creation we don't bother to add a
							// pending inbound payment here, as offers can be reused and don't
							// correspond with individual payments.
							println!("{}", offer);
						},
					}
				},
				"getinvoice" => {
					let amt_str = match words.next() {
						Some(v) => v,
						None => {
							user_err!(&*logger, "getinvoice requires an amount in millisatoshis");
							continue;
						},
					};

					let amt_msat: u64 = match amt_str.parse() {
						Ok(v) => v,
						Err(_) => {
							user_err!(&*logger, "getinvoice provided payment amount was not a number");
							continue;
						},
					};

					let expiry_secs_str = match words.next() {
						Some(v) => v,
						None => {
							user_err!(&*logger, "getinvoice requires an expiry in seconds");
							continue;
						},
					};

					let expiry_secs: u32 = match expiry_secs_str.parse() {
						Ok(v) => v,
						Err(_) => {
							user_err!(&*logger, "getinvoice provided expiry was not a number");
							continue;
						},
					};

					let write_future = {
						let mut inbound_payments = inbound_payments.lock().await;
						get_invoice(
							amt_msat,
							&mut inbound_payments,
							&channel_manager,
							expiry_secs,
							&logger,
						);
						fs_store.write("", "", INBOUND_PAYMENTS_FNAME, inbound_payments.encode())
					};
					if let Err(e) = write_future.await {
						user_err!(&*logger, "failed to persist inbound payments: {}", e);
					}
				},
				"connectpeer" => {
					let peer_pubkey_and_ip_addr = match words.next() {
						Some(v) => v,
						None => {
							user_err!(&*logger, "connectpeer requires peer connection info: `connectpeer pubkey@host:port`");
							continue;
						},
					};
					let (pubkey, peer_addr) =
						match parse_peer_info(peer_pubkey_and_ip_addr.to_string()) {
							Ok(info) => info,
							Err(e) => {
								user_err!(&*logger, "{}", e);
								continue;
							},
						};
					match connect_peer_if_necessary(pubkey, peer_addr, peer_manager.clone()).await {
						Ok(()) => println!("SUCCESS: connected to peer {}", pubkey),
						Err(e) => user_err!(&*logger, "{}", e),
					}
				},
				"disconnectpeer" => {
					let peer_pubkey = match words.next() {
						Some(v) => v,
						None => {
							user_err!(&*logger, "disconnectpeer requires peer public key: `disconnectpeer <peer_pubkey>`");
							continue;
						},
					};

					let peer_pubkey = match bitcoin::secp256k1::PublicKey::from_str(peer_pubkey) {
						Ok(pubkey) => pubkey,
						Err(e) => {
							user_err!(&*logger, "{}", e);
							continue;
						},
					};

					match do_disconnect_peer(
						peer_pubkey,
						peer_manager.clone(),
						channel_manager.clone(),
					) {
						Ok(()) => println!("SUCCESS: disconnected from peer {}", peer_pubkey),
						Err(e) => user_err!(&*logger, "{}", e),
					}
				},
				"listchannels" => list_channels(&channel_manager, &network_graph),
				"listpayments" => {
					let inbound = inbound_payments.lock().await;
					let outbound = outbound_payments.lock().await;
					list_payments(&inbound, &outbound);
				},
				"closechannel" => {
					let channel_id_str = match words.next() {
						Some(v) => v,
						None => {
							user_err!(&*logger, "closechannel requires a channel ID: `closechannel <channel_id> <peer_pubkey>`");
							continue;
						},
					};
					let channel_id_vec = match hex_utils::to_vec(channel_id_str) {
						Some(v) if v.len() == 32 => v,
						_ => {
							user_err!(&*logger, "couldn't parse channel_id");
							continue;
						},
					};
					let mut channel_id = [0; 32];
					channel_id.copy_from_slice(&channel_id_vec);

					let peer_pubkey_str = match words.next() {
						Some(v) => v,
						None => {
							user_err!(&*logger, "closechannel requires a peer pubkey: `closechannel <channel_id> <peer_pubkey>`");
							continue;
						},
					};
					let peer_pubkey_vec = match hex_utils::to_vec(peer_pubkey_str) {
						Some(peer_pubkey_vec) => peer_pubkey_vec,
						None => {
							user_err!(&*logger, "couldn't parse peer_pubkey");
							continue;
						},
					};
					let peer_pubkey = match PublicKey::from_slice(&peer_pubkey_vec) {
						Ok(peer_pubkey) => peer_pubkey,
						Err(_) => {
							user_err!(&*logger, "couldn't parse peer_pubkey");
							continue;
						},
					};

					close_channel(channel_id, peer_pubkey, channel_manager.clone(), &logger);
				},
				"forceclosechannel" => {
					let channel_id_str = match words.next() {
						Some(v) => v,
						None => {
							user_err!(&*logger, "forceclosechannel requires a channel ID: `forceclosechannel <channel_id> <peer_pubkey>`");
							continue;
						},
					};
					let channel_id_vec = match hex_utils::to_vec(channel_id_str) {
						Some(v) if v.len() == 32 => v,
						_ => {
							user_err!(&*logger, "couldn't parse channel_id");
							continue;
						},
					};
					let mut channel_id = [0; 32];
					channel_id.copy_from_slice(&channel_id_vec);

					let peer_pubkey_str = match words.next() {
						Some(v) => v,
						None => {
							user_err!(&*logger, "forceclosechannel requires a peer pubkey: `forceclosechannel <channel_id> <peer_pubkey>`");
							continue;
						},
					};
					let peer_pubkey_vec = match hex_utils::to_vec(peer_pubkey_str) {
						Some(peer_pubkey_vec) => peer_pubkey_vec,
						None => {
							user_err!(&*logger, "couldn't parse peer_pubkey");
							continue;
						},
					};
					let peer_pubkey = match PublicKey::from_slice(&peer_pubkey_vec) {
						Ok(peer_pubkey) => peer_pubkey,
						Err(_) => {
							user_err!(&*logger, "couldn't parse peer_pubkey");
							continue;
						},
					};

					force_close_channel(
						channel_id,
						peer_pubkey,
						channel_manager.clone(),
						&logger,
					);
				},
				"nodeinfo" => {
					node_info(&channel_manager, &chain_monitor, &peer_manager, &network_graph)
				},
				"listclaimablebalances" => list_claimable_balances(&chain_monitor),
				"listsweeperoutputs" => list_sweeper_outputs(&output_sweeper),
				"listpeers" => list_peers(peer_manager.clone()),
				"signmessage" => {
					const MSG_STARTPOS: usize = "signmessage".len() + 1;
					if line.trim().len() <= MSG_STARTPOS {
						user_err!(&*logger, "signmsg requires a message");
						continue;
					}
					println!(
						"{:?}",
						lightning::util::message_signing::sign(
							&line.trim().as_bytes()[MSG_STARTPOS..],
							&keys_manager.get_node_secret_key()
						)
					);
				},
				"quit" | "exit" => break,
				_ => println!("Unknown command. See `\"help\" for available commands."),
			}
		}
	}
}
// connect_peer_if_necessary / do_connect_peer moved to `src/net/peer.rs` in
// Phase 5 so that `events.rs` no longer depends on `cli`. The thin wrappers
// below preserve the prior `CliError` surface for the REPL call-sites.
pub(crate) async fn connect_peer_if_necessary(
	pubkey: PublicKey, peer_addr: SocketAddr, peer_manager: Arc<PeerManager>,
) -> Result<(), CliError> {
	crate::net::peer::connect_peer_if_necessary(pubkey, peer_addr, peer_manager)
		.await
		.map_err(Into::into)
}

pub(crate) async fn do_connect_peer(
	pubkey: PublicKey, peer_addr: SocketAddr, peer_manager: Arc<PeerManager>,
) -> Result<(), CliError> {
	crate::net::peer::do_connect_peer(pubkey, peer_addr, peer_manager).await.map_err(Into::into)
}

impl From<crate::net::peer::ConnectError> for CliError {
	fn from(e: crate::net::peer::ConnectError) -> Self {
		use crate::net::peer::ConnectError;
		match e {
			ConnectError::ConnectionFailed => CliError::PeerConnectionFailed,
			ConnectError::ConnectionClosed => CliError::ConnectionClosed,
		}
	}
}
