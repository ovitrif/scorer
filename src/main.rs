#![allow(clippy::drop_non_drop)]
// Phase 0 guardrail: catches the most common async footgun — holding a
// std::sync::Mutex / RwLock guard across an `.await` point. This is the
// lint that makes Phase 4 (tokio::sync::Mutex for payments) a compile-time
// invariant rather than a careful-code convention.
#![deny(clippy::await_holding_lock)]

mod args;
pub mod bitcoind_client;
mod cli;
mod config;
mod convert;
mod disk;
mod dns_bootstrap;
mod events;
mod hex_utils;
#[macro_use]
mod logging;
mod net;
mod persist;
mod probing;
mod rapid_sync;
mod runtime_config;
mod state;
mod sweep;
mod types;

pub(crate) use disk::FilesystemLogger;
pub(crate) use state::{
	HTLCStatus, InboundPaymentInfoStorage, MillisatAmount, OutboundPaymentInfoStorage, PaymentInfo,
};
pub(crate) use types::{
	BumpTxEventHandler, ChainMonitor, ChannelManager, GossipVerifier, NetworkGraph, OnionMessenger,
	OutputSweeper, PeerManager,
};

use crate::bitcoind_client::BitcoindClient;
use crate::persist::{ScorerKeyRemappingStore, SCORER_PERSISTENCE_FILE_NAME};
use bitcoin::io;
use bitcoin::BlockHash;
use disk::{INBOUND_PAYMENTS_FNAME, OUTBOUND_PAYMENTS_FNAME};
use lightning::chain;
use lightning::chain::{chainmonitor, ChannelMonitorUpdateStatus};
use lightning::chain::BestBlock;
use lightning::events::bump_transaction::{BumpTransactionEventHandler, Wallet};
use lightning::events::Event;
use lightning::ln::channelmanager::{self, RecentPaymentDetails};
use lightning::ln::channelmanager::{ChainParameters, ChannelManagerReadArgs, PaymentId};
use lightning::ln::peer_handler::{IgnoringMessageHandler, MessageHandler};
use lightning::onion_message::messenger::DefaultMessageRouter;
use lightning::routing::gossip::NodeId;
use lightning::routing::gossip::P2PGossipSync;
use lightning::routing::router::DefaultRouter;
use lightning::routing::scoring::ProbabilisticScoringFeeParameters;
use lightning::sign::{KeysManager, NodeSigner};
use lightning::util::config::UserConfig;
use lightning::util::logger::Logger;
// `lightning::util::persist` is imported as `ldk_persist` because our
// local `crate::persist` module (home of `ScorerKeyRemappingStore`) shadows
// the upstream name in this file.
use lightning::util::persist as ldk_persist;
use lightning::util::persist::{
	KVStore, MonitorUpdatingPersister, OUTPUT_SWEEPER_PERSISTENCE_KEY,
	OUTPUT_SWEEPER_PERSISTENCE_PRIMARY_NAMESPACE, OUTPUT_SWEEPER_PERSISTENCE_SECONDARY_NAMESPACE,
};
use lightning::util::ser::{ReadableArgs, Writeable};
use lightning_background_processor::{process_events_async, GossipSync, NO_LIQUIDITY_MANAGER};
use lightning_block_sync::gossip::TokioSpawner;
use lightning_block_sync::{init, poll, BlockSourceErrorKind, SpvClient, UnboundedCache};
use lightning_dns_resolver::OMDomainResolver;
use lightning_persister::fs_store::FilesystemStore;
use rand::{thread_rng, Rng};
use std::convert::TryInto;
use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::net::ToSocketAddrs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime};

async fn start_ldk() {
	let args = match args::parse_startup_args() {
		Ok(user_args) => user_args,
		Err(args::StartupArgsError::MissingStorageDirectory) => {
			let argv0 = std::env::args().next().unwrap_or_else(|| "ldk-sample".to_string());
			println!("Usage: {} <ldk_storage_directory_path>", argv0);
			println!();
			println!(
				"The config.toml file should be located at <ldk_storage_directory_path>/.ldk/config.toml",
			);
			crate::config::print_config_help();
			return;
		},
		Err(args::StartupArgsError::Config(config_err)) => {
			println!("ERROR: {}", config_err);
			if matches!(
				config_err,
				config::ConfigError::FileNotFound(_)
					| config::ConfigError::DeprecatedJsonConfig(_)
					| config::ConfigError::ParseError(_)
			) {
				println!();
				crate::config::print_config_help();
			}
			return;
		},
		Err(e) => {
			println!("ERROR: {}", e);
			return;
		},
	};

	// Initialize the LDK data directory if necessary.
	let ldk_data_dir = format!("{}/.ldk", args.ldk_storage_dir_path);
	if let Err(e) = fs::create_dir_all(ldk_data_dir.clone()) {
		println!("ERROR: Failed to create LDK data directory {}: {}", ldk_data_dir, e);
		return;
	}

	// ## Setup
	// Step 1: Initialize the Logger
	//
	// This is the earliest thing that can fail *after* the data directory
	// exists, and the error path here is the ONLY one that is allowed to
	// `println!` an error message, because no logger is available yet to
	// receive it. All subsequent bootstrap errors route through the logger
	// as well as stdout via the `user_err!` macro (see `src/logging.rs`).
	let logger = match FilesystemLogger::try_new(ldk_data_dir.clone()) {
		Ok(l) => Arc::new(l),
		Err(e) => {
			println!(
				"ERROR: Failed to initialise filesystem logger at {}/logs: {}",
				ldk_data_dir, e
			);
			return;
		},
	};

	// Initialize our bitcoind client.
	let bitcoind_client = match BitcoindClient::new(
		args.bitcoind_rpc_host.clone(),
		args.bitcoind_rpc_port,
		args.bitcoind_rpc_username.clone(),
		args.bitcoind_rpc_password.clone(),
		args.network,
		tokio::runtime::Handle::current(),
		Arc::clone(&logger),
	)
	.await
	{
		Ok(client) => Arc::new(client),
		Err(e) => {
			user_err!(&*logger, "Failed to connect to bitcoind client: {}", e);
			return;
		},
	};

	// Check that the bitcoind we've connected to is running the network we expect
	let bitcoind_chain = match bitcoind_client.get_blockchain_info().await {
		Ok(info) => info.chain,
		Err(e) => {
			user_err!(&*logger, "Failed to fetch bitcoind chain info: {}", e);
			return;
		},
	};
	if bitcoind_chain
		!= match args.network {
			bitcoin::Network::Bitcoin => "main",
			bitcoin::Network::Regtest => "regtest",
			bitcoin::Network::Signet => "signet",
			bitcoin::Network::Testnet => "test",
			_ => "test",
		} {
		user_err!(
			&*logger,
			"Chain argument ({}) didn't match bitcoind chain ({})",
			args.network,
			bitcoind_chain
		);
		return;
	}

	// Step 2: Initialize the FeeEstimator

	// BitcoindClient implements the FeeEstimator trait, so it'll act as our fee estimator.
	let fee_estimator = bitcoind_client.clone();

	// Step 3: Initialize the BroadcasterInterface

	// BitcoindClient implements the BroadcasterInterface trait, so it'll act as our transaction
	// broadcaster.
	let broadcaster = bitcoind_client.clone();

	// Step 4: Initialize the KeysManager

	// The key seed that we use to derive the node privkey (that corresponds to the node pubkey) and
	// other secret key material.
	let keys_seed_path = format!("{}/keys_seed", ldk_data_dir.clone());
	let keys_seed = if let Ok(seed) = fs::read(keys_seed_path.clone()) {
		if seed.len() != 32 {
			user_err!(
				&*logger,
				"Invalid keys seed length in {}. Expected 32 bytes, found {}.",
				keys_seed_path,
				seed.len()
			);
			return;
		}
		let mut key = [0; 32];
		key.copy_from_slice(&seed);
		key
	} else {
		let mut key = [0; 32];
		thread_rng().fill_bytes(&mut key);
		match File::create(keys_seed_path.clone()) {
			Ok(mut f) => {
				if let Err(e) = std::io::Write::write_all(&mut f, &key) {
					user_err!(&*logger, "Failed to write node keys seed to disk: {}", e);
					return;
				}
				if let Err(e) = f.sync_all() {
					user_err!(&*logger, "Failed to sync node keys seed to disk: {}", e);
					return;
				}
			},
			Err(e) => {
				user_err!(&*logger, "Unable to create keys seed file {}: {}", keys_seed_path, e);
				return;
			},
		}
		key
	};
	let cur = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
		Ok(d) => d,
		Err(e) => {
			user_err!(&*logger, "System clock appears to be before UNIX_EPOCH: {}", e);
			return;
		},
	};
	let keys_manager =
		Arc::new(KeysManager::new(&keys_seed, cur.as_secs(), cur.subsec_nanos(), true));

	let bump_tx_event_handler = Arc::new(BumpTransactionEventHandler::new(
		Arc::clone(&broadcaster),
		Arc::new(Wallet::new(Arc::clone(&bitcoind_client), Arc::clone(&logger))),
		Arc::clone(&keys_manager),
		Arc::clone(&logger),
	));

	// Step 5: Initialize Persistence
	let fs_store = Arc::new(FilesystemStore::new(ldk_data_dir.clone().into()));
	let scorer_store = Arc::new(ScorerKeyRemappingStore::new(Arc::clone(&fs_store)));
	let persister = Arc::new(MonitorUpdatingPersister::new(
		Arc::clone(&fs_store),
		Arc::clone(&logger),
		1000,
		Arc::clone(&keys_manager),
		Arc::clone(&keys_manager),
		Arc::clone(&bitcoind_client),
		Arc::clone(&bitcoind_client),
	));
	// Alternatively, you can use the `FilesystemStore` as a `Persist` directly, at the cost of
	// larger `ChannelMonitor` update writes (but no deletion or cleanup):
	//let persister = Arc::clone(&fs_store);

	// Step 6: Read ChannelMonitor state from disk
	let mut channelmonitors = match persister.read_all_channel_monitors_with_updates() {
		Ok(monitors) => monitors,
		Err(e) => {
			user_err!(&*logger, "Failed to read channel monitors from disk: {}", e);
			return;
		},
	};
	// If you are using the `FilesystemStore` as a `Persist` directly, use
	// `lightning::util::persist::read_channel_monitors` like this:
	// read_channel_monitors(Arc::clone(&persister), Arc::clone(&keys_manager), Arc::clone(&keys_manager)).unwrap();

	// Step 7: Initialize the ChainMonitor
	let chain_monitor: Arc<ChainMonitor> = Arc::new(chainmonitor::ChainMonitor::new(
		None,
		Arc::clone(&broadcaster),
		Arc::clone(&logger),
		Arc::clone(&fee_estimator),
		Arc::clone(&persister),
		Arc::clone(&keys_manager),
		keys_manager.get_peer_storage_key(),
	));

	// Step 8: Poll for the best chain tip, which may be used by the channel manager & spv client
	let polled_chain_tip = match init::validate_best_block_header(bitcoind_client.as_ref()).await {
		Ok(tip) => tip,
		Err(e) => {
			user_err!(&*logger, "Failed to fetch best block header and best block: {:?}", e);
			return;
		},
	};

	// Step 9: Initialize routing ProbabilisticScorer
	let network_graph_path = format!("{}/network_graph", ldk_data_dir.clone());
	let network_graph =
		Arc::new(disk::read_network(Path::new(&network_graph_path), args.network, logger.clone()));

	let scorer_path = format!("{}/{}", ldk_data_dir.clone(), SCORER_PERSISTENCE_FILE_NAME);
	let scorer = Arc::new(RwLock::new(disk::read_scorer(
		Path::new(&scorer_path),
		Arc::clone(&network_graph),
		Arc::clone(&logger),
	)));

	// Step 10: Create Routers
	let scoring_fee_params = ProbabilisticScoringFeeParameters {
		base_penalty_msat: 500_000,
		base_penalty_amount_multiplier_msat: 131_072 * 3,
		..Default::default()
	};
	let router = Arc::new(DefaultRouter::new(
		network_graph.clone(),
		logger.clone(),
		keys_manager.clone(),
		scorer.clone(),
		scoring_fee_params.clone(),
	));

	let message_router =
		Arc::new(DefaultMessageRouter::new(Arc::clone(&network_graph), Arc::clone(&keys_manager)));

	// Step 11: Initialize the ChannelManager
	let mut user_config = UserConfig::default();
	user_config.channel_handshake_limits.force_announced_channel_preference = false;
	user_config.channel_handshake_config.negotiate_anchors_zero_fee_htlc_tx = true;
	user_config.manually_accept_inbound_channels = true;
	let mut restarting_node = true;
	let (channel_manager_blockhash, channel_manager) = {
		if let Ok(f) = fs::File::open(format!("{}/manager", ldk_data_dir.clone())) {
			let mut channel_monitor_references = Vec::new();
			for (_, channel_monitor) in channelmonitors.iter() {
				channel_monitor_references.push(channel_monitor);
			}
			let read_args = ChannelManagerReadArgs::new(
				keys_manager.clone(),
				keys_manager.clone(),
				keys_manager.clone(),
				fee_estimator.clone(),
				chain_monitor.clone(),
				broadcaster.clone(),
				router,
				Arc::clone(&message_router),
				logger.clone(),
				user_config,
				channel_monitor_references,
			);
			match <(BlockHash, ChannelManager)>::read(&mut BufReader::new(f), read_args) {
				Ok(v) => v,
				Err(e) => {
					user_err!(&*logger, "Failed to deserialize channel manager: {:?}", e);
					return;
				},
			}
		} else {
			// We're starting a fresh node.
			restarting_node = false;

			let polled_best_block = polled_chain_tip.to_best_block();
			let polled_best_block_hash = polled_best_block.block_hash;
			let chain_params =
				ChainParameters { network: args.network, best_block: polled_best_block };
			let fresh_channel_manager = channelmanager::ChannelManager::new(
				fee_estimator.clone(),
				chain_monitor.clone(),
				broadcaster.clone(),
				router,
				Arc::clone(&message_router),
				logger.clone(),
				keys_manager.clone(),
				keys_manager.clone(),
				keys_manager.clone(),
				user_config,
				chain_params,
				cur.as_secs() as u32,
			);
			(polled_best_block_hash, fresh_channel_manager)
		}
	};

	// Step 12: Initialize the OutputSweeper.
	let (sweeper_best_block, output_sweeper) = match fs_store
		.read(
			OUTPUT_SWEEPER_PERSISTENCE_PRIMARY_NAMESPACE,
			OUTPUT_SWEEPER_PERSISTENCE_SECONDARY_NAMESPACE,
			OUTPUT_SWEEPER_PERSISTENCE_KEY,
		)
		.await
	{
		Err(e) if e.kind() == io::ErrorKind::NotFound => {
			let sweeper = OutputSweeper::new(
				channel_manager.current_best_block(),
				broadcaster.clone(),
				fee_estimator.clone(),
				None,
				keys_manager.clone(),
				bitcoind_client.clone(),
				scorer_store.clone(),
				logger.clone(),
			);
			(channel_manager.current_best_block(), sweeper)
		},
		Ok(mut bytes) => {
			let read_args = (
				broadcaster.clone(),
				fee_estimator.clone(),
				None,
				keys_manager.clone(),
				bitcoind_client.clone(),
				scorer_store.clone(),
				logger.clone(),
			);
			let mut reader = io::Cursor::new(&mut bytes);
			match <(BestBlock, OutputSweeper)>::read(&mut reader, read_args) {
				Ok(v) => v,
				Err(e) => {
					user_err!(&*logger, "Failed to deserialize OutputSweeper: {:?}", e);
					return;
				},
			}
		},
		Err(e) => {
			user_err!(&*logger, "Failed to read OutputSweeper: {}", e);
			return;
		},
	};

	// Step 13: Sync ChannelMonitors, ChannelManager and OutputSweeper to chain tip
	let mut chain_listener_channel_monitors = Vec::new();
	let mut cache = UnboundedCache::new();
	let chain_tip = if restarting_node {
		let mut chain_listeners = vec![
			(channel_manager_blockhash, &channel_manager as &(dyn chain::Listen + Send + Sync)),
			(sweeper_best_block.block_hash, &output_sweeper as &(dyn chain::Listen + Send + Sync)),
		];

		for (blockhash, channel_monitor) in channelmonitors.drain(..) {
			let funding_txo = channel_monitor.get_funding_txo();
			chain_listener_channel_monitors.push((
				blockhash,
				(channel_monitor, broadcaster.clone(), fee_estimator.clone(), logger.clone()),
				funding_txo,
			));
		}

		for monitor_listener_info in chain_listener_channel_monitors.iter_mut() {
			chain_listeners.push((
				monitor_listener_info.0,
				&monitor_listener_info.1 as &(dyn chain::Listen + Send + Sync),
			));
		}

		match init::synchronize_listeners(
			bitcoind_client.as_ref(),
			args.network,
			&mut cache,
			chain_listeners,
		)
		.await
		{
			Ok(tip) => tip,
			Err(e) => {
				user_err!(&*logger, "Failed to synchronize chain listeners: {:?}", e);
				return;
			},
		}
	} else {
		polled_chain_tip
	};

	// Step 14: Give ChannelMonitors to ChainMonitor
	for (_, (channel_monitor, _, _, _), _) in chain_listener_channel_monitors {
		let channel_id = channel_monitor.channel_id();
		// Note that this may not return `Completed` for ChannelMonitors which were last written by
		// a version of LDK prior to 0.1.
		if chain_monitor.load_existing_monitor(channel_id, channel_monitor)
			!= Ok(ChannelMonitorUpdateStatus::Completed)
		{
			user_err!(&*logger, "Failed to load existing monitor for channel {}", channel_id);
			return;
		}
	}

	// Step 15: Initialize RapidGossipSync if enabled
	let rapid_sync_manager = if args.rapid_gossip_sync_enabled {
		lightning::log_info!(
			&*logger,
			"Initializing RapidGossipSync with URL: {}",
			args.rapid_gossip_sync_url.as_ref().unwrap_or(&"default".to_string())
		);

		match rapid_sync::RapidGossipSyncManager::new(
			Arc::clone(&network_graph),
			args.rapid_gossip_sync_url.clone(),
			Arc::clone(&logger),
			ldk_data_dir.clone(),
		)
		.await
		{
			Ok(manager) => {
				lightning::log_info!(&*logger, "RapidGossipSync initialized successfully");
				Some(Arc::new(tokio::sync::Mutex::new(manager)))
			},
			Err(e) => {
				lightning::log_error!(
					&*logger,
					"Failed to initialize RapidGossipSync: {:?}. Continuing with P2P sync only.",
					e
				);
				None
			},
		}
	} else {
		lightning::log_info!(&*logger, "RapidGossipSync disabled by configuration");
		None
	};

	// Step 15.5: Initialize the P2PGossipSync
	let gossip_sync =
		Arc::new(P2PGossipSync::new(Arc::clone(&network_graph), None, Arc::clone(&logger)));

	// Step 16 an OMDomainResolver as a service to other nodes
	// As a service to other LDK users, using an `OMDomainResolver` allows others to resolve BIP
	// 353 Human Readable Names for others, providing them DNSSEC proofs over lightning onion
	// messages. Doing this only makes sense for a always-online public routing node, and doesn't
	// provide you any direct value, but its nice to offer the service for others.
	let channel_manager: Arc<ChannelManager> = Arc::new(channel_manager);
	let resolver = match "8.8.8.8:53".to_socket_addrs() {
		Ok(mut addrs) => match addrs.next() {
			Some(addr) => addr,
			None => {
				user_err!(&*logger, "Resolver address lookup returned no addresses");
				return;
			},
		},
		Err(e) => {
			user_err!(&*logger, "Failed to resolve default DNS resolver: {}", e);
			return;
		},
	};
	let domain_resolver =
		Arc::new(OMDomainResolver::new(resolver, Some(Arc::clone(&channel_manager))));

	// Step 17: Initialize the PeerManager
	let onion_messenger: Arc<OnionMessenger> = Arc::new(OnionMessenger::new(
		Arc::clone(&keys_manager),
		Arc::clone(&keys_manager),
		Arc::clone(&logger),
		Arc::clone(&channel_manager),
		Arc::clone(&message_router),
		Arc::clone(&channel_manager),
		Arc::clone(&channel_manager),
		domain_resolver,
		IgnoringMessageHandler {},
	));
	let mut ephemeral_bytes = [0; 32];
	let current_time = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
		Ok(d) => d.as_secs(),
		Err(e) => {
			user_err!(&*logger, "System clock appears to be before UNIX_EPOCH: {}", e);
			return;
		},
	};
	rand::thread_rng().fill_bytes(&mut ephemeral_bytes);
	let lightning_msg_handler = MessageHandler {
		chan_handler: Arc::clone(&channel_manager),
		route_handler: Arc::clone(&gossip_sync),
		onion_message_handler: Arc::clone(&onion_messenger),
		custom_message_handler: IgnoringMessageHandler {},
		send_only_message_handler: Arc::clone(&chain_monitor),
	};
	let peer_manager: Arc<PeerManager> = Arc::new(PeerManager::new(
		lightning_msg_handler,
		match current_time.try_into() {
			Ok(ts) => ts,
			Err(_) => {
				user_err!(&*logger, "Current timestamp does not fit expected peer-manager type");
				return;
			},
		},
		&ephemeral_bytes,
		logger.clone(),
		Arc::clone(&keys_manager),
	));

	// Install a GossipVerifier in in the P2PGossipSync
	let utxo_lookup = GossipVerifier::new(
		Arc::clone(&bitcoind_client.bitcoind_rpc_client),
		TokioSpawner,
		Arc::clone(&gossip_sync),
		Arc::clone(&peer_manager),
	);
	gossip_sync.add_utxo_lookup(Some(Arc::new(utxo_lookup)));

	// ## Running LDK
	// Step 18: Initialize networking

	let peer_manager_connection_handler = peer_manager.clone();
	let listening_port = args.ldk_peer_listening_port;
	let stop_listen_connect = Arc::new(AtomicBool::new(false));
	let stop_listen = Arc::clone(&stop_listen_connect);
	let listen_logger = Arc::clone(&logger);
	tokio::spawn(async move {
		let listener = match tokio::net::TcpListener::bind(format!("[::]:{}", listening_port)).await
		{
			Ok(listener) => listener,
			Err(e) => {
				// Listener bind failures are fatal for inbound connectivity;
				// surface via the dual-output macro so operators see the
				// message whether they run with or without a terminal.
				user_err!(
					&*listen_logger,
					"Failed to bind to listen port {} - is something else already listening on it? {}",
					listening_port,
					e
				);
				return;
			},
		};
		loop {
			let peer_mgr = peer_manager_connection_handler.clone();
			let (tcp_stream, _) = match listener.accept().await {
				Ok(conn) => conn,
				Err(e) => {
					lightning::log_warn!(
						&*listen_logger,
						"Failed to accept inbound connection: {}",
						e
					);
					continue;
				},
			};
			if stop_listen.load(Ordering::Acquire) {
				return;
			}
			let conn_logger = Arc::clone(&listen_logger);
			tokio::spawn(async move {
				match tcp_stream.into_std() {
					Ok(std_stream) => {
						lightning_net_tokio::setup_inbound(peer_mgr.clone(), std_stream).await;
					},
					Err(e) => {
						lightning::log_warn!(
							&*conn_logger,
							"Failed to convert inbound TCP stream: {}",
							e
						);
					},
				}
			});
		}
	});

	// Step 19: Connect and Disconnect Blocks
	let output_sweeper: Arc<OutputSweeper> = Arc::new(output_sweeper);
	let channel_manager_listener = channel_manager.clone();
	let chain_monitor_listener = chain_monitor.clone();
	let output_sweeper_listener = output_sweeper.clone();
	let bitcoind_block_source = bitcoind_client.clone();
	let network = args.network;
	let block_poll_logger = Arc::clone(&logger);
	tokio::spawn(async move {
		let chain_poller = poll::ChainPoller::new(bitcoind_block_source.as_ref(), network);
		let chain_listener =
			(chain_monitor_listener, &(channel_manager_listener, output_sweeper_listener));
		let mut spv_client = SpvClient::new(chain_tip, chain_poller, &mut cache, &chain_listener);
		let mut retry_delay = Duration::from_secs(1);
		let max_retry_delay = Duration::from_secs(30);
		loop {
			match spv_client.poll_best_tip().await {
				Ok(_) => {
					retry_delay = Duration::from_secs(1);
					tokio::time::sleep(Duration::from_secs(1)).await;
				},
				Err(e) => {
					match e.kind() {
						BlockSourceErrorKind::Transient => lightning::log_warn!(
							&*block_poll_logger,
							"SPV poll_best_tip transient error: {:?}. Retrying in {}s",
							e,
							retry_delay.as_secs()
						),
						BlockSourceErrorKind::Persistent => lightning::log_error!(
							&*block_poll_logger,
							"SPV poll_best_tip persistent error: {:?}. Retrying in {}s",
							e,
							retry_delay.as_secs()
						),
					}
					tokio::time::sleep(retry_delay).await;
					retry_delay = std::cmp::min(retry_delay + retry_delay, max_retry_delay);
				},
			}
		}
	});

	// Phase 4: payment storage now uses `tokio::sync::Mutex`. The lock
	// helper returns a guard directly (not a Result), because a
	// tokio-mutex cannot be poisoned — there's no panic-propagation path
	// through an `.await` in the way there is for `std::sync::Mutex`.
	let inbound_payments = Arc::new(tokio::sync::Mutex::new(disk::read_inbound_payment_info(
		Path::new(&format!("{}/{}", ldk_data_dir, INBOUND_PAYMENTS_FNAME)),
		&logger,
	)));
	let outbound_payments = Arc::new(tokio::sync::Mutex::new(disk::read_outbound_payment_info(
		Path::new(&format!("{}/{}", ldk_data_dir, OUTBOUND_PAYMENTS_FNAME)),
		&logger,
	)));
	let recent_payments_payment_ids = channel_manager
		.list_recent_payments()
		.into_iter()
		.map(|p| match p {
			RecentPaymentDetails::Pending { payment_id, .. } => payment_id,
			RecentPaymentDetails::Fulfilled { payment_id, .. } => payment_id,
			RecentPaymentDetails::Abandoned { payment_id, .. } => payment_id,
			RecentPaymentDetails::AwaitingInvoice { payment_id } => payment_id,
		})
		.collect::<Vec<PaymentId>>();
	{
		let mut outbound_payments_lock = outbound_payments.lock().await;
		for (payment_id, payment_info) in outbound_payments_lock
			.payments
			.iter_mut()
			.filter(|(_, i)| matches!(i.status, HTLCStatus::Pending))
		{
			if !recent_payments_payment_ids.contains(payment_id) {
				payment_info.status = HTLCStatus::Failed;
			}
		}
	}
	let outbound_payments_bytes = {
		let outbound_payments_lock = outbound_payments.lock().await;
		outbound_payments_lock.encode()
	};
	if let Err(e) = fs_store.write("", "", OUTBOUND_PAYMENTS_FNAME, outbound_payments_bytes).await {
		user_err!(&*logger, "Failed to persist outbound payments: {}", e);
		return;
	}

	// Step 20: Handle LDK Events
	let probe_tracker = Arc::new(Mutex::new(probing::ProbeTracker::new()));
	let event_context = Arc::new(events::EventContext {
		channel_manager: Arc::clone(&channel_manager),
		bitcoind_client: Arc::clone(&bitcoind_client),
		network_graph: Arc::clone(&network_graph),
		keys_manager: Arc::clone(&keys_manager),
		bump_tx_event_handler: Arc::clone(&bump_tx_event_handler),
		peer_manager: Arc::clone(&peer_manager),
		inbound_payments: Arc::clone(&inbound_payments),
		outbound_payments: Arc::clone(&outbound_payments),
		fs_store: Arc::clone(&fs_store),
		output_sweeper: Arc::clone(&output_sweeper),
		network: args.network,
		probe_tracker: Arc::clone(&probe_tracker),
		logger: Arc::clone(&logger),
	});
	let event_handler = move |event: Event| {
		let event_context = Arc::clone(&event_context);
		async move {
			events::handle_ldk_events(event_context, event).await;
			Ok(())
		}
	};

	// Step 21: Background Processing
	let (bp_exit, bp_exit_check) = tokio::sync::watch::channel(());
	let mut background_processor = tokio::spawn(process_events_async(
		Arc::clone(&scorer_store),
		event_handler,
		Arc::clone(&chain_monitor),
		Arc::clone(&channel_manager),
		Some(onion_messenger),
		GossipSync::p2p(Arc::clone(&gossip_sync)),
		Arc::clone(&peer_manager),
		NO_LIQUIDITY_MANAGER,
		Some(Arc::clone(&output_sweeper)),
		Arc::clone(&logger),
		Some(Arc::clone(&scorer)),
		move |t| {
			let mut bp_exit_fut_check = bp_exit_check.clone();
			Box::pin(async move {
				tokio::select! {
					_ = tokio::time::sleep(t) => false,
					_ = bp_exit_fut_check.changed() => true,
				}
			})
		},
		false,
		|| SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).ok(),
	));

	// Regularly reconnect to channel peers.
	let connect_cm = Arc::clone(&channel_manager);
	let connect_pm = Arc::clone(&peer_manager);
	let stop_connect = Arc::clone(&stop_listen_connect);
	let graph_connect = Arc::clone(&network_graph);
	tokio::spawn(async move {
		let mut interval = tokio::time::interval(Duration::from_secs(1));
		interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
		loop {
			interval.tick().await;
			for node_id in connect_cm
				.list_channels()
				.iter()
				.map(|chan| chan.counterparty.node_id)
				.filter(|id| connect_pm.peer_by_node_id(id).is_none())
			{
				if stop_connect.load(Ordering::Acquire) {
					return;
				}
				let id = NodeId::from_pubkey(&node_id);
				let addrs = if let Some(node) = graph_connect.read_only().node(&id) {
					if let Some(ann) = &node.announcement_info {
						let non_onion = |addr: &lightning::ln::msgs::SocketAddress| match addr {
							lightning::ln::msgs::SocketAddress::OnionV2(_) => None,
							lightning::ln::msgs::SocketAddress::OnionV3 { .. } => None,
							_ => Some(addr.clone()),
						};
						ann.addresses().iter().filter_map(non_onion).collect::<Vec<_>>()
					} else {
						Vec::new()
					}
				} else {
					Vec::new()
				};
				for addr in addrs {
					let sockaddrs = match addr.to_socket_addrs() {
						Ok(addrs) => addrs,
						Err(_) => continue,
					};
					for sockaddr in sockaddrs {
						let _ =
							cli::do_connect_peer(node_id, sockaddr, Arc::clone(&connect_pm)).await;
					}
				}
			}
		}
	});

	// Capture values before moving args
	let rapid_gossip_sync_interval_hours = args.rapid_gossip_sync_interval_hours;
	let probing_config = args.probing.clone();
	let dns_bootstrap_config = args.dns_bootstrap.clone();

	// DNS bootstrap: discover peers from BOLT-0010 DNS seeds.
	if let Some(dns_config) = dns_bootstrap_config {
		if dns_config.enabled {
			match dns_bootstrap::DnsBootstrapper::new(dns_config) {
				Ok(bootstrapper) => {
					let bootstrap_pm = Arc::clone(&peer_manager);
					let bootstrap_logger = Arc::clone(&logger);
					let stop_bootstrap = Arc::clone(&stop_listen_connect);
					let interval_secs = bootstrapper.interval_secs();
					let num_peers = bootstrapper.num_peers();

					lightning::log_info!(
						&*logger,
						"DNS bootstrap enabled: {} seeds, target {} peers, interval {}s",
						bootstrapper.config().seeds.len(),
						num_peers,
						interval_secs,
					);

					tokio::spawn(async move {
						// Initial delay to let networking and gossip start.
						tokio::time::sleep(Duration::from_secs(5)).await;

						let mut interval =
							tokio::time::interval(Duration::from_secs(interval_secs));
						loop {
							interval.tick().await;

							if stop_bootstrap.load(Ordering::Acquire) {
								return;
							}

							// Build ignore set from currently connected peers.
							let ignore: std::collections::HashSet<
								lightning::routing::gossip::NodeId,
							> = bootstrap_pm
								.list_peers()
								.iter()
								.map(|p| {
									lightning::routing::gossip::NodeId::from_pubkey(
										&p.counterparty_node_id,
									)
								})
								.collect();

							match bootstrapper
								.sample_node_addrs(num_peers, &ignore, &*bootstrap_logger)
								.await
							{
								Ok(peers) => {
									lightning::log_info!(
										&*bootstrap_logger,
										"[dns_bootstrap] Discovered {} peers",
										peers.len()
									);
									for peer in peers {
										let _ = cli::do_connect_peer(
											peer.pubkey,
											peer.addr,
											Arc::clone(&bootstrap_pm),
										)
										.await;
									}
								},
								Err(e) => {
									lightning::log_warn!(
										&*bootstrap_logger,
										"[dns_bootstrap] Bootstrap failed: {}",
										e
									);
								},
							}
						}
					});
				},
				Err(e) => {
					lightning::log_error!(
						&*logger,
						"[dns_bootstrap] Failed to initialize bootstrapper: {}",
						e
					);
				},
			}
		}
	}

	// Regularly broadcast our node_announcement. This is only required (or possible) if we have
	// some public channels.
	let peer_man = Arc::clone(&peer_manager);
	let chan_man = Arc::clone(&channel_manager);
	tokio::spawn(async move {
		// First wait a minute until we have some peers and maybe have opened a channel.
		tokio::time::sleep(Duration::from_secs(60)).await;
		// Then, update our announcement once an hour to keep it fresh but avoid unnecessary churn
		// in the global gossip network.
		let mut interval = tokio::time::interval(Duration::from_secs(3600));
		loop {
			interval.tick().await;
			// Don't bother trying to announce if we don't have any public channls, though our
			// peers should drop such an announcement anyway. Note that announcement may not
			// propagate until we have a channel with 6+ confirmations.
			if chan_man.list_channels().iter().any(|chan| chan.is_announced) {
				peer_man.broadcast_node_announcement(
					[0; 3],
					args.ldk_announced_node_name,
					args.ldk_announced_listen_addr.clone(),
				);
			}
		}
	});

	tokio::spawn(sweep::migrate_deprecated_spendable_outputs(
		ldk_data_dir.clone(),
		Arc::clone(&keys_manager),
		Arc::clone(&logger),
		Arc::clone(&fs_store),
		Arc::clone(&output_sweeper),
	));

	// Regularly probe (if probing config is present)
	if let Some(probe_config) = probing_config {
		probing::spawn_probing_loop(
			probe_config,
			probing::ProbingDeps {
				channel_manager: Arc::clone(&channel_manager),
				network_graph: Arc::clone(&network_graph),
				logger: Arc::clone(&logger),
				scorer: Arc::clone(&scorer),
				scoring_fee_params,
				tracker: Arc::clone(&probe_tracker),
			},
		);
	}

	// Start periodic rapid gossip sync updates
	if let Some(rapid_sync) = rapid_sync_manager {
		let rapid_sync_interval = Duration::from_secs(rapid_gossip_sync_interval_hours * 3600);
		let rapid_sync_logger = Arc::clone(&logger);

		tokio::spawn(async move {
			let mut interval = tokio::time::interval(rapid_sync_interval);
			interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

			// Skip the first tick since we just synced on startup
			interval.tick().await;

			loop {
				interval.tick().await;

				lightning::log_info!(
					&*rapid_sync_logger,
					"Starting periodic rapid gossip sync update"
				);

				let mut sync_manager = rapid_sync.lock().await;
				match sync_manager.sync_network_graph().await {
					Ok(new_timestamp) => {
						lightning::log_info!(
							&*rapid_sync_logger,
							"Periodic rapid gossip sync completed successfully. New timestamp: {}",
							new_timestamp
						);
					},
					Err(e) => {
						lightning::log_error!(
							&*rapid_sync_logger,
							"Periodic rapid gossip sync failed: {:?}",
							e
						);
					},
				}
			}
		});
	}

	// Start the CLI.
	let cli_channel_manager = Arc::clone(&channel_manager);
	let cli_chain_monitor = Arc::clone(&chain_monitor);
	let cli_fs_store = Arc::clone(&fs_store);
	let cli_peer_manager = Arc::clone(&peer_manager);
	let cli_output_sweeper = Arc::clone(&output_sweeper);
	let cli_logger = Arc::clone(&logger);
	let cli_poll = tokio::task::spawn(cli::poll_for_user_input(cli::CliRuntime {
		peer_manager: cli_peer_manager,
		channel_manager: cli_channel_manager,
		chain_monitor: cli_chain_monitor,
		keys_manager,
		network_graph,
		inbound_payments,
		outbound_payments,
		output_sweeper: cli_output_sweeper,
		fs_store: cli_fs_store,
		logger: cli_logger,
	}));

	// Exit if either CLI polling exits or the background processor exits (which shouldn't happen
	// unless we fail to write to the filesystem).
	let mut bg_res = Ok(Ok(()));
	tokio::select! {
		_ = cli_poll => {},
		bg_exit = &mut background_processor => {
			bg_res = bg_exit;
		},
	}

	// Disconnect our peers and stop accepting new connections. This ensures we don't continue
	// updating our channel data after we've stopped the background processor.
	stop_listen_connect.store(true, Ordering::Release);
	peer_manager.disconnect_all_peers();

	if let Err(e) = bg_res {
		if let Err(write_err) = fs_store
			.write(
				ldk_persist::CHANNEL_MANAGER_PERSISTENCE_PRIMARY_NAMESPACE,
				ldk_persist::CHANNEL_MANAGER_PERSISTENCE_SECONDARY_NAMESPACE,
				ldk_persist::CHANNEL_MANAGER_PERSISTENCE_KEY,
				channel_manager.encode(),
			)
			.await
		{
			lightning::log_error!(
				&*logger,
				"Last-ditch ChannelManager persistence failed: {}",
				write_err
			);
		} else {
			lightning::log_error!(&*logger, "Last-ditch ChannelManager persistence completed");
		}
		lightning::log_error!(&*logger, "ERR: background processing stopped: {:?}", e);
		return;
	}

	// Stop the background processor.
	if !bp_exit.is_closed() {
		if let Err(e) = bp_exit.send(()) {
			lightning::log_warn!(&*logger, "Failed to signal background processor shutdown: {}", e);
		}
		match background_processor.await {
			Ok(Ok(())) => {},
			Ok(Err(e)) => {
				lightning::log_error!(&*logger, "Background processor shutdown failed: {:?}", e);
			},
			Err(e) => {
				lightning::log_error!(&*logger, "Background processor task join failed: {}", e);
			},
		}
	}
}

#[tokio::main]
pub async fn main() {
	#[cfg(not(target_os = "windows"))]
	{
		// Catch Ctrl-C with a dummy signal handler.
		unsafe {
			let mut new_action: libc::sigaction = core::mem::zeroed();
			let mut old_action: libc::sigaction = core::mem::zeroed();

			extern "C" fn dummy_handler(
				_: libc::c_int, _: *const libc::siginfo_t, _: *const libc::c_void,
			) {
			}

			new_action.sa_sigaction = dummy_handler as *const () as libc::sighandler_t;
			new_action.sa_flags = libc::SA_SIGINFO;

			libc::sigaction(
				libc::SIGINT,
				&new_action as *const libc::sigaction,
				&mut old_action as *mut libc::sigaction,
			);
		}
	}

	start_ldk().await;
}
