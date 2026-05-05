//! Crate-wide LDK type aliases.
//!
//! LDK's generics produce deeply-nested, noise-heavy type signatures.
//! Historically these aliases lived in `main.rs`, which forced every
//! sibling module to `use crate::{ChannelManager, PeerManager, …}` and
//! made `main.rs` the de-facto "types" module. Moving them here lets
//! `main.rs` focus on orchestration and lets this file be the single
//! place to audit when an LDK upgrade changes a generic signature.

use std::sync::Arc;

use lightning::chain::chainmonitor;
use lightning::chain::Filter;
use lightning::events::bump_transaction::{BumpTransactionEventHandler, Wallet};
use lightning::ln::channelmanager::SimpleArcChannelManager;
use lightning::ln::peer_handler::{IgnoringMessageHandler, PeerManager as LdkPeerManager};
use lightning::onion_message::messenger::{
	DefaultMessageRouter, OnionMessenger as LdkOnionMessenger,
};
use lightning::routing::gossip;
use lightning::routing::gossip::P2PGossipSync;
use lightning::sign::{InMemorySigner, KeysManager};
use lightning::util::persist::MonitorUpdatingPersister;
use lightning::util::sweep as ldk_sweep;
use lightning_block_sync::gossip::TokioSpawner;
use lightning_dns_resolver::OMDomainResolver;
use lightning_net_tokio::SocketDescriptor;
use lightning_persister::fs_store::FilesystemStore;

use crate::bitcoind_client::BitcoindClient;
use crate::disk::FilesystemLogger;
use crate::persist::ScorerKeyRemappingStore;

pub(crate) type ChainMonitor = chainmonitor::ChainMonitor<
	InMemorySigner,
	Arc<dyn Filter + Send + Sync>,
	Arc<BitcoindClient>,
	Arc<BitcoindClient>,
	Arc<FilesystemLogger>,
	Arc<
		MonitorUpdatingPersister<
			Arc<FilesystemStore>,
			Arc<FilesystemLogger>,
			Arc<KeysManager>,
			Arc<KeysManager>,
			Arc<BitcoindClient>,
			Arc<BitcoindClient>,
		>,
	>,
	Arc<KeysManager>,
>;

pub(crate) type GossipVerifier = lightning_block_sync::gossip::GossipVerifier<
	TokioSpawner,
	Arc<lightning_block_sync::rpc::RpcClient>,
	Arc<FilesystemLogger>,
>;

// Note that if you do not use an `OMDomainResolver` here you should use SimpleArcPeerManager
// instead.
pub(crate) type PeerManager = LdkPeerManager<
	SocketDescriptor,
	Arc<ChannelManager>,
	Arc<P2PGossipSync<Arc<NetworkGraph>, Arc<GossipVerifier>, Arc<FilesystemLogger>>>,
	Arc<OnionMessenger>,
	Arc<FilesystemLogger>,
	IgnoringMessageHandler,
	Arc<KeysManager>,
	Arc<ChainMonitor>,
>;

pub(crate) type ChannelManager =
	SimpleArcChannelManager<ChainMonitor, BitcoindClient, BitcoindClient, FilesystemLogger>;

pub(crate) type NetworkGraph = gossip::NetworkGraph<Arc<FilesystemLogger>>;

// Note that if you do not use an `OMDomainResolver` here you should use SimpleArcOnionMessenger
// instead.
pub(crate) type OnionMessenger = LdkOnionMessenger<
	Arc<KeysManager>,
	Arc<KeysManager>,
	Arc<FilesystemLogger>,
	Arc<ChannelManager>,
	Arc<DefaultMessageRouter<Arc<NetworkGraph>, Arc<FilesystemLogger>, Arc<KeysManager>>>,
	Arc<ChannelManager>,
	Arc<ChannelManager>,
	Arc<OMDomainResolver<Arc<ChannelManager>>>,
	IgnoringMessageHandler,
>;

pub(crate) type BumpTxEventHandler = BumpTransactionEventHandler<
	Arc<BitcoindClient>,
	Arc<Wallet<Arc<BitcoindClient>, Arc<FilesystemLogger>>>,
	Arc<KeysManager>,
	Arc<FilesystemLogger>,
>;

pub(crate) type OutputSweeper = ldk_sweep::OutputSweeper<
	Arc<BitcoindClient>,
	Arc<BitcoindClient>,
	Arc<BitcoindClient>,
	Arc<dyn Filter + Send + Sync>,
	Arc<ScorerKeyRemappingStore>,
	Arc<FilesystemLogger>,
	Arc<KeysManager>,
>;
