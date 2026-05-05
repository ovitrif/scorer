use crate::convert::{
	BlockchainInfo, FeeResponse, FundedTx, ListUnspentResponse, MempoolMinFeeResponse, NewAddress,
	RawTx, SignedTx,
};
use crate::disk::FilesystemLogger;
use crate::hex_utils;
use base64;
use bitcoin::address::Address;
use bitcoin::blockdata::constants::WITNESS_SCALE_FACTOR;
use bitcoin::blockdata::script::ScriptBuf;
use bitcoin::blockdata::transaction::Transaction;
use bitcoin::consensus::{encode, Decodable, Encodable};
use bitcoin::hash_types::{BlockHash, Txid};
use bitcoin::hashes::Hash;
use bitcoin::key::XOnlyPublicKey;
use bitcoin::psbt::Psbt;
use bitcoin::{Network, OutPoint, TxOut, WPubkeyHash};
use lightning::chain::chaininterface::{BroadcasterInterface, ConfirmationTarget, FeeEstimator};
use lightning::events::bump_transaction::{Utxo, WalletSource};
use lightning::log_error;
use lightning::sign::ChangeDestinationSource;
use lightning::util::async_poll::AsyncResult;
use lightning::util::logger::Logger;
use lightning_block_sync::http::HttpEndpoint;
use lightning_block_sync::rpc::RpcClient;
use lightning_block_sync::{AsyncBlockSourceResult, BlockData, BlockHeaderData, BlockSource};
use serde_json;
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::runtime::Handle;

pub struct BitcoindClient {
	pub(crate) bitcoind_rpc_client: Arc<RpcClient>,
	network: Network,
	host: String,
	port: u16,
	rpc_user: String,
	rpc_password: String,
	fees: Arc<HashMap<ConfirmationTarget, AtomicU32>>,
	main_runtime_handle: Handle,
	logger: Arc<FilesystemLogger>,
}

impl BlockSource for BitcoindClient {
	fn get_header<'a>(
		&'a self, header_hash: &'a BlockHash, height_hint: Option<u32>,
	) -> AsyncBlockSourceResult<'a, BlockHeaderData> {
		Box::pin(async move { self.bitcoind_rpc_client.get_header(header_hash, height_hint).await })
	}

	fn get_block<'a>(
		&'a self, header_hash: &'a BlockHash,
	) -> AsyncBlockSourceResult<'a, BlockData> {
		Box::pin(async move { self.bitcoind_rpc_client.get_block(header_hash).await })
	}

	fn get_best_block(&self) -> AsyncBlockSourceResult<'_, (BlockHash, Option<u32>)> {
		Box::pin(async move { self.bitcoind_rpc_client.get_best_block().await })
	}
}

/// The minimum feerate we are allowed to send, as specify by LDK.
const MIN_FEERATE: u32 = 253;

/// Helper used by [`BitcoindClient::poll_for_fee_estimates`] to call
/// `estimatesmartfee` for one confirmation target, enforce the
/// `MIN_FEERATE` floor, fall back to `fallback_when_none` when bitcoind
/// returns no estimate, and preserve the previous stored value when the
/// RPC itself fails.
///
/// Factored out to remove ~140 lines of near-identical copy-paste.
#[allow(clippy::too_many_arguments)]
async fn estimate_smartfee<F>(
	rpc_client: &RpcClient, logger: &FilesystemLogger, conf_target_blocks: u32, mode: &str,
	fallback_when_none: u32, fallback_target_for_previous: ConfirmationTarget, label: &str,
	load_current: &F,
) -> u32
where
	F: Fn(ConfirmationTarget) -> u32,
{
	let conf_target = serde_json::json!(conf_target_blocks);
	let estimate_mode = serde_json::json!(mode);
	match rpc_client
		.call_method::<FeeResponse>("estimatesmartfee", &[conf_target, estimate_mode])
		.await
	{
		Ok(resp) => match resp.feerate_sat_per_kw {
			Some(feerate) => std::cmp::max(feerate, MIN_FEERATE),
			None => fallback_when_none,
		},
		Err(e) => {
			lightning::log_warn!(
				logger,
				"Fee polling {} estimatesmartfee failed: {}. Keeping previous value.",
				label,
				e
			);
			load_current(fallback_target_for_previous)
		},
	}
}

impl BitcoindClient {
	pub(crate) async fn new(
		host: String, port: u16, rpc_user: String, rpc_password: String, network: Network,
		handle: Handle, logger: Arc<FilesystemLogger>,
	) -> std::io::Result<Self> {
		let http_endpoint = HttpEndpoint::for_host(host.clone()).with_port(port);
		let rpc_credentials =
			base64::encode(format!("{}:{}", rpc_user.clone(), rpc_password.clone()));
		let bitcoind_rpc_client = RpcClient::new(&rpc_credentials, http_endpoint);
		let _dummy = bitcoind_rpc_client
			.call_method::<BlockchainInfo>("getblockchaininfo", &[])
			.await
			.map_err(|_| {
				std::io::Error::new(std::io::ErrorKind::PermissionDenied,
				"Failed to make initial call to bitcoind - please check your RPC user/password and access settings")
			})?;
		let mut fees: HashMap<ConfirmationTarget, AtomicU32> = HashMap::new();
		fees.insert(ConfirmationTarget::MaximumFeeEstimate, AtomicU32::new(50000));
		fees.insert(ConfirmationTarget::UrgentOnChainSweep, AtomicU32::new(5000));
		fees.insert(
			ConfirmationTarget::MinAllowedAnchorChannelRemoteFee,
			AtomicU32::new(MIN_FEERATE),
		);
		fees.insert(
			ConfirmationTarget::MinAllowedNonAnchorChannelRemoteFee,
			AtomicU32::new(MIN_FEERATE),
		);
		fees.insert(ConfirmationTarget::AnchorChannelFee, AtomicU32::new(MIN_FEERATE));
		fees.insert(ConfirmationTarget::NonAnchorChannelFee, AtomicU32::new(2000));
		fees.insert(ConfirmationTarget::ChannelCloseMinimum, AtomicU32::new(MIN_FEERATE));
		fees.insert(ConfirmationTarget::OutputSpendingFee, AtomicU32::new(MIN_FEERATE));

		let client = Self {
			bitcoind_rpc_client: Arc::new(bitcoind_rpc_client),
			host,
			port,
			rpc_user,
			rpc_password,
			network,
			fees: Arc::new(fees),
			main_runtime_handle: handle.clone(),
			logger,
		};
		BitcoindClient::poll_for_fee_estimates(
			client.fees.clone(),
			client.bitcoind_rpc_client.clone(),
			handle,
			client.logger.clone(),
		);
		Ok(client)
	}

	fn poll_for_fee_estimates(
		fees: Arc<HashMap<ConfirmationTarget, AtomicU32>>, rpc_client: Arc<RpcClient>,
		handle: Handle, logger: Arc<FilesystemLogger>,
	) {
		handle.spawn(async move {
			loop {
				let load_current = |target: ConfirmationTarget| {
					if let Some(value) = fees.get(&target) {
						value.load(Ordering::Acquire)
					} else {
						lightning::log_error!(
							&*logger,
							"Missing fee bucket for {:?}, defaulting to MIN_FEERATE",
							target
						);
						MIN_FEERATE
					}
				};
				let store_fee = |target: ConfirmationTarget, value: u32| {
					if let Some(slot) = fees.get(&target) {
						slot.store(value, Ordering::Release);
					} else {
						lightning::log_error!(&*logger, "Missing fee bucket for {:?}", target);
					}
				};

				// `getmempoolinfo` is shaped differently from
				// `estimatesmartfee` (different RPC method, different
				// response type), so it's the one call that cannot go
				// through `estimate_smartfee`.
				let mempoolmin_estimate = match rpc_client
					.call_method::<MempoolMinFeeResponse>("getmempoolinfo", &[])
					.await
				{
					Ok(resp) => resp.feerate_sat_per_kw.map(|f| f.max(MIN_FEERATE)).unwrap_or(MIN_FEERATE),
					Err(e) => {
						lightning::log_warn!(
							&*logger,
							"Fee polling getmempoolinfo failed: {}. Keeping previous value.",
							e
						);
						load_current(ConfirmationTarget::MinAllowedAnchorChannelRemoteFee)
					},
				};

				// Each row:  (conf_target_blocks, mode, fallback_when_rpc_returns_none,
				//             fallback_target_for_previous_value, human_label)
				//
				// Previously this was ~140 lines of 5 near-identical
				// `match rpc_client.call_method::<FeeResponse>(...)` blocks.
				// Collapsing to one helper kept every semantic: the
				// minimum-floor at MIN_FEERATE, the per-target fallback
				// default, and the per-target "keep previous value on RPC
				// error" behaviour (via `load_current`).
				let background_estimate = estimate_smartfee(
					&rpc_client,
					&logger,
					144,
					"ECONOMICAL",
					MIN_FEERATE,
					ConfirmationTarget::AnchorChannelFee,
					"background",
					&load_current,
				)
				.await;
				let normal_estimate = estimate_smartfee(
					&rpc_client,
					&logger,
					18,
					"ECONOMICAL",
					2000,
					ConfirmationTarget::NonAnchorChannelFee,
					"normal",
					&load_current,
				)
				.await;
				let high_prio_estimate = estimate_smartfee(
					&rpc_client,
					&logger,
					6,
					"CONSERVATIVE",
					5000,
					ConfirmationTarget::UrgentOnChainSweep,
					"high-priority",
					&load_current,
				)
				.await;
				let very_high_prio_estimate = estimate_smartfee(
					&rpc_client,
					&logger,
					2,
					"CONSERVATIVE",
					50000,
					ConfirmationTarget::MaximumFeeEstimate,
					"very-high-priority",
					&load_current,
				)
				.await;

				store_fee(ConfirmationTarget::MaximumFeeEstimate, very_high_prio_estimate);
				store_fee(ConfirmationTarget::UrgentOnChainSweep, high_prio_estimate);
				store_fee(
					ConfirmationTarget::MinAllowedAnchorChannelRemoteFee,
					mempoolmin_estimate,
				);
				store_fee(
					ConfirmationTarget::MinAllowedNonAnchorChannelRemoteFee,
					background_estimate.saturating_sub(250),
				);
				store_fee(ConfirmationTarget::AnchorChannelFee, background_estimate);
				store_fee(ConfirmationTarget::NonAnchorChannelFee, normal_estimate);
				store_fee(ConfirmationTarget::ChannelCloseMinimum, background_estimate);
				store_fee(ConfirmationTarget::OutputSpendingFee, background_estimate);

				tokio::time::sleep(Duration::from_secs(60)).await;
			}
		});
	}

	pub fn get_new_rpc_client(&self) -> RpcClient {
		let http_endpoint = HttpEndpoint::for_host(self.host.clone()).with_port(self.port);
		let rpc_credentials = base64::encode(format!("{}:{}", self.rpc_user, self.rpc_password));
		RpcClient::new(&rpc_credentials, http_endpoint)
	}

	pub async fn create_raw_transaction(
		&self, outputs: Vec<HashMap<String, f64>>,
	) -> io::Result<RawTx> {
		let outputs_json = serde_json::json!(outputs);
		self.bitcoind_rpc_client
			.call_method::<RawTx>("createrawtransaction", &[serde_json::json!([]), outputs_json])
			.await
	}

	pub async fn fund_raw_transaction(&self, raw_tx: RawTx) -> io::Result<FundedTx> {
		let raw_tx_json = serde_json::json!(raw_tx.0);
		let options = serde_json::json!({
			// LDK gives us feerates in satoshis per KW but Bitcoin Core here expects fees
			// denominated in satoshis per vB. First we need to multiply by 4 to convert weight
			// units to virtual bytes, then divide by 1000 to convert KvB to vB.
			"fee_rate": self
				.get_est_sat_per_1000_weight(ConfirmationTarget::NonAnchorChannelFee) as f64 / 250.0,
			// While users could "cancel" a channel open by RBF-bumping and paying back to
			// themselves, we don't allow it here as its easy to have users accidentally RBF bump
			// and pay to the channel funding address, which results in loss of funds. Real
			// LDK-based applications should enable RBF bumping and RBF bump either to a local
			// change address or to a new channel output negotiated with the same node.
			"replaceable": false,
		});
		self.bitcoind_rpc_client.call_method("fundrawtransaction", &[raw_tx_json, options]).await
	}

	pub async fn send_raw_transaction(&self, raw_tx: RawTx) -> io::Result<()> {
		let raw_tx_json = serde_json::json!(raw_tx.0);
		self.bitcoind_rpc_client
			.call_method::<Txid>("sendrawtransaction", &[raw_tx_json])
			.await
			.map(|_| ())
	}

	pub fn sign_raw_transaction_with_wallet(
		&self, tx_hex: String,
	) -> impl Future<Output = io::Result<SignedTx>> {
		let tx_hex_json = serde_json::json!(tx_hex);
		let rpc_client = self.get_new_rpc_client();
		async move { rpc_client.call_method("signrawtransactionwithwallet", &[tx_hex_json]).await }
	}

	pub fn get_new_address(&self) -> impl Future<Output = io::Result<Address>> {
		let addr_args = [serde_json::json!("LDK output address")];
		let network = self.network;
		let rpc_client = self.get_new_rpc_client();
		async move {
			let addr = rpc_client.call_method::<NewAddress>("getnewaddress", &addr_args).await?;
			let parsed = Address::from_str(addr.0.as_str()).map_err(|e| {
				io::Error::new(
					io::ErrorKind::InvalidData,
					format!("bitcoind returned invalid address {}: {}", addr.0, e),
				)
			})?;
			parsed.require_network(network).map_err(|e| {
				io::Error::new(
					io::ErrorKind::InvalidData,
					format!("bitcoind returned address for wrong network: {}", e),
				)
			})
		}
	}

	pub async fn get_blockchain_info(&self) -> io::Result<BlockchainInfo> {
		self.bitcoind_rpc_client.call_method::<BlockchainInfo>("getblockchaininfo", &[]).await
	}

	pub fn list_unspent(&self) -> impl Future<Output = io::Result<ListUnspentResponse>> {
		let rpc_client = self.get_new_rpc_client();
		async move { rpc_client.call_method::<ListUnspentResponse>("listunspent", &[]).await }
	}
}

impl FeeEstimator for BitcoindClient {
	fn get_est_sat_per_1000_weight(&self, confirmation_target: ConfirmationTarget) -> u32 {
		if let Some(fee) = self.fees.get(&confirmation_target) {
			fee.load(Ordering::Acquire)
		} else {
			lightning::log_error!(
				&*self.logger,
				"Missing fee bucket for {:?}, defaulting to MIN_FEERATE",
				confirmation_target
			);
			MIN_FEERATE
		}
	}
}

impl BroadcasterInterface for BitcoindClient {
	fn broadcast_transactions(&self, txs: &[&Transaction]) {
		// As of Bitcoin Core 28, using `submitpackage` allows us to broadcast multiple
		// transactions at once and have them propagate through the network as a whole, avoiding
		// some pitfalls with anchor channels where the first transaction doesn't make it into the
		// mempool at all. Several older versions of Bitcoin Core also support `submitpackage`,
		// however, so we just use it unconditionally here.
		// Sadly, Bitcoin Core has an arbitrary restriction on `submitpackage` - it must actually
		// contain a package (see https://github.com/bitcoin/bitcoin/issues/31085).
		let txn = txs.iter().map(encode::serialize_hex).collect::<Vec<_>>();
		let bitcoind_rpc_client = Arc::clone(&self.bitcoind_rpc_client);
		let logger = Arc::clone(&self.logger);
		self.main_runtime_handle.spawn(async move {
			let res = if txn.len() == 1 {
				let tx_json = serde_json::json!(txn[0]);
				bitcoind_rpc_client
					.call_method::<serde_json::Value>("sendrawtransaction", &[tx_json])
					.await
			} else {
				let tx_json = serde_json::json!(txn);
				bitcoind_rpc_client
					.call_method::<serde_json::Value>("submitpackage", &[tx_json])
					.await
			};
			// This may error due to RL calling `broadcast_transactions` with the same transaction
			// multiple times, but the error is safe to ignore.
			//
			// Historically we also emitted a `print!` here so the REPL user
			// would see the warning; it has been removed because (a) it
			// duplicated the log line without the timestamp/level prefix,
			// and (b) it printed without a newline discipline that
			// interleaved badly with `> ` prompt redraws. The log_error!
			// entry above reaches the rotating log file in `disk.rs` and
			// is the canonical record.
			if let Err(e) = res {
				let err_str =
					e.get_ref().map(|inner| inner.to_string()).unwrap_or_else(|| e.to_string());
				log_error!(
					logger,
					"Warning, failed to broadcast a transaction, this is likely okay but may indicate an error: {}\nTransactions: {:?}",
					err_str,
					txn
				);
			}
		});
	}
}

impl ChangeDestinationSource for BitcoindClient {
	fn get_change_destination_script<'a>(&'a self) -> AsyncResult<'a, ScriptBuf, ()> {
		Box::pin(async move {
			self.get_new_address().await.map(|addr| addr.script_pubkey()).map_err(|_| ())
		})
	}
}

impl WalletSource for BitcoindClient {
	fn list_confirmed_utxos<'a>(&'a self) -> AsyncResult<'a, Vec<Utxo>, ()> {
		Box::pin(async move {
			let utxos = self.list_unspent().await.map_err(|_| ())?.0;
			Ok(utxos
				.into_iter()
				.filter_map(|utxo| {
					let outpoint = OutPoint { txid: utxo.txid, vout: utxo.vout };
					let value = bitcoin::Amount::from_sat(utxo.amount);
					match utxo.address.witness_program() {
						Some(prog) if prog.is_p2wpkh() => {
							WPubkeyHash::from_slice(prog.program().as_bytes())
								.map(|wpkh| Utxo::new_v0_p2wpkh(outpoint, value, &wpkh))
								.ok()
						},
						Some(prog) if prog.is_p2tr() => {
							// TODO: Add `Utxo::new_v1_p2tr` upstream.
							XOnlyPublicKey::from_slice(prog.program().as_bytes())
								.map(|_| Utxo {
									outpoint,
									output: TxOut {
										value,
										script_pubkey: utxo.address.script_pubkey(),
									},
									satisfaction_weight: WITNESS_SCALE_FACTOR as u64 +
									1 /* witness items */ + 1 /* schnorr sig len */ + 64, /* schnorr sig */
								})
								.ok()
						},
						_ => None,
					}
				})
				.collect())
		})
	}

	fn get_change_script<'a>(&'a self) -> AsyncResult<'a, ScriptBuf, ()> {
		Box::pin(async move {
			self.get_new_address().await.map(|addr| addr.script_pubkey()).map_err(|_| ())
		})
	}

	fn sign_psbt<'a>(&'a self, tx: Psbt) -> AsyncResult<'a, Transaction, ()> {
		Box::pin(async move {
			let mut tx_bytes = Vec::new();
			let _ = tx.unsigned_tx.consensus_encode(&mut tx_bytes).map_err(|_| ());
			let tx_hex = hex_utils::hex_str(&tx_bytes);
			let signed_tx = self.sign_raw_transaction_with_wallet(tx_hex).await.map_err(|_| ())?;
			let signed_tx_bytes = hex_utils::to_vec(&signed_tx.hex).ok_or(())?;
			Transaction::consensus_decode(&mut signed_tx_bytes.as_slice()).map_err(|_| ())
		})
	}
}
