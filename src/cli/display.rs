//! Read-only printers for the REPL.
//!
//! Every function here takes immutable views of the runtime state and
//! emits a human-readable block to stdout. These deliberately use bare
//! `println!`/`print!` rather than the dual-output `user_out!` macro:
//! the contents are interactive user output (tables of channels,
//! balances, payments) and have no log-file value. They are NOT error
//! paths — errors still route through `user_err!`.

use std::sync::Arc;

use lightning::chain::channelmonitor::Balance;
use lightning::routing::gossip::NodeId;
use lightning::sign::SpendableOutputDescriptor;
use lightning::util::sweep::{OutputSpendStatus, TrackedSpendableOutput};

use crate::hex_utils;
use crate::{
	ChainMonitor, ChannelManager, HTLCStatus, InboundPaymentInfoStorage, NetworkGraph,
	OutboundPaymentInfoStorage, OutputSweeper, PeerManager,
};

pub(super) fn help() {
	let package_version = env!("CARGO_PKG_VERSION");
	let package_name = env!("CARGO_PKG_NAME");
	println!("\nVERSION:");
	println!("  {} v{}", package_name, package_version);
	println!("\nUSAGE:");
	println!("  Command [arguments]");
	println!("\nCOMMANDS:");
	println!("  help\tShows a list of commands.");
	println!("  quit\tClose the application.");
	println!("\n  Channels:");
	println!("      openchannel pubkey@[host:port] <amt_satoshis> [--public] [--with-anchors]");
	println!("      closechannel <channel_id> <peer_pubkey>");
	println!("      forceclosechannel <channel_id> <peer_pubkey>");
	println!("      listchannels");
	println!("\n  Peers:");
	println!("      connectpeer pubkey@host:port");
	println!("      disconnectpeer <peer_pubkey>");
	println!("      listpeers");
	println!("\n  Payments:");
	println!("      sendpayment <invoice|offer|human readable name> [<amount_msat>]");
	println!("      keysend <dest_pubkey> <amt_msats>");
	println!("      listpayments");
	println!("\n  Invoices:");
	println!("      getinvoice <amt_msats> <expiry_secs>");
	println!("      getoffer [<amt_msats>]");
	println!("\n  Other:");
	println!("      signmessage <message>");
	println!("      nodeinfo");
	println!("      listclaimablebalances");
	println!("      listsweeperoutputs");
}

pub(super) fn list_claimable_balances(chain_monitor: &Arc<ChainMonitor>) {
	let balances = chain_monitor.get_claimable_balances(&[]);
	if balances.is_empty() {
		println!("No claimable balances.");
		return;
	}

	let total_claimable_sats =
		balances.iter().map(|balance| balance.claimable_amount_satoshis()).sum::<u64>();
	println!("Claimable balances: {} (total={} sats)", balances.len(), total_claimable_sats);
	print!("[");
	for balance in balances {
		println!();
		println!("\t{{");
		println!("\t\tclaimable_amount_satoshis: {},", balance.claimable_amount_satoshis());
		println!("\t\tdetails: {:?},", balance);
		println!("\t}},");
	}
	println!("]");
}

pub(super) fn descriptor_kind_and_outpoint(
	tracked_output: &TrackedSpendableOutput,
) -> (&'static str, lightning::chain::transaction::OutPoint) {
	match &tracked_output.descriptor {
		SpendableOutputDescriptor::StaticOutput { outpoint, .. } => ("static_output", *outpoint),
		SpendableOutputDescriptor::DelayedPaymentOutput(output) => {
			("delayed_payment_output", output.outpoint)
		},
		SpendableOutputDescriptor::StaticPaymentOutput(output) => {
			("static_payment_output", output.outpoint)
		},
	}
}

pub(super) fn output_spend_status_string(status: &OutputSpendStatus) -> String {
	match status {
		OutputSpendStatus::PendingInitialBroadcast { delayed_until_height } => {
			if let Some(height) = delayed_until_height {
				format!("pending_initial_broadcast (delayed_until_height={})", height)
			} else {
				"pending_initial_broadcast".to_string()
			}
		},
		OutputSpendStatus::PendingFirstConfirmation { latest_broadcast_height, .. } => {
			format!(
				"pending_first_confirmation (latest_broadcast_height={})",
				latest_broadcast_height
			)
		},
		OutputSpendStatus::PendingThresholdConfirmations {
			latest_broadcast_height,
			confirmation_height,
			..
		} => {
			format!(
				"pending_threshold_confirmations (latest_broadcast_height={}, confirmation_height={})",
				latest_broadcast_height,
				confirmation_height
			)
		},
	}
}

pub(super) fn list_sweeper_outputs(output_sweeper: &Arc<OutputSweeper>) {
	let outputs = output_sweeper.tracked_spendable_outputs();
	if outputs.is_empty() {
		println!("No tracked spendable outputs.");
		return;
	}

	println!("Tracked spendable outputs: {}", outputs.len());
	print!("[");
	for tracked_output in outputs {
		let (descriptor_kind, outpoint) = descriptor_kind_and_outpoint(&tracked_output);
		let channel_id = tracked_output
			.channel_id
			.map(|channel_id| channel_id.to_string())
			.unwrap_or_else(|| "none".to_string());

		println!();
		println!("\t{{");
		println!("\t\tdescriptor_type: {},", descriptor_kind);
		println!("\t\toutpoint: {},", outpoint);
		println!("\t\tchannel_id: {},", channel_id);
		println!("\t\tstatus: {},", output_spend_status_string(&tracked_output.status));
		println!("\t}},");
	}
	println!("]");
}

pub(super) fn node_info(
	channel_manager: &Arc<ChannelManager>, chain_monitor: &Arc<ChainMonitor>,
	peer_manager: &Arc<PeerManager>, network_graph: &Arc<NetworkGraph>,
) {
	println!("\t{{");
	println!("\t\t node_pubkey: {}", channel_manager.get_our_node_id());
	let chans = channel_manager.list_channels();
	println!("\t\t num_channels: {}", chans.len());
	println!("\t\t num_usable_channels: {}", chans.iter().filter(|c| c.is_usable).count());
	let balances = chain_monitor.get_claimable_balances(&[]);
	let local_balance_sat = balances.iter().map(|b| b.claimable_amount_satoshis()).sum::<u64>();
	println!("\t\t local_balance_sats: {}", local_balance_sat);
	let close_fees_map = |b| match b {
		&Balance::ClaimableOnChannelClose {
			ref balance_candidates,
			confirmed_balance_candidate_index,
			..
		} => balance_candidates[confirmed_balance_candidate_index].transaction_fee_satoshis,
		_ => 0,
	};
	let close_fees_sats = balances.iter().map(close_fees_map).sum::<u64>();
	println!("\t\t eventual_close_fees_sats: {}", close_fees_sats);
	let pending_payments_map = |b| match b {
		&Balance::MaybeTimeoutClaimableHTLC { amount_satoshis, outbound_payment, .. } => {
			if outbound_payment {
				amount_satoshis
			} else {
				0
			}
		},
		_ => 0,
	};
	let pending_payments = balances.iter().map(pending_payments_map).sum::<u64>();
	println!("\t\t pending_outbound_payments_sats: {}", pending_payments);
	println!("\t\t num_peers: {}", peer_manager.list_peers().len());
	let graph_lock = network_graph.read_only();
	println!("\t\t network_nodes: {}", graph_lock.nodes().len());
	println!("\t\t network_channels: {}", graph_lock.channels().len());
	println!("\t}},");
}

pub(super) fn list_peers(peer_manager: Arc<PeerManager>) {
	println!("\t{{");
	for peer_details in peer_manager.list_peers() {
		println!("\t\t pubkey: {}", peer_details.counterparty_node_id);
	}
	println!("\t}},");
}

pub(super) fn list_channels(
	channel_manager: &Arc<ChannelManager>, network_graph: &Arc<NetworkGraph>,
) {
	print!("[");
	for chan_info in channel_manager.list_channels() {
		println!();
		println!("\t{{");
		println!("\t\tchannel_id: {},", chan_info.channel_id);
		if let Some(funding_txo) = chan_info.funding_txo {
			println!("\t\tfunding_txid: {},", funding_txo.txid);
		}

		println!(
			"\t\tpeer_pubkey: {},",
			hex_utils::hex_str(&chan_info.counterparty.node_id.serialize())
		);
		if let Some(node_info) = network_graph
			.read_only()
			.nodes()
			.get(&NodeId::from_pubkey(&chan_info.counterparty.node_id))
		{
			if let Some(announcement) = &node_info.announcement_info {
				println!("\t\tpeer_alias: {}", announcement.alias());
			}
		}

		if let Some(id) = chan_info.short_channel_id {
			println!("\t\tshort_channel_id: {},", id);
		}
		println!("\t\tis_channel_ready: {},", chan_info.is_channel_ready);
		println!("\t\tchannel_value_satoshis: {},", chan_info.channel_value_satoshis);
		println!("\t\toutbound_capacity_msat: {},", chan_info.outbound_capacity_msat);
		if chan_info.is_usable {
			println!("\t\tavailable_balance_for_send_msat: {},", chan_info.outbound_capacity_msat);
			println!("\t\tavailable_balance_for_recv_msat: {},", chan_info.inbound_capacity_msat);
		}
		println!("\t\tchannel_can_send_payments: {},", chan_info.is_usable);
		println!("\t\tpublic: {},", chan_info.is_announced);
		println!("\t}},");
	}
	println!("]");
}

pub(super) fn list_payments(
	inbound_payments: &InboundPaymentInfoStorage, outbound_payments: &OutboundPaymentInfoStorage,
) {
	print!("[");
	for (payment_hash, payment_info) in &inbound_payments.payments {
		println!();
		println!("\t{{");
		println!("\t\tamount_millisatoshis: {},", payment_info.amt_msat);
		println!("\t\tpayment_hash: {},", payment_hash);
		println!("\t\thtlc_direction: inbound,");
		println!(
			"\t\thtlc_status: {},",
			match payment_info.status {
				HTLCStatus::Pending => "pending",
				HTLCStatus::Succeeded => "succeeded",
				HTLCStatus::Failed => "failed",
			}
		);

		println!("\t}},");
	}

	for (payment_hash, payment_info) in &outbound_payments.payments {
		println!();
		println!("\t{{");
		println!("\t\tamount_millisatoshis: {},", payment_info.amt_msat);
		println!("\t\tpayment_hash: {},", payment_hash);
		println!("\t\thtlc_direction: outbound,");
		println!(
			"\t\thtlc_status: {},",
			match payment_info.status {
				HTLCStatus::Pending => "pending",
				HTLCStatus::Succeeded => "succeeded",
				HTLCStatus::Failed => "failed",
			}
		);

		println!("\t}},");
	}
	println!("]");
}
