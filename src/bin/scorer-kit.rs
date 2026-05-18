//! Offline scorer-file toolkit.
//!
//! `scorer-kit` intentionally operates on serialized `ChannelLiquidities` bytes without
//! starting a node. It is meant for inspecting, decoding, comparing, and merging score files.

use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use humansize::{format_size, BINARY};
use lightning_invoice::Bolt11Invoice;
use scorer_kit_lightning::io::Cursor;
use scorer_kit_lightning::routing::gossip::{NetworkGraph, NodeId};
use scorer_kit_lightning::routing::scoring::{
	ChannelLiquidities, ChannelLiquidityDiagnostic, ChannelLiquidityMergeAction,
	ProbabilisticScoringDecayParameters,
};
use scorer_kit_lightning::util::logger::{Logger, Record};
use scorer_kit_lightning::util::ser::{Readable, ReadableArgs, Writeable};
use serde::Serialize;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(
	name = "scorer-kit",
	about = "Inspect, decode, compare, and merge LDK scorer files",
	long_about = None,
)]
struct Cli {
	#[command(subcommand)]
	command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
	/// Print a summary and selected channel rows from a binary scorer file.
	Inspect(InspectArgs),
	/// Decode a binary scorer file to full JSON, including historical buckets.
	Decode(DecodeArgs),
	/// Compare two binary scorer files.
	Compare(CompareArgs),
	/// Merge two or more binary scorer files into a new scorer binary.
	Merge(MergeArgs),
	/// Extract short-channel-ids for selected node pubkeys from an LDK network graph.
	NodeScids(NodeScidsArgs),
	/// Validate that one or more files can be decoded as scorer files.
	Validate(ValidateArgs),
}

#[derive(Clone, Debug, Parser)]
struct InspectArgs {
	/// Path to a binary scorer file.
	file: PathBuf,
	/// Stable label to use in reports instead of the input path.
	#[arg(long)]
	label: Option<String>,
	/// Output format.
	#[arg(long, value_enum, default_value_t = OutputFormat::Text)]
	output: OutputFormat,
	/// Write output to a file instead of stdout.
	#[arg(long)]
	save: Option<PathBuf>,
	/// Limit per-channel rows to this number. Ignored if `--all` is set.
	#[arg(long)]
	top: Option<usize>,
	/// Dump every entry. Overrides `--top`.
	#[arg(long, default_value_t = false)]
	all: bool,
	/// Sort key for per-channel rows.
	#[arg(long, value_enum, default_value_t = Sort::Narrow)]
	sort: Sort,
}

#[derive(Clone, Debug, Parser)]
struct DecodeArgs {
	/// Path to a binary scorer file.
	file: PathBuf,
	/// Stable label to use in reports instead of the input path.
	#[arg(long)]
	label: Option<String>,
	/// Write JSON to a file instead of stdout.
	#[arg(long)]
	save: Option<PathBuf>,
	/// Write compact JSON instead of pretty-printed JSON.
	#[arg(long)]
	compact: bool,
}

#[derive(Clone, Debug, Parser)]
struct CompareArgs {
	/// First binary scorer file.
	left: PathBuf,
	/// Second binary scorer file.
	right: PathBuf,
	/// Stable label for the first file.
	#[arg(long)]
	left_label: Option<String>,
	/// Stable label for the second file.
	#[arg(long)]
	right_label: Option<String>,
	/// Output format.
	#[arg(long, value_enum, default_value_t = OutputFormat::Text)]
	output: OutputFormat,
	/// Write output to a file instead of stdout.
	#[arg(long)]
	save: Option<PathBuf>,
}

#[derive(Clone, Debug, Parser)]
struct MergeArgs {
	/// Binary scorer files to merge. The first input is the existing value when policies keep ties.
	inputs: Vec<PathBuf>,
	/// Output path for the merged binary scorer file.
	#[arg(long, short)]
	output: PathBuf,
	/// Optional labels, one per input, for the JSON merge report.
	#[arg(long = "label")]
	labels: Vec<String>,
	/// Duplicate-entry policy.
	#[arg(long, value_enum, default_value_t = MergePolicy::RicherHistory)]
	policy: MergePolicy,
	/// Write a JSON report describing the merge.
	#[arg(long)]
	report: Option<PathBuf>,
	/// Unix timestamp to normalize scores to before merging. Ignored for overlay merges.
	#[arg(long)]
	decay_to_secs: Option<u64>,
	/// Restrict incoming files to this short-channel-id. Repeatable. The first input is not filtered.
	#[arg(long = "overlay-scid")]
	overlay_scids: Vec<u64>,
	/// Restrict incoming files to short-channel-ids listed in a text file. Accepts whitespace or commas.
	#[arg(long = "overlay-scids-file")]
	overlay_scids_files: Vec<PathBuf>,
}

#[derive(Clone, Debug, Parser)]
struct ValidateArgs {
	/// Binary scorer files to validate.
	files: Vec<PathBuf>,
}

#[derive(Clone, Debug, Parser)]
struct NodeScidsArgs {
	/// Serialized LDK NetworkGraph file.
	#[arg(long)]
	graph: PathBuf,
	/// Node pubkey to match. Repeat for multiple nodes.
	#[arg(long = "node")]
	nodes: Vec<String>,
	/// Bolt11 invoice whose recovered payee pubkey should be matched. Repeatable.
	#[arg(long = "invoice")]
	invoices: Vec<String>,
	/// Optional scorer file used to keep only SCIDs that have score entries.
	#[arg(long)]
	scores: Option<PathBuf>,
	/// Output format. Text writes one SCID per line for direct use as an overlay allowlist.
	#[arg(long, value_enum, default_value_t = OutputFormat::Text)]
	output: OutputFormat,
	/// Write output to a file instead of stdout.
	#[arg(long)]
	save: Option<PathBuf>,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum OutputFormat {
	Text,
	Csv,
	Json,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum Sort {
	/// Smallest `[min_offset, max_offset]` window first.
	Narrow,
	/// Most recently updated first.
	Recent,
	/// Highest historical-bucket weight first.
	History,
	/// Smallest short-channel-id first.
	Scid,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum MergePolicy {
	/// Keep the existing entry when a short-channel-id appears in both files.
	PreferFirst,
	/// Replace the existing entry when a short-channel-id appears in both files.
	PreferLast,
	/// Combine duplicates with LDK's per-channel merge semantics.
	Combine,
	/// Prefer the entry with richer historical signal. Ties keep the existing entry.
	RicherHistory,
	/// Prefer the entry with the newer datapoint. Ties keep the existing entry.
	Newer,
}

impl MergePolicy {
	fn as_str(self) -> &'static str {
		match self {
			Self::PreferFirst => "prefer-first",
			Self::PreferLast => "prefer-last",
			Self::Combine => "combine",
			Self::RicherHistory => "richer-history",
			Self::Newer => "newer",
		}
	}
}

struct ScoreFile {
	label: String,
	file_size_bytes: u64,
	liquidities: ChannelLiquidities,
	diagnostics: Vec<ChannelLiquidityDiagnostic>,
}

#[derive(Debug, Serialize)]
struct Summary {
	label: String,
	file_size_bytes: u64,
	file_size_human: String,
	entry_count: usize,
	history_populated_count: usize,
	history_empty_count: usize,
	history_populated_pct: f64,
	min_offset_msat_p50: u64,
	min_offset_msat_p95: u64,
	min_offset_msat_max: u64,
	max_offset_msat_p50: u64,
	max_offset_msat_p95: u64,
	max_offset_msat_max: u64,
	offset_window_msat_p50: u64,
	offset_window_msat_p95: u64,
	offset_window_msat_max: u64,
	total_valid_points_tracked_p50: f64,
	total_valid_points_tracked_p95: f64,
	total_valid_points_tracked_max: f64,
}

#[derive(Debug, Serialize)]
struct ChannelRow {
	scid: u64,
	min_liquidity_offset_msat: u64,
	max_liquidity_offset_msat: u64,
	offset_window_msat: u64,
	has_history: bool,
	total_valid_points_tracked: f64,
	history_bucket_sum: u64,
	last_updated_secs: u64,
	offset_history_last_updated_secs: u64,
	last_datapoint_time_secs: u64,
}

#[derive(Debug, Serialize)]
struct DecodedChannel {
	scid: u64,
	min_liquidity_offset_msat: u64,
	max_liquidity_offset_msat: u64,
	offset_window_msat: u64,
	has_history: bool,
	total_valid_points_tracked: f64,
	history_bucket_sum: u64,
	last_updated_secs: u64,
	offset_history_last_updated_secs: u64,
	last_datapoint_time_secs: u64,
	min_history_buckets: [u16; 32],
	max_history_buckets: [u16; 32],
}

#[derive(Debug, Serialize)]
struct InspectReport {
	summary: Summary,
	channels: Vec<ChannelRow>,
}

#[derive(Debug, Serialize)]
struct DecodeReport {
	format: &'static str,
	summary: Summary,
	channels: Vec<DecodedChannel>,
}

#[derive(Debug, Serialize)]
struct CompareReport {
	left: Summary,
	right: Summary,
	overlap_count: usize,
	left_only_count: usize,
	right_only_count: usize,
	left_richer_overlap_count: usize,
	right_richer_overlap_count: usize,
	equal_richness_overlap_count: usize,
	left_newer_overlap_count: usize,
	right_newer_overlap_count: usize,
	equal_datapoint_time_overlap_count: usize,
}

#[derive(Debug, Serialize)]
struct CompareCsvRow {
	left_label: String,
	right_label: String,
	left_entry_count: usize,
	right_entry_count: usize,
	overlap_count: usize,
	left_only_count: usize,
	right_only_count: usize,
	left_richer_overlap_count: usize,
	right_richer_overlap_count: usize,
	equal_richness_overlap_count: usize,
	left_newer_overlap_count: usize,
	right_newer_overlap_count: usize,
	equal_datapoint_time_overlap_count: usize,
}

#[derive(Debug, Serialize)]
struct MergeReport {
	policy: String,
	output: String,
	output_file_size_bytes: u64,
	output_file_size_human: String,
	merge_timestamp_secs: u64,
	preserve_baseline_without_decay: bool,
	overlay_filter: Option<OverlayFilterReport>,
	inputs: Vec<Summary>,
	output_summary: Summary,
	stats: MergeStats,
	duplicate_decisions: Vec<MergeDecision>,
}

#[derive(Debug, Serialize)]
struct OverlayFilterReport {
	mode: &'static str,
	allowed_scid_count: usize,
	incoming_files: Vec<OverlayFilteredInput>,
}

#[derive(Debug, Serialize)]
struct OverlayFilteredInput {
	label: String,
	original_entry_count: usize,
	included_entry_count: usize,
	removed_entry_count: usize,
	missing_allowed_scid_count: usize,
}

#[derive(Default, Debug, Serialize)]
struct MergeStats {
	input_count: usize,
	duplicate_count: usize,
	kept_existing_count: usize,
	replaced_with_incoming_count: usize,
	combined_count: usize,
}

#[derive(Debug, Serialize)]
struct MergeDecision {
	scid: u64,
	existing_label: String,
	incoming_label: String,
	action: String,
	reason: String,
	existing_total_valid_points_tracked: f64,
	incoming_total_valid_points_tracked: f64,
	existing_history_bucket_sum: u64,
	incoming_history_bucket_sum: u64,
	existing_last_datapoint_time_secs: u64,
	incoming_last_datapoint_time_secs: u64,
}

#[derive(Debug, Serialize)]
struct NodeScidsReport {
	graph: String,
	graph_node_count: usize,
	graph_channel_count: usize,
	requested_node_count: usize,
	matched_node_count: usize,
	scid_count: usize,
	score_filter: Option<NodeScidsScoreFilter>,
	channels: Vec<NodeScidRow>,
}

#[derive(Clone, Debug, Serialize)]
struct NodeScidsScoreFilter {
	file: String,
	entry_count: usize,
}

#[derive(Debug, Serialize)]
struct NodeScidRow {
	scid: u64,
	node: String,
	peer: String,
	capacity_sats: Option<u64>,
	has_one_to_two_update: bool,
	has_two_to_one_update: bool,
}

#[derive(Debug, Serialize)]
struct NodeScidCsvRow {
	scid: u64,
	node: String,
	peer: String,
	capacity_sats: Option<u64>,
	has_one_to_two_update: bool,
	has_two_to_one_update: bool,
}

#[derive(Clone, Copy, Debug)]
struct NoopLogger;

impl Logger for NoopLogger {
	fn log(&self, _record: Record) {}
}

fn main() -> Result<()> {
	let cli = Cli::parse();
	match cli.command {
		Command::Inspect(args) => inspect(args),
		Command::Decode(args) => decode(args),
		Command::Compare(args) => compare(args),
		Command::Merge(args) => merge(args),
		Command::NodeScids(args) => node_scids(args),
		Command::Validate(args) => validate(args),
	}
}

fn inspect(args: InspectArgs) -> Result<()> {
	let label = label_for(&args.file, args.label);
	let score_file = read_score_file(&args.file, label)?;
	let mut diagnostics = score_file.diagnostics;
	sort_diagnostics(&mut diagnostics, args.sort);

	let limit = if args.all { diagnostics.len() } else { args.top.unwrap_or(20) };
	let report = InspectReport {
		summary: build_summary(score_file.label, score_file.file_size_bytes, &diagnostics),
		channels: diagnostics.into_iter().take(limit).map(ChannelRow::from).collect(),
	};

	write_rendered(args.save, args.output, |writer, output| match output {
		OutputFormat::Text => render_inspect_text(writer, &report),
		OutputFormat::Csv => render_inspect_csv(writer, &report),
		OutputFormat::Json => render_json(writer, &report),
	})
}

fn decode(args: DecodeArgs) -> Result<()> {
	let label = label_for(&args.file, args.label);
	let score_file = read_score_file(&args.file, label)?;
	let summary =
		build_summary(score_file.label, score_file.file_size_bytes, &score_file.diagnostics);
	let channels = score_file.diagnostics.into_iter().map(DecodedChannel::from).collect();
	let report =
		DecodeReport { format: "scorer-kit.decoded-channel-liquidities.v1", summary, channels };

	write_json(args.save, &report, !args.compact)
}

fn compare(args: CompareArgs) -> Result<()> {
	let left_label = label_for(&args.left, args.left_label);
	let right_label = label_for(&args.right, args.right_label);
	let left = read_score_file(&args.left, left_label)?;
	let right = read_score_file(&args.right, right_label)?;

	let left_by_scid: BTreeMap<u64, ChannelLiquidityDiagnostic> =
		left.diagnostics.iter().cloned().map(|diag| (diag.scid, diag)).collect();
	let right_by_scid: BTreeMap<u64, ChannelLiquidityDiagnostic> =
		right.diagnostics.iter().cloned().map(|diag| (diag.scid, diag)).collect();

	let left_scids: BTreeSet<u64> = left_by_scid.keys().copied().collect();
	let right_scids: BTreeSet<u64> = right_by_scid.keys().copied().collect();
	let overlap: Vec<u64> = left_scids.intersection(&right_scids).copied().collect();

	let mut left_richer_overlap_count = 0;
	let mut right_richer_overlap_count = 0;
	let mut equal_richness_overlap_count = 0;
	let mut left_newer_overlap_count = 0;
	let mut right_newer_overlap_count = 0;
	let mut equal_datapoint_time_overlap_count = 0;

	for scid in &overlap {
		let left_diag = left_by_scid.get(scid).expect("overlap key exists in left");
		let right_diag = right_by_scid.get(scid).expect("overlap key exists in right");
		match compare_history_richness(left_diag, right_diag) {
			Ordering::Greater => left_richer_overlap_count += 1,
			Ordering::Less => right_richer_overlap_count += 1,
			Ordering::Equal => equal_richness_overlap_count += 1,
		}
		match compare_datapoint_time(left_diag, right_diag) {
			Ordering::Greater => left_newer_overlap_count += 1,
			Ordering::Less => right_newer_overlap_count += 1,
			Ordering::Equal => equal_datapoint_time_overlap_count += 1,
		}
	}

	let report = CompareReport {
		left: build_summary(left.label, left.file_size_bytes, &left.diagnostics),
		right: build_summary(right.label, right.file_size_bytes, &right.diagnostics),
		overlap_count: overlap.len(),
		left_only_count: left_scids.difference(&right_scids).count(),
		right_only_count: right_scids.difference(&left_scids).count(),
		left_richer_overlap_count,
		right_richer_overlap_count,
		equal_richness_overlap_count,
		left_newer_overlap_count,
		right_newer_overlap_count,
		equal_datapoint_time_overlap_count,
	};

	write_rendered(args.save, args.output, |writer, output| match output {
		OutputFormat::Text => render_compare_text(writer, &report),
		OutputFormat::Csv => render_compare_csv(writer, &report),
		OutputFormat::Json => render_json(writer, &report),
	})
}

fn merge(args: MergeArgs) -> Result<()> {
	if args.inputs.len() < 2 {
		bail!("merge requires at least two input scorer files");
	}
	if !args.labels.is_empty() && args.labels.len() != args.inputs.len() {
		bail!("if provided, --label must be passed once per input");
	}

	let mut input_files = Vec::with_capacity(args.inputs.len());
	for (idx, path) in args.inputs.iter().enumerate() {
		let label = args.labels.get(idx).cloned().unwrap_or_else(|| default_label(path));
		input_files.push(read_score_file(path, label)?);
	}

	let overlay_scids = read_overlay_scids(&args)?;
	let preserve_baseline_without_decay = overlay_scids.is_some();
	let overlay_filter = overlay_scids
		.as_ref()
		.map(|allowed_scids| apply_overlay_filter(&mut input_files, allowed_scids));

	let merge_timestamp_secs = args.decay_to_secs.unwrap_or_else(|| {
		input_files
			.iter()
			.flat_map(|file| file.diagnostics.iter())
			.map(max_diagnostic_timestamp)
			.max()
			.unwrap_or(0)
	});
	let merge_timestamp = Duration::from_secs(merge_timestamp_secs);

	let input_summaries: Vec<Summary> = input_files
		.iter()
		.map(|file| build_summary(file.label.clone(), file.file_size_bytes, &file.diagnostics))
		.collect();

	let mut stats = MergeStats { input_count: input_files.len(), ..MergeStats::default() };
	let mut decisions = Vec::new();
	let mut existing_label = input_files[0].label.clone();
	let mut merged = input_files.remove(0).liquidities;

	for incoming in input_files {
		let incoming_label = incoming.label.clone();
		let policy = args.policy;
		let current_existing_label = existing_label.clone();
		let mut record_decision = |existing: &ChannelLiquidityDiagnostic,
		                           other: &ChannelLiquidityDiagnostic| {
			let (action, reason) = choose_merge_action(policy, existing, other);
			stats.duplicate_count += 1;
			match action {
				ChannelLiquidityMergeAction::KeepExisting => stats.kept_existing_count += 1,
				ChannelLiquidityMergeAction::ReplaceWithOther => {
					stats.replaced_with_incoming_count += 1
				},
				ChannelLiquidityMergeAction::Combine => stats.combined_count += 1,
			}
			decisions.push(MergeDecision {
				scid: existing.scid,
				existing_label: current_existing_label.clone(),
				incoming_label: incoming_label.clone(),
				action: merge_action_name(action).to_string(),
				reason,
				existing_total_valid_points_tracked: existing.total_valid_points_tracked,
				incoming_total_valid_points_tracked: other.total_valid_points_tracked,
				existing_history_bucket_sum: history_bucket_sum(existing),
				incoming_history_bucket_sum: history_bucket_sum(other),
				existing_last_datapoint_time_secs: existing.last_datapoint_time_secs,
				incoming_last_datapoint_time_secs: other.last_datapoint_time_secs,
			});
			action
		};
		if preserve_baseline_without_decay {
			merged.merge_without_decay(incoming.liquidities, &mut record_decision);
		} else {
			merged.merge_with(
				incoming.liquidities,
				merge_timestamp,
				ProbabilisticScoringDecayParameters::default(),
				&mut record_decision,
			);
		}
		existing_label = format!("merged-through-{}", incoming_label);
	}

	let mut serialized = Vec::new();
	merged.write(&mut serialized).map_err(|e| anyhow!("encode merged scorer: {}", e))?;
	fs::write(&args.output, &serialized)
		.with_context(|| format!("write merged scorer {}", args.output.display()))?;

	let output_diagnostics = merged.diagnostics();
	let output_summary =
		build_summary("merged".to_string(), serialized.len() as u64, &output_diagnostics);
	let report = MergeReport {
		policy: args.policy.as_str().to_string(),
		output: args.output.display().to_string(),
		output_file_size_bytes: serialized.len() as u64,
		output_file_size_human: format_size(serialized.len(), BINARY),
		merge_timestamp_secs,
		preserve_baseline_without_decay,
		overlay_filter,
		inputs: input_summaries,
		output_summary,
		stats,
		duplicate_decisions: decisions,
	};

	if let Some(report_path) = args.report {
		write_json(Some(report_path), &report, true)?;
	}

	eprintln!(
		"wrote {} ({}, {} entries)",
		args.output.display(),
		format_size(serialized.len(), BINARY),
		output_diagnostics.len()
	);
	Ok(())
}

fn node_scids(args: NodeScidsArgs) -> Result<()> {
	let mut requested_nodes = BTreeSet::new();
	for node in &args.nodes {
		let node =
			NodeId::from_str(node).with_context(|| format!("parse node pubkey '{}'", node))?;
		requested_nodes.insert(node);
	}
	for invoice in &args.invoices {
		let invoice = parse_bolt11(invoice)?;
		let payee = invoice.recover_payee_pub_key().to_string();
		let node = NodeId::from_str(&payee)
			.with_context(|| format!("parse recovered invoice payee pubkey '{}'", payee))?;
		requested_nodes.insert(node);
	}
	if requested_nodes.is_empty() {
		bail!("node-scids requires at least one --node or --invoice");
	}

	let score_filter = match &args.scores {
		Some(path) => {
			let score_file = read_score_file(path, default_label(path))?;
			let scids: BTreeSet<u64> =
				score_file.diagnostics.iter().map(|diag| diag.scid).collect();
			Some((
				NodeScidsScoreFilter {
					file: path.display().to_string(),
					entry_count: score_file.diagnostics.len(),
				},
				scids,
			))
		},
		None => None,
	};

	let graph = read_network_graph(&args.graph)?;
	let report = build_node_scids_report(&args.graph, &graph, &requested_nodes, score_filter);

	write_rendered(args.save, args.output, |writer, output| match output {
		OutputFormat::Text => render_node_scids_text(writer, &report),
		OutputFormat::Csv => render_node_scids_csv(writer, &report),
		OutputFormat::Json => render_json(writer, &report),
	})
}

fn build_node_scids_report(
	graph_path: &Path, graph: &NetworkGraph<NoopLogger>, requested_nodes: &BTreeSet<NodeId>,
	score_filter: Option<(NodeScidsScoreFilter, BTreeSet<u64>)>,
) -> NodeScidsReport {
	let read_only_graph = graph.read_only();
	let graph_node_count = read_only_graph.nodes().len();
	let graph_channel_count = read_only_graph.channels().len();
	let mut matched_nodes = BTreeSet::new();
	let mut rows_by_scid = BTreeMap::new();

	for (scid, channel) in read_only_graph.channels().unordered_iter() {
		let matched = if requested_nodes.contains(&channel.node_one) {
			Some((channel.node_one, channel.node_two))
		} else if requested_nodes.contains(&channel.node_two) {
			Some((channel.node_two, channel.node_one))
		} else {
			None
		};

		let Some((node, peer)) = matched else { continue };
		if let Some((_, score_scids)) = &score_filter {
			if !score_scids.contains(scid) {
				continue;
			}
		}

		matched_nodes.insert(node);
		rows_by_scid.entry(*scid).or_insert_with(|| NodeScidRow {
			scid: *scid,
			node: node.to_string(),
			peer: peer.to_string(),
			capacity_sats: channel.capacity_sats,
			has_one_to_two_update: channel.one_to_two.is_some(),
			has_two_to_one_update: channel.two_to_one.is_some(),
		});
	}

	let channels: Vec<NodeScidRow> = rows_by_scid.into_values().collect();
	let report = NodeScidsReport {
		graph: graph_path.display().to_string(),
		graph_node_count,
		graph_channel_count,
		requested_node_count: requested_nodes.len(),
		matched_node_count: matched_nodes.len(),
		scid_count: channels.len(),
		score_filter: score_filter.map(|(report, _)| report),
		channels,
	};

	report
}

fn read_network_graph(path: &Path) -> Result<NetworkGraph<NoopLogger>> {
	let bytes = fs::read(path).with_context(|| format!("read network graph {}", path.display()))?;
	let mut cursor = Cursor::new(&bytes);
	ReadableArgs::read(&mut cursor, NoopLogger)
		.map_err(|e| anyhow!("decode NetworkGraph from {}: {:?}", path.display(), e))
}

fn parse_bolt11(invoice: &str) -> Result<Bolt11Invoice> {
	let invoice = invoice.trim();
	let invoice = invoice
		.strip_prefix("lightning:")
		.or_else(|| invoice.strip_prefix("LIGHTNING:"))
		.unwrap_or(invoice);
	Bolt11Invoice::from_str(invoice).map_err(|e| anyhow!("parse bolt11 invoice: {:?}", e))
}

fn read_overlay_scids(args: &MergeArgs) -> Result<Option<BTreeSet<u64>>> {
	let mut scids: BTreeSet<u64> = args.overlay_scids.iter().copied().collect();
	for path in &args.overlay_scids_files {
		let contents = fs::read_to_string(path)
			.with_context(|| format!("read overlay scids file {}", path.display()))?;
		parse_scid_list(&contents, path, &mut scids)?;
	}

	if scids.is_empty() {
		Ok(None)
	} else {
		Ok(Some(scids))
	}
}

fn parse_scid_list(contents: &str, path: &Path, scids: &mut BTreeSet<u64>) -> Result<()> {
	for (line_idx, line) in contents.lines().enumerate() {
		let line_without_comment = line.split('#').next().unwrap_or("").trim();
		for token in line_without_comment
			.split(|c: char| c == ',' || c.is_ascii_whitespace())
			.filter(|token| !token.is_empty())
		{
			let scid = token.parse::<u64>().with_context(|| {
				format!("parse scid '{}' in {}:{}", token, path.display(), line_idx + 1)
			})?;
			scids.insert(scid);
		}
	}
	Ok(())
}

fn apply_overlay_filter(
	input_files: &mut [ScoreFile], allowed_scids: &BTreeSet<u64>,
) -> OverlayFilterReport {
	let mut incoming_files = Vec::new();
	for file in input_files.iter_mut().skip(1) {
		let original_entry_count = file.diagnostics.len();
		let scids_to_remove: Vec<u64> = file
			.diagnostics
			.iter()
			.filter(|diag| !allowed_scids.contains(&diag.scid))
			.map(|diag| diag.scid)
			.collect();

		for scid in scids_to_remove {
			file.liquidities.remove(scid);
		}
		file.diagnostics = file.liquidities.diagnostics();

		let included_entry_count = file.diagnostics.len();
		incoming_files.push(OverlayFilteredInput {
			label: file.label.clone(),
			original_entry_count,
			included_entry_count,
			removed_entry_count: original_entry_count.saturating_sub(included_entry_count),
			missing_allowed_scid_count: allowed_scids.len().saturating_sub(included_entry_count),
		});
	}

	OverlayFilterReport {
		mode: "incoming-scid-allowlist",
		allowed_scid_count: allowed_scids.len(),
		incoming_files,
	}
}

fn validate(args: ValidateArgs) -> Result<()> {
	if args.files.is_empty() {
		bail!("validate requires at least one file");
	}

	for path in args.files {
		let label = default_label(&path);
		let score_file = read_score_file(&path, label)?;
		println!(
			"ok: {} ({} entries, {})",
			path.display(),
			score_file.diagnostics.len(),
			format_size(score_file.file_size_bytes, BINARY)
		);
	}
	Ok(())
}

fn read_score_file(path: &Path, label: String) -> Result<ScoreFile> {
	let bytes = fs::read(path).with_context(|| format!("read scorer file {}", path.display()))?;
	let mut cursor = Cursor::new(&bytes);
	let liquidities = ChannelLiquidities::read(&mut cursor)
		.map_err(|e| anyhow!("decode ChannelLiquidities from {}: {:?}", path.display(), e))?;
	let diagnostics = liquidities.diagnostics();
	Ok(ScoreFile { label, file_size_bytes: bytes.len() as u64, liquidities, diagnostics })
}

fn build_summary(
	label: String, file_size_bytes: u64, diagnostics: &[ChannelLiquidityDiagnostic],
) -> Summary {
	let entry_count = diagnostics.len();
	let history_populated_count = diagnostics.iter().filter(|d| d.has_history).count();
	let history_empty_count = entry_count - history_populated_count;
	let history_populated_pct = if entry_count == 0 {
		0.0
	} else {
		100.0 * history_populated_count as f64 / entry_count as f64
	};

	let mut min_offsets: Vec<u64> =
		diagnostics.iter().map(|d| d.min_liquidity_offset_msat).collect();
	let mut max_offsets: Vec<u64> =
		diagnostics.iter().map(|d| d.max_liquidity_offset_msat).collect();
	let mut windows: Vec<u64> = diagnostics.iter().map(offset_window_msat).collect();
	let mut weights: Vec<f64> = diagnostics.iter().map(|d| d.total_valid_points_tracked).collect();

	min_offsets.sort_unstable();
	max_offsets.sort_unstable();
	windows.sort_unstable();
	weights.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));

	Summary {
		label,
		file_size_bytes,
		file_size_human: format_size(file_size_bytes, BINARY),
		entry_count,
		history_populated_count,
		history_empty_count,
		history_populated_pct,
		min_offset_msat_p50: percentile_u64(&min_offsets, 0.50),
		min_offset_msat_p95: percentile_u64(&min_offsets, 0.95),
		min_offset_msat_max: min_offsets.last().copied().unwrap_or(0),
		max_offset_msat_p50: percentile_u64(&max_offsets, 0.50),
		max_offset_msat_p95: percentile_u64(&max_offsets, 0.95),
		max_offset_msat_max: max_offsets.last().copied().unwrap_or(0),
		offset_window_msat_p50: percentile_u64(&windows, 0.50),
		offset_window_msat_p95: percentile_u64(&windows, 0.95),
		offset_window_msat_max: windows.last().copied().unwrap_or(0),
		total_valid_points_tracked_p50: percentile_f64(&weights, 0.50),
		total_valid_points_tracked_p95: percentile_f64(&weights, 0.95),
		total_valid_points_tracked_max: weights.last().copied().unwrap_or(0.0),
	}
}

fn sort_diagnostics(diagnostics: &mut [ChannelLiquidityDiagnostic], by: Sort) {
	match by {
		Sort::Narrow => diagnostics.sort_by_key(offset_window_msat),
		Sort::Recent => diagnostics.sort_by(|a, b| {
			max_diagnostic_timestamp(b).cmp(&max_diagnostic_timestamp(a)).then(a.scid.cmp(&b.scid))
		}),
		Sort::History => {
			diagnostics.sort_by(|a, b| compare_history_richness(b, a).then(a.scid.cmp(&b.scid)))
		},
		Sort::Scid => diagnostics.sort_by_key(|d| d.scid),
	}
}

fn choose_merge_action(
	policy: MergePolicy, existing: &ChannelLiquidityDiagnostic, other: &ChannelLiquidityDiagnostic,
) -> (ChannelLiquidityMergeAction, String) {
	match policy {
		MergePolicy::PreferFirst => {
			(ChannelLiquidityMergeAction::KeepExisting, "prefer-first policy".to_string())
		},
		MergePolicy::PreferLast => {
			(ChannelLiquidityMergeAction::ReplaceWithOther, "prefer-last policy".to_string())
		},
		MergePolicy::Combine => {
			(ChannelLiquidityMergeAction::Combine, "combine policy".to_string())
		},
		MergePolicy::RicherHistory => match compare_history_richness(other, existing) {
			Ordering::Greater => (
				ChannelLiquidityMergeAction::ReplaceWithOther,
				"incoming has richer historical signal".to_string(),
			),
			Ordering::Less => (
				ChannelLiquidityMergeAction::KeepExisting,
				"existing has richer historical signal".to_string(),
			),
			Ordering::Equal => (
				ChannelLiquidityMergeAction::KeepExisting,
				"equal historical signal; keeping existing".to_string(),
			),
		},
		MergePolicy::Newer => match compare_datapoint_time(other, existing) {
			Ordering::Greater => (
				ChannelLiquidityMergeAction::ReplaceWithOther,
				"incoming has newer datapoint".to_string(),
			),
			Ordering::Less => (
				ChannelLiquidityMergeAction::KeepExisting,
				"existing has newer datapoint".to_string(),
			),
			Ordering::Equal => (
				ChannelLiquidityMergeAction::KeepExisting,
				"equal datapoint time; keeping existing".to_string(),
			),
		},
	}
}

fn compare_history_richness(
	left: &ChannelLiquidityDiagnostic, right: &ChannelLiquidityDiagnostic,
) -> Ordering {
	left.has_history
		.cmp(&right.has_history)
		.then_with(|| {
			left.total_valid_points_tracked
				.partial_cmp(&right.total_valid_points_tracked)
				.unwrap_or(Ordering::Equal)
		})
		.then_with(|| history_bucket_sum(left).cmp(&history_bucket_sum(right)))
		.then_with(|| compare_datapoint_time(left, right))
		.then_with(|| compare_offset_window(right, left))
}

fn compare_datapoint_time(
	left: &ChannelLiquidityDiagnostic, right: &ChannelLiquidityDiagnostic,
) -> Ordering {
	left.last_datapoint_time_secs
		.cmp(&right.last_datapoint_time_secs)
		.then_with(|| left.last_updated_secs.cmp(&right.last_updated_secs))
		.then_with(|| {
			left.offset_history_last_updated_secs.cmp(&right.offset_history_last_updated_secs)
		})
}

fn compare_offset_window(
	left: &ChannelLiquidityDiagnostic, right: &ChannelLiquidityDiagnostic,
) -> Ordering {
	offset_window_msat(left).cmp(&offset_window_msat(right))
}

fn history_bucket_sum(diag: &ChannelLiquidityDiagnostic) -> u64 {
	diag.min_history_buckets.iter().map(|v| *v as u64).sum::<u64>()
		+ diag.max_history_buckets.iter().map(|v| *v as u64).sum::<u64>()
}

fn max_diagnostic_timestamp(diag: &ChannelLiquidityDiagnostic) -> u64 {
	diag.last_updated_secs
		.max(diag.offset_history_last_updated_secs)
		.max(diag.last_datapoint_time_secs)
}

fn offset_window_msat(diag: &ChannelLiquidityDiagnostic) -> u64 {
	diag.max_liquidity_offset_msat.saturating_sub(diag.min_liquidity_offset_msat)
}

fn percentile_u64(sorted: &[u64], p: f64) -> u64 {
	if sorted.is_empty() {
		return 0;
	}
	let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
	sorted[idx.min(sorted.len() - 1)]
}

fn percentile_f64(sorted: &[f64], p: f64) -> f64 {
	if sorted.is_empty() {
		return 0.0;
	}
	let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
	sorted[idx.min(sorted.len() - 1)]
}

fn render_inspect_text<W: Write + ?Sized>(writer: &mut W, report: &InspectReport) -> Result<()> {
	let s = &report.summary;
	writeln!(writer, "scorer-kit inspect")?;
	writeln!(writer, "  label:                  {}", s.label)?;
	writeln!(
		writer,
		"  file size:              {} ({} bytes)",
		s.file_size_human, s.file_size_bytes
	)?;
	writeln!(writer, "  entries:                {}", s.entry_count)?;
	writeln!(
		writer,
		"  history populated:      {} ({:.1}%)",
		s.history_populated_count, s.history_populated_pct
	)?;
	writeln!(writer, "  history empty:          {}", s.history_empty_count)?;
	writeln!(writer)?;
	writeln!(
		writer,
		"  min_offset_msat:        p50={}  p95={}  max={}",
		s.min_offset_msat_p50, s.min_offset_msat_p95, s.min_offset_msat_max
	)?;
	writeln!(
		writer,
		"  max_offset_msat:        p50={}  p95={}  max={}",
		s.max_offset_msat_p50, s.max_offset_msat_p95, s.max_offset_msat_max
	)?;
	writeln!(
		writer,
		"  offset_window_msat:     p50={}  p95={}  max={}",
		s.offset_window_msat_p50, s.offset_window_msat_p95, s.offset_window_msat_max
	)?;
	writeln!(
		writer,
		"  history weight:         p50={:.0}  p95={:.0}  max={:.0}",
		s.total_valid_points_tracked_p50,
		s.total_valid_points_tracked_p95,
		s.total_valid_points_tracked_max
	)?;
	writeln!(writer)?;

	if !report.channels.is_empty() {
		writeln!(
			writer,
			"{:>20}  {:>14}  {:>14}  {:>14}  {:>3}  {:>16}  {:>10}",
			"scid", "min_offset", "max_offset", "window", "his", "history_weight", "datapoint"
		)?;
		for row in &report.channels {
			writeln!(
				writer,
				"{:>20}  {:>14}  {:>14}  {:>14}  {:>3}  {:>16.0}  {:>10}",
				row.scid,
				row.min_liquidity_offset_msat,
				row.max_liquidity_offset_msat,
				row.offset_window_msat,
				if row.has_history { "yes" } else { "no" },
				row.total_valid_points_tracked,
				row.last_datapoint_time_secs,
			)?;
		}
	}
	Ok(())
}

fn render_compare_text<W: Write + ?Sized>(writer: &mut W, report: &CompareReport) -> Result<()> {
	writeln!(writer, "scorer-kit compare")?;
	writeln!(writer, "  left:                   {}", report.left.label)?;
	writeln!(writer, "  right:                  {}", report.right.label)?;
	writeln!(writer, "  left entries:           {}", report.left.entry_count)?;
	writeln!(writer, "  right entries:          {}", report.right.entry_count)?;
	writeln!(writer, "  overlap:                {}", report.overlap_count)?;
	writeln!(writer, "  left only:              {}", report.left_only_count)?;
	writeln!(writer, "  right only:             {}", report.right_only_count)?;
	writeln!(writer)?;
	writeln!(writer, "  richer history left:    {}", report.left_richer_overlap_count)?;
	writeln!(writer, "  richer history right:   {}", report.right_richer_overlap_count)?;
	writeln!(writer, "  equal history richness: {}", report.equal_richness_overlap_count)?;
	writeln!(writer)?;
	writeln!(writer, "  newer datapoint left:   {}", report.left_newer_overlap_count)?;
	writeln!(writer, "  newer datapoint right:  {}", report.right_newer_overlap_count)?;
	writeln!(writer, "  equal datapoint time:   {}", report.equal_datapoint_time_overlap_count)?;
	Ok(())
}

fn render_inspect_csv<W: Write + ?Sized>(writer: &mut W, report: &InspectReport) -> Result<()> {
	{
		let mut csv = csv::Writer::from_writer(&mut *writer);
		csv.serialize(&report.summary).context("write summary csv row")?;
		csv.flush()?;
	}
	if !report.channels.is_empty() {
		writer.write_all(b"\n")?;
		let mut csv = csv::Writer::from_writer(writer);
		for row in &report.channels {
			csv.serialize(row).context("write channel csv row")?;
		}
		csv.flush()?;
	}
	Ok(())
}

fn render_compare_csv<W: Write + ?Sized>(writer: &mut W, report: &CompareReport) -> Result<()> {
	let mut csv = csv::Writer::from_writer(writer);
	csv.serialize(CompareCsvRow::from(report)).context("write compare csv row")?;
	csv.flush()?;
	Ok(())
}

fn render_node_scids_text<W: Write + ?Sized>(
	writer: &mut W, report: &NodeScidsReport,
) -> Result<()> {
	for row in &report.channels {
		writeln!(writer, "{}", row.scid)?;
	}
	Ok(())
}

fn render_node_scids_csv<W: Write + ?Sized>(
	writer: &mut W, report: &NodeScidsReport,
) -> Result<()> {
	let mut csv = csv::Writer::from_writer(writer);
	for row in &report.channels {
		csv.serialize(NodeScidCsvRow::from(row)).context("write node scid csv row")?;
	}
	csv.flush()?;
	Ok(())
}

fn render_json<W: Write + ?Sized, T: Serialize>(writer: &mut W, value: &T) -> Result<()> {
	serde_json::to_writer_pretty(writer, value).context("write json")?;
	Ok(())
}

fn write_rendered<F>(save: Option<PathBuf>, output: OutputFormat, render: F) -> Result<()>
where
	F: FnOnce(&mut dyn Write, OutputFormat) -> Result<()>,
{
	match save {
		Some(path) => {
			let file = fs::File::create(&path)
				.with_context(|| format!("create output file {}", path.display()))?;
			let mut writer = BufWriter::new(file);
			render(&mut writer, output)?;
			writer.flush()?;
		},
		None => {
			let stdout = std::io::stdout();
			let mut writer = stdout.lock();
			render(&mut writer, output)?;
			writer.flush()?;
		},
	}
	Ok(())
}

fn write_json<T: Serialize>(save: Option<PathBuf>, value: &T, pretty: bool) -> Result<()> {
	match save {
		Some(path) => {
			let file = fs::File::create(&path)
				.with_context(|| format!("create output file {}", path.display()))?;
			let mut writer = BufWriter::new(file);
			if pretty {
				render_json(&mut writer, value)?;
			} else {
				serde_json::to_writer(&mut writer, value).context("write compact json")?;
			}
			writer.flush()?;
		},
		None => {
			let stdout = std::io::stdout();
			let mut writer = stdout.lock();
			if pretty {
				render_json(&mut writer, value)?;
			} else {
				serde_json::to_writer(&mut writer, value).context("write compact json")?;
			}
			writer.write_all(b"\n")?;
			writer.flush()?;
		},
	}
	Ok(())
}

fn label_for(path: &Path, label: Option<String>) -> String {
	label.unwrap_or_else(|| default_label(path))
}

fn default_label(path: &Path) -> String {
	path.file_name().and_then(|name| name.to_str()).unwrap_or("scorer.bin").to_string()
}

fn merge_action_name(action: ChannelLiquidityMergeAction) -> &'static str {
	match action {
		ChannelLiquidityMergeAction::KeepExisting => "keep-existing",
		ChannelLiquidityMergeAction::ReplaceWithOther => "replace-with-incoming",
		ChannelLiquidityMergeAction::Combine => "combine",
	}
}

impl From<ChannelLiquidityDiagnostic> for ChannelRow {
	fn from(diag: ChannelLiquidityDiagnostic) -> Self {
		Self {
			scid: diag.scid,
			min_liquidity_offset_msat: diag.min_liquidity_offset_msat,
			max_liquidity_offset_msat: diag.max_liquidity_offset_msat,
			offset_window_msat: offset_window_msat(&diag),
			has_history: diag.has_history,
			total_valid_points_tracked: diag.total_valid_points_tracked,
			history_bucket_sum: history_bucket_sum(&diag),
			last_updated_secs: diag.last_updated_secs,
			offset_history_last_updated_secs: diag.offset_history_last_updated_secs,
			last_datapoint_time_secs: diag.last_datapoint_time_secs,
		}
	}
}

impl From<ChannelLiquidityDiagnostic> for DecodedChannel {
	fn from(diag: ChannelLiquidityDiagnostic) -> Self {
		Self {
			scid: diag.scid,
			min_liquidity_offset_msat: diag.min_liquidity_offset_msat,
			max_liquidity_offset_msat: diag.max_liquidity_offset_msat,
			offset_window_msat: offset_window_msat(&diag),
			has_history: diag.has_history,
			total_valid_points_tracked: diag.total_valid_points_tracked,
			history_bucket_sum: history_bucket_sum(&diag),
			last_updated_secs: diag.last_updated_secs,
			offset_history_last_updated_secs: diag.offset_history_last_updated_secs,
			last_datapoint_time_secs: diag.last_datapoint_time_secs,
			min_history_buckets: diag.min_history_buckets,
			max_history_buckets: diag.max_history_buckets,
		}
	}
}

impl From<&CompareReport> for CompareCsvRow {
	fn from(report: &CompareReport) -> Self {
		Self {
			left_label: report.left.label.clone(),
			right_label: report.right.label.clone(),
			left_entry_count: report.left.entry_count,
			right_entry_count: report.right.entry_count,
			overlap_count: report.overlap_count,
			left_only_count: report.left_only_count,
			right_only_count: report.right_only_count,
			left_richer_overlap_count: report.left_richer_overlap_count,
			right_richer_overlap_count: report.right_richer_overlap_count,
			equal_richness_overlap_count: report.equal_richness_overlap_count,
			left_newer_overlap_count: report.left_newer_overlap_count,
			right_newer_overlap_count: report.right_newer_overlap_count,
			equal_datapoint_time_overlap_count: report.equal_datapoint_time_overlap_count,
		}
	}
}

impl From<&NodeScidRow> for NodeScidCsvRow {
	fn from(row: &NodeScidRow) -> Self {
		Self {
			scid: row.scid,
			node: row.node.clone(),
			peer: row.peer.clone(),
			capacity_sats: row.capacity_sats,
			has_one_to_two_update: row.has_one_to_two_update,
			has_two_to_one_update: row.has_two_to_one_update,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use bitcoin::network::Network;
	use bitcoin::secp256k1::{PublicKey, Secp256k1, SecretKey};
	use scorer_kit_lightning::types::features::ChannelFeatures;

	const TEST_INVOICE: &str = concat!(
		"lnbc100p1psj9jhxdqud3jxktt5w46x7unfv9kz6mn0v3jsnp4q0d3p2sfluzdx45tqcs",
		"h2pu5qc7lgq0xs578ngs6s0s68ua4h7cvspp5q6rmq35js88zp5dvwrv9m459tnk2zunwj5jalqtyxqulh0l",
		"5gflssp5nf55ny5gcrfl30xuhzj3nphgj27rstekmr9fw3ny5989s300gyus9qyysgqcqpcrzjqw2sxwe993",
		"h5pcm4dxzpvttgza8zhkqxpgffcrf5v25nwpr3cmfg7z54kuqq8rgqqqqqqqq2qqqqq9qq9qrzjqd0ylaqcl",
		"j9424x9m8h2vcukcgnm6s56xfgu3j78zyqzhgs4hlpzvznlugqq9vsqqqqqqqlgqqqqqeqq9qrzjqwldmj9d",
		"ha74df76zhx6l9we0vjdquygcdt3kssupehe64g6yyp5yz5rhuqqwccqqyqqqqlgqqqqjcqq9qrzjqf9e58a",
		"guqr0rcun0ajlvmzq3ek63cw2w282gv3z5uupmuwvgjtq2z55qsqqg6qqqyqqqrtnqqqzq3cqygrzjqvphms",
		"ywntrrhqjcraumvc4y6r8v4z5v593trte429v4hredj7ms5z52usqq9ngqqqqqqqlgqqqqqqgq9qrzjq2v0v",
		"p62g49p7569ev48cmulecsxe59lvaw3wlxm7r982zxa9zzj7z5l0cqqxusqqyqqqqlgqqqqqzsqygarl9fh3",
		"8s0gyuxjjgux34w75dnc6xp2l35j7es3jd4ugt3lu0xzre26yg5m7ke54n2d5sym4xcmxtl8238xxvw5h5h5",
		"j5r6drg6k6zcqj0fcwg"
	);

	#[test]
	fn parse_scid_list_accepts_comments_commas_and_whitespace() {
		let mut scids = BTreeSet::new();
		parse_scid_list(
			"42, 43\n# ignored\n44 45 # trailing\n",
			Path::new("scids.txt"),
			&mut scids,
		)
		.expect("parse scids");

		assert_eq!(scids.into_iter().collect::<Vec<_>>(), vec![42, 43, 44, 45]);
	}

	#[test]
	fn parse_scid_list_rejects_non_decimal_tokens() {
		let mut scids = BTreeSet::new();
		let err = parse_scid_list("42\nnot-a-scid\n", Path::new("scids.txt"), &mut scids)
			.expect_err("invalid scid token should fail");

		assert!(err.to_string().contains("not-a-scid"));
	}

	#[test]
	fn parse_bolt11_accepts_lightning_uri_prefix() {
		let invoice = parse_bolt11(&format!("lightning:{}", TEST_INVOICE)).expect("parse invoice");

		assert_eq!(invoice.to_string(), TEST_INVOICE);
	}

	#[test]
	fn node_scids_requires_node_or_invoice() {
		let err = node_scids(NodeScidsArgs {
			graph: PathBuf::from("unused-network-graph"),
			nodes: vec![],
			invoices: vec![],
			scores: None,
			output: OutputFormat::Text,
			save: None,
		})
		.expect_err("empty node selection should fail before graph read");

		assert!(err.to_string().contains("at least one --node or --invoice"));
	}

	#[test]
	fn node_scids_report_filters_graph_channels_to_requested_nodes_and_score_scids() {
		let target = test_node_id(2);
		let peer_a = test_node_id(3);
		let peer_b = test_node_id(4);
		let graph = test_graph(&[
			(10, target, peer_a, Some(1_000)),
			(20, peer_b, target, Some(2_000)),
			(30, peer_a, peer_b, Some(3_000)),
		]);
		let requested_nodes = BTreeSet::from([target]);
		let score_scids = BTreeSet::from([20, 30]);

		let report = build_node_scids_report(
			Path::new("graph.bin"),
			&graph,
			&requested_nodes,
			Some((
				NodeScidsScoreFilter { file: "incoming.bin".to_string(), entry_count: 2 },
				score_scids,
			)),
		);

		assert_eq!(report.graph_node_count, 3);
		assert_eq!(report.graph_channel_count, 3);
		assert_eq!(report.requested_node_count, 1);
		assert_eq!(report.matched_node_count, 1);
		assert_eq!(report.scid_count, 1);
		assert_eq!(report.score_filter.as_ref().unwrap().entry_count, 2);
		assert_eq!(report.channels[0].scid, 20);
		assert_eq!(report.channels[0].node, target.to_string());
		assert_eq!(report.channels[0].peer, peer_b.to_string());
		assert_eq!(report.channels[0].capacity_sats, Some(2_000));
	}

	#[test]
	fn read_network_graph_decodes_serialized_ldk_graph() {
		let target = test_node_id(2);
		let peer = test_node_id(3);
		let graph = test_graph(&[(42, target, peer, Some(50_000))]);
		let mut graph_bytes = Vec::new();
		graph.write(&mut graph_bytes).expect("serialize graph");
		let dir = tempfile::tempdir().expect("tempdir");
		let graph_path = dir.path().join("network_graph_cache");
		fs::write(&graph_path, graph_bytes).expect("write graph");

		let decoded = read_network_graph(&graph_path).expect("read graph");
		let report =
			build_node_scids_report(&graph_path, &decoded, &BTreeSet::from([target]), None);

		assert_eq!(report.scid_count, 1);
		assert_eq!(report.channels[0].scid, 42);
		assert_eq!(report.channels[0].capacity_sats, Some(50_000));
	}

	#[test]
	fn richer_history_policy_prefers_incoming_when_history_signal_is_stronger() {
		let existing = diagnostic(7, true, 10.0, 1, 20);
		let incoming = diagnostic(7, true, 20.0, 1, 10);

		let (action, reason) =
			choose_merge_action(MergePolicy::RicherHistory, &existing, &incoming);

		assert_eq!(action, ChannelLiquidityMergeAction::ReplaceWithOther);
		assert_eq!(reason, "incoming has richer historical signal");
	}

	#[test]
	fn newer_policy_keeps_existing_on_equal_datapoint_time() {
		let existing = diagnostic(7, true, 10.0, 1, 20);
		let incoming = diagnostic(7, true, 20.0, 2, 20);

		let (action, reason) = choose_merge_action(MergePolicy::Newer, &existing, &incoming);

		assert_eq!(action, ChannelLiquidityMergeAction::KeepExisting);
		assert_eq!(reason, "equal datapoint time; keeping existing");
	}

	fn test_graph(channels: &[(u64, NodeId, NodeId, Option<u64>)]) -> NetworkGraph<NoopLogger> {
		let graph = NetworkGraph::new(Network::Bitcoin, NoopLogger);
		for (scid, node_one, node_two, capacity_sats) in channels {
			graph
				.add_channel_from_partial_announcement(
					*scid,
					*capacity_sats,
					0,
					ChannelFeatures::empty(),
					*node_one,
					*node_two,
				)
				.expect("add test graph channel");
		}
		graph
	}

	fn test_node_id(secret_byte: u8) -> NodeId {
		let secp_ctx = Secp256k1::new();
		let secret = SecretKey::from_slice(&[secret_byte; 32]).expect("valid test secret key");
		NodeId::from_pubkey(&PublicKey::from_secret_key(&secp_ctx, &secret))
	}

	fn diagnostic(
		scid: u64, has_history: bool, total_valid_points_tracked: f64, history_bucket_sum: u16,
		last_datapoint_time_secs: u64,
	) -> ChannelLiquidityDiagnostic {
		let mut min_history_buckets = [0; 32];
		min_history_buckets[0] = history_bucket_sum;
		ChannelLiquidityDiagnostic {
			scid,
			min_liquidity_offset_msat: 0,
			max_liquidity_offset_msat: 0,
			last_updated_secs: last_datapoint_time_secs,
			offset_history_last_updated_secs: last_datapoint_time_secs,
			last_datapoint_time_secs,
			has_history,
			total_valid_points_tracked,
			min_history_buckets,
			max_history_buckets: [0; 32],
		}
	}
}
