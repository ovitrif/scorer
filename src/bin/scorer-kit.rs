//! Offline scorer-file toolkit.
//!
//! `scorer-kit` intentionally operates on serialized `ChannelLiquidities` bytes without
//! starting a node. It is meant for inspecting, decoding, comparing, and merging score files.

use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use humansize::{format_size, BINARY};
use scorer_kit_lightning::io::Cursor;
use scorer_kit_lightning::routing::scoring::{
	ChannelLiquidities, ChannelLiquidityDiagnostic, ChannelLiquidityMergeAction,
	ProbabilisticScoringDecayParameters,
};
use scorer_kit_lightning::util::ser::{Readable, Writeable};
use serde::Serialize;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
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
	/// Unix timestamp to normalize scores to before merging. Defaults to the newest timestamp found.
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

fn main() -> Result<()> {
	let cli = Cli::parse();
	match cli.command {
		Command::Inspect(args) => inspect(args),
		Command::Decode(args) => decode(args),
		Command::Compare(args) => compare(args),
		Command::Merge(args) => merge(args),
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
		merged.merge_with(
			incoming.liquidities,
			merge_timestamp,
			ProbabilisticScoringDecayParameters::default(),
			|existing, other| {
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
			},
		);
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

#[cfg(test)]
mod tests {
	use super::*;

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
}
