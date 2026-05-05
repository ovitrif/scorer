use lightning::types::payment::PaymentHash;
use std::collections::HashMap as StdHashMap;
use std::time::{Duration, Instant};

const TIMED_OUT_PROBE_TTL: Duration = Duration::from_secs(15 * 60);
const MAX_TIMED_OUT_PROBES: usize = 4096;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(super) enum ProbeOutcome {
	Success,
	Failed,
}

pub(crate) struct ProbeTracker {
	pending_probes: StdHashMap<PaymentHash, tokio::sync::oneshot::Sender<ProbeOutcome>>,
	completed_probes: StdHashMap<PaymentHash, ProbeOutcome>,
	timed_out_probes: StdHashMap<PaymentHash, Instant>,
}

impl ProbeTracker {
	pub(crate) fn new() -> Self {
		Self {
			pending_probes: StdHashMap::new(),
			completed_probes: StdHashMap::new(),
			timed_out_probes: StdHashMap::new(),
		}
	}

	pub(super) fn register_probe(
		&mut self, payment_hash: PaymentHash,
	) -> tokio::sync::oneshot::Receiver<ProbeOutcome> {
		self.prune_timed_out_probes();
		self.timed_out_probes.remove(&payment_hash);

		if let Some(outcome) = self.completed_probes.remove(&payment_hash) {
			let (tx, rx) = tokio::sync::oneshot::channel();
			let _ = tx.send(outcome);
			return rx;
		}

		let (tx, rx) = tokio::sync::oneshot::channel();
		self.pending_probes.insert(payment_hash, tx);
		rx
	}

	pub(crate) fn complete_success(&mut self, payment_hash: &PaymentHash) {
		self.complete_probe(payment_hash, ProbeOutcome::Success);
	}

	pub(crate) fn complete_failed(&mut self, payment_hash: &PaymentHash) {
		self.complete_probe(payment_hash, ProbeOutcome::Failed);
	}

	pub(super) fn mark_timed_out(&mut self, payment_hash: &PaymentHash) {
		self.prune_timed_out_probes();
		self.pending_probes.remove(payment_hash);
		self.completed_probes.remove(payment_hash);
		self.timed_out_probes.insert(*payment_hash, Instant::now());
	}

	fn complete_probe(&mut self, payment_hash: &PaymentHash, outcome: ProbeOutcome) {
		self.prune_timed_out_probes();
		if self.timed_out_probes.remove(payment_hash).is_some() {
			return;
		}

		if let Some(sender) = self.pending_probes.remove(payment_hash) {
			let _ = sender.send(outcome);
		} else {
			self.completed_probes.insert(*payment_hash, outcome);
		}
	}

	fn prune_timed_out_probes(&mut self) {
		let now = Instant::now();
		self.timed_out_probes.retain(|_, timed_out_at| {
			now.saturating_duration_since(*timed_out_at) <= TIMED_OUT_PROBE_TTL
		});

		if self.timed_out_probes.len() <= MAX_TIMED_OUT_PROBES {
			return;
		}

		let remove_count = self.timed_out_probes.len() - MAX_TIMED_OUT_PROBES;
		let mut by_age = self
			.timed_out_probes
			.iter()
			.map(|(payment_hash, timed_out_at)| (*payment_hash, *timed_out_at))
			.collect::<Vec<_>>();
		by_age.sort_by_key(|(_, timed_out_at)| *timed_out_at);

		for (payment_hash, _) in by_age.into_iter().take(remove_count) {
			self.timed_out_probes.remove(&payment_hash);
		}
	}
}

#[cfg(test)]
mod tests {
	use super::{ProbeOutcome, ProbeTracker, MAX_TIMED_OUT_PROBES, TIMED_OUT_PROBE_TTL};
	use lightning::types::payment::PaymentHash;
	use std::time::{Duration, Instant};
	use tokio::sync::oneshot::error::TryRecvError;

	#[test]
	fn complete_before_register_delivers_buffered_success() {
		let mut tracker = ProbeTracker::new();
		let hash = PaymentHash([1; 32]);

		tracker.complete_success(&hash);

		let mut rx = tracker.register_probe(hash);
		assert!(matches!(rx.try_recv(), Ok(ProbeOutcome::Success)));
	}

	#[test]
	fn complete_before_register_delivers_buffered_failed() {
		let mut tracker = ProbeTracker::new();
		let hash = PaymentHash([2; 32]);

		tracker.complete_failed(&hash);

		let mut rx = tracker.register_probe(hash);
		assert!(matches!(rx.try_recv(), Ok(ProbeOutcome::Failed)));
	}

	#[test]
	fn register_before_complete_still_delivers_over_channel() {
		let mut tracker = ProbeTracker::new();
		let hash = PaymentHash([3; 32]);

		let mut rx = tracker.register_probe(hash);
		assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));

		tracker.complete_success(&hash);
		assert!(matches!(rx.try_recv(), Ok(ProbeOutcome::Success)));
	}

	#[test]
	fn late_completion_after_timeout_is_dropped() {
		let mut tracker = ProbeTracker::new();
		let hash = PaymentHash([4; 32]);

		tracker.mark_timed_out(&hash);
		tracker.complete_success(&hash);

		assert!(!tracker.completed_probes.contains_key(&hash));
		assert!(!tracker.timed_out_probes.contains_key(&hash));
	}

	#[test]
	fn prune_removes_entries_older_than_ttl() {
		let mut tracker = ProbeTracker::new();
		let hash = PaymentHash([5; 32]);
		let stale_time = Instant::now() - (TIMED_OUT_PROBE_TTL + Duration::from_secs(1));
		tracker.timed_out_probes.insert(hash, stale_time);

		tracker.prune_timed_out_probes();

		assert!(!tracker.timed_out_probes.contains_key(&hash));
	}

	#[test]
	fn prune_enforces_max_size_cap() {
		let mut tracker = ProbeTracker::new();
		let base = Instant::now();

		for i in 0..(MAX_TIMED_OUT_PROBES + 3) {
			let mut bytes = [0u8; 32];
			bytes[0] = (i % 256) as u8;
			bytes[1] = ((i / 256) % 256) as u8;
			bytes[2] = ((i / 65536) % 256) as u8;
			tracker
				.timed_out_probes
				.insert(PaymentHash(bytes), base + Duration::from_nanos(i as u64));
		}

		tracker.prune_timed_out_probes();

		assert!(tracker.timed_out_probes.len() <= MAX_TIMED_OUT_PROBES);
		assert!(!tracker.timed_out_probes.contains_key(&PaymentHash([0; 32])));
	}
}
