use crate::disk::FilesystemLogger;
use crate::NetworkGraph;
use lightning::routing::scoring::ProbabilisticScorer;
use std::sync::{Arc, RwLock};

mod runner;
mod sender;
mod tracker;

type ProbingScorer = ProbabilisticScorer<Arc<NetworkGraph>, Arc<FilesystemLogger>>;
type ProbingScorerLock = RwLock<ProbingScorer>;

pub(crate) use runner::{spawn_probing_loop, ProbingDeps};
pub(crate) use tracker::ProbeTracker;
