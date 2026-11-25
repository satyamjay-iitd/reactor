//! What an actor has done so far, for the node's `GET /metrics`.
//!
//! The node controller makes an [`ActorMetrics`] for each actor it spawns
//! and hands it over in the [`RuntimeCtx`](crate::RuntimeCtx); the actor's
//! runtime ([`Behaviour::run`](crate::Behaviour::run)) counts into it as
//! messages flow, and the node reads it when asked. It is shared by
//! reference (an `Arc` of atomics), so it works across the boundary of an
//! operator library loaded at run time, which has its own copy of this crate's
//! statics.
//!
//! Counters only grow; whoever reads them takes rates between two reads.
//! Operators can record their own values too, through
//! [`RuntimeCtx::metrics`](crate::RuntimeCtx::metrics): [`ActorMetrics::add`]
//! and [`ActorMetrics::set`].

use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, AtomicU64, Ordering::Relaxed},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::Serialize;

/// Where an actor is in its life.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ActorState {
    /// Spawned, not yet processing.
    Starting,
    Running,
    /// Ended without an error: stopped, or its inputs closed.
    Stopped,
    /// Ended with an error, e.g. its processor panicked.
    Failed,
}

impl ActorState {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => ActorState::Starting,
            1 => ActorState::Running,
            2 => ActorState::Stopped,
            _ => ActorState::Failed,
        }
    }
}

/// Counts for one destination.
#[derive(Debug, Default)]
struct PeerCounters {
    sent: AtomicU64,
    sent_bytes: AtomicU64,
    failed: AtomicU64,
}

/// One actor's counters. Made by the node; see the module comment.
#[derive(Debug)]
pub struct ActorMetrics {
    state: AtomicU8,
    error: Mutex<Option<String>>,
    started_ms: u64,
    received: AtomicU64,
    processed: AtomicU64,
    outputs: AtomicU64,
    busy_ns: AtomicU64,
    /// Messages waiting to be processed (all priorities).
    queued_in: AtomicU64,
    /// Outputs waiting to be sent.
    queued_out: AtomicU64,
    last_active_ms: AtomicU64,
    peers: Mutex<BTreeMap<String, Arc<PeerCounters>>>,
    custom: Mutex<BTreeMap<String, f64>>,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

impl Default for ActorMetrics {
    fn default() -> Self {
        ActorMetrics {
            state: AtomicU8::new(ActorState::Starting as u8),
            error: Mutex::new(None),
            started_ms: now_ms(),
            received: AtomicU64::new(0),
            processed: AtomicU64::new(0),
            outputs: AtomicU64::new(0),
            busy_ns: AtomicU64::new(0),
            queued_in: AtomicU64::new(0),
            queued_out: AtomicU64::new(0),
            last_active_ms: AtomicU64::new(0),
            peers: Mutex::new(BTreeMap::new()),
            custom: Mutex::new(BTreeMap::new()),
        }
    }
}

/// Sends to one destination: a handle that counts without a lookup.
#[derive(Debug, Clone)]
pub(crate) struct PeerMetrics(Arc<PeerCounters>);

impl PeerMetrics {
    pub(crate) fn sent(&self, bytes: Option<usize>) {
        self.0.sent.fetch_add(1, Relaxed);
        if let Some(b) = bytes {
            self.0.sent_bytes.fetch_add(b as u64, Relaxed);
        }
    }
    pub(crate) fn failed(&self) {
        self.0.failed.fetch_add(1, Relaxed);
    }
}

impl ActorMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn state(&self) -> ActorState {
        ActorState::from_u8(self.state.load(Relaxed))
    }

    pub(crate) fn set_running(&self) {
        self.state.store(ActorState::Running as u8, Relaxed);
    }

    /// Records how the actor ended: a failure is kept over a later stop, and
    /// the first error over later ones.
    pub(crate) fn ended(&self, error: Option<String>) {
        match error {
            Some(e) => {
                // The first error is the cause; later ones follow from it.
                self.error.lock().unwrap().get_or_insert(e);
                self.state.store(ActorState::Failed as u8, Relaxed);
            }
            None => {
                let _ = self.state.compare_exchange(
                    ActorState::Running as u8,
                    ActorState::Stopped as u8,
                    Relaxed,
                    Relaxed,
                );
                let _ = self.state.compare_exchange(
                    ActorState::Starting as u8,
                    ActorState::Stopped as u8,
                    Relaxed,
                    Relaxed,
                );
            }
        }
    }

    pub(crate) fn received(&self) {
        self.received.fetch_add(1, Relaxed);
    }

    /// One message processed, taking `busy`, giving `outputs` messages.
    pub(crate) fn processed(&self, busy: Duration, outputs: usize) {
        self.processed.fetch_add(1, Relaxed);
        self.outputs.fetch_add(outputs as u64, Relaxed);
        self.busy_ns.fetch_add(busy.as_nanos() as u64, Relaxed);
        self.last_active_ms.store(now_ms(), Relaxed);
    }

    pub(crate) fn queued(&self, inputs: usize, outputs: usize) {
        self.queued_in.store(inputs as u64, Relaxed);
        self.queued_out.store(outputs as u64, Relaxed);
    }

    /// The counters for sends to `to`.
    pub(crate) fn peer(&self, to: &str) -> PeerMetrics {
        let mut peers = self.peers.lock().unwrap();
        PeerMetrics(peers.entry(to.to_string()).or_default().clone())
    }

    /// Adds `by` to the operator's own counter `name`.
    pub fn add(&self, name: &str, by: f64) {
        *self
            .custom
            .lock()
            .unwrap()
            .entry(name.to_string())
            .or_default() += by;
    }

    /// Sets the operator's own value `name`.
    pub fn set(&self, name: &str, value: f64) {
        self.custom.lock().unwrap().insert(name.to_string(), value);
    }

    /// Everything, as of now.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            state: self.state(),
            error: self.error.lock().unwrap().clone(),
            started_ms: self.started_ms,
            last_active_ms: match self.last_active_ms.load(Relaxed) {
                0 => None,
                t => Some(t),
            },
            received: self.received.load(Relaxed),
            processed: self.processed.load(Relaxed),
            outputs: self.outputs.load(Relaxed),
            busy_ms: self.busy_ns.load(Relaxed) as f64 / 1e6,
            queued_in: self.queued_in.load(Relaxed),
            queued_out: self.queued_out.load(Relaxed),
            sent: self
                .peers
                .lock()
                .unwrap()
                .iter()
                .map(|(to, c)| {
                    (
                        to.clone(),
                        SentTo {
                            messages: c.sent.load(Relaxed),
                            bytes: c.sent_bytes.load(Relaxed),
                            failed: c.failed.load(Relaxed),
                        },
                    )
                })
                .collect(),
            custom: self.custom.lock().unwrap().clone(),
        }
    }
}

/// Sends to one destination so far.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SentTo {
    pub messages: u64,
    /// Encoded bytes; 0 to an actor on the same node (nothing is encoded).
    pub bytes: u64,
    /// Messages that couldn't be sent, given up on after the retries.
    pub failed: u64,
}

/// An actor's metrics at one moment.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MetricsSnapshot {
    pub state: ActorState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// When it was spawned, in ms since the Unix epoch.
    pub started_ms: u64,
    /// When it last finished processing a message.
    pub last_active_ms: Option<u64>,
    /// Messages that reached its processor, generators' included.
    pub received: u64,
    pub processed: u64,
    /// Messages its processor gave.
    pub outputs: u64,
    /// Time spent in its processor.
    pub busy_ms: f64,
    pub queued_in: u64,
    pub queued_out: u64,
    /// By destination.
    pub sent: BTreeMap<String, SentTo>,
    /// The operator's own values.
    pub custom: BTreeMap<String, f64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_and_snapshots() {
        let m = ActorMetrics::new();
        assert_eq!(m.state(), ActorState::Starting);
        m.set_running();
        m.received();
        m.received();
        m.processed(Duration::from_millis(3), 2);
        m.processed(Duration::from_millis(1), 0);
        let p = m.peer("b");
        p.sent(Some(100));
        p.sent(None);
        m.peer("b").failed();
        m.add("frames", 2.0);
        m.add("frames", 1.0);
        m.set("latency_ms", 7.5);
        let s = m.snapshot();
        assert_eq!(s.state, ActorState::Running);
        assert_eq!((s.received, s.processed, s.outputs), (2, 2, 2));
        assert!((s.busy_ms - 4.0).abs() < 1e-9);
        assert_eq!(
            s.sent["b"],
            SentTo {
                messages: 2,
                bytes: 100,
                failed: 1
            }
        );
        assert_eq!(s.custom["frames"], 3.0);
        assert_eq!(s.custom["latency_ms"], 7.5);
        assert!(s.last_active_ms.is_some());
    }

    #[test]
    fn a_failure_outlasts_the_stop() {
        let m = ActorMetrics::new();
        m.set_running();
        m.ended(Some("boom".into()));
        m.ended(Some("and then this".into()));
        m.ended(None);
        assert_eq!(m.state(), ActorState::Failed);
        assert_eq!(m.snapshot().error.as_deref(), Some("boom"));

        let m = ActorMetrics::new();
        m.set_running();
        m.ended(None);
        assert_eq!(m.state(), ActorState::Stopped);
    }
}
