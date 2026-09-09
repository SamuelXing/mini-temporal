//! # The event history
//!
//! A workflow execution *is* its event history. Everything else -- the worker,
//! the suspended coroutine, the local variables on its stack -- is a cache that
//! can be thrown away at any moment and rebuilt from these events.
//!
//! Two properties of this log carry the whole design, and they are different:
//!
//! - **Which** results are here. Order-independent, keyed by [`Seq`].
//! - **In what order** they were written. NOT reproducible, and not meant to
//!   be: the log freezes whatever arbitrary interleaving the real world
//!   produced. Replay must re-deliver events in exactly this order, or a
//!   workflow that raced two activities will pick a different winner on
//!   recovery than it did live. See [`History::results_in_order`].
//!
//! REAL TEMPORAL ~ `temporal/api/history/v1/message.proto` (`HistoryEvent`).
//! Ours has 9 attribute variants; the real one has ~50. Same shape.

use crate::command::Command;
use std::collections::HashSet;

/// Every payload in real Temporal is a protobuf `Payload` carrying bytes plus
/// encoding metadata. A `String` is enough to see the mechanism.
pub type Payload = String;

/// Identifies one thing the workflow asked for, in the order it asked.
///
/// This number is the linchpin of the design. It is handed out when
/// `ctx.activity(..)` is *called*, so it is anchored to Rust's specified
/// expression evaluation order -- a language guarantee -- and not to the order
/// a combinator happens to poll things in, which is a library detail.
/// (`tokio::select!` randomises its poll order by default. Anchoring identity
/// to poll order would be catastrophic.)
pub type Seq = u32;

/// The attributes of an event: *what* happened.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// Always event #1. `randomness_seed` is what makes `ctx.random_u64()`
    /// replay-stable, exactly as in real Temporal.
    WorkflowExecutionStarted { input: Payload, randomness_seed: u64 },

    /// A worker picked up a workflow task and is about to replay. Its
    /// timestamp is the workflow's notion of "now" for the code that runs in
    /// this task.
    WorkflowTaskStarted { attempt: u32 },

    ActivityTaskScheduled { seq: Seq, activity_type: String, input: Payload },
    ActivityTaskCompleted { seq: Seq, result: Payload },
    ActivityTaskFailed { seq: Seq, failure: String },

    TimerStarted { seq: Seq, fire_after_ms: u64 },
    TimerFired { seq: Seq },

    /// A value the worker computed locally and froze into history. Carries its
    /// own result: there is no separate "completed" event, because nothing was
    /// dispatched anywhere.
    MarkerRecorded { seq: Seq, marker_name: String, value: Payload },

    WorkflowExecutionCompleted { result: Payload },
    WorkflowExecutionFailed { failure: String },
}

/// An event as it sits in the log: attributes plus the two things every real
/// `HistoryEvent` carries, an id and a time.
#[derive(Debug, Clone, PartialEq)]
pub struct HistoryEvent {
    pub event_id: u64,
    pub time_ms: u64,
    pub attrs: Event,
}

/// What history says happened to command `seq`.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    Completed(Payload),
    Failed(String),
}

/// One recorded resolution, in the position the log put it.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordedResult {
    pub seq: Seq,
    pub outcome: Outcome,
    /// The event's timestamp. Feeds `ctx.now_ms()`.
    pub time_ms: u64,
}

/// An append-only log. In Phase 1 this moves behind a `HistoryStore` trait and
/// gets a SQLite implementation; the append becomes a conditional write on
/// `next_event_id`, which is how Temporal gets exactly-once history growth
/// under concurrent workers.
#[derive(Debug, Default, Clone)]
pub struct History {
    events: Vec<HistoryEvent>,
}

impl History {
    pub fn start(input: impl Into<Payload>) -> Self {
        let input = input.into();
        // Real Temporal derives this from the run id. Deriving it from the
        // input keeps demo output stable while still differing per workflow.
        let seed = fnv1a64(&input) | 1;
        History {
            events: vec![HistoryEvent {
                event_id: 1,
                time_ms: 0,
                attrs: Event::WorkflowExecutionStarted { input, randomness_seed: seed },
            }],
        }
    }

    /// Returns the 1-based event id, exactly like Temporal's `eventId`.
    pub fn append(&mut self, time_ms: u64, attrs: Event) -> u64 {
        let event_id = self.events.len() as u64 + 1;
        self.events.push(HistoryEvent { event_id, time_ms, attrs });
        event_id
    }

    pub fn events(&self) -> &[HistoryEvent] {
        &self.events
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn last_time_ms(&self) -> u64 {
        self.events.last().map(|e| e.time_ms).unwrap_or(0)
    }

    pub fn input(&self) -> Payload {
        match self.events.first().map(|e| &e.attrs) {
            Some(Event::WorkflowExecutionStarted { input, .. }) => input.clone(),
            _ => Payload::new(),
        }
    }

    pub fn randomness_seed(&self) -> u64 {
        match self.events.first().map(|e| &e.attrs) {
            Some(Event::WorkflowExecutionStarted { randomness_seed, .. }) => *randomness_seed,
            _ => 1,
        }
    }

    /// True once a terminal event has been written. A closed history is
    /// immutable forever.
    pub fn is_closed(&self) -> bool {
        matches!(
            self.events.last().map(|e| &e.attrs),
            Some(Event::WorkflowExecutionCompleted { .. } | Event::WorkflowExecutionFailed { .. })
        )
    }

    pub fn result(&self) -> Option<Result<Payload, String>> {
        match self.events.last().map(|e| &e.attrs) {
            Some(Event::WorkflowExecutionCompleted { result }) => Some(Ok(result.clone())),
            Some(Event::WorkflowExecutionFailed { failure }) => Some(Err(failure.clone())),
            _ => None,
        }
    }

    /// The commands this history says the workflow already issued, in order.
    ///
    /// On the next replay the workflow issues commands again from scratch.
    /// Comparing the new list against this one position by position is the
    /// entire non-determinism check.
    pub fn recorded_commands(&self) -> Vec<Command> {
        self.events
            .iter()
            .filter_map(|e| match &e.attrs {
                Event::ActivityTaskScheduled { seq, activity_type, input } => {
                    Some(Command::ScheduleActivity {
                        seq: *seq,
                        activity_type: activity_type.clone(),
                        input: input.clone(),
                    })
                }
                Event::TimerStarted { seq, fire_after_ms } => {
                    Some(Command::StartTimer { seq: *seq, fire_after_ms: *fire_after_ms })
                }
                Event::MarkerRecorded { seq, marker_name, value } => Some(Command::RecordMarker {
                    seq: *seq,
                    marker_name: marker_name.clone(),
                    value: value.clone(),
                }),
                _ => None,
            })
            .collect()
    }

    /// Values frozen by `ctx.side_effect`. Order-independent on purpose: a
    /// marker never blocks, so it can never be part of a race.
    pub fn markers(&self) -> std::collections::HashMap<Seq, Payload> {
        self.events
            .iter()
            .filter_map(|e| match &e.attrs {
                Event::MarkerRecorded { seq, value, .. } => Some((*seq, value.clone())),
                _ => None,
            })
            .collect()
    }

    /// Recorded resolutions **in log order**.
    ///
    /// The order matters as much as the contents. Replay hands these to the
    /// workflow one at a time, polling in between, so that a workflow which
    /// raced two activities observes the same winner it observed live. A
    /// lookup table keyed by `Seq` would lose exactly this information -- and
    /// silently, only for workflows that use concurrency.
    pub fn results_in_order(&self) -> Vec<RecordedResult> {
        self.events
            .iter()
            .filter_map(|e| {
                let (seq, outcome) = match &e.attrs {
                    Event::ActivityTaskCompleted { seq, result } => {
                        (*seq, Outcome::Completed(result.clone()))
                    }
                    Event::ActivityTaskFailed { seq, failure } => {
                        (*seq, Outcome::Failed(failure.clone()))
                    }
                    Event::TimerFired { seq } => (*seq, Outcome::Completed(Payload::new())),
                    _ => return None,
                };
                Some(RecordedResult { seq, outcome, time_ms: e.time_ms })
            })
            .collect()
    }

    /// Which commands already have a result. Order-independent on purpose:
    /// this one is only used by the driver to find outstanding work.
    pub fn resolved_seqs(&self) -> HashSet<Seq> {
        self.results_in_order().into_iter().map(|r| r.seq).collect()
    }

    /// Roughly what `temporal workflow show` prints.
    pub fn pretty(&self) -> String {
        let mut s = String::new();
        for e in &self.events {
            let line = match &e.attrs {
                Event::WorkflowExecutionStarted { input, .. } => {
                    format!("WorkflowExecutionStarted        input={input}")
                }
                Event::WorkflowTaskStarted { attempt } => {
                    format!("WorkflowTaskStarted             attempt={attempt}")
                }
                Event::ActivityTaskScheduled { seq, activity_type, input } => {
                    format!("ActivityTaskScheduled     [{seq}]  {activity_type}({input})")
                }
                Event::ActivityTaskCompleted { seq, result } => {
                    format!("ActivityTaskCompleted     [{seq}]  -> {result}")
                }
                Event::ActivityTaskFailed { seq, failure } => {
                    format!("ActivityTaskFailed        [{seq}]  !! {failure}")
                }
                Event::TimerStarted { seq, fire_after_ms } => {
                    format!("TimerStarted              [{seq}]  {fire_after_ms}ms")
                }
                Event::TimerFired { seq } => format!("TimerFired                [{seq}]"),
                Event::MarkerRecorded { seq, marker_name, value } => {
                    format!("MarkerRecorded            [{seq}]  {marker_name}={value}")
                }
                Event::WorkflowExecutionCompleted { result } => {
                    format!("WorkflowExecutionCompleted      -> {result}")
                }
                Event::WorkflowExecutionFailed { failure } => {
                    format!("WorkflowExecutionFailed         !! {failure}")
                }
            };
            s.push_str(&format!("{:>3}  t={:<6} {line}\n", e.event_id, e.time_ms));
        }
        s
    }
}

fn fnv1a64(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x1000_0000_01b3);
    }
    h
}
