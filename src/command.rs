//! # Commands: what the workflow *asks for*
//!
//! The distinction between a Command and an Event is the one people get wrong
//! most often, so it is worth being pedantic:
//!
//! - A **Command** is an intent, produced by replaying workflow code. It is
//!   cheap, repeatable, and produced fresh on every single replay.
//! - An **Event** is a durable fact, produced by the server. It is written once
//!   and never changes.
//!
//! The workflow NEVER performs I/O. It cannot call an HTTP API, read a clock,
//! or generate a UUID. All it can do is return a list of commands and stop.
//! Something on the other side of that boundary decides what to actually do and
//! records the outcome as an event.
//!
//! That boundary is why the whole thing works. Everything on the workflow side
//! is a pure function of history; everything non-deterministic lives on the
//! other side and has its result frozen into the log.
//!
//! REAL TEMPORAL ~ `temporal/api/command/v1/message.proto`.

use crate::history::{Event, Payload, Seq};

#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    ScheduleActivity { seq: Seq, activity_type: String, input: Payload },
    StartTimer { seq: Seq, fire_after_ms: u64 },
    /// Freeze a locally-computed value into history. The one command that
    /// carries its own result, because the worker computed it inline rather
    /// than asking the server to do anything.
    RecordMarker { seq: Seq, marker_name: String, value: Payload },
}

impl Command {
    pub fn seq(&self) -> Seq {
        match self {
            Command::ScheduleActivity { seq, .. }
            | Command::StartTimer { seq, .. }
            | Command::RecordMarker { seq, .. } => *seq,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Command::ScheduleActivity { .. } => "ScheduleActivity",
            Command::StartTimer { .. } => "StartTimer",
            Command::RecordMarker { .. } => "RecordMarker",
        }
    }

    /// Does this command block the workflow until the server answers?
    /// `RecordMarker` does not -- it resolves inline.
    pub fn is_blocking(&self) -> bool {
        !matches!(self, Command::RecordMarker { .. })
    }

    /// The event the server writes when it accepts this command.
    pub fn to_event(&self) -> Event {
        match self {
            Command::ScheduleActivity { seq, activity_type, input } => Event::ActivityTaskScheduled {
                seq: *seq,
                activity_type: activity_type.clone(),
                input: input.clone(),
            },
            Command::StartTimer { seq, fire_after_ms } => {
                Event::TimerStarted { seq: *seq, fire_after_ms: *fire_after_ms }
            }
            Command::RecordMarker { seq, marker_name, value } => Event::MarkerRecorded {
                seq: *seq,
                marker_name: marker_name.clone(),
                value: value.clone(),
            },
        }
    }

    /// Does a freshly-replayed command line up with what history recorded?
    ///
    /// We compare identity (kind, seq, name) and not the payload. Real Temporal
    /// is similarly lenient: it flags non-determinism when the *shape* of the
    /// command stream changes, not when an argument's bytes differ.
    ///
    /// This is the honest limit of the check. A workflow that reads the real
    /// clock, branches on it, and still emits the same command shape will pass
    /// this test and compute different values. Detection catches structure;
    /// only a sandbox or discipline catches the rest.
    pub fn matches(&self, recorded: &Command) -> bool {
        match (self, recorded) {
            (
                Command::ScheduleActivity { seq: a, activity_type: at, .. },
                Command::ScheduleActivity { seq: b, activity_type: bt, .. },
            ) => a == b && at == bt,
            (Command::StartTimer { seq: a, .. }, Command::StartTimer { seq: b, .. }) => a == b,
            (
                Command::RecordMarker { seq: a, marker_name: an, .. },
                Command::RecordMarker { seq: b, marker_name: bn, .. },
            ) => a == b && an == bn,
            _ => false,
        }
    }

    pub fn describe(&self) -> String {
        match self {
            Command::ScheduleActivity { seq, activity_type, input } => {
                format!("[{seq}] ScheduleActivity {activity_type}({input})")
            }
            Command::StartTimer { seq, fire_after_ms } => {
                format!("[{seq}] StartTimer {fire_after_ms}ms")
            }
            Command::RecordMarker { seq, marker_name, value } => {
                format!("[{seq}] RecordMarker {marker_name}={value}")
            }
        }
    }
}
