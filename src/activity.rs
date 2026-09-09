//! # Activities: the only place real work happens
//!
//! An activity is an ordinary, arbitrarily non-deterministic function. It may
//! call APIs, read clocks, charge credit cards. The engine guarantees it will
//! be attempted *at least once* and that its result, once recorded, is frozen
//! into history forever.
//!
//! At-least-once, not exactly-once. If the worker dies after the activity's
//! side effect but before its result reaches the log, the activity runs again
//! on recovery. `examples/order_saga.rs --crash-before-record` demonstrates
//! this happening, which is the shortest possible explanation of why activities
//! must be idempotent.
//!
//! REAL TEMPORAL ~ `sdk-core/sdk/src/activity_context.rs`, plus the activity
//! task dispatch in `service/matching/`.

use crate::history::Payload;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

/// One real invocation of an activity function. Recorded so tests and demos can
/// assert on how many times a side effect actually happened.
#[derive(Debug, Clone, PartialEq)]
pub struct Execution {
    pub activity_type: String,
    pub input: Payload,
}

type ActivityFn = Box<dyn Fn(Payload) -> Result<Payload, String>>;

#[derive(Default)]
pub struct ActivityRegistry {
    fns: HashMap<String, ActivityFn>,
    executions: Rc<RefCell<Vec<Execution>>>,
}

impl ActivityRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a registry that writes into an existing side-effect log.
    ///
    /// Used to model a worker restart: the new process gets fresh closures and
    /// no memory of the old run, but the outside world -- the log of what was
    /// really executed -- carries over, because the real world does.
    pub fn with_log(log: Rc<RefCell<Vec<Execution>>>) -> Self {
        ActivityRegistry { fns: HashMap::new(), executions: log }
    }

    pub fn register(
        &mut self,
        activity_type: &str,
        f: impl Fn(Payload) -> Result<Payload, String> + 'static,
    ) -> &mut Self {
        self.fns.insert(activity_type.to_string(), Box::new(f));
        self
    }

    /// The audit log of real side effects. Shared handle: it deliberately
    /// survives a simulated worker crash, the way the outside world does.
    pub fn executions(&self) -> Rc<RefCell<Vec<Execution>>> {
        self.executions.clone()
    }

    pub fn execution_count(&self, activity_type: &str) -> usize {
        self.executions
            .borrow()
            .iter()
            .filter(|e| e.activity_type == activity_type)
            .count()
    }

    pub(crate) fn invoke(&self, activity_type: &str, input: &Payload) -> Result<Payload, String> {
        self.executions.borrow_mut().push(Execution {
            activity_type: activity_type.to_string(),
            input: input.clone(),
        });
        match self.fns.get(activity_type) {
            Some(f) => f(input.clone()),
            None => Err(format!("no activity registered for type `{activity_type}`")),
        }
    }
}
