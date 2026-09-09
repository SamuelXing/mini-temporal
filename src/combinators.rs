//! # Concurrency inside a workflow
//!
//! A note on why this file is so short.
//!
//! The Go SDK ships a full coroutine dispatcher; the Java SDK ships
//! `DeterministicRunner` with its own threads; the Python SDK implements an
//! entire `asyncio` event loop. They need all of that because their concurrency
//! primitives are *detached*: a goroutine runs on its own, so the SDK must own
//! the scheduler to make "run until every coroutine is blocked" deterministic.
//!
//! Rust hands us most of that for free. A composed future is a tree, and one
//! `poll` of the root drives the entire tree to quiescence in a single,
//! deterministic pass. `ExecuteUntilAllBlocked` is just `root.poll()`.
//!
//! Detached tasks still need a scheduler -- that is `WfContext::spawn` plus the
//! fixpoint loop in `driver::run_until_all_blocked`.
//!
//! One rule these combinators must obey: **poll order is fixed**. `select2`
//! always polls the left branch first. `tokio::select!` randomises branch order
//! by default to avoid starvation, which is exactly right for a server and
//! exactly wrong for a workflow.

use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::task::Poll;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Either<A, B> {
    Left(A),
    Right(B),
}

/// Wait for both. Commands are issued in argument evaluation order, so both
/// activities are scheduled before either is awaited.
pub async fn join2<A, B>(a: A, b: B) -> (A::Output, B::Output)
where
    A: Future,
    B: Future,
{
    let mut a = Box::pin(a);
    let mut b = Box::pin(b);
    let mut ra = None;
    let mut rb = None;
    poll_fn(move |cx| {
        if ra.is_none() {
            if let Poll::Ready(v) = a.as_mut().poll(cx) {
                ra = Some(v);
            }
        }
        if rb.is_none() {
            if let Poll::Ready(v) = b.as_mut().poll(cx) {
                rb = Some(v);
            }
        }
        match (ra.take(), rb.take()) {
            (Some(x), Some(y)) => Poll::Ready((x, y)),
            (x, y) => {
                ra = x;
                rb = y;
                Poll::Pending
            }
        }
    })
    .await
}

/// Take whichever resolves first. Left is polled first, always.
///
/// Correctness here does not rest on the bias -- it rests on the driver feeding
/// recorded results to the workflow one at a time in history order, so that on
/// replay the branch that won live is the only one resolved when the select is
/// polled. The fixed bias is what keeps the *tie* deterministic.
pub async fn select2<A, B>(a: A, b: B) -> Either<A::Output, B::Output>
where
    A: Future,
    B: Future,
{
    let mut a = Box::pin(a);
    let mut b = Box::pin(b);
    poll_fn(move |cx| {
        if let Poll::Ready(v) = a.as_mut().poll(cx) {
            return Poll::Ready(Either::Left(v));
        }
        if let Poll::Ready(v) = b.as_mut().poll(cx) {
            return Poll::Ready(Either::Right(v));
        }
        Poll::Pending
    })
    .await
}

/// Wait for all of them, preserving input order in the output.
pub async fn join_all<F: Future>(futs: Vec<F>) -> Vec<F::Output> {
    let mut futs: Vec<Pin<Box<F>>> = futs.into_iter().map(Box::pin).collect();
    let mut out: Vec<Option<F::Output>> = (0..futs.len()).map(|_| None).collect();
    poll_fn(move |cx| {
        for (i, f) in futs.iter_mut().enumerate() {
            if out[i].is_none() {
                if let Poll::Ready(v) = f.as_mut().poll(cx) {
                    out[i] = Some(v);
                }
            }
        }
        if out.iter().all(Option::is_some) {
            Poll::Ready(out.iter_mut().map(|s| s.take().unwrap()).collect())
        } else {
            Poll::Pending
        }
    })
    .await
}
