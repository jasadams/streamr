//! Align a finite set of original-event responses before native evaluation.
use anyhow::{Result, ensure};
use arroyo_operator::CheckpointCounter;
use arroyo_types::SignalMessage;
use arroyo_types::event_response::{EVENT_RESPONSE_VERSION, EventContext, EventResponse};

pub(crate) struct EventPair {
    pub(crate) context: EventContext,
    pub(crate) branches: Vec<EventResponse>,
}

enum Phase {
    Empty,
    Waiting(Vec<Option<EventResponse>>),
    Processing(EventContext),
}

/// One slot per registered input, then caller-owned complete responses during
/// native evaluation and delivery. No map of events or additional admission.
pub(crate) struct EventPairState {
    root: (u32, u32),
    inputs: usize,
    nonce: Option<[u8; 16]>,
    next_sequence: Option<u64>,
    phase: Phase,
}
impl EventPairState {
    pub(crate) fn new(root_operator: u32, root_subtask: u32) -> Self {
        Self::with_inputs(root_operator, root_subtask, 2)
            .expect("two response inputs are supported")
    }
    pub(crate) fn with_inputs(
        root_operator: u32,
        root_subtask: u32,
        inputs: usize,
    ) -> Result<Self> {
        ensure!(
            inputs > 0,
            "response alignment requires a positive registered input count"
        );
        Ok(Self {
            root: (root_operator, root_subtask),
            inputs,
            nonce: None,
            next_sequence: Some(1),
            phase: Phase::Empty,
        })
    }
    pub(crate) fn blocked(&self, side: usize) -> bool {
        match &self.phase {
            Phase::Empty => false,
            Phase::Waiting(slots) => slots.get(side).is_some_and(Option::is_some),
            Phase::Processing(_) => true,
        }
    }
    pub(crate) fn pending(&self) -> bool {
        !matches!(self.phase, Phase::Empty)
    }
    /// Run-loop precondition before response dispatch (also for a chained head).
    /// A previously accepted barrier parks that input until both sides align;
    /// do not let E+1 enter the JOIN through a chained/binary callback bypass.
    pub(crate) fn before_response(
        &self,
        side: usize,
        checkpoints: &CheckpointCounter,
    ) -> Result<()> {
        ensure!(
            side < self.inputs,
            "event response input exceeds registered arity"
        );
        ensure!(
            checkpoints.all_clear(),
            "event response crossed an unaligned checkpoint prefix"
        );
        ensure!(
            !self.blocked(side),
            "event response entered an event-parked input"
        );
        Ok(())
    }
    pub(crate) fn before_response_with_closed(
        &self,
        side: usize,
        checkpoints: &CheckpointCounter,
        closed: &std::collections::HashSet<usize>,
    ) -> Result<()> {
        ensure!(
            closed.is_empty(),
            "event response follows a terminal branch input"
        );
        self.before_response(side, checkpoints)
    }
    pub(crate) fn check_eof(
        &self,
        side: usize,
        checkpoints: &CheckpointCounter,
        _closed: &std::collections::HashSet<usize>,
    ) -> Result<()> {
        ensure!(
            side < self.inputs,
            "event response input exceeds registered arity"
        );
        ensure!(
            !self.pending(),
            "input {side} EOF before matching event completed"
        );
        ensure!(
            checkpoints.all_clear(),
            "input {side} EOF during checkpoint alignment"
        );
        Ok(())
    }
    pub(crate) fn check_control_with_closed(
        &self,
        side: usize,
        control: &SignalMessage,
        checkpoints: &CheckpointCounter,
        closed: &std::collections::HashSet<usize>,
    ) -> Result<()> {
        ensure!(
            !closed.contains(&side),
            "control follows a terminal branch input"
        );
        ensure!(
            closed.is_empty() || !matches!(control, SignalMessage::Barrier(_)),
            "barrier follows a terminal branch input"
        );
        self.check_control(side, control, checkpoints)
    }
    /// Keep ordinary watermark handling; it is not an event completion.
    /// Example: W_L,E_L,W_R,E_R is FIFO-valid and must preserve pending E.
    pub(crate) fn check_control(
        &self,
        side: usize,
        control: &SignalMessage,
        checkpoints: &CheckpointCounter,
    ) -> Result<()> {
        self.before_maintenance(side)?;
        ensure!(
            !checkpoints.is_blocked(side),
            "control entered a checkpoint-parked input"
        );
        match &self.phase {
            Phase::Empty => Ok(()),
            Phase::Processing(_) => anyhow::bail!("control reentered a processing event callback"),
            Phase::Waiting(slots) => {
                ensure!(
                    slots[side].is_none(),
                    "control entered an event-parked input"
                );
                ensure!(
                    checkpoints.all_clear(),
                    "pending event crossed an earlier checkpoint prefix"
                );
                match control {
                    SignalMessage::Watermark(_) => Ok(()),
                    // With the shared root, both E responses precede a later
                    // barrier; an earlier barrier blocks E at before_response.
                    // Thus this side cannot reach a barrier before its mate E.
                    SignalMessage::Barrier(_) => {
                        anyhow::bail!("barrier overtook the missing event response on input {side}")
                    }
                    SignalMessage::Stop | SignalMessage::EndOfData => {
                        anyhow::bail!("input {side} closed before the matching event response")
                    }
                }
            }
        }
    }
    pub(crate) fn receive(
        &mut self,
        side: usize,
        response: EventResponse,
    ) -> Result<Option<EventPair>> {
        ensure!(
            side < self.inputs,
            "event response input exceeds registered arity"
        );
        let context = response.context;
        ensure!(
            context.version == EVENT_RESPONSE_VERSION,
            "unsupported event response version"
        );
        ensure!(
            (context.root_operator, context.root_subtask) == self.root,
            "event response root mismatch"
        );
        ensure!(
            self.next_sequence == Some(context.sequence),
            "event response sequence mismatch or exhausted sequence"
        );
        if let Some(expected) = self.nonce {
            ensure!(
                context.attempt_nonce == expected,
                "event response attempt mismatch"
            );
        }
        match &self.phase {
            Phase::Processing(_) => {
                anyhow::bail!("event response arrived before prior delivery completed")
            }
            Phase::Waiting(slots) => {
                ensure!(slots[side].is_none(), "duplicate branch response");
                ensure!(
                    slots
                        .iter()
                        .flatten()
                        .all(|pending| pending.context == context),
                    "branches disagree on original-event context/clock"
                );
            }
            Phase::Empty => {}
        }
        self.nonce = Some(context.attempt_nonce);
        let mut slots = match std::mem::replace(&mut self.phase, Phase::Processing(context)) {
            Phase::Empty => (0..self.inputs).map(|_| None).collect::<Vec<_>>(),
            Phase::Waiting(slots) => slots,
            Phase::Processing(_) => unreachable!("validated before replacement"),
        };
        slots[side] = Some(response);
        if slots.iter().all(Option::is_some) {
            Ok(Some(EventPair {
                context,
                branches: slots
                    .into_iter()
                    .map(|slot| slot.expect("all inputs complete"))
                    .collect(),
            }))
        } else {
            self.phase = Phase::Waiting(slots);
            Ok(None)
        }
    }
    /// Maintenance on an unfilled input precedes that input's pending event.
    /// Filled inputs are parked, and Processing remains parked through delivery.
    pub(crate) fn before_maintenance(&self, side: usize) -> Result<()> {
        ensure!(
            side < self.inputs && !self.blocked(side),
            "maintenance entered an event-parked input"
        );
        Ok(())
    }
    /// Call only AFTER native changes, final native probes and awaited output.
    /// If any step errors, fail the attempt; never finish and resume that owner.
    pub(crate) fn finish(&mut self, context: EventContext) -> Result<()> {
        let Phase::Processing(expected) = self.phase else {
            anyhow::bail!("no matched event pair is being processed");
        };
        ensure!(
            expected == context,
            "finished original-event context mismatch"
        );
        self.next_sequence = context.sequence.checked_add(1);
        self.phase = Phase::Empty;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;
    use arroyo_types::{CheckpointBarrier, Watermark};
    use std::time::SystemTime;
    fn response(sequence: u64, trigger: i64) -> EventResponse {
        EventResponse {
            native_body: None,
            context: EventContext {
                version: EVENT_RESPONSE_VERSION,
                root_operator: 5,
                root_subtask: 0,
                attempt_nonce: [7; 16],
                sequence,
                trigger_nanos: trigger,
            },
            rows: None,
            touched_keys: None,
        }
    }
    #[test]
    fn event_pair_both_arrival_orders_empty_responses_and_late_clock() -> Result<()> {
        for first in [0, 1] {
            let mut state = EventPairState::new(5, 0);
            assert!(state.receive(first, response(1, 20))?.is_none());
            assert!(state.blocked(first));
            assert!(!state.blocked(1 - first));
            let pair = state
                .receive(1 - first, response(1, 20))?
                .context("missing pair")?;
            assert_eq!(pair.branches[0].context, pair.branches[1].context);
            assert!(state.blocked(0) && state.blocked(1));
            assert!(state.receive(0, response(2, 5)).is_err());
            state.finish(pair.context)?;
            assert!(!state.pending());
            assert!(state.receive(0, response(2, 5))?.is_none());
            let pair = state
                .receive(1, response(2, 5))?
                .context("missing late pair")?;
            assert_eq!(pair.context.trigger_nanos, 5);
            state.finish(pair.context)?;
        }
        Ok(())
    }
    #[test]
    fn four_responses_allow_independent_prior_maintenance_without_crossing_the_cut() -> Result<()> {
        let mut state = EventPairState::with_inputs(5, 0, 4)?;
        let counter = CheckpointCounter::new(4);
        // Two ordinary branches finish while calendar and finalized-window
        // branches are still delivering the previous watermark's native work.
        for side in [1, 2] {
            assert!(state.receive(side, response(1, 5))?.is_none());
        }
        for side in [0, 3] {
            state.before_maintenance(side)?;
            assert!(state.before_maintenance(1).is_err());
            state.check_control(side, &SignalMessage::Watermark(Watermark::Idle), &counter)?;
        }
        assert!(state.receive(3, response(1, 5))?.is_none());
        assert!(state.before_maintenance(3).is_err());
        let complete = state
            .receive(0, response(1, 5))?
            .context("missing fourth response")?;
        assert_eq!(complete.branches.len(), 4);
        for side in 0..4 {
            assert!(state.before_maintenance(side).is_err());
            assert!(
                state
                    .check_control(side, &SignalMessage::Barrier(barrier()), &counter)
                    .is_err()
            );
        }
        state.finish(complete.context)?;
        for side in 0..4 {
            state.before_maintenance(side)?;
        }
        Ok(())
    }

    #[test]
    fn finite_responses_wait_for_three_inputs_and_preserve_prior_watermark_maintenance()
    -> Result<()> {
        let mut state = EventPairState::with_inputs(5, 0, 3)?;
        let counter = CheckpointCounter::new(3);
        let watermark = SignalMessage::Watermark(Watermark::Idle);
        // Optional branches reach E+1 while core is still processing the prior W.
        state.check_control(1, &watermark, &counter)?;
        assert!(state.receive(1, response(1, 5))?.is_none());
        assert!(state.receive(2, response(1, 5))?.is_none());
        assert!(state.blocked(1) && state.blocked(2) && !state.blocked(0));
        state.before_maintenance(0)?;
        assert!(state.before_maintenance(1).is_err());
        state.check_control(0, &watermark, &counter)?;
        assert!(
            state
                .check_control(0, &SignalMessage::Barrier(barrier()), &counter)
                .is_err()
        );
        let complete = state
            .receive(0, response(1, 5))?
            .context("missing third response")?;
        assert_eq!(complete.branches.len(), 3);
        assert!(
            complete
                .branches
                .iter()
                .all(|branch| branch.context == complete.context)
        );
        assert!((0..3).all(|side| state.blocked(side)));
        assert!(state.before_maintenance(0).is_err());
        state.finish(complete.context)?;
        assert!((0..3).all(|side| !state.blocked(side)));
        assert!(EventPairState::with_inputs(5, 0, 0).is_err());
        assert!(EventPairState::with_inputs(5, 0, 4).is_ok());
        let mut unary = EventPairState::with_inputs(5, 0, 1)?;
        let complete = unary
            .receive(0, response(1, 20))?
            .context("unary completion")?;
        unary.finish(complete.context)?;
        Ok(())
    }
    #[test]
    fn event_pair_rejects_duplicate_future_root_attempt_clock_and_barrier() -> Result<()> {
        let mut state = EventPairState::new(5, 0);
        assert!(state.receive(0, response(2, 10)).is_err());
        assert!(state.receive(0, response(1, 10))?.is_none());
        assert!(state.receive(0, response(1, 10)).is_err());
        assert!(state.receive(1, response(1, 5)).is_err());
        let mut wrong = response(1, 10);
        wrong.context.root_operator = 6;
        assert!(state.receive(1, wrong).is_err());
        let mut wrong = response(1, 10);
        wrong.context.attempt_nonce = [8; 16];
        assert!(state.receive(1, wrong).is_err());
        let counter = CheckpointCounter::new(2);
        for control in [
            SignalMessage::Barrier(barrier()),
            SignalMessage::Stop,
            SignalMessage::EndOfData,
        ] {
            assert!(state.check_control(1, &control, &counter).is_err());
        }
        let pair = state.receive(1, response(1, 10))?.context("missing pair")?;
        assert!(
            state
                .check_control(1, &SignalMessage::Barrier(barrier()), &counter)
                .is_err()
        );
        state.finish(pair.context)?;
        state.check_control(0, &SignalMessage::Barrier(barrier()), &counter)?;
        Ok(())
    }
    fn barrier() -> CheckpointBarrier {
        CheckpointBarrier {
            epoch: 1,
            min_epoch: 0,
            timestamp: SystemTime::UNIX_EPOCH,
            then_stop: false,
        }
    }
    #[test]
    fn event_pair_accepts_fifo_watermark_skew_without_finishing_pending_event() -> Result<()> {
        for first in [0, 1] {
            let mut state = EventPairState::new(5, 0);
            let counter = CheckpointCounter::new(2);
            let watermark = SignalMessage::Watermark(Watermark::EventTime(SystemTime::UNIX_EPOCH));
            state.check_control(first, &watermark, &counter)?; // W_L (or W_R)
            state.before_response(first, &counter)?;
            assert!(state.receive(first, response(1, 10))?.is_none()); // E_L
            state.check_control(1 - first, &watermark, &counter)?; // older W_R
            state.check_control(
                1 - first,
                &SignalMessage::Watermark(Watermark::Idle),
                &counter,
            )?;
            assert!(state.pending() && state.blocked(first) && !state.blocked(1 - first));
            state.before_response(1 - first, &counter)?;
            let pair = state
                .receive(1 - first, response(1, 10))?
                .context("watermark lost event")?; // E_R
            state.finish(pair.context)?;
            assert!(!state.pending());
        }
        Ok(())
    }
    #[test]
    fn event_pair_composes_actual_checkpoint_counter_with_response_and_control_parking()
    -> Result<()> {
        let mut state = EventPairState::new(5, 0);
        let mut counter = CheckpointCounter::new(2);
        let checkpoint = barrier();
        state.check_control(0, &SignalMessage::Barrier(checkpoint), &counter)?;
        assert!(!counter.mark(0, &checkpoint)); // earlier B_L parks L
        assert!(state.before_response(0, &counter).is_err());
        assert!(state.before_response(1, &counter).is_err()); // no cross-prefix E
        state.check_control(1, &SignalMessage::Watermark(Watermark::Idle), &counter)?;
        state.check_control(1, &SignalMessage::Barrier(checkpoint), &counter)?;
        assert!(counter.mark(1, &checkpoint)); // B_R aligns; both sides clear
        state.before_response(0, &counter)?;
        assert!(state.receive(0, response(1, 10))?.is_none());
        assert!(state.before_response(0, &counter).is_err()); // no chained E+1
        state.before_response(1, &counter)?;
        assert!(
            state
                .check_control(1, &SignalMessage::Barrier(checkpoint), &counter)
                .is_err()
        );
        let pair = state
            .receive(1, response(1, 10))?
            .context("missing post-checkpoint pair")?;
        state.finish(pair.context)?;
        state.check_control(
            0,
            &SignalMessage::Barrier(CheckpointBarrier {
                epoch: 2,
                ..checkpoint
            }),
            &counter,
        )?;
        Ok(())
    }
    #[test]
    fn event_pair_fresh_attempt_latches_new_nonce_and_sequence_exhaustion() -> Result<()> {
        let mut fresh = EventPairState::new(5, 0);
        let mut one = response(1, 5);
        one.context.attempt_nonce = [9; 16];
        assert!(fresh.receive(0, one.clone())?.is_none());
        let pair = fresh.receive(1, one)?.context("missing restored pair")?;
        fresh.finish(pair.context)?;
        fresh.next_sequence = Some(u64::MAX);
        let last = response(u64::MAX, i64::MIN);
        let mut last = last;
        last.context.attempt_nonce = [9; 16];
        assert!(fresh.receive(1, last.clone())?.is_none());
        let pair = fresh.receive(0, last)?.context("missing final pair")?;
        fresh.finish(pair.context)?;
        assert!(fresh.receive(0, response(0, 10)).is_err());
        fresh.check_control(0, &SignalMessage::EndOfData, &CheckpointCounter::new(2))?;
        let mut closed = std::collections::HashSet::new();
        closed.insert(0);
        assert!(
            fresh
                .before_response_with_closed(1, &CheckpointCounter::new(2), &closed)
                .is_err()
        );
        // EOF following an already explicit terminal is normal stream closure.
        fresh.check_eof(0, &CheckpointCounter::new(2), &closed)?;
        fresh.check_eof(1, &CheckpointCounter::new(2), &closed)?;
        fresh.check_control_with_closed(
            1,
            &SignalMessage::Watermark(Watermark::Idle),
            &CheckpointCounter::new(2),
            &closed,
        )?;
        assert!(
            fresh
                .check_control_with_closed(
                    1,
                    &SignalMessage::Barrier(barrier()),
                    &CheckpointCounter::new(2),
                    &closed
                )
                .is_err()
        );
        let mut pending = EventPairState::new(5, 0);
        assert!(pending.receive(0, response(1, 10))?.is_none());
        assert!(
            pending
                .check_eof(
                    1,
                    &CheckpointCounter::new(2),
                    &std::collections::HashSet::new()
                )
                .is_err()
        );
        Ok(())
    }
}
