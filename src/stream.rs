//! Timebase assembly: turning USB reads into correctly *placed* sample blocks.
//!
//! # What this module exists to prevent
//!
//! The PPK2 streams continuously at [`SPS_MAX`]. Sample *spacing* is therefore
//! free — the device's crystal defines it — and the only hard question is where
//! the run of samples sits on the wall clock. Answering that from a
//! monotonically advancing sample index (`anchor + n * period`) is what produced
//! the failure this module was written for: `n` advances only when samples
//! *arrive*, wall time advances regardless, so any sample-time not received is
//! permanently subtracted from the clock. The output stays perfectly contiguous
//! and monotonic while sliding arbitrarily far into the past — measured at 7
//! minutes 13 seconds of drift after an overnight run, with nothing to detect
//! because there are no gaps and no collisions.
//!
//! The fix is to stop carrying an anchor at all. Every block is anchored from
//! the read that delivered it, and no anchor outlives a single block. Crystal
//! skew over a 1024-sample block is 0.5 µs at 50 ppm, so an anchor with that
//! lifetime needs none of the machinery a long-lived one does.
//!
//! # The one hard part: which reads may be used as anchors
//!
//! A read's return time is only a statement about capture time if that read
//! *drained the port*. A read that swept up a backlog says nothing: measured on
//! macOS, four consecutive 1020-byte reads arrived 253 µs apart while each
//! carried 2.55 ms of samples. Stamping from those arrival times in either
//! direction — forward from the first sample or back-dated from the last —
//! produces overlapping, non-monotonic output.
//!
//! So reads are classified (see [`probe_indicates_drained`]), and:
//!
//! * a read that drained the port anchors a block, which is stamped and emitted;
//! * a read that did not is *held* in a ring buffer, un-emitted;
//! * when a draining read finally arrives, the held run is **pinned backward**
//!   so its last sample sits immediately before the new block's first sample,
//!   and the whole thing is emitted as one correctly-placed run.
//!
//! Backward pinning is not an edge case: under CPU saturation 61.65% of samples
//! arrived inside untrusted runs.
//!
//! # Nothing held is trusted for longer than [`STALL_GUARD`]
//!
//! Held samples are only placeable relative to the *next* draining read. If that
//! read is a second late, the held run is stale and pinning it backward would
//! place old samples as though they were current — the original bug, rebuilt. So
//! a stall discards the ring and reports [`MeasurementMatch::Dropped`].
//!
//! The guard is measured on [`SystemTime`], not [`Instant`], because the case it
//! most has to catch is a host suspend, and a measured 225-second suspend
//! appeared to [`Instant`] as **622 ms** — a 360× under-report that lands below
//! any plausible monotonic threshold. Both clocks are carried per read
//! ([`ReadClocks`]) because a suspend and an ordinary device stall have identical
//! read-level signatures and the clock *pair* is the only thing that separates
//! them.

use std::collections::VecDeque;
use std::time::{Duration, Instant, SystemTime};

use crate::measurement::{
    Measurement, MeasurementAccumulator, MeasurementIterExt, MeasurementMatch,
};
use crate::types::{LogicPortPins, Metadata};
use crate::SPS_MAX;

/// Nanoseconds between two consecutive raw device samples.
///
/// [`SPS_MAX`] divides 1e9 exactly, so this is exact and no rounding error can
/// accumulate across a block however long the block is.
const RAW_SAMPLE_PERIOD_NANOS: u64 = 1_000_000_000 / SPS_MAX as u64;

/// Spacing of two consecutive raw device samples: exactly 1/[`SPS_MAX`] s.
///
/// This is the spacing every emitted block guarantees *internally*. Blocks carry
/// no such guarantee relative to one another.
pub const RAW_SAMPLE_PERIOD: Duration = Duration::from_nanos(RAW_SAMPLE_PERIOD_NANOS);

/// How long a run of un-anchored samples may be held before it is considered
/// stale and discarded.
///
/// # Why one second, and why on `SystemTime`
///
/// The threshold is not derived from an observed maximum. That methodology is
/// what produced the constellation of bounds that made the original bug
/// unfixable: every one of them (150 ms deficit, 2 s repair, 2 s gap shift,
/// 500–1000 ppm rate bounds) was a plausible number derived from something
/// somebody had measured, and every one of them refused to act once reality
/// exceeded it. One second is chosen instead as a bound the ring is *sized to*,
/// so overflow is impossible before the guard fires, by construction.
///
/// For scale: the worst stall observed under CPU saturation was 417.6 ms, whose
/// drain delivered ~53,856 samples. This leaves ~2.4× headroom.
///
/// It is evaluated on [`SystemTime`] because a host suspend — the dominant real
/// cause, firing roughly every 4 minutes on a sleeping laptop, ~200 times
/// overnight — is invisible to [`Instant`]: a measured 225 s suspend advanced
/// the monotonic clock by 622 ms.
pub const STALL_GUARD: Duration = Duration::from_secs(1);

/// Capacity of the held-sample ring, in raw samples.
///
/// Deliberately equal to [`STALL_GUARD`] × [`SPS_MAX`]: the guard fires before
/// the ring can fill, so overflow is unreachable rather than merely unlikely.
/// 100,000 samples is 400 KB.
pub const RING_CAPACITY_SAMPLES: usize = SPS_MAX;

/// Largest backwards seam step still treated as transport jitter rather than as
/// a wall-clock discontinuity. See [`StreamAssembler`]'s seam-guard discussion.
pub const SEAM_MAX_OVERLAP: Duration = STALL_GUARD;

/// Whether a follow-up non-blocking probe returning `probe_filled` bytes means
/// the preceding read drained the port.
///
/// The rule is **strictly zero**. Any byte at all behind the read means the read
/// did not drain the port, so its arrival time says nothing about when its
/// samples were captured, and it may not anchor a block.
///
/// # Why not a small tolerance
///
/// A tolerance is tempting, because the probe costs a syscall and the device
/// streams ~4 bytes every 10 µs, so a few bytes can genuinely arrive in the gap
/// between the read returning and the probe issuing. That much is real.
///
/// The tempting threshold is 64 bytes — but on macOS 64 bytes is exactly one USB
/// full-speed bulk packet, so `<= 64` does not tolerate instrument noise, it
/// accepts up to a whole packet of genuine backlog, and with it a ~160 µs
/// placement error. Measured on the loaded-macOS capture, strict zero classifies
/// 97.88% of reads trustable against 99.22% for `<= 64`. Those 1.34 percentage
/// points cost nothing: the reads are still delivered in full, just placed by
/// backward pinning against the next draining read instead of anchoring
/// themselves. Being conservative here is free, and a strict rule is provable
/// where a threshold is only approximate.
///
/// # Why drainage rather than read size
///
/// A `filled < requested` heuristic was measured on both platforms and rejected:
///
/// | | Windows | macOS idle | macOS under load |
/// |---|---|---|---|
/// | read-size rule says trustable | 0.00% | 99.83% | 79.66% |
/// | drain rule says trustable | 100% | 99.97% | 97.88% |
///
/// On Windows the driver coalesces to fill the request, so size carries no
/// information whatsoever. On loaded macOS the size rule needlessly withholds
/// 18.89% of reads that had in fact drained the port — and, far worse,
/// *over*-trusts in the other direction: 424 reads returned a single packet
/// while up to 1020 bytes (2.55 ms) were still queued behind them. That is the
/// dangerous direction, since it yields confidently wrong timestamps rather than
/// merely conservative ones.
///
/// Trustability's *meaning* is platform-independent; only its observation is
/// not. Encoding a macOS-specific observation as the definition is what made
/// Windows look broken when it is in fact the cleaner platform.
pub fn probe_indicates_drained(probe_filled: usize) -> bool {
    probe_filled == 0
}

/// Consecutive zero-byte reads after which the stream is declared dead.
///
/// # This is a backstop, not the mechanism
///
/// The application detects a suspend and stops gathering of its own accord, so
/// it holds primary responsibility for shutting the stream down. This crate's
/// only job is to allow a reasonable window to be told to stop before stopping
/// itself, rather than spinning on an empty port forever. Ten reads is that
/// window. It is explicitly NOT the primary mechanism and should not be tuned as
/// though it were.
///
/// For scale: a host suspend produces exactly ONE zero-byte read before the
/// stream recovers, measured across both capture runs. Ten is far clear of that.
pub const DEAD_STREAM_EMPTY_READS: u32 = 10;

/// Minimum silence before the stream may be declared dead, regardless of how
/// many reads that took.
///
/// # Why a count alone is not enough
///
/// A read's duration is a platform property, not a constant. On unix the port
/// timeout is 500 ms, so [`DEAD_STREAM_EMPTY_READS`] empty reads is the intended
/// ~5 second window. On Windows the driver is programmed to coalesce with a 5 ms
/// window, so the same ten reads take **50 ms** — and the stream is legitimately
/// silent for longer than that at startup, between `AverageStart` being written
/// and the device actually streaming. A pure count would kill every Windows
/// measurement before it began.
///
/// Requiring both conditions makes the backstop mean the same thing on both
/// platforms: roughly five seconds of silence, and enough reads to be sure it
/// was silence rather than one unlucky timeout.
pub const DEAD_STREAM_MIN_SILENCE: Duration = Duration::from_secs(5);

/// What the backstop observed when it fired. Carries the evidence so the error
/// can state what actually happened rather than just that something did.
#[derive(Debug, Clone, Copy)]
pub struct DeadStream {
    /// Consecutive reads that returned no data.
    pub reads: u32,
    /// How long it has been since a read last delivered any bytes.
    pub elapsed: Duration,
}

/// Tracks consecutive zero-byte reads so a stream that has genuinely stopped can
/// be declared dead instead of spinning forever.
///
/// A zero-byte read is NOT an error — 3.75% of Windows reads are routinely empty
/// with `TimedOut`, and a host suspend produces one on every platform. Treating
/// any single one as fatal is the bug this guard replaces: it killed the
/// measurement worker on every suspend, roughly 200 times an overnight run, and
/// a dead worker cannot observe the stream recovering.
///
/// So silence is tolerated, and only *sustained* silence is fatal. See
/// [`DEAD_STREAM_EMPTY_READS`] and [`DEAD_STREAM_MIN_SILENCE`] for the two
/// conditions and why both are needed.
#[derive(Debug, Clone, Copy)]
pub struct SilenceGuard {
    consecutive: u32,
    last_data_at: Instant,
}

impl SilenceGuard {
    /// Start the guard. `now` seeds the silence clock, so a device that never
    /// streams at all is still eventually declared dead.
    pub fn new(now: Instant) -> Self {
        Self {
            consecutive: 0,
            last_data_at: now,
        }
    }

    /// Record a read that delivered bytes. Resets both conditions.
    pub fn on_data(&mut self, now: Instant) {
        self.consecutive = 0;
        self.last_data_at = now;
    }

    /// Record a read that delivered nothing, however the platform reported it —
    /// an empty-kind error or a zero-length `Ok`.
    ///
    /// Returns `Some` once the stream should be declared dead.
    pub fn on_empty_read(&mut self, now: Instant) -> Option<DeadStream> {
        self.consecutive = self.consecutive.saturating_add(1);
        let elapsed = now.saturating_duration_since(self.last_data_at);
        if self.consecutive < DEAD_STREAM_EMPTY_READS || elapsed < DEAD_STREAM_MIN_SILENCE {
            return None;
        }
        Some(DeadStream {
            reads: self.consecutive,
            elapsed,
        })
    }
}

/// The pair of clocks read together the instant a USB read returns.
///
/// Both are needed. [`SystemTime`] is the timebase the samples are placed on and
/// the only clock that sees a host suspend. [`Instant`] is immune to NTP steps
/// and manual clock changes, and is what distinguishes "the host was asleep" from
/// "the wall clock was adjusted" — two situations with identical read-level
/// signatures.
#[derive(Debug, Clone, Copy)]
pub struct ReadClocks {
    /// Wall clock at the instant the read returned.
    pub sys: SystemTime,
    /// Monotonic clock at the same instant.
    pub mono: Instant,
}

impl ReadClocks {
    /// Sample both clocks now.
    ///
    /// Call this immediately after `read()` returns and before anything else —
    /// in particular before the drain probe and before parsing. Everything after
    /// the read adds latency that would be silently folded into the timebase.
    pub fn now() -> Self {
        Self {
            sys: SystemTime::now(),
            mono: Instant::now(),
        }
    }
}

/// Why a run of held samples was discarded. Recorded for diagnostics; the
/// response is the same either way, because both lose the data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StallKind {
    /// The wall clock jumped forward far more than the monotonic clock did: the
    /// host was suspended. Only ~30 ms of stream survives a suspend (the
    /// device-side buffer), versus ~538 ms after a host-awake stall, so this is
    /// total loss for the suspended interval and must be reported as such.
    HostSuspend,
    /// Both clocks agree that a long time passed: the device or the driver
    /// stopped delivering.
    DeviceStall,
    /// The wall clock moved backwards or was stepped: samples cannot be placed
    /// relative to anything that came before.
    ClockStep,
}

/// Assembles raw USB reads into timestamped, correctly-placed
/// [`MeasurementMatch`]es.
///
/// Feed it every read in order via [`StreamAssembler::on_read`]; it emits into a
/// caller-supplied `Vec`. It owns the parser, the held-sample ring, the
/// skipped-sample accounting and the seam guard, so all of the timebase's
/// invariants live in one place and can be tested without hardware.
///
/// # Emission contract
///
/// * `read_at` on an emitted [`MeasurementMatch::Match`] / `NoMatch` is the
///   **capture time of the first raw sample** averaged into it, back-dated from
///   a drain-verified anchor. It is not the instant any read returned.
/// * Consecutive emissions from one block are spaced by exactly
///   `chunk × `[`RAW_SAMPLE_PERIOD`], with no gaps inside the block.
/// * Nothing is promised *across* blocks. Sample loss is reported out of band as
///   [`MeasurementMatch::Dropped`], and a backwards seam as
///   [`MeasurementMatch::SeamOverlap`].
///
/// # The seam guard
///
/// Transport delay is up to ~8 ms and roughly constant, but it varies. When it
/// varies downwards, a block anchored from a draining read can start *before*
/// the previous block ended. That must never leave this crate: downstream
/// seam-dedupe silently DROPS records whose first timestamp goes backwards, so
/// the symptom would be missing data with no error anywhere.
///
/// So the first timestamp of every block is compared against the last one
/// delivered, and an overlap is resolved by **trimming samples off the front of
/// the block**, not by shifting the block forward. Trimming keeps every sample
/// that *is* delivered at its true computed time and makes the loss an honest
/// absence; shifting would deliver a known-wrong time for every sample in the
/// block. The trim is reported as [`MeasurementMatch::SeamOverlap`], which is
/// deliberately a different event from `Dropped`: `Dropped` means there is a hole
/// in the timeline, `SeamOverlap` means the transport delay wobbled and there is
/// no hole. Conflating them would send someone hunting a USB fault that does not
/// exist.
///
/// An overlap larger than [`SEAM_MAX_OVERLAP`] is not jitter — the wall clock
/// moved backwards — and is reported as a timeline break (`Dropped`) instead,
/// after which the guard re-bases. Trimming there would discard everything until
/// real time caught up with the old clock.
pub struct StreamAssembler {
    /// Raw samples per emitted measurement: `SPS_MAX / sps`, at least 1.
    chunk: usize,
    /// Logic-port pattern emitted measurements must match.
    pins: LogicPortPins,
    /// The frame parser. Owned here so that the skipped-sample accounting and
    /// the partial-frame remainder cannot get out of step with the ring.
    accumulator: MeasurementAccumulator,
    /// Samples parsed but not yet emitted, oldest first. Bounded at
    /// [`RING_CAPACITY_SAMPLES`].
    ///
    /// Holds two different things at once, deliberately: the sub-`chunk`
    /// remainder of the last emitted block (which is contiguous with whatever
    /// arrives next and so may be completed across the block boundary), and any
    /// samples held because no draining read has arrived yet.
    pending: VecDeque<Measurement>,
    /// Raw samples the DEVICE skipped, detected via its 6-bit counter, not yet
    /// reported. Reset only on a successful emission, so summing the emitted
    /// values reconstructs the exact total.
    missed: usize,
    /// Raw samples THIS CRATE discarded (stall or ring overflow), not yet
    /// reported.
    dropped: u64,
    /// A [`MeasurementMatch::Dropped`] has been emitted and no data has been
    /// emitted since. Suppresses a storm of drop events while a stall repeats.
    drop_signalled: bool,
    /// Capture time of the last raw sample handed to the consumer. `None` before
    /// anything has been delivered, and after a wall-clock step re-bases the
    /// timeline.
    last_delivered: Option<SystemTime>,
    /// Clocks from the last read that delivered bytes, for the stall guard.
    last_read: Option<ReadClocks>,
}

impl StreamAssembler {
    /// Create an assembler for a stream decimated to `sps` samples/sec, emitting
    /// only measurements whose logic port matches `pins`.
    ///
    /// `metadata` must be current: it carries the calibration the parser applies.
    pub fn new(metadata: Metadata, sps: usize, pins: LogicPortPins) -> Self {
        Self {
            chunk: (SPS_MAX / sps.max(1)).max(1),
            pins,
            accumulator: MeasurementAccumulator::new(metadata),
            pending: VecDeque::with_capacity(RING_CAPACITY_SAMPLES),
            missed: 0,
            dropped: 0,
            drop_signalled: false,
            last_delivered: None,
            last_read: None,
        }
    }

    /// Number of raw samples averaged into one emitted measurement.
    pub fn chunk(&self) -> usize {
        self.chunk
    }

    /// Samples currently held un-emitted. Diagnostic; the value is meaningful
    /// only between calls.
    pub fn held(&self) -> usize {
        self.pending.len()
    }

    /// Account for one USB read.
    ///
    /// Call this ONLY for reads that returned bytes. A zero-byte read (routine
    /// on Windows: 3.75% of reads, and the strongest drain signal there is)
    /// delivers nothing to place and must not disturb the stall guard, whose
    /// question is "how long since data last arrived".
    ///
    /// * `clocks` — both clocks, sampled the instant the read returned.
    /// * `main` — the bytes that read returned.
    /// * `probe` — bytes the follow-up drain probe returned, if any. These are
    ///   real stream data and are kept and accounted like any other; they are
    ///   simply known to have been captured *after* the anchor sample.
    /// * `trustable` — [`probe_indicates_drained`] applied to the probe result.
    ///   Pass `false` if the probe itself failed: an unknown is not a drain.
    /// * `out` — emissions are appended here, in order.
    pub fn on_read(
        &mut self,
        clocks: ReadClocks,
        main: &[u8],
        probe: &[u8],
        trustable: bool,
        out: &mut Vec<MeasurementMatch>,
    ) {
        // The guard runs BEFORE the new bytes are ingested. What it judges is
        // whether the samples already held can still be placed relative to this
        // read; the new bytes are innocent either way.
        if let Some(prev) = self.last_read {
            if let Some(kind) = classify_stall(&prev, &clocks) {
                tracing::debug!(
                    "PPK2 stream gap classified as {:?}; discarding {} held raw samples",
                    kind,
                    self.pending.len()
                );
                self.discard_held(clocks.sys, out);
            }
        }
        self.last_read = Some(clocks);

        let added = self.ingest(main);

        // The anchor is the LAST sample this read delivered: the read completed
        // after that sample was captured, so `clocks.sys` dates it (modulo the
        // transport delay that the contract documents as a constant). Every other
        // sample in the block is placed by counting periods away from it.
        //
        // If the read delivered no whole frame there is nothing to anchor: the
        // newest held sample belongs to an older read, and dating it from now
        // would place it late. Hold instead; the next read costs one more block.
        let anchor_index = if added > 0 {
            Some(self.pending.len() - 1)
        } else {
            None
        };

        // Probe bytes are strictly newer than the anchor sample, so they take
        // indices above it and are stamped forward. Keeping them is not optional:
        // a discarded probe result is silently lost stream data.
        //
        // Under the strict-zero rule in `probe_indicates_drained`, a non-empty
        // probe always arrives with `trustable == false`, so in practice these
        // bytes are held and placed by the NEXT draining read rather than stamped
        // here. The forward-stamping path below is kept correct anyway: this
        // method's contract is "`trustable` means this read drained the port",
        // and it does not assume how the caller established that.
        self.ingest(probe);

        if trustable {
            if let Some(anchor_index) = anchor_index {
                self.emit_block(clocks.sys, anchor_index, out);
            }
        }
    }

    /// Parse `bytes` into the ring, enforcing its capacity. Returns how many
    /// samples the parse produced.
    fn ingest(&mut self, bytes: &[u8]) -> usize {
        if bytes.is_empty() {
            return 0;
        }
        let before = self.pending.len();
        // Disjoint field borrows: the parser and the ring are separate fields.
        self.missed += self.accumulator.feed_into(bytes, &mut self.pending);
        let added = self.pending.len() - before;

        // Overflow discards the OLDEST, which is the only choice that keeps the
        // survivors contiguous with the read that will eventually anchor them.
        // Unreachable by construction while the stall guard is doing its job (the
        // ring is sized to the guard), so reaching it is worth a warning.
        while self.pending.len() > RING_CAPACITY_SAMPLES {
            self.pending.pop_front();
            self.dropped += 1;
        }
        added
    }

    /// Discard everything held, because it can no longer be placed.
    ///
    /// This also throws away the sub-`chunk` remainder, which is the point: a
    /// partial decimation chunk carried across a discontinuity would produce one
    /// averaged value straddling a gap, quietly mixing samples from either side
    /// of it. The cost of discarding is bounded at one chunk.
    fn discard_held(&mut self, at: SystemTime, out: &mut Vec<MeasurementMatch>) {
        self.dropped += self.pending.len() as u64;
        self.pending.clear();
        // Repeated stalls emit nothing until the stream is caught up: the count
        // keeps accumulating and rides out on the next event.
        if !self.drop_signalled {
            out.push(MeasurementMatch::Dropped {
                at,
                samples: self.dropped,
            });
            self.dropped = 0;
            self.drop_signalled = true;
        }
    }

    /// Stamp and emit everything the ring holds, anchored on the sample at
    /// `anchor_index`, which was captured at `anchor`.
    fn emit_block(
        &mut self,
        anchor: SystemTime,
        anchor_index: usize,
        out: &mut Vec<MeasurementMatch>,
    ) {
        debug_assert!(anchor_index < self.pending.len());

        // Back-date to the block's FIRST sample. This is where the untrusted run
        // gets pinned: everything held, however it arrived, is placed by counting
        // periods back from the one sample whose time is known.
        let Some(mut first_ts) = sub_samples(anchor, anchor_index as u64) else {
            // Only reachable if the wall clock is within a second of the epoch.
            tracing::warn!("PPK2 anchor {:?} precedes the epoch; holding block", anchor);
            return;
        };

        let mut timeline_break = false;
        if let Some(last) = self.last_delivered {
            if first_ts <= last {
                let overlap = last.duration_since(first_ts).unwrap_or_default();
                if overlap > SEAM_MAX_OVERLAP {
                    // Not jitter. Trimming here would discard every sample until
                    // real time caught up with the old clock, which for a one-hour
                    // step is one hour of data.
                    tracing::warn!(
                        "PPK2 timeline stepped backwards by {:?}; re-basing",
                        overlap
                    );
                    timeline_break = true;
                    self.last_delivered = None;
                } else {
                    // +1 so the first surviving sample is strictly after the last
                    // delivered one, never merely equal to it.
                    let trim =
                        (overlap.as_nanos() / u128::from(RAW_SAMPLE_PERIOD_NANOS)) as u64 + 1;
                    let len = self.pending.len() as u64;
                    out.push(MeasurementMatch::SeamOverlap {
                        at: first_ts,
                        samples_trimmed: trim.min(len),
                        overlap,
                    });
                    if trim >= len {
                        // The whole block lies inside already-delivered time.
                        // Nothing here can be placed after the seam, so it goes.
                        self.pending.clear();
                        return;
                    }
                    self.pending.drain(..trim as usize);
                    first_ts = add_samples(first_ts, trim);
                }
            }
        }

        let n_chunks = self.pending.len() / self.chunk;
        if n_chunks == 0 {
            // Not enough for one decimated value yet. Everything stays; the
            // pending `missed` and `dropped` counts stay with it.
            return;
        }

        // `timeline_break` only needs its own event if the stall guard has not
        // already reported one for the same clock step — a backwards step big
        // enough to break the seam usually trips both, and two drop events for
        // one discontinuity would over-report the damage.
        if self.dropped > 0 || (timeline_break && !self.drop_signalled) {
            // `first_ts` is where the recovered data resumes, which is the edge
            // of the hole a consumer needs to know about.
            out.push(MeasurementMatch::Dropped {
                at: first_ts,
                samples: self.dropped,
            });
            self.dropped = 0;
        }

        for c in 0..n_chunks {
            let read_at = add_samples(first_ts, (c * self.chunk) as u64);
            let m =
                self.pending
                    .drain(..self.chunk)
                    .combine_matching(self.missed, read_at, self.pins);
            // Reset ONLY after the value is built, so no skipped-sample count is
            // ever dropped: summing `missed` over every emission reconstructs the
            // exact total the device reported.
            self.missed = 0;
            out.push(m);
        }

        self.last_delivered = Some(add_samples(first_ts, (n_chunks * self.chunk - 1) as u64));
        self.drop_signalled = false;
    }
}

/// Classify the gap between two successful reads, or `None` if it is normal.
///
/// The two clocks are compared rather than just the wall clock because a
/// suspend, a device stall and an NTP step are indistinguishable from the read
/// pattern alone. All three currently lose the held samples, but the
/// classification is what makes a log line say something true.
fn classify_stall(prev: &ReadClocks, now: &ReadClocks) -> Option<StallKind> {
    let mono_gap = now.mono.saturating_duration_since(prev.mono);
    match now.sys.duration_since(prev.sys) {
        Ok(sys_gap) => {
            if sys_gap <= STALL_GUARD && mono_gap <= STALL_GUARD {
                return None;
            }
            // A 225 s suspend advanced the monotonic clock by 622 ms; the wall
            // clock is the only witness. A device stall moves both together.
            if sys_gap > mono_gap.saturating_mul(2) && sys_gap > STALL_GUARD {
                Some(StallKind::HostSuspend)
            } else {
                Some(StallKind::DeviceStall)
            }
        }
        // The wall clock moved backwards between two reads.
        Err(e) if e.duration() > STALL_GUARD => Some(StallKind::ClockStep),
        Err(_) => None,
    }
}

/// `t + n` sample periods, saturating at the representable maximum rather than
/// panicking.
fn add_samples(t: SystemTime, n: u64) -> SystemTime {
    t.checked_add(Duration::from_nanos(n * RAW_SAMPLE_PERIOD_NANOS))
        .unwrap_or(t)
}

/// `t - n` sample periods, or `None` if that precedes the epoch.
fn sub_samples(t: SystemTime, n: u64) -> Option<SystemTime> {
    t.checked_sub(Duration::from_nanos(n * RAW_SAMPLE_PERIOD_NANOS))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Calibration metadata reused from the parser's own unit test, so the
    /// assembler is constructed exactly as production does.
    const RAW_METADATA: &str = "Calibrated: 0\n\
R0: 1003.3506\nR1: 101.5865\nR2: 10.3027\nR3: 0.9636\nR4: 0.0564\n\
GS0: 0.0000\nGS1: 112.7890\nGS2: 18.0115\nGS3: 2.4217\nGS4: 0.0729\n\
GI0: 1.0000\nGI1: 0.9695\nGI2: 0.9609\nGI3: 0.9519\nGI4: 0.9582\n\
O0: 112.9420\nO1: 75.4627\nO2: 64.6020\nO3: 50.4983\nO4: 87.2177\n\
VDD: 3741\nHW: 9173\nmode: 2\n\
S0: 0.000000048\nS1: 0.000000596\nS2: 0.000005281\nS3: 0.000062577\nS4: 0.002940743\n\
I0: -0.000000104\nI1: -0.000001443\nI2: 0.000036439\nI3: -0.000374119\nI4: -0.009388455\n\
UG0: 1.00\nUG1: 1.00\nUG2: 1.00\nUG3: 1.00\nUG4: 1.00\n\
IA: 56\nEND\n";

    fn metadata() -> Metadata {
        Metadata::from_bytes(RAW_METADATA.as_bytes()).expect("metadata parse failed")
    }

    /// One synthetic 4-byte device frame for logical sample index `i`, with a
    /// consecutive 6-bit counter so the parser reports no skipped samples.
    fn frame(i: usize) -> [u8; 4] {
        let adc = (i.wrapping_mul(7) & 0x3FFF) as u32;
        let range = (i % 5) as u32;
        let counter = (i % 64) as u32;
        let logic = (i & 0xFF) as u32;
        (adc | (range << 14) | (counter << 18) | (logic << 24)).to_le_bytes()
    }

    fn frames(range: std::ops::Range<usize>) -> Vec<u8> {
        range.flat_map(frame).collect()
    }

    fn base() -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000)
    }

    fn clocks(sys: SystemTime, mono: Instant) -> ReadClocks {
        ReadClocks { sys, mono }
    }

    fn stamps(out: &[MeasurementMatch]) -> Vec<SystemTime> {
        out.iter()
            .filter_map(|m| match *m {
                MeasurementMatch::Match { read_at, .. }
                | MeasurementMatch::NoMatch { read_at, .. } => Some(read_at),
                _ => None,
            })
            .collect()
    }

    fn n_dropped(out: &[MeasurementMatch]) -> usize {
        out.iter()
            .filter(|m| matches!(m, MeasurementMatch::Dropped { .. }))
            .count()
    }

    /// A drained read anchors its own block: the LAST sample it delivered is
    /// dated by the read, and the first is back-dated by `len - 1` periods.
    ///
    /// Getting the direction wrong here is the whole bug in miniature — the read
    /// completes *after* the samples were captured, so stamping the first sample
    /// with the read's own time would place the entire block late by its own
    /// duration.
    #[test]
    fn trustable_read_back_dates_to_its_first_sample() {
        let mut a = StreamAssembler::new(metadata(), SPS_MAX, LogicPortPins::default());
        let mut out = Vec::new();
        let t = base();

        a.on_read(
            clocks(t, Instant::now()),
            &frames(0..16),
            &[],
            true,
            &mut out,
        );

        let ts = stamps(&out);
        assert_eq!(ts.len(), 16, "chunk=1 must emit one value per raw sample");
        assert_eq!(
            ts[15], t,
            "the last sample of the block is the one the read dates"
        );
        assert_eq!(
            ts[0],
            t - RAW_SAMPLE_PERIOD * 15,
            "the first sample must be back-dated by the block's own duration"
        );
        for (i, w) in ts.windows(2).enumerate() {
            assert_eq!(
                w[1].duration_since(w[0]).unwrap(),
                RAW_SAMPLE_PERIOD,
                "samples {i} and {} are not exactly one period apart",
                i + 1
            );
        }
    }

    /// Decimated emissions are spaced by `chunk` periods and each is stamped with
    /// the capture time of the FIRST raw sample it averages.
    #[test]
    fn decimated_block_stamps_the_first_sample_of_each_chunk() {
        let sps = 10_000; // chunk = 10
        let mut a = StreamAssembler::new(metadata(), sps, LogicPortPins::default());
        let mut out = Vec::new();
        let t = base();

        a.on_read(
            clocks(t, Instant::now()),
            &frames(0..40),
            &[],
            true,
            &mut out,
        );

        let ts = stamps(&out);
        assert_eq!(ts.len(), 4);
        // 40 samples, last one at `t`, so the first is at t - 39 periods.
        let first = t - RAW_SAMPLE_PERIOD * 39;
        for (c, stamp) in ts.iter().enumerate() {
            assert_eq!(
                *stamp,
                first + RAW_SAMPLE_PERIOD * (c as u32 * 10),
                "chunk {c} is not stamped at its own first raw sample"
            );
        }
    }

    /// THE central property (design §4.4, §4.7, §4.8): samples delivered by reads
    /// that did NOT drain the port are held, and when a draining read finally
    /// arrives the held run is pinned backward so it butts against the new
    /// block — producing one contiguous, monotonic run.
    ///
    /// The read arrival times here are deliberately pathological, reproducing the
    /// measured drain: four reads 253 µs apart each carrying 2.55 ms of samples.
    /// Stamping from arrival in either direction would produce overlapping,
    /// non-monotonic output; only pinning places them correctly.
    #[test]
    fn untrusted_run_is_pinned_backward_onto_the_next_drained_read() {
        let mut a = StreamAssembler::new(metadata(), SPS_MAX, LogicPortPins::default());
        let mut out = Vec::new();
        let t0 = base();
        let m0 = Instant::now();

        // Three backlogged reads, 253 µs apart, each carrying 255 samples
        // (2.55 ms of stream). None drained the port.
        for r in 0..3usize {
            let at = t0 + Duration::from_micros(253 * r as u64);
            a.on_read(
                clocks(at, m0 + Duration::from_micros(253 * r as u64)),
                &frames(r * 255..(r + 1) * 255),
                &[],
                false,
                &mut out,
            );
            assert!(out.is_empty(), "untrusted read {r} must emit nothing");
        }
        assert_eq!(a.held(), 765);

        // A fourth read drains the port and carries 100 more samples.
        let anchor = t0 + Duration::from_micros(759);
        a.on_read(
            clocks(anchor, m0 + Duration::from_micros(759)),
            &frames(765..865),
            &[],
            true,
            &mut out,
        );

        let ts = stamps(&out);
        assert_eq!(ts.len(), 865, "the whole held run must be emitted at once");
        assert_eq!(
            n_dropped(&out),
            0,
            "nothing was lost, so nothing is Dropped"
        );
        assert_eq!(
            ts[864], anchor,
            "the anchor read still dates its own last sample"
        );
        assert_eq!(
            ts[0],
            anchor - RAW_SAMPLE_PERIOD * 864,
            "the held run must be pinned back from the anchor, not from its own \
             arrival times"
        );
        for w in ts.windows(2) {
            assert_eq!(
                w[1].duration_since(w[0]).unwrap(),
                RAW_SAMPLE_PERIOD,
                "pinning produced a non-contiguous run"
            );
        }
    }

    /// A host suspend: the wall clock jumps 225 s while the monotonic clock
    /// advances only 622 ms. The held run is 225 seconds stale and pinning it
    /// backward would place it as though it were current — the original bug. It
    /// must be discarded and reported.
    ///
    /// A guard on `Instant` would not fire here. That is why this test exists.
    #[test]
    fn suspend_invisible_to_the_monotonic_clock_still_discards_held_samples() {
        let mut a = StreamAssembler::new(metadata(), SPS_MAX, LogicPortPins::default());
        let mut out = Vec::new();
        let t0 = base();
        let m0 = Instant::now();

        a.on_read(clocks(t0, m0), &frames(0..500), &[], false, &mut out);
        assert!(out.is_empty());
        assert_eq!(a.held(), 500);

        // Measured suspend signature: 225 s of wall clock, 622 ms of monotonic.
        let after = clocks(
            t0 + Duration::from_secs(225),
            m0 + Duration::from_millis(622),
        );
        a.on_read(after, &frames(500..600), &[], true, &mut out);

        assert_eq!(n_dropped(&out), 1, "the stale run must be reported as loss");
        let dropped_samples = out
            .iter()
            .find_map(|m| match *m {
                MeasurementMatch::Dropped { samples, .. } => Some(samples),
                _ => None,
            })
            .expect("a Dropped event");
        assert_eq!(dropped_samples, 500);

        let ts = stamps(&out);
        assert_eq!(ts.len(), 100, "only the post-suspend read may be emitted");
        assert_eq!(ts[99], after.sys);
    }

    /// Repeated stalls emit one drop event, not one per stall, and the discarded
    /// count keeps accumulating so nothing is under-reported when it finally
    /// rides out.
    #[test]
    fn repeated_stalls_emit_nothing_until_caught_up() {
        let mut a = StreamAssembler::new(metadata(), SPS_MAX, LogicPortPins::default());
        let mut out = Vec::new();
        let t0 = base();
        let m0 = Instant::now();

        a.on_read(clocks(t0, m0), &frames(0..100), &[], false, &mut out);
        for r in 1..4u64 {
            let at = t0 + Duration::from_secs(10 * r);
            a.on_read(
                clocks(at, m0 + Duration::from_secs(10 * r)),
                &frames(0..100),
                &[],
                false,
                &mut out,
            );
        }
        assert_eq!(
            n_dropped(&out),
            1,
            "three stalls in a row must not produce three drop events"
        );
    }

    /// The seam guard: a block whose first sample lands before the last delivered
    /// one is TRIMMED at the front, never shifted, and reports `SeamOverlap`
    /// rather than `Dropped` — there is no hole, only jitter.
    #[test]
    fn backwards_seam_is_trimmed_at_the_front_and_reported_as_overlap() {
        let mut a = StreamAssembler::new(metadata(), SPS_MAX, LogicPortPins::default());
        let mut out = Vec::new();
        let t0 = base();
        let m0 = Instant::now();

        a.on_read(clocks(t0, m0), &frames(0..100), &[], true, &mut out);
        let last_delivered = *stamps(&out).last().unwrap();
        assert_eq!(last_delivered, t0);
        out.clear();

        // The next read carries 100 more samples but arrives only 70 periods
        // later, because the transport delay shrank. Its block therefore reaches
        // 29 periods back into already-delivered time (its first sample sits at
        // `at - 99 periods`, i.e. 29 periods before `t0`).
        let at = t0 + RAW_SAMPLE_PERIOD * 70;
        a.on_read(
            clocks(at, m0 + Duration::from_millis(1)),
            &frames(100..200),
            &[],
            true,
            &mut out,
        );

        let (trimmed, overlap) = out
            .iter()
            .find_map(|m| match *m {
                MeasurementMatch::SeamOverlap {
                    samples_trimmed,
                    overlap,
                    ..
                } => Some((samples_trimmed, overlap)),
                _ => None,
            })
            .expect("a SeamOverlap event");
        assert_eq!(
            overlap,
            RAW_SAMPLE_PERIOD * 29,
            "the reported overlap must be the real one"
        );
        assert_eq!(trimmed, 30, "floor(overlap / period) + 1 samples");
        assert_eq!(
            n_dropped(&out),
            0,
            "a seam overlap is jitter, not a hole — emitting Dropped here would \
             send someone chasing a USB fault that does not exist"
        );

        let ts = stamps(&out);
        assert_eq!(ts.len(), 100 - 30);
        assert!(
            ts[0] > last_delivered,
            "the first surviving sample must be strictly after the seam"
        );
        assert_eq!(
            *ts.last().unwrap(),
            at,
            "trimming must not move the surviving samples: every one keeps its \
             own true computed time"
        );
    }

    /// A block lying entirely inside already-delivered time is dropped whole, and
    /// still reports the overlap rather than a hole.
    #[test]
    fn block_entirely_inside_delivered_time_is_dropped_whole() {
        let mut a = StreamAssembler::new(metadata(), SPS_MAX, LogicPortPins::default());
        let mut out = Vec::new();
        let t0 = base();
        let m0 = Instant::now();

        a.on_read(clocks(t0, m0), &frames(0..1000), &[], true, &mut out);
        out.clear();

        // 10 samples arriving with a stamp far behind the delivered edge.
        let at = t0 - RAW_SAMPLE_PERIOD * 500;
        a.on_read(
            clocks(at, m0 + Duration::from_millis(1)),
            &frames(1000..1010),
            &[],
            true,
            &mut out,
        );

        assert!(stamps(&out).is_empty(), "nothing may be emitted");
        assert_eq!(n_dropped(&out), 0);
        assert!(matches!(
            out.as_slice(),
            [MeasurementMatch::SeamOverlap {
                samples_trimmed: 10,
                ..
            }]
        ));
    }

    /// An overlap larger than [`SEAM_MAX_OVERLAP`] is a wall-clock step, not
    /// jitter. Trimming would discard every sample until real time caught up, so
    /// the timeline is declared broken and re-based instead.
    #[test]
    fn large_backwards_step_rebases_instead_of_trimming_forever() {
        let mut a = StreamAssembler::new(metadata(), SPS_MAX, LogicPortPins::default());
        let mut out = Vec::new();
        let t0 = base();
        let m0 = Instant::now();

        a.on_read(clocks(t0, m0), &frames(0..100), &[], true, &mut out);
        out.clear();

        // Wall clock steps back an hour; the monotonic clock barely moves, so the
        // stall guard sees a ClockStep and the seam guard sees a huge overlap.
        let at = t0 - Duration::from_secs(3600);
        a.on_read(
            clocks(at, m0 + Duration::from_millis(1)),
            &frames(100..200),
            &[],
            true,
            &mut out,
        );

        assert_eq!(
            n_dropped(&out),
            1,
            "a backwards clock step breaks the timeline and must say so — exactly \
             once, even though it trips both the stall guard and the seam guard"
        );
        assert_eq!(
            stamps(&out).len(),
            100,
            "the samples themselves are fine and must still be delivered"
        );
    }

    /// Ring overflow discards the oldest samples and reports them.
    #[test]
    fn ring_overflow_discards_oldest_and_reports_it() {
        let mut a = StreamAssembler::new(metadata(), SPS_MAX, LogicPortPins::default());
        let mut out = Vec::new();
        let t0 = base();
        let m0 = Instant::now();

        // Fill past capacity with untrusted reads, staying inside the stall guard
        // so the guard does not fire first.
        let per_read = 5_000;
        let reads = RING_CAPACITY_SAMPLES / per_read + 2;
        for r in 0..reads {
            let at = t0 + Duration::from_millis(10 * r as u64);
            a.on_read(
                clocks(at, m0 + Duration::from_millis(10 * r as u64)),
                &frames(r * per_read..(r + 1) * per_read),
                &[],
                false,
                &mut out,
            );
        }
        assert_eq!(a.held(), RING_CAPACITY_SAMPLES);

        let at = t0 + Duration::from_millis(10 * reads as u64);
        a.on_read(
            clocks(at, m0 + Duration::from_millis(10 * reads as u64)),
            &frames(reads * per_read..reads * per_read + 10),
            &[],
            true,
            &mut out,
        );
        assert_eq!(n_dropped(&out), 1, "overflow must be reported");
    }

    /// Only a COMPLETELY empty probe means the port drained.
    ///
    /// The tempting tolerance is 64 bytes, but that is exactly one USB
    /// full-speed bulk packet on macOS: accepting it would accept a whole packet
    /// of real backlog and a ~160 µs placement error, not instrument noise.
    #[test]
    fn only_a_completely_empty_probe_means_drained() {
        assert!(probe_indicates_drained(0));
        for n in [1usize, 4, 16, 63, 64, 65, 1020, 2048] {
            assert!(
                !probe_indicates_drained(n),
                "a probe that returned {n} bytes found real backlog behind the \
                 read, so that read cannot anchor a block"
            );
        }
    }

    /// Probe bytes are stream data: they are kept, and they end up placed
    /// immediately after the samples of the read they followed.
    ///
    /// This is the shape the worker actually produces under the strict-zero rule:
    /// a non-empty probe means the read did NOT drain the port, so the read
    /// arrives with `trustable == false` and everything it carried — probe bytes
    /// included — is held until the next draining read anchors the run.
    #[test]
    fn probe_bytes_are_kept_and_placed_after_the_read_they_followed() {
        let mut a = StreamAssembler::new(metadata(), SPS_MAX, LogicPortPins::default());
        let mut out = Vec::new();
        let t = base();
        let m0 = Instant::now();

        // 16 samples from the read, 4 more the probe swept up. A non-empty probe
        // is exactly what makes this read un-anchorable.
        a.on_read(
            clocks(t, m0),
            &frames(0..16),
            &frames(16..20),
            false,
            &mut out,
        );
        assert!(
            out.is_empty(),
            "a read with backlog behind it emits nothing"
        );
        assert_eq!(a.held(), 20, "probe bytes must not be discarded");

        // The next read drains the port and anchors the whole run.
        let at = t + Duration::from_micros(200);
        a.on_read(
            clocks(at, m0 + Duration::from_micros(200)),
            &frames(20..26),
            &[],
            true,
            &mut out,
        );

        let ts = stamps(&out);
        assert_eq!(ts.len(), 26);
        assert_eq!(*ts.last().unwrap(), at);
        for w in ts.windows(2) {
            assert_eq!(
                w[1].duration_since(w[0]).unwrap(),
                RAW_SAMPLE_PERIOD,
                "probe samples must sit contiguously between the read that \
                 preceded them and the one that followed"
            );
        }
    }

    /// The dead-stream backstop needs BOTH conditions. Ten empty reads that took
    /// 50 ms are the normal Windows startup shape and must not kill the stream.
    #[test]
    fn dead_stream_backstop_requires_both_the_count_and_the_window() {
        let t0 = Instant::now();

        // Windows shape: reads return every 5 ms because the driver's coalescing
        // window expired, not because the device is gone.
        let mut fast = SilenceGuard::new(t0);
        for r in 1..=200u64 {
            assert!(
                fast.on_empty_read(t0 + Duration::from_millis(5 * r))
                    .is_none(),
                "empty read {r} at {} ms declared the stream dead — a pure count \
                 would kill every Windows measurement before it started",
                5 * r
            );
        }

        // Unix shape: 500 ms per read. Nine is not enough however long it took.
        let mut slow = SilenceGuard::new(t0);
        for r in 1..DEAD_STREAM_EMPTY_READS as u64 {
            assert!(
                slow.on_empty_read(t0 + Duration::from_millis(500 * r))
                    .is_none(),
                "fired after only {r} empty reads"
            );
        }
        let dead = slow
            .on_empty_read(t0 + Duration::from_millis(500 * DEAD_STREAM_EMPTY_READS as u64))
            .expect("ten empty reads over five seconds must declare the stream dead");
        assert_eq!(dead.reads, DEAD_STREAM_EMPTY_READS);
        assert_eq!(
            dead.elapsed,
            Duration::from_secs(5),
            "the reported silence must be the real span since data last arrived, \
             not a figure computed from the nominal timeout"
        );
    }

    /// A host suspend produces exactly ONE empty read before the stream recovers,
    /// measured across both capture runs. Firing on that is the bug the guard
    /// replaced — it killed the worker ~200 times an overnight run.
    #[test]
    fn a_suspend_sized_gap_never_trips_the_backstop() {
        let t0 = Instant::now();
        let mut guard = SilenceGuard::new(t0);
        let mut now = t0;

        // 200 suspends, the overnight count: one empty read, then recovery.
        for _ in 0..200 {
            now += Duration::from_millis(500);
            assert!(
                guard.on_empty_read(now).is_none(),
                "a single suspend timeout must never declare the stream dead"
            );
            now += Duration::from_millis(250);
            guard.on_data(now);
        }
    }

    /// Any read that delivers bytes resets both conditions, so silence has to be
    /// unbroken to be fatal.
    #[test]
    fn data_resets_the_backstop() {
        let t0 = Instant::now();
        let mut guard = SilenceGuard::new(t0);
        let mut now = t0;

        for _ in 0..(DEAD_STREAM_EMPTY_READS - 1) {
            now += Duration::from_millis(500);
            assert!(guard.on_empty_read(now).is_none());
        }
        guard.on_data(now);
        // A full run short of the limit again, well past the silence window in
        // absolute terms — but the clock restarted with the data.
        for r in 1..DEAD_STREAM_EMPTY_READS {
            now += Duration::from_millis(500);
            assert!(
                guard.on_empty_read(now).is_none(),
                "empty read {r} after recovery fired; the counter did not reset"
            );
        }
    }

    /// The skipped-sample accounting survives the rewrite: summing `missed` over
    /// every emission reconstructs the exact count the device reported, whether
    /// or not blocks were held, trimmed or dropped along the way.
    #[test]
    fn missed_accounting_survives_held_and_dropped_blocks() {
        // Physically delete runs of frames so the device counter shows a gap.
        let drops: &[(usize, usize)] = &[(100, 5), (600, 13), (1_500, 63)];
        let removed: usize = drops.iter().map(|&(_, n)| n).sum();
        let mut stream = Vec::new();
        for i in 0..3_000usize {
            if drops.iter().any(|&(s, n)| i >= s && i < s + n) {
                continue;
            }
            stream.extend_from_slice(&frame(i));
        }

        let mut a = StreamAssembler::new(metadata(), SPS_MAX, LogicPortPins::default());
        let mut out = Vec::new();
        let t0 = base();
        let m0 = Instant::now();

        let mut total = 0u64;
        for (r, bytes) in stream.chunks(333).enumerate() {
            let at = Duration::from_millis(r as u64);
            // Alternate trustable / untrusted so held runs really occur.
            a.on_read(clocks(t0 + at, m0 + at), bytes, &[], r % 3 == 0, &mut out);
            for m in out.drain(..) {
                total += match m {
                    MeasurementMatch::Match { missed, .. }
                    | MeasurementMatch::NoMatch { missed, .. } => u64::from(missed),
                    _ => 0,
                };
            }
        }
        assert_eq!(
            total + a.missed as u64,
            removed as u64,
            "summed `missed` plus what is still pending must equal the skipped \
             sample count exactly"
        );
    }

    /// A read that delivered no whole frame cannot anchor anything: the newest
    /// held sample belongs to an older read and dating it from now would place it
    /// late.
    #[test]
    fn read_without_a_whole_frame_does_not_anchor() {
        let mut a = StreamAssembler::new(metadata(), SPS_MAX, LogicPortPins::default());
        let mut out = Vec::new();
        let t0 = base();
        let m0 = Instant::now();

        a.on_read(clocks(t0, m0), &frames(0..8), &[], false, &mut out);
        assert!(out.is_empty());

        // Two bytes: not a frame.
        let at = t0 + Duration::from_millis(1);
        a.on_read(
            clocks(at, m0 + Duration::from_millis(1)),
            &[0, 0],
            &[],
            true,
            &mut out,
        );
        assert!(
            out.is_empty(),
            "an anchor-less read must hold rather than mis-place the run"
        );
        assert_eq!(a.held(), 8);
    }

    /// A partial decimation chunk is carried across an ordinary block boundary
    /// (the samples really are consecutive) but discarded across a discontinuity,
    /// so one averaged value never straddles a gap.
    #[test]
    fn partial_chunk_carries_across_blocks_but_not_across_a_gap() {
        let sps = 10_000; // chunk = 10
        let mut a = StreamAssembler::new(metadata(), sps, LogicPortPins::default());
        let mut out = Vec::new();
        let t0 = base();
        let m0 = Instant::now();

        // 14 samples: one chunk emitted, 4 held.
        a.on_read(clocks(t0, m0), &frames(0..14), &[], true, &mut out);
        assert_eq!(stamps(&out).len(), 1);
        assert_eq!(a.held(), 4, "the sub-chunk remainder is carried");
        out.clear();

        // 6 more complete that chunk across the block boundary.
        let at = t0 + Duration::from_millis(1);
        a.on_read(
            clocks(at, m0 + Duration::from_millis(1)),
            &frames(14..20),
            &[],
            true,
            &mut out,
        );
        assert_eq!(
            stamps(&out).len(),
            1,
            "a chunk completed across an ordinary boundary must still be emitted"
        );
        out.clear();

        // Now a stall. The remainder must not survive it.
        a.on_read(
            clocks(at, m0 + Duration::from_millis(1)),
            &frames(20..25),
            &[],
            false,
            &mut out,
        );
        assert_eq!(a.held(), 5);
        let stalled = t0 + Duration::from_secs(30);
        a.on_read(
            clocks(stalled, m0 + Duration::from_secs(30)),
            &frames(25..30),
            &[],
            false,
            &mut out,
        );
        assert_eq!(
            a.held(),
            5,
            "the pre-stall remainder must be discarded, not averaged together \
             with post-stall samples"
        );
    }
}
