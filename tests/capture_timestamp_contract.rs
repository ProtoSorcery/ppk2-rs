//! Regression guard for the crate's timestamp contract: `read_at` is the
//! wall-clock CAPTURE time of the first raw sample of the chunk it rides on,
//! samples inside a block are exactly one period apart, and nothing that leaves
//! the crate ever goes backwards in time.
//!
//! TEST-ONLY, non-shipping. Uses only `std` (no added crates).
//!
//! # This file replaces `read_timestamp_propagation.rs`
//!
//! That test pinned down the OPPOSITE promise, and it did so deliberately and
//! correctly for the design it was written against: `read_at` meant "the instant
//! the USB read returned", was documented as an upper bound on capture time, and
//! a chunk spanning two reads carried the LATER stamp specifically so the stamp
//! could never under-state arrival. Its central assertions were
//!
//! * `chunk_spanning_two_reads_carries_the_later_timestamp`, and
//! * `read_timestamp_never_understates_arrival_and_missed_still_balances`.
//!
//! Both are now false BY CONSTRUCTION. The crate back-dates: the stamp on a
//! chunk is earlier than the read that delivered it, always, by the block's own
//! duration. Keeping those tests green would have meant keeping a timebase that
//! cannot work — arrival bears no relationship to capture during a drain, where
//! four consecutive 1020-byte reads were measured arriving 253 µs apart while
//! each carried 2.55 ms of samples.
//!
//! What was worth keeping from that file is carried over here: the `missed`
//! accounting must survive the rewrite untouched (checked below, and in
//! `missed_sample_accounting.rs`), and the emitted stream must never be able to
//! place a sample somewhere a consumer would reject.
//!
//! # Why this drives `StreamAssembler` rather than the worker thread
//!
//! The worker loop in `Ppk2::start_measurement_matching` is unreachable without
//! hardware: it is entered only after `Ppk2::new` has opened a port and
//! completed the metadata handshake. The timebase therefore lives in
//! `ppk2::stream::StreamAssembler`, which takes raw reads and a clock pair as
//! plain arguments — so the interesting behaviour is testable directly instead
//! of re-implemented in a test that could drift from production. What is NOT
//! covered here is exactly the worker's plumbing: that the clocks are sampled
//! the instant `read()` returns, that the drain probe's result is what
//! `trustable` carries, and that emissions reach the mpsc channel in order.
//!
//! Run:
//!   cargo test --test capture_timestamp_contract -- --nocapture

use std::time::{Duration, Instant, SystemTime};

use ppk2::measurement::MeasurementMatch;
use ppk2::stream::{ReadClocks, StreamAssembler, RAW_SAMPLE_PERIOD, STALL_GUARD};
use ppk2::types::{LogicPortPins, Metadata};
use ppk2::SPS_MAX;

/// Calibration metadata reused from the in-crate unit test so the assembler is
/// constructed exactly as production does.
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

/// Bytes per raw device sample.
const SAMPLE_BYTES: usize = 4;

/// Default USB full-speed bulk packet size, and the size 97.26% of macOS reads
/// return: 16 samples, 160 µs of stream.
const PACKET_BYTES: usize = 64;

/// What Windows returns for 96.25% of reads under native coalescing.
const WIN_READ_BYTES: usize = 2048;

fn metadata() -> Metadata {
    Metadata::from_bytes(RAW_METADATA.as_bytes()).expect("metadata parse failed")
}

/// One synthetic 4-byte device frame for logical sample index `i`.
///
/// Frame bit layout (matches `measurement.rs` masks):
///   adc     : bits  0..13 (14 bits)
///   range   : bits 14..16 (3 bits)
///   counter : bits 18..23 (6 bits)
///   logic   : bits 24..31 (8 bits)
fn frame(i: usize) -> [u8; 4] {
    let adc = (i.wrapping_mul(7) & 0x3FFF) as u32;
    let range = (i % 5) as u32;
    let counter = (i % 64) as u32; // consecutive => no device-side loss
    let logic = (i & 0xFF) as u32;
    (adc | (range << 14) | (counter << 18) | (logic << 24)).to_le_bytes()
}

fn stream(n_frames: usize) -> Vec<u8> {
    (0..n_frames).flat_map(frame).collect()
}

fn base() -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000)
}

/// Every emission that carries a timestamp, in order.
fn stamps(out: &[MeasurementMatch]) -> Vec<SystemTime> {
    out.iter()
        .filter_map(|m| match *m {
            MeasurementMatch::Match { read_at, .. } | MeasurementMatch::NoMatch { read_at, .. } => {
                Some(read_at)
            }
            _ => None,
        })
        .collect()
}

fn missed_total(out: &[MeasurementMatch]) -> u64 {
    out.iter()
        .map(|m| match *m {
            MeasurementMatch::Match { missed, .. } | MeasurementMatch::NoMatch { missed, .. } => {
                u64::from(missed)
            }
            _ => 0,
        })
        .sum()
}

/// Assert the stream a consumer sees is strictly increasing.
///
/// This is the property the seam guard exists for. Downstream seam-dedupe
/// silently DROPS records whose first timestamp goes backwards, so a violation
/// here shows up as missing data with no error anywhere in the system.
fn assert_strictly_monotonic(out: &[MeasurementMatch], chunk: usize) {
    let ts = stamps(out);
    for (i, w) in ts.windows(2).enumerate() {
        assert!(
            w[1] > w[0],
            "emission {} is not strictly after emission {i} ({:?} then {:?}) — \
             downstream seam-dedupe would silently drop it",
            i + 1,
            w[0],
            w[1]
        );
    }
    // Within a block, spacing is exact. Across blocks it may jump forward.
    let step = RAW_SAMPLE_PERIOD * chunk as u32;
    for (i, w) in ts.windows(2).enumerate() {
        let d = w[1].duration_since(w[0]).expect("monotonic checked above");
        assert!(
            d >= step,
            "emissions {i} and {} are {d:?} apart, closer than one decimation \
             period ({step:?}) — the samples would overlap in time",
            i + 1
        );
    }
}

/// Drive the assembler over `bytes`, split into `read_size` reads, each declared
/// `trustable`, with a synthetic clock advanced by exactly the stream time each
/// read carried.
///
/// Advancing the clock by the delivered sample count is what a perfect transport
/// with a perfectly constant delay would produce. Any timestamp error the
/// assembler introduces therefore shows up directly, undisguised by jitter.
fn run_uniform(
    bytes: &[u8],
    read_size: usize,
    sps: usize,
    trustable: bool,
) -> Vec<MeasurementMatch> {
    let mut a = StreamAssembler::new(metadata(), sps, LogicPortPins::default());
    let mut out = Vec::new();
    let mut all = Vec::new();
    let mono = Instant::now();
    let mut elapsed = Duration::ZERO;

    for read in bytes.chunks(read_size) {
        let samples = (read.len() / SAMPLE_BYTES) as u32;
        // The read completes after its LAST sample was captured, so the clock
        // sits at that sample's capture time.
        elapsed += RAW_SAMPLE_PERIOD * samples;
        out.clear();
        a.on_read(
            ReadClocks {
                sys: base() + elapsed,
                mono: mono + elapsed,
            },
            read,
            &[],
            trustable,
            &mut out,
        );
        all.append(&mut out);
    }
    all
}

/// A macOS-shaped capture: every read is one 64-byte USB packet and every read
/// drained the port. Each emitted measurement must land on its own true capture
/// time.
#[test]
fn packet_per_read_stream_places_every_sample_at_its_capture_time() {
    let n = 4_000;
    let out = run_uniform(&stream(n), PACKET_BYTES, SPS_MAX, true);

    let ts = stamps(&out);
    assert_eq!(ts.len(), n, "chunk=1 emits one value per raw sample");
    // Sample 0 was captured one period before sample 1, and so on back from the
    // first read's completion at base() + 16 periods.
    let first = base() + RAW_SAMPLE_PERIOD * 16 - RAW_SAMPLE_PERIOD * 15;
    for (i, stamp) in ts.iter().enumerate() {
        assert_eq!(
            *stamp,
            first + RAW_SAMPLE_PERIOD * i as u32,
            "sample {i} is not at its capture time"
        );
    }
    assert_strictly_monotonic(&out, 1);
}

/// A Windows-shaped capture: 2048-byte reads, all draining. Decimated to 1 ksps,
/// so a chunk is 100 raw samples and every read spans several chunks with a
/// remainder carried across the boundary.
#[test]
fn coalesced_windows_stream_is_contiguous_across_read_boundaries() {
    let sps = 1_000;
    let chunk = SPS_MAX / sps;
    let n = 40_000;
    let out = run_uniform(&stream(n), WIN_READ_BYTES, sps, true);

    let ts = stamps(&out);
    assert!(!ts.is_empty());
    for (i, w) in ts.windows(2).enumerate() {
        assert_eq!(
            w[1].duration_since(w[0]).unwrap(),
            RAW_SAMPLE_PERIOD * chunk as u32,
            "chunks {i} and {} are not exactly one decimation period apart — a \
             chunk completed across a read boundary was mis-placed",
            i + 1
        );
    }
    assert_strictly_monotonic(&out, chunk);
}

/// Decimated output is identical whether it arrives 16 samples at a time or 512,
/// INCLUDING the timestamps. Read size is a property of the OS driver and must
/// never be visible in the contract.
#[test]
fn output_is_independent_of_read_size() {
    let bytes = stream(40_000);
    for sps in [SPS_MAX, 1_000usize] {
        let small = run_uniform(&bytes, PACKET_BYTES, sps, true);
        let large = run_uniform(&bytes, WIN_READ_BYTES, sps, true);
        assert_eq!(
            stamps(&small),
            stamps(&large),
            "sps={sps}: read size changed the emitted timestamps"
        );
    }
}

/// The pathological case the whole design exists for: a run of reads that swept
/// up a backlog, followed by one that drained the port.
///
/// Arrival times inside the drain are deliberately nothing like capture times —
/// reads 253 µs apart each carrying 2.55 ms of samples, the measured signature.
/// The output must still be one contiguous, strictly monotonic run ending at the
/// draining read.
#[test]
fn backlogged_drain_still_produces_one_contiguous_run() {
    let mut a = StreamAssembler::new(metadata(), SPS_MAX, LogicPortPins::default());
    let mut out = Vec::new();
    let sys0 = base();
    let mono0 = Instant::now();

    // 1020 bytes = 255 samples = 2.55 ms of stream, arriving every 253 µs.
    let per_read = 255;
    let n_backlogged = 8;
    for r in 0..n_backlogged {
        let d = Duration::from_micros(253 * r as u64);
        a.on_read(
            ReadClocks {
                sys: sys0 + d,
                mono: mono0 + d,
            },
            &(r * per_read..(r + 1) * per_read)
                .flat_map(frame)
                .collect::<Vec<_>>(),
            &[],
            false,
            &mut out,
        );
    }
    assert!(
        out.is_empty(),
        "reads that swept up a backlog must not be emitted on their own arrival \
         times — doing so is what produces overlapping, non-monotonic output"
    );

    // The drain ends: one more read, and this time nothing was queued behind it.
    let d = Duration::from_micros(253 * n_backlogged as u64);
    let anchor = sys0 + d;
    a.on_read(
        ReadClocks {
            sys: anchor,
            mono: mono0 + d,
        },
        &(n_backlogged * per_read..n_backlogged * per_read + 16)
            .flat_map(frame)
            .collect::<Vec<_>>(),
        &[],
        true,
        &mut out,
    );

    let ts = stamps(&out);
    assert_eq!(ts.len(), n_backlogged * per_read + 16);
    assert_eq!(
        *ts.last().unwrap(),
        anchor,
        "the draining read dates its own last sample"
    );
    assert_strictly_monotonic(&out, 1);
    assert!(
        out.iter()
            .all(|m| !matches!(m, MeasurementMatch::Dropped { .. })),
        "nothing was lost here, so nothing may be reported as lost"
    );
}

/// A long stream with realistic transport-delay jitter and a mixture of draining
/// and backlogged reads. Nothing may ever leave the crate out of order, and the
/// jitter must be reported as `SeamOverlap`, never as `Dropped`.
#[test]
fn jittery_transport_never_emits_a_backwards_timestamp() {
    let sps = 10_000; // chunk = 10
    let chunk = SPS_MAX / sps;
    let mut a = StreamAssembler::new(metadata(), sps, LogicPortPins::default());
    let mut out = Vec::new();
    let mut all = Vec::new();

    let per_read = 64; // samples
    let reads = 3_000;
    let mono0 = Instant::now();
    let mut elapsed = Duration::ZERO;

    for r in 0..reads {
        elapsed += RAW_SAMPLE_PERIOD * per_read as u32;
        // Transport delay wobbles by up to ±8 sample periods around a constant.
        // This is what makes a block start before the previous one ended.
        let wobble = RAW_SAMPLE_PERIOD * ((r * 37) % 17) as u32;
        let sys = base() + elapsed + wobble;
        out.clear();
        a.on_read(
            ReadClocks {
                sys,
                mono: mono0 + elapsed,
            },
            &(r * per_read..(r + 1) * per_read)
                .flat_map(frame)
                .collect::<Vec<_>>(),
            &[],
            // Two reads in five swept up a backlog.
            r % 5 < 3,
            &mut out,
        );
        all.append(&mut out);
    }

    assert_strictly_monotonic(&all, chunk);
    let overlaps = all
        .iter()
        .filter(|m| matches!(m, MeasurementMatch::SeamOverlap { .. }))
        .count();
    assert!(
        overlaps > 0,
        "the fixture never produced a backwards seam, so the guard was not \
         actually exercised"
    );
    assert!(
        all.iter()
            .all(|m| !matches!(m, MeasurementMatch::Dropped { .. })),
        "timing jitter is not sample loss and must never be reported as a hole — \
         doing so sends someone hunting a USB fault that does not exist"
    );
}

/// The `missed` invariant, unchanged from the contract this file replaces:
/// summing `missed` over every emission plus whatever is still pending equals the
/// exact number of raw samples the device skipped. No double-counting, no silent
/// loss — even across held runs and discarded blocks.
#[test]
fn missed_accounting_still_balances_exactly() {
    // Run lengths stay in 1..64: the device counter is 6 bits, so a run of
    // exactly 64 wraps to a zero gap and is undetectable in principle.
    let drops: &[(usize, usize)] = &[(1_000, 1), (5_000, 7), (12_345, 63), (30_000, 3)];
    let removed: u64 = drops.iter().map(|&(_, n)| n as u64).sum();

    let mut bytes = Vec::new();
    for i in 0..60_000usize {
        if drops.iter().any(|&(s, n)| i >= s && i < s + n) {
            continue;
        }
        bytes.extend_from_slice(&frame(i));
    }

    for sps in [SPS_MAX, 10_000usize] {
        for read_size in [SAMPLE_BYTES, PACKET_BYTES, WIN_READ_BYTES] {
            // Every read draining, so nothing is discarded and everything the
            // device reported must arrive.
            let out = run_uniform(&bytes, read_size, sps, true);
            assert_eq!(
                missed_total(&out),
                removed,
                "sps={sps} read={read_size}: summed `missed` != skipped samples"
            );
        }
    }
}

/// A stall longer than the guard discards what was held and says so. The samples
/// that arrive afterwards are placed on the new read, not stitched onto the old
/// run.
#[test]
fn stall_reports_loss_rather_than_stitching_across_it() {
    let mut a = StreamAssembler::new(metadata(), SPS_MAX, LogicPortPins::default());
    let mut out = Vec::new();
    let sys0 = base();
    let mono0 = Instant::now();

    a.on_read(
        ReadClocks {
            sys: sys0,
            mono: mono0,
        },
        &stream(512),
        &[],
        false,
        &mut out,
    );
    assert!(out.is_empty());

    let gap = STALL_GUARD + Duration::from_millis(500);
    a.on_read(
        ReadClocks {
            sys: sys0 + gap,
            mono: mono0 + gap,
        },
        &(512..528).flat_map(frame).collect::<Vec<_>>(),
        &[],
        true,
        &mut out,
    );

    let dropped: Vec<u64> = out
        .iter()
        .filter_map(|m| match *m {
            MeasurementMatch::Dropped { samples, .. } => Some(samples),
            _ => None,
        })
        .collect();
    assert_eq!(dropped, vec![512], "the stale run must be reported as lost");
    assert_eq!(
        stamps(&out).len(),
        16,
        "only the post-stall read may be emitted"
    );
}
