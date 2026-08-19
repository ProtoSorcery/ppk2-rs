//! Regression guard: the skipped-raw-sample count MUST reach the consumer
//! intact.
//!
//! TEST-ONLY, non-shipping. Uses only `std` (no added crates).
//!
//! Background: the device stamps every sample with a 6-bit counter. When the
//! host loses samples, `MeasurementAccumulator::feed_into` detects the counter
//! gap, DROPS the frame, and reports how many raw samples went missing. That
//! count used to be folded into the averaging denominator and then discarded,
//! so a consumer that synthesizes timestamps as
//! `anchor + sample_index * period` (advancing `sample_index` only for samples
//! that actually arrived) shifted its whole remaining timeline earlier by one
//! sample period per lost sample. Measured live at ~414 ms of accumulated
//! error over a 30-minute capture (ProtoSorcery/MM_py#869).
//!
//! `MeasurementMatch` now carries `missed` on BOTH variants. The invariant the
//! consumer relies on, and that this test pins down, is:
//!
//!   summing `missed` across every emitted `MeasurementMatch` (plus anything
//!   still pending when the stream ends) yields the EXACT total number of
//!   skipped raw samples — no double-counting, no silent loss.
//!
//! Note `missed` is in RAW device samples at `SPS_MAX`, NOT decimated output
//! periods; this test therefore checks the identity at both a non-decimating
//! (`chunk == 1`) and a decimating (`chunk == 10`) rate.
//!
//! ## What the timebase rewrite changed here
//!
//! The invariant is unchanged and deliberately so — it is the one thing the new
//! timebase had to leave alone. What changed is that this test no longer
//! REPLICATES the worker's feed-then-drain loop: that loop moved into
//! `ppk2::stream::StreamAssembler`, which takes reads as plain arguments, so the
//! accounting is now checked against production code instead of against a copy
//! of it. Reads alternate between draining and backlogged, which exercises the
//! path where `missed` accumulates across many held reads before anything is
//! emitted — the path where a premature reset would be hardest to spot.
//!
//! `missed` counts DEVICE-side loss only. Samples this crate discards because
//! they can no longer be placed in time are a different thing entirely and are
//! reported as `MeasurementMatch::Dropped`; neither of the new event variants
//! carries a `missed` count, and neither may occur in this fixture at all.
//!
//! Run:
//!   cargo test --test missed_sample_accounting -- --nocapture

use std::time::{Duration, Instant, SystemTime};

use ppk2::measurement::{Measurement, MeasurementIterExt, MeasurementMatch};
use ppk2::stream::{ReadClocks, StreamAssembler, RAW_SAMPLE_PERIOD};
use ppk2::types::{LogicPortPins, Metadata};
use ppk2::SPS_MAX;

/// Number of synthetic 4-byte frames in the pristine (pre-deletion) stream.
const N_FRAMES: usize = 60_000;

/// Deliberate dropouts as `(first_frame_index, run_length)`. Each run is
/// physically deleted from the byte stream to emulate samples the host never
/// received.
///
/// Run lengths MUST stay in `1..64`: the device counter is 6 bits, so a run of
/// exactly 64 wraps to a zero gap and is undetectable in principle. Runs are
/// spaced far apart so no two can merge into a single >= 64 gap.
const DROPS: &[(usize, usize)] = &[(1_000, 1), (5_000, 7), (12_345, 63), (30_000, 3)];

/// Calibration metadata reused from the in-crate unit test so the accumulator
/// is constructed exactly as production does.
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

/// Build one synthetic 4-byte PPK2 frame for LOGICAL sample index `i`.
///
/// Frame bit layout (matches `measurement.rs` masks):
///   adc     : bits  0..13 (14 bits)
///   range   : bits 14..16 (3 bits)
///   counter : bits 18..23 (6 bits)
///   logic   : bits 24..31 (8 bits)
///
/// The counter is `i % 64`, i.e. strictly consecutive over the LOGICAL index.
/// Deleting frames from the emitted stream therefore leaves a counter gap of
/// exactly the deleted run length, which is what the accumulator reports.
fn make_frame(i: usize) -> u32 {
    let adc = (i.wrapping_mul(7) & 0x3FFF) as u32; // 14 bits
    let range = (i % 5) as u32; // 0..=4, drives spike-filter path
    let counter = (i % 64) as u32; // consecutive over the logical index
    let logic = (i & 0xFF) as u32; // 8 logic-port bits
    adc | (range << 14) | (counter << 18) | (logic << 24)
}

fn is_dropped(i: usize) -> bool {
    DROPS
        .iter()
        .any(|&(start, len)| i >= start && i < start + len)
}

/// Emit the stream with `DROPS` physically removed, plus the total number of
/// removed frames.
fn build_stream_with_gaps() -> (Vec<u8>, usize) {
    let mut stream = Vec::with_capacity(N_FRAMES * 4);
    let mut removed = 0usize;
    for i in 0..N_FRAMES {
        if is_dropped(i) {
            removed += 1;
            continue;
        }
        stream.extend_from_slice(&make_frame(i).to_le_bytes());
    }
    (stream, removed)
}

/// Drive the real production timebase (`ppk2::stream::StreamAssembler`, which
/// `Ppk2::start_measurement_matching` hands every read to) and return
/// `(missed_summed_over_emitted, emitted_count)`.
///
/// This test used to REPLICATE the worker's feed-then-drain loop, because the
/// loop was inline in a thread that cannot run without hardware. The timebase now
/// lives in a struct that takes reads as plain arguments, so the accounting can
/// be checked against production code instead of against a copy of it.
///
/// Reads alternate between "drained the port" and "swept up a backlog", so the
/// invariant is exercised across held runs — the path where `missed` accumulates
/// over many reads before anything is emitted, and therefore the path where a
/// premature reset would be invisible at a glance.
///
/// Nothing is still pending at the end here because the final read is declared
/// draining, but `missed` for a partial trailing chunk legitimately stays inside
/// the assembler; the assertion accounts for that by construction (the fixture's
/// sample count is a multiple of every `chunk` used).
fn run(stream: &[u8], metadata: &Metadata, read_size: usize, sps: usize) -> (u64, usize) {
    let mut assembler = StreamAssembler::new(metadata.clone(), sps, LogicPortPins::default());
    let mut out = Vec::new();
    let mut missed_total = 0u64;
    let mut emitted = 0usize;
    let mono = Instant::now();
    let mut elapsed = Duration::ZERO;

    let n_reads = stream.len().div_ceil(read_size);
    for (read_index, read) in stream.chunks(read_size).enumerate() {
        // Advance the synthetic clock by exactly the stream time this read
        // carried, so no seam trimming or stall can occur and the only thing
        // under test is the accounting.
        elapsed += RAW_SAMPLE_PERIOD * (read.len() / 4) as u32;
        out.clear();
        assembler.on_read(
            ReadClocks {
                sys: SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000) + elapsed,
                mono: mono + elapsed,
            },
            read,
            &[],
            // The last read must drain, or the tail stays held and unreported.
            read_index % 3 == 0 || read_index + 1 == n_reads,
            &mut out,
        );
        for m in out.drain(..) {
            missed_total += match m {
                MeasurementMatch::Match { missed, .. }
                | MeasurementMatch::NoMatch { missed, .. } => u64::from(missed),
                // Neither event carries a device-side skipped-sample count:
                // `Dropped` is host-side loss and `SeamOverlap` is redundant
                // data. Neither may occur in this fixture at all.
                other => panic!("unexpected event in a clean stream: {other:?}"),
            };
            emitted += 1;
        }
    }
    (missed_total, emitted)
}

#[test]
fn summed_missed_equals_skipped_sample_count() {
    let metadata = Metadata::from_bytes(RAW_METADATA.as_bytes()).expect("metadata parse failed");
    let (stream, removed) = build_stream_with_gaps();

    let expected_removed: usize = DROPS.iter().map(|&(_, len)| len).sum();
    assert_eq!(removed, expected_removed, "fixture removed the wrong count");
    assert_eq!(stream.len(), (N_FRAMES - removed) * 4);

    // Non-decimating (chunk = 1) and decimating (chunk = 10), each at a small
    // (one-sample) and a large (1024-sample) read size, to prove the accounting
    // is independent of both decimation and read chunking.
    for sps in [SPS_MAX, 10_000usize] {
        for read_size in [4usize, 4096usize] {
            let (missed_total, emitted) = run(&stream, &metadata, read_size, sps);

            assert!(emitted > 0, "sps={sps} read={read_size}: nothing emitted");
            assert_eq!(
                missed_total, removed as u64,
                "sps={sps} read={read_size}: summed `missed` ({missed_total}) != \
                 actually skipped samples ({removed}) — the consumer's timeline \
                 would drift"
            );
        }
    }

    println!("{removed} skipped raw samples fully accounted for across all sps/read-size cases");
}

#[test]
fn missed_survives_the_empty_input_path() {
    // A chunk in which NOTHING matched must still report the pending count;
    // dropping it here would silently lose elapsed-but-sampleless time.
    let empty: Vec<Measurement> = Vec::new();
    match empty.into_iter().combine(42, SystemTime::UNIX_EPOCH) {
        MeasurementMatch::NoMatch { missed, .. } => assert_eq!(missed, 42),
        // `combine` builds measurements only; the out-of-band `Dropped` /
        // `SeamOverlap` events come from the assembler and can never appear here.
        other => panic!("expected NoMatch for an empty chunk, got {other:?}"),
    }
}

#[test]
fn missed_survives_the_pin_filtered_path() {
    // Same, but reached via `combine_matching`'s logic-port filter rejecting
    // every measurement rather than via an empty input.
    let measurements = vec![
        Measurement {
            micro_amps: 1.0,
            pins: [false; 8].into(),
        },
        Measurement {
            micro_amps: 2.0,
            pins: [false; 8].into(),
        },
    ];
    // Require every pin HIGH; all-low measurements cannot match.
    let require_all_high: LogicPortPins = [true; 8].into();
    match measurements
        .into_iter()
        .combine_matching(7, SystemTime::UNIX_EPOCH, require_all_high)
    {
        MeasurementMatch::NoMatch { missed, .. } => assert_eq!(missed, 7),
        other => panic!("expected NoMatch when no pins match, got {other:?}"),
    }
}
