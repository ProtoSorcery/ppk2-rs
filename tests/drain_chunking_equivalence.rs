//! Regression guard: the streaming worker's decimation drain must emit the
//! SAME sequence of averaged measurements regardless of how large each USB
//! `read()` is.
//!
//! TEST-ONLY, non-shipping. Uses only `std` (no added crates).
//!
//! Background: the PPK2 always streams at `SPS_MAX` (100_000) samples/sec; the
//! driver decimates by AVERAGING `SPS_MAX/sps` parsed samples into one output
//! via `combine_matching`. A prior change enlarged the worker read buffer,
//! which (with the old `drain(..)` that averaged the WHOLE buffer into one
//! output) collapsed the output rate and lost data. The fix drains in FIXED
//! `chunk = SPS_MAX/sps`-sample units in a while-loop so averaging no longer
//! depends on read size.
//!
//! This test replicates that feed + fixed-chunk-drain loop for two read-chunk
//! sizes (4 bytes vs 4096 bytes) at two `sps` values (sps=100_000 => chunk=1,
//! non-decimating; sps=10_000 => chunk=10, decimating) and asserts the emitted
//! `MeasurementMatch` sequences are bit-identical across read sizes for the
//! same sps. That is the guard that read-buffer size never changes decimated
//! output.
//!
//! Run:
//!   cargo test --test drain_chunking_equivalence -- --nocapture

use std::collections::VecDeque;
use std::time::{Duration, SystemTime};

use ppk2::measurement::{
    Measurement, MeasurementAccumulator, MeasurementIterExt, MeasurementMatch,
};
use ppk2::types::{LogicPortPins, Metadata};
/// Device hardware sample rate (samples/sec); the decimation chunk size is
/// `SPS_MAX / sps`. Re-exported from the crate so this test cannot drift from
/// production.
use ppk2::SPS_MAX;

/// Number of synthetic 4-byte frames in the stream. Enough to produce many
/// decimated outputs at chunk=10 while keeping the test fast.
const N_FRAMES: usize = 200_000;

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

/// Build one synthetic 4-byte PPK2 frame for sample index `i`.
///
/// Frame bit layout (matches `measurement.rs` masks):
///   adc     : bits  0..13 (14 bits)
///   range   : bits 14..16 (3 bits)
///   counter : bits 18..23 (6 bits)
///   logic   : bits 24..31 (8 bits)
///
/// The 6-bit counter increments by 1 (mod 64) per frame so the accumulator does
/// NOT treat anything as "samples missed". All fields are derived
/// deterministically from `i` (no RNG), exercising range changes and varied
/// logic pins / adc values.
fn make_frame(i: usize) -> u32 {
    let adc = (i.wrapping_mul(7) & 0x3FFF) as u32; // 14 bits
    let range = (i % 5) as u32; // 0..=4, drives spike-filter path
    let counter = (i % 64) as u32; // consecutive => no skipped samples
    let logic = (i & 0xFF) as u32; // 8 logic-port bits
    adc | (range << 14) | (counter << 18) | (logic << 24)
}

/// Generate the full deterministic byte stream of `N_FRAMES` little-endian frames.
fn build_stream() -> Vec<u8> {
    let mut stream = Vec::with_capacity(N_FRAMES * 4);
    for i in 0..N_FRAMES {
        stream.extend_from_slice(&make_frame(i).to_le_bytes());
    }
    stream
}

/// A `MeasurementMatch` reduced to a bit-comparable form. `MeasurementMatch`
/// has no `PartialEq`, so we compare exact float bits + the 8-bit pin bitmask,
/// paired with the variant's skipped-raw-sample count.
/// `None` in the first slot represents `MeasurementMatch::NoMatch`.
///
/// `read_at` is deliberately EXCLUDED from this comparison. It is stamped per
/// USB `read()`, so it legitimately differs between read sizes: the same
/// sample stream split into 4-byte reads produces a thousand times more
/// distinct stamps than the same stream split into 4096-byte reads. Only the
/// MEASUREMENT output is required to be read-size independent. `read_at`'s own
/// behaviour is pinned down in `tests/read_timestamp_propagation.rs`.
type EmittedMatch = (Option<(u32, u8)>, u32);

fn pin_mask(m: &Measurement) -> u8 {
    let mut mask = 0u8;
    for (i, level) in m.pins.inner().iter().enumerate() {
        if level.is_high() {
            mask |= 1 << i;
        }
    }
    mask
}

fn reduce(m: MeasurementMatch) -> EmittedMatch {
    match m {
        MeasurementMatch::Match {
            measurement,
            missed,
            read_at: _,
        } => (
            Some((measurement.micro_amps.to_bits(), pin_mask(&measurement))),
            missed,
        ),
        MeasurementMatch::NoMatch { missed, read_at: _ } => (None, missed),
    }
}

/// Replicate the production streaming loop's feed + fixed-chunk-drain logic
/// (see `Ppk2::start_measurement_matching` in `src/lib.rs`): read the stream in
/// `read_size`-byte slices, feed each to the accumulator, then drain in fixed
/// `chunk = (SPS_MAX/sps).max(1)`-sample units, combining each chunk via
/// `combine_matching`. Returns the sequence of emitted (reduced) matches.
fn run(stream: &[u8], metadata: &Metadata, read_size: usize, sps: usize) -> Vec<EmittedMatch> {
    let pins = LogicPortPins::default();
    let chunk = (SPS_MAX / sps).max(1);

    let mut acc = MeasurementAccumulator::new(metadata.clone());
    let mut measurement_buf: VecDeque<Measurement> = VecDeque::with_capacity(SPS_MAX);
    let mut emitted: Vec<EmittedMatch> = Vec::new();
    let mut missed = 0;

    for (read_index, read) in stream.chunks(read_size).enumerate() {
        // Production stamps `SystemTime::now()` here, immediately after
        // `read()` returns. A synthetic monotonically increasing stamp keeps
        // this test deterministic; the value is not compared across read sizes
        // (see `EmittedMatch`).
        let read_at = SystemTime::UNIX_EPOCH + Duration::from_micros(read_index as u64);
        missed += acc.feed_into(read, &mut measurement_buf);
        while measurement_buf.len() >= chunk {
            let m = measurement_buf
                .drain(..chunk)
                .combine_matching(missed, read_at, pins);
            emitted.push(reduce(m));
            missed = 0;
        }
    }
    // Leftover samples (< chunk) intentionally remain undrained, mirroring the
    // production loop which holds them for the next read.
    emitted
}

#[test]
fn drain_output_is_independent_of_read_size() {
    let metadata = Metadata::from_bytes(RAW_METADATA.as_bytes()).expect("metadata parse failed");
    let stream = build_stream();
    assert_eq!(stream.len(), N_FRAMES * 4);

    // (sps, expected chunk size) — one non-decimating and one decimating case.
    let cases = [(100_000usize, 1usize), (10_000usize, 10usize)];
    // Small (one-sample) reads vs large (1024-sample) reads.
    let read_sizes = [4usize, 4096usize];

    for (sps, expected_chunk) in cases {
        assert_eq!(
            (SPS_MAX / sps).max(1),
            expected_chunk,
            "sps={sps}: chunk size sanity check"
        );

        let baseline = run(&stream, &metadata, read_sizes[0], sps);
        assert!(
            !baseline.is_empty(),
            "sps={sps}: drain produced no measurements"
        );
        // At chunk=1 we expect one output per frame; at chunk=10, N/10.
        assert_eq!(
            baseline.len(),
            N_FRAMES / expected_chunk,
            "sps={sps}: unexpected number of emitted measurements"
        );

        for &read_size in &read_sizes[1..] {
            let other = run(&stream, &metadata, read_size, sps);
            assert_eq!(
                other.len(),
                baseline.len(),
                "sps={sps}: emitted COUNT differs between read sizes {} and {read_size}",
                read_sizes[0]
            );
            assert!(
                other == baseline,
                "sps={sps}: emitted measurements differ between read size {} and {read_size} \
                 — read buffer size changed decimated output!",
                read_sizes[0]
            );
        }

        println!(
            "sps={sps} chunk={expected_chunk}: {} emitted matches, IDENTICAL across read sizes {read_sizes:?}",
            baseline.len()
        );
    }
}
