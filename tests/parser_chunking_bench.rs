//! Phase 0, Bench A — parser chunking equivalence + throughput baseline.
//!
//! TEST-ONLY, non-shipping. Uses only `std` (no criterion/dhat/added crates).
//!
//! Purpose: before a later phase enlarges the PPK2 USB read buffer (today the
//! driver reads ~4 bytes/syscall; later it will read 4–64KB chunks), prove that
//! `MeasurementAccumulator::feed_into` produces *byte-identical* decoded output
//! regardless of how the incoming byte stream is sliced, and quantify parse
//! throughput per chunk size.
//!
//! Run:
//!   cargo test --test parser_chunking_bench -- --nocapture
//!   cargo test --release --test parser_chunking_bench -- --nocapture   (stable timing)

use std::collections::VecDeque;
use std::time::Instant;

use ppk2::measurement::{Measurement, MeasurementAccumulator};
use ppk2::types::Metadata;

/// Number of synthetic 4-byte frames in the stream. ~3M => 12MB stream; the
/// 4-byte-chunk pass dominates and keeps the whole run to a few seconds.
const N_FRAMES: usize = 3_000_000;

/// Chunk sizes (bytes) to slice the identical stream into.
const CHUNK_SIZES: [usize; 4] = [4, 256, 4096, 65536];

/// Calibration metadata reused from the in-crate unit test so the accumulator
/// is constructed exactly as production does (`MeasurementAccumulator::new`
/// takes a parsed `Metadata`).
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
/// The 6-bit counter MUST increment by 1 (mod 64) per frame, otherwise the
/// accumulator treats the gap as "samples missed" and skips them. All fields
/// are derived deterministically from `i` (no RNG) for reproducibility, while
/// still exercising range changes (the spike-filter branch) and varied logic
/// pins / adc values.
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

/// A decoded sample reduced to a byte-comparable form: exact float bits + an
/// 8-bit pin bitmask. Two parser runs are "byte-identical" iff these match.
type DecodedSample = (u32, u8);

fn pin_mask(m: &Measurement) -> u8 {
    let mut mask = 0u8;
    for (i, level) in m.pins.inner().iter().enumerate() {
        if level.is_high() {
            mask |= 1 << i;
        }
    }
    mask
}

/// Feed the whole `stream` through a fresh accumulator, slicing it into
/// `chunk_size`-byte pieces (the final piece may be shorter). Returns the
/// decoded samples and the elapsed parse time.
fn run_chunked(stream: &[u8], metadata: &Metadata, chunk_size: usize) -> (Vec<DecodedSample>, std::time::Duration) {
    let mut acc = MeasurementAccumulator::new(metadata.clone());
    let mut ring: VecDeque<Measurement> = VecDeque::with_capacity(1024);
    let mut decoded: Vec<DecodedSample> = Vec::with_capacity(N_FRAMES);

    let start = Instant::now();
    for chunk in stream.chunks(chunk_size) {
        acc.feed_into(chunk, &mut ring);
        // Drain as we go so the ring stays small and we don't conflate
        // allocation with parse cost.
        while let Some(m) = ring.pop_front() {
            decoded.push((m.micro_amps.to_bits(), pin_mask(&m)));
        }
    }
    let elapsed = start.elapsed();
    (decoded, elapsed)
}

#[test]
fn parser_chunking_equivalence_and_throughput() {
    let metadata = Metadata::from_bytes(RAW_METADATA.as_bytes()).expect("metadata parse failed");

    println!("\n=== Phase 0 Bench A: PPK2 parser chunking ===");
    println!("frames = {N_FRAMES} ({} bytes stream)", N_FRAMES * 4);

    let stream = build_stream();
    assert_eq!(stream.len(), N_FRAMES * 4);

    let mut results: Vec<(usize, Vec<DecodedSample>, std::time::Duration)> = Vec::new();
    for &cs in &CHUNK_SIZES {
        let (decoded, elapsed) = run_chunked(&stream, &metadata, cs);
        results.push((cs, decoded, elapsed));
    }

    // --- CORRECTNESS: all four chunkings must be byte-identical ---
    let reference = &results[0].1;
    assert!(!reference.is_empty(), "parser produced no samples");
    for (cs, decoded, _) in &results[1..] {
        assert_eq!(
            decoded.len(),
            reference.len(),
            "chunk size {cs}: sample COUNT differs from 4-byte baseline ({} vs {})",
            decoded.len(),
            reference.len()
        );
        assert!(
            decoded == reference,
            "chunk size {cs}: decoded output is NOT byte-identical to 4-byte baseline"
        );
    }
    let n_samples = reference.len();
    println!(
        "EQUIVALENCE: PASS — all {} chunk sizes produced {} byte-identical samples\n",
        CHUNK_SIZES.len(),
        n_samples
    );

    // --- THROUGHPUT table ---
    println!(
        "{:>12} | {:>14} | {:>16} | {:>14}",
        "chunk bytes", "elapsed (ms)", "ns/sample", "samples/sec"
    );
    println!("{}", "-".repeat(64));
    for (cs, _, elapsed) in &results {
        let secs = elapsed.as_secs_f64();
        let ns_per_sample = (elapsed.as_nanos() as f64) / (n_samples as f64);
        let samples_per_sec = (n_samples as f64) / secs;
        println!(
            "{:>12} | {:>14.2} | {:>16.2} | {:>14.0}",
            cs,
            secs * 1000.0,
            ns_per_sample,
            samples_per_sec
        );
    }
    println!();
}
