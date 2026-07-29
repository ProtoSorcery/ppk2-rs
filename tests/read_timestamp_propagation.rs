//! Regression guard: the per-USB-read arrival stamp (`read_at`) MUST reach the
//! consumer on every emitted `MeasurementMatch`, and MUST never under-state
//! arrival.
//!
//! TEST-ONLY, non-shipping. Uses only `std` (no added crates).
//!
//! Background: a consumer that synthesizes PPK2 timestamps as
//! `anchor + sample_index * period` gets exact sample SPACING for free; the
//! only unknown is one absolute offset, estimated from `arrival - synthesized`.
//! If "arrival" is taken when the consumer DEQUEUES from this crate's mpsc
//! channel it also contains the queueing delay plus the consumer's own
//! scheduling — swings of tens of milliseconds that dominate, and destroy, the
//! estimate. `read_at` is therefore stamped inside the worker the instant the
//! USB `read()` returns, leaving only device buffering plus USB transfer.
//!
//! ## Why this test replicates the loop instead of driving it
//!
//! The production stamp is taken in `Ppk2::start_measurement_matching`
//! (`src/lib.rs`), inside a worker thread whose input is a real
//! `serialport::SerialPort` read from PPK2 hardware. That loop is NOT
//! reachable without a device: it is entered only after `Ppk2::new` has opened
//! a port and completed the metadata handshake, and the crate exposes no seam
//! for injecting a fake reader. So this test replicates the loop's
//! feed-then-fixed-chunk-drain structure EXACTLY (same as the sibling tests
//! `drain_chunking_equivalence.rs` and `missed_sample_accounting.rs` do) and
//! exercises the real `MeasurementAccumulator` / `combine_matching` code that
//! the stamp flows through. What is replicated, and therefore NOT covered
//! here, is precisely two lines of `src/lib.rs`: that `SystemTime::now()` is
//! called immediately after `port.read()` returns, and that the resulting
//! value is the one handed to `combine_matching`. Everything downstream of
//! those two lines is covered.
//!
//! Synthetic, monotonically increasing stamps stand in for `SystemTime::now()`
//! so the assertions are exact rather than timing-dependent — the test never
//! sleeps and has no wall-clock flake surface.
//!
//! Run:
//!   cargo test --test read_timestamp_propagation -- --nocapture

use std::collections::VecDeque;
use std::time::{Duration, SystemTime};

use ppk2::measurement::{
    Measurement, MeasurementAccumulator, MeasurementIterExt, MeasurementMatch,
};
use ppk2::types::{LogicPortPins, Metadata};
use ppk2::SPS_MAX;

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

/// Number of synthetic 4-byte frames in the long mixed-read-size stream.
const N_FRAMES: usize = 60_000;

/// Deliberate dropouts as `(first_frame_index, run_length)`, physically deleted
/// from the byte stream to emulate samples the host never received. Run lengths
/// stay in `1..64` (the device counter is 6 bits, so a run of exactly 64 wraps
/// to a zero gap and is undetectable in principle) and are spaced far apart so
/// no two can merge into a single >= 64 gap.
const DROPS: &[(usize, usize)] = &[(1_000, 1), (5_000, 7), (12_345, 63), (30_000, 3)];

fn metadata() -> Metadata {
    Metadata::from_bytes(RAW_METADATA.as_bytes()).expect("metadata parse failed")
}

/// Build one synthetic 4-byte PPK2 frame for LOGICAL sample index `i`.
///
/// Frame bit layout (matches `measurement.rs` masks):
///   adc     : bits  0..13 (14 bits)
///   range   : bits 14..16 (3 bits)
///   counter : bits 18..23 (6 bits)
///   logic   : bits 24..31 (8 bits)
fn make_frame(i: usize) -> u32 {
    let adc = (i.wrapping_mul(7) & 0x3FFF) as u32; // 14 bits
    let range = (i % 5) as u32; // 0..=4, drives spike-filter path
    let counter = (i % 64) as u32; // consecutive over the logical index
    let logic = (i & 0xFF) as u32; // 8 logic-port bits
    adc | (range << 14) | (counter << 18) | (logic << 24)
}

/// Bytes for logical frames `range`, with no gaps.
fn frames(range: std::ops::Range<usize>) -> Vec<u8> {
    let mut v = Vec::with_capacity(range.len() * 4);
    for i in range {
        v.extend_from_slice(&make_frame(i).to_le_bytes());
    }
    v
}

/// The synthetic stand-in for `SystemTime::now()` at USB read number `n`.
///
/// Strictly increasing in `n`, and far enough from the epoch that "the stamp
/// of an earlier read" is always representable.
fn stamp(n: usize) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_millis(1_000 + n as u64)
}

fn read_at_of(m: &MeasurementMatch) -> SystemTime {
    match *m {
        MeasurementMatch::Match { read_at, .. } => read_at,
        MeasurementMatch::NoMatch { read_at, .. } => read_at,
    }
}

fn missed_of(m: &MeasurementMatch) -> u64 {
    match *m {
        MeasurementMatch::Match { missed, .. } => u64::from(missed),
        MeasurementMatch::NoMatch { missed, .. } => u64::from(missed),
    }
}

/// Replicate the production streaming loop (see
/// `Ppk2::start_measurement_matching` in `src/lib.rs`), feeding each element of
/// `reads` as one USB read stamped with `stamp(read_index)`. Returns every
/// emitted match paired with the index of the read during which it was emitted.
fn run_reads(reads: &[Vec<u8>], sps: usize) -> Vec<(usize, MeasurementMatch)> {
    let pins = LogicPortPins::default();
    let chunk = (SPS_MAX / sps).max(1);

    let mut acc = MeasurementAccumulator::new(metadata());
    let mut measurement_buf: VecDeque<Measurement> = VecDeque::with_capacity(SPS_MAX);
    let mut missed = 0usize;
    let mut emitted = Vec::new();

    for (read_index, read) in reads.iter().enumerate() {
        let read_at = stamp(read_index);
        missed += acc.feed_into(read, &mut measurement_buf);
        while measurement_buf.len() >= chunk {
            let m = measurement_buf
                .drain(..chunk)
                .combine_matching(missed, read_at, pins);
            emitted.push((read_index, m));
            missed = 0;
        }
    }
    emitted
}

/// A measurement emitted from a read carries THAT read's stamp.
///
/// At `chunk == 1` every read of exactly one frame emits exactly one match, so
/// the emission-to-read correspondence is unambiguous.
#[test]
fn emitted_chunk_carries_the_timestamp_of_the_read_that_delivered_it() {
    let reads: Vec<Vec<u8>> = (0..5).map(|i| frames(i..i + 1)).collect();

    let emitted = run_reads(&reads, SPS_MAX);

    assert_eq!(
        emitted.len(),
        reads.len(),
        "chunk=1 must emit exactly one match per single-frame read"
    );
    for (i, (read_index, m)) in emitted.iter().enumerate() {
        assert_eq!(*read_index, i, "emission {i} came from an unexpected read");
        assert_eq!(
            read_at_of(m),
            stamp(i),
            "emission {i} must carry read {i}'s stamp, got {:?}",
            read_at_of(m)
        );
    }
}

/// A chunk that SPANS two reads carries the LATER read's stamp.
///
/// This is the documented upper-bound behaviour: leftover samples below `chunk`
/// stay buffered for the next read, and the completed chunk is attributed to the
/// read that finished it. Attributing the EARLIER read instead would under-state
/// arrival, which would let a consumer place a sample before it could possibly
/// have been captured.
#[test]
fn chunk_spanning_two_reads_carries_the_later_timestamp() {
    // sps=10_000 => chunk = 10 samples per emitted measurement.
    let sps = 10_000usize;
    let chunk = SPS_MAX / sps;
    assert_eq!(chunk, 10, "fixture assumes a 10-sample decimation chunk");

    let reads = vec![
        frames(0..4),   // read 0: 4 samples buffered, below chunk => no emission
        frames(4..10),  // read 1: completes the chunk that STARTED in read 0
        frames(10..20), // read 2: a chunk contained entirely within one read
    ];

    let emitted = run_reads(&reads, sps);

    assert_eq!(emitted.len(), 2, "expected exactly two emitted chunks");

    let (spanning_read, spanning) = &emitted[0];
    assert_eq!(*spanning_read, 1, "the spanning chunk completes in read 1");
    assert_eq!(
        read_at_of(spanning),
        stamp(1),
        "a chunk spanning reads 0 and 1 must carry read 1's (the LATER) stamp"
    );
    assert_ne!(
        read_at_of(spanning),
        stamp(0),
        "carrying read 0's stamp would UNDER-state arrival for the 6 samples \
         that only arrived in read 1"
    );

    let (contained_read, contained) = &emitted[1];
    assert_eq!(*contained_read, 2);
    assert_eq!(
        read_at_of(contained),
        stamp(2),
        "a chunk contained within a single read carries that read's stamp"
    );
}

/// Over a long stream with dropouts and irregular, non-frame-aligned read
/// sizes: the stamp never under-states arrival, AND threading `read_at` through
/// leaves the `missed` accounting invariant untouched.
///
/// The `missed` invariant (summing `missed` over every emission, plus whatever
/// is still pending at the end, equals the exact number of skipped raw samples)
/// is the property `missed_sample_accounting.rs` pins down; it is re-checked
/// here specifically in the presence of the new parameter.
#[test]
fn read_timestamp_never_understates_arrival_and_missed_still_balances() {
    let md = metadata();
    let pins = LogicPortPins::default();
    let sps = 10_000usize;
    let chunk = SPS_MAX / sps;

    // Build the stream with DROPS physically removed.
    let mut stream = Vec::with_capacity(N_FRAMES * 4);
    let mut removed = 0u64;
    for i in 0..N_FRAMES {
        if DROPS
            .iter()
            .any(|&(start, len)| i >= start && i < start + len)
        {
            removed += 1;
            continue;
        }
        stream.extend_from_slice(&make_frame(i).to_le_bytes());
    }
    let expected_removed: u64 = DROPS.iter().map(|&(_, len)| len as u64).sum();
    assert_eq!(removed, expected_removed, "fixture removed the wrong count");

    // Deliberately includes sizes that are NOT multiples of 4, so samples get
    // split across reads and the accumulator's partial-frame remainder is
    // exercised. A sample whose bytes span two reads is parsed during (and so
    // attributed to) the LATER one — still an upper bound on its capture time.
    let read_sizes = [6usize, 4096, 14, 4, 333];

    let mut acc = MeasurementAccumulator::new(md);
    let mut measurement_buf: VecDeque<Measurement> = VecDeque::with_capacity(SPS_MAX);
    // Parallel to `measurement_buf`: which read each buffered sample arrived in.
    let mut arrival: VecDeque<usize> = VecDeque::with_capacity(SPS_MAX);

    let mut missed = 0usize;
    let mut missed_total = 0u64;
    let mut emitted = 0usize;
    let mut spanning = 0usize;
    let mut offset = 0usize;
    let mut read_index = 0usize;

    while offset < stream.len() {
        let size = read_sizes[read_index % read_sizes.len()].min(stream.len() - offset);
        let read = &stream[offset..offset + size];
        offset += size;
        let read_at = stamp(read_index);

        let before = measurement_buf.len();
        missed += acc.feed_into(read, &mut measurement_buf);
        for _ in 0..(measurement_buf.len() - before) {
            arrival.push_back(read_index);
        }

        while measurement_buf.len() >= chunk {
            let m = measurement_buf
                .drain(..chunk)
                .combine_matching(missed, read_at, pins);
            let arrivals: Vec<usize> = arrival.drain(..chunk).collect();
            let first = arrivals[0];
            let last = *arrivals.last().expect("chunk is non-empty");
            if first != last {
                spanning += 1;
            }

            let emitted_at = read_at_of(&m);
            assert_eq!(
                emitted_at,
                stamp(read_index),
                "emission {emitted} must carry the stamp of the read that \
                 completed it (read {read_index})"
            );
            assert!(
                emitted_at >= stamp(last),
                "emission {emitted} carries {emitted_at:?}, EARLIER than the \
                 arrival of its own last sample (read {last}) — the stamp \
                 under-stated arrival, so a consumer could place a sample \
                 before it could possibly have been captured"
            );

            missed_total += missed_of(&m);
            emitted += 1;
            missed = 0;
        }
        read_index += 1;
    }

    assert!(emitted > 0, "nothing was emitted");
    assert!(
        spanning > 0,
        "fixture never produced a chunk spanning two reads, so the \
         upper-bound property was not actually exercised"
    );
    assert_eq!(
        missed_total + missed as u64,
        removed,
        "summed `missed` ({missed_total}) + pending ({missed}) != actually \
         skipped samples ({removed}) — threading `read_at` through broke the \
         skipped-sample accounting"
    );

    println!(
        "{emitted} emissions across {read_index} reads ({spanning} spanned two reads), \
         {removed} skipped raw samples fully accounted for"
    );
}

/// `combine` puts the stamp on BOTH variants, so a consumer never has to handle
/// its absence.
#[test]
fn combine_propagates_read_at_into_both_variants() {
    let t = stamp(7);

    let populated = vec![
        Measurement {
            micro_amps: 1.0,
            pins: [false; 8].into(),
        },
        Measurement {
            micro_amps: 3.0,
            pins: [false; 8].into(),
        },
    ];
    match populated.into_iter().combine(0, t) {
        MeasurementMatch::Match { read_at, .. } => assert_eq!(read_at, t),
        MeasurementMatch::NoMatch { .. } => panic!("expected Match for a non-empty chunk"),
    }

    // The empty-input path must carry the stamp too, exactly as it must carry
    // the pending `missed` count.
    let empty: Vec<Measurement> = Vec::new();
    match empty.into_iter().combine(42, t) {
        MeasurementMatch::NoMatch { read_at, missed } => {
            assert_eq!(read_at, t);
            assert_eq!(missed, 42, "the stamp must not displace the missed count");
        }
        MeasurementMatch::Match { .. } => panic!("expected NoMatch for an empty chunk"),
    }
}

/// Same for `combine_matching`, including the path where the logic-port filter
/// rejects every measurement.
#[test]
fn combine_matching_propagates_read_at_into_both_variants() {
    let t = stamp(9);
    let measurements = || {
        vec![
            Measurement {
                micro_amps: 1.0,
                pins: [false; 8].into(),
            },
            Measurement {
                micro_amps: 2.0,
                pins: [false; 8].into(),
            },
        ]
    };

    // Default pins match anything => Match.
    match measurements()
        .into_iter()
        .combine_matching(0, t, LogicPortPins::default())
    {
        MeasurementMatch::Match { read_at, .. } => assert_eq!(read_at, t),
        MeasurementMatch::NoMatch { .. } => panic!("expected Match with permissive pins"),
    }

    // Require every pin HIGH; all-low measurements cannot match => NoMatch.
    let require_all_high: LogicPortPins = [true; 8].into();
    match measurements()
        .into_iter()
        .combine_matching(7, t, require_all_high)
    {
        MeasurementMatch::NoMatch { read_at, missed } => {
            assert_eq!(read_at, t);
            assert_eq!(missed, 7, "the stamp must not displace the missed count");
        }
        MeasurementMatch::Match { .. } => panic!("expected NoMatch when no pins match"),
    }
}
