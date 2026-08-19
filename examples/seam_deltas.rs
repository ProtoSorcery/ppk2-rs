//! Measure how much the per-block timestamp's *lateness* varies between
//! consecutive blocks.
//!
//! Every block is stamped from its own `read_at`, which is always LATE by some
//! amount `d` (transport plus scheduling). `d` is always positive — we can
//! never observe a sample before it was acquired. But `d` is an estimate, and
//! consecutive estimates need not be late by the same amount.
//!
//! At the seam between two blocks the physical truth is always exactly one
//! sample period, so what we observe is:
//!
//! ```text
//! observed_seam = one_period + (d_next - d_prev)
//! ```
//!
//! `d_next < d_prev` closes the seam (the crate's seam guard trims that).
//! `d_next > d_prev` opens it, and looks exactly like a hole that isn't there.
//!
//! This matters because the downstream decimation cascade closes a bucket
//! early whenever it sees a seam wider than float rounding error. If lateness
//! varies by more than that, real coarse buckets get shattered by noise. This
//! example measures the distribution so that question is settled by data
//! rather than argument.
//!
//! Emissions carrying the same `read_at` form one block; a seam is the
//! boundary between two such runs. Seams immediately following a `Dropped`
//! are excluded, since those gaps are real by construction.

use anyhow::Result;
use clap::Parser;
use ppk2::{
    measurement::MeasurementMatch,
    try_find_ppk2_port,
    types::{DevicePower, LogicPortPins, MeasurementMode, SourceVoltage},
    Ppk2,
};
use std::{
    fs::File,
    io::{BufWriter, Write},
    sync::mpsc::RecvTimeoutError,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Parser)]
struct Args {
    /// Serial port the PPK2 is on. Auto-detected if unspecified.
    #[clap(short = 'p', long)]
    serial_port: Option<String>,
    /// Samples per second requested from the device.
    #[clap(long, default_value = "1000")]
    sps: usize,
    /// Capture duration in seconds.
    #[clap(long, default_value = "60")]
    seconds: u64,
    /// Optional ndjson of every seam, so the run can be re-analysed without
    /// touching the hardware again.
    #[clap(long)]
    out: Option<String>,
    /// Source voltage in mV.
    #[clap(long, default_value = "3300")]
    voltage: SourceVoltage,
}

fn epoch_nanos(t: SystemTime) -> i128 {
    t.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i128
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx]
}

fn main() -> Result<()> {
    let args = Args::parse();
    let port = match args.serial_port {
        Some(p) => p,
        None => try_find_ppk2_port()?,
    };

    let mut ppk2 = Ppk2::new(port.clone(), MeasurementMode::Source)?;
    ppk2.set_source_voltage(args.voltage)?;
    ppk2.set_device_power(DevicePower::Enabled)?;

    let period_ns = 1_000_000_000i128 / args.sps as i128;
    let (rx, kill) = ppk2.start_measurement_matching(LogicPortPins::default(), args.sps)?;

    eprintln!("port {port}, {} sps, period {period_ns} ns", args.sps);
    eprintln!("measuring seam deltas for {} s ...", args.seconds);

    let mut writer = match &args.out {
        Some(path) => Some(BufWriter::new(File::create(path)?)),
        None => None,
    };

    // Current run: the stamp it carries and how many emissions share it.
    let mut run_stamp: Option<i128> = None;
    let mut run_len: i128 = 0;
    let mut deltas_us: Vec<f64> = Vec::new();
    let mut skip_next_seam = false;

    let mut matches = 0u64;
    let mut no_matches = 0u64;
    let mut dropped = 0u64;
    let mut dropped_samples = 0u64;
    let mut overlaps = 0u64;
    let mut excluded = 0u64;

    let deadline = Instant::now() + Duration::from_secs(args.seconds);
    while Instant::now() < deadline {
        let msg = match rx.recv_timeout(Duration::from_millis(500)) {
            Ok(m) => m,
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => break,
        };

        let stamp = match msg {
            MeasurementMatch::Match { read_at, .. } => {
                matches += 1;
                epoch_nanos(read_at)
            }
            MeasurementMatch::NoMatch { read_at, .. } => {
                no_matches += 1;
                epoch_nanos(read_at)
            }
            MeasurementMatch::Dropped { samples, .. } => {
                dropped += 1;
                dropped_samples += samples;
                // The next seam spans a real hole; not a lateness measurement.
                skip_next_seam = true;
                continue;
            }
            MeasurementMatch::SeamOverlap { .. } => {
                overlaps += 1;
                continue;
            }
        };

        match run_stamp {
            None => {
                run_stamp = Some(stamp);
                run_len = 1;
            }
            Some(prev) if stamp == prev => run_len += 1,
            Some(prev) => {
                if skip_next_seam {
                    excluded += 1;
                    skip_next_seam = false;
                } else {
                    let expected = prev + run_len * period_ns;
                    let delta_ns = stamp - expected;
                    let delta_us = delta_ns as f64 / 1000.0;
                    deltas_us.push(delta_us);
                    if let Some(w) = writer.as_mut() {
                        writeln!(
                            w,
                            "{{\"prev_read_at_ns\":{prev},\"run_len\":{run_len},\"delta_us\":{delta_us:.4}}}"
                        )?;
                    }
                }
                run_stamp = Some(stamp);
                run_len = 1;
            }
        }
    }

    let _ = kill();
    if let Some(w) = writer.as_mut() {
        w.flush()?;
    }

    println!("\n================ seam delta summary ================");
    println!("emissions       : {matches} match, {no_matches} no-match");
    println!("events          : {dropped} Dropped ({dropped_samples} samples), {overlaps} SeamOverlap");
    println!("seams measured  : {} (excluded after Dropped: {excluded})", deltas_us.len());

    if deltas_us.is_empty() {
        println!("no seams observed — nothing to report");
        return Ok(());
    }

    let mut sorted = deltas_us.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

    println!("\n---- seam delta, microseconds (0 = perfectly contiguous) ----");
    println!("  min    {:>12.3}", sorted[0]);
    println!("  p50    {:>12.3}", percentile(&sorted, 0.50));
    println!("  p90    {:>12.3}", percentile(&sorted, 0.90));
    println!("  p99    {:>12.3}", percentile(&sorted, 0.99));
    println!("  max    {:>12.3}", sorted[sorted.len() - 1]);

    let n = sorted.len() as f64;
    let within = |lim: f64| sorted.iter().filter(|d| d.abs() <= lim).count();
    println!("\n---- what the gatherer's gap detector would see ----");
    println!(
        "  |delta| <= 1.5 us (its current threshold) : {} ({:.2}%)",
        within(1.5),
        100.0 * within(1.5) as f64 / n
    );
    for lim in [10.0f64, 100.0, 1000.0] {
        println!(
            "  |delta| <= {:>6.0} us                        : {} ({:.2}%)",
            lim,
            within(lim),
            100.0 * within(lim) as f64 / n
        );
    }

    let one_period_us = period_ns as f64 / 1000.0;
    let opening = sorted.iter().filter(|d| **d > 1.5).count();
    let closing = sorted.iter().filter(|d| **d < -1.5).count();
    println!("\n  seams opening  (> +1.5 us, look like a hole)    : {opening}");
    println!("  seams closing  (< -1.5 us, look like an overlap): {closing}");
    println!("  one sample period at this rate                  : {one_period_us:.1} us");
    println!("====================================================");

    Ok(())
}
