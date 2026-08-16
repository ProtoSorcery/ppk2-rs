//! `transport_probe` — characterise the PPK2 serial transport's read cadence.
//!
//! # What this measures and why
//!
//! A consumer that synthesises sample timestamps as `anchor + N * period` — where
//! `N` only advances when samples actually arrive — subtracts every un-received
//! sample-time from its own clock. The output stays perfectly contiguous and
//! monotonic while sliding arbitrarily far into the past. Fixing that needs a
//! per-block timestamp contract, and that contract rests on knowing which
//! blocks arrived promptly and which arrived as a backlog.
//!
//! # The predicate, version 2 — packet depth
//!
//! The original rule was "a block is trustable when the read that completed it
//! DRAINED the port", detected as `filled < req_len`. **The macOS baseline
//! killed that rule.** Asking for 4096 bytes, the largest read observed across
//! 722,141 reads was 1020 bytes: the driver has its own ceiling well below the
//! request, so `filled == req_len` can never fire and `filled < req_len` is
//! true of literally every read. The predicate carried no information.
//!
//! What the same capture showed instead: 97.26% of reads return exactly 64
//! bytes — one USB full-speed bulk packet, 16 samples, 160 µs of stream — and
//! the reads that are larger are the interesting ones. During an observed
//! 15.2 ms stall, four consecutive 1020-byte reads arrived 253/262/24 µs apart,
//! each carrying 2.55 ms of samples: a backlog draining. So the real signal is
//! **how many packets a read returned**:
//!
//! * `filled <= packet_size` — one packet, or a short terminating packet. The
//!   port had nothing more queued. **Trustable.**
//! * `filled > packet_size` — the read swept up a backlog.
//!   **Untrusted**, with `depth = ceil(filled / packet_size)`.
//!
//! `--packet-size` defaults to 64 and is deliberately NOT hardcoded: full-speed
//! bulk fixes the packet at 64 bytes, but a high-speed host would use 512, and
//! that must surface as a visibly wrong flag rather than as silently wrong data.
//!
//! # Untrusted runs — the ring-buffer sizing measurement
//!
//! An **untrusted run** begins at the first read with `filled > packet_size` and
//! ends at the next read with `filled <= packet_size`. The samples inside a run
//! are exactly what a real implementation must hold before it can place them, so
//! the **maximum `samples` across all runs IS the required ring size**. This
//! tool measures that rather than letting anyone pick a constant; the maximum is
//! called out prominently in the summary and every run is emitted as its own
//! `"record":"untrusted_run"` line, joinable to the read records by `seq`.
//!
//! # Why it does not use `Ppk2::start_measurement_matching`
//!
//! We are characterising the TRANSPORT, not the crate's accumulation and
//! decimation logic. So this opens the serial port directly, replicates only the
//! command sequence that `Ppk2::new` + `start_measurement_matching` use to put
//! the device into streaming mode, and then runs its own raw read loop.
//!
//! # The probe is now diagnostic only
//!
//! `--probe` performs a follow-up non-blocking read that reports whether bytes
//! were still queued. It no longer feeds `trustable` — the packet-depth rule
//! above is unconditional — but it remains a useful independent cross-check on
//! that rule and its result is still recorded as `probe_filled`.
//!
//! The probe is strictly NON-BLOCKING (a zero poll timeout on unix, the
//! documented zero-wait `COMMTIMEOUTS` form on Windows). This is not an
//! optimisation: the PPK2 streams ~400 bytes per millisecond, so a probe that
//! waited even 1 ms would find bytes essentially every time. Expect a small
//! non-zero `probe_filled` occasionally regardless, from the few microseconds
//! of syscall overhead between the read returning and the probe issuing.
//!
//! Bytes returned by a probe are real stream data and are preserved: they are
//! appended to the raw file and described by their own `"record":"probe"` line,
//! so `raw_offset + filled` chains exactly into the next byte-carrying record's
//! `raw_offset` with no gaps. (`untrusted_run` records carry no raw bytes and
//! are not part of that chain.)
//!
//! # Output
//!
//! * `capture_raw.bin` — the concatenated raw bytes exactly as read, nothing else.
//! * `capture_meta.ndjson` — a header line, then one line per read, with
//!   `untrusted_run` records interleaved as runs close. `raw_offset` and
//!   `filled` join every byte back to the read that delivered it.
//!
//! The header carries `"format_version": 2`. Captures taken before this rule
//! change have no such field and are version 1; their `trustable` field was
//! computed under the dead drain rule and MUST NOT be read as if it meant
//! packet depth.
//!
//! Both a monotonic (`instant_us`) and a wall (`systime_us`) stamp are recorded
//! on every read: `Instant` does not advance across OS suspend on macOS/Linux
//! while `SystemTime` does, and detecting that divergence is one of the things
//! this capture must be able to show.
//!
//! # Usage
//!
//! ```text
//! cargo run --release --example transport_probe -- --seconds 120 --out ./capture
//! ```

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use ppk2::{
    cmd::Command,
    try_find_ppk2_port,
    types::{MeasurementMode, Metadata},
    SPS_MAX,
};
use serialport::{FlowControl, SerialPort};
use std::{
    cmp::Reverse,
    fs::{self, File},
    io::{self, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// Size of the crate's reusable USB read buffer, in bytes (`src/lib.rs`,
/// `USB_READ_BUF_BYTES`). Mirrored here because the crate's constant is
/// private, and this example must not modify `src/`.
const USB_READ_BUF_BYTES: usize = 4 * 1024;

/// The crate's per-read byte target when native read coalescing is active
/// (`src/lib.rs`, `READ_ACCUM_TARGET_BYTES`). Mirrored for the same reason.
const READ_ACCUM_TARGET_BYTES: usize = 2 * 1024;

/// Bytes per raw PPK2 sample (`u32::from_le_bytes`, `src/measurement.rs`).
const SAMPLE_BYTES: usize = 4;

/// The port timeout the crate configures in `Ppk2::new`.
const PORT_TIMEOUT: Duration = Duration::from_millis(500);

/// Read timeout used for the follow-up probe on unix.
///
/// # This MUST be zero
///
/// The probe asks "was anything still buffered at the instant the read
/// returned". Any non-zero timeout turns it into "did anything arrive within
/// the next N", which is a different — and at this data rate, useless —
/// question: the PPK2 streams ~400 bytes per millisecond, so a 1 ms probe would
/// find bytes essentially every time and report that no read ever drains the
/// port. That would be a false negative manufactured by the instrument.
///
/// A zero timeout makes the backend's `poll` return immediately with only
/// whether data is ready right now.
///
/// The residual error is the probe's own latency: the few microseconds between
/// the main read returning and the probe's `poll`, during which ~4 bytes
/// (1 sample) arrive per 10 µs. That is three orders of magnitude below the
/// ~10 ms a full buffer represents, so it does not distort the measurement —
/// but it does mean a small non-zero `probe_filled` is expected occasionally
/// even on a genuinely drained port.
#[cfg(not(windows))]
const PROBE_TIMEOUT: Duration = Duration::ZERO;

/// Windows only: the coalescing window the crate programs for the duration of a
/// measurement (`src/lib.rs`, `WIN_READ_COALESCE_MS`).
#[cfg(windows)]
const WIN_READ_COALESCE_MS: u32 = 5;

/// How many of the largest inter-read gaps to keep for the summary.
const TOP_GAPS: usize = 12;

/// How many of the largest untrusted runs to keep for the summary.
const TOP_RUNS: usize = 10;

/// ndjson schema version, written into the header record.
///
/// Version 1 captures used the "a short read means the port drained" predicate,
/// which the macOS baseline disproved (see the module docs). Their `trustable`
/// field means something different and must not be mixed with version 2 data.
const FORMAT_VERSION: u32 = 2;

/// Default USB bulk packet size: full-speed bulk endpoints are fixed at 64
/// bytes, which is what the PPK2 enumerates as. Overridable via `--packet-size`
/// because a high-speed host would use 512.
const DEFAULT_PACKET_SIZE: usize = 64;

/// Bucket edges for the `filled` histogram. Bucket `i` covers
/// `[EDGES[i], EDGES[i + 1] - 1]`; the last is open-ended.
const BUCKET_EDGES: [usize; 10] = [1, 64, 128, 256, 512, 1024, 2048, 3072, 4096, 8192];

/// Packet-depth buckets for the summary: 1, 2, 3–4, 5–8, 9–16, >16 packets.
/// Each entry is the inclusive lower bound; the last is open-ended.
const DEPTH_BUCKETS: [usize; 6] = [1, 2, 3, 5, 9, 17];

/// Bucket edges for the untrusted-run sample histogram. Same convention.
const RUN_SAMPLE_EDGES: [u64; 7] = [1, 16, 64, 256, 1024, 4096, 16384];

/// Windows-native serial timeout programming.
///
/// This mirrors the crate's private `win_read_coalescing` module (`src/lib.rs`)
/// so the probe observes the SAME driver behaviour the production read loop
/// does, plus an "immediate return" variant used only by the follow-up probe.
///
/// `windows-sys` is already a `cfg(windows)` dependency of this crate, so this
/// introduces nothing new into the dependency tree.
#[cfg(windows)]
mod win_timeouts {
    use std::io;
    use windows_sys::Win32::Devices::Communication::{SetCommTimeouts, COMMTIMEOUTS};
    use windows_sys::Win32::Foundation::HANDLE;

    /// Program the device to batch reads: a read completes when the caller's
    /// buffer is full or `coalesce_ms` expires.
    pub fn apply_coalescing(
        handle: isize,
        coalesce_ms: u32,
        write_timeout_ms: u32,
    ) -> io::Result<()> {
        set(
            handle,
            &COMMTIMEOUTS {
                ReadIntervalTimeout: 0,
                ReadTotalTimeoutMultiplier: 0,
                ReadTotalTimeoutConstant: coalesce_ms,
                WriteTotalTimeoutMultiplier: 0,
                WriteTotalTimeoutConstant: write_timeout_ms,
            },
        )
    }

    /// Program the device to return IMMEDIATELY with whatever is already
    /// buffered, even if that is nothing.
    ///
    /// `ReadIntervalTimeout = MAXDWORD` with BOTH total-timeout fields zero is
    /// the exact combination the `COMMTIMEOUTS` documentation defines as
    /// "return immediately with the bytes that have already been received, even
    /// if no bytes have been received". Both zeros are load-bearing: a non-zero
    /// constant would let the read block for that long when the buffer is
    /// empty, and at ~400 bytes/ms the probe would then find bytes essentially
    /// every time and report that no read ever drains the port — a false
    /// negative manufactured by the instrument.
    ///
    /// Note this differs from what `serialport`'s `set_timeout` writes
    /// (`Multiplier = MAXDWORD`, `Constant = timeout`); we want the documented
    /// zero-wait form, not an approximation of it.
    pub fn apply_immediate(handle: isize, write_timeout_ms: u32) -> io::Result<()> {
        set(
            handle,
            &COMMTIMEOUTS {
                ReadIntervalTimeout: u32::MAX,
                ReadTotalTimeoutMultiplier: 0,
                ReadTotalTimeoutConstant: 0,
                WriteTotalTimeoutMultiplier: 0,
                WriteTotalTimeoutConstant: write_timeout_ms,
            },
        )
    }

    fn set(handle: isize, timeouts: &COMMTIMEOUTS) -> io::Result<()> {
        // `HANDLE` is a plain `isize` in windows-sys 0.52 (the version pinned in
        // Cargo.toml), so the stored handle passes through without a cast.
        let handle: HANDLE = handle;
        // SAFETY: `handle` is the raw handle of the `COMPort` owned by `main`'s
        // `port`, which outlives every call site; `timeouts` is a fully
        // initialized `#[repr(C)]` `COMMTIMEOUTS` that the call only reads.
        if unsafe { SetCommTimeouts(handle, timeouts) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

#[derive(Parser)]
#[command(
    about = "Characterise the PPK2 serial transport: per-read sizes, timing, packet depth, and the untrusted-run backlog that sets the required ring size",
    long_about = None
)]
struct Args {
    /// Serial port the PPK2 is on. If unspecified, the PPK2 is located by USB VID/PID.
    #[arg(short = 'p', long)]
    port: Option<String>,

    /// Capture duration in seconds. 0 means run until Ctrl-C.
    #[arg(long, default_value_t = 60)]
    seconds: u64,

    /// Directory for capture_raw.bin and capture_meta.ndjson.
    #[arg(long, default_value = "./capture")]
    out: PathBuf,

    /// Bytes to request per read. Defaults to the crate's platform choice:
    /// 2048 on Windows (native read coalescing), 4096 elsewhere.
    #[arg(long)]
    read_len: Option<usize>,

    /// USB bulk packet size in bytes. A read returning more than this swept up a
    /// backlog and is untrusted. 64 is correct for full-speed bulk; a
    /// high-speed host would need 512.
    #[arg(long, default_value_t = DEFAULT_PACKET_SIZE)]
    packet_size: usize,

    /// Perform a follow-up non-blocking probe read. Diagnostic only — it no
    /// longer feeds `trustable`. Defaults to on for Windows, off elsewhere.
    #[arg(long, num_args = 0..=1, default_missing_value = "true")]
    probe: Option<bool>,
}

/// Resolved run configuration, for the header line and the summary.
struct Cfg {
    port_path: String,
    read_len: usize,
    packet_size: usize,
    probe_enabled: bool,
    native_coalescing: bool,
    seconds: u64,
    out_dir: PathBuf,
}

/// One byte-carrying line of `capture_meta.ndjson`.
struct ReadRecord<'a> {
    /// `"read"` for a main read, `"probe"` for bytes a follow-up probe took.
    kind: &'a str,
    seq: u64,
    req_len: usize,
    filled: usize,
    /// `ceil(filled / packet_size)`; 0 for an empty read. 1 means a single USB
    /// packet, anything higher means the read swept up a backlog.
    depth: usize,
    raw_offset: u64,
    systime_us: u128,
    instant_us: u128,
    probe_filled: Option<usize>,
    trustable: bool,
    err: Option<&'a str>,
}

/// A closed (or capture-truncated) untrusted run.
///
/// A run is the consecutive stretch of reads with `filled > packet_size`. The
/// trustable read that ends the run is NOT counted in any of these fields — it
/// is the boundary, not a member — so `samples` is exactly the backlog a real
/// implementation would have had to buffer before it could place anything.
#[derive(Clone, Copy)]
struct UntrustedRun {
    /// `seq` of the run's first untrusted read.
    start_seq: u64,
    /// `seq` of the run's LAST untrusted read (not the trustable read that
    /// terminated it).
    end_seq: u64,
    start_instant_us: u128,
    /// Wall time from the run's first read to its last.
    duration_us: u128,
    reads: u64,
    samples: u64,
    bytes: u64,
    peak_depth: usize,
    /// False when the capture ended before a trustable read closed the run, so
    /// its totals are a lower bound rather than a complete measurement.
    terminated: bool,
}

/// A run still being accumulated.
struct RunAccum {
    start_seq: u64,
    end_seq: u64,
    start_instant_us: u128,
    last_instant_us: u128,
    reads: u64,
    samples: u64,
    bytes: u64,
    peak_depth: usize,
}

impl RunAccum {
    fn new(seq: u64, instant_us: u128) -> Self {
        Self {
            start_seq: seq,
            end_seq: seq,
            start_instant_us: instant_us,
            last_instant_us: instant_us,
            reads: 0,
            samples: 0,
            bytes: 0,
            peak_depth: 0,
        }
    }

    fn push(&mut self, seq: u64, instant_us: u128, filled: usize, depth: usize) {
        self.end_seq = seq;
        self.last_instant_us = instant_us;
        self.reads += 1;
        self.samples += (filled / SAMPLE_BYTES) as u64;
        self.bytes += filled as u64;
        self.peak_depth = self.peak_depth.max(depth);
    }

    fn close(&self, terminated: bool) -> UntrustedRun {
        UntrustedRun {
            start_seq: self.start_seq,
            end_seq: self.end_seq,
            start_instant_us: self.start_instant_us,
            duration_us: self.last_instant_us.saturating_sub(self.start_instant_us),
            reads: self.reads,
            samples: self.samples,
            bytes: self.bytes,
            peak_depth: self.peak_depth,
            terminated,
        }
    }
}

/// A large gap between the completion of two consecutive reads.
#[derive(Clone, Copy)]
struct GapRec {
    gap_us: u128,
    seq: u64,
    systime_us: u128,
    instant_us: u128,
}

/// Result of a follow-up probe read.
struct ProbeOutcome {
    /// `None` means the probe itself failed, so drainage is unknown.
    filled: Option<usize>,
    err: Option<String>,
    systime: SystemTime,
    instant: Instant,
}

impl ProbeOutcome {
    fn failed(err: String) -> Self {
        Self {
            filled: None,
            err: Some(err),
            systime: SystemTime::now(),
            instant: Instant::now(),
        }
    }

    fn from_read(res: io::Result<usize>, systime: SystemTime, instant: Instant) -> Self {
        match res {
            Ok(n) => Self {
                filled: Some(n),
                err: None,
                systime,
                instant,
            },
            // An "empty" error is the probe's SUCCESS case: nothing was waiting,
            // so the preceding read drained the port.
            Err(e) if is_empty_error(&e) => Self {
                filled: Some(0),
                err: None,
                systime,
                instant,
            },
            Err(e) => Self {
                filled: None,
                err: Some(format!("probe read failed: {e}")),
                systime,
                instant,
            },
        }
    }
}

/// Running totals over the whole capture.
struct Stats {
    /// Main reads only (probe reads are excluded so the distribution describes
    /// the read size we actually asked for).
    total_reads: u64,
    /// ALL bytes written to the raw file, probe bytes included — every byte the
    /// device delivered counts towards the drift computation.
    total_bytes: u64,
    probe_reads: u64,
    probe_bytes: u64,
    zero_reads: u64,
    short_reads: u64,
    full_reads: u64,
    err_reads: u64,
    trustable_reads: u64,
    /// Largest single read seen. The baseline's key finding — 1020 against a
    /// 4096-byte request — lives here: it exposes the driver's own ceiling.
    max_filled: usize,
    /// `hist[n]` counts main reads that returned exactly `n` bytes.
    hist: Vec<u64>,
    /// `depth_hist[d]` counts main reads with packet depth `d`.
    depth_hist: Vec<u64>,
    top_gaps: Vec<GapRec>,
    // Untrusted-run accounting.
    total_runs: u64,
    untrusted_reads: u64,
    /// `run_hist[i]` counts runs whose `samples` fall in `RUN_SAMPLE_EDGES[i]`.
    run_hist: Vec<u64>,
    /// The single largest run by `samples`. THIS is the required ring size.
    max_run: Option<UntrustedRun>,
    top_runs: Vec<UntrustedRun>,
}

impl Stats {
    fn new(read_len: usize, packet_size: usize) -> Self {
        Self {
            total_reads: 0,
            total_bytes: 0,
            probe_reads: 0,
            probe_bytes: 0,
            zero_reads: 0,
            short_reads: 0,
            full_reads: 0,
            err_reads: 0,
            trustable_reads: 0,
            max_filled: 0,
            hist: vec![0; read_len + 1],
            depth_hist: vec![0; read_len.div_ceil(packet_size) + 1],
            top_gaps: Vec::with_capacity(TOP_GAPS + 1),
            total_runs: 0,
            untrusted_reads: 0,
            run_hist: vec![0; RUN_SAMPLE_EDGES.len()],
            max_run: None,
            top_runs: Vec::with_capacity(TOP_RUNS + 1),
        }
    }

    fn record_gap(&mut self, gap: GapRec) {
        let smallest = self.top_gaps.last().map_or(0, |g| g.gap_us);
        if self.top_gaps.len() < TOP_GAPS || gap.gap_us > smallest {
            self.top_gaps.push(gap);
            self.top_gaps.sort_unstable_by_key(|g| Reverse(g.gap_us));
            self.top_gaps.truncate(TOP_GAPS);
        }
    }

    fn record_run(&mut self, run: UntrustedRun) {
        self.total_runs += 1;
        let bucket = RUN_SAMPLE_EDGES
            .iter()
            .rposition(|&lo| run.samples >= lo)
            .unwrap_or(0);
        self.run_hist[bucket] += 1;

        if self.max_run.is_none_or(|m| run.samples > m.samples) {
            self.max_run = Some(run);
        }

        let smallest = self.top_runs.last().map_or(0, |r| r.samples);
        if self.top_runs.len() < TOP_RUNS || run.samples > smallest {
            self.top_runs.push(run);
            self.top_runs.sort_unstable_by_key(|r| Reverse(r.samples));
            self.top_runs.truncate(TOP_RUNS);
        }
    }
}

/// Per-second counters for the live progress line.
struct Window {
    start: Instant,
    reads: u64,
    bytes: u64,
    trustable: u64,
    runs: u64,
    max_depth: usize,
    max_gap_us: u128,
}

impl Window {
    fn new(start: Instant) -> Self {
        Self {
            start,
            reads: 0,
            bytes: 0,
            trustable: 0,
            runs: 0,
            max_depth: 0,
            max_gap_us: 0,
        }
    }
}

fn main() -> Result<()> {
    let args = Args::parse();

    // The crate picks the read length by platform: with native read coalescing
    // (Windows) it asks for the accumulation target, otherwise for the whole
    // buffer. See the read loop in `Ppk2::start_measurement_matching`.
    let default_read_len = if cfg!(windows) {
        READ_ACCUM_TARGET_BYTES.min(USB_READ_BUF_BYTES)
    } else {
        USB_READ_BUF_BYTES
    };
    let read_len = args.read_len.unwrap_or(default_read_len);
    if read_len == 0 {
        return Err(anyhow!("--read-len must be greater than 0"));
    }
    if args.packet_size == 0 {
        return Err(anyhow!("--packet-size must be greater than 0"));
    }
    if args.packet_size > read_len {
        return Err(anyhow!(
            "--packet-size ({}) exceeds --read-len ({}), so no read could ever exceed one packet \
             and every read would be trivially trustable",
            args.packet_size,
            read_len
        ));
    }
    let probe_enabled = args.probe.unwrap_or(cfg!(windows));

    let port_path = match args.port {
        Some(p) => p,
        None => try_find_ppk2_port().context("could not locate a PPK2 by USB VID/PID")?,
    };

    fs::create_dir_all(&args.out)
        .with_context(|| format!("failed to create output directory {}", args.out.display()))?;
    let raw_path = args.out.join("capture_raw.bin");
    let meta_path = args.out.join("capture_meta.ndjson");

    let builder = serialport::new(port_path.clone(), 9600)
        .timeout(PORT_TIMEOUT)
        .flow_control(FlowControl::Hardware);

    // On Windows open the NATIVE port type so the raw handle can be captured
    // before boxing — `SetCommTimeouts` needs it and the `SerialPort` trait
    // exposes no way to recover a handle from a `Box<dyn SerialPort>`. This
    // mirrors `Ppk2::new`.
    #[cfg(windows)]
    let (mut port, native_handle): (Box<dyn SerialPort>, isize) = {
        use std::os::windows::io::AsRawHandle;
        let native = builder
            .open_native()
            .with_context(|| format!("failed to open {port_path}"))?;
        let handle = native.as_raw_handle() as isize;
        (Box::new(native), handle)
    };
    #[cfg(not(windows))]
    let mut port: Box<dyn SerialPort> = builder
        .open()
        .with_context(|| format!("failed to open {port_path}"))?;

    let metadata = setup_device(&mut port).context("failed to put the PPK2 into a known state")?;

    // Ask the OS serial driver to batch reads for the duration of the capture,
    // exactly as `start_measurement_matching` does, so what we measure is what
    // the production read loop would see. Failure is not fatal; it only means
    // reads keep arriving in the small pieces the default timeouts produce.
    #[cfg(windows)]
    let write_timeout_ms = u32::try_from(PORT_TIMEOUT.as_millis()).unwrap_or(u32::MAX);
    #[cfg(windows)]
    let native_coalescing =
        match win_timeouts::apply_coalescing(native_handle, WIN_READ_COALESCE_MS, write_timeout_ms)
        {
            Ok(()) => true,
            Err(e) => {
                eprintln!("warning: failed to enable native read coalescing: {e}");
                false
            }
        };
    #[cfg(not(windows))]
    let native_coalescing = false;

    let cfg = Cfg {
        port_path,
        read_len,
        packet_size: args.packet_size,
        probe_enabled,
        native_coalescing,
        seconds: args.seconds,
        out_dir: args.out,
    };

    let mut raw = BufWriter::with_capacity(
        1 << 20,
        File::create(&raw_path)
            .with_context(|| format!("failed to create {}", raw_path.display()))?,
    );
    let mut meta = BufWriter::with_capacity(
        1 << 16,
        File::create(&meta_path)
            .with_context(|| format!("failed to create {}", meta_path.display()))?,
    );

    let running = Arc::new(AtomicBool::new(true));
    {
        let running = running.clone();
        ctrlc::set_handler(move || running.store(false, Ordering::SeqCst))
            .context("failed to install the Ctrl-C handler")?;
    }

    // Clear the input buffer and start the stream. This is the exact handshake
    // `start_measurement_matching` performs before its read loop runs.
    if let Err(e) = port.clear(serialport::ClearBuffer::Input) {
        eprintln!("warning: failed to clear the input buffer: {e}");
    }
    let start_systime = SystemTime::now();
    let start_instant = Instant::now();
    write_command(&mut port, Command::AverageStart).context("failed to send AverageStart")?;

    writeln!(
        meta,
        "{}",
        header_line(&cfg, start_systime, metadata.as_ref())
    )
    .context("failed to write the ndjson header")?;

    eprintln!(
        "capturing from {} for {} — req_len {} B, probe {}, native coalescing {}",
        cfg.port_path,
        if cfg.seconds == 0 {
            "an unlimited time (Ctrl-C to stop)".to_string()
        } else {
            format!("{} s", cfg.seconds)
        },
        cfg.read_len,
        if cfg.probe_enabled { "on" } else { "off" },
        cfg.native_coalescing
    );

    let outcome = capture_loop(
        &mut port,
        &cfg,
        &running,
        start_instant,
        &mut raw,
        &mut meta,
        #[cfg(windows)]
        native_handle,
        #[cfg(windows)]
        write_timeout_ms,
    );

    let wall_instant = start_instant.elapsed();
    let wall_systime = SystemTime::now()
        .duration_since(start_systime)
        .unwrap_or_default();

    // Best-effort quiesce so the next client sees a clean line.
    if let Err(e) = write_command(&mut port, Command::AverageStop) {
        eprintln!("warning: AverageStop on shutdown failed: {e}");
    }

    raw.flush().context("failed to flush capture_raw.bin")?;
    meta.flush()
        .context("failed to flush capture_meta.ndjson")?;

    let (stats, fatal) = outcome;
    print_summary(
        &cfg,
        &stats,
        wall_instant,
        wall_systime,
        &raw_path,
        &meta_path,
    );
    if let Some(e) = fatal {
        eprintln!("\ncapture ended early on a read error: {e}");
    }
    Ok(())
}

/// The raw read loop. Returns the accumulated statistics and, if the capture
/// ended on a fatal read error, that error's description.
///
/// Both files are written as we go, so a hard kill still leaves everything up
/// to the last flush usable.
fn capture_loop(
    port: &mut Box<dyn SerialPort>,
    cfg: &Cfg,
    running: &Arc<AtomicBool>,
    start_instant: Instant,
    raw: &mut BufWriter<File>,
    meta: &mut BufWriter<File>,
    #[cfg(windows)] native_handle: isize,
    #[cfg(windows)] write_timeout_ms: u32,
) -> (Stats, Option<String>) {
    let mut stats = Stats::new(cfg.read_len, cfg.packet_size);
    let mut window = Window::new(start_instant);
    let mut buf = vec![0u8; cfg.read_len];
    let mut probe_buf = vec![0u8; cfg.read_len];

    let limit = (cfg.seconds > 0).then(|| Duration::from_secs(cfg.seconds));
    let mut seq: u64 = 0;
    let mut raw_offset: u64 = 0;
    let mut prev_instant_us: Option<u128> = None;
    let mut fatal: Option<String> = None;
    let mut open_run: Option<RunAccum> = None;

    while running.load(Ordering::SeqCst) {
        if limit.is_some_and(|l| start_instant.elapsed() >= l) {
            break;
        }

        let read_res = port.read(&mut buf);
        // Stamp both clocks the INSTANT the read returns, before parsing,
        // writing, or anything else. Doing it later folds in this loop's own
        // scheduling and destroys the estimate this capture exists to make.
        let t_sys = SystemTime::now();
        let t_inst = Instant::now();

        let (filled, mut err) = match read_res {
            Ok(n) => (n.min(cfg.read_len), None),
            Err(e) => {
                let msg = format!("{:?}: {e}", e.kind());
                if !is_empty_error(&e) {
                    fatal = Some(msg.clone());
                }
                (0usize, Some(msg))
            }
        };

        // The probe only makes sense after a read that delivered bytes: it asks
        // whether the port still had more, which is exactly the question a
        // zero-byte read has already answered.
        let probe = if cfg.probe_enabled && filled > 0 && err.is_none() {
            #[cfg(windows)]
            let p = do_probe(port, &mut probe_buf, native_handle, write_timeout_ms);
            #[cfg(not(windows))]
            let p = do_probe(port, &mut probe_buf);
            Some(p)
        } else {
            None
        };
        let probe_filled = probe.as_ref().and_then(|p| p.filled);
        if let Some(msg) = probe.as_ref().and_then(|p| p.err.clone()) {
            err = Some(match err {
                Some(prev) => format!("{prev}; {msg}"),
                None => msg,
            });
        }

        let depth = packet_depth(filled, cfg.packet_size);
        let trustable = is_trustable(filled, cfg.packet_size, err.is_some());

        let systime_us = to_epoch_us(t_sys);
        let instant_us = t_inst.saturating_duration_since(start_instant).as_micros();

        if let Err(e) = raw.write_all(&buf[..filled]) {
            fatal = Some(format!("failed to write capture_raw.bin: {e}"));
            break;
        }
        let main_offset = raw_offset;
        raw_offset += filled as u64;

        let read_seq = seq;
        let record = ReadRecord {
            kind: "read",
            seq: read_seq,
            req_len: cfg.read_len,
            filled,
            depth,
            raw_offset: main_offset,
            systime_us,
            instant_us,
            probe_filled,
            trustable,
            err: err.as_deref(),
        };
        if let Err(e) = writeln!(meta, "{}", record_line(&record)) {
            fatal = Some(format!("failed to write capture_meta.ndjson: {e}"));
            break;
        }
        seq += 1;

        // A probe that returned bytes consumed real stream data. Preserve it,
        // and describe it with its own record so every byte in the raw file is
        // still attributable and consecutive records remain gapless.
        if let (Some(p), Some(n)) = (probe.as_ref(), probe_filled) {
            if n > 0 {
                let n = n.min(probe_buf.len());
                if let Err(e) = raw.write_all(&probe_buf[..n]) {
                    fatal = Some(format!("failed to write capture_raw.bin: {e}"));
                    break;
                }
                let probe_record = ReadRecord {
                    kind: "probe",
                    seq,
                    req_len: probe_buf.len(),
                    filled: n,
                    depth: packet_depth(n, cfg.packet_size),
                    raw_offset,
                    systime_us: to_epoch_us(p.systime),
                    instant_us: p
                        .instant
                        .saturating_duration_since(start_instant)
                        .as_micros(),
                    probe_filled: None,
                    // These bytes were taken by a probe, not by a read whose
                    // drainage we tested, so they carry no trust claim.
                    trustable: false,
                    err: None,
                };
                if let Err(e) = writeln!(meta, "{}", record_line(&probe_record)) {
                    fatal = Some(format!("failed to write capture_meta.ndjson: {e}"));
                    break;
                }
                seq += 1;
                raw_offset += n as u64;
                stats.probe_reads += 1;
                stats.probe_bytes += n as u64;
                stats.total_bytes += n as u64;
                window.bytes += n as u64;
            }
        }

        // UNTRUSTED RUN tracking. A run is the consecutive stretch of reads that
        // each swept up more than one packet; it closes on the first read that
        // did not. The samples inside it are the backlog a real implementation
        // would have had to hold, so the largest run is the ring size we need.
        if filled > cfg.packet_size {
            open_run
                .get_or_insert_with(|| RunAccum::new(read_seq, instant_us))
                .push(read_seq, instant_us, filled, depth);
            stats.untrusted_reads += 1;
        } else if let Some(run) = open_run.take() {
            let closed = run.close(true);
            // Account for the run BEFORE writing it out, so a failing ndjson
            // write costs us the record but never the summary statistic — the
            // ring-size result must survive a full output device.
            stats.record_run(closed);
            window.runs += 1;
            if let Err(e) = writeln!(meta, "{}", run_line(&closed)) {
                fatal = Some(format!("failed to write capture_meta.ndjson: {e}"));
                break;
            }
        }

        stats.total_reads += 1;
        stats.total_bytes += filled as u64;
        stats.hist[filled] += 1;
        stats.max_filled = stats.max_filled.max(filled);
        let depth_slot = depth.min(stats.depth_hist.len() - 1);
        stats.depth_hist[depth_slot] += 1;
        window.reads += 1;
        window.bytes += filled as u64;
        window.max_depth = window.max_depth.max(depth);
        if err.is_some() {
            stats.err_reads += 1;
        }
        if filled == 0 {
            stats.zero_reads += 1;
        } else if filled == cfg.read_len {
            stats.full_reads += 1;
        } else {
            stats.short_reads += 1;
        }
        if trustable {
            stats.trustable_reads += 1;
            window.trustable += 1;
        }

        if let Some(prev) = prev_instant_us {
            let gap_us = instant_us.saturating_sub(prev);
            window.max_gap_us = window.max_gap_us.max(gap_us);
            stats.record_gap(GapRec {
                gap_us,
                seq: record.seq,
                systime_us,
                instant_us,
            });
        }
        prev_instant_us = Some(instant_us);

        if window.start.elapsed() >= Duration::from_secs(1) {
            print_progress(&window, &stats, start_instant);
            // Flush on the same cadence so a hard kill loses at most a second.
            let _ = raw.flush();
            let _ = meta.flush();
            window = Window::new(Instant::now());
        }

        if fatal.is_some() {
            break;
        }
    }

    // The capture ended mid-run. Emit it anyway — the samples are real — but
    // flagged `terminated: false`, because no trustable read ever closed it and
    // its totals are therefore a lower bound, not a measurement.
    if let Some(run) = open_run.take() {
        let closed = run.close(false);
        if let Err(e) = writeln!(meta, "{}", run_line(&closed)) {
            fatal.get_or_insert(format!("failed to write capture_meta.ndjson: {e}"));
        }
        stats.record_run(closed);
    }

    (stats, fatal)
}

/// Put the device into a known, non-streaming state and read its metadata.
///
/// This replicates `Ppk2::new`: clear buffers, assert DTR, stop any averaging a
/// crashed client left running, drain the stale bytes that would otherwise
/// corrupt the metadata response, read metadata, then set the measurement mode.
///
/// Metadata is best-effort — it is only used to annotate the capture header —
/// so a failure is reported and the capture continues.
fn setup_device(port: &mut Box<dyn SerialPort>) -> Result<Option<Metadata>> {
    if let Err(e) = port.clear(serialport::ClearBuffer::All) {
        eprintln!("warning: failed to clear buffers: {e}");
    }
    // Required to work on Windows.
    if let Err(e) = port.write_data_terminal_ready(true) {
        eprintln!("warning: failed to set DTR: {e}");
    }

    write_command(port, Command::AverageStop).context("failed to send AverageStop")?;
    thread::sleep(Duration::from_millis(50));
    let drained = drain(port);
    if drained > 0 {
        eprintln!("drained {drained} stale bytes during recovery");
    }

    let metadata = match read_metadata(port) {
        Ok(m) => Some(m),
        Err(e) => {
            eprintln!("warning: could not read device metadata: {e:#}");
            None
        }
    };

    // The crate's `Ppk2::new` takes the mode as an argument; this example fixes
    // it at `Source`, matching the crate's `Default` and the `cli` example.
    // Nothing here depends on the mode — it affects sample VALUES, not the byte
    // cadence this tool measures.
    write_command(port, Command::SetPowerMode(MeasurementMode::Source))
        .context("failed to send SetPowerMode")?;
    Ok(metadata)
}

/// Read and discard whatever the device is still emitting, so a metadata
/// response can be parsed out of a clean line. Returns the byte count.
fn drain(port: &mut Box<dyn SerialPort>) -> usize {
    let original = port.timeout();
    if port.set_timeout(Duration::from_millis(20)).is_err() {
        return 0;
    }
    let mut drained = 0usize;
    let mut scratch = [0u8; 256];
    let deadline = Instant::now() + Duration::from_millis(500);
    while Instant::now() < deadline {
        match port.read(&mut scratch) {
            Ok(0) => break,
            Ok(n) => drained += n,
            Err(_) => break,
        }
    }
    if let Err(e) = port.set_timeout(original) {
        eprintln!("warning: failed to restore the port timeout after draining: {e}");
    }
    drained
}

/// Fetch device metadata, retrying as `Ppk2::get_metadata` does — the command
/// intermittently fails on a device that has just been quiesced.
fn read_metadata(port: &mut Box<dyn SerialPort>) -> Result<Metadata> {
    let mut last: Option<anyhow::Error> = None;
    for _ in 0..3 {
        match try_read_metadata(port) {
            Ok(m) => return Ok(m),
            Err(e) => last = Some(e),
        }
    }
    Err(last.unwrap_or_else(|| anyhow!("metadata unavailable")))
}

fn try_read_metadata(port: &mut Box<dyn SerialPort>) -> Result<Metadata> {
    write_command(port, Command::GetMetaData)?;
    let mut response = Vec::with_capacity(Command::GetMetaData.expected_response_len());
    let mut buf = [0u8; 256];
    let deadline = Instant::now() + Duration::from_secs(2);
    while !response.ends_with(b"END\n") {
        if Instant::now() >= deadline {
            return Err(anyhow!(
                "timed out after {} bytes of metadata response",
                response.len()
            ));
        }
        match port.read(&mut buf) {
            Ok(n) => response.extend_from_slice(&buf[..n]),
            Err(e) if is_empty_error(&e) => {}
            Err(e) => return Err(e).context("metadata read failed"),
        }
    }
    Metadata::from_bytes(&response).map_err(|e| anyhow!("failed to parse metadata: {e}"))
}

/// Write a command's raw bytes. None of the commands used here except
/// `GetMetaData` expects a response, so nothing is read back.
fn write_command(port: &mut Box<dyn SerialPort>, command: Command) -> Result<()> {
    let bytes: Vec<u8> = command.bytes().collect();
    port.write_all(&bytes)?;
    port.flush()?;
    Ok(())
}

/// The follow-up probe: read whatever is buffered RIGHT NOW, without waiting.
///
/// The port is temporarily reprogrammed to return immediately and restored
/// afterwards, so the probe does not become another coalescing window (which at
/// 100 ksps would always come back full and answer nothing).
#[cfg(windows)]
fn do_probe(
    port: &mut Box<dyn SerialPort>,
    buf: &mut [u8],
    native_handle: isize,
    write_timeout_ms: u32,
) -> ProbeOutcome {
    if let Err(e) = win_timeouts::apply_immediate(native_handle, write_timeout_ms) {
        return ProbeOutcome::failed(format!("probe SetCommTimeouts(immediate) failed: {e}"));
    }
    let res = port.read(buf);
    let t_sys = SystemTime::now();
    let t_inst = Instant::now();
    let mut outcome = ProbeOutcome::from_read(res, t_sys, t_inst);
    if let Err(e) =
        win_timeouts::apply_coalescing(native_handle, WIN_READ_COALESCE_MS, write_timeout_ms)
    {
        let msg = format!("probe SetCommTimeouts(coalescing) restore failed: {e}");
        outcome.err = Some(match outcome.err {
            Some(prev) => format!("{prev}; {msg}"),
            None => msg,
        });
    }
    outcome
}

/// See the Windows variant. On unix the same effect is had by shortening the
/// port timeout, since the backend polls with it before reading.
#[cfg(not(windows))]
fn do_probe(port: &mut Box<dyn SerialPort>, buf: &mut [u8]) -> ProbeOutcome {
    let original = port.timeout();
    if let Err(e) = port.set_timeout(PROBE_TIMEOUT) {
        return ProbeOutcome::failed(format!("probe set_timeout failed: {e}"));
    }
    let res = port.read(buf);
    let t_sys = SystemTime::now();
    let t_inst = Instant::now();
    let mut outcome = ProbeOutcome::from_read(res, t_sys, t_inst);
    if let Err(e) = port.set_timeout(original) {
        let msg = format!("probe set_timeout restore failed: {e}");
        outcome.err = Some(match outcome.err {
            Some(prev) => format!("{prev}; {msg}"),
            None => msg,
        });
    }
    outcome
}

/// How many USB packets a read swept up: `ceil(filled / packet_size)`, and 0
/// for an empty read.
///
/// Note this is a ceiling, so a read that is not an exact packet multiple
/// rounds up — a 1020-byte read at a 64-byte packet size reports depth 16 even
/// though 16 packets would be 1024 bytes. Depth is a backlog magnitude, not a
/// literal packet count.
fn packet_depth(filled: usize, packet_size: usize) -> usize {
    if filled == 0 {
        0
    } else {
        filled.div_ceil(packet_size)
    }
}

/// The trustability rule, version 2 (see the module docs).
///
/// A read is trustable when it returned at most one USB packet: either a full
/// packet with nothing queued behind it, or a short terminating packet. More
/// than a packet means the read swept up a backlog, so its arrival stamp says
/// nothing useful about when those samples were captured.
///
/// The `filled < req_len` "the port drained" rule this replaces was disproved
/// by the macOS baseline: the driver caps reads well below `req_len`, so that
/// test was true of every single read and carried no information.
///
/// `probe_filled` is deliberately NOT an input any more — the rule is
/// unconditional — but the probe result is still recorded so an analysis pass
/// can cross-check it independently.
fn is_trustable(filled: usize, packet_size: usize, errored: bool) -> bool {
    // An errored read delivered nothing, so it makes no claim either way.
    !errored && filled <= packet_size
}

fn is_empty_error(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
    )
}

fn to_epoch_us(t: SystemTime) -> u128 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros())
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// JSON emission. There is no serde in this crate's dependency tree and none may
// be added, so lines are built by hand.
// ---------------------------------------------------------------------------

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 8);
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 || c as u32 == 0x7f => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out
}

fn opt_usize(v: Option<usize>) -> String {
    v.map_or_else(|| "null".to_string(), |n| n.to_string())
}

fn opt_str(v: Option<&str>) -> String {
    v.map_or_else(|| "null".to_string(), |s| format!("\"{}\"", json_escape(s)))
}

fn record_line(r: &ReadRecord) -> String {
    format!(
        "{{\"record\":\"{}\",\"seq\":{},\"req_len\":{},\"filled\":{},\"depth\":{},\
         \"raw_offset\":{},\"systime_us\":{},\"instant_us\":{},\"probe_filled\":{},\
         \"trustable\":{},\"err\":{}}}",
        json_escape(r.kind),
        r.seq,
        r.req_len,
        r.filled,
        r.depth,
        r.raw_offset,
        r.systime_us,
        r.instant_us,
        opt_usize(r.probe_filled),
        r.trustable,
        opt_str(r.err),
    )
}

/// An `untrusted_run` record. It carries no raw bytes, so it takes no
/// `raw_offset` and is not part of the raw-file offset chain; it joins to the
/// read records by `start_seq`..=`end_seq`.
fn run_line(r: &UntrustedRun) -> String {
    format!(
        "{{\"record\":\"untrusted_run\",\"start_seq\":{},\"end_seq\":{},\
         \"start_instant_us\":{},\"duration_us\":{},\"reads\":{},\"samples\":{},\
         \"bytes\":{},\"peak_depth\":{},\"terminated\":{}}}",
        r.start_seq,
        r.end_seq,
        r.start_instant_us,
        r.duration_us,
        r.reads,
        r.samples,
        r.bytes,
        r.peak_depth,
        r.terminated,
    )
}

fn header_line(cfg: &Cfg, start_systime: SystemTime, metadata: Option<&Metadata>) -> String {
    let metadata_json = metadata.map_or_else(
        || "null".to_string(),
        |m| {
            format!(
                "{{\"calibrated\":{},\"vdd\":{},\"hw\":{},\"mode\":\"{:?}\",\"ia\":{}}}",
                m.calibrated, m.vdd, m.hw, m.mode, m.ia
            )
        },
    );
    format!(
        "{{\"record\":\"header\",\"format_version\":{version},\"port\":\"{port}\",\
         \"os\":\"{os}\",\"arch\":\"{arch}\",\
         \"start_systime_us\":{start_sys},\"start_instant_us\":0,\
         \"requested_seconds\":{seconds},\"req_len\":{req_len},\"packet_size\":{packet_size},\
         \"usb_read_buf_bytes\":{usb_buf},\"read_accum_target_bytes\":{accum},\
         \"native_read_coalescing\":{coalescing},\"probe_enabled\":{probe},\
         \"probe_is_diagnostic_only\":true,\"sample_bytes\":{sample_bytes},\"sps_max\":{sps_max},\
         \"measurement_mode\":\"Source\",\"out_dir\":\"{out}\",\"metadata\":{metadata_json}}}",
        version = FORMAT_VERSION,
        packet_size = cfg.packet_size,
        port = json_escape(&cfg.port_path),
        os = json_escape(std::env::consts::OS),
        arch = json_escape(std::env::consts::ARCH),
        start_sys = to_epoch_us(start_systime),
        seconds = cfg.seconds,
        req_len = cfg.read_len,
        usb_buf = USB_READ_BUF_BYTES,
        accum = READ_ACCUM_TARGET_BYTES,
        coalescing = cfg.native_coalescing,
        probe = cfg.probe_enabled,
        sample_bytes = SAMPLE_BYTES,
        sps_max = SPS_MAX,
        out = json_escape(&cfg.out_dir.display().to_string()),
    )
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

fn pct(n: u64, d: u64) -> f64 {
    if d == 0 {
        0.0
    } else {
        n as f64 * 100.0 / d as f64
    }
}

/// Drift in ppm for one interval: how much wall time had no samples behind it.
///
/// Positive means samples are missing. A steady ~31 ppm is the crystal floor
/// (the host and the PPK2 disagree by that much); a jump well above it means
/// samples are being lost right now.
fn drift_ppm(bytes: u64, elapsed_s: f64) -> f64 {
    if elapsed_s <= 0.0 {
        return 0.0;
    }
    let implied_s = (bytes / SAMPLE_BYTES as u64) as f64 / SPS_MAX as f64;
    (elapsed_s - implied_s) / elapsed_s * 1e6
}

/// The live loss detector. This is printed once per second so a stimulus (bus
/// contention, CPU load) can be applied and its effect watched AS IT HAPPENS,
/// rather than being discovered in the end-of-run summary.
fn print_progress(window: &Window, stats: &Stats, start_instant: Instant) {
    let dt = window.start.elapsed().as_secs_f64().max(1e-9);
    eprintln!(
        "[{elapsed:>7.1}s] reads/s {rps:>8.1}  bytes/s {bps:>10.0}  trustable {trust:>6.2}%  \
         runs {runs:>4}  max depth {depth:>4}  drift {ppm:>+9.1} ppm  \
         max gap {gap:>8.2} ms  total reads {total}",
        elapsed = start_instant.elapsed().as_secs_f64(),
        rps = window.reads as f64 / dt,
        bps = window.bytes as f64 / dt,
        trust = pct(window.trustable, window.reads),
        runs = window.runs,
        depth = window.max_depth,
        ppm = drift_ppm(window.bytes, dt),
        gap = window.max_gap_us as f64 / 1000.0,
        total = stats.total_reads,
    );
}

fn print_summary(
    cfg: &Cfg,
    stats: &Stats,
    wall_instant: Duration,
    wall_systime: Duration,
    raw_path: &Path,
    meta_path: &Path,
) {
    let samples = stats.total_bytes / SAMPLE_BYTES as u64;
    let partial = stats.total_bytes % SAMPLE_BYTES as u64;
    let implied = samples as f64 / SPS_MAX as f64;
    let actual = wall_instant.as_secs_f64();
    let drift = actual - implied;
    let ppm = if actual > 0.0 {
        drift / actual * 1e6
    } else {
        0.0
    };
    let clock_skew_ms = (wall_systime.as_secs_f64() - actual) * 1000.0;

    println!("\n================ transport_probe summary ================");
    println!("port                 : {}", cfg.port_path);
    println!(
        "platform             : {} / {}",
        std::env::consts::OS,
        std::env::consts::ARCH
    );
    println!("req_len              : {} B", cfg.read_len);
    println!("packet_size          : {} B", cfg.packet_size);
    println!(
        "probe                : {}",
        if cfg.probe_enabled {
            "enabled"
        } else {
            "disabled"
        }
    );
    println!("native coalescing    : {}", cfg.native_coalescing);
    println!("raw                  : {}", raw_path.display());
    println!("meta                 : {}", meta_path.display());

    println!("\n---- volume ----");
    println!("main reads           : {}", stats.total_reads);
    println!(
        "  zero-byte          : {} ({:.2}%)",
        stats.zero_reads,
        pct(stats.zero_reads, stats.total_reads)
    );
    println!(
        "  short (< req_len)  : {} ({:.2}%)",
        stats.short_reads,
        pct(stats.short_reads, stats.total_reads)
    );
    println!(
        "  full (== req_len)  : {} ({:.2}%)",
        stats.full_reads,
        pct(stats.full_reads, stats.total_reads)
    );
    println!(
        "  errored            : {} ({:.2}%)",
        stats.err_reads,
        pct(stats.err_reads, stats.total_reads)
    );
    println!(
        "  TRUSTABLE (<= {:>4} B) : {} ({:.2}%)",
        cfg.packet_size,
        stats.trustable_reads,
        pct(stats.trustable_reads, stats.total_reads)
    );
    println!(
        "  untrusted (> packet) : {} ({:.2}%)",
        stats.untrusted_reads,
        pct(stats.untrusted_reads, stats.total_reads)
    );
    println!(
        "largest single read  : {} B (of {} requested){}",
        stats.max_filled,
        cfg.read_len,
        if stats.max_filled < cfg.read_len {
            " — the driver caps reads below the request, so `filled == req_len` never fires"
        } else {
            ""
        }
    );
    println!(
        "probe reads w/ bytes : {} ({} B) — these prove the preceding read did NOT drain",
        stats.probe_reads, stats.probe_bytes
    );
    println!("total bytes          : {}", stats.total_bytes);
    if partial != 0 {
        println!("  (trailing partial sample: {partial} B)");
    }

    println!("\n---- DRIFT (the number this capture exists to produce) ----");
    println!("implied samples      : {samples} ({SAMPLE_BYTES} B each)");
    println!("implied duration     : {implied:.6} s  @ {SPS_MAX} sps");
    println!("actual wall duration : {actual:.6} s  (monotonic Instant)");
    println!(
        "  >>> DRIFT          : {drift:+.6} s  ({ppm:+.1} ppm)  \
         [positive = wall time with NO samples behind it]"
    );
    println!(
        "SystemTime duration  : {:.6} s  (skew vs Instant: {clock_skew_ms:+.3} ms — \
         a large value means the host suspended or the wall clock stepped)",
        wall_systime.as_secs_f64()
    );

    println!("\n---- `filled` distribution (main reads only) ----");
    println!(
        "exact 0              : {:>10} ({:.2}%)",
        stats.hist.first().copied().unwrap_or(0),
        pct(stats.hist.first().copied().unwrap_or(0), stats.total_reads)
    );
    let full_count = stats.hist.get(cfg.read_len).copied().unwrap_or(0);
    println!(
        "exact req_len ({:>5}) : {:>10} ({:.2}%)",
        cfg.read_len,
        full_count,
        pct(full_count, stats.total_reads)
    );
    println!("buckets:");
    for (i, &lo) in BUCKET_EDGES.iter().enumerate() {
        if lo > cfg.read_len {
            break;
        }
        let hi = BUCKET_EDGES
            .get(i + 1)
            .map_or(usize::MAX, |&next| next - 1)
            .min(cfg.read_len);
        let count: u64 = stats.hist[lo..=hi].iter().sum();
        println!(
            "  {lo:>6} ..= {hi:<6} : {count:>10} ({:.2}%)",
            pct(count, stats.total_reads)
        );
    }

    let mut distinct: Vec<(usize, u64)> = stats
        .hist
        .iter()
        .enumerate()
        .filter(|&(_, &c)| c > 0)
        .map(|(n, &c)| (n, c))
        .collect();
    distinct.sort_unstable_by_key(|&(_, count)| Reverse(count));
    println!("most common exact sizes:");
    for (n, c) in distinct.iter().take(10) {
        println!(
            "  {n:>6} B          : {c:>10} ({:.2}%)",
            pct(*c, stats.total_reads)
        );
    }

    print_depth_histogram(cfg, stats);
    print_run_section(stats);

    println!("\n---- largest inter-read wall gaps ----");
    if stats.top_gaps.is_empty() {
        println!("  (none recorded)");
    } else {
        println!("       gap (ms)        seq     instant_us          systime_us");
        for g in &stats.top_gaps {
            println!(
                "  {:>13.3} {:>10} {:>14} {:>19}",
                g.gap_us as f64 / 1000.0,
                g.seq,
                g.instant_us,
                g.systime_us
            );
        }
    }
    println!("=========================================================");
}

/// Packet-depth distribution: how many USB packets each read swept up.
fn print_depth_histogram(cfg: &Cfg, stats: &Stats) {
    println!(
        "\n---- packet depth (req_len {} B / packet {} B) ----",
        cfg.read_len, cfg.packet_size
    );
    let empty = stats.depth_hist.first().copied().unwrap_or(0);
    println!(
        "  0 packets (empty)  : {empty:>10} ({:.2}%)",
        pct(empty, stats.total_reads)
    );
    for (i, &lo) in DEPTH_BUCKETS.iter().enumerate() {
        let hi = DEPTH_BUCKETS
            .get(i + 1)
            .map_or(usize::MAX, |&next| next - 1);
        if lo >= stats.depth_hist.len() {
            break;
        }
        let hi_idx = hi.min(stats.depth_hist.len() - 1);
        let count: u64 = stats.depth_hist[lo..=hi_idx].iter().sum();
        let label = if hi == usize::MAX || hi_idx < hi {
            format!(">{}", lo - 1)
        } else if lo == hi {
            format!("{lo}")
        } else {
            format!("{lo}-{hi}")
        };
        println!(
            "  {label:>9} packets  : {count:>10} ({:.2}%)",
            pct(count, stats.total_reads)
        );
    }
}

/// Untrusted runs — the ring-buffer sizing result.
fn print_run_section(stats: &Stats) {
    println!("\n---- UNTRUSTED RUNS (ring-buffer sizing) ----");
    println!("total runs           : {}", stats.total_runs);

    match stats.max_run {
        None => {
            println!("  >>> no untrusted run occurred: every read stayed within one packet.");
            println!("      No backlog buffering was required during this capture.");
        }
        Some(m) => {
            println!(
                "  >>> REQUIRED RING SIZE : {} samples ({} B, {:.3} ms of stream)",
                m.samples,
                m.bytes,
                m.samples as f64 * 1000.0 / SPS_MAX as f64
            );
            println!(
                "      largest run        : seq {}..={}, {} reads, peak depth {}, \
                 {:.3} ms wall, starting at instant_us {}{}",
                m.start_seq,
                m.end_seq,
                m.reads,
                m.peak_depth,
                m.duration_us as f64 / 1000.0,
                m.start_instant_us,
                if m.terminated {
                    ""
                } else {
                    " [TRUNCATED by end of capture — a lower bound]"
                }
            );
            println!(
                "      This is measured, not chosen: it is exactly the backlog the largest\n\
                 \x20     observed stall forced into flight before a trustable read closed it."
            );
        }
    }

    if stats.total_runs > 0 {
        println!("run size histogram (samples per run):");
        for (i, &lo) in RUN_SAMPLE_EDGES.iter().enumerate() {
            let count = stats.run_hist[i];
            let label = match RUN_SAMPLE_EDGES.get(i + 1) {
                Some(&next) => format!("{lo}-{}", next - 1),
                None => format!(">={lo}"),
            };
            println!(
                "  {label:>14} : {count:>10} ({:.2}%)",
                pct(count, stats.total_runs)
            );
        }

        println!("largest runs:");
        println!("        samples      bytes  reads  peak_depth   dur (ms)   start_seq    end_seq");
        for r in &stats.top_runs {
            println!(
                "  {:>13} {:>10} {:>6} {:>11} {:>10.3} {:>11} {:>10}{}",
                r.samples,
                r.bytes,
                r.reads,
                r.peak_depth,
                r.duration_us as f64 / 1000.0,
                r.start_seq,
                r.end_seq,
                if r.terminated { "" } else { "  [truncated]" }
            );
        }
    }
}
