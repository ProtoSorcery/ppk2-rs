#![doc = include_str!("../README.md")]
#![deny(missing_docs)]

use measurement::MeasurementMatch;
use serialport::{ClearBuffer::Input, FlowControl, SerialPort};
use std::io::{Read, Write};
use std::str::Utf8Error;
use std::sync::mpsc::{self, Receiver, SendError, TryRecvError};
use std::{
    borrow::Cow,
    io,
    sync::{Arc, Condvar, Mutex},
    thread,
    time::{Duration, Instant},
};
use stream::{probe_indicates_drained, ReadClocks, SilenceGuard, StreamAssembler};
use thiserror::Error;
use types::{DevicePower, LogicPortPins, MeasurementMode, Metadata, SourceVoltage};

use crate::cmd::Command;

pub mod cmd;
pub mod measurement;
pub mod stream;
pub mod types;

/// The device's raw hardware sample rate, in samples/sec.
///
/// The PPK2 always streams at this rate; requesting a lower `sps` decimates by
/// AVERAGING `chunk = SPS_MAX / sps` raw samples into one emitted
/// [`measurement::MeasurementMatch`].
///
/// This is also the unit in which [`measurement::MeasurementMatch`]'s `missed`
/// counts are expressed: `missed` counts RAW samples at `SPS_MAX`, not
/// decimated output periods. Consumers should compute
/// `chunk = SPS_MAX / sps` from this constant rather than hardcoding 100_000.
pub const SPS_MAX: usize = 100_000;

/// Size of the reusable USB read buffer for the measurement worker, in bytes.
///
/// The PPK2 emits one 4-byte sample per measurement and streams ~100,000
/// samples/sec (~400 KB/s) at the highest rate. Previously the worker read with
/// a 4-byte buffer, forcing roughly one trapping `read()` syscall per sample
/// (~1M syscalls/sec). The parser (`MeasurementAccumulator::feed_into`) already
/// tolerates arbitrary byte counts via an internal partial-sample remainder, so
/// we batch the read to cut the syscall count.
///
/// We use a MODEST 4 KB buffer (≈ one USB transfer). A Phase-0 parser bench
/// showed parse CPU plateaus by ~256 B, so 4 KB already captures essentially
/// all of the syscall-reduction benefit. Crucially, read size no longer affects
/// the decimated output: the streaming drain in `start_measurement_matching`
/// emits in FIXED `SPS_MAX/sps`-sample chunks (see the `while measurement_buf...`
/// loop), so averaging is independent of how many bytes each `read()` returns.
/// A huge buffer (the prior 64 KB) is avoided pending live read-latency
/// verification; 4 KB keeps latency bounded while still collapsing syscalls.
const USB_READ_BUF_BYTES: usize = 4 * 1024;

/// Bytes requested per read while Windows native read coalescing is active.
///
/// At the PPK2's ~400 KB/s this makes the BYTE count the binding constraint, so
/// the cadence is ~5 ms of stream per read regardless of the system timer
/// granularity that governs the coalescing window (a 5 ms `COMMTIMEOUTS`
/// constant can round up to a ~15 ms tick, which against the full 4 KB buffer
/// would let a read cover ~10 ms).
///
/// Measured: 96.25% of reads return exactly this, at ~203 reads/s. Requesting
/// 4096 instead halves the read rate and doubles the largest inter-read gap
/// (5.8 → 10.4 ms) for no benefit. This value is well tuned; leave it.
#[cfg(windows)]
const WIN_READ_LEN_BYTES: usize = 2 * 1024;

/// Read timeout used for the follow-up drain probe on unix.
///
/// # This MUST be zero
///
/// The probe asks "was anything still buffered at the instant the read
/// returned". Any non-zero timeout turns it into "did anything arrive within the
/// next N", which is a different — and at this data rate, useless — question:
/// the PPK2 streams ~400 bytes per millisecond, so a 1 ms probe would find bytes
/// essentially every time and report that no read ever drains the port. That
/// would be a false negative manufactured by the instrument, and it would push
/// every sample onto the backward-pinning path.
///
/// `serialport`'s unix backend `poll`s with the port timeout before reading, so
/// zero makes the poll return immediately with only whether data is ready right
/// now. It is per-object on unix, so shortening it here cannot disturb a
/// concurrent [`Ppk2Commander`] write the way the Windows device-wide timeouts
/// would.
#[cfg(not(windows))]
const PROBE_TIMEOUT: Duration = Duration::ZERO;

/// Windows only: how long `ReadFile` may block while the serial driver
/// coalesces bytes into a single read, in milliseconds.
///
/// See [`win_read_coalescing`] for what this programs and why. At the PPK2's
/// ~400 KB/s this yields ≈2 KB per read — the same batch size the userspace
/// accumulation targets, but produced by the driver, so it costs no extra
/// syscalls and no timeout mutation.
#[cfg(windows)]
const WIN_READ_COALESCE_MS: u32 = 5;

/// Windows-native read coalescing for the measurement port.
///
/// # The problem
///
/// `serialport` programs every Windows port with `ReadIntervalTimeout =
/// MAXDWORD`, `ReadTotalTimeoutMultiplier = MAXDWORD` and
/// `ReadTotalTimeoutConstant = <timeout>` (`serialport::windows::COMPort::
/// set_timeout`). That combination is documented to mean *return immediately
/// with whatever is already buffered, however little*. It is the right default
/// for a request/response protocol and completely wrong for a 100 kHz sample
/// stream: `ReadFile` hands back a handful of bytes tens of thousands of times
/// a second.
///
/// # The fix
///
/// For the duration of a measurement we reprogram the port with
/// `ReadIntervalTimeout = 0` (no inter-byte timeout),
/// `ReadTotalTimeoutMultiplier = 0` and `ReadTotalTimeoutConstant =
/// WIN_READ_COALESCE_MS`, so a read completes when the caller's buffer is
/// full **or** the coalescing window expires — the driver does the batching.
///
/// The write fields are deliberately kept at the port's configured timeout
/// rather than the coalescing window. `SerialPort::set_timeout` cannot express
/// that (it writes one value into both), which matters because
/// [`Ppk2Commander`](crate::Ppk2Commander) writes through a duplicate of this
/// same handle while the stream runs, and `set_device_power(Disabled)` must not
/// fail spuriously.
///
/// # Scope: this affects every handle to the port
///
/// `COMPort::try_clone` duplicates the handle with `DuplicateHandle`, so all
/// clones — the measurement worker's reader and the
/// [`Ppk2Commander`](crate::Ppk2Commander)'s writer —
/// reference the SAME kernel file object. Timeouts live on that object (Win32
/// has no per-handle device state; `SetCommTimeouts` is a wrapper over
/// `IOCTL_SERIAL_SET_TIMEOUTS`, which the serial driver stores per device), so
/// programming them through any one handle governs them all. Three consequences,
/// all load-bearing:
///
/// 1. It is enough to apply this once, before the worker starts, through ANY
///    handle to the device — including the worker's own clone, which is the one
///    used here precisely so the raw handle the worker later needs for the drain
///    probe is owned by the worker's port and cannot outlive it.
/// 2. Any later `set_timeout` call on ANY clone **clobbers** it, restoring
///    serialport's return-immediately idiom. That is why the drain probe
///    programs [`win_read_coalescing::apply_immediate`] directly rather than
///    calling `set_timeout(Duration::ZERO)`, and why a single `set_timeout` is
///    all that is needed to undo coalescing on stop.
/// 3. Every form written here keeps the WRITE timeout at the port's configured
///    value, so a concurrent `Ppk2Commander` write — which carries
///    `set_device_power(Disabled)` and must not fail spuriously — is never
///    affected by anything the read path does.
///
/// # No command response is ever read while this is active
///
/// A shortened read timeout would matter for command responses, but none are
/// read during a measurement: [`Ppk2Commander`](crate::Ppk2Commander) issues
/// only zero-response commands and never reads, and the sole `send_command` on
/// the stop path is `AverageStop`, also zero-response. `GetMetaData` — the one
/// command that reads — runs in [`Ppk2::new`](crate::Ppk2::new) before any
/// stream exists, and the stop path
/// restores the default timeouts before the handle is handed back, so a
/// `get_metadata()` on a recovered device reads with the port's normal timeout.
#[cfg(windows)]
mod win_read_coalescing {
    use super::{Error, Result};
    use std::io;
    use windows_sys::Win32::Devices::Communication::{SetCommTimeouts, COMMTIMEOUTS};
    use windows_sys::Win32::Foundation::HANDLE;

    /// Program `handle`'s device for read coalescing.
    ///
    /// `coalesce_ms` bounds how long a read may block; `write_timeout_ms` is
    /// passed through unchanged so concurrent writes keep their normal budget.
    pub(super) fn apply(handle: isize, coalesce_ms: u32, write_timeout_ms: u32) -> Result<()> {
        set(
            handle,
            &COMMTIMEOUTS {
                // No inter-byte timeout: do not cut a read short just because
                // the stream paused briefly mid-transfer.
                ReadIntervalTimeout: 0,
                // Total read timeout is a flat constant, independent of how many
                // bytes were requested, so the latency bound does not scale with
                // the buffer size.
                ReadTotalTimeoutMultiplier: 0,
                ReadTotalTimeoutConstant: coalesce_ms,
                // Writes keep the port's configured timeout. See the module docs.
                WriteTotalTimeoutMultiplier: 0,
                WriteTotalTimeoutConstant: write_timeout_ms,
            },
        )
    }

    /// Program `handle`'s device to return IMMEDIATELY with whatever is already
    /// buffered, even if that is nothing. This is the drain probe's timeout form.
    ///
    /// `ReadIntervalTimeout = MAXDWORD` with BOTH total-timeout fields zero is
    /// the exact combination the `COMMTIMEOUTS` documentation defines as "return
    /// immediately with the bytes that have already been received, even if no
    /// bytes have been received". Both zeros are load-bearing: a non-zero
    /// constant would let the read block for that long when the buffer is empty,
    /// and at ~400 bytes/ms the probe would then find bytes essentially every
    /// time and report that no read ever drains the port — a false negative
    /// manufactured by the instrument.
    ///
    /// Note this differs from what `serialport`'s `set_timeout` writes
    /// (`Multiplier = MAXDWORD`, `Constant = timeout`). The documented zero-wait
    /// form is required, not an approximation of it — which is why the probe
    /// cannot simply call `set_timeout(Duration::ZERO)` the way the unix path
    /// does.
    pub(super) fn apply_immediate(handle: isize, write_timeout_ms: u32) -> Result<()> {
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

    fn set(handle: isize, timeouts: &COMMTIMEOUTS) -> Result<()> {
        // `HANDLE` is a plain `isize` in windows-sys 0.52 (the version pinned
        // in Cargo.toml, and the one serialport itself uses), so the stored
        // handle is passed through without a cast.
        let handle: HANDLE = handle;

        // SAFETY: every call site passes the raw handle of a `COMPort` it owns
        // or that is owned by a value outliving the call — the main thread's
        // `Ppk2::port`, or the measurement worker's own cloned port, which the
        // worker holds for its entire life. `timeouts` is a fully initialized
        // `#[repr(C)]` `COMMTIMEOUTS` and is only read by the call.
        if unsafe { SetCommTimeouts(handle, timeouts) } == 0 {
            return Err(Error::Io(io::Error::last_os_error()));
        }
        Ok(())
    }
}

#[derive(Error, Debug)]
/// PPK2 communication or data parsing error.
#[allow(missing_docs)]
pub enum Error {
    #[error("Serial port error: {0}")]
    SerialPort(#[from] serialport::Error),
    #[error("PPK2 not found. Is the device connected and are permissions set correctly?")]
    Ppk2NotFound,
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Utf8 error {0}")]
    Utf8(#[from] Utf8Error),
    #[error("Parse error in \"{0}\"")]
    Parse(String),
    #[error("Error sending measurement: {0}")]
    SendMeasurement(#[from] SendError<MeasurementMatch>),
    #[error("Error sending stop signal: {0}")]
    SendStopSignal(#[from] SendError<()>),
    #[error("Worker thread signal error: {0}")]
    WorkerSignalError(#[from] TryRecvError),
    #[error("Error deserializeing a measurement: {0:?}")]
    DeserializeMeasurement(Vec<u8>),
    /// The measurement stream went silent and stayed silent.
    ///
    /// A single empty read is routine — see [`stream::SilenceGuard`] — so this
    /// is only produced after sustained silence, and it means the device stopped
    /// streaming or went away rather than that any one read failed.
    #[error(
        "PPK2 stream is dead: {reads} consecutive reads returned no data over {elapsed:?} \
         (port read timeout is {timeout:?}). The device stopped streaming or was \
         disconnected; the measurement worker gave up rather than spin on an empty port."
    )]
    StreamDead {
        /// Consecutive reads that returned no data.
        reads: u32,
        /// How long the port had been silent when the worker gave up.
        elapsed: Duration,
        /// The port's configured read timeout, for judging whether `reads` and
        /// `elapsed` are consistent with each other.
        timeout: Duration,
    },
}

#[allow(missing_docs)]
pub type Result<T> = std::result::Result<T, Error>;

/// The concrete port type a [`Ppk2`] holds open.
///
/// On Windows this is deliberately the NATIVE `COMPort` rather than a
/// `Box<dyn SerialPort>`: the measurement worker's drain probe has to call
/// `SetCommTimeouts`, which needs a raw handle, and the only way to give the
/// worker a handle whose lifetime it controls is `COMPort::try_clone_native`.
/// A `Box<dyn SerialPort>` cannot produce one.
///
/// Storing a raw handle from the MAIN thread's port instead (what this crate
/// used to do) would be a use-after-free waiting to happen: dropping the `stop`
/// closure without calling it drops the `Ppk2`, closing that handle while the
/// worker is still mid-read, and Windows recycles handle values.
#[cfg(windows)]
type OwnedPort = serialport::COMPort;
/// The concrete port type a [`Ppk2`] holds open. See the Windows variant for why
/// this is a type alias at all.
#[cfg(not(windows))]
type OwnedPort = Box<dyn SerialPort>;

/// PPK2 device representation.
///
/// # Issuing commands during a measurement
///
/// [`Ppk2::start_measurement_matching`] (and [`Ppk2::start_measurement`])
/// consume `self`, so the device handle is unreachable for as long as the
/// measurement runs; the only way to get it back is the `stop` closure, which
/// tears the stream down and discards the timebase. To change the source
/// voltage or device power *without* stopping, take an [`Ppk2Commander`] via
/// [`Ppk2::commander`] BEFORE calling `start_measurement_matching` and keep it
/// alongside the [`Receiver`]:
///
/// ```no_run
/// # use ppk2::{Ppk2, types::{MeasurementMode, SourceVoltage, DevicePower}};
/// # fn main() -> ppk2::Result<()> {
/// let ppk2 = Ppk2::new("/dev/ttyACM0", MeasurementMode::Source)?;
/// let mut commander = ppk2.commander()?;
/// let (rx, stop) = ppk2.start_measurement(1000)?;
///
/// // Measurement keeps streaming into `rx` throughout.
/// commander.set_device_power(DevicePower::Enabled)?;
/// commander.set_source_voltage(SourceVoltage::from_millivolts(3300))?;
/// # let _ = (rx, stop);
/// # Ok(())
/// # }
/// ```
pub struct Ppk2 {
    port: OwnedPort,
    metadata: Metadata,
    /// Set by the `stop` closure of [`Ppk2::start_measurement_matching`] when
    /// the measurement worker ended abnormally (returned an error or panicked).
    ///
    /// The device is still returned to the caller in that case — see
    /// [`Ppk2::take_last_worker_error`] — because "the stream broke" and "the
    /// handle is unusable" are independent facts, and the caller usually needs
    /// the handle precisely to make the hardware safe.
    last_worker_error: Option<Error>,
}

impl Ppk2 {
    /// Create a new instance and configure the given [MeasurementMode].
    pub fn new<'a>(path: impl Into<Cow<'a, str>>, mode: MeasurementMode) -> Result<Self> {
        let builder = serialport::new(path, 9600)
            .timeout(Duration::from_millis(500))
            .flow_control(FlowControl::Hardware);

        // On Windows keep the NATIVE port type rather than a trait object. See
        // [`OwnedPort`]: `SetCommTimeouts` needs a raw handle, and only the
        // native type can hand one out — or clone itself into another one.
        #[cfg(windows)]
        let mut port: OwnedPort = builder.open_native()?;
        #[cfg(not(windows))]
        let mut port: OwnedPort = builder.open()?;

        if let Err(e) = port.clear(serialport::ClearBuffer::All) {
            tracing::warn!("failed to clear buffers: {:?}", e);
        }

        // Required to work on Windows.
        if let Err(e) = port.write_data_terminal_ready(true) {
            tracing::warn!("failed to set DTR: {:?}", e);
        }

        let mut ppk2 = Self {
            port,
            metadata: Metadata::default(),
            last_worker_error: None,
        };

        // A prior client may have crashed mid-stream, leaving the device
        // emitting 4-byte measurement frames. GetMetaData's text response
        // (terminated by "END\n") cannot be parsed out of that stream, so
        // stop any in-progress averaging and drain stale bytes first.
        if let Err(e) = ppk2.send_command(Command::AverageStop) {
            tracing::debug!("AverageStop during recovery failed: {:?}", e);
        }
        thread::sleep(Duration::from_millis(50));

        let original_timeout = ppk2.port.timeout();
        if let Err(e) = ppk2.port.set_timeout(Duration::from_millis(20)) {
            tracing::debug!("failed to set drain timeout: {:?}", e);
        }
        let mut drained: usize = 0;
        let mut scratch = [0u8; 256];
        let drain_deadline = Instant::now() + Duration::from_millis(500);
        loop {
            if Instant::now() >= drain_deadline {
                break;
            }
            match ppk2.port.read(&mut scratch) {
                Ok(0) => break,
                Ok(n) => drained += n,
                Err(e)
                    if e.kind() == io::ErrorKind::TimedOut
                        || e.kind() == io::ErrorKind::WouldBlock =>
                {
                    break;
                }
                Err(e) => {
                    tracing::debug!("drain read error: {:?}", e);
                    break;
                }
            }
        }
        if let Err(e) = ppk2.port.set_timeout(original_timeout) {
            tracing::debug!("failed to restore port timeout: {:?}", e);
        }
        tracing::debug!("drained {} stale bytes during recovery", drained);

        ppk2.metadata = ppk2.get_metadata()?;
        ppk2.set_power_mode(mode)?;
        Ok(ppk2)
    }

    /// Send a raw command and return the result.
    pub fn send_command(&mut self, command: Command) -> Result<Vec<u8>> {
        self.port.write_all(&Vec::from_iter(command.bytes()))?;
        // Doesn't allocate if expected response length is 0
        let mut response = Vec::with_capacity(command.expected_response_len());
        let mut buf = [0u8; 128];
        while !command.response_complete(&response) {
            let n = self.port.read(&mut buf)?;
            response.extend_from_slice(&buf[..n]);
        }
        Ok(response)
    }

    fn try_get_metadata(&mut self) -> Result<Metadata> {
        let response = self.send_command(Command::GetMetaData)?;
        Metadata::from_bytes(&response)
    }

    /// Get the device metadata.
    pub fn get_metadata(&mut self) -> Result<Metadata> {
        let mut result: Result<Metadata> = Err(Error::Parse("Metadata".to_string()));

        // Retry a few times, as the metadata command sometimes fails
        for _ in 0..3 {
            match self.try_get_metadata() {
                Ok(metadata) => {
                    result = Ok(metadata);
                    break;
                }
                Err(e) => {
                    tracing::warn!("Error fetching metadata: {:?}. Retrying..", e);
                }
            }
        }

        result
    }

    /// Enable or disable the device power.
    pub fn set_device_power(&mut self, power: DevicePower) -> Result<()> {
        self.send_command(Command::DeviceRunningSet(power))?;
        Ok(())
    }

    /// Set the voltage of the device voltage source.
    pub fn set_source_voltage(&mut self, vdd: SourceVoltage) -> Result<()> {
        self.send_command(Command::RegulatorSet(vdd))?;
        Ok(())
    }

    /// Take the error that ended the last measurement stream, if it ended
    /// abnormally. Clears the stored error, so a second call returns `None`.
    ///
    /// The `stop` closure returned by [`Ppk2::start_measurement`] /
    /// [`Ppk2::start_measurement_matching`] hands the device back as `Ok` even
    /// when the measurement worker returned an error or panicked, because a
    /// broken stream does not imply a broken port — and losing the handle is
    /// far worse than a broken stream, since the handle is what the caller
    /// needs to disable device power and close the port cleanly. The worker's
    /// error is logged at `warn` and recorded here rather than discarded.
    ///
    /// A device carrying a recorded worker error **is usable**: commands,
    /// metadata reads and a fresh [`Ppk2::start_measurement`] all work as
    /// normal. The error says only that the *previous* measurement stream
    /// terminated abnormally — e.g. the [`Receiver`] was dropped before
    /// `stop` was called, or the port read failed — so any samples that
    /// stream would still have produced are missing.
    ///
    /// Returns `None` if the last stream ended cleanly, or if no measurement
    /// has been stopped on this handle.
    pub fn take_last_worker_error(&mut self) -> Option<Error> {
        self.last_worker_error.take()
    }

    /// Obtain a write-only [`Ppk2Commander`]: a command handle that stays
    /// usable while a measurement is running.
    ///
    /// The commander gets its OWN OS-level handle to the same serial port via
    /// [`SerialPort::try_clone`] — exactly the mechanism
    /// [`Ppk2::start_measurement_matching`] already uses to hand a port to the
    /// data-receive thread. Two independent handles to the same fd therefore
    /// already coexist in normal operation; this adds a third for writes only.
    ///
    /// Take the commander BEFORE calling [`Ppk2::start_measurement`] /
    /// [`Ppk2::start_measurement_matching`], since those consume `self`. The
    /// commander is independent of the `Ppk2` value and outlives the move into
    /// the measurement pipeline.
    ///
    /// # Why this is safe
    ///
    /// See the [`Ppk2Commander`] docs: every command it can issue has an
    /// `expected_response_len()` of 0, so the device sends nothing back and the
    /// commander never reads a byte from the port. It cannot steal bytes from
    /// the measurement stream.
    ///
    /// # Caller responsibilities
    ///
    /// The commander does not serialize writes. If it is shared across threads
    /// the caller must ensure two writes are never interleaved mid-command (a
    /// `RegulatorSet` is 3 bytes, a `DeviceRunningSet` is 2), or the device
    /// will see a spliced, corrupt opcode stream. Wrapping it in a `Mutex` is
    /// sufficient.
    pub fn commander(&self) -> Result<Ppk2Commander> {
        Ok(Ppk2Commander {
            port: self.port.try_clone()?,
        })
    }

    /// Start measurements. Returns a tuple of:
    /// - [Ppk2<Measuring>],
    /// - [Receiver] of [measurement::MeasurementMatch], and
    /// - A closure that can be called to stop the measurement parsing pipeline and return the
    ///   device.
    ///
    /// # The device is recoverable even when the stream failed
    ///
    /// The `stop` closure returns the [`Ppk2`] whether or not the measurement
    /// worker ended cleanly; see
    /// [`start_measurement_matching`](Ppk2::start_measurement_matching) for the
    /// full rationale and [`Ppk2::take_last_worker_error`] for inspecting what
    /// went wrong.
    pub fn start_measurement(
        self,
        sps: usize,
    ) -> Result<(Receiver<MeasurementMatch>, impl FnOnce() -> Result<Self>)> {
        self.start_measurement_matching(LogicPortPins::default(), sps)
    }

    /// Start measurements. Returns a tuple of:
    /// - [Ppk2<Measuring>],
    /// - [Receiver] of [measurement::Result], and
    /// - A closure that can be called to stop the measurement parsing pipeline and return the
    ///   device.
    ///
    /// # The device is recoverable even when the stream failed
    ///
    /// This call consumes `self`, so the `stop` closure is the ONLY way to get
    /// the handle back — and it is a `FnOnce`, so there is exactly one attempt.
    /// A `stop` that returned `Err` on a broken stream would therefore destroy
    /// the device permanently, leaving the caller unable to disable device
    /// power or close the port: with a DUT wired up, that can leave hardware
    /// energized. "Did the worker exit cleanly" and "can I have my device back"
    /// are independent questions, and the second must never depend on the
    /// first.
    ///
    /// So `stop` returns the [`Ppk2`] in every worker outcome — clean exit,
    /// returned error, or panic. A worker error is logged at `warn` and stored
    /// on the returned device, where [`Ppk2::take_last_worker_error`] can
    /// retrieve it. The returned device is fully usable; the recorded error
    /// means only that the measurement stream ended abnormally and samples it
    /// would have produced are missing.
    ///
    /// The common trigger is dropping the returned [`Receiver`] before calling
    /// `stop`: the worker's next send then fails, ending the stream early.
    /// The worker treats that as a benign shutdown (nobody wants the
    /// measurements any more), so it does not even count as a worker error.
    pub fn start_measurement_matching(
        mut self,
        pins: LogicPortPins,
        sps: usize,
    ) -> Result<(Receiver<MeasurementMatch>, impl FnOnce() -> Result<Self>)> {
        // Stuff needed to communicate with the main thread
        // ready allows main thread to signal worker when serial input buf is cleared.
        let ready = Arc::new((Mutex::new(false), Condvar::new()));
        // This channel is for sending measurements to the main thread.
        let (meas_tx, meas_rx) = mpsc::channel::<MeasurementMatch>();
        // This channel allows the main thread to notify that the worker thread can stop
        // parsing data.
        let (sig_tx, sig_rx) = mpsc::channel::<()>();

        let task_ready = ready.clone();
        let metadata = self.metadata.clone();

        // The worker gets its OWN handle to the port. On Windows that clone is
        // NATIVE, so the raw handle the drain probe needs belongs to the port the
        // worker owns and cannot outlive it. Deriving it from `self.port` instead
        // would leave the worker calling `SetCommTimeouts` on a closed handle
        // whenever the caller drops the `stop` closure without calling it — and
        // Windows recycles handle values.
        #[cfg(windows)]
        let (mut port, worker_handle): (Box<dyn SerialPort>, isize) = {
            use std::os::windows::io::AsRawHandle;
            let native = self.port.try_clone_native()?;
            let handle = native.as_raw_handle() as isize;
            (Box::new(native), handle)
        };
        #[cfg(not(windows))]
        let mut port: Box<dyn SerialPort> = self.port.try_clone()?;

        // Every `COMMTIMEOUTS` form written from here on preserves this, so a
        // concurrent `Ppk2Commander` write — which carries
        // `set_device_power(Disabled)` — keeps its normal budget no matter what
        // the read path is doing.
        #[cfg(windows)]
        let write_timeout_ms = u32::try_from(self.port.timeout().as_millis()).unwrap_or(u32::MAX);

        // Ask the OS serial driver to batch reads for the duration of the
        // stream. See the `win_read_coalescing` module docs: this governs every
        // handle to the port, so applying it through the worker's clone here on
        // the main thread — while the worker is still parked on the condvar —
        // governs the commander's handle too.
        //
        // Failure is not fatal: it only means reads keep arriving in the small
        // pieces serialport's default timeouts produce. Those are still
        // classified and placed correctly, just more of them.
        #[cfg(windows)]
        let native_read_coalescing =
            match win_read_coalescing::apply(worker_handle, WIN_READ_COALESCE_MS, write_timeout_ms)
            {
                Ok(()) => true,
                Err(e) => {
                    tracing::warn!("failed to enable native read coalescing ({:?})", e);
                    false
                }
            };

        // How many bytes to ask for per read. On Windows the driver coalesces,
        // and asking for exactly the batch we want makes the BYTE count the
        // binding constraint (see `WIN_READ_LEN_BYTES`). Everywhere else the
        // driver caps the read at its own transfer size regardless — measured at
        // 1020 B on macOS against a 4096 B request — so asking for the whole
        // buffer costs nothing.
        #[cfg(windows)]
        let read_len = if native_read_coalescing {
            WIN_READ_LEN_BYTES.min(USB_READ_BUF_BYTES)
        } else {
            USB_READ_BUF_BYTES
        };
        #[cfg(not(windows))]
        let read_len = USB_READ_BUF_BYTES;

        let t = thread::spawn(move || {
            let r = || -> Result<()> {
                // Owns the parser, the held-sample ring, the skipped-sample
                // accounting, the stall guard and the seam guard. See
                // `crate::stream` for why the timebase lives in one testable
                // place rather than being spread through this loop.
                let mut assembler = StreamAssembler::new(metadata, sps, pins);
                // First wait for main thread to clear
                // serial port input buffer
                let (lock, cvar) = &*task_ready;
                let _l = cvar
                    .wait_while(lock.lock().unwrap(), |ready| !*ready)
                    .unwrap();

                /* A single PPK2 sample is 4 bytes and the device streams ~400 bytes/ms
                   continuously. Each iteration performs exactly ONE blocking read, then ONE
                   strictly non-blocking follow-up read (the drain probe) that answers the only
                   question that matters for placing the samples in time: was anything still
                   queued behind them?

                   The probe is not an optimisation and its result is not advisory. A read that
                   drained the port dates its own last sample; a read that swept up a backlog
                   dates nothing at all, and its samples have to be held until a draining read
                   arrives to pin them against. Measured on macOS under CPU saturation, 61.65%
                   of samples arrive on the second path.

                   Note what is NOT here any more: the userspace read accumulation that used to
                   merge several reads into one emission. It cannot coexist with this design —
                   merging reads destroys exactly the per-read drain observation the timebase is
                   built on, and its `set_timeout` calls clobbered the Windows COMMTIMEOUTS. The
                   assembler batches instead, by holding samples it cannot yet place.
                */
                let mut buf = [0u8; USB_READ_BUF_BYTES];
                let mut probe_buf = [0u8; USB_READ_BUF_BYTES];
                let mut out: Vec<MeasurementMatch> = Vec::new();

                // Backstop against a device that stops streaming and never
                // resumes. See `SilenceGuard`: an empty read on its own is
                // routine and must not be fatal, but spinning on an empty port
                // forever is not an option either.
                let mut silence = SilenceGuard::new(Instant::now());
                let port_timeout = port.timeout();

                loop {
                    // Check whether the main thread has signaled
                    // us to stop
                    match sig_rx.try_recv() {
                        Ok(_) => return Ok(()),
                        Err(TryRecvError::Empty) => {}
                        Err(e) => return Err(e.into()),
                    }

                    let filled = match port.read(&mut buf[..read_len]) {
                        Ok(n) => n,
                        // An empty read is NOT an error. On Windows 3.75% of
                        // reads return zero bytes with `TimedOut` as a matter of
                        // routine, and on macOS one errored read is the measured
                        // signature of a host suspend — which fires roughly every
                        // 4 minutes on a sleeping laptop, ~200 times overnight.
                        //
                        // This used to be fatal on every platform without native
                        // coalescing, so a suspend killed the measurement. It no
                        // longer is: the stall guard in the assembler is what
                        // decides whether silence cost us anything, and it can
                        // only do that if the worker is still alive to see the
                        // stream resume. A genuinely broken port still fails with
                        // a different error kind and still propagates.
                        //
                        // What DOES end the stream is sustained silence, counted
                        // by the guard below.
                        Err(e) if is_empty_read(&e) => {
                            if let Some(dead) = silence.on_empty_read(Instant::now()) {
                                return Err(Error::StreamDead {
                                    reads: dead.reads,
                                    elapsed: dead.elapsed,
                                    timeout: port_timeout,
                                });
                            }
                            continue;
                        }
                        Err(e) => return Err(e.into()),
                    };
                    // A zero-length `Ok` is the same event as an empty-kind
                    // error, just reported differently by the platform, and it
                    // must count the same way. Left uncounted it would be the one
                    // path that can spin without bound: a disconnected tty whose
                    // `poll` reports ready and whose `read` then returns 0.
                    if filled == 0 {
                        if let Some(dead) = silence.on_empty_read(Instant::now()) {
                            return Err(Error::StreamDead {
                                reads: dead.reads,
                                elapsed: dead.elapsed,
                                timeout: port_timeout,
                            });
                        }
                        continue;
                    }

                    // Both clocks, the INSTANT the read returns: before the
                    // probe, before parsing, before anything is queued. Every
                    // sample this read delivered is placed relative to this pair,
                    // so anything folded in here is folded into the timebase.
                    let clocks = ReadClocks::now();
                    // Data arrived, so the stream is alive; reuse the monotonic
                    // stamp just taken rather than paying for another syscall.
                    silence.on_data(clocks.mono);

                    #[cfg(windows)]
                    let (probe_filled, probe_err) = drain_probe(
                        &mut port,
                        &mut probe_buf,
                        worker_handle,
                        write_timeout_ms,
                        native_read_coalescing,
                    );
                    #[cfg(not(windows))]
                    let (probe_filled, probe_err) = drain_probe(&mut port, &mut probe_buf);

                    // A probe that failed leaves drainage UNKNOWN, and an unknown
                    // is not a drain: the read is treated as un-anchorable rather
                    // than optimistically trusted. Any bytes it did return are
                    // still stream data and are still passed on — discarding them
                    // would be silent sample loss.
                    if let Some(e) = &probe_err {
                        tracing::debug!(
                            "drain probe failed ({:?}); treating the read as un-anchorable",
                            e
                        );
                    }
                    let trustable = probe_err.is_none() && probe_indicates_drained(probe_filled);

                    out.clear();
                    assembler.on_read(
                        clocks,
                        &buf[..filled],
                        &probe_buf[..probe_filled],
                        trustable,
                        &mut out,
                    );

                    for m in out.drain(..) {
                        // A send failure here has exactly one cause: the
                        // `Receiver` was dropped. That is a request to stop,
                        // not a fault — nobody wants measurements any more —
                        // so exit cleanly instead of reporting an error.
                        if meas_tx.send(m).is_err() {
                            tracing::debug!(
                                "Measurement receiver dropped; stopping the measurement worker"
                            );
                            return Ok(());
                        }
                    }
                }
            };
            let res = r();
            if let Err(e) = &res {
                tracing::error!("Error fetching measurements: {:?}", e);
            };
            res
        });
        self.port.clear(Input)?;

        let (lock, cvar) = &*ready;
        let mut ready = lock.lock().unwrap();
        *ready = true;
        cvar.notify_all();

        self.send_command(Command::AverageStart)?;

        // Returning the device is unconditional. Every step below can fail
        // without preventing a `Ppk2` from being produced, so none of them may
        // consume it: this closure is a `FnOnce` and is the caller's only route
        // back to the handle they need in order to make the hardware safe.
        let stop = move || {
            // A failed signal send is not a stop failure. The only way this can
            // fail is that `sig_rx` was dropped, i.e. the worker has ALREADY
            // exited — which is precisely the situation we are trying to
            // recover from. The join below reports the worker's actual outcome,
            // which is strictly more informative than this symptom.
            if let Err(e) = sig_tx.send(()) {
                tracing::debug!(
                    "Measurement worker had already exited before the stop signal: {:?}",
                    e
                );
            }

            // Both abnormal join outcomes still yield the device.
            let worker_error = match t.join() {
                Ok(Ok(())) => None,
                // The worker returned an error (e.g. the measurement `Receiver`
                // was dropped, or a port read failed).
                Ok(Err(e)) => Some(e),
                // The worker panicked. Do NOT `expect()` here: that would
                // unwind through this closure and take the device with it.
                Err(payload) => Some(Error::Io(io::Error::other(format!(
                    "measurement worker thread panicked: {}",
                    panic_payload_message(&*payload)
                )))),
            };
            if let Some(e) = worker_error {
                tracing::warn!(
                    "Measurement stream ended abnormally: {:?}. Returning the device anyway; \
                     retrieve this error with `Ppk2::take_last_worker_error`.",
                    e
                );
                self.last_worker_error = Some(e);
            }

            // Undo the streaming read coalescing now that the worker has
            // joined, so the device handed back reads with the timeouts the
            // caller configured — `take_last_worker_error`'s contract promises
            // a fully usable device, and `get_metadata` on a port still
            // programmed to give up after a few milliseconds would not be.
            //
            // Re-asserting the SAME timeout is not a no-op: `set_timeout`
            // unconditionally rewrites COMMTIMEOUTS, which is exactly how it
            // clobbers our settings elsewhere. Here that is the point.
            //
            // Windows-only: nothing else in the stream leaves device-wide state
            // behind. The drain probe rewrites COMMTIMEOUTS on every read but
            // always restores whichever form governs the main read before it
            // returns, and on unix the probe's timeout is per-object and dies
            // with the worker's cloned port.
            #[cfg(windows)]
            if native_read_coalescing {
                let timeout = self.port.timeout();
                if let Err(e) = self.port.set_timeout(timeout) {
                    tracing::warn!("failed to restore default port timeouts: {:?}", e);
                }
            }

            // Best-effort quiesce. A write failure here says the port is in
            // trouble, but it does not make the handle worthless — the caller
            // may still manage to disable device power with it, and
            // `Drop for Ppk2` retries this same write.
            if let Err(e) = self.send_command(Command::AverageStop) {
                tracing::warn!("AverageStop while stopping the measurement failed: {:?}", e);
            }

            Ok(self)
        };

        Ok((meas_rx, stop))
    }

    /// Reset the device, making the device unusable.
    pub fn reset(mut self) -> Result<()> {
        self.send_command(Command::Reset)?;
        Ok(())
    }

    /// Change the measurement mode (source-meter vs. ampere-meter) on a live,
    /// already-open handle without reopening the device.
    ///
    /// # Must be called while measurements are stopped
    ///
    /// This issues a `SetPowerMode` command directly and does not coordinate
    /// with the streaming data-receive thread. It must therefore only be called
    /// while no measurement stream is running — i.e. on the handle returned by
    /// the `stop` closure of [`Ppk2::start_measurement`] (which has joined the
    /// receive thread and stopped averaging). Calling it during an active
    /// stream would race the measurement handshake and corrupt the sample
    /// stream. The internal command handshake is otherwise unchanged.
    pub fn set_power_mode(&mut self, mode: MeasurementMode) -> Result<()> {
        self.send_command(Command::SetPowerMode(mode))?;
        Ok(())
    }
}

impl Drop for Ppk2 {
    fn drop(&mut self) {
        // Best-effort quiesce: stop any streaming and clear buffers so the
        // next client sees a clean line. Failures are expected if the port
        // is already closed or the device is unplugged - swallow them.
        let _ = self
            .port
            .write_all(&Vec::from_iter(Command::AverageStop.bytes()));
        let _ = self.port.clear(serialport::ClearBuffer::All);
    }
}

/// A **write-only** command handle to a PPK2, usable concurrently with a
/// running measurement. Obtained from [`Ppk2::commander`].
///
/// # What this is for
///
/// [`Ppk2::start_measurement_matching`] consumes the [`Ppk2`], so ordinarily
/// the only way to issue a command mid-measurement is to stop the measurement,
/// which discards buffered samples and destroys the timebase. A driver that
/// needs to re-issue voltage or power commands repeatedly cannot pay that cost.
/// The commander lifts that restriction for the subset of commands where doing
/// so is provably safe.
///
/// # Why this is safe
///
/// `Ppk2::send_command` writes the command, then loops
/// `while !command.response_complete(&response) { port.read(..) }`. For every
/// command except `GetMetaData`, `response_complete` is
/// `expected_response_len() >= response.len()`. A command whose
/// `expected_response_len()` is **0** therefore satisfies `0 >= 0` immediately,
/// the read loop body never runs, and the write is the entire exchange: the
/// device sends nothing back.
///
/// Every command reachable through this type has been verified to have
/// `expected_response_len() == 0` (see `cmd.rs`):
///
/// | method | command | `expected_response_len()` |
/// |---|---|---|
/// | [`set_source_voltage`](Ppk2Commander::set_source_voltage) | `RegulatorSet` | 0 |
/// | [`set_device_power`](Ppk2Commander::set_device_power) | `DeviceRunningSet` | 0 |
///
/// Because nothing is ever read on this path, the commander cannot consume
/// bytes belonging to the measurement stream, and the data-receive thread's
/// own cloned port handle keeps seeing an unbroken sample stream.
///
/// Note that the methods below deliberately do NOT call `Ppk2::send_command`,
/// even though it would currently behave identically for these commands. They
/// write the bytes directly, so that a future edit to `send_command` (an
/// unconditional drain, a handshake, a status read) cannot silently introduce a
/// read on this path.
///
/// # What this must NEVER be used for
///
/// **Any command that expects a response.** Issuing one here would leave the
/// device's reply sitting in the same byte stream the measurement thread is
/// parsing; the reply bytes would be misread as 4-byte sample frames, silently
/// corrupting measurements and desynchronizing the parser's frame alignment
/// from that point on. `GetMetaData` (512 bytes) is the obvious case, but the
/// rule is categorical.
///
/// This is why the type exposes no general `send_command` escape hatch and no
/// way to construct an arbitrary [`cmd::Command`]. The restricted surface IS
/// the safety mechanism — do not widen it without re-verifying
/// `expected_response_len()` for the command being added.
///
/// # Caller responsibilities
///
/// * **Do not interleave writes mid-command.** Writes are not serialized
///   internally. A `RegulatorSet` is 3 bytes and a `DeviceRunningSet` is 2; if
///   two threads write through separate commanders (or a shared one) at the
///   same time, the device can observe a spliced opcode stream. Serialize
///   externally (e.g. a `Mutex`) if the handle is shared.
/// * **This is a borrowed capability, not ownership.** The commander does not
///   own the device: it has no `Drop` behaviour, does not stop averaging, and
///   does not close or quiesce the port. Dropping it is a no-op beyond
///   releasing its cloned handle. The [`Ppk2`] (or the `stop` closure that
///   returns it) remains the owner and is responsible for shutdown.
pub struct Ppk2Commander {
    port: Box<dyn SerialPort>,
}

impl Ppk2Commander {
    /// Write a zero-response command to the port and return immediately,
    /// without reading.
    ///
    /// Private on purpose: exposing this would let callers issue a
    /// response-bearing command and corrupt a concurrent measurement stream.
    /// See the type-level docs.
    ///
    /// Every call site must pass a command whose `expected_response_len()` is
    /// 0; the `debug_assert!` records that invariant for anyone adding one.
    fn write_zero_response_command(&mut self, command: Command) -> Result<()> {
        debug_assert_eq!(
            command.expected_response_len(),
            0,
            "Ppk2Commander may only issue commands that expect no response; \
             this one would leave reply bytes in the measurement stream"
        );
        // Mirrors the write half of `Ppk2::send_command`, intentionally
        // without its read loop.
        self.port.write_all(&Vec::from_iter(command.bytes()))?;
        Ok(())
    }

    /// Set the voltage of the device voltage source, without disturbing a
    /// running measurement.
    ///
    /// Issues `RegulatorSet`, whose `expected_response_len()` is 0: the device
    /// sends no reply, so nothing is read and no measurement bytes are
    /// consumed.
    pub fn set_source_voltage(&mut self, vdd: SourceVoltage) -> Result<()> {
        self.write_zero_response_command(Command::RegulatorSet(vdd))
    }

    /// Enable or disable the device power, without disturbing a running
    /// measurement.
    ///
    /// Issues `DeviceRunningSet`, whose `expected_response_len()` is 0: the
    /// device sends no reply, so nothing is read and no measurement bytes are
    /// consumed.
    pub fn set_device_power(&mut self, power: DevicePower) -> Result<()> {
        self.write_zero_response_command(Command::DeviceRunningSet(power))
    }
}

/// Whether an `io::Error` from a read means "nothing was waiting" rather than
/// "the port is broken".
///
/// All three kinds mean the same thing here: the read consumed nothing and the
/// caller may simply go round again. `TimedOut` in particular is ROUTINE, not
/// exceptional — 3.75% of Windows reads return zero bytes with it, and it is the
/// strongest drain signal the platform offers.
fn is_empty_read(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
    )
}

/// Issue the strictly non-blocking follow-up read that decides whether the
/// preceding read drained the port.
///
/// Returns `(bytes_read, error)`. Both may be meaningful at once: a probe that
/// read bytes and then failed to restore the port's timeouts reports both, and
/// the bytes are still the caller's to keep. **Probe bytes are real stream data
/// and must never be discarded** — they are simply the newest samples in the
/// stream.
///
/// The probe MUST be strictly non-blocking. The PPK2 streams ~400 bytes per
/// millisecond, so even a 1 ms wait would find bytes essentially every time and
/// report that no read ever drains the port — a false negative manufactured by
/// the instrument, which would push every sample onto the backward-pinning path.
///
/// A small non-zero result does occur occasionally even on a port that had in
/// fact drained, because the probe costs a syscall during which ~4 bytes arrive
/// every 10 µs. It is NOT tolerated: [`probe_indicates_drained`] requires a
/// strict zero, so such a read is placed by backward pinning instead of
/// anchoring itself. See that function for why the obvious 64-byte tolerance is
/// the wrong trade on macOS.
///
/// # Windows
///
/// Programs the documented zero-wait `COMMTIMEOUTS` form directly rather than
/// calling `set_timeout(Duration::ZERO)`, which writes a different (and merely
/// approximate) combination — see [`win_read_coalescing::apply_immediate`]. The
/// write timeout is preserved throughout, so a concurrent [`Ppk2Commander`] is
/// never affected.
#[cfg(windows)]
fn drain_probe(
    port: &mut Box<dyn SerialPort>,
    buf: &mut [u8],
    handle: isize,
    write_timeout_ms: u32,
    native_read_coalescing: bool,
) -> (usize, Option<Error>) {
    if let Err(e) = win_read_coalescing::apply_immediate(handle, write_timeout_ms) {
        return (0, Some(e));
    }
    let res = port.read(buf);

    // Restore whatever governs the MAIN read. Without this the next blocking
    // read would return immediately with nothing, forever.
    let restore = if native_read_coalescing {
        win_read_coalescing::apply(handle, WIN_READ_COALESCE_MS, write_timeout_ms)
    } else {
        let timeout = port.timeout();
        port.set_timeout(timeout).map_err(Error::from)
    };

    let (n, mut err) = match res {
        Ok(n) => (n, None),
        // "Nothing was waiting" is the probe's SUCCESS case: the preceding read
        // drained the port.
        Err(e) if is_empty_read(&e) => (0, None),
        Err(e) => (0, Some(Error::from(e))),
    };
    if let Err(e) = restore {
        let _ = err.get_or_insert(e);
    }
    (n, err)
}

/// See the Windows variant. On unix the same effect is had by shortening the
/// port timeout to zero, since the backend `poll`s with it before reading — and
/// unlike Windows that timeout is per-object, so it cannot disturb any other
/// handle to the same device.
#[cfg(not(windows))]
fn drain_probe(port: &mut Box<dyn SerialPort>, buf: &mut [u8]) -> (usize, Option<Error>) {
    let original = port.timeout();
    if let Err(e) = port.set_timeout(PROBE_TIMEOUT) {
        return (0, Some(Error::from(e)));
    }
    let res = port.read(buf);
    let restore = port.set_timeout(original);

    let (n, mut err) = match res {
        Ok(n) => (n, None),
        // "Nothing was waiting" is the probe's SUCCESS case: the preceding read
        // drained the port.
        Err(e) if is_empty_read(&e) => (0, None),
        Err(e) => (0, Some(Error::from(e))),
    };
    if let Err(e) = restore {
        let _ = err.get_or_insert(Error::from(e));
    }
    (n, err)
}

/// Render a [`thread::JoinHandle::join`] panic payload as a human-readable
/// string.
///
/// `panic!` produces a `&'static str` payload for a literal message and a
/// `String` for a formatted one; anything else (a `panic_any`) is not
/// printable, so it is described generically rather than dropped silently.
fn panic_payload_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

/// Try to find the serial port the PPK2 is connected to.
pub fn try_find_ppk2_port() -> Result<String> {
    use serialport::SerialPortType::UsbPort;

    Ok(serialport::available_ports()?
        .into_iter()
        .find(|p| match &p.port_type {
            UsbPort(usb) => usb.vid == 0x1915 && usb.pid == 0xc00a,
            _ => false,
        })
        .ok_or(Error::Ppk2NotFound)?
        .port_name)
}
