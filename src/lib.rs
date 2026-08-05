#![doc = include_str!("../README.md")]
#![deny(missing_docs)]

use measurement::{MeasurementAccumulator, MeasurementIterExt, MeasurementMatch};
use serialport::{ClearBuffer::Input, FlowControl, SerialPort};
use std::io::Read;
use std::str::Utf8Error;
use std::sync::mpsc::{self, Receiver, SendError, TryRecvError};
use std::{
    borrow::Cow,
    collections::VecDeque,
    io,
    sync::{Arc, Condvar, Mutex},
    thread,
    time::{Duration, Instant, SystemTime},
};
use thiserror::Error;
use types::{DevicePower, LogicPortPins, MeasurementMode, Metadata, SourceVoltage};

use crate::cmd::Command;

pub mod cmd;
pub mod measurement;
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

/// Byte target for one read accumulation in the measurement worker.
///
/// # Why accumulate at all
///
/// How many bytes a single `read()` returns is a property of the OS serial
/// driver, not of this crate, and it differs wildly across platforms:
///
/// - On macOS/Linux the tty layer hands back a large chunk per read, typically
///   a whole USB transfer, so one read already carries hundreds of samples.
/// - On **Windows** `serialport` configures `COMMTIMEOUTS` with
///   `ReadIntervalTimeout = MAXDWORD` and `ReadTotalTimeoutMultiplier =
///   MAXDWORD` (see `serialport::windows::COMPort::set_timeout`). Per the
///   `COMMTIMEOUTS` remarks that combination means `ReadFile` returns
///   IMMEDIATELY with whatever bytes are already in the driver buffer, however
///   few — it only waits at all when the buffer is completely empty. At
///   ~100,000 samples/sec (~400 KB/s) that yields tens of thousands of reads
///   per second, each returning a handful of bytes.
///
/// Each such read is a syscall, a thread wakeup, its own `read_at` stamp, and
/// its own trickle of `MeasurementMatch`es into the mpsc channel. The result on
/// Windows is heavily fragmented delivery and ragged arrival timing, which
/// destabilizes any consumer estimating an absolute offset from `read_at`.
///
/// So after a read returns we keep reading into the remaining buffer space
/// until we hold this many bytes (or [`READ_ACCUM_WINDOW`] elapses, or the
/// buffer fills), then parse and emit the whole accumulation at once.
///
/// 2 KB is 512 raw samples, ≈5 ms of stream at the full rate — deliberately
/// matched to [`READ_ACCUM_WINDOW`] so neither bound dominates the other at
/// full speed. It is well under [`USB_READ_BUF_BYTES`], so the buffer-full
/// bound is a safety net rather than the normal exit.
///
/// This is intentionally NOT `cfg(windows)`-gated: it is stated in terms of
/// bytes in hand, not platform. Where reads are already large (macOS) the
/// first read alone meets this target and the accumulation block is skipped
/// entirely, so those platforms are unchanged by construction.
const READ_ACCUM_TARGET_BYTES: usize = 2 * 1024;

/// Maximum wall time spent accumulating bytes after the first read returns,
/// measured from the moment that first read returned.
///
/// This bounds the latency the accumulation can add when the stream is slower
/// than full rate (or has stopped): we never hold parsed-able bytes longer than
/// this waiting for [`READ_ACCUM_TARGET_BYTES`]. 5 ms sits comfortably inside
/// the 10 ms coalescing window the consumer already applies.
const READ_ACCUM_WINDOW: Duration = Duration::from_millis(5);

/// Read timeout used for the CONTINUATION reads inside an accumulation.
///
/// The port's normal timeout is hundreds of milliseconds, which is right for
/// the first read (nothing is held, so blocking costs nothing) but wrong for a
/// continuation read: a stream that stalls mid-accumulation would hold the
/// bytes we already have hostage for that whole timeout. So the timeout is
/// temporarily shortened for the continuation reads and restored afterwards.
///
/// # Why this equals [`READ_ACCUM_WINDOW`] rather than being much smaller
///
/// `SerialPort::set_timeout` sets the READ and WRITE timeouts together — the
/// Windows backend writes `ReadTotalTimeoutConstant` AND
/// `WriteTotalTimeoutConstant` from the same value. And a `try_clone`d port
/// only *caches* the timeout per object: the handles are `DuplicateHandle`
/// duplicates of one file object, so `SetCommTimeouts` changes the device for
/// ALL of them (`serialport`'s own `try_clone` docs warn that changing settings
/// through one clone of a port causes "nasty behavior" in the others).
///
/// [`Ppk2Commander`] holds such a clone and WRITES through it while this worker
/// is streaming. Since a Windows read blocks whenever the driver buffer is
/// empty, the shortened timeout is in force for essentially the whole duration
/// of a measurement — so whatever value is chosen here IS the write timeout the
/// commander gets. That path carries `set_device_power(Disabled)`, so making it
/// fail spuriously could leave a DUT energized: exactly the hazard
/// [`Ppk2::take_last_worker_error`]'s commit set out to remove.
///
/// 5 ms is therefore a deliberate floor. It is generous for the 2–3 byte writes
/// the commander issues, while still bounding how long an accumulation can hold
/// samples when the stream stalls. The deadline is checked BEFORE each read, so
/// a read starting just under the deadline can overshoot by one timeout: worst
/// case ≈ 2 × [`READ_ACCUM_WINDOW`] ≈ 10 ms, and only when the device has gone
/// quiet mid-accumulation — a stream at rate hits
/// [`READ_ACCUM_TARGET_BYTES`] long before any read blocks that long.
///
/// It must also never be sub-millisecond: the Windows backend converts the
/// timeout to whole milliseconds, and the "return as soon as any bytes are
/// available" `COMMTIMEOUTS` idiom is only documented for a constant strictly
/// greater than zero. A sub-millisecond `Duration` would truncate to 0 and
/// leave the total-timeout computation (`MAXDWORD * bytes + 0`) unbounded.
const READ_ACCUM_POLL_TIMEOUT: Duration = READ_ACCUM_WINDOW;

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
/// programming them through any one handle governs them all. Two consequences,
/// both load-bearing:
///
/// 1. It is enough to apply this once, on the main thread, through the handle
///    this `Ppk2` opened — the worker's cloned reader inherits it. No raw
///    handle has to cross the thread boundary.
/// 2. Any later `set_timeout` call on ANY clone **clobbers** it, restoring
///    serialport's return-immediately idiom. That is why the userspace read
///    accumulation is disabled while this is active (see the read loop) and why
///    a single `set_timeout` is all that is needed to undo it on stop.
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
        let timeouts = COMMTIMEOUTS {
            // No inter-byte timeout: do not cut a read short just because the
            // stream paused briefly mid-transfer.
            ReadIntervalTimeout: 0,
            // Total read timeout is a flat constant, independent of how many
            // bytes were requested, so the latency bound does not scale with
            // the buffer size.
            ReadTotalTimeoutMultiplier: 0,
            ReadTotalTimeoutConstant: coalesce_ms,
            // Writes keep the port's configured timeout. See the module docs.
            WriteTotalTimeoutMultiplier: 0,
            WriteTotalTimeoutConstant: write_timeout_ms,
        };

        // `HANDLE` is a plain `isize` in windows-sys 0.52 (the version pinned
        // in Cargo.toml, and the one serialport itself uses), so the stored
        // handle is passed through without a cast.
        let handle: HANDLE = handle;

        // SAFETY: `handle` is the raw handle of the `COMPort` owned by this
        // `Ppk2`'s `port` field, which outlives every call site (both are on
        // the main thread, with `self` borrowed). `timeouts` is a fully
        // initialized `#[repr(C)]` `COMMTIMEOUTS` and is only read by the call.
        if unsafe { SetCommTimeouts(handle, &timeouts) } == 0 {
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
}

#[allow(missing_docs)]
pub type Result<T> = std::result::Result<T, Error>;

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
    port: Box<dyn SerialPort>,
    metadata: Metadata,
    /// Set by the `stop` closure of [`Ppk2::start_measurement_matching`] when
    /// the measurement worker ended abnormally (returned an error or panicked).
    ///
    /// The device is still returned to the caller in that case — see
    /// [`Ppk2::take_last_worker_error`] — because "the stream broke" and "the
    /// handle is unusable" are independent facts, and the caller usually needs
    /// the handle precisely to make the hardware safe.
    last_worker_error: Option<Error>,
    /// Windows only: the raw handle backing `port`, kept as an `isize` so this
    /// struct stays `Send` (a `RawHandle` is a raw pointer and would silently
    /// make `Ppk2` un-sendable for downstream callers).
    ///
    /// Needed because [`win_read_coalescing`] programs the driver through
    /// `SetCommTimeouts`, which the `SerialPort` trait object cannot expose.
    /// Only ever used while `port` is alive and borrowed, so it is never
    /// dangling — see the SAFETY note at the call.
    #[cfg(windows)]
    native_handle: isize,
}

impl Ppk2 {
    /// Create a new instance and configure the given [MeasurementMode].
    pub fn new<'a>(path: impl Into<Cow<'a, str>>, mode: MeasurementMode) -> Result<Self> {
        let builder = serialport::new(path, 9600)
            .timeout(Duration::from_millis(500))
            .flow_control(FlowControl::Hardware);

        // On Windows open the NATIVE port type rather than a trait object, so
        // the raw handle can be captured before boxing. `win_read_coalescing`
        // needs it to call `SetCommTimeouts`, and the `SerialPort` trait
        // exposes no way to recover a handle from a `Box<dyn SerialPort>`.
        #[cfg(windows)]
        let (mut port, native_handle): (Box<dyn SerialPort>, isize) = {
            use std::os::windows::io::AsRawHandle;
            let native = builder.open_native()?;
            let handle = native.as_raw_handle() as isize;
            (Box::new(native), handle)
        };
        #[cfg(not(windows))]
        let mut port: Box<dyn SerialPort> = builder.open()?;

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
            #[cfg(windows)]
            native_handle,
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
        let mut port = self.port.try_clone()?;
        let metadata = self.metadata.clone();

        // Ask the OS serial driver to batch reads for the duration of the
        // stream. See the `win_read_coalescing` module docs: this governs every
        // handle to the port, including the clone above and any
        // `Ppk2Commander`, so it is applied once here on the main thread while
        // the worker is still parked on the condvar.
        //
        // Failure is not fatal — it only means reads keep arriving in the
        // small pieces serialport's default timeouts produce, which is what the
        // userspace accumulation in the worker exists to absorb.
        #[cfg(windows)]
        let native_read_coalescing = {
            let write_timeout_ms =
                u32::try_from(self.port.timeout().as_millis()).unwrap_or(u32::MAX);
            match win_read_coalescing::apply(
                self.native_handle,
                WIN_READ_COALESCE_MS,
                write_timeout_ms,
            ) {
                Ok(()) => true,
                Err(e) => {
                    tracing::warn!(
                        "failed to enable native read coalescing ({:?}); \
                         falling back to userspace read accumulation",
                        e
                    );
                    false
                }
            }
        };
        #[cfg(not(windows))]
        let native_read_coalescing = false;

        let t = thread::spawn(move || {
            let r = || -> Result<()> {
                // Create an accumulator with the current device metadata
                let mut accumulator = MeasurementAccumulator::new(metadata);
                // First wait for main thread to clear
                // serial port input buffer
                let (lock, cvar) = &*task_ready;
                let _l = cvar
                    .wait_while(lock.lock().unwrap(), |ready| !*ready)
                    .unwrap();

                /* A single PPK2 sample is 4 bytes and the device streams up to 100,000 samples/sec.
                   We read into a reusable USB_READ_BUF_BYTES buffer and feed the exact number of
                   bytes accumulated to the accumulator. `port.read()` returns whatever bytes are
                   currently available (it does NOT wait to fill the buffer), and on Windows that is
                   often only a handful — so each iteration keeps reading into the remaining buffer
                   space until READ_ACCUM_TARGET_BYTES / READ_ACCUM_WINDOW / buffer-full, then parses
                   and emits the whole accumulation at once. The accumulator carries a partial-sample
                   remainder internally, so feeding any byte count is correctness-preserving.

                   The driver decimates by AVERAGING `SPS_MAX/sps` parsed samples into one output via
                   `combine_matching`. The drain below pulls samples in FIXED `chunk`-sized units in a
                   while-loop, so each emitted measurement always averages exactly `chunk` samples
                   regardless of how many bytes the last `read()` delivered. Leftover samples
                   (< chunk) stay in `measurement_buf` for the next read. This decouples the averaging
                   window from the USB read size — read buffer size never changes decimated output.
                */
                let mut buf = [0u8; USB_READ_BUF_BYTES];
                let mut measurement_buf = VecDeque::with_capacity(SPS_MAX);
                let mut missed = 0;

                // How many bytes to ask for per read. With native coalescing a
                // read ends at whichever comes first: this many bytes, or the
                // driver's few-millisecond window. Asking for exactly the batch
                // we want makes the BYTE count the binding constraint at full
                // rate, so the cadence is ~5 ms of stream per read regardless of
                // the system timer granularity that governs the window (a 5 ms
                // COMMTIMEOUTS constant can round up to a ~15 ms tick, which
                // against the full 4 KB buffer would let a read cover ~10 ms).
                // Without coalescing this is the whole buffer, exactly as
                // before, and the userspace accumulation bounds the batch.
                let read_len = if native_read_coalescing {
                    READ_ACCUM_TARGET_BYTES.min(buf.len())
                } else {
                    buf.len()
                };
                loop {
                    // Check whether the main thread has signaled
                    // us to stop
                    match sig_rx.try_recv() {
                        Ok(_) => return Ok(()),
                        Err(TryRecvError::Empty) => {}
                        Err(e) => return Err(e.into()),
                    }

                    // Now we read chunks and feed them to the accumulator.
                    //
                    // FIRST read: blocking. Nothing is held yet, so its
                    // semantics are exactly what they were before reads were
                    // batched — every error propagates — with ONE exception,
                    // below.
                    let mut filled = match port.read(&mut buf[..read_len]) {
                        Ok(n) => n,
                        // With native coalescing the read is bounded by a few
                        // milliseconds rather than the port's timeout, so an
                        // empty return means only "nothing arrived in the last
                        // coalescing window" — the normal state whenever the
                        // device is between bursts, and in particular before
                        // `AverageStart` reaches it. Treat it as no data and
                        // go round again (which re-checks the stop signal);
                        // killing the stream over it would make the worker die
                        // milliseconds after it started.
                        //
                        // Without native coalescing this arm is never taken and
                        // a timeout stays fatal exactly as before.
                        Err(e) if native_read_coalescing && e.kind() == io::ErrorKind::TimedOut => {
                            continue
                        }
                        Err(e) => return Err(e.into()),
                    };
                    // Stamp the wall clock the INSTANT the read returns, before
                    // parsing and before anything is queued. This is the whole
                    // point of the field: it is an upper bound on the capture
                    // time of the samples in `buf[..filled]` that excludes the
                    // mpsc queueing delay and the consumer's own scheduling.
                    // Doing this any later (in the consumer, on dequeue) folds
                    // in tens of milliseconds of scheduling jitter and destroys
                    // the estimate. Do NOT move this below `feed_into`.
                    //
                    // When the accumulation below pulls in more bytes, this is
                    // re-stamped after each successful read, so the emission
                    // carries the LAST read's arrival. That is the required
                    // direction: no sample may be stamped EARLIER than the read
                    // that actually delivered its bytes, and the earlier reads
                    // in an accumulation are strictly older than the last one.
                    let mut read_at = SystemTime::now();

                    // A continuation read may fail fatally. If it does we must
                    // still parse and emit what we already hold before
                    // propagating, so no delivered sample is dropped on the way
                    // out. Deferred here, returned after the drain below.
                    let mut fatal: Option<Error> = None;

                    // ACCUMULATION: keep reading into the remaining buffer space
                    // until we hold READ_ACCUM_TARGET_BYTES, the window expires,
                    // or the buffer is full. Motivation and constant choices are
                    // documented on READ_ACCUM_TARGET_BYTES; the short version is
                    // that Windows' COMMTIMEOUTS make every read return with a
                    // handful of bytes, and emitting per-read there fragments
                    // delivery and the arrival timing that consumers depend on.
                    //
                    // Skipped in two cases:
                    //
                    // * The first read already met the target — the normal case
                    //   on macOS/Linux, whose drivers return a full transfer per
                    //   read. Those platforms take the identical code path they
                    //   did before, with no extra read, no timeout churn, and
                    //   the first read's stamp.
                    // * Native read coalescing is active. The driver is then
                    //   already doing exactly this job inside `ReadFile` —
                    //   fill the buffer, bounded by a short window — with no
                    //   syscalls and no timeout mutation. Running the userspace
                    //   loop on top would be strictly worse than redundant: its
                    //   `set_timeout` calls CLOBBER the COMMTIMEOUTS that make
                    //   coalescing work (see `win_read_coalescing`), so the two
                    //   mechanisms would fight, and the driver would fall back
                    //   to returning immediately after the first accumulation.
                    if !native_read_coalescing
                        && filled > 0
                        && filled < READ_ACCUM_TARGET_BYTES
                        && filled < buf.len()
                    {
                        let deadline = Instant::now() + READ_ACCUM_WINDOW;
                        let port_timeout = port.timeout();
                        // Best-effort: if the timeout cannot be shortened, skip
                        // accumulating rather than risk blocking for the port's
                        // full timeout with parsed-able bytes already in hand.
                        if port.set_timeout(READ_ACCUM_POLL_TIMEOUT).is_ok() {
                            while filled < READ_ACCUM_TARGET_BYTES
                                && filled < buf.len()
                                && Instant::now() < deadline
                            {
                                match port.read(&mut buf[filled..]) {
                                    // Nothing more to be had right now: emit
                                    // what we hold rather than spinning.
                                    Ok(0) => break,
                                    Ok(n) => {
                                        filled += n;
                                        read_at = SystemTime::now();
                                    }
                                    // The stream went quiet inside the window.
                                    // End the accumulation and emit; do NOT
                                    // retry, or a stalled device would hold
                                    // these samples for the whole window while
                                    // burning wakeups.
                                    Err(e)
                                        if matches!(
                                            e.kind(),
                                            io::ErrorKind::TimedOut
                                                | io::ErrorKind::WouldBlock
                                                | io::ErrorKind::Interrupted
                                        ) =>
                                    {
                                        break
                                    }
                                    // Real failure (disconnect, etc.). Emit
                                    // first, then propagate below.
                                    Err(e) => {
                                        fatal = Some(e.into());
                                        break;
                                    }
                                }
                            }
                            // Always restore, including on the fatal path. On
                            // Windows this timeout is device-wide rather than
                            // per-handle (see READ_ACCUM_POLL_TIMEOUT), so it is
                            // also what `Ppk2Commander` sees and what the device
                            // is left with after the worker exits — and
                            // `take_last_worker_error`'s contract promises the
                            // returned device is fully usable, metadata reads
                            // included, no matter how the worker ended.
                            //
                            // Every exit from this loop passes through here, so
                            // the only way to leak the shortened timeout is a
                            // panic between the two `set_timeout` calls. Nothing
                            // in the loop can panic: `filled` is held below
                            // `buf.len()` by the loop condition so the slice
                            // index is always valid, and the remaining calls are
                            // all `Result`-returning or infallible. If a
                            // fallible-in-a-new-way operation is ever added
                            // here, convert this into an RAII guard rather than
                            // relying on that argument.
                            if let Err(e) = port.set_timeout(port_timeout) {
                                tracing::warn!("failed to restore port timeout: {:?}", e);
                            }
                        }
                    }

                    missed += accumulator.feed_into(&buf[..filled], &mut measurement_buf);
                    // Emit in fixed-size decimation units so each averaged output
                    // covers exactly `chunk` samples regardless of read size.
                    //
                    // INVARIANT (consumers depend on this — do not break it):
                    // summing the `missed` field of EVERY emitted
                    // `MeasurementMatch` yields the exact total number of raw
                    // samples the device skipped, with no double-counting and
                    // no silent loss. That holds because `missed` accumulates
                    // across reads while nothing is emitted, is handed to
                    // `combine_matching` (which propagates it into BOTH the
                    // `Match` and `NoMatch` variants), and is reset to 0 ONLY
                    // after a successful send. Any future edit that resets,
                    // skips, or conditionally forwards `missed` will make
                    // consumers' synthesized timelines drift early.
                    //
                    // `read_at` is orthogonal to that accounting: it is stamped
                    // per ACCUMULATION (at its last successful read) and
                    // forwarded to every chunk emitted from this iteration, so
                    // it neither contributes to nor is consumed by the `missed`
                    // sum above.
                    //
                    // Note a chunk can SPAN two reads: leftover samples below
                    // `chunk` stay in `measurement_buf` and are completed by the
                    // next read, and such a chunk is emitted here with the LATER
                    // (current) read's `read_at`. That is intentional — see the
                    // field docs on `MeasurementMatch::Match::read_at`. The
                    // stamp must never under-state arrival, so the later read
                    // is the correct one to attribute.
                    let chunk = (SPS_MAX / sps).max(1);
                    while measurement_buf.len() >= chunk {
                        let measurement = measurement_buf
                            .drain(..chunk)
                            .combine_matching(missed, read_at, pins);
                        // A send failure here has exactly one cause: the
                        // `Receiver` was dropped. That is a request to stop,
                        // not a fault — nobody wants measurements any more —
                        // so exit cleanly instead of reporting an error.
                        if meas_tx.send(measurement).is_err() {
                            tracing::debug!(
                                "Measurement receiver dropped; stopping the measurement worker"
                            );
                            return Ok(());
                        }
                        missed = 0;
                    }

                    // A continuation read failed fatally. Everything it had
                    // already delivered has now been parsed and emitted above,
                    // so the error can propagate without losing samples.
                    //
                    // This deliberately sits AFTER the drain, which means a
                    // `Receiver` dropped in the same iteration wins and returns
                    // `Ok(())` instead: if nobody wants the measurements any
                    // more, the read error that ended the stream is moot, and
                    // reporting it would turn a benign shutdown into a recorded
                    // worker error. Keep this ordering.
                    if let Some(e) = fatal {
                        return Err(e);
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
