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
}

impl Ppk2 {
    /// Create a new instance and configure the given [MeasurementMode].
    pub fn new<'a>(path: impl Into<Cow<'a, str>>, mode: MeasurementMode) -> Result<Self> {
        let mut port = serialport::new(path, 9600)
            .timeout(Duration::from_millis(500))
            .flow_control(FlowControl::Hardware)
            .open()?;

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
        let mut port = self.port.try_clone()?;
        let metadata = self.metadata.clone();

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
                   We read up to USB_READ_BUF_BYTES per `read()` into a reusable buffer and feed the
                   exact number of bytes returned to the accumulator. `port.read()` returns whatever
                   bytes are currently available (it does NOT wait to fill the buffer), so a larger
                   buffer only reduces syscall count without adding latency. The accumulator carries a
                   partial-sample remainder internally, so feeding any byte count is correctness-preserving.

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
                loop {
                    // Check whether the main thread has signaled
                    // us to stop
                    match sig_rx.try_recv() {
                        Ok(_) => return Ok(()),
                        Err(TryRecvError::Empty) => {}
                        Err(e) => return Err(e.into()),
                    }

                    // Now we read chunks and feed them to the accumulator
                    let n = port.read(&mut buf)?;
                    // Stamp the wall clock the INSTANT the read returns, before
                    // parsing and before anything is queued. This is the whole
                    // point of the field: it is an upper bound on the capture
                    // time of the samples in `buf[..n]` that excludes the mpsc
                    // queueing delay and the consumer's own scheduling. Doing
                    // this any later (in the consumer, on dequeue) folds in tens
                    // of milliseconds of scheduling jitter and destroys the
                    // estimate. Do NOT move this below `feed_into`.
                    let read_at = SystemTime::now();
                    missed += accumulator.feed_into(&buf[..n], &mut measurement_buf);
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
                    // per READ and forwarded to every chunk emitted from this
                    // iteration, so it neither contributes to nor is consumed by
                    // the `missed` sum above.
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
