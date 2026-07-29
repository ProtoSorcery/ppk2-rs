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
pub struct Ppk2 {
    port: Box<dyn SerialPort>,
    metadata: Metadata,
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

    /// Start measurements. Returns a tuple of:
    /// - [Ppk2<Measuring>],
    /// - [Receiver] of [measurement::MeasurementMatch], and
    /// - A closure that can be called to stop the measurement parsing pipeline and return the
    ///   device.
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
                        meas_tx.send(measurement)?;
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

        let stop = move || {
            sig_tx.send(())?;
            t.join().expect("Data receive thread panicked")?;
            self.send_command(Command::AverageStop)?;
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
