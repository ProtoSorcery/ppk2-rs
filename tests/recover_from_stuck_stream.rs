//! Hardware-in-the-loop integration tests covering two distinct "PPK2 left
//! streaming" failure modes.
//!
//! These tests are `#[ignore]` by default because they require a physical
//! Nordic Power Profiler Kit 2 (VID 0x1915 / PID 0xc00a) connected over USB.
//! Run with:
//!
//! ```text
//! cargo test --test recover_from_stuck_stream -- --ignored --nocapture
//! ```
//!
//! The two tests cover orthogonal scenarios:
//!
//! - [`recover_from_stuck_stream`] — the crashed-client case. A subprocess
//!   opens the PPK2, starts streaming, and calls `std::process::abort()`,
//!   bypassing `Drop` entirely. The firmware is left in `AverageStart` with
//!   no Rust-side cleanup. The parent then **witnesses** that the device is
//!   actually flooding the port with measurement frames before invoking
//!   `Ppk2::new`, which must drain and quiesce the device to succeed. This is
//!   the true test of the recovery-in-`new` code path.
//!
//! - [`drop_impl_quiesces_device_on_stop_fn_drop`] — the in-process unwind
//!   case. The caller drops the `stop_fn` closure without calling it (e.g. a
//!   panic unwinds past it). `Drop for Ppk2` fires synchronously and must
//!   write `AverageStop` so the device is quiesced before the next
//!   `Ppk2::new`. This test exercises the `Drop` impl, not recovery-in-`new`:
//!   by the time the reopen happens, the device is already idle.
//!
//! Exit codes from the child subprocess:
//! - abort (SIGABRT / non-zero) — expected, the parent proceeds to recovery.
//! - 2                           — child couldn't find the PPK2; parent skips.

use std::io::Read;
use std::process::{Command, Stdio};
use std::sync::mpsc::RecvTimeoutError;
use std::sync::Mutex;
use std::thread::sleep;
use std::time::Duration;

use anyhow::{anyhow, Result};
use ppk2::{try_find_ppk2_port, types::MeasurementMode, Error as Ppk2Error, Ppk2};

const CHILD_ENV: &str = "PPK2_CRASHER_CHILD";
const TEST_NAME: &str = "recover_from_stuck_stream";
// Ampere mode leaves the source meter disabled — safest when no DUT is wired up.
const TEST_MODE: MeasurementMode = MeasurementMode::Ampere;

// Cargo runs integration tests in parallel threads within one binary. Both
// hardware tests below talk to the same PPK2, so they must serialize. This
// lock applies only to the parent-process test bodies — the child process
// spawned by `recover_from_stuck_stream` lives in a different address space
// and does not participate.
static HARDWARE_LOCK: Mutex<()> = Mutex::new(());

#[test]
#[ignore = "requires a physical PPK2; run with `cargo test -- --ignored`"]
fn recover_from_stuck_stream() -> Result<()> {
    if std::env::var(CHILD_ENV).is_ok() {
        run_child()
    } else {
        run_parent()
    }
}

/// Parent branch: spawn the child, verify it crashed, then reopen the device.
fn run_parent() -> Result<()> {
    // Serialize against the sibling hardware test running in the same binary.
    let _guard = HARDWARE_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());

    // Skip cleanly if there is no PPK2 attached. Devs running `--ignored` on a
    // machine without hardware shouldn't get a confusing failure.
    let port = match try_find_ppk2_port() {
        Ok(p) => p,
        Err(Ppk2Error::Ppk2NotFound) => {
            eprintln!("SKIP: no PPK2 attached (try_find_ppk2_port returned Ppk2NotFound)");
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };
    eprintln!("parent: found PPK2 at {port}");

    // Spawn ourselves with the child env var + an --exact filter so the child
    // only runs this test and nothing else in the binary.
    let exe = std::env::current_exe()?;
    eprintln!("parent: spawning child: {}", exe.display());
    let status = Command::new(&exe)
        .env(CHILD_ENV, "1")
        .args([TEST_NAME, "--exact", "--ignored", "--nocapture"])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()?;

    // The child is supposed to call std::process::abort(), which yields a
    // non-zero / signalled exit. A clean exit would mean the abort path was
    // skipped, invalidating the whole test premise.
    if status.success() {
        return Err(anyhow!(
            "child exited cleanly ({status:?}); expected abort() — the bug scenario was not reproduced"
        ));
    }
    if status.code() == Some(2) {
        eprintln!("SKIP: child could not find a PPK2 (exit code 2)");
        return Ok(());
    }
    eprintln!("parent: child exited as expected: {status:?}");

    // Give the OS a moment to fully release the USB-CDC device node.
    // macOS in particular lingers briefly after process death.
    sleep(Duration::from_millis(300));

    // Confirm the device is actually stuck streaming. A crashed-but-quiesced
    // device would return 0 bytes here; a stuck device floods the port with
    // 4-byte measurement frames.
    let mut witness_bytes = 0usize;
    for attempt in 1..=3 {
        match serialport::new(&port, 9600)
            .timeout(Duration::from_millis(50))
            .flow_control(serialport::FlowControl::Hardware)
            .open()
        {
            Ok(mut raw) => {
                // Read for ~100ms total. At 100_000 samples/sec * 4 bytes/sample
                // we'd expect ~40_000 bytes if truly streaming; any nonzero count
                // is sufficient proof.
                let deadline = std::time::Instant::now() + Duration::from_millis(100);
                let mut buf = [0u8; 256];
                while std::time::Instant::now() < deadline {
                    match raw.read(&mut buf) {
                        Ok(0) => break,
                        Ok(n) => witness_bytes += n,
                        Err(e)
                            if e.kind() == std::io::ErrorKind::TimedOut
                                || e.kind() == std::io::ErrorKind::WouldBlock =>
                        {
                            break
                        }
                        Err(e) => return Err(anyhow!("witness read failed: {e}")),
                    }
                }
                drop(raw);
                break;
            }
            Err(e) if attempt < 3 => {
                eprintln!("parent: witness open attempt {attempt} busy ({e}), retrying...");
                sleep(Duration::from_millis(200));
            }
            Err(e) => return Err(e.into()),
        }
    }
    eprintln!("parent: witness observed {witness_bytes} stale bytes from device");
    assert!(
        witness_bytes > 0,
        "device was NOT streaming after child abort — test premise violated, \
         recovery-in-new is not actually being exercised"
    );

    // Let the OS fully release the port before Ppk2::new opens it.
    sleep(Duration::from_millis(200));

    // Core assertion: we can reopen the device despite it still streaming.
    // Retry only the port-open step (not the recovery assertion itself) to
    // absorb residual OS-level delay releasing the CDC node on macOS.
    let mut ppk2 = None;
    let mut last_err: Option<Ppk2Error> = None;
    for attempt in 0..3 {
        match Ppk2::new(port.clone(), TEST_MODE) {
            Ok(dev) => {
                eprintln!("parent: reopened PPK2 on attempt {}", attempt + 1);
                ppk2 = Some(dev);
                break;
            }
            Err(e) => {
                eprintln!("parent: reopen attempt {} failed: {e:?}", attempt + 1);
                last_err = Some(e);
                sleep(Duration::from_millis(200));
            }
        }
    }
    let ppk2 = ppk2.ok_or_else(|| {
        anyhow!(
            "failed to reopen PPK2 after child crash: {:?}",
            last_err.expect("loop ran at least once")
        )
    })?;

    // The device should be fully usable: start a fresh measurement session,
    // read at least one sample, and shut down cleanly.
    let (rx, stop) = ppk2.start_measurement(1000)?;
    match rx.recv_timeout(Duration::from_secs(2)) {
        Ok(_) => eprintln!("parent: received sample after recovery"),
        Err(RecvTimeoutError::Timeout) => {
            let _ = stop();
            return Err(anyhow!("no sample received within 2s after recovery"));
        }
        Err(RecvTimeoutError::Disconnected) => {
            let _ = stop();
            return Err(anyhow!("measurement channel disconnected after recovery"));
        }
    }
    let _ppk2 = stop()?;
    // _ppk2 is dropped here; with the companion fix in src/lib.rs, Drop should
    // cleanly stop streaming.
    Ok(())
}

/// Companion to [`recover_from_stuck_stream`]. That test uses a subprocess +
/// `abort()` to simulate OS-level process death, where `Drop` cannot run, and
/// it genuinely exercises the recovery path inside `Ppk2::new` (proven by a
/// witness step that observes stale streaming bytes before reopen).
///
/// This test covers a different failure mode: the in-process caller unwinding
/// that drops the stop closure without calling it. In that path `Drop for
/// Ppk2` *does* run synchronously and writes `AverageStop`, so the device is
/// **already quiesced** before the next `Ppk2::new` opens the port. That
/// means this test exercises the `Drop` impl, not recovery-in-`new` — hence
/// the name. Together the two tests cover both failure modes of "PPK2 left
/// streaming".
#[test]
#[ignore = "requires a physical PPK2; run with `cargo test -- --ignored`"]
fn drop_impl_quiesces_device_on_stop_fn_drop() -> Result<()> {
    // Serialize against the sibling hardware test running in the same binary.
    let _guard = HARDWARE_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());

    // Step 1: locate hardware or skip cleanly.
    let port = match try_find_ppk2_port() {
        Ok(p) => p,
        Err(Ppk2Error::Ppk2NotFound) => {
            eprintln!("SKIP: no PPK2 attached (try_find_ppk2_port returned Ppk2NotFound)");
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };
    eprintln!("drop-impl: found PPK2 at {port}");

    // Step 2-3: open and start streaming.
    let ppk2 = Ppk2::new(&*port, TEST_MODE)?;
    let (rx, stop_fn) = ppk2.start_measurement(1000)?;

    // Step 4: let the firmware settle firmly into streaming mode with frames
    // in flight, so the dropped-stop-fn path actually has something to clean up.
    sleep(Duration::from_millis(300));

    // Step 5: deliberately drop stop_fn WITHOUT calling it, simulating a
    // caller panic unwinding past the closure. The FnOnce captures self; when
    // it falls out of scope, Drop for Ppk2 must fire and write AverageStop.
    // Order here is independent (the two are unrelated values), but we drop
    // stop_fn first to make the Drop-triggering intent clear in the code.
    drop(stop_fn);
    drop(rx);

    // Step 6: give the AverageStop byte time to reach firmware and let any
    // trailing 4-byte frames drain from the OS RX buffer before we reopen.
    sleep(Duration::from_millis(200));

    // Step 7: reopen with the same 3x/200ms retry pattern the sibling test
    // uses, to absorb macOS USB-CDC device-node lingering.
    let mut reopened = None;
    let mut last_err: Option<Ppk2Error> = None;
    for attempt in 0..3 {
        match Ppk2::new(port.clone(), TEST_MODE) {
            Ok(dev) => {
                eprintln!(
                    "drop-impl: reopened PPK2 on attempt {}",
                    attempt + 1
                );
                reopened = Some(dev);
                break;
            }
            Err(e) => {
                eprintln!(
                    "drop-impl: reopen attempt {} failed: {e:?}",
                    attempt + 1
                );
                last_err = Some(e);
                sleep(Duration::from_millis(200));
            }
        }
    }

    // Step 8: self-debugging assertion — if this ever fails, the message
    // points at the two mechanisms that have to cooperate.
    let ppk2 = match reopened {
        Some(dev) => dev,
        None => {
            let err = last_err.expect("loop ran at least once");
            panic!(
                "reopen after dropped stop_fn failed: {err} \
                 — did Drop for Ppk2 fire, and did the recovery in Ppk2::new drain the port?"
            );
        }
    };

    // Step 9: confirm the reopened device is actually usable end-to-end.
    let (rx, stop) = ppk2.start_measurement(1000)?;
    match rx.recv_timeout(Duration::from_secs(2)) {
        Ok(_) => eprintln!("drop-impl: received sample after recovery"),
        Err(RecvTimeoutError::Timeout) => {
            let _ = stop();
            return Err(anyhow!("no sample received within 2s after recovery"));
        }
        Err(RecvTimeoutError::Disconnected) => {
            let _ = stop();
            return Err(anyhow!("measurement channel disconnected after recovery"));
        }
    }
    let _ppk2 = stop()?;
    // _ppk2 dropped here — Drop quiesces streaming.
    Ok(())
}

/// Child branch: open the device, start streaming, and abort without cleanup.
fn run_child() -> Result<()> {
    let port = match try_find_ppk2_port() {
        Ok(p) => p,
        Err(_) => {
            eprintln!("child: no PPK2 attached, signalling skip via exit code 2");
            std::process::exit(2);
        }
    };
    eprintln!("child: opening PPK2 at {port}");
    let ppk2 = Ppk2::new(port, TEST_MODE)?;
    let (_rx, _stop) = ppk2.start_measurement(1000)?;
    // Let the firmware get firmly into streaming mode before we bail.
    sleep(Duration::from_millis(500));
    eprintln!("child: aborting to simulate crash (Drop and stop() will NOT run)");
    // abort() skips destructors — exactly what reproduces the bug. Using
    // exit() would run some drop glue and mask the scenario on some setups.
    std::process::abort();
}
