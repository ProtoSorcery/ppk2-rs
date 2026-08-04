//! Hardware-in-the-loop regression test for the "stop closure eats the device"
//! defect.
//!
//! This test is `#[ignore]` by default because it requires a physical Nordic
//! Power Profiler Kit 2 (VID 0x1915 / PID 0xc00a) connected over USB. Run with:
//!
//! ```text
//! cargo test --test stop_returns_device_after_stream_error -- --ignored --nocapture
//! ```
//!
//! # The defect
//!
//! `start_measurement_matching` consumes the `Ppk2` and hands back a `FnOnce`
//! stop closure as the ONLY route to the handle. If the caller drops the
//! measurement `Receiver` before stopping, the worker's next `meas_tx.send`
//! fails; the old stop closure propagated that with `?`, returned `Err`, and —
//! being a `FnOnce` — destroyed the device with it. The caller was then unable
//! to disable device power or close the port, which can leave a DUT energized.
//!
//! # What this asserts
//!
//! 1. `stop()` returns `Ok(ppk2)` even though the stream ended abnormally.
//! 2. The returned handle is genuinely usable for the safety action the caller
//!    needs it for: disabling device power.
//!
//! It deliberately does NOT assert on `take_last_worker_error()`, because
//! whether receiver-drop is classified as a worker error or as a benign
//! shutdown is a separate policy decision; both are correct here and either
//! must still yield the device. The observed value is printed instead.

use std::sync::mpsc::RecvTimeoutError;
use std::thread::sleep;
use std::time::Duration;

use anyhow::{anyhow, Result};
use ppk2::{
    try_find_ppk2_port,
    types::{DevicePower, MeasurementMode},
    Error as Ppk2Error, Ppk2,
};

// Ampere mode leaves the source meter disabled — safest when no DUT is wired up.
const TEST_MODE: MeasurementMode = MeasurementMode::Ampere;

#[test]
#[ignore = "requires a physical PPK2; run with `cargo test -- --ignored`"]
fn stop_returns_device_after_receiver_dropped() -> Result<()> {
    // Skip cleanly if there is no PPK2 attached, matching the convention in
    // tests/recover_from_stuck_stream.rs.
    let port = match try_find_ppk2_port() {
        Ok(p) => p,
        Err(Ppk2Error::Ppk2NotFound) => {
            eprintln!("SKIP: no PPK2 attached (try_find_ppk2_port returned Ppk2NotFound)");
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };
    eprintln!("found PPK2 at {port}");

    let ppk2 = Ppk2::new(port.as_str(), TEST_MODE)?;
    let (rx, stop) = ppk2.start_measurement(1000)?;

    // Prove the stream is actually live before breaking it, so a later failure
    // can't be blamed on the measurement never having started.
    match rx.recv_timeout(Duration::from_secs(2)) {
        Ok(_) => eprintln!("stream is live; received a sample"),
        Err(RecvTimeoutError::Timeout) => {
            let _ = stop();
            return Err(anyhow!("no sample within 2s; stream never started"));
        }
        Err(RecvTimeoutError::Disconnected) => {
            let _ = stop();
            return Err(anyhow!(
                "measurement channel disconnected before the test began"
            ));
        }
    }

    // Reproduce the trigger: drop the Receiver while the worker is still
    // running, then give it time to attempt a send and discover the drop. At
    // 1000 sps the worker emits ~1000 times/sec, so 250 ms is ample.
    drop(rx);
    sleep(Duration::from_millis(250));

    // THE REGRESSION: this used to return Err and consume the device forever.
    let mut ppk2 = stop().map_err(|e| {
        anyhow!("stop() refused to return the device after an abnormal stream end: {e}")
    })?;

    eprintln!(
        "stop() returned the device; last_worker_error = {:?}",
        ppk2.take_last_worker_error()
    );

    // The whole point of getting the handle back: put the hardware in a safe
    // state. This is a write-only command, so it does not depend on the port
    // being drained of stale sample bytes.
    ppk2.set_device_power(DevicePower::Disabled)
        .map_err(|e| anyhow!("recovered device was not usable to disable device power: {e}"))?;

    Ok(())
}
