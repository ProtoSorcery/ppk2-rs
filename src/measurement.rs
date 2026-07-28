//! Measurement parsing and preprocessing

use std::collections::VecDeque;

use crate::types::{LogicPortPins, Metadata};

const ADC_MULTIPLIER: f32 = 1.8 / 163840.;
const SPIKE_FILTER_ALPHA: f32 = 0.18;
const SPIKE_FILTER_ALPHA_5: f32 = 0.06;
const SPIKE_FILTER_SAMPLES: isize = 3;

#[derive(Debug)]
/// A single parsed measurement
pub struct Measurement {
    /// The measured current in mA.
    pub micro_amps: f32,
    /// Logic port bits
    pub pins: LogicPortPins,
}

struct AccumulatorState {
    rolling_avg_4: Option<f32>,
    rolling_avg: Option<f32>,
    prev_range: Option<usize>,
    after_spike: isize,
    consecutive_range_sample: usize,
    expected_counter: Option<u8>,
}

/// An acumulator for [Measurement]s. Keeps an internal state
/// as well as a byte buffer and builds [Measurement]s from bytes
/// that were fed. See [MeasurementAccumulator::feed_into] for more details.
pub struct MeasurementAccumulator {
    state: AccumulatorState,
    buf: Vec<u8>,
    metadata: Metadata,
}

impl MeasurementAccumulator {
    /// Create a new [MeasurementAccumulator], that uses the
    /// passed [Metadata] to parse the measurements. Make sure the
    /// [Metadata] is recent.
    pub fn new(metadata: Metadata) -> Self {
        Self {
            metadata,
            state: AccumulatorState {
                rolling_avg_4: None,
                rolling_avg: None,
                prev_range: None,
                after_spike: 0,
                consecutive_range_sample: 0,
                expected_counter: None,
            },
            buf: Vec::with_capacity(4096),
        }
    }

    /// Feed a number of bytes to the accumulator, pushing the [Result]s into the
    /// passed ring buffer.
    pub fn feed_into(&mut self, bytes: &[u8], buf: &mut VecDeque<Measurement>) -> usize {
        if bytes.is_empty() {
            return 0;
        }
        self.buf.extend_from_slice(bytes);
        let end = self.buf.len() - self.buf.len() % 4;
        let chunks = self.buf[..end]
            .chunks_exact(4)
            .map(|c| c.try_into().unwrap());
        let mut samples_missed = 0;
        for chunk in chunks {
            let raw = u32::from_le_bytes(chunk);
            let current_measurement_range = get_range(raw).min(4) as usize;
            let counter = get_counter(raw) as u8;

            let expected = self.state.expected_counter;
            self.state.expected_counter.replace((counter + 1) & 0x3F);
            if let Some(exp) = expected {
                if counter != exp {
                    // Number of samples skipped, mod 64
                    let gap = (counter.wrapping_sub(exp)) & 0x3F;
                    samples_missed += gap as usize;
                    continue;
                }
            }

            let adc_result = get_adc(raw) * 4;
            let pins = get_logic(raw).into();
            let micro_amps = get_adc_result(
                &self.metadata,
                &mut self.state,
                current_measurement_range,
                adc_result,
            ) * 10f32.powi(6);
            if self.state.expected_counter.is_none() {
                self.state.expected_counter.replace(counter);
            }

            buf.push_back(Measurement { micro_amps, pins })
        }
        self.buf.drain(..end);
        samples_missed
    }
}

fn get_adc_result(
    metadata: &Metadata,
    state: &mut AccumulatorState,
    range: usize,
    adc_val: u32,
) -> f32 {
    let modifiers = &metadata.modifiers;

    let result_without_gain: f32 =
        (adc_val as f32 - modifiers.o[range]) * (ADC_MULTIPLIER / modifiers.r[range]);
    let mut adc = modifiers.ug[range]
        * (result_without_gain * (modifiers.gs[range] * result_without_gain + modifiers.gi[range])
            + (modifiers.s[range] * (f32::from(metadata.vdd) / 1000.) + modifiers.i[range]));

    let prev_rolling_avg_4 = state.rolling_avg_4;
    let prev_rolling_avg = state.rolling_avg;

    state
        .rolling_avg
        .replace(if let Some(rolling_avg) = state.rolling_avg {
            SPIKE_FILTER_ALPHA * adc + (1. - SPIKE_FILTER_ALPHA) * rolling_avg
        } else {
            adc
        });

    state
        .rolling_avg_4
        .replace(if let Some(rolling_avg_4) = state.rolling_avg_4 {
            SPIKE_FILTER_ALPHA_5 * adc + (1. - SPIKE_FILTER_ALPHA_5) * rolling_avg_4
        } else {
            adc
        });

    state.prev_range.get_or_insert(range);

    if !matches!(state.prev_range, Some(r) if r == range) || state.after_spike > 0 {
        if matches!(state.prev_range, Some(r) if r == range) {
            state.consecutive_range_sample = 0;
            state.after_spike = SPIKE_FILTER_SAMPLES;
        } else {
            state.consecutive_range_sample += 1;
        }

        if range == 4 {
            if state.consecutive_range_sample < 2 {
                state.rolling_avg_4 = prev_rolling_avg_4;
                state.rolling_avg = prev_rolling_avg;
            }
            adc = state.rolling_avg_4.unwrap();
        } else {
            adc = state.rolling_avg.unwrap();
        }
        state.after_spike -= 1;
    }
    state.prev_range = Some(range);
    adc
}

/// Indicates whether a set of [Measurement]s matched.
///
/// Both variants carry a `missed` count so that a consumer can reconstruct a
/// correct timeline even when the device dropped samples. See the per-variant
/// docs for the exact units.
#[derive(Debug)]
pub enum MeasurementMatch {
    /// A set of [Measurement]s did match
    Match {
        /// The combined (averaged) measurement for this chunk.
        measurement: Measurement,
        /// Number of RAW device samples that were skipped (detected via a
        /// sample-counter gap) since the previous emitted [MeasurementMatch].
        ///
        /// **Units are raw device samples at `SPS_MAX`, not decimated output
        /// periods.** A consumer decimating by `chunk = SPS_MAX / sps` must
        /// divide by `chunk` to convert to output periods. A consumer that
        /// synthesizes timestamps from a sample index MUST advance that index
        /// by these skipped periods, or its timeline will run progressively
        /// early.
        missed: u32,
    },
    /// No matching [Measurement]s in the last chunk
    NoMatch {
        /// Number of RAW device samples that were skipped (detected via a
        /// sample-counter gap) since the previous emitted [MeasurementMatch].
        ///
        /// **Units are raw device samples at `SPS_MAX`, not decimated output
        /// periods.** A consumer decimating by `chunk = SPS_MAX / sps` must
        /// divide by `chunk` to convert to output periods. A consumer that
        /// synthesizes timestamps from a sample index MUST advance that index
        /// by these skipped periods, or its timeline will run progressively
        /// early.
        missed: u32,
    },
}

/// Saturating cast of an accumulated skipped-sample count to the wire width.
///
/// Never panics: counts beyond `u32::MAX` (which would require ~12 hours of a
/// fully-dropped 100 kHz stream between two emitted measurements) clamp instead.
fn saturating_missed(missed: usize) -> u32 {
    u32::try_from(missed).unwrap_or(u32::MAX)
}

/// Extension trait for VecDeque<Measurement>
pub trait MeasurementIterExt {
    /// Combine items into a single [MeasurementMatch::Match], if there are items.
    /// If there are none, [MeasurementMatch::NoMatch] is returned.
    /// Set combined logic port pin high if and only if more than half
    /// of the measurements indicate the pin was high
    ///
    /// `missed` is the number of RAW device samples that were skipped (detected
    /// via a sample-counter gap) since the previous emitted [MeasurementMatch].
    /// **Units are raw device samples at `SPS_MAX`, not decimated output
    /// periods.** A consumer decimating by `chunk = SPS_MAX / sps` must divide
    /// by `chunk` to convert to output periods. A consumer that synthesizes
    /// timestamps from a sample index MUST advance that index by these skipped
    /// periods, or its timeline will run progressively early. The value is
    /// propagated verbatim into the returned variant (saturating at
    /// [`u32::MAX`]), including on the empty-input path, so no pending count is
    /// ever silently dropped.
    fn combine(self, missed: usize) -> MeasurementMatch;

    /// Combine items with matching logic port state into a single [MeasurementMatch::Match],
    /// if there are items. If there are none, [MeasurementMatch::NoMatch] is returned.
    /// Set combined logic port pin high if and only if more than half
    /// of the measurements indicate the pin was high
    ///
    /// `missed` is the number of RAW device samples that were skipped (detected
    /// via a sample-counter gap) since the previous emitted [MeasurementMatch].
    /// **Units are raw device samples at `SPS_MAX`, not decimated output
    /// periods.** A consumer decimating by `chunk = SPS_MAX / sps` must divide
    /// by `chunk` to convert to output periods. A consumer that synthesizes
    /// timestamps from a sample index MUST advance that index by these skipped
    /// periods, or its timeline will run progressively early. The value is
    /// propagated verbatim into the returned variant (saturating at
    /// [`u32::MAX`]), including on the empty-input path, so no pending count is
    /// ever silently dropped.
    fn combine_matching(self, missed: usize, matching_pins: LogicPortPins) -> MeasurementMatch;
}

impl<I: Iterator<Item = Measurement>> MeasurementIterExt for I {
    fn combine(self, missed: usize) -> MeasurementMatch {
        let mut pin_high_count = [0usize; 8];
        let mut count = 0;
        let mut sum = 0f32;
        self.for_each(|m| {
            count += 1;
            sum += m.micro_amps;
            m.pins
                .inner()
                .iter()
                .enumerate()
                .filter(|(_, &p)| p.is_high())
                .for_each(|(i, _)| pin_high_count[i] += 1);
        });

        if count == 0 {
            // No measurements. The pending skipped-sample count MUST still be
            // reported here — dropping it on this path would silently lose
            // elapsed-but-sampleless time from the consumer's timeline.
            return MeasurementMatch::NoMatch {
                missed: saturating_missed(missed),
            };
        }

        // Set combined pin high if and only if more than half
        // of the measurements indicate the pin was high
        let mut pins = [false; 8];
        pin_high_count
            .into_iter()
            .enumerate()
            .filter(|(_, p)| *p > count / 2)
            .for_each(|(i, _)| pins[i] = true);
        // NOTE: this averaging denominator is deliberately left exactly as-is.
        // Subtracting `missed` from `count` is questionable (it inflates the
        // average), but changing it would alter measured current values, which
        // is out of scope for propagating the skipped-sample count.
        let avg = sum / count.saturating_sub(missed).max(1) as f32;

        MeasurementMatch::Match {
            measurement: Measurement {
                micro_amps: avg,
                pins: pins.into(),
            },
            missed: saturating_missed(missed),
        }
    }

    fn combine_matching(self, missed: usize, matching_pins: LogicPortPins) -> MeasurementMatch {
        let iter = self.filter(|m| {
            m.pins
                .inner()
                .iter()
                .enumerate()
                .all(|(i, l)| l.matches(matching_pins.inner()[i]))
        });
        iter.combine(missed)
    }
}

const fn generate_mask(bits: u32, pos: u32) -> u32 {
    (2u32.pow(bits) - 1) << pos
}

macro_rules! masked_value {
    ($name:ident, $bits:literal, $pos:literal) => {
        fn $name(raw: u32) -> u32 {
            (raw & generate_mask($bits, $pos)) >> $pos
        }
    };
}

masked_value!(get_adc, 14, 0);
masked_value!(get_range, 3, 14);
masked_value!(get_counter, 6, 18);
masked_value!(get_logic, 8, 24);

#[cfg(test)]
mod tests {
    use crate::{
        measurement::{get_adc_result, AccumulatorState},
        types::Metadata,
    };

    #[test]
    #[allow(clippy::excessive_precision)]
    pub fn test_get_adc_result() {
        let raw_metadata = r#"Calibrated: 0
R0: 1003.3506
R1: 101.5865
R2: 10.3027
R3: 0.9636
R4: 0.0564
GS0: 0.0000
GS1: 112.7890
GS2: 18.0115
GS3: 2.4217
GS4: 0.0729
GI0: 1.0000
GI1: 0.9695
GI2: 0.9609
GI3: 0.9519
GI4: 0.9582
O0: 112.9420
O1: 75.4627
O2: 64.6020
O3: 50.4983
O4: 87.2177
VDD: 3741
HW: 9173
mode: 2
S0: 0.000000048
S1: 0.000000596
S2: 0.000005281
S3: 0.000062577
S4: 0.002940743
I0: -0.000000104
I1: -0.000001443
I2: 0.000036439
I3: -0.000374119
I4: -0.009388455
UG0: 1.00
UG1: 1.00
UG2: 1.00
UG3: 1.00
UG4: 1.00
IA: 56
END
"#;
        let metadata =
            Metadata::from_bytes(raw_metadata.as_bytes()).expect("Error parsing metadata");

        let mut state = AccumulatorState {
            rolling_avg_4: Some(9.478947833765696e-8),
            rolling_avg: Some(1.0589385070753649e-7),
            prev_range: Some(0),
            after_spike: 0,
            consecutive_range_sample: 0,
            expected_counter: Some(62),
        };
        let range: usize = 0;
        let adc_val: u32 = 108;
        let adc_result = get_adc_result(&metadata, &mut state, range, adc_val) * 10f32.powi(6);

        // JS result: 0.021454880761611544
        assert!((adc_result - 0.021454880761611544).abs() < f32::EPSILON)
    }
}
