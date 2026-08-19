//! Real audio devices via cpal (feature `hardware-audio`).
//!
//! `cpal::Stream` is not `Send`, so each device lives on its own named
//! thread that builds the stream, reports success or failure back to the
//! caller, then parks until the keepalive handle drops. The capture
//! callback pushes canonical 48 kHz mono frames (see [`crate::pcm`]) into
//! a channel the async sender loop drains; the playback callback drains a
//! shared queue the receiver's [`AudioSink`] fills. All format shaping
//! (downmix, resample, chunking) is the pure code in [`crate::pcm`] —
//! this module is only device glue.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{FromSample, SampleFormat, SizedSample};
use tokio::sync::mpsc;

use crate::engine::{AudioSink, FRAME_SAMPLES};
use crate::pcm::{self, FrameChunker, Resampler, PIPELINE_RATE};

/// Playback latency cap: samples queued beyond this are dropped oldest-first
/// so a stalled device can never grow unbounded delay (400 ms at 48 kHz).
const MAX_QUEUED_SAMPLES: usize = 19_200;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HardwareError {
    #[error("no default {0} device")]
    NoDevice(&'static str),
    #[error("{0} device configuration failed: {1}")]
    Config(&'static str, String),
    #[error("unsupported {0} sample format {1}")]
    UnsupportedFormat(&'static str, String),
    #[error("{0} stream failed to start: {1}")]
    Stream(&'static str, String),
}

/// Keepalive for a device thread: dropping it stops the stream and joins
/// the thread — no device stays open past its call.
#[derive(Debug)]
struct DeviceThread {
    stop: Option<std::sync::mpsc::Sender<()>>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl DeviceThread {
    /// Run `build` on a fresh thread, wait for it to report the stream up
    /// (or why not), and keep the thread parked holding the stream.
    fn spawn(
        name: &'static str,
        build: impl FnOnce() -> Result<cpal::Stream, HardwareError> + Send + 'static,
    ) -> Result<Self, HardwareError> {
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<(), HardwareError>>();
        let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
        let join = std::thread::Builder::new()
            .name(format!("mikall-{name}"))
            .spawn(move || {
                let stream = match build() {
                    Ok(stream) => {
                        let _ = ready_tx.send(Ok(()));
                        stream
                    }
                    Err(error) => {
                        let _ = ready_tx.send(Err(error));
                        return;
                    }
                };
                // Park holding the stream; any result (stop signal or the
                // handle dropping) releases it.
                let _ = stop_rx.recv();
                drop(stream);
            })
            .map_err(|e| HardwareError::Stream(name, e.to_string()))?;
        match ready_rx.recv_timeout(std::time::Duration::from_secs(5)) {
            Ok(Ok(())) => Ok(DeviceThread {
                stop: Some(stop_tx),
                join: Some(join),
            }),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(HardwareError::Stream(name, "device did not start".into())),
        }
    }
}

impl Drop for DeviceThread {
    fn drop(&mut self) {
        drop(self.stop.take()); // closes the channel; recv() unblocks
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

/// The default microphone as a stream of 48 kHz mono pipeline frames.
/// Dropping the handle stops capture and closes the frame channel, which
/// ends the sender loop.
#[derive(Debug)]
pub struct CpalMic {
    _thread: DeviceThread,
}

impl CpalMic {
    pub fn start() -> Result<(Self, mpsc::Receiver<Vec<i16>>), HardwareError> {
        let (frame_tx, frame_rx) = mpsc::channel::<Vec<i16>>(64);
        let thread = DeviceThread::spawn("mic", move || build_input_stream(frame_tx))?;
        Ok((CpalMic { _thread: thread }, frame_rx))
    }
}

fn build_input_stream(frame_tx: mpsc::Sender<Vec<i16>>) -> Result<cpal::Stream, HardwareError> {
    let device = cpal::default_host()
        .default_input_device()
        .ok_or(HardwareError::NoDevice("input"))?;
    let supported = device
        .default_input_config()
        .map_err(|e| HardwareError::Config("input", e.to_string()))?;
    let sample_format = supported.sample_format();
    let config: cpal::StreamConfig = supported.config();
    let channels = config.channels as usize;
    let rate = config.sample_rate.0;
    tracing::info!(
        device = device.name().unwrap_or_else(|_| "?".into()),
        rate,
        channels,
        format = %sample_format,
        "voice: microphone capture starting"
    );
    let resampler = Resampler::new(rate, PIPELINE_RATE);
    match sample_format {
        SampleFormat::F32 => input_stream::<f32>(&device, &config, channels, resampler, frame_tx),
        SampleFormat::I16 => input_stream::<i16>(&device, &config, channels, resampler, frame_tx),
        SampleFormat::U16 => input_stream::<u16>(&device, &config, channels, resampler, frame_tx),
        SampleFormat::I32 => input_stream::<i32>(&device, &config, channels, resampler, frame_tx),
        SampleFormat::F64 => input_stream::<f64>(&device, &config, channels, resampler, frame_tx),
        other => Err(HardwareError::UnsupportedFormat("input", other.to_string())),
    }
}

fn input_stream<T>(
    device: &cpal::Device,
    config: &cpal::StreamConfig,
    channels: usize,
    mut resampler: Resampler,
    frame_tx: mpsc::Sender<Vec<i16>>,
) -> Result<cpal::Stream, HardwareError>
where
    T: SizedSample,
    f32: FromSample<T>,
{
    let mut chunker = FrameChunker::default();
    let mut resampled: Vec<f32> = Vec::with_capacity(FRAME_SAMPLES * 2);
    let stream = device
        .build_input_stream(
            config,
            move |data: &[T], _: &cpal::InputCallbackInfo| {
                let floats: Vec<f32> = data.iter().map(|s| f32::from_sample_(*s)).collect();
                let mono = pcm::downmix(&floats, channels);
                resampled.clear();
                resampler.push(&mono, &mut resampled);
                chunker.push(&resampled, |frame| {
                    // Never block the audio callback; a full channel means
                    // the sender stalled and the frame is best dropped.
                    let _ = frame_tx.try_send(frame);
                });
            },
            |error| tracing::warn!(%error, "voice: microphone stream error"),
            None,
        )
        .map_err(|e| HardwareError::Stream("input", e.to_string()))?;
    stream
        .play()
        .map_err(|e| HardwareError::Stream("input", e.to_string()))?;
    Ok(stream)
}

/// The default output device, fed through a bounded shared queue.
/// Dropping the handle stops playback; the paired [`SpeakerSink`] then
/// just accumulates into (and trims) a queue nobody drains.
#[derive(Debug)]
pub struct CpalSpeaker {
    _thread: DeviceThread,
}

/// The receiver-side [`AudioSink`]: pushes decoded 48 kHz mono PCM into
/// the queue the output callback drains. Bounded — audio older than the
/// latency cap is dropped rather than played late.
#[derive(Debug, Clone)]
pub struct SpeakerSink {
    queue: Arc<Mutex<VecDeque<i16>>>,
}

impl AudioSink for SpeakerSink {
    fn play(&mut self, pcm: &[i16]) {
        if let Ok(mut queue) = self.queue.lock() {
            queue.extend(pcm.iter().copied());
            let excess = queue.len().saturating_sub(MAX_QUEUED_SAMPLES);
            if excess > 0 {
                queue.drain(..excess);
                tracing::debug!(
                    excess,
                    "voice: playback queue over latency cap, dropped oldest"
                );
            }
        }
    }
}

impl CpalSpeaker {
    pub fn start() -> Result<(Self, SpeakerSink), HardwareError> {
        let queue: Arc<Mutex<VecDeque<i16>>> = Arc::new(Mutex::new(VecDeque::new()));
        let sink = SpeakerSink {
            queue: Arc::clone(&queue),
        };
        let thread = DeviceThread::spawn("speaker", move || build_output_stream(queue))?;
        Ok((CpalSpeaker { _thread: thread }, sink))
    }
}

fn build_output_stream(queue: Arc<Mutex<VecDeque<i16>>>) -> Result<cpal::Stream, HardwareError> {
    let device = cpal::default_host()
        .default_output_device()
        .ok_or(HardwareError::NoDevice("output"))?;
    let supported = device
        .default_output_config()
        .map_err(|e| HardwareError::Config("output", e.to_string()))?;
    let sample_format = supported.sample_format();
    let config: cpal::StreamConfig = supported.config();
    let channels = config.channels as usize;
    let rate = config.sample_rate.0;
    tracing::info!(
        device = device.name().unwrap_or_else(|_| "?".into()),
        rate,
        channels,
        format = %sample_format,
        "voice: speaker playback starting"
    );
    let resampler = Resampler::new(PIPELINE_RATE, rate);
    match sample_format {
        SampleFormat::F32 => output_stream::<f32>(&device, &config, channels, resampler, queue),
        SampleFormat::I16 => output_stream::<i16>(&device, &config, channels, resampler, queue),
        SampleFormat::U16 => output_stream::<u16>(&device, &config, channels, resampler, queue),
        SampleFormat::I32 => output_stream::<i32>(&device, &config, channels, resampler, queue),
        SampleFormat::F64 => output_stream::<f64>(&device, &config, channels, resampler, queue),
        other => Err(HardwareError::UnsupportedFormat(
            "output",
            other.to_string(),
        )),
    }
}

fn output_stream<T>(
    device: &cpal::Device,
    config: &cpal::StreamConfig,
    channels: usize,
    mut resampler: Resampler,
    queue: Arc<Mutex<VecDeque<i16>>>,
) -> Result<cpal::Stream, HardwareError>
where
    T: SizedSample + FromSample<f32>,
{
    // Device-rate mono samples staged between callbacks (the resampler
    // does not emit exactly what one callback needs).
    let mut staged: VecDeque<f32> = VecDeque::new();
    let stream = device
        .build_output_stream(
            config,
            move |data: &mut [T], _: &cpal::OutputCallbackInfo| {
                let frames_needed = data.len() / channels.max(1);
                while staged.len() < frames_needed {
                    // Feed the resampler one pipeline frame from the queue;
                    // an empty queue (underrun / nobody talking) is silence.
                    let mut chunk = [0.0f32; FRAME_SAMPLES];
                    if let Ok(mut queue) = queue.lock() {
                        for slot in chunk.iter_mut() {
                            match queue.pop_front() {
                                Some(sample) => *slot = pcm::i16_to_f32(sample),
                                None => break,
                            }
                        }
                    }
                    let mut out = Vec::with_capacity(FRAME_SAMPLES + 8);
                    resampler.push(&chunk, &mut out);
                    staged.extend(out);
                }
                for frame in data.chunks_mut(channels.max(1)) {
                    let sample = staged.pop_front().unwrap_or(0.0);
                    let converted = T::from_sample_(sample);
                    for slot in frame.iter_mut() {
                        *slot = converted;
                    }
                }
            },
            |error| tracing::warn!(%error, "voice: speaker stream error"),
            None,
        )
        .map_err(|e| HardwareError::Stream("output", e.to_string()))?;
    stream
        .play()
        .map_err(|e| HardwareError::Stream("output", e.to_string()))?;
    Ok(stream)
}
