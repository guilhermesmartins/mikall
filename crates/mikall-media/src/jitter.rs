//! A small reorder/jitter buffer per sender stream.
//!
//! Frames arrive keyed by their monotonic counter. `pop_next` releases them
//! in order once the buffer holds `depth` frames (or the next expected
//! frame is present), reporting gaps as `Missing` so the codec can run
//! packet-loss concealment instead of stalling.

use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Popped {
    /// The next in-order frame.
    Frame(Vec<u8>),
    /// The next slot never arrived (within the reorder window) — conceal.
    Missing,
    /// Nothing ready yet.
    Waiting,
}

#[derive(Debug)]
pub struct JitterBuffer {
    frames: BTreeMap<u64, Vec<u8>>,
    next: Option<u64>,
    /// How many frames ahead of the expected one we allow before declaring
    /// the expected frame lost.
    depth: u64,
    /// Frames older than the playhead are dropped (late arrivals).
    dropped_late: u64,
    concealed: u64,
}

impl JitterBuffer {
    pub fn new(depth: u64) -> Self {
        JitterBuffer {
            frames: BTreeMap::new(),
            next: None,
            depth: depth.max(1),
            dropped_late: 0,
            concealed: 0,
        }
    }

    pub fn push(&mut self, counter: u64, payload: Vec<u8>) {
        if let Some(next) = self.next {
            if counter < next {
                self.dropped_late += 1;
                return; // too late — the playhead has passed
            }
        }
        self.frames.insert(counter, payload);
    }

    pub fn pop_next(&mut self) -> Popped {
        let Some(&earliest) = self.frames.keys().next() else {
            return Popped::Waiting;
        };
        let next = *self.next.get_or_insert(earliest);
        if let Some(frame) = self.frames.remove(&next) {
            self.next = Some(next + 1);
            return Popped::Frame(frame);
        }
        // The expected frame is absent (earliest > next). Declare it lost —
        // and conceal — once enough is buffered ahead or the gap exceeds
        // the reorder window; otherwise keep waiting for it.
        if self.frames.len() as u64 >= self.depth || earliest - next > self.depth {
            self.next = Some(next + 1);
            self.concealed += 1;
            return Popped::Missing;
        }
        Popped::Waiting
    }

    pub fn concealed(&self) -> u64 {
        self.concealed
    }

    pub fn dropped_late(&self) -> u64 {
        self.dropped_late
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn frame(n: u8) -> Vec<u8> {
        vec![n]
    }

    #[test]
    fn in_order_stream_flows() {
        let mut jb = JitterBuffer::new(3);
        for n in 0..5u64 {
            jb.push(n, frame(n as u8));
        }
        for n in 0..5u8 {
            assert_eq!(jb.pop_next(), Popped::Frame(frame(n)));
        }
        assert_eq!(jb.pop_next(), Popped::Waiting);
    }

    #[test]
    fn reordered_frames_come_out_in_order() {
        let mut jb = JitterBuffer::new(3);
        jb.push(1, frame(1));
        jb.push(0, frame(0));
        jb.push(2, frame(2));
        assert_eq!(jb.pop_next(), Popped::Frame(frame(0)));
        assert_eq!(jb.pop_next(), Popped::Frame(frame(1)));
        assert_eq!(jb.pop_next(), Popped::Frame(frame(2)));
    }

    #[test]
    fn loss_is_concealed_after_window() {
        let mut jb = JitterBuffer::new(2);
        jb.push(0, frame(0));
        assert_eq!(jb.pop_next(), Popped::Frame(frame(0)));
        // frame 1 lost; 2 and 3 arrive
        jb.push(2, frame(2));
        jb.push(3, frame(3));
        assert_eq!(jb.pop_next(), Popped::Missing); // 1 concealed
        assert_eq!(jb.pop_next(), Popped::Frame(frame(2)));
        assert_eq!(jb.pop_next(), Popped::Frame(frame(3)));
        assert_eq!(jb.concealed(), 1);
    }

    #[test]
    fn late_frames_are_dropped() {
        let mut jb = JitterBuffer::new(2);
        jb.push(0, frame(0));
        assert_eq!(jb.pop_next(), Popped::Frame(frame(0)));
        jb.push(0, frame(0)); // duplicate of already-played frame
        assert_eq!(jb.dropped_late(), 1);
        assert_eq!(jb.pop_next(), Popped::Waiting);
    }

    #[test]
    fn waits_within_reorder_window() {
        let mut jb = JitterBuffer::new(3);
        jb.push(0, frame(0));
        assert_eq!(jb.pop_next(), Popped::Frame(frame(0)));
        jb.push(2, frame(2)); // 1 might still arrive
        assert_eq!(jb.pop_next(), Popped::Waiting);
        jb.push(1, frame(1)); // it does
        assert_eq!(jb.pop_next(), Popped::Frame(frame(1)));
        assert_eq!(jb.pop_next(), Popped::Frame(frame(2)));
    }
}
