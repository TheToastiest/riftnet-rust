// crates/protocol/src/protocol.rs
use crate::packet::ReliabilityPacketHeader;
use std::time::{Instant, Duration};

// 1. Annotate UnackedPacket with PartialEq
#[derive(Debug, Clone)]
pub struct UnackedPacket {
    pub sequence: u16,
    pub time_sent: Instant,
    pub data: Vec<u8>,
    pub retries: u32,
}

// 2. Manually implement PartialEq for UnackedPacket, ignoring Instant
impl PartialEq for UnackedPacket {
    fn eq(&self, other: &Self) -> bool {
        self.sequence == other.sequence &&
            self.data == other.data &&
            self.retries == other.retries
    }
}

// 3. Now derive PartialEq on ReliableConnectionState
#[derive(Debug, PartialEq, Clone)]
pub struct ReliableConnectionState {
    pub next_outgoing_sequence: u16,
    pub highest_received_sequence: u16,
    pub received_sequence_bitfield: u32,
    pub smoothed_rtt_ms: f32,
    pub rtt_variance_ms: f32,
    pub retransmission_timeout_ms: f32,
    pub is_first_rtt_sample: bool,
    pub unacknowledged_packets: Vec<UnackedPacket>,
    pub last_packet_received_time: Instant,
    pub has_pending_ack_to_send: bool,
    pub last_ack_flush_time: Option<Instant>,
    pub stats_retransmits: u32,
    pub stats_ack_only_sent: u32,
}

impl ReliableConnectionState {
    pub fn new() -> Self {
        Self {
            next_outgoing_sequence: 1,
            highest_received_sequence: 0,
            received_sequence_bitfield: 0,
            smoothed_rtt_ms: 0.0,
            rtt_variance_ms: 0.0,
            retransmission_timeout_ms: 100.0, // Default start RTO
            is_first_rtt_sample: true,
            unacknowledged_packets: Vec::new(),
            last_packet_received_time: Instant::now(),
            has_pending_ack_to_send: false,
            last_ack_flush_time: None,
            stats_retransmits: 0,
            stats_ack_only_sent: 0,
        }
    }

    pub fn is_sequence_more_recent(s1: u16, s2: u16) -> bool {
        const HALF_RANGE: u16 = 0x8000;
        ((s1 > s2) && (s1 - s2 < HALF_RANGE)) || ((s2 > s1) && (s2.wrapping_sub(s1) > HALF_RANGE))
    }

    fn apply_rtt_sample(&mut self, sample_rtt_ms: f32) {
        const RTT_ALPHA: f32 = 0.125;
        const RTT_BETA: f32 = 0.25;
        const RTO_K: f32 = 4.0;
        const MIN_RTO_MS: f32 = 30.0;
        const MAX_RTO_MS: f32 = 500.0;

        if self.is_first_rtt_sample {
            self.smoothed_rtt_ms = sample_rtt_ms;
            self.rtt_variance_ms = sample_rtt_ms * 0.5;
            self.is_first_rtt_sample = false;
        } else {
            let delta = sample_rtt_ms - self.smoothed_rtt_ms;
            self.smoothed_rtt_ms += RTT_ALPHA * delta;
            self.rtt_variance_ms += RTT_BETA * (delta.abs() - self.rtt_variance_ms);
        }

        self.retransmission_timeout_ms = (self.smoothed_rtt_ms + RTO_K * self.rtt_variance_ms)
            .clamp(MIN_RTO_MS, MAX_RTO_MS);
    }

    /// Processes incoming header, clears unacked packets, calculates RTT, and updates receive window.
    /// Returns `true` if the packet is new and should be processed, `false` if it's a duplicate/too old.
    pub fn process_incoming_header(&mut self, header: &ReliabilityPacketHeader, now: Instant) -> bool {
        self.last_packet_received_time = now;

        // 1. Accumulator for RTT samples to avoid borrow checker overlap
        let mut rtt_samples_to_apply = Vec::new();

        // --- 2. Process Acknowledgments ---
        self.unacknowledged_packets.retain_mut(|pkt| {
            let mut acked = false;

            if pkt.sequence == header.ack {
                acked = true;
            } else if Self::is_sequence_more_recent(header.ack, pkt.sequence) {
                let diff = header.ack.wrapping_sub(pkt.sequence);
                if diff > 0 && diff <= 32 {
                    if ((header.ack_bitfield >> (diff - 1)) & 1) == 1 {
                        acked = true;
                    }
                }
            }

            if acked {
                if pkt.retries == 0 {
                    let rtt_ms = now.duration_since(pkt.time_sent).as_secs_f32() * 1000.0;
                    // Push to the local vector instead of calling self.apply_rtt_sample
                    rtt_samples_to_apply.push(rtt_ms);
                }
                false // Remove from unacknowledged_packets
            } else {
                true // Keep in unacknowledged_packets
            }
        });

        // 3. Apply the collected RTT samples safely outside the borrow
        for rtt in rtt_samples_to_apply {
            self.apply_rtt_sample(rtt);
        }

        // --- 4. Update Receive Window ---
        if Self::is_sequence_more_recent(header.sequence, self.highest_received_sequence) {
            let diff = header.sequence.wrapping_sub(self.highest_received_sequence);
            self.received_sequence_bitfield = self.received_sequence_bitfield.checked_shl(diff as u32).unwrap_or(0);
            self.received_sequence_bitfield |= 1;
            self.highest_received_sequence = header.sequence;
        } else {
            let diff = self.highest_received_sequence.wrapping_sub(header.sequence);
            if diff == 0 || diff > 32 {
                return false; // Duplicate or out of window
            }
            if ((self.received_sequence_bitfield >> diff) & 1) == 1 {
                return false; // Duplicate bit already set
            }
            self.received_sequence_bitfield |= 1 << diff;
        }

        self.has_pending_ack_to_send = true;
        true
    }

    /// Checks the unacknowledged queue against the RTO and returns a vector of payloads to resend.
    pub fn process_retransmissions(&mut self, now: Instant) -> Vec<Vec<u8>> {
        const MAX_BACKOFF_MS: f32 = 2000.0;
        let mut resend_queue = Vec::new();

        for pkt in &mut self.unacknowledged_packets {
            let elapsed_ms = now.duration_since(pkt.time_sent).as_secs_f32() * 1000.0;

            if elapsed_ms >= self.retransmission_timeout_ms {
                resend_queue.push(pkt.data.clone());

                pkt.time_sent = now;
                pkt.retries += 1;
                self.stats_retransmits += 1;

                // Exponential backoff
                self.retransmission_timeout_ms = (self.retransmission_timeout_ms * 1.5).min(MAX_BACKOFF_MS);
            }
        }

        resend_queue
    }

    pub fn is_timed_out(&self, now: Instant, timeout: Duration) -> bool {
        now.duration_since(self.last_packet_received_time) > timeout
    }
}