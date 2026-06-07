use zerocopy::{FromBytes, AsBytes, FromZeroes, KnownLayout};

pub type Tick = u64;

// Update your PacketType enum
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum PacketType {
    Reliable = 1,
    Disconnect = 2,
    Snapshot = 3,
    Input = 4,
}

#[repr(C, packed)]
#[derive(Copy, Clone, Debug, FromBytes, AsBytes, FromZeroes, KnownLayout, PartialEq)]
pub struct SnapshotHeader {
    pub tick: Tick,
    pub state_hash: u64,
    pub last_input_tick: u64,
}

#[repr(C, packed)]
#[derive(Copy, Clone, Debug, FromBytes, AsBytes, FromZeroes, KnownLayout, PartialEq)]
pub struct InputPacket {
    pub tick: Tick,
    pub input_bitmask: u32,
}
#[repr(C, packed)]
#[derive(Copy, Clone, Debug, FromBytes, AsBytes, FromZeroes, KnownLayout, PartialEq)]
pub struct GeneralPacketHeader {
    pub packet_type: u8,
}

#[repr(C, packed)]
#[derive(Copy, Clone, Debug, FromBytes, AsBytes, FromZeroes, KnownLayout, PartialEq)]
pub struct ReliabilityPacketHeader {
    pub sequence: u16,
    pub ack: u16,
    pub ack_bitfield: u32,
}

#[repr(C, packed)]
#[derive(Copy, Clone, Debug, FromBytes, AsBytes, FromZeroes, KnownLayout, PartialEq)]
pub struct DisconnectPacket {
    pub header: GeneralPacketHeader,
    pub reason_code: u32,
}

#[repr(C, packed)]
#[derive(Copy, Clone, Debug, FromBytes, AsBytes, FromZeroes, KnownLayout, PartialEq)]
pub struct DataPacketHeader {
    pub uncompressed_size: u32,
}