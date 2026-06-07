// history.rs
pub type Tick = u64;

#[derive(Clone, Debug)]
pub struct FrameRecord<T, I> {
    pub tick: Tick,
    pub state: T,
    pub state_hash: u64,
    pub input: I,
}

pub struct HistoryBuffer<T, I> {
    buffer: Vec<Option<FrameRecord<T, I>>>,
    mask: usize,
}

impl<T: Clone, I: Clone> HistoryBuffer<T, I> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity.is_power_of_two(), "Capacity must be a power of two");
        let mut buffer = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            buffer.push(None);
        }
        Self { buffer, mask: capacity - 1 }
    }

    pub fn insert(&mut self, record: FrameRecord<T, I>) {
        let index = (record.tick as usize) & self.mask;
        self.buffer[index] = Some(record);
    }

    pub fn get(&self, tick: Tick) -> Option<&FrameRecord<T, I>> {
        let index = (tick as usize) & self.mask;
        self.buffer[index].as_ref().filter(|r| r.tick == tick)
    }
}