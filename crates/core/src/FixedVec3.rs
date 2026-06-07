// In a new module or at the top of your files:
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FixedVec3 {
    pub x: i32,
    pub y: i32,
    pub z: i32,
}

impl FixedVec3 {
    // Using a scaling factor of 1000 for millimeter precision in the deterministic simulation
    pub const SCALE: i32 = 1000;

    pub fn new(x: f32, y: f32, z: f32) -> Self {
        Self {
            x: (x * Self::SCALE as f32).round() as i32,
            y: (y * Self::SCALE as f32).round() as i32,
            z: (z * Self::SCALE as f32).round() as i32,
        }
    }

    // Convert back to floats ONLY for the rendering interpolator
    pub fn to_f32(&self) -> [f32; 3] {
        [
            self.x as f32 / Self::SCALE as f32,
            self.y as f32 / Self::SCALE as f32,
            self.z as f32 / Self::SCALE as f32,
        ]
    }
}