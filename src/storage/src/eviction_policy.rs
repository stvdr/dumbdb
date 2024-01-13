pub trait EvictionPolicy {
    /// Adds a new frame to be available for eviction.
    fn add(&mut self, frame: usize);

    /// Removes a frame from being available for eviction (e.g. the buffer in the frame has been
    /// pinned.)
    fn remove(&mut self, frame: usize);

    /// Identify a frame that is available for eviction;
    fn evict(&mut self) -> Option<usize>;
}

pub struct SimpleEvictionPolicy {
    frames: Vec<usize>,
}

impl SimpleEvictionPolicy {
    fn new() -> Self {
        Self { frames: Vec::new() }
    }
}

impl EvictionPolicy for SimpleEvictionPolicy {
    fn add(&mut self, frame: usize) {
        if self.frames.contains(&frame) {
            return;
        }

        self.frames.push(frame);
    }

    fn remove(&mut self, frame: usize) {
        if let Some(i) = self.frames.iter().position(|f| *f == frame) {
            self.frames.remove(i);
        }
    }

    fn evict(&mut self) -> Option<usize> {
        self.frames.pop()
    }
}
