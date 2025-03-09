use crate::probe;

pub struct ElasticProbe {
    seq: probe::ProbeSequence,
    pos: usize,
}

impl ElasticProbe {
    pub fn new(seq: probe::ProbeSequence) -> Self {
        Self { seq, pos: 0 }
    }

    pub fn probe(&mut self, i: u32, j: u32) -> usize {
        debug_assert!(j > 0);
        let x = super::map::ElasticHashMap::<i32, i32>::phi(i, j);
        while self.pos < x as usize {
            self.pos += 1;
            self.seq.next();
        }
        self.pos += 1;
        self.seq.next()
    }

    pub fn next_no_limit(&mut self) -> usize {
        self.pos += 1;
        self.seq.next_no_limit()
    }
}
