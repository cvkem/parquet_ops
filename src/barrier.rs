

pub struct Barrier {
    pattern: u32,
    length_barrier: usize,
    barriers: Vec<Vec<u32>>
}

impl Barrier {

    pub fn new(pattern: u32, length_barrier: usize) -> Self {
        let barriers = Vec::new();
        Barrier {
            pattern,
            length_barrier,
            barriers
        }
    } 

    pub fn check_barriers(&self) -> bool {
        let mut succes = true;
        for (bar_idx, bar) in self.barriers.iter().enumerate() {
            for (val_idx, val) in bar.iter().enumerate() {
                if *val != self.pattern {
                    println!("On barrier {bar_idx} slot {val_idx} has value: {val} instead of {}", self.pattern);
                    succes = false;
                }
            }
        }
        succes
    }

    fn create_barrier(&self) -> Vec<u32> {
        let mut bar = Vec::with_capacity(self.length_barrier);
        for _ in 0..self.length_barrier {
            bar.push(self.pattern);
        }
        bar
    }

    pub fn add_barrier(&mut self) {
        assert!(self.check_barriers());

        let bar = self.create_barrier();
        self.barriers.push(bar);
        println!("Added barrier {}", self.barriers.len());
    }

}