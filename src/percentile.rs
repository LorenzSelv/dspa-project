use math::round;

#[derive(Clone)]
pub struct Percentile {
    perc:          u64,
    buckets:       Vec<u64>,
    total:         u64,
    bucket_width:  f64,
    min:           f64,
    threshold_val: f64,
    lower_bound:   f64,
}

impl Percentile {
    pub fn new(lower_bound: f64, perc: u64, bucket_len: usize, min: f64, max: f64) -> Percentile {
        Percentile {
            threshold_val: lower_bound,
            lower_bound:   lower_bound,
            perc:          perc,
            buckets:       vec![0; bucket_len],
            min:           min,
            bucket_width:  (max - min) / bucket_len as f64,
            total:         0_u64,
        }
    }

    pub fn add(&mut self, entry: f64) {
        let bucket_idx: usize = round::ceil((entry - self.min) / self.bucket_width, 0) as usize - 1;

        self.buckets[bucket_idx] += 1;
        self.total += 1;
    }

    pub fn remove(&mut self, entry: f64) {
        let bucket_idx: usize = round::ceil((entry - self.min) / self.bucket_width, 0) as usize - 1;

        self.buckets[bucket_idx] -= 1;
        self.total -= 1;
    }

    pub fn threshold(&mut self) -> f64 {
        self.update_threshold();
        return self.threshold_val.clone();
    }

    fn update_threshold(&mut self) {
        if self.total < 10 {
            return;
        }

        let mut count: u64 = 0;
        let mut bucket_idx = 0_f64;
        let perc_count: u64 = self.perc * self.total / 100;
        for val in self.buckets.iter() {
            count += val;

            if count > perc_count {
                self.threshold_val = self.min + (bucket_idx * self.bucket_width);
                if self.threshold_val < self.lower_bound {
                    self.threshold_val = self.lower_bound;
                }
                // println!("NEW THRESHOLD!!!!! {}", self.threshold_val);
                return;
            }

            bucket_idx += 1_f64;
        }
    }
}
