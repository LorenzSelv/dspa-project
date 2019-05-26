use math::round;

/// Given a set of f64 values, approximate n'th percentile. This struct is tailored
/// specifically for spam detection and data distributions we observed in the dataset.
///
/// This struct makes an assumption that the distribution of the events
/// looks similar to this:
///
///  count
///   ^
///   |                                       |
///   |                                      /
///   |                                     /
///   |                                    /
///   |                                   /
///   |                                  .
///   |                               ..
///   |                           ...
///   |                      ....
///   |.....................
///   |------------------------------------------|-------> percentile
///   min_value                                max_value
///
/// (Note: unique_counts fits this distribution perfectly, while post_frequencies
///         values had to be "normalised" in order to fit.)
///
/// Percentile splits the range between [min_value, max_value] into <bucket_len>
/// equal-sided regions - buckets. Each bucket has an associated count.
///
/// When a new value is added, the count of its corresponding bucket is incremented,
/// while if the event is removed the count gets decremented.
///
/// The new threshold is calculated by iterating over the buckets from min to max.
/// We keeping summing counts of buckets until this sum is >= _perc_ % of _total_.
/// At the point, the threshold is the right boundary of the last bucket added.
///
/// The constructor takes an argument _upper_bound_. This is set to be the initial
/// threshold value. Moreover, threshold value is not allowed to grow past
/// _upper_bound_. This is to disallow marking user activity as spam if there is
/// very little or no spam activity across all users.
///
#[derive(Clone)]
pub struct Percentile {
    perc:          u64,
    buckets:       Vec<u64>,
    total:         u64,
    bucket_width:  f64,
    min:           f64,
    threshold_val: f64,
    upper_bound:   f64,
}

impl Percentile {
    pub fn new(upper_bound: f64, perc: u64, bucket_len: usize, min: f64, max: f64) -> Percentile {
        Percentile {
            threshold_val: upper_bound,
            upper_bound:   upper_bound,
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
            bucket_idx += 1_f64;

            if count > perc_count {
                self.threshold_val = self.min + (bucket_idx * self.bucket_width);
                if self.threshold_val > self.upper_bound {
                    self.threshold_val = self.upper_bound;
                }
                // println!("NEW THRESHOLD!!!!! {}", self.threshold_val);
                return;
            }
        }
    }
}
