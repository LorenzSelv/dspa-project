use crate::event::Event;
use crate::percentile::Percentile;

use std::ops::Bound::{Excluded, Included};

use std::collections::{BTreeMap, HashMap, HashSet};

const BURST_WINDOW: u64 = 120; // consider events only for the last 1 minute

// use colored::*;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::*;
use timely::dataflow::{Scope, Stream};

pub trait PostFrequency<G: Scope> {
    fn post_frequency(&self, worker_id: usize) -> Stream<G, u64>;
}

impl<G: Scope<Timestamp = u64>> PostFrequency<G> for Stream<G, Event> {
    fn post_frequency(&self, worker_id: usize) -> Stream<G, u64> {
        let mut state = PostFrequencyState::new(worker_id);

        self.unary(Pipeline, "PostFrequency", move |_, _| {
            move |input, output| {
                let mut buf = Vec::new();

                input.for_each(|time, data| {
                    data.swap(&mut buf);

                    for event in buf.drain(..) {
                        state.update(&event, *time.time());
                    }

                    let mut session = output.session(&time);
                    for id in state.new_spam_person_ids.drain(..) {
                        session.give(id);
                    }
                });
            }
        })
    }
}

#[derive(Clone)]
struct PostFrequencyState {
    worker_id: usize,

    // list of events created by a person in the last BURST_WINDOW
    person_to_event_maps:  HashMap<u64, BTreeMap<u64, u64>>,
    person_to_event_count: HashMap<u64, u64>,

    percentile:          Percentile,
    new_spam_person_ids: Vec<u64>,
    all_spam_person_ids: HashSet<u64>,
}

impl PostFrequencyState {
    fn new(worker_id: usize) -> PostFrequencyState {
        PostFrequencyState {
            worker_id:             worker_id,
            percentile:            Percentile::new(0.5, 95, 10, 0_f64, 1_f64),
            person_to_event_maps:  HashMap::new(),
            person_to_event_count: HashMap::new(),
            new_spam_person_ids:   Vec::new(),
            all_spam_person_ids:   HashSet::new(),
        }
    }

    fn update(&mut self, event: &Event, timestamp: u64) {
        if timestamp == 0 {
            return;
        }
        let pid = event.person_id();

        let total_count = self.person_to_event_count.entry(pid).or_insert(0);
        *total_count += 1;

        let map = self.person_to_event_maps.entry(pid).or_insert(BTreeMap::<u64, u64>::new());

        // see if there is a window which spans last 60 seconds.
        if let Some((_, count)) =
            map.range_mut((Excluded(timestamp - 60), Included(timestamp))).next_back()
        {
            *count += 1;
        } else {
            map.insert(timestamp, 1);
        }

        // remove events that went out of date:
        let mut to_remove: Vec<u64> = Vec::new();
        for (time, partial_count) in map.range((Included(0), Excluded(timestamp - BURST_WINDOW))) {
            *total_count -= partial_count;
            to_remove.push(*time);
        }

        for time in to_remove.iter() {
            map.remove(time);
        }

        // we have new total. Add the inverse to percentile struct
        let new_ratio: f64 = 1_f64 / *total_count as f64;

        self.percentile.add(new_ratio);
        if *total_count > 1 {
            self.percentile.remove(1_f64 / (*total_count - 1) as f64);
        }

        // check if number of event in window is above threshold
        // and not yet marked as spam
        if new_ratio <= self.percentile.threshold() && !self.all_spam_person_ids.contains(&pid) {
            self.new_spam_person_ids.push(pid);
            self.all_spam_person_ids.insert(pid);
        }
    }
}
