use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Bound::{Excluded, Included};

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::*;
use timely::dataflow::{Scope, Stream};

use crate::event::Event;

use colored::*;

const BURST_WINDOW: u64 = 120; // consider events only for the last 1 minute
                               // const BURST_THRESHOLD: u64 = 100; // trigger if a person authors >= 100 posts/comments in the last window

pub trait SpamDetection<G: Scope> {
    fn spam_detection(&self, worker_id: usize) -> Stream<G, u64>;
}

/// Given a stream of events, emit the person ids of the
/// that are considered spams based on the following criteria:
/// TODO do the same for comments?
///
/// 1) number of posts/comments created in the last minute by a person:
///    bursts of posts/comments (probably originated by a bot) will be detected
///    likes are ignored
///
///
/// 2) same but for IP addresses
///    TODO we need to use a different threshold
///
///
/// 3) ratio of number unique words over total number of words:
///    results in a real value in the range [0,1], smaller
///    small values or extremely high values (for long posts)
///    are probably symptoms of a bot-generate post.
///    TODO compare against a running average and
///         trigger if very different from that value?
///
///    Examples:
///
///    P1: "this is a post with no repetitions" -- ratio=1 length="short" => fine
///    P2: "hello hello hello hello"            -- ratio=.25 length="short" => SPAM
///    P3: "aa ab ac ad ae af ag ah ... "       -- ratio=1 length="long"  => SPAM
///               ->now sure about the last one. It could just be a more formal text.
///
///
///  -----------------------------------------------------------------------------------
///  1) Per-parition by user_id
///  2) Make thresholds dynamic.
///

impl<G: Scope<Timestamp = u64>> SpamDetection<G> for Stream<G, Event> {
    fn spam_detection(&self, worker_id: usize) -> Stream<G, u64> {
        let mut state = SpamDetectionState::new(worker_id);

        self.unary(Pipeline, "SpamDetection", move |_, _| {
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
struct SpamDetectionState {
    worker_id: usize,

    // list of events created by a person in the last BURST_WINDOW
    person_to_event_maps:  HashMap<u64, BTreeMap<u64, u64>>,
    person_to_event_count: HashMap<u64, u64>,

    // ip_to_events: HashMap<String, VecDeque<Event>>,

    // TODO unique words feature

    // list of person ids marked as spam; will be drained after every batch
    new_spam_person_ids: Vec<u64>,
    all_spam_person_ids: HashSet<u64>,

    threshold_burst:    u64,
    threshold_unique:   f32,
    threshold_repeated: u64,
}

impl SpamDetectionState {
    fn new(worker_id: usize) -> SpamDetectionState {
        SpamDetectionState {
            worker_id:             worker_id,
            person_to_event_maps:  HashMap::new(),
            person_to_event_count: HashMap::new(),
            new_spam_person_ids:   Vec::new(),
            all_spam_person_ids:   HashSet::new(),
            threshold_burst:       100_u64,
            threshold_unique:      0.50,
            threshold_repeated:    50_u64,
        }
    }

    #[allow(dead_code)]
    fn dump(&self) {
        println!(
            "{}",
            format!(
                "{} {}",
                format!("[W{}]", self.worker_id).bold().blue(),
                "Current state".bold().blue()
            )
        );
        println!("{}", "  Current stats".bold().blue());
        println!("    person_to_events -- {:?}", self.person_to_event_count);
    }

    fn update(&mut self, event: &Event, timestamp: u64) {
        if let Event::Like(_) = &event {
            return; // ignore likes
        }

        self.check_frequency(event, timestamp);
        self.check_unique(event);
    }

    fn check_frequency(&mut self, event: &Event, timestamp: u64) {
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

        // check if number of event in window is above threshold
        // and not yet marked as spam
        if *total_count > self.threshold_burst && !self.all_spam_person_ids.contains(&pid) {
            self.new_spam_person_ids.push(pid);
            self.all_spam_person_ids.insert(pid);
        }
    }

    fn check_unique(&mut self, event: &Event) {
        let mut content = "";
        match event {
            Event::Post(post) => {
                content = &post.content;
            }
            Event::Comment(comment) => {
                content = &comment.content;
            }
            Event::Like(_) => {}
        }

        // parse content
        let mut tokens: Vec<String> = content
            .split(|c: char| c.is_whitespace() || c.is_ascii_punctuation())
            .map(|s| s.trim().to_lowercase())
            .filter(|s| !s.is_empty())
            .collect();

        let count: usize = tokens.len();
        let mut unique: HashSet<String> = HashSet::new();

        for s in tokens.drain(..) {
            unique.insert(s);
        }

        let unique_ratio: f32 = unique.len() as f32 / count as f32;
        let pid = event.person_id();

        if unique_ratio < self.threshold_unique && !self.all_spam_person_ids.contains(&pid) {
            self.new_spam_person_ids.push(pid);
            self.all_spam_person_ids.insert(pid);
        }
    }
}
