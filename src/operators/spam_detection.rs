use std::cmp::max;
use std::collections::{HashMap, HashSet};

use timely::dataflow::{Scope, Stream};

use colored::*;

const BURST_WINDOW: u64 = 60; // consider events only for the last 60 seconds 
const BURST_THRESHOLD: u64 = 100; // trigger if a person authors >= 100 posts/comments in the last window 

pub trait SpamDetection<G: Scope> {
    fn spam_detection(&self, worker_id: usize) -> Stream<G, u64>>;
}

/// Given a stream of events, emit the person ids of the
/// that are considered spams based on the following criteria:
/// TODO do the same for comments?
///
/// 1) number of posts/comments created in the last minute by a person:
///    bursts of posts/comments (probably originated by a bot) will be detected
///    likes are ignored
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
///
///
impl<G: Scope<Timestamp = u64>> SpamDetection<G> for Stream<G, Event> {

    fn spam_detection(&self, worker_id: usize) -> Stream<G, HashMap<u64, u64>> {

        let mut state = SpamDetectionState::new(worker_id);

	self.unary(Pipeline, "SpamDetection", move |_, _| move |input, output| {

            let mut buf = Vec::new();

            input.for_each(move |time, data| {
                data.swap(&mut buf);

                for event in buf.drain(..) {
                    state.update(event, *time.time());
                }

                let mut session = output.session(&time);
                for id in state.new_spam_person_ids.drain(..) {
                    session.give(id);
                }
            });
        })
    }
}

#[derive(Clone)]
struct SpamDetectionState {
    worker_id: usize,
    
    // list of events created by a person in the last BURST_WINDOW
    person_to_events: HashMap<u64, VecDeque<PostEvent>>,

    // ip_to_events: HashMap<String, VecDeque<PostEvent>>,

    // TODO unique words feature

    // list of person ids marked as spam; will be drained after every batch
    new_spam_person_ids: Vec<u64>,
    all_spam_person_ids: HashSet<u64>,
}

impl SpamDetectionState {
    fn new(worker_id: usize) -> SpamDetectionState {
        SpamDetectionState {
            worker_id:      worker_id,
            person_to_events: HashMap::new(),
            new_spam_person_ids: Vec::new(),
            all_spam_person_ids: HashSet::new(),
        }
    }

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
        println!("    person_to_events -- {:?}", self.person_to_events);
    }

    fn update(&mut self, event: &PostEvent, timestamp: u64) {

        if let Event::Like(_) = &event {
            return; // ignore likes
        }

        let pid = event.person_id();

        *self.person_to_events
            .entry(pid)
            .or_insert(VecDeque::new())
            .push_back(event);

        // remove events that went out of the window
        let mut discard_until = 0;
        for event in &self.person_to_events.get(&pid).unwrap() {
            // check if event is "old"
            if event.timestamp < timestamp - BURST_WINDOW {
                discard_until += 1
            }
        }

        // discard old events
        self.person_to_events
            .get_mut(&pid)
            .unwrap()
            .drain(0..discard_until);

        // check if number of event in window is above threshold
        // and not yet marked as spam
        if self.person_to_events.get(&pid).len() > BURST_THRESHOLD &&
           !self.all_spam_person_ids.contain(pid)
        {
            self.new_spam_person_ids.push_back(pid);
            self.all_spam_person_ids.add(pid);
        }
    }
}
