use crate::event::Event;
use crate::percentile::Percentile;
use std::collections::HashSet;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::*;
use timely::dataflow::{Scope, Stream};

// We define unique_ratio for a post or a comment to be the ratio between
// the number of unique words and the number of words in its content field.
//
// Given a stream of events, UniqueWords returns pids of users who's unique_ratio
// is in the (or close to) 95th percentile of users. The dynamic threshold
// computation is carried inside the Percentile struct.
//
// Once a user is marked as spam, all subsequent events of that user are disregarded.
// Thus, the operator cannot mark the user's activity as spam multiple times.

pub trait UniqueWords<G: Scope> {
    fn unique_words(&self, worker_id: usize) -> Stream<G, u64>;
}

impl<G: Scope<Timestamp = u64>> UniqueWords<G> for Stream<G, Event> {
    fn unique_words(&self, worker_id: usize) -> Stream<G, u64> {
        let mut state = UniqueWordsState::new(worker_id);

        self.unary(Pipeline, "UniqueWords", move |_, _| {
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
struct UniqueWordsState {
    worder_id: usize,

    percentile:          Percentile,
    new_spam_person_ids: Vec<u64>,
    all_spam_person_ids: HashSet<u64>,
}

impl UniqueWordsState {
    fn new(worker_id: usize) -> UniqueWordsState {
        UniqueWordsState {
            worder_id:           worker_id,
            percentile:          Percentile::new(
                0.5,   /* initial threshold */
                5,     /* 100-5 percentile */
                10,    /* number of buckets */
                0_f64, /* min value */
                1_f64, /* max value */
            ),
            new_spam_person_ids: Vec::new(),
            all_spam_person_ids: HashSet::new(),
        }
    }

    fn update(&mut self, event: &Event, _: u64) {
        if let Event::Like(_) = &event {
            return;
        }

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

        let unique_ratio: f64 = if count == 0 { 1_f64 } else { unique.len() as f64 / count as f64 };
        let pid = event.person_id();

        self.percentile.add(unique_ratio);

        if unique_ratio <= self.percentile.threshold() && !self.all_spam_person_ids.contains(&pid) {
            self.new_spam_person_ids.push(pid);
            self.all_spam_person_ids.insert(pid);
        }
    }
}
