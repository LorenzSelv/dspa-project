extern crate kafkaesque;
extern crate rdkafka;
extern crate serde;
#[macro_use]
extern crate serde_derive;

use colored::*;

extern crate timely;
use timely::dataflow::{Stream, Scope};
use timely::dataflow::operators::{Operator, Inspect, Map};
use timely::dataflow::channels::pact::Pipeline;

use std::cmp::{min, max};
use std::collections::{HashMap, HashSet};

use std::rc::Rc;
use std::cell::RefCell;

mod event;
use event::Event;

mod kafka;

const ACTIVE_WINDOW_SECONDS: u64 = 12*3600;

#[derive(Debug,Clone)]
struct Stats {
    num_comments: u64,
    num_replies: u64,
    unique_people: HashSet<u64>
}

impl Stats {
    fn new() -> Stats {
        Stats {
            num_comments: 0,
            num_replies: 0,
            unique_people: HashSet::new()
        }
    }

    fn new_comment(&mut self) { self.num_comments += 1; }
    fn new_reply(&mut self)   { self.num_replies  += 1; }
    fn new_person(&mut self, id: u64)  { self.unique_people.insert(id); }
}

fn dump_stats(stats: &HashMap<u64,Stats>, tabs: usize) {
    let spaces = " ".repeat(4*tabs);
    println!("{}----", spaces);
    for (post_id, stats) in stats {
        println!("{}post_id = {} -- {:?}", spaces, post_id, stats);
    }
    println!("{}----", spaces);
}

// TODO emit just the postId or the whole statistics structs? could be expensive
// to emit them as output (probably need to clone)
trait ActivePosts<G: Scope> {
    fn active_posts(&self, active_window_seconds: u64) -> Stream<G, Vec<u64>>;
}

impl <G:Scope<Timestamp=u64>> ActivePosts<G> for Stream<G, Event> {

    fn active_posts(&self, active_window_seconds: u64) -> Stream<G, Vec<u64>> {

        // event ID --> post ID it refers to (root of the tree)
        let mut root_of = HashMap::<u64,u64>::new();
        // post ID --> timestamp of last event associated with it
        let mut last_timestamp = HashMap::<u64,u64>::new();
        // post ID --> stats
        let mut stats = HashMap::<u64,Stats>::new();

        let mut first_notification = true;

        self.unary_notify(Pipeline, "ActivePosts", None, move |input, output, notificator| {

            input.for_each(|time, data| {

                let mut buf = Vec::<Event>::new();
                data.swap(&mut buf);

                let mut min_t = std::u64::MAX;
                for event in buf.drain(..) {
                    println!("{} {}", "+".bold().yellow(), event.to_string().bold().yellow());
                    let cur_t = match event {
                        Event::Post(post) => {
                            let timestamp = post.creation_date.timestamp() as u64;
                            root_of.insert(post.post_id, post.post_id);
                            last_timestamp.insert(post.post_id, timestamp);

                            stats.insert(post.post_id, Stats::new());
                            stats.get_mut(&post.post_id).unwrap().new_person(post.person_id);

                            timestamp
                        },
                        Event::Like(like) => {
                            let timestamp = like.creation_date.timestamp() as u64;
                            let root_post_id = *root_of.get(&like.post_id).expect("TODO out of order");

                            match last_timestamp.get(&root_post_id) {
                                Some(&prev) => last_timestamp.insert(root_post_id, max(prev, timestamp)),
                                None => last_timestamp.insert(root_post_id, timestamp)
                            };

                            stats.get_mut(&root_post_id).unwrap().new_person(like.person_id);

                            timestamp
                        },
                        Event::Comment(comment) => {
                            let timestamp = comment.creation_date.timestamp() as u64;
                            let reply_to_id = comment.reply_to_post_id
                                          .or(comment.reply_to_comment_id).unwrap();

                            let root_post_id = *root_of.get(&reply_to_id).expect("TODO out of order");
                            root_of.insert(comment.comment_id, root_post_id);

                            match last_timestamp.get(&root_post_id) {
                                Some(&prev) => last_timestamp.insert(root_post_id, max(prev, timestamp)),
                                None => last_timestamp.insert(root_post_id, timestamp)
                            };

                            if comment.reply_to_post_id != None {
                                stats.get_mut(&root_post_id).unwrap().new_comment();
                            } else {
                                stats.get_mut(&root_post_id).unwrap().new_reply();
                            }

                            stats.get_mut(&root_post_id).unwrap().new_person(comment.person_id);

                            timestamp
                        }
                    };

                    min_t = min(min_t, cur_t);
                    println!("{}", "Current state".bold().blue());
                    println!("    root_of -- {:?}", root_of);
                    println!("    last_timestamp -- {:?}", last_timestamp);
                    dump_stats(&stats, 1);
                }

                if first_notification {
                    notificator.notify_at(time.delayed(&(min_t + 30*60)));
                    first_notification = false;
                }
            });

            let notified_time = None;
            let ref1 = Rc::new(RefCell::new(notified_time));
            let ref2 = Rc::clone(&ref1);

            notificator.for_each(|time, _, _| {
                let cur_t = *time.time();
                let mut borrow = ref1.borrow_mut();
                *borrow = Some(time.clone());
                println!("~~~~~~~~~~~~~~~~~~~~~~~~");
                println!("{} at timestamp {}", "notified".bold().green(), cur_t);
                println!("    root_of -- {:?}", root_of);
                println!("    last_timestamp -- {:?}", last_timestamp);
                dump_stats(&stats, 1);

                let active_posts = last_timestamp.iter().filter_map(|(&post_id, &last_t)| {
                    if last_t >= cur_t - active_window_seconds { Some(post_id) }
                    else { None }
                }).collect::<Vec<_>>();

                let mut session = output.session(&time);
                session.give(active_posts.clone());

                println!("    active_posts    -- {:?}", active_posts);
                println!("~~~~~~~~~~~~~~~~~~~~~~~~");

            });

            // set next notification in 30 minutes
            let borrow = ref2.borrow();
            if let Some(cap) = &*borrow {
                notificator.notify_at(cap.delayed(&(*cap.time() + 30*60)));
            }
        })
    }
}

fn main() {

    timely::execute_from_args(std::env::args(), |worker| {

        worker.dataflow::<u64,_,_>(|scope| {

            let events_stream =
                kafka::consumer::string_stream(scope, "events")
                    .map(|record: String| event::deserialize(record));

            events_stream
                .active_posts(ACTIVE_WINDOW_SECONDS)
                .inspect(|active_ids: &Vec<u64>| { println!("{} {:?}", "inspect".bold().red(), active_ids); });
        });

    }).expect("Timely computation failed somehow");
}
