extern crate kafkaesque;
extern crate rdkafka;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate lazy_static;
extern crate config;

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

fn dump_stats(stats: &HashMap<u64,Stats>, num_spaces: usize) {
    let spaces = " ".repeat(num_spaces);
    println!("{}----", spaces);
    for (post_id, stats) in stats {
        println!("{}post_id = {} -- {:?}", spaces, post_id, stats);
    }
    println!("{}----", spaces);
}

fn dump_state(root_of: &HashMap<u64,u64>,
              ooo_events: &HashMap<u64,Event>,
              cur_last_timestamp: &HashMap<u64,u64>,
              cur_stats: &HashMap<u64,Stats>,
              next_last_timestamp: &HashMap<u64,u64>,
              next_stats: &HashMap<u64,Stats>
) {
    println!("{}", "Current state".bold().blue());
    println!("  root_of -- {:?}", root_of);
    println!("  ooo_events -- {:?}", ooo_events);
    println!("{}", "  Current stats".bold().blue());
    println!("    cur_last_timestamp -- {:?}", cur_last_timestamp);
    dump_stats(&cur_stats, 4);
    println!("{}", "  Next stats".bold().blue());
    println!("    next_last_timestamp -- {:?}", next_last_timestamp);
    dump_stats(&next_stats, 4);
}

fn update_post_tree(event: &Event, root_of: &mut HashMap<u64,u64>) -> (Option<u64>, Option<u64>) {
    match event {
        Event::Post(post) => {
            root_of.insert(post.post_id, post.post_id);
            (None, Some(post.post_id))
        },
        Event::Like(like) => { // likes are not stored in the tree
            (Some(like.post_id), root_of.get(&like.post_id).cloned())
        },
        Event::Comment(comment) => {
            let reply_to_id = comment.reply_to_post_id
                          .or(comment.reply_to_comment_id).unwrap();

            if let Some(&root_post_id) = root_of.get(&reply_to_id) {
                root_of.insert(comment.comment_id, root_post_id);
                (Some(reply_to_id), Some(root_post_id))
            } else {
                (Some(reply_to_id), None)
            }
        }
    }
}

fn update_stats(event: &Event,
                root_post_id: u64,
                last_timestamp: &mut HashMap<u64,u64>,
                stats: &mut HashMap<u64,Stats>
) {
    let timestamp = event.timestamp();

    // update last_timestamp
    match last_timestamp.get(&root_post_id) {
        Some(&prev) => last_timestamp.insert(root_post_id, max(prev, timestamp)),
        None => last_timestamp.insert(root_post_id, timestamp)
    };

    // update number of comments / replies
    if let Event::Comment(comment) = &event {
        if comment.reply_to_post_id != None {
            stats.get_mut(&root_post_id).unwrap().new_comment();
        } else {
            stats.get_mut(&root_post_id).unwrap().new_reply();
        }
    }

    // update unique people set
    stats.entry(root_post_id)
         .or_insert(Stats::new())
         .new_person(event.person_id());
}

trait ActivePosts<G: Scope> {
    fn active_posts(&self, active_window_seconds: u64) -> Stream<G, HashMap<u64,Stats>>;
}

impl <G:Scope<Timestamp=u64>> ActivePosts<G> for Stream<G, Event> {

    fn active_posts(&self, active_window_seconds: u64) -> Stream<G, HashMap<u64,Stats>> {

        // cur_* variables refer to the current window
        // next_* variables refer to the next window

        // event ID --> post ID it refers to (root of the tree)
        let mut root_of  = HashMap::<u64,u64>::new(); // TODO ids are not unique for posts + comments => change to pair
        // out-of-order events: id of missing event --> event that depends on it
        let mut ooo_events  = HashMap::<u64,Event>::new();
        // post ID --> timestamp of last event associated with it
        let mut cur_last_timestamp  = HashMap::<u64,u64>::new();
        let mut next_last_timestamp = HashMap::<u64,u64>::new();
        // post ID --> stats
        let mut cur_stats  = HashMap::<u64,Stats>::new();
        let mut next_stats = HashMap::<u64,Stats>::new();

        let mut first_notification = true;
        let mut next_notification_time = std::u64::MAX;


        self.unary_notify(Pipeline, "ActivePosts", None, move |input, output, notificator| {

            input.for_each(|time, data| {

                let mut buf = Vec::<Event>::new();
                data.swap(&mut buf);

                let mut min_t = std::u64::MAX;

                for event in buf.drain(..) {
                    println!("{} {}", "+".bold().yellow(), event.to_string().bold().yellow());

                    let (target_id, opt_root_post_id) = update_post_tree(&event, &mut root_of);

                    match opt_root_post_id {
                        Some(root_post_id) => {
                            let timestamp = event.timestamp();
                            update_stats(&event, root_post_id, &mut next_last_timestamp, &mut next_stats);
                            if timestamp <= next_notification_time {
                                update_stats(&event, root_post_id, &mut cur_last_timestamp, &mut cur_stats);
                            }
                            min_t = min(min_t, timestamp);
                        },
                        // out-of-order event
                        None => { ooo_events.insert(target_id.unwrap(), event); }
                    };

                    dump_state(&root_of, &ooo_events, &cur_last_timestamp, &cur_stats, &next_last_timestamp, &next_stats);
                }

                // first event might be out of order ..
                if min_t != std::u64::MAX && first_notification {
                    next_notification_time = min_t + 30*60;
                    notificator.notify_at(time.delayed(&next_notification_time));
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
                dump_state(&root_of, &ooo_events, &cur_last_timestamp, &cur_stats, &next_last_timestamp, &next_stats);

                let active_posts = cur_last_timestamp.iter().filter_map(|(&post_id, &last_t)| {
                    if last_t >= cur_t - active_window_seconds { Some(post_id) }
                    else { None }
                }).collect::<HashSet<_>>();

                let active_posts_stats = cur_stats.clone().into_iter()
                    .filter(|&(id, _)| active_posts.contains(&id))
                    .collect::<HashMap<_,_>>();

                let mut session = output.session(&time);
                session.give(active_posts_stats);

                cur_last_timestamp = next_last_timestamp.clone();
                cur_stats = next_stats.clone();

                println!("    active_posts    -- {:?}", active_posts);
                println!("~~~~~~~~~~~~~~~~~~~~~~~~");

            });

            // set next notification in 30 minutes
            let borrow = ref2.borrow();
            if let Some(cap) = &*borrow {
                next_notification_time = *cap.time() + 30*60;
                notificator.notify_at(cap.delayed(&next_notification_time));
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
                .inspect(|stats: &HashMap<u64,Stats>| {
                    println!("{}", "inspect".bold().red());
                    dump_stats(stats, 4);
                });
        });

    }).expect("Timely computation failed somehow");
}
