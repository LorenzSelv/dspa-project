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
use event::{Event, PostEvent, LikeEvent, CommentEvent};

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

fn handle_post(
    post: &PostEvent,
    root_of: &mut HashMap<u64,u64>,
    last_timestamp: &mut HashMap<u64,u64>,
    stats: &mut HashMap<u64,Stats>
) -> u64 {
    let timestamp = post.creation_date.timestamp() as u64;
    root_of.insert(post.post_id, post.post_id);
    last_timestamp.insert(post.post_id, timestamp);

    stats.insert(post.post_id, Stats::new());
    stats.get_mut(&post.post_id).unwrap().new_person(post.person_id);

    timestamp
}

fn handle_like(
    like: &LikeEvent,
    root_of: &mut HashMap<u64,u64>,
    last_timestamp: &mut HashMap<u64,u64>,
    stats: &mut HashMap<u64,Stats>
) -> u64 {
    let timestamp = like.creation_date.timestamp() as u64;
    // TODO can you like a comment?
    let root_post_id = *root_of.get(&like.post_id).expect("TODO out of order");

    match last_timestamp.get(&root_post_id) {
        Some(&prev) => last_timestamp.insert(root_post_id, max(prev, timestamp)),
        None => last_timestamp.insert(root_post_id, timestamp)
    };

    stats.get_mut(&root_post_id).unwrap().new_person(like.person_id);

    timestamp
}

fn handle_comment(
    comment: &CommentEvent,
    root_of: &mut HashMap<u64,u64>,
    last_timestamp: &mut HashMap<u64,u64>,
    stats: &mut HashMap<u64,Stats>
) -> u64 {
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

trait ActivePosts<G: Scope> {
    fn active_posts(&self, active_window_seconds: u64) -> Stream<G, HashMap<u64,Stats>>;
}

impl <G:Scope<Timestamp=u64>> ActivePosts<G> for Stream<G, Event> {

    fn active_posts(&self, active_window_seconds: u64) -> Stream<G, HashMap<u64,Stats>> {

        // cur_* variables refer to the current window
        // next_* variables refer to the next window

        // event ID --> post ID it refers to (root of the tree)
        let mut cur_root_of  = HashMap::<u64,u64>::new(); // TODO ids are not unique for posts + comments => change to pair
        let mut next_root_of = HashMap::<u64,u64>::new(); // TODO ids are not unique for posts + comments => change to pair
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
                    let cur_t = match event {
                        Event::Post(post) => {
                            let time = handle_post(&post, &mut next_root_of, &mut next_last_timestamp, &mut next_stats);
                            if time <= next_notification_time {
                                handle_post(&post, &mut cur_root_of, &mut cur_last_timestamp, &mut cur_stats);
                            }
                            time
                        },
                        Event::Like(like) => {
                            let time = handle_like(&like, &mut next_root_of, &mut next_last_timestamp, &mut next_stats);
                            if time <= next_notification_time {
                                handle_like(&like, &mut cur_root_of, &mut cur_last_timestamp, &mut cur_stats);
                            }
                            time
                        },
                        Event::Comment(comment) => {
                            let time = handle_comment(&comment, &mut next_root_of, &mut next_last_timestamp, &mut next_stats);
                            if time <= next_notification_time {
                                handle_comment(&comment, &mut cur_root_of, &mut cur_last_timestamp, &mut cur_stats);
                            }
                            time
                        }
                    };

                    min_t = min(min_t, cur_t);
                    println!("{}", "Current state".bold().blue());
                    println!("    cur_root_of -- {:?}", cur_root_of);
                    println!("    cur_last_timestamp -- {:?}", cur_last_timestamp);
                    dump_stats(&cur_stats, 1);
                    println!("{}", "Next state".bold().blue());
                    println!("    next_root_of -- {:?}", next_root_of);
                    println!("    next_last_timestamp -- {:?}", next_last_timestamp);
                    dump_stats(&next_stats, 1);
                }

                if first_notification {
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
                println!("  {}", "Current state".bold().blue());
                println!("    cur_root_of -- {:?}", cur_root_of);
                println!("    cur_last_timestamp -- {:?}", cur_last_timestamp);
                dump_stats(&cur_stats, 1);
                println!("  {}", "Next state".bold().blue());
                println!("    next_root_of -- {:?}", next_root_of);
                println!("    next_last_timestamp -- {:?}", next_last_timestamp);
                dump_stats(&next_stats, 1);

                let active_posts = cur_last_timestamp.iter().filter_map(|(&post_id, &last_t)| {
                    if last_t >= cur_t - active_window_seconds { Some(post_id) }
                    else { None }
                }).collect::<HashSet<_>>();

                let active_posts_stats = cur_stats.clone().into_iter()
                    .filter(|&(id, _)| active_posts.contains(&id))
                    .collect::<HashMap<_,_>>();

                let mut session = output.session(&time);
                session.give(active_posts_stats);

                cur_root_of = next_root_of.clone();
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
                    dump_stats(stats, 1);
                });
        });

    }).expect("Timely computation failed somehow");
}
