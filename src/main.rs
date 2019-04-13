/* TODO
 * - review all kafka options we pass to the BaseConsumer config
 * - handle the case when number of workers and number of partitions is not the same
 * - change other producer
 * - cargo fmt  
 * - more TODOs...
 */

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
use timely::dataflow::operators::{Operator, Exchange, Branch, Broadcast, Inspect, Concat, Map};
use timely::dataflow::channels::pact::Pipeline;

extern crate chrono;
use chrono::{Utc, TimeZone};

use std::cmp::{min, max};
use std::collections::{HashMap, HashSet};

use std::rc::Rc;
use std::cell::RefCell;

mod event;
use event::{Event, ID};

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
    println!("{}---- stats", spaces);
    for (post_id, stats) in stats {
        println!("{}post_id = {} -- {:?}", spaces, post_id, stats);
    }
    println!("{}----", spaces);
}

fn dump_ooo_events(ooo_events: &HashMap<ID,Vec<Event>>, num_spaces: usize) {
    let spaces = " ".repeat(num_spaces);
    println!("{}---- ooo_events", spaces);
    for (post_id, events) in ooo_events {
        println!("{}{:?} -- \n{}  {}", spaces, post_id, spaces,
                 events.iter().map(|e| e.to_string())
                 .collect::<Vec<_>>()
                 .join(&format!("\n{}  ", spaces))
        );
    }
    println!("{}----", spaces);
}

struct ActivePostsState {
    // cur_* variables refer to the current window
    // next_* variables refer to the next window

    worker_id: usize,
    // event ID --> post ID it refers to (root of the tree)
    root_of: HashMap<ID,ID>,
    // out-of-order events: id of missing event --> event that depends on it
    ooo_events: HashMap<ID, Vec<Event>>,
    // post ID --> timestamp of last event associated with it
    cur_last_timestamp: HashMap<u64,u64>,
    next_last_timestamp: HashMap<u64,u64>,
    // post ID --> stats
    cur_stats: HashMap<u64,Stats>,
    next_stats: HashMap<u64,Stats>,

    pub next_notification_time: u64,
}

impl ActivePostsState {

    fn new(worker_id: usize) -> ActivePostsState {
        ActivePostsState {
            worker_id: worker_id,
            root_of: HashMap::<ID,ID>::new(),
            ooo_events: HashMap::<ID,Vec<Event>>::new(),
            cur_last_timestamp:  HashMap::<u64,u64>::new(),
            next_last_timestamp: HashMap::<u64,u64>::new(),
            cur_stats:  HashMap::<u64,Stats>::new(),
            next_stats: HashMap::<u64,Stats>::new(),
            next_notification_time: std::u64::MAX
        }
    }

    fn dump(&self) {
        println!("{}", format!("{} {}", format!("[W{}]",self.worker_id).bold().blue(), "Current state".bold().blue()));
        println!("  root_of -- {:?}", self.root_of);
        dump_ooo_events(&self.ooo_events, 2);
        println!("{}", "  Current stats".bold().blue());
        println!("    cur_last_timestamp -- {:?}", self.cur_last_timestamp);
        dump_stats(&self.cur_stats, 4);
        println!("{}", "  Next stats".bold().blue());
        println!("    next_last_timestamp -- {:?}", self.next_last_timestamp);
        dump_stats(&self.next_stats, 4);
    }

    fn update_post_tree(&mut self, event: &Event) -> (Option<ID>, Option<ID>) {
        match event {
            Event::Post(post) => {
                self.root_of.insert(post.post_id, post.post_id);
                (None, Some(post.post_id))
            },
            Event::Like(like) => {
                // likes are not stored in the tree
                (Some(like.post_id), Some(like.post_id)) // can only like a post
            },
            Event::Comment(comment) => {
                let reply_to_id = comment.reply_to_post_id
                              .or(comment.reply_to_comment_id).unwrap();

                if let Some(&root_post_id) = self.root_of.get(&reply_to_id) {
                    self.root_of.insert(comment.comment_id, root_post_id);
                    (Some(reply_to_id), Some(root_post_id))
                } else {
                    (Some(reply_to_id), None)
                }
            }
        }
    }

    fn __update_stats(event: &Event,
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

    fn update_stats(&mut self, event: &Event, root_post_id: u64) -> u64 {
        let timestamp = event.timestamp();
        ActivePostsState::__update_stats(&event, root_post_id, &mut self.next_last_timestamp, &mut self.next_stats);
        if timestamp <= self.next_notification_time {
           ActivePostsState::__update_stats(&event, root_post_id, &mut self.cur_last_timestamp, &mut self.cur_stats);
        }
        timestamp
    }

    /// process events that have `id` as their target,
    /// recursively process the newly inserted ids
    fn process_ooo_events(&mut self, id: ID) {
        if let Some(events) = self.ooo_events.remove(&id) {
            println!("-- {} for id = {:?}", "process_ooo_events".bold().yellow(), id);

            let mut new_ids = Vec::new();
            for event in events {

                let (opt_target_id, opt_root_post_id) = self.update_post_tree(&event);
                assert!(opt_target_id.unwrap() == id, "wtf");
                let root_post_id = opt_root_post_id.expect("[process_ooo_events] root_post_id is None");

                self.update_stats(&event, root_post_id.u64());

                if let Some(new_id) = event.id() { new_ids.push(new_id) }
            }

            // adding events might unlock other ooo events
            // TODO foreach
            for new_id in new_ids.drain(..) {
                self.process_ooo_events(new_id);
            }
        }
    }

    fn active_posts_stats(&mut self, cur_timestamp: u64, active_window_seconds: u64) -> HashMap<u64,Stats> {

        let active_posts = self.cur_last_timestamp.iter().filter_map(|(&post_id, &last_t)| {
            if last_t >= cur_timestamp - active_window_seconds { Some(post_id) }
            else { None }
        }).collect::<HashSet<_>>();

        let active_posts_stats = self.cur_stats.clone().into_iter()
            .filter(|&(id, _)| active_posts.contains(&id))
            .collect::<HashMap<_,_>>();

        self.cur_last_timestamp = self.next_last_timestamp.clone();
        self.cur_stats = self.next_stats.clone();

        active_posts_stats
    }

    fn push_ooo_event(&mut self, event: Event, target_id: ID) {
        self.ooo_events.entry(target_id).or_insert(Vec::new()).push(event);
    }

    fn clean_ooo_events(&mut self, timestamp: u64) {
        self.ooo_events = self.ooo_events.clone().into_iter().filter(|(_, events)| {
            events.iter().all(|event| event.timestamp() > timestamp)
        }).collect::<HashMap<_,_>>();
    }
}

trait ActivePosts<G: Scope> {
    fn active_posts(&self, active_window_seconds: u64, worker_id: usize) -> Stream<G, HashMap<u64,Stats>>;
}

impl <G:Scope<Timestamp=u64>> ActivePosts<G> for Stream<G, Event> {

    fn active_posts(&self, active_window_seconds: u64, worker_id: usize) -> Stream<G, HashMap<u64,Stats>> {

        let mut state: ActivePostsState = ActivePostsState::new(worker_id);
        let mut first_notification = true;

        self.unary_notify(Pipeline, "ActivePosts", None, move |input, output, notificator| {

            input.for_each(|time, data| {

                let mut buf = Vec::<Event>::new();
                data.swap(&mut buf);

                let mut min_t = std::u64::MAX;

                for event in buf.drain(..) {
                    println!("{} {}", "+".bold().yellow(), event.to_string().bold().yellow());

                    let (opt_target_id, opt_root_post_id) = state.update_post_tree(&event);

                    match opt_root_post_id {
                        Some(root_post_id) => {
                            if let ID::Post(pid) = root_post_id {
                                let timestamp = state.update_stats(&event, pid);
                                min_t = min(min_t, timestamp);
                            } else {
                                panic!("expect ID::Post, got ID::Comment");
                            }

                            // check whether we can pop some stuff out of the ooo map
                            if let Some(eid) = event.id() { state.process_ooo_events(eid); }
                        },
                        None => { state.push_ooo_event(event, opt_target_id.unwrap()); }
                    };

                    state.dump();
                }

                // first event might be out of order ..
                if min_t != std::u64::MAX && first_notification {
                    state.next_notification_time = min_t + 30*60;
                    notificator.notify_at(time.delayed(&state.next_notification_time));
                    first_notification = false;
                }
            });

            let notified_time = None;
            let ref1 = Rc::new(RefCell::new(notified_time));
            let ref2 = Rc::clone(&ref1);

            notificator.for_each(|time, _, _| {
                let mut borrow = ref1.borrow_mut();
                *borrow = Some(time.clone());

                let timestamp = *time.time();
                let date = Utc.timestamp(timestamp as i64, 0);
                println!("~~~~~~~~~~~~~~~~~~~~~~~~");
                println!("{} at timestamp {}", "notified".bold().green(), date);
                state.dump();

                let stats = state.active_posts_stats(timestamp, active_window_seconds);
                println!("  {}", "Active post stats".bold().blue());
                dump_stats(&stats, 4);

                let mut session = output.session(&time);
                session.give(stats);

                state.clean_ooo_events(timestamp);

                println!("~~~~~~~~~~~~~~~~~~~~~~~~");
            });

            // set next notification in 30 minutes
            let borrow = ref2.borrow();
            if let Some(cap) = &*borrow {
                state.next_notification_time = *cap.time() + 30*60;
                notificator.notify_at(cap.delayed(&state.next_notification_time));
            }
        })
    }
}

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {

        let partition = worker.index() as i32;

        worker.dataflow::<u64,_,_>(|scope| {

            let events_stream =
                kafka::consumer::string_stream(scope, "events", partition)
                    .map(|record: String| event::deserialize(record));

            let (single, broad) = events_stream
                .branch(|_, event| {
                    match event {
                        Event::Comment(c) => c.reply_to_comment_id != None,
                        _ => false,
                    }
                });

            let single = single.exchange(|event| event.target_post_id());
            let broad  = broad.broadcast();

            single
                .concat(&broad)
                // TODO make sure that each worker sees what it's supposed to see
                .inspect(move |event|
                         println!("{}", format!("[W{}] seen event {:?}", partition, event.id()).bright_cyan().bold()))
                .active_posts(ACTIVE_WINDOW_SECONDS, partition as usize)
                .inspect(|stats: &HashMap<u64,Stats>| {
                    println!("{}", "inspect".bold().red());
                    dump_stats(stats, 4);
                });
        });

    }).expect("Timely computation failed somehow");
}
