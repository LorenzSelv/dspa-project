use std::cmp::max;
use std::collections::{HashMap, HashSet};

use timely::dataflow::{Scope, Stream};

use colored::*;

use crate::operators::window_notify::{Timestamp, WindowNotify};

const NOTIFICATION_FREQ: u64 = 30 * 60; // every 30 minutes
const ACTIVE_WINDOW_SECONDS: u64 = 12 * 3600; // stats for the last 12 hours

pub trait ActivePosts<G: Scope> {
    fn active_posts(&self, worker_id: usize) -> Stream<G, HashMap<u64, Stats>>;
}

impl<G: Scope<Timestamp = u64>> ActivePosts<G> for Stream<G, StatUpdate> {
    fn active_posts(&self, worker_id: usize) -> Stream<G, HashMap<u64, Stats>> {
        self.window_notify(
            NOTIFICATION_FREQ,
            "ActivePosts",
            ActivePostsState::new(worker_id),
            |state, stat_update, _| state.update_stats(&stat_update),
            |state, timestamp| state.active_posts_stats(timestamp),
        )
    }
}

#[derive(Debug, Clone)]
pub struct Stats {
    num_comments:  u64,
    num_replies:   u64,
    unique_people: HashSet<u64>,
}

impl Stats {
    fn new() -> Stats { Stats { num_comments: 0, num_replies: 0, unique_people: HashSet::new() } }
    fn new_comment(&mut self) { self.num_comments += 1; }
    fn new_reply(&mut self) { self.num_replies += 1; }
    fn new_person(&mut self, id: u64) { self.unique_people.insert(id); }
}

pub fn dump_stats(stats: &HashMap<u64, Stats>, num_spaces: usize) {
    let spaces = " ".repeat(num_spaces);
    println!("{}---- stats", spaces);
    for (post_id, stats) in stats {
        println!("{}post_id = {} -- {:?}", spaces, post_id, stats);
    }
    println!("{}----", spaces);
}

#[derive(Clone, Debug)]
pub enum StatUpdateType {
    Post,
    Comment,
    Reply,
    Like,
}

/// event type sent by the PostTrees operator
#[derive(Clone, Debug)]
pub struct StatUpdate {
    pub update_type: StatUpdateType,
    pub post_id:     u64,
    pub person_id:   u64,
    pub timestamp:   u64,
}

impl Timestamp for StatUpdate {
    fn timestamp(&self) -> u64 { self.timestamp }
}

#[derive(Clone)]
struct ActivePostsState {
    worker_id: usize,
    // post ID --> timestamp of last event associated with it
    last_timestamp: HashMap<u64, u64>,
    // post ID --> stats
    stats: HashMap<u64, Stats>,
}

impl ActivePostsState {
    fn new(worker_id: usize) -> ActivePostsState {
        ActivePostsState {
            worker_id:      worker_id,
            last_timestamp: HashMap::<u64, u64>::new(),
            stats:          HashMap::<u64, Stats>::new(),
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
        println!("    last_timestamp -- {:?}", self.last_timestamp);
        dump_stats(&self.stats, 4);
        // println!("{}", "  Next stats".bold().blue());
        // println!("    next_last_timestamp -- {:?}", self.next_last_timestamp);
        // dump_stats(&self.next_stats, 4);
    }

    fn update_stats(&mut self, stat_update: &StatUpdate) {
        let post_id = stat_update.post_id;
        let timestamp = stat_update.timestamp;

        // update last_timestamp
        match self.last_timestamp.get(&post_id) {
            Some(&prev) => self.last_timestamp.insert(post_id, max(prev, timestamp)),
            None => self.last_timestamp.insert(post_id, timestamp),
        };

        match stat_update.update_type {
            StatUpdateType::Comment => self.stats.get_mut(&post_id).unwrap().new_comment(),
            StatUpdateType::Reply => self.stats.get_mut(&post_id).unwrap().new_reply(),
            _ => {} // nothing to do for posts and likes
        }

        // update unique people set
        self.stats.entry(post_id).or_insert(Stats::new()).new_person(stat_update.person_id);
    }

    fn active_posts_stats(&mut self, cur_timestamp: u64) -> HashMap<u64, Stats> {
        let active_posts = self
            .last_timestamp
            .iter()
            .filter_map(|(&post_id, &last_t)| {
                if last_t >= cur_timestamp - ACTIVE_WINDOW_SECONDS {
                    Some(post_id)
                } else {
                    None
                }
            })
            .collect::<HashSet<_>>();

        let active_posts_stats = self
            .stats
            .clone()
            .into_iter()
            .filter(|&(id, _)| active_posts.contains(&id))
            .collect::<HashMap<_, _>>();

        active_posts_stats
    }
}
