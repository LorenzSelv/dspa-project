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
            "ActivePosts",
            NOTIFICATION_FREQ,
            ActivePostsState::new(worker_id),
            |state, stat_update| state.update_stats(&stat_update),
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

struct ActivePostsState {
    // cur_* variables refer to the current window
    // next_* variables refer to the next window
    worker_id: usize,
    // post ID --> timestamp of last event associated with it
    cur_last_timestamp:  HashMap<u64, u64>,
    next_last_timestamp: HashMap<u64, u64>,
    // post ID --> stats
    cur_stats:  HashMap<u64, Stats>,
    next_stats: HashMap<u64, Stats>,

    next_notification_time: u64,
}

impl ActivePostsState {
    fn new(worker_id: usize) -> ActivePostsState {
        ActivePostsState {
            worker_id:              worker_id,
            cur_last_timestamp:     HashMap::<u64, u64>::new(),
            next_last_timestamp:    HashMap::<u64, u64>::new(),
            cur_stats:              HashMap::<u64, Stats>::new(),
            next_stats:             HashMap::<u64, Stats>::new(),
            next_notification_time: std::u64::MAX,
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
        println!("    cur_last_timestamp -- {:?}", self.cur_last_timestamp);
        dump_stats(&self.cur_stats, 4);
        println!("{}", "  Next stats".bold().blue());
        println!("    next_last_timestamp -- {:?}", self.next_last_timestamp);
        dump_stats(&self.next_stats, 4);
    }

    fn __update_stats(
        stat_update: &StatUpdate,
        last_timestamp: &mut HashMap<u64, u64>,
        stats: &mut HashMap<u64, Stats>,
    ) {
        let post_id = stat_update.post_id;
        let timestamp = stat_update.timestamp;

        // update last_timestamp
        match last_timestamp.get(&post_id) {
            Some(&prev) => last_timestamp.insert(post_id, max(prev, timestamp)),
            None => last_timestamp.insert(post_id, timestamp),
        };

        match stat_update.update_type {
            StatUpdateType::Comment => stats.get_mut(&post_id).unwrap().new_comment(),
            StatUpdateType::Reply => stats.get_mut(&post_id).unwrap().new_reply(),
            _ => {} // nothing to do for posts and likes
        }

        // update unique people set
        stats.entry(post_id).or_insert(Stats::new()).new_person(stat_update.person_id);
    }

    fn update_stats(&mut self, stat_update: &StatUpdate) {
        ActivePostsState::__update_stats(
            &stat_update,
            &mut self.next_last_timestamp,
            &mut self.next_stats,
        );
        if stat_update.timestamp <= self.next_notification_time {
            ActivePostsState::__update_stats(
                &stat_update,
                &mut self.cur_last_timestamp,
                &mut self.cur_stats,
            );
        }
    }

    fn active_posts_stats(&mut self, cur_timestamp: u64) -> HashMap<u64, Stats> {
        // TODO refactor
        let active_posts = self
            .cur_last_timestamp
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
            .cur_stats
            .clone()
            .into_iter()
            .filter(|&(id, _)| active_posts.contains(&id))
            .collect::<HashMap<_, _>>();

        self.cur_last_timestamp = self.next_last_timestamp.clone();
        self.cur_stats = self.next_stats.clone();

        active_posts_stats
    }
}
