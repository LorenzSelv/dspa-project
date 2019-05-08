use std::cell::RefCell;
use std::cmp::min;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::rc::Rc;

use colored::*;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, Stream};

use postgres::{Connection, TlsMode};

use crate::db::query;
use crate::operators::window_notify::{Timestamp, WindowNotify};

const ACTIVE_WINDOW_SECONDS: u64 = 4 * 3600;
const NOTIFICATION_FREQ: u64 = 1 * 3600;
const RECOMMENDATION_SIZE: u64 = 5;
const POSTGRES_URI: &'static str = "postgres://postgres:password@localhost:5432";

#[derive(Clone, Debug)]
pub enum RecommendationUpdate {
    // TODO add more update types, the one below is just an example
    Like { timestamp: u64, from_person_id: u64, to_person_id: u64 },
}

impl Timestamp for RecommendationUpdate {
    fn timestamp(&self) -> u64 {
        match self {
            RecommendationUpdate::Like { timestamp: t, from_person_id: _, to_person_id: _ } => *t,
        }
    }
}

// TODO so far, it makes recommendations for a single person.
pub trait FriendRecommendations<G: Scope> {
    fn friend_recommendations(&self, person_id: u64) -> Stream<G, Vec<u64>>;
}

impl<G: Scope<Timestamp = u64>> FriendRecommendations<G> for Stream<G, RecommendationUpdate> {
    fn friend_recommendations(&self, person_id: u64) -> Stream<G, Vec<u64>> {
        self.window_notify(
            NOTIFICATION_FREQ,
            "FriendRecommendations",
            FriendRecommendationsState::new(person_id),
            |state, rec_update| state.update(rec_update),
            |state, _timestamp| state.get_recommendations(),
        )
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
struct Score {
    person_id: u64,
    val:       u64,
}

impl Ord for Score {
    fn cmp(&self, other: &Score) -> Ordering {
        other.val.cmp(&self.val).then_with(|| self.person_id.cmp(&other.person_id))
    }
}

// `PartialOrd` needs to be implemented as well.
impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Score) -> Option<Ordering> { Some(self.cmp(other)) }
}

struct FriendRecommendationsState {
    person_id: u64,
    all_scores: HashMap<u64, u64>, // person_id, score_val
    top_scores: BinaryHeap<Score>, // size = RECOMMENDATION_SIZE
    conn: Connection,
    pub next_notification_time: u64,
    friends: HashSet<u64>,
}

impl FriendRecommendationsState {
    fn new(person_id: u64) -> FriendRecommendationsState {
        let mut state = FriendRecommendationsState {
            person_id: person_id,
            all_scores: HashMap::<u64, u64>::new(),
            top_scores: BinaryHeap::<Score>::new(),
            conn: Connection::connect(POSTGRES_URI, TlsMode::None).unwrap(), // TODO move out
            next_notification_time: std::u64::MAX,                           // TODO move outside
            friends: HashSet::<u64>::new(),
        };

        state.init_static_scores();
        state
    }

    fn init_static_scores(&mut self) {
        // compute common friends
        let mut query = query::friends(self.person_id);
        for row in &self.conn.query(&query, &[]).unwrap() {
            let person_id = row.get::<_, i64>(0) as u64;
            self.friends.insert(person_id);
        }

        query = query::common_friends(self.person_id);
        for row in &self.conn.query(&query, &[]).unwrap() {
            let person_id = row.get::<_, i64>(0) as u64;
            let score = row.get::<_, i64>(1) as u64;

            if self.friends.contains(&person_id) {
                continue;
            }

            *self.all_scores.entry(person_id).or_insert(0) += score;
        }

        query = query::work_at(self.person_id);
        for row in &self.conn.query(&query, &[]).unwrap() {
            let person_id = row.get::<_, i64>(0) as u64;
            let score = row.get::<_, i64>(1) as u64;

            if self.friends.contains(&person_id) {
                continue;
            }

            *self.all_scores.entry(person_id).or_insert(0) += score; // TODO scale
        }

        query = query::study_at(self.person_id);
        for row in &self.conn.query(&query, &[]).unwrap() {
            let person_id = row.get::<_, i64>(0) as u64;
            let score = row.get::<_, i64>(1) as u64;

            if self.friends.contains(&person_id) {
                continue;
            }

            *self.all_scores.entry(person_id).or_insert(0) += score; // TODO scale
        }

        for (&person_id, &score) in self.all_scores.iter() {
            if self.top_scores.len() < RECOMMENDATION_SIZE as usize {
                self.top_scores.push(Score { person_id: person_id, val: score })
            } else if self.top_scores.peek().unwrap().val < score {
                self.top_scores.pop();
                self.top_scores.push(Score { person_id: person_id, val: score })
            }
        }
    }

    fn get_recommendations(&mut self) -> Vec<u64> {
        let mut res = Vec::<u64>::new();
        for s in self.top_scores.iter() {
            res.push(s.person_id);
        }
        res
    }

    fn dump_recommendations(&mut self) {
        let spaces = " ".repeat(10);
        println!("{}--- recommendations", spaces);
        for r in self.top_scores.iter() {
            println!("{}person id: {} \t score: {}", spaces, r.person_id, r.val);
        }
        println!("{}--- ", spaces);
    }

    fn update(&mut self, rec_update: RecommendationUpdate) {
        if rec_update.timestamp() > self.next_notification_time {
            // TODO keep a cur_score, next_score
        }

        // otherwise process rec_update straigt away.
        // do we need an "else"?
        // TODO: MAIN LOGIC!
    }
}

// TODO extend to more people
pub fn dump_recommendations(recommendations: &Vec<u64>) {
    println!("{:?}", recommendations);
}
