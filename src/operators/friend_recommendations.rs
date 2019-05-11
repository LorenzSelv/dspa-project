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
const RECOMMENDATION_SIZE: usize = 5;
const POSTGRES_URI: &'static str = "postgres://postgres:password@localhost:5432";

// TODO tune the weights
const COMMON_FRIENDS_WEIGHT: u64 = 1;
const WORK_AT_WEIGHT: u64 = 1;
const STUDY_AT_WEIGHT: u64 = 1;

// TODO so far, it makes recommendations for a single person.
pub trait FriendRecommendations<G: Scope> {
    fn friend_recommendations(&self, person_id: u64) -> Stream<G, Vec<Score>>;
}

impl<G: Scope<Timestamp = u64>> FriendRecommendations<G> for Stream<G, RecommendationUpdate> {
    fn friend_recommendations(&self, person_id: u64) -> Stream<G, Vec<Score>> {
        let conn = Connection::connect(POSTGRES_URI, TlsMode::None).unwrap();

        let static_state = StaticState::new(person_id, &conn);

        self.window_notify(
            NOTIFICATION_FREQ,
            "FriendRecommendations",
            DynamicState::new(person_id),
            |dyn_state, rec_update| dyn_state.update(rec_update),
            move |dyn_state, _timestamp| dyn_state.get_recommendations(&static_state),
        )
    }
}

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

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct Score {
    person_id: u64,
    score:     u64,
}

impl Ord for Score {
    fn cmp(&self, other: &Score) -> Ordering {
        other // min heap
            .score
            .cmp(&self.score)
            .then_with(|| self.person_id.cmp(&other.person_id))
    }
}

impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Score) -> Option<Ordering> { Some(self.cmp(other)) }
}

#[derive(Clone)]
struct DynamicState {
    person_id: u64,
    scores:    HashMap<u64, u64>, // person_id, score_val
}

impl DynamicState {
    fn new(person_id: u64) -> DynamicState {
        DynamicState { person_id: person_id, scores: HashMap::<u64, u64>::new() }
    }

    /// given a recommendationUpdate update the dynamic score
    fn update(&mut self, rec_update: &RecommendationUpdate) {
        // TODO
    }

    /// compute final scores and emit the top RECOMMENDATION_SIZE person_ids
    fn get_recommendations(&mut self, static_state: &StaticState) -> Vec<Score> {
        // keep a min-heap
        let mut top_scores = BinaryHeap::with_capacity(RECOMMENDATION_SIZE);

        for (&person_id, &static_score) in static_state.scores.iter() {
            let dyn_score = self.scores.get(&person_id).unwrap_or(&0);
            let score = static_score + dyn_score;

            if top_scores.len() < RECOMMENDATION_SIZE {
                top_scores.push(Score { person_id: person_id, score: score })
            } else if top_scores.peek().unwrap().score < score {
                // the current person has higher score than the minimum, update it
                top_scores.pop();
                top_scores.push(Score { person_id: person_id, score: score })
            }
        }

        top_scores.into_sorted_vec()
    }
}

/// Store states that does not change over time
/// i.e scores computed from static data and the list of friends
struct StaticState {
    person_id: u64,
    scores:    HashMap<u64, u64>, // person_id, score_val
    friends:   HashSet<u64>,
}

impl StaticState {
    fn new(person_id: u64, conn: &Connection) -> StaticState {
        let mut state = StaticState {
            person_id: person_id,
            scores:    HashMap::<u64, u64>::new(),
            friends:   HashSet::<u64>::new(),
        };

        state.init_static_scores(conn);
        state
    }

    /// run a query and update the static score
    /// the output format of the query should be a pair (person_id, score)
    /// the score is scaled by the weight
    fn run_score_query(&mut self, query_str: &String, weight: u64, conn: &Connection) {
        for row in &conn.query(&query_str, &[]).unwrap() {
            let person_id = row.get::<_, i64>(0) as u64;
            let score = row.get::<_, i64>(1) as u64;

            // we don't want to recommend those that are friends already
            if self.friends.contains(&person_id) {
                continue;
            }

            *self.scores.entry(person_id).or_insert(0) += score;
        }
    }

    fn init_static_scores(&mut self, conn: &Connection) {
        // compute common friends
        let query = query::friends(self.person_id);
        for row in &conn.query(&query, &[]).unwrap() {
            let person_id = row.get::<_, i64>(0) as u64;
            self.friends.insert(person_id);
        }

        let query_weight = vec![
            (query::common_friends(self.person_id), COMMON_FRIENDS_WEIGHT),
            (query::work_at(self.person_id), WORK_AT_WEIGHT),
            (query::study_at(self.person_id), STUDY_AT_WEIGHT),
        ];

        query_weight.iter().for_each(|(q, w)| self.run_score_query(q, *w, conn));
    }
}

// TODO extend to multiple people
pub fn dump_recommendations(scores: &Vec<Score>) {
    let spaces = " ".repeat(4);
    println!("{}--- recommendations", spaces);
    for score in scores.iter() {
        println!("{}  {:?}", spaces, score);
    }
    println!("{}--- ", spaces);
}
