use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};

use colored::*;

use timely::dataflow::{Scope, Stream};

use postgres::{Connection, TlsMode};

use crate::db::query;
use crate::operators::window_notify::{Timestamp, WindowNotify};

const ACTIVE_WINDOW_SECONDS: u64 = 4 * 3600;
const NOTIFICATION_FREQ: u64 = 1 * 3600;
const NUM_WINDOWS: usize = (ACTIVE_WINDOW_SECONDS / NOTIFICATION_FREQ) as usize;
const RECOMMENDATION_SIZE: usize = 5;
const POSTGRES_URI: &'static str = "postgres://postgres:password@localhost:5432";

// TODO tune the weights
const COMMON_FRIENDS_WEIGHT: u64 = 1;
const WORK_AT_WEIGHT: u64 = 1;
const STUDY_AT_WEIGHT: u64 = 1;

// weights for dynamic events
const LIKE_WEIGHT: u64 = 1;

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
            |dyn_state, rec_update, next_notification_time| dyn_state.update(rec_update, next_notification_time),
            move |dyn_state, timestamp| dyn_state.get_recommendations(&static_state, timestamp),
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
    window_scores:    VecDeque<HashMap<u64, u64>>, // person_id, score_val
    last_notification: u64,
}

impl DynamicState {
    fn new(person_id: u64) -> DynamicState {
        DynamicState {
            person_id: person_id,
            window_scores: VecDeque::new(),
            last_notification: 0, 
        }
    }

    /// given a recommendationUpdate update the dynamic score
    fn update(&mut self, rec_update: &RecommendationUpdate, next_notification_time: u64) {
        let delta_opt = match rec_update {
            RecommendationUpdate::Like { timestamp: _, from_person_id: fpid, to_person_id: tpid } =>
                if self.person_id == *fpid {
                    Some(Score { person_id: *tpid, score: LIKE_WEIGHT })
                } else { None }
        };

        if self.last_notification == 0 {
            self.last_notification = next_notification_time;
            self.window_scores.push_back(HashMap::new());
        }

        if let Some(delta) = delta_opt {
            self.delta_update(delta, rec_update.timestamp());
        }
    }

    fn delta_update(&mut self, delta: Score, event_timestamp: u64) {
         
        let idx =
            if event_timestamp <= self.last_notification {
                let back_offset = ((self.last_notification - event_timestamp) / NOTIFICATION_FREQ) as usize;
                
                if back_offset >= self.window_scores.len() {
                    self.window_scores.push_back(HashMap::new());
                }
                assert!(back_offset < self.window_scores.len());
                back_offset
            } else {
                while event_timestamp > self.last_notification {
                    self.window_scores.push_front(HashMap::new());
                    self.last_notification += NOTIFICATION_FREQ;
                }

                self.window_scores.truncate(NUM_WINDOWS);
                0
            };

        *self.window_scores[idx].entry(delta.person_id).or_insert(0) += delta.score;
    }

    /// compute final scores and emit the top RECOMMENDATION_SIZE person_ids
    fn get_recommendations(&mut self, static_state: &StaticState, notification_timestamp: u64) -> Vec<Score> {
        // TODO use timestamp (of the notification) to discard events older than 4 hours
        // either keep recommendation events and discard those older that timestamp - 4 hours
        // or .. ? 

        assert!(notification_timestamp >= self.last_notification);

        // discard old windows
        let empty_windows = ((notification_timestamp - self.last_notification) / NOTIFICATION_FREQ) as usize;
        self.window_scores.truncate(NUM_WINDOWS - empty_windows);

        // keep a min-heap
        let mut top_scores = BinaryHeap::with_capacity(RECOMMENDATION_SIZE);

        for (&person_id, &static_score) in static_state.scores.iter() {
            let dyn_score: u64 = self.window_scores.iter().map(|ws| ws.get(&person_id).unwrap_or(&0)).sum();
            let score = static_score + dyn_score;

            if top_scores.len() < RECOMMENDATION_SIZE {
                top_scores.push(Score { person_id: person_id, score: score })
            } else if top_scores.peek().unwrap().score < score {
                // the current person has higher score than the minimum, update it
                top_scores.pop();
                top_scores.push(Score { person_id: person_id, score: score })
            }
        }

        println!("returning {:?}", top_scores);

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

            *self.scores.entry(person_id).or_insert(0) += score * weight;
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
