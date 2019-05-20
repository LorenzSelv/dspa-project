// From midterm report:
//
// Suppose we wish to recommend friend to person A. From the stream, we consider
// following scenarios when computing score for person B:
//
// Person A is interested in tag T and person B creates a post with tag T.
//  x  Person A likes post P created by person B.
//  x  Person A comments on post P created by person B.
//     Person A replies to comment C created by person B.
//     Person A and person B comments / likes / replies to the same post P.
//  x  Person B posts in a forum that person A is a member of.
//     Person B comments in a forum that person A is a member of.

use std::cmp::min;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};

use timely::dataflow::{Scope, Stream};

use postgres::{Connection, TlsMode};

use crate::db::query;
use crate::operators::window_notify::{Timestamp, WindowNotify};

const ACTIVE_WINDOW_SECONDS: u64 = 4 * 3600;
const NOTIFICATION_FREQ: u64 = 1 * 3600;
const NUM_WINDOWS: usize = (ACTIVE_WINDOW_SECONDS / NOTIFICATION_FREQ) as usize;
const RECOMMENDATION_SIZE: usize = 5;
const POSTGRES_URI: &'static str = "postgres://postgres:postgres@localhost:5432";

// TODO tune the weights
const COMMON_FRIENDS_WEIGHT: u64 = 1;
const WORK_AT_WEIGHT: u64 = 1;
const STUDY_AT_WEIGHT: u64 = 1;

// weights for dynamic events
const LIKE_WEIGHT: u64 = 50;
const COMMENT_WEIGHT: u64 = 50;
const REPLY_WEIGHT: u64 = 50;
const FORUM_POST_WEIGHT: u64 = 20;
const TAG_POST_WEIGHT: u64 = 100;

pub fn parse_tags(tags: &Option<String>) -> Vec<u64> {
    let mut v: Vec<u64> = Vec::new();
    if let Some(tag_string) = tags {
        v = tag_string[1..tag_string.len() - 2]
            .split(", ")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.parse().unwrap())
            .collect();
        v
    } else {
        v
    }
}

pub trait FriendRecommendations<G: Scope> {
    fn friend_recommendations(&self, person_ids: &Vec<u64>) -> Stream<G, HashMap<u64, Vec<Score>>>;
}

impl<G: Scope<Timestamp = u64>> FriendRecommendations<G> for Stream<G, RecommendationUpdate> {
    fn friend_recommendations(&self, person_ids: &Vec<u64>) -> Stream<G, HashMap<u64, Vec<Score>>> {
        let conn = Connection::connect(POSTGRES_URI, TlsMode::None).unwrap();

        let static_state = StaticState::new(person_ids, &conn);
        let static_state_copy = static_state.clone();

        self.window_notify(
            NOTIFICATION_FREQ,
            "FriendRecommendations",
            DynamicState::new(person_ids),
            move |dyn_state, rec_update, next_notification_time| {
                dyn_state.update(rec_update, &static_state_copy, next_notification_time)
            },
            move |dyn_state, timestamp| dyn_state.get_recommendations(&static_state, timestamp),
        )
    }
}

#[derive(Clone, Debug)]
pub enum RecommendationUpdate {
    Post { timestamp: u64, person_id: u64, forum_id: u64, tags: Option<String> },
    Like { timestamp: u64, from_person_id: u64, to_person_id: u64 },
    Comment { timestamp: u64, from_person_id: u64, to_person_id: u64 },
    Reply { timestamp: u64, from_person_id: u64, to_person_id: u64 },
}

impl Timestamp for RecommendationUpdate {
    fn timestamp(&self) -> u64 {
        match self {
            RecommendationUpdate::Post { timestamp: t, person_id: _, forum_id: _, tags: _ } => *t,
            RecommendationUpdate::Like { timestamp: t, from_person_id: _, to_person_id: _ } => *t,
            RecommendationUpdate::Comment { timestamp: t, from_person_id: _, to_person_id: _ } => {
                *t
            }
            RecommendationUpdate::Reply { timestamp: t, from_person_id: _, to_person_id: _ } => *t,
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
    pid_to_state: HashMap<u64, DynamicStateSingle>,
}

impl DynamicState {
    fn new(person_ids: &Vec<u64>) -> DynamicState {
        let mut ds = DynamicState { pid_to_state: HashMap::new() };
        for pid in person_ids {
            ds.pid_to_state.insert(*pid, DynamicStateSingle::new(*pid));
        }
        ds
    }

    fn update(
        &mut self,
        rec_update: &RecommendationUpdate,
        static_state: &StaticState,
        next_notification_time: u64,
    ) {
        for (pid, state) in self.pid_to_state.iter_mut() {
            state.update(rec_update, static_state.get(*pid), next_notification_time);
        }
    }

    fn get_recommendations(
        &mut self,
        static_state: &StaticState,
        notification_timestamp: u64,
    ) -> HashMap<u64, Vec<Score>> {
        let mut map: HashMap<u64, Vec<Score>> = HashMap::new();
        for (pid, state) in self.pid_to_state.iter_mut() {
            map.insert(
                *pid,
                state.get_recommendations(static_state.get(*pid), notification_timestamp),
            );
        }
        map
    }
}

#[derive(Clone)]
struct DynamicStateSingle {
    person_id:         u64,
    window_scores:     VecDeque<HashMap<u64, u64>>, // person_id, score_val
    last_notification: u64,
    post_tags:         HashSet<u64>,
}

impl DynamicStateSingle {
    fn new(person_id: u64) -> DynamicStateSingle {
        DynamicStateSingle {
            person_id:         person_id,
            window_scores:     VecDeque::new(),
            last_notification: 0,
            post_tags:         HashSet::new(),
        }
    }

    /// given a recommendationUpdate update the dynamic score
    fn update(
        &mut self,
        rec_update: &RecommendationUpdate,
        static_state: &StaticStateSingle,
        next_notification_time: u64,
    ) {
        let delta_opt = match rec_update {
            // person A likes post P created by person B => suggest B to A
            RecommendationUpdate::Like {
                timestamp: _,
                from_person_id: fpid,
                to_person_id: tpid,
            } => {
                if self.person_id == *fpid {
                    Some(Score { person_id: *tpid, score: LIKE_WEIGHT })
                } else {
                    None
                }
            }
            RecommendationUpdate::Comment {
                timestamp: _,
                from_person_id: fpid,
                to_person_id: tpid,
            } => {
                if self.person_id == *fpid {
                    Some(Score { person_id: *tpid, score: COMMENT_WEIGHT })
                } else {
                    None
                }
            }
            RecommendationUpdate::Reply {
                timestamp: _,
                from_person_id: fpid,
                to_person_id: tpid,
            } => {
                if self.person_id == *fpid {
                    Some(Score { person_id: *tpid, score: REPLY_WEIGHT })
                } else {
                    None
                }
            }
            RecommendationUpdate::Post {
                timestamp: _,
                person_id: pid,
                forum_id: forum,
                tags: tags_string,
            } => {
                let tags: Vec<u64> = parse_tags(tags_string);
                if self.person_id == *pid {
                    // Insert tags into dynamic state
                    for n in tags {
                        self.post_tags.insert(n);
                    }
                    None
                } else {
                    // Base the score on the tags of the post as well as the forum.
                    let relevan_tags: Vec<&u64> =
                        tags.iter().filter(|n| self.post_tags.contains(n)).collect();
                    let mut new_score: u64 = TAG_POST_WEIGHT * relevan_tags.len() as u64;
                    if static_state.forums.contains(&forum) {
                        new_score += FORUM_POST_WEIGHT;
                    }
                    Some(Score { person_id: *pid, score: new_score })
                }
            }
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
        let idx = if event_timestamp <= self.last_notification {
            let back_offset =
                ((self.last_notification - event_timestamp) / NOTIFICATION_FREQ) as usize;

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
    fn get_recommendations(
        &mut self,
        static_state: &StaticStateSingle,
        notification_timestamp: u64,
    ) -> Vec<Score> {
        assert!(notification_timestamp >= self.last_notification);

        // discard old windows
        let empty_windows = min(
            NUM_WINDOWS,
            ((notification_timestamp - self.last_notification) / NOTIFICATION_FREQ) as usize,
        );
        self.window_scores.truncate(NUM_WINDOWS - empty_windows);

        // keep a min-heap
        let mut top_scores = BinaryHeap::with_capacity(RECOMMENDATION_SIZE);

        for (&person_id, &static_score) in static_state.scores.iter() {
            let dyn_score: u64 =
                self.window_scores.iter().map(|ws| ws.get(&person_id).unwrap_or(&0)).sum();
            if dyn_score > 0 {
                println!("dynamic score for {} of {}", person_id, dyn_score);
            }
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

#[derive(Clone)]
struct StaticState {
    pid_to_state: HashMap<u64, StaticStateSingle>,
}

impl StaticState {
    fn new(person_ids: &Vec<u64>, conn: &Connection) -> StaticState {
        let mut ss = StaticState { pid_to_state: HashMap::new() };
        for pid in person_ids {
            ss.pid_to_state.insert(*pid, StaticStateSingle::new(*pid, conn));
        }
        ss
    }

    fn get(&self, pid: u64) -> &StaticStateSingle { self.pid_to_state.get(&pid).unwrap() }
}

/// Store states that does not change over time
/// i.e scores computed from static data and the list of friends
#[derive(Clone)]
struct StaticStateSingle {
    person_id: u64,
    scores:    HashMap<u64, u64>, // person_id, score_val
    friends:   HashSet<u64>,
    forums:    HashSet<u64>,
}

impl StaticStateSingle {
    fn new(person_id: u64, conn: &Connection) -> StaticStateSingle {
        let mut state = StaticStateSingle {
            person_id: person_id,
            scores:    HashMap::<u64, u64>::new(),
            friends:   HashSet::<u64>::new(),
            forums:    HashSet::<u64>::new(),
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

        let query2 = query::forums(self.person_id);
        for row in &conn.query(&query2, &[]).unwrap() {
            let forum_id = row.get::<_, i64>(0) as u64;
            self.forums.insert(forum_id);
        }

        let query_weight = vec![
            (query::non_friends(self.person_id), 0_u64),
            (query::common_friends(self.person_id), COMMON_FRIENDS_WEIGHT),
            (query::work_at(self.person_id), WORK_AT_WEIGHT),
            (query::study_at(self.person_id), STUDY_AT_WEIGHT),
        ];

        query_weight.iter().for_each(|(q, w)| self.run_score_query(q, *w, conn));
    }
}

// TODO extend to multiple people
pub fn dump_recommendations(pid: u64, scores: &Vec<Score>) {
    let spaces = " ".repeat(4);
    println!("{}--- recommendations for {}", spaces, pid);
    for score in scores.iter() {
        println!("{}  {:?}", spaces, score);
    }
    println!("{}--- ", spaces);
}
