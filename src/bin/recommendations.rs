/* TODO
 * - review all kafka options we pass to the BaseConsumer config
 * - handle the case when number of workers and number of partitions is not the same
 * - more TODOs...
 */

extern crate rdkafka;
extern crate serde;
extern crate serde_derive;
extern crate lazy_static;
extern crate config;
extern crate postgres;
extern crate ordered_float;

use colored::*;

use postgres::{Connection, TlsMode};
use std::collections::{BinaryHeap, HashMap};

use std::cmp::Ordering;

extern crate timely;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Map, Operator};
use timely::dataflow::{Scope, Stream};

use std::cmp::min;

use std::cell::RefCell;
use std::rc::Rc;

extern crate dspa;
use dspa::event;
use dspa::event::{Event};

use dspa::kafka;

use dspa::db::query;

const ACTIVE_WINDOW_SECONDS: u64 = 4 * 3600;
const UPDATE_WINDOW_SECONDS: u64 = 1 * 3600;
const RECOMMENDATION_SIZE: u64   = 5;

#[derive(Copy, Clone, Eq, PartialEq)]
struct Score {
    person_id: u64,
    val: u64,
}

impl Ord for Score {
    fn cmp(&self, other: &Score) -> Ordering {
        other.val.cmp(&self.val)
            .then_with(|| self.person_id.cmp(&other.person_id))
    }
}

// `PartialOrd` needs to be implemented as well.
impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Score) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct FriendRecommendations {
    person_id:  u64,
    all_scores: HashMap<u64, u64>,  // person_id, score_val
    top_scores: BinaryHeap<Score>,  // size = RECOMMENDATION_SIZE
    ooo_events: Vec<Event>,
    conn:       Connection,
    pub next_notification_time: u64,
}

impl FriendRecommendations {
    fn new(person_id: u64) -> FriendRecommendations {
        FriendRecommendations {
            person_id:              person_id,
            all_scores:             HashMap::<u64, u64>::new(),
            top_scores:             BinaryHeap::<Score>::new(),
            ooo_events:             Vec::<Event>::new(),  // process after notification
            conn:                   Connection::connect(
                                        "postgres://postgres:password@localhost:5432",
                                        TlsMode::None).unwrap(),
            next_notification_time: std::u64::MAX,
        }
    }

    fn initialize(&mut self) {
        // compute common friends
        let query = query::common_friends(self.person_id);

        for row in &self.conn.query(&query, &[]).unwrap() {
            let person_id = row.get::<_,i64>(0) as u64;
            let count     = row.get::<_,i64>(1) as u64;

            self.all_scores.insert(person_id, count);

            if self.top_scores.len() < RECOMMENDATION_SIZE as usize ||
                self.top_scores.peek().unwrap().val < count {
                self.top_scores.push(Score { person_id: person_id,
                                             val:       count
                })
            };

            if self.top_scores.len() > RECOMMENDATION_SIZE as usize {
                self.top_scores.pop();
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

    fn update(&mut self, event: Event) {
        if event.timestamp() > self.next_notification_time {
            self.ooo_events.push(event)
        }

        // otherwise process event straigt away.
        // do we need an "else"?
        // TODO: MAIN LOGIC!
    }

    fn new_window(&mut self) {
        for event in self.ooo_events.drain(..) {
            // TODO: LOGIC!
        }
    }
}

// so far, it makes reommendations for a single person.
trait RecommendFriend<G: Scope> {
    fn recommend_friends(
        &self,
        person_id: u64,
    ) -> Stream<G, Vec<u64>>;
}

impl<G: Scope<Timestamp = u64>> RecommendFriend<G> for Stream<G, Event> {
    fn recommend_friends(
        &self,
        person_id: u64
    ) -> Stream<G, Vec<u64>> {
        let mut state: FriendRecommendations = FriendRecommendations::new(person_id);
        state.initialize();

        let mut first_notification = true;

        self.unary_notify(Pipeline, "FriendRecommendations", None, move | input, output, notificator| {
            input.for_each(|time, data| {
                let mut buf = Vec::<Event>::new();
                data.swap(&mut buf);

                let mut min_t = std::u64::MAX;

                // do some processing on data.
                // For now we will always send number++;
                for event in buf.drain(..) {
                    println!("{} {}", "+".bold().yellow(), event.to_string().bold().yellow());
                    min_t = min(min_t, event.timestamp());
                    state.update(event);
                }

                // Set up the first notification.
                if first_notification && min_t != std::u64::MAX  {
                    state.next_notification_time = min_t + UPDATE_WINDOW_SECONDS;
                    println!("set first notification for: {}", state.next_notification_time);
                    notificator.notify_at(time.delayed(&state.next_notification_time));
                    first_notification = false;
                }
            });


            // Some setup in order to schedule new notifications inside a notification.
            let notified_time = None;
            let ref1 = Rc::new(RefCell::new(notified_time));
            let ref2 = Rc::clone(&ref1);

            notificator.for_each(|time, _, _| {
                let mut borrow = ref1.borrow_mut();
                *borrow = Some(time.clone());

                let res = state.get_recommendations();
                state.dump_recommendations();
                state.new_window();

                let mut session = output.session(&time);
                session.give(res);
            });

            let borrow = ref2.borrow();
            if let Some(cap) = &*borrow {
                state.next_notification_time = *cap.time() + UPDATE_WINDOW_SECONDS;
                notificator.notify_at(cap.delayed(&state.next_notification_time));
            }

        })
    }
}

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        let index = worker.index() as i32;
        let total_workers = worker.peers() as i32;

        worker.dataflow::<u64, _, _>(|scope| {
            let _events_stream =
                kafka::consumer::string_stream(scope, "events", index, total_workers)
                .map(|record: String| event::deserialize(record))
                .recommend_friends(471_u64);
        });
    })
    .expect("Timely computation failed somehow");
}
