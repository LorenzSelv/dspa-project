extern crate kafkaesque;
extern crate rdkafka;
extern crate serde;
#[macro_use]
extern crate serde_derive;

extern crate timely;
use timely::dataflow::{Stream, Scope};
use timely::dataflow::operators::{Operator, Map};
use timely::dataflow::channels::pact::Pipeline;

use std::cmp::{min, max};
use std::collections::HashMap;

use std::rc::Rc;
use std::cell::RefCell;

mod event;
use event::Event;

mod kafka;

const ACTIVE_WINDOW_SECONDS: u64 = 12*3600;

// TODO emit just the postId or the whole statistics structs? could be expensive
// to emit them as output (probably need to clone)
trait ActivePosts<G: Scope> {
    fn active_posts(&self, active_window_seconds: u64) -> Stream<G, u64>;
}

impl <G:Scope<Timestamp=u64>> ActivePosts<G> for Stream<G, Event> {

    fn active_posts(&self, active_window_seconds: u64) -> Stream<G, u64> {

        // event ID --> post ID it refers to (root of the tree)
        let mut root_of = HashMap::<u64,u64>::new();
        // post ID --> timestamp of last event associated with it
        let mut last_timestamp = HashMap::<u64,u64>::new();

        let mut first_notification = true;

        self.unary_notify(Pipeline, "ActivePosts", None, move |input, _output, notificator| {

//            println!("⏩ enter ActivePosts");

            input.for_each(|time, data| {

                let mut buf = Vec::<Event>::new();
                data.swap(&mut buf);

                let mut min_t = std::u64::MAX;
                for event in buf.drain(..) {
                    let cur_t = match event {
                        Event::Post(post) => {
                            let timestamp = post.creation_date.timestamp() as u64;
                            println!("➕ post at timestamp {} -- id = {}, date = {:?}", timestamp, post.post_id, post.creation_date);
                            root_of.insert(post.post_id, post.post_id);
                            last_timestamp.insert(post.post_id, timestamp);
                            
                            timestamp
                        },
                        Event::Like(like) => {
                            let timestamp = like.creation_date.timestamp() as u64;
                            println!("➕ like at timestamp {} -- to post_id = {}, date = {:?}", timestamp, like.post_id, like.creation_date);
                            // you can also like comments
                            let root_post_id = *root_of.get(&like.post_id).expect("TODO out of order");

                            match last_timestamp.get(&root_post_id) {
                                Some(&prev) => last_timestamp.insert(root_post_id, max(prev, timestamp)),
                                None => last_timestamp.insert(root_post_id, timestamp)
                            };
                            
                            timestamp
                        },
                        Event::Comment(comment) => {
                            let timestamp = comment.creation_date.timestamp() as u64;
                            println!("➕ comment at timestamp {} -- id = {}, date = {:?}", timestamp, comment.comment_id, comment.creation_date);
                            let reply_to_id = comment.reply_to_post_id
                                          .or(comment.reply_to_comment_id).unwrap();

                            let root_post_id = *root_of.get(&reply_to_id).expect("TODO out of order");
                            root_of.insert(comment.comment_id, root_post_id);

                            match last_timestamp.get(&root_post_id) {
                                Some(&prev) => last_timestamp.insert(root_post_id, max(prev, timestamp)),
                                None => last_timestamp.insert(root_post_id, timestamp)
                            };

                            timestamp
                        }
                    };

                    min_t = min(min_t, cur_t);
                    println!("🔵 Current state");
                    println!("   root_of -- {:?}", root_of);
                    println!("   last_timestamp -- {:?}", last_timestamp);
                }

                if first_notification {
//                    println!("🔜 setting notification at time {}", min_t +30*60);
                    notificator.notify_at(time.delayed(&(min_t + 30*60)));
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
                println!("🆘 🆘 🆘 notified at timestamp {}", cur_t);
                println!("  root_of -- {:?}", root_of);
                println!("  last_timestamp -- {:?}", last_timestamp);

                let active_posts = last_timestamp.iter().filter_map(|(&post_id, &last_t)| {
                    if last_t >= cur_t - active_window_seconds { Some(post_id) }
                    else { None }
                }).collect::<Vec<_>>();

                println!("  active_posts -- {:?}", active_posts);
                println!("~~~~~~~~~~~~~~~~~~~~~~~~");

            });

            // set next notification in 30 minutes
            let borrow = ref2.borrow();
            if let Some(cap) = &*borrow {
//                println!("🔜 setting notification at time {}", *cap.time() +30*60);
                notificator.notify_at(cap.delayed(&(*cap.time() + 30*60)));
            }
//            println!("⏪ exit ActivePosts");
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
                .active_posts(ACTIVE_WINDOW_SECONDS);
//                .inspect(|event: &Event| { println!("{:?}", event); });
        });

    }).expect("Timely computation failed somehow");
}
