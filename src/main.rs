extern crate kafkaesque;
extern crate rdkafka;
extern crate serde;
#[macro_use]
extern crate serde_derive;

extern crate timely;
use timely::dataflow::{Stream, Scope};
use timely::dataflow::operators::{Operator, Concatenate, Inspect, Map};
use timely::dataflow::channels::pact::Pipeline;

use std::cmp::max;
use std::collections::HashMap;

mod event;
use event::{Event, LikeEvent, CommentEvent, PostEvent};

mod kafka;

// TODO emit just the postId or the whole statistics structs? could be expensive
// to emit them as output (probably need to clone)
trait ActivePosts<G: Scope> {
    fn active_posts(&self) -> Stream<G, u64>;
}

impl <G:Scope<Timestamp=u64>> ActivePosts<G> for Stream<G, Event> {

    fn active_posts(&self) -> Stream<G, u64> {

        // event ID --> post ID it refers to (root of the tree)
        let mut root_of = HashMap::<u64,u64>::new();

        // post ID --> timestamp of last event associated with it
        let mut last_timestamp = HashMap::<u64,u64>::new();

        self.unary_notify(Pipeline, "ActivePosts", None, move |input, output, notificator| {

            input.for_each(|time, data| {

                let mut buf = Vec::<Event>::new();
                data.swap(&mut buf);

                for event in buf.drain(..) {
                    println!("event -- {:?}", event);
                    match event {
                        Event::Post(post) => {
                            root_of.insert(post.post_id, post.post_id);
                            last_timestamp.insert(post.post_id,
                                                  post.creation_date.timestamp() as u64);
                        },
                        Event::Like(like) => {
                            // TODO can you like comments / replies ?
                            let root_post_id = *root_of.get(&like.post_id).expect("TODO out of order");
                            let prev_t = last_timestamp.entry(root_post_id).or_insert(0); // TODO handle out of order
                            *prev_t = max(*prev_t, like.creation_date.timestamp() as u64);
                        },
                        Event::Comment(comment) => {
                            let reply_to_id = comment.reply_to_post_id
                                          .or(comment.reply_to_comment_id).unwrap();

                            let root_post_id = *root_of.get(&reply_to_id).expect("TODO out of order");
                            let prev_t = last_timestamp.entry(root_post_id).or_insert(0); // TODO again
                            *prev_t = max(*prev_t, comment.creation_date.timestamp() as u64);
                        }
                    }
                }

                println!("========================");
                println!("root_of -- {:?}", root_of);
                println!("last_timestamp -- {:?}", last_timestamp);
                println!("========================");

//                // TODO when to request the notification ?
//                notificator.notify_at(time.retain());
            });

            notificator.for_each(|time, _, _| {
                println!("notified at time {}", *time.time());
            });

        })
    }
}

fn main() {

    timely::execute_from_args(std::env::args(), |worker| {

        worker.dataflow::<u64,_,_>(|scope| {

            let likes_stream =
                kafka::consumer::string_stream(scope, "likes")
                    .map(|record: String| event::deserialize::<LikeEvent>(record))
                    .map(|like| Event::Like(like));

            let comments_stream =
                kafka::consumer::string_stream(scope, "comments")
                    .map(|record: String| event::deserialize::<CommentEvent>(record))
                    .map(|comment| Event::Comment(comment));

            let posts_stream =
                kafka::consumer::string_stream(scope, "posts")
                    .map(|record: String| event::deserialize::<PostEvent>(record))
                    .map(|post| Event::Post(post));

            let streams = vec![likes_stream, comments_stream, posts_stream];

            scope.concatenate(streams) // TODO reorder or produce and read from a single topic ?
//                .active_posts();
                .inspect(|x| println!("seen {:?}", x));

//            likes_stream
//                .inspect(|event: &LikeEvent| { println!("{:?}", event); });
//            comments_stream
//                .inspect(|event: &CommentEvent| { println!("{:?}", event); });
//            posts_stream
//                .inspect(|event: &PostEvent| { println!("{:?}", event); });
        });

    }).expect("Timely computation failed somehow");
}
