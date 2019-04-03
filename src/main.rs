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
                    match event {
                        Event::Post(post) => {
                            println!("post at timestamp {} -- {:?}", post.creation_date.timestamp(), post);
                            root_of.insert(post.post_id, post.post_id);
                            last_timestamp.insert(post.post_id,
                                                  post.creation_date.timestamp() as u64);
                        },
                        Event::Like(like) => {
                            println!("like at timestamp {} -- {:?}", like.creation_date.timestamp(), like);
                            // you can also like comments
                            let root_post_id = *root_of.get(&like.post_id).expect("TODO out of order");

                            let cur = like.creation_date.timestamp() as u64;
                            match last_timestamp.get(&root_post_id) {
                                Some(&prev) => last_timestamp.insert(root_post_id, max(prev, cur)),
                                None => last_timestamp.insert(root_post_id, cur)
                            };

                        },
                        Event::Comment(comment) => {
                            println!("comment at timestamp {} -- {:?}", comment.creation_date.timestamp(), comment);
                            let reply_to_id = comment.reply_to_post_id
                                          .or(comment.reply_to_comment_id).unwrap();

                            let root_post_id = *root_of.get(&reply_to_id).expect("TODO out of order");
                            root_of.insert(comment.comment_id, root_post_id);

                            let cur = comment.creation_date.timestamp() as u64;
                            match last_timestamp.get(&root_post_id) {
                                Some(&prev) => last_timestamp.insert(root_post_id, max(prev, cur)),
                                None => last_timestamp.insert(root_post_id, cur)
                            };
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

            let events_stream =
                kafka::consumer::string_stream(scope, "events")
                    .map(|record: String| event::deserialize(record));

            events_stream
                .active_posts();
//                .inspect(|event: &Event| { println!("{:?}", event); });
        });

    }).expect("Timely computation failed somehow");
}
