extern crate kafkaesque;
extern crate rdkafka;
extern crate serde;
#[macro_use]
extern crate serde_derive;

extern crate timely;
use timely::dataflow::operators::{Concatenate, Inspect, Map};
use timely::dataflow::Scope;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::stream::Stream;
use timely::dataflow::operators::generic::operator::Operator;


use std::collections::HashMap;

mod event;
use event::{Event, LikeEvent, CommentEvent, PostEvent};

mod kafka;

trait ActiveWindow<G: Scope> {
    fn active_window(&self, epoch_timeout: u64) -> Stream<G, Vec<u64>>;
}

impl<G: Scope<Timestamp=u64>> ActiveWindow<G> for Stream<G, Event> {
    fn active_window(&self, epoch_timeout: u64) -> Stream<G, Vec<u64>> {
        self.unary_notify(Pipeline, "active_window", None, move | input, output, notificator| {
            let mut eventid_to_postid = HashMap::new();
            let mut parentid_to_children = HashMap::new();
            let mut post_to_time = HashMap::new();

            input.for_each(|time, data| {
                let mut vector = Vec::new();
                data.swap(&mut vector);

                for event in vector.drain(..) {
                    match event {
                        Event::Post(post) => {
                            // insert into hashmap
                            eventid_to_postid.insert(post.post_id, post.post_id);
                            // update last seen time if greater
                            // post_to_time.insert(post.post_id, post.time);
                        },
                        Event::Comment(com) => {
                            // This is a comment to the post
                            if let Some(parent_id) = com.reply_to_post_id {
                                eventid_to_postid.insert(com.comment_id, parent_id);
                                // post_to_time.insert(parent_id, com.time);
                            }
                            if let Some(parent_id) = com.reply_to_comment_id {
                                // This is a comment ot a comment/reply.
                                // check that the paent is already stored.
                                // Otherwise, put in the second map.
                                if let Some(post_id) = eventid_to_postid.get(&parent_id) {
                                    eventid_to_postid.insert(com.comment_id, *post_id);
                                } else {
                                    parentid_to_children
                                        .entry(parent_id)
                                        .or_insert(Vec::new())
                                        .push(com.comment_id);
                                }
                            }
                        },
                        Event::Like(like) => (),
                    }
                }

                notificator.notify_at(time.delayed(&(time.time().clone() + epoch_timeout)));
            });

            notificator.for_each(|time, _, _| {
                let mut v : Vec<u64> = Vec::new();
                output.session(&time).give(v);
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

            scope.concatenate(streams)
                .inspect(|x| println!("seen {:?}", x)).
                active_window(100);
        });

    }).expect("Timely computation failed somehow");
}

