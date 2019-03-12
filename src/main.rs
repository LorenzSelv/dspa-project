extern crate timely;
use timely::dataflow::operators::{Inspect, Map};

extern crate kafkaesque;

extern crate rdkafka;

extern crate serde;
#[macro_use]
extern crate serde_derive;

mod event;
use event::{LikeEvent, CommentEvent, PostEvent};

mod kafka;


fn main() {

    timely::execute_from_args(std::env::args(), |worker| {

        worker.dataflow::<u64,_,_>(|scope| {
            
            let likes_stream = 
                kafka::consumer::string_stream(scope, "likes")
                    .map(|record: String| event::deserialize::<LikeEvent>(record));

            let comments_stream = 
                kafka::consumer::string_stream(scope, "comments")
                    .map(|record: String| event::deserialize::<CommentEvent>(record));

            let posts_stream = 
                kafka::consumer::string_stream(scope, "posts")
                    .map(|record: String| event::deserialize::<PostEvent>(record));

            likes_stream
                .inspect(|event: &LikeEvent| { println!("{:?}", event); });
            comments_stream
                .inspect(|event: &CommentEvent| { println!("{:?}", event); });
            posts_stream
                .inspect(|event: &PostEvent| { println!("{:?}", event); });
        });

    }).expect("Timely computation failed somehow");
}
