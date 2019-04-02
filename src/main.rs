extern crate kafkaesque;
extern crate rdkafka;
extern crate serde;
#[macro_use]
extern crate serde_derive;

extern crate timely;
use timely::dataflow::operators::{Concatenate, Inspect, Map};

mod event;
use event::{Event, LikeEvent, CommentEvent, PostEvent};

mod kafka;


fn main() {

    timely::execute_from_args(std::env::args(), |worker| {

        worker.dataflow::<u64,_,_>(|scope| {
            
            let likes_stream = 
                kafka::consumer::string_stream(scope, "likes")
                    .map(|record: String| event::deserialize::<LikeEvent>(record))
                    .map(|like| Event::Like(like.creation_date.timestamp() as u64, like));

            let comments_stream = 
                kafka::consumer::string_stream(scope, "comments")
                    .map(|record: String| event::deserialize::<CommentEvent>(record))
                    .map(|comment| Event::Comment(comment.creation_date.timestamp() as u64, comment));

            let posts_stream = 
                kafka::consumer::string_stream(scope, "posts")
                    .map(|record: String| event::deserialize::<PostEvent>(record))
                    .map(|post| Event::Post(post.creation_date.timestamp() as u64, post));

            let streams = vec![likes_stream, comments_stream, posts_stream];

            scope.concatenate(streams)
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
