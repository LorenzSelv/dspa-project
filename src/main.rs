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

fn deserialize<T>(record: String) -> T 
    where for<'de> T: serde::Deserialize<'de>
{

    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .delimiter(b'|')
        .from_reader(record.as_bytes());

    // TODO do not crash on wrong format
    let res = rdr.deserialize::<T>().next().unwrap().expect("aaa");
    res
}

fn main() {

    let args = ::std::env::args();
    //args.next();

    timely::execute_from_args(args, |worker| {


        worker.dataflow::<u64,_,_>(|scope| {
            
            let likes_stream = 
                kafka::consumer::string_stream(&scope, "likes")
                    .map(|record: String| deserialize::<LikeEvent>(record));

            let comments_stream = 
                kafka::consumer::string_stream(&scope, "comments")
                    .map(|record: String| deserialize::<CommentEvent>(record));

            let posts_stream = 
                kafka::consumer::string_stream(&scope, "posts")
                    .map(|record: String| deserialize::<PostEvent>(record));

            likes_stream
                .inspect(|event: &LikeEvent| { println!("{:?}", event); });
            comments_stream
                .inspect(|event: &CommentEvent| { println!("{:?}", event); });
            posts_stream
                .inspect(|event: &PostEvent| { println!("{:?}", event); });
        });

    }).expect("Timely computation failed somehow");

}

