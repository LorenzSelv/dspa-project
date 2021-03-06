#![allow(non_snake_case)]
#[macro_use]
extern crate lazy_static;

mod utils;
use utils::test_prod_utils::TestEventStream;

extern crate rdkafka;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

extern crate chrono;

extern crate config;

use std::{thread, time};

lazy_static! {
    static ref SETTINGS: config::Config = {
        let mut s = config::Config::default();
        s.merge(config::File::with_name("../Settings")).unwrap();
        s
    };
    static ref TOPIC: String = SETTINGS.get::<String>("TOPIC").unwrap();
    static ref SPEEDUP_FACTOR: u64 = SETTINGS.get::<u64>("SPEEDUP_FACTOR").unwrap();
    static ref NUM_PARTITIONS: i32 = SETTINGS.get::<i32>("NUM_PARTITIONS").unwrap();
}

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    assert!(args.len() == 2, "specify dataset as command line arg");
    let dataset = &args[1];
    println!("dataset is {}", dataset);

    let prod: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("produce.offset.report", "true")
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let event_stream = TestEventStream::new(
        dataset.clone() + "posts_event_stream.csv",
        dataset.clone() + "likes_event_stream.csv",
        dataset.clone() + "comments_event_stream.csv",
    );

    let mut prev_timestamp = None;

    // Round robin on the partition to send the record to
    let mut partition = 0;

    // change to fake_event_stream.
    for (time, event) in event_stream {
        let timestamp: u64 = time.timestamp() as u64;
        let delta =
            if let Some(pt) = prev_timestamp {
                assert!(timestamp >= pt);
                (timestamp - pt) * 1000 / *SPEEDUP_FACTOR
            } else { 0 };

        prev_timestamp = Some(timestamp);
        thread::sleep(time::Duration::from_millis(delta));

        if event.is_watermark {
            for p in 0..*NUM_PARTITIONS {
                prod.send(
                    FutureRecord::to(&TOPIC)
                        .partition(p)
                        .payload(&event.payload)
                        .key("key"),
                    -1
                );
//                println!("sent event at {} to partition {} -- {:?}",
//                         event.creation_date,
//                         p,
//                         event);
            }
        } else {
            prod.send(
                FutureRecord::to(&TOPIC)
                    .partition(partition)
                    .payload(&event.payload)
                    .key("key"),
                -1
            );
            println!("sent event at {} to partition {} -- {:?}",
                     event.creation_date,
                     partition,
                     event);
        }

        partition += 1;
        partition = partition % *NUM_PARTITIONS;
    }
}
