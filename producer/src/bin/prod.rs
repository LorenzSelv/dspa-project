#![allow(non_snake_case)]
#[macro_use]
extern crate lazy_static;

mod utils;
use utils::prod_utils::{Event, EventStream};

extern crate rand;
use rand::Rng;

extern crate rdkafka;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

extern crate chrono;

extern crate config;

use std::{thread, time};
use std::sync::mpsc;

lazy_static! {
    static ref SETTINGS: config::Config = {
        let mut s = config::Config::default();
        s.merge(config::File::with_name("../Settings")).unwrap();
        s
    };
    static ref TOPIC: String = SETTINGS.get::<String>("TOPIC").unwrap();
    static ref DELAY_PROB: f64 = SETTINGS.get::<f64>("DELAY_PROB").unwrap();
    static ref MAX_DELAY_SEC: u64 = SETTINGS.get::<u64>("MAX_DELAY_SEC").unwrap();
    static ref SPEEDUP_FACTOR: u64 = SETTINGS.get::<u64>("SPEEDUP_FACTOR").unwrap();
    static ref NUM_PARTITIONS: i32 = SETTINGS.get::<i32>("NUM_PARTITIONS").unwrap();
}

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    assert!(args.len() == 2, "specify dataset as command line arg");
    let dataset = &args[1];
    println!("dataset is {}", dataset);

    let prod1: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("produce.offset.report", "true")
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let prod2 = prod1.clone();

    let (tx, rx) = mpsc::channel::<(Event, i32)>();

    let handle = thread::spawn(move || {
        let delay = time::Duration::from_millis(*MAX_DELAY_SEC*1000 / *SPEEDUP_FACTOR); // TODO wrapper
        loop {
            thread::sleep(delay);
            while let Ok((event, partition)) = rx.try_recv() {
                println!("[delayed] event at {} is -- {:?}", event.creation_date, event);
                prod1.send(
                    FutureRecord::to(&TOPIC)
                        .partition(partition)
                        .payload(&event.payload)
                        .key("key"),
                        -1
                );
            }
        }
    });

    let event_stream = EventStream::new(
        dataset.clone() + "posts_event_stream.csv",
        dataset.clone() + "likes_event_stream.csv",
        dataset.clone() + "comments_event_stream.csv",
    );

    let mut rng = rand::thread_rng();
    let mut prev_timestamp = None;

    // Round robin on the partition to send the record to
    let mut partition = 0;

    let mut prev_was_delayed = false;

    for event in event_stream {
        let delta = 
            if let Some(pt) = prev_timestamp {
                assert!(event.timestamp >= pt);
                (event.timestamp - pt) * 1000 / *SPEEDUP_FACTOR
            } else { 0 };

        prev_timestamp = Some(event.timestamp);
        thread::sleep(time::Duration::from_millis(delta));

        // do not delay watermarks
        if event.is_watermark {
            for p in 0..*NUM_PARTITIONS {
                prod2.send(
                    FutureRecord::to(&TOPIC)
                        .partition(p)
                        .payload(&event.payload)
                        .key("key"),
                    -1
                );
            }
            continue;
        }

        if !prev_was_delayed && rng.gen_range(0.0, 1.0) < *DELAY_PROB {
            prev_was_delayed = true;
            tx.send((event, partition)).unwrap();
        } else {
            prev_was_delayed = false;
            println!("[ontime]  event at {} is -- {:?}", event.creation_date, event);
            prod2.send(
                FutureRecord::to(&TOPIC)
                    .partition(partition)
                    .payload(&event.payload)
                    .key("key"),
                    -1
            );
        }

        partition += 1;
        partition = partition % *NUM_PARTITIONS;
    }

    drop(tx);
    handle.join().unwrap();
}
