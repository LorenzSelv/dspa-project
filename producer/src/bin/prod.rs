#![allow(non_snake_case)]
#[macro_use]
extern crate lazy_static;

mod utils;
use utils::prod_utils::{Event, EventStream, read_event, next_watermark};

extern crate rand;
use rand::Rng;

extern crate rdkafka;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

extern crate chrono;

extern crate config;

use std::{thread, time};
use std::sync::mpsc;
use std::cmp::min;

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
    static ref WATERMARK_INTERVAL_MIN: u64 = SETTINGS.get::<u64>("WATERMARK_INTERVAL_MIN").unwrap();
}

impl Iterator for EventStream {
    type Item = Event;

    fn next(&mut self) -> Option<Event> {
        // If an event "slot" is None, try to fill it by reading a new record
        if self.post_event == None { self.post_event = read_event(&mut self.posts_stream_reader); }
        if self.like_event == None { self.like_event = read_event(&mut self.likes_stream_reader); }
        if self.comment_event == None { self.comment_event = read_event(&mut self.comments_stream_reader); }

        let mut res: Option<Event> = None;

        let mut maybe_update = |other: &Event| {
            if let Some(r) = res.clone() { res = Some(min(r, other.clone())); }
            else { res = Some(other.clone()); }
        };

        // Find the event that happened at the earliest time
        if let Some(p) = &self.post_event      { maybe_update(p); }
        if let Some(l) = &self.like_event      { maybe_update(l); }
        if let Some(c) = &self.comment_event   { maybe_update(c); }
        if let Some(w) = &self.watermark_event { maybe_update(w); }

        // Consume the event
        if self.post_event == res    { self.post_event = None; }
        if self.like_event == res    { self.like_event = None; }
        if self.comment_event == res { self.comment_event = None; }


        // If watermark was sent or not init, advance to next watermark
        if self.watermark_event == None || self.watermark_event == res {
            self.watermark_event = next_watermark(res.as_ref().unwrap(),
                                                  *WATERMARK_INTERVAL_MIN);
        }

        res
    }
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

    let (tx, rx) = mpsc::channel::<Event>();

    let handle = thread::spawn(move || {
        let delay = time::Duration::from_millis(*MAX_DELAY_SEC*1000 / *SPEEDUP_FACTOR); // TODO wrapper
        loop {
            thread::sleep(delay);
            while let Ok(event) = rx.try_recv() {
                println!("event is -- {:?}", event.creation_date);
                prod1.send(
                    FutureRecord::to(&TOPIC)
                        .partition(0) // TODO
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
            prod2.send(
                FutureRecord::to(&TOPIC)
                    .partition(0) // TODO
                    .payload(&event.payload)
                    .key("key"),
                    -1
            );
            continue;
        }

        if !prev_was_delayed && rng.gen_range(0.0, 1.0) < *DELAY_PROB {
            prev_was_delayed = true;
            tx.send(event).unwrap();
        } else {
            prev_was_delayed = false;
            println!("event is -- {:?}", event);
            prod2.send(
                FutureRecord::to(&TOPIC)
                    .partition(0) // TODO
                    .payload(&event.payload)
                    .key("key"),
                    -1
            );
        }
    }

    drop(tx);
    handle.join().unwrap();
}
