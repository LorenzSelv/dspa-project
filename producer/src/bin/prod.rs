extern crate rand;
use rand::Rng;

extern crate rdkafka;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

extern crate chrono;
use chrono::{DateTime,Utc,TimeZone};

use std::{thread, time};
use std::sync::mpsc;
use std::io::{BufRead, BufReader};
use std::fs::File;
use std::cmp::min;

const POSTS_TOPIC: &'static str = "posts_events";
const LIKES_TOPIC: &'static str = "likes_events";
const COMMENTS_TOPIC: &'static str = "comments_events";

const DELAY_PROB: f64 = 0.3;
const MAX_DELAY_SECONDS: u64 = 360;
const SPEEDUP_FACTOR: u64 = 3600;

#[derive(Debug,Clone,Ord,PartialOrd,PartialEq,Eq)]
struct Event {
    timestamp: i64,
    payload: String,
    creation_date: DateTime<Utc>,
    topic: &'static str,
//    partition: TODO here ?
}

impl Event {
    fn from_record(record: String, topic: &'static str) -> Event {
        // Creation date is always the 3rd field
        let date_str = record.split("|").collect::<Vec<_>>()[2].trim();
        let date = Utc.datetime_from_str(date_str, "%FT%TZ")
               .or(Utc.datetime_from_str(date_str, "%FT%T%.3fZ")).expect("failed to parse");

        Event {
            payload: record.trim().to_string(),
            creation_date: date,
            timestamp: date.timestamp(),
            topic: topic 
        }
    }
}

fn read_event(reader: &mut BufReader<File>, topic: &'static str) -> Option<Event> {
    let mut record = String::new();
    match reader.read_line(&mut record) {
        Err(err) => { println!("ERR {}", err); None },
        Ok(0)    => { println!("EOF"); None },
        Ok(_)    => Some(Event::from_record(record, topic))
    }
}

struct EventStream {
    posts_stream_reader: BufReader<File>,
    likes_stream_reader: BufReader<File>,
    comments_stream_reader: BufReader<File>,

    // next event for each stream
    post_event: Option<Event>,
    like_event: Option<Event>,
    comment_event: Option<Event>
}

impl EventStream {
    
    fn new(posts_fn: String, likes_fn: String, comments_fn: String) -> EventStream {
        let get_reader = |name| BufReader::new(File::open(&name)
                                               .expect(&format!("file {:?} not found", name)));
        EventStream {
            posts_stream_reader: get_reader(posts_fn),
            likes_stream_reader: get_reader(likes_fn),
            comments_stream_reader: get_reader(comments_fn),
            post_event: None,
            like_event: None,
            comment_event: None
        }
    }
}

impl Iterator for EventStream {
    type Item = Event;

    fn next(&mut self) -> Option<Event> {
        // If an event "slot" is None, try to fill it by reading a new record
        if let None = &self.post_event { self.post_event = read_event(&mut self.posts_stream_reader, POSTS_TOPIC); }
        if let None = &self.like_event { self.like_event = read_event(&mut self.likes_stream_reader, LIKES_TOPIC); }
        if let None = &self.comment_event { self.comment_event = read_event(&mut self.comments_stream_reader, COMMENTS_TOPIC); }

        let mut res: Option<Event> = None;

        let mut maybe_update = |other: &Event| {
            println!("maybe update other={}", other.creation_date);
            if let Some(r) = res.clone() { res = Some(min(r, other.clone())); }
            else { res = Some(other.clone()); }
        };

        // Find the event that happened at the earliest time
        if let Some(p) = &self.post_event    { maybe_update(p); }
        if let Some(l) = &self.like_event    { maybe_update(l); }
        if let Some(c) = &self.comment_event { maybe_update(c); }

        // Consume the event
        if self.post_event == res    { self.post_event = None; }
        if self.like_event == res    { self.like_event = None; }
        if self.comment_event == res { self.comment_event = None; }

        res
    }
}


fn main() {
    
    let prod1: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("produce.offset.report", "true")
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let prod2 = prod1.clone();

    let (tx, rx) = mpsc::channel::<Event>();

    let handle = thread::spawn(move || {

        let delay = time::Duration::from_millis(MAX_DELAY_SECONDS*1000 / SPEEDUP_FACTOR); // TODO wrapper
        let mut it = 0;
        loop {
            thread::sleep(delay);
            while let Ok(event) = rx.try_recv() {
//                println!("delayed (it={}): {:?}", it, event);
                prod1.send(
                    FutureRecord::to(event.topic)
                        .partition(0) // TODO
                        .payload(&event.payload)
                        .key("key"),
                        -1
                 );
            }
            it += 1;
        }
    });

    let mut rng = rand::thread_rng();
 
    let home = "/home/lorenzo/".to_string();//dspa/project/dataset/1k-users-sorted/streams/";
    let event_stream = EventStream::new(
        home.clone() + "posts_event_stream.csv",
        home.clone() + "likes_event_stream.csv",
        home.clone() + "comments_event_stream.csv"
    );

    let mut prev_timestamp = None;
    for event in event_stream {
        println!("event is -- {:?}", event.creation_date);

        let delta = match prev_timestamp {
            None => 0,
            Some(pt) => { 
                assert!(event.timestamp >= pt);
                (event.timestamp - pt) as u64 * 1000 / SPEEDUP_FACTOR
            }
        };

        prev_timestamp = Some(event.timestamp);
        thread::sleep(time::Duration::from_millis(delta));

        if rng.gen_range(0.0, 1.0) < DELAY_PROB {
            tx.send(event).unwrap();
        } else {
//            println!("ontime: {:?}", event);
            prod2.send(
                FutureRecord::to(event.topic)
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
