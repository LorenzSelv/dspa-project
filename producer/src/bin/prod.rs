extern crate rand;
use rand::Rng;

extern crate rdkafka;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

extern crate chrono;
use chrono::{DateTime,Utc,TimeZone,Duration};

use std::{thread, time};
use std::sync::mpsc;
use std::io::{BufRead, BufReader};
use std::fs::File;
use std::cmp::min;

const TOPIC: &'static str = "events";

const DELAY_PROB: f64 = 0.0;
const MAX_DELAY_SECONDS: u64 = 0;
const SPEEDUP_FACTOR: u64 = 600;
const WATERMARK_INTERVAL: u64 = 10*60; // every 10 minutes

#[derive(Debug,Clone,Ord,PartialOrd,PartialEq,Eq)]
struct Event {
    timestamp: u64,
    payload: String,
    creation_date: DateTime<Utc>,
}

impl Event {
    fn from_record(record: String) -> Event {
        // Creation date is always the 3rd field
        let date_str = record.split("|").collect::<Vec<_>>()[2].trim();
        let date = Utc.datetime_from_str(date_str, "%FT%TZ")
               .or(Utc.datetime_from_str(date_str, "%FT%T%.3fZ")).expect("failed to parse");

        Event {
            payload: record.trim().to_string(),
            creation_date: date,
            timestamp: date.timestamp() as u64
        }
    }
}

fn read_event(reader: &mut BufReader<File>) -> Option<Event> {
    let mut record = String::new();
    match reader.read_line(&mut record) {
        Err(err) => { println!("ERR {}", err); None },
        Ok(0)    => { println!("EOF"); None },
        Ok(_)    => Some(Event::from_record(record))
    }
}

struct EventStream {
    posts_stream_reader: BufReader<File>,
    likes_stream_reader: BufReader<File>,
    comments_stream_reader: BufReader<File>,

    // next event for each stream
    post_event: Option<Event>,
    like_event: Option<Event>,
    comment_event: Option<Event>,
    watermark_event: Option<Event>
}

impl EventStream {

    fn new(posts_fn: String, likes_fn: String, comments_fn: String) -> EventStream {
        let get_reader = |name| BufReader::new(File::open(&name)
                                               .expect(&format!("file {:?} not found", name)));
        let mut res = EventStream {
            posts_stream_reader: get_reader(posts_fn),
            likes_stream_reader: get_reader(likes_fn),
            comments_stream_reader: get_reader(comments_fn),
            post_event: None,
            like_event: None,
            comment_event: None,
            watermark_event: None
        };

        // discard headers
        let mut buf = String::new();
        res.posts_stream_reader.read_line(&mut buf).unwrap();
        res.likes_stream_reader.read_line(&mut buf).unwrap();
        res.comments_stream_reader.read_line(&mut buf).unwrap();
        res
    }
}

fn next_watermark(cur: &Event) -> Option<Event> {
    let duration = Duration::seconds(WATERMARK_INTERVAL as i64);
    let date = cur.creation_date.checked_add_signed(duration).unwrap();
    Some(Event {
        timestamp: cur.timestamp + WATERMARK_INTERVAL,
        payload: format!("WATERMARK|{}", date.timestamp()),
        creation_date: date
    })
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
            self.watermark_event = next_watermark(res.as_ref().unwrap());
        }

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
        loop {
            thread::sleep(delay);
            while let Ok(event) = rx.try_recv() {
                println!("event is -- {:?}", event.creation_date);
                prod1.send(
                    FutureRecord::to(TOPIC)
                        .partition(0) // TODO
                        .payload(&event.payload)
                        .key("key"),
                        -1
                 );
            }
        }
    });

    let mut rng = rand::thread_rng();

    let dataset = &std::env::args().collect::<Vec<_>>()[1];
    println!("dataset is {}", dataset);

    let posts_csv = dataset.clone() + "posts_event_stream.csv";
    let likes_csv = dataset.clone() + "likes_event_stream.csv";
    let comments_csv = dataset.clone() + "comments_event_stream.csv";

    let event_stream = EventStream::new(
        posts_csv,
        likes_csv,
        comments_csv
    );

    let mut prev_timestamp = None;

    for event in event_stream {

        let delta = match prev_timestamp {
            None => 0,
            Some(pt) => {
                assert!(event.timestamp >= pt);
                (event.timestamp - pt) * 1000 / SPEEDUP_FACTOR
            }
        };

        prev_timestamp = Some(event.timestamp);
        thread::sleep(time::Duration::from_millis(delta));

        if rng.gen_range(0.0, 1.0) < DELAY_PROB {
            tx.send(event).unwrap();
        } else {
            println!("event is -- {:?}", event);
            prod2.send(
                FutureRecord::to(TOPIC)
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
