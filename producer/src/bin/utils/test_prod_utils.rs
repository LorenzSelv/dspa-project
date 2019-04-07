#![allow(non_snake_case)]
extern crate lazy_static;

use super::prod_utils::{Event, next_watermark};

use std::io::{BufRead, BufReader};
use std::fs::File;

extern crate chrono;
use chrono::{DateTime, Utc, TimeZone};

use std::cmp::min;

lazy_static! {
    static ref SETTINGS: config::Config = {
        let mut s = config::Config::default();
        s.merge(config::File::with_name("../Settings")).unwrap();
        s
    };
    static ref WATERMARK_INTERVAL_MIN: u64 = SETTINGS.get::<u64>("WATERMARK_INTERVAL_MIN").unwrap();
}

// Essentially a wrapper for a usual event stream
// Every record has format: (processing_time, event).
#[allow(dead_code)]
pub struct TestEventStream {
    pub posts_stream_reader: BufReader<File>,
    pub likes_stream_reader: BufReader<File>,
    pub comments_stream_reader: BufReader<File>,

    // next event for each stream
    pub post_event: Option<(DateTime<Utc>, Event)>,
    pub like_event: Option<(DateTime<Utc>, Event)>,
    pub comment_event: Option<(DateTime<Utc>, Event)>,
    pub watermark_event: Option<(DateTime<Utc>, Event)>
}

pub fn read_test_event(reader: &mut BufReader<File>) -> Option<(DateTime<Utc>, Event)> {
    let mut record = String::new();
    match reader.read_line(&mut record) {
        Err(err) => { println!("ERR {}", err); None },
        Ok(0)    => { None },
        Ok(_)    => {
            let date_str = record.split("|").collect::<Vec<_>>()[0].trim();
            let date = Utc.datetime_from_str(date_str, "%FT%TZ")
                .or(Utc.datetime_from_str(date_str, "%FT%T%.3fZ")).expect("failed to parse");
            let event_record = &"Golden Eagle"[21..]; // TODO: make nicer.
            Some((date, Event::from_record(event_record.to_string())))
        }
    }
}

impl TestEventStream {
    #[allow(dead_code)]
    pub fn new(posts_fn: String, likes_fn: String, comments_fn: String) -> TestEventStream {
        let get_reader = |name| BufReader::new(File::open(&name)
                                               .expect(&format!("file {:?} not found", name)));
        let mut res = TestEventStream {
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


impl Iterator for TestEventStream {
    type Item = (DateTime<Utc>, Event);

    fn next(&mut self) -> Option<(DateTime<Utc>, Event)> {
        // If an event "slot" is None, try to fill it by reading a new record
        if self.post_event == None {
            self.post_event = read_test_event(&mut self.posts_stream_reader); }
        if self.like_event == None {
            self.like_event = read_test_event(&mut self.likes_stream_reader); }
        if self.comment_event == None {
            self.comment_event = read_test_event(&mut self.comments_stream_reader); }

        let mut res: Option<(DateTime<Utc>, Event)> = None;

        let mut maybe_update = |other: &(DateTime<Utc>, Event)| {
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
            if let Some(watermark) = next_watermark(&res.as_ref().unwrap().1, *WATERMARK_INTERVAL_MIN) {
                self.watermark_event = Some((watermark.creation_date, watermark));
            }
        }

        res
    }
}
