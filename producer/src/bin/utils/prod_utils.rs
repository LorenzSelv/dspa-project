#![allow(non_snake_case)]
extern crate lazy_static;

use std::io::{BufRead, BufReader};
use std::fs::File;

extern crate chrono;
use chrono::{DateTime, Duration, Utc, TimeZone};

use std::cmp::min;

lazy_static! {
    static ref SETTINGS: config::Config = {
        let mut s = config::Config::default();
        s.merge(config::File::with_name("../Settings")).unwrap();
        s
    };
    static ref WATERMARK_INTERVAL_MIN: u64 = SETTINGS.get::<u64>("WATERMARK_INTERVAL_MIN").unwrap();
}

#[derive(Debug,Clone,Ord,PartialOrd,PartialEq,Eq)]
pub struct Event {
    pub timestamp: u64,
    pub payload: String,
    pub creation_date: DateTime<Utc>,
    pub is_watermark: bool
}

impl Event {
    pub fn from_record(record: String) -> Event {
        // Creation date is always the 3rd field
        let date_str = record.split("|").collect::<Vec<_>>()[2].trim();
        let date = Utc.datetime_from_str(date_str, "%FT%TZ")
               .or(Utc.datetime_from_str(date_str, "%FT%T%.3fZ")).expect("failed to parse");

        Event {
            payload: record.trim().to_string(),
            creation_date: date,
            timestamp: date.timestamp() as u64,
            is_watermark: false
        }
    }
}

pub fn read_event(reader: &mut BufReader<File>) -> Option<Event> {
    let mut record = String::new();
    match reader.read_line(&mut record) {
        Err(err) => { println!("ERR {}", err); None },
        Ok(0)    => { None },
        Ok(1)    => { read_event(reader) },
        Ok(_)    => { Some(Event::from_record(record)) }
    }
}

#[allow(dead_code)]
pub struct EventStream {
    pub posts_stream_reader: BufReader<File>,
    pub likes_stream_reader: BufReader<File>,
    pub comments_stream_reader: BufReader<File>,

    // next event for each stream
    pub post_event: Option<Event>,
    pub like_event: Option<Event>,
    pub comment_event: Option<Event>,
    pub watermark_event: Option<Event>
}

impl EventStream {
    #[allow(dead_code)]
    pub fn new(posts_fn: String, likes_fn: String, comments_fn: String) -> EventStream {
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

pub fn next_watermark(cur: &Event, interval: u64) -> Option<Event> {
    let duration = Duration::minutes(interval as i64);
    let date = cur.creation_date.checked_add_signed(duration).unwrap();
    Some(Event {
        timestamp: cur.timestamp + interval*60,
        payload: format!("WATERMARK|{}", date.timestamp()),
        creation_date: date,
        is_watermark: true
    })
}

impl Iterator for EventStream {
    type Item = Event;

    fn next(&mut self) -> Option<Event> {
        // If an event "slot" is None, try to fill it by reading a new record
        if self.post_event == None { self.post_event = read_event(&mut self.posts_stream_reader); }
        if self.like_event == None { self.like_event = read_event(&mut self.likes_stream_reader); }
        if self.comment_event == None {
            self.comment_event = read_event(&mut self.comments_stream_reader); }

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
