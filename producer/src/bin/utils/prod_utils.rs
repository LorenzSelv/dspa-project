use std::io::{BufRead, BufReader};
use std::fs::File;

extern crate chrono;
use chrono::{DateTime, Duration, Utc, TimeZone};



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
        Ok(0)    => { println!("EOF"); None },
        Ok(_)    => Some(Event::from_record(record))
    }
}

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
