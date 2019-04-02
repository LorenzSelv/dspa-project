// Generic event with timestamp (u64)
#[derive(Debug,Clone)]
pub enum Event {
    Post(u64, PostEvent),
    Like(u64, LikeEvent),
    Comment(u64, CommentEvent)
}

#[derive(Debug,Deserialize,Clone)]
pub struct LikeEvent {
    pub person_id: u64,
    pub post_id: u64,
    pub creation_date: chrono::DateTime<chrono::Utc>
}

#[derive(Debug,Deserialize,Clone)]
pub struct CommentEvent {
    pub comment_id: u64,
    pub person_id: u64,
    pub creation_date: chrono::DateTime<chrono::Utc>,
    pub location_ip: String,
    pub browser_used: String,
    pub content: String,
    pub reply_to_post_id: Option<u64>,
    pub reply_to_comment_id: Option<u64>,
    pub place_id: u64
}

#[derive(Debug,Deserialize,Clone)]
pub struct PostEvent {
    pub post_id: u64,
    pub person_id: u64,
    pub creation_date: chrono::DateTime<chrono::Utc>,
    pub image_file: Option<String>,
    pub location_ip: String,
    pub browser_used: String,
    pub language: Option<String>,
    pub content: Option<String>,
    pub tags: Option<String>, // TODO should be Vec<u64>>
    pub forum_id: u64,
    pub place_id: u64
}


pub fn deserialize<T>(record: String) -> T
    where for <'a> T: serde::Deserialize<'a>
{
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .delimiter(b'|')
        .from_reader(record.as_bytes());

    // TODO do not crash on wrong format
    reader.deserialize::<T>().next().unwrap()
        .expect(&format!("Could not deserialize record: {} ", record))
}
