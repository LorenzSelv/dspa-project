#[derive(Debug,Clone)]
pub enum Event {
    Post(PostEvent),
    Like(LikeEvent),
    Comment(CommentEvent)
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
    pub tags: Option<String>, // TODO should be Vec<u64>> #[serde(flatten)]
    pub forum_id: u64,
    pub place_id: u64
}


pub fn deserialize(record: String) -> Event {
    let reader = || csv::ReaderBuilder::new()
                        .has_headers(false)
                        .delimiter(b'|')
                        .from_reader(record.as_bytes());

    // Note: the order below matters, if you try to deserialize a `LikeEvent`
    //       given a post record it would succeed. Maybe there is a way to enforce
    //       a perfect match of the fields of the struct?
    if let Ok(post) = reader().deserialize::<PostEvent>().next().unwrap() {
        Event::Post(post)
    } else if let Ok(comment) = reader().deserialize::<CommentEvent>().next().unwrap() {
        Event::Comment(comment)
    } else if let Ok(like) = reader().deserialize::<LikeEvent>().next().unwrap() {
        Event::Like(like)
    } else {
        panic!("could not deserialize record {:?}", record);
    }
}
