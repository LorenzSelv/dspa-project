use std::string::ToString;

#[derive(Debug,Clone)]
pub enum Event {
    Post(PostEvent),
    Like(LikeEvent),
    Comment(CommentEvent)
}

impl Event {
    pub fn timestamp(&self) -> u64 {
        match self {
            Event::Post(post) => post.creation_date.timestamp() as u64,
            Event::Like(like) => like.creation_date.timestamp() as u64,
            Event::Comment(comm) => comm.creation_date.timestamp() as u64
        }
    }

    pub fn id(&self) -> Option<u64> {
        match self {
            Event::Post(post) => Some(post.post_id),
            Event::Like(_like) => None, // like has no id
            Event::Comment(comm) => Some(comm.comment_id)
        }
    }

    pub fn person_id(&self) -> u64 {
        match self {
            Event::Post(post) => post.person_id,
            Event::Like(like) => like.person_id,
            Event::Comment(comm) => comm.person_id
        }
    }
}

impl ToString for Event {
    fn to_string(&self) -> String {
        match self {
            Event::Post(post) => post.to_string(),
            Event::Like(like) => like.to_string(),
            Event::Comment(comm) => comm.to_string(),
        }
    }
}

#[derive(Debug,Deserialize,Clone)]
pub struct LikeEvent {
    pub person_id: u64,
    pub post_id: u64,
    pub creation_date: chrono::DateTime<chrono::Utc>
}

impl ToString for LikeEvent {
    fn to_string(&self) -> String {
        format!("like at timestamp {} -- to post_id = {}, by = {}, date = {}",
                self.creation_date.timestamp(),
                self.post_id,
                self.person_id,
                self.creation_date)
    }
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

impl ToString for CommentEvent {
    fn to_string(&self) -> String {
        format!("comment at timestamp {} -- id = {}, reply_to_{} = {}, by = {}, date = {}",
                self.creation_date.timestamp(),
                self.comment_id,
                if self.reply_to_post_id != None { "post" } else { "comment" },
                self.reply_to_post_id.or(self.reply_to_comment_id).unwrap(),
                self.person_id,
                self.creation_date)
    }
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

impl ToString for PostEvent {
    fn to_string(&self) -> String {
        format!("post at timestamp {} -- id = {}, by = {}, date = {}",
                self.creation_date.timestamp(),
                self.post_id,
                self.person_id,
                self.creation_date)
    }
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
