use abomonation;
use std::fmt;
use std::string::ToString;

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ID {
    Post(u64),
    Comment(u64),
}

impl Default for ID {
    fn default() -> Self { ID::Post(0) }
}

impl ID {
    #[allow(dead_code)]
    pub fn u64(&self) -> u64 {
        match self {
            ID::Post(id) => *id,
            ID::Comment(id) => *id,
        }
    }
}

impl fmt::Display for ID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ID::Post(id) => write!(f, "Post({})", id),
            ID::Comment(id) => write!(f, "Comment({})", id),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Event {
    Post(PostEvent),
    Like(LikeEvent),
    Comment(CommentEvent),
}

impl abomonation::Abomonation for Event {}

#[allow(dead_code)]
impl Event {
    pub fn timestamp(&self) -> u64 {
        match self {
            Event::Post(post) => post.creation_date.timestamp() as u64,
            Event::Like(like) => like.creation_date.timestamp() as u64,
            Event::Comment(comm) => comm.creation_date.timestamp() as u64,
        }
    }

    pub fn id(&self) -> Option<ID> {
        match self {
            Event::Post(post) => Some(post.post_id),
            Event::Like(_like) => None, // like has no id
            Event::Comment(comm) => Some(comm.comment_id),
        }
    }

    pub fn person_id(&self) -> u64 {
        match self {
            Event::Post(post) => post.person_id,
            Event::Like(like) => like.person_id,
            Event::Comment(comm) => comm.person_id,
        }
    }

    pub fn target_post_id(&self) -> u64 {
        match self {
            Event::Post(post) => post.post_id_u64,
            Event::Like(like) => like.post_id_u64,
            Event::Comment(comm) => {
                comm.reply_to_post_id_u64.or(comm.reply_to_comment_id_u64).unwrap()
            }
        }
    }
}

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Event::Post(post) => write!(f, "{}", post.to_string()),
            Event::Like(like) => write!(f, "{}", like.to_string()),
            Event::Comment(comm) => write!(f, "{}", comm.to_string()),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct LikeEvent {
    pub person_id: u64,

    #[serde(skip)]
    pub post_id: ID,
    pub post_id_u64: u64,

    pub creation_date: chrono::DateTime<chrono::Utc>,
}

impl LikeEvent {
    fn init(mut self) -> Self {
        self.post_id = ID::Post(self.post_id_u64);
        self
    }
}

impl ToString for LikeEvent {
    fn to_string(&self) -> String {
        format!(
            "like at timestamp {} -- to post_id = {}, by = {}, date = {}",
            self.creation_date.timestamp(),
            self.post_id,
            self.person_id,
            self.creation_date
        )
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct CommentEvent {
    #[serde(skip)]
    pub comment_id: ID,
    pub comment_id_u64: u64,

    pub person_id:     u64,
    pub creation_date: chrono::DateTime<chrono::Utc>,
    pub location_ip:   String,
    pub browser_used:  String,
    pub content:       String,

    #[serde(skip)]
    pub reply_to_post_id: Option<ID>,
    pub reply_to_post_id_u64: Option<u64>,

    #[serde(skip)]
    pub reply_to_comment_id: Option<ID>,
    pub reply_to_comment_id_u64: Option<u64>,

    pub place_id: u64,
}

impl CommentEvent {
    fn init(mut self) -> Self {
        self.comment_id = ID::Comment(self.comment_id_u64);
        if let Some(id) = self.reply_to_post_id_u64 {
            self.reply_to_post_id = Some(ID::Post(id));
        }
        if let Some(id) = self.reply_to_comment_id_u64 {
            self.reply_to_comment_id = Some(ID::Comment(id));
        }
        self
    }
}

impl ToString for CommentEvent {
    fn to_string(&self) -> String {
        format!(
            "comment at timestamp {} -- id = {}, reply_to_{} = {}, by = {}, date = {}",
            self.creation_date.timestamp(),
            self.comment_id,
            if self.reply_to_post_id != None { "post" } else { "comment" },
            self.reply_to_post_id.or(self.reply_to_comment_id).unwrap(),
            self.person_id,
            self.creation_date
        )
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct PostEvent {
    #[serde(skip)]
    pub post_id: ID,
    pub post_id_u64: u64,

    pub person_id:     u64,
    pub creation_date: chrono::DateTime<chrono::Utc>,
    pub image_file:    Option<String>,
    pub location_ip:   String,
    pub browser_used:  String,
    pub language:      Option<String>,
    pub content:       Option<String>,
    pub tags:          Option<String>, // TODO should be Vec<u64>> #[serde(flatten)]
    pub forum_id:      u64,
    pub place_id:      u64,
}

impl PostEvent {
    fn init(mut self) -> Self {
        self.post_id = ID::Post(self.post_id_u64);
        self
    }
}

impl ToString for PostEvent {
    fn to_string(&self) -> String {
        format!(
            "post at timestamp {} -- id = {}, by = {}, date = {}",
            self.creation_date.timestamp(),
            self.post_id,
            self.person_id,
            self.creation_date
        )
    }
}

pub fn deserialize(record: String) -> Event {
    let reader = || {
        csv::ReaderBuilder::new().has_headers(false).delimiter(b'|').from_reader(record.as_bytes())
    };

    // Note: the order below matters, if you try to deserialize a `LikeEvent`
    //       given a post record it would succeed. Maybe there is a way to enforce
    //       a perfect match of the fields of the struct?
    if let Ok(post) = reader().deserialize::<PostEvent>().next().unwrap() {
        Event::Post(post.init())
    } else if let Ok(comment) = reader().deserialize::<CommentEvent>().next().unwrap() {
        Event::Comment(comment.init())
    } else if let Ok(like) = reader().deserialize::<LikeEvent>().next().unwrap() {
        Event::Like(like.init())
    } else {
        panic!("could not deserialize record {:?}", record);
    }
}
