#[derive(Debug,Deserialize,Clone)]
pub struct LikeEvent {
    person_id: u64,
    post_id: u64,
    creation_date: chrono::DateTime<chrono::Utc>
}

#[derive(Debug,Deserialize,Clone)]
pub struct CommentEvent {
    comment_id: u64,
    person_id: u64,
    creation_date: chrono::DateTime<chrono::Utc>,
    location_ip: String,
    browser_used: String,
    content: String,
    reply_to_post_id: Option<u64>,
    reply_to_comment_id: Option<u64>,
    place_id: u64
}

#[derive(Debug,Deserialize,Clone)]
pub struct PostEvent {
    post_id: u64,
    person_id: u64,
    creation_date: chrono::DateTime<chrono::Utc>,
    image_file: Option<String>,
    location_ip: String,
    browser_used: String,
    language: Option<String>,
    content: Option<String>,
    tags: Option<String>, // TODO should be Vec<u64>>
    forum_id: u64,
    place_id: u64
}
