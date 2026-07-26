use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreatorSnapshot {
    pub handle: String,
    pub display_name: Option<String>,
    pub country_code: Option<String>,
    pub bio: String,
    pub follower_count: Option<u64>,
    pub following_count: Option<u64>,
    pub following: Vec<String>,
    pub videos: Vec<TiktokPost>,
    pub raw: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TiktokPost {
    pub id: Option<String>,
    pub url: String,
    pub caption: Option<String>,
    pub views: u64,
    pub published_at: Option<DateTime<Utc>>,
    pub kind: TiktokPostKind,
    pub is_pinned: bool,
    pub source_url: Option<String>,
    pub slide_image_urls: Vec<String>,
    pub visual_image_urls: Vec<String>,
    pub raw: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TiktokPostKind {
    Video,
    Photo,
}

impl TiktokPostKind {
    pub fn from_url_and_type(url: &str, kind_hint: Option<&str>) -> Self {
        let hint = kind_hint.unwrap_or_default().to_ascii_lowercase();
        let url = url.to_ascii_lowercase();
        if hint.contains("photo")
            || hint.contains("slideshow")
            || hint.contains("image")
            || url.contains("/photo/")
        {
            Self::Photo
        } else {
            Self::Video
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreatorStats {
    pub avg_views: f64,
    pub median_views: f64,
    pub most_viral_video_url: Option<String>,
    pub most_viral_video_views: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractedContent {
    pub text: String,
    pub language_code: String,
}

impl ExtractedContent {
    pub fn empty() -> Self {
        Self {
            text: String::new(),
            language_code: "UNKNOWN".to_string(),
        }
    }

    pub fn normalized(mut self) -> Self {
        self.language_code = self.language_code.trim().to_ascii_uppercase();
        if self.language_code.is_empty() {
            self.language_code = "UNKNOWN".to_string();
        }
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppClassification {
    pub promotes_app: bool,
    pub app_name: Option<String>,
    pub is_existing_app: bool,
    pub existing_app_name: Option<String>,
    pub is_new_app: bool,
    pub supporting_post_count: u8,
    pub language_code: String,
    pub creator_name: Option<String>,
    pub creator_email: Option<String>,
    pub confidence: f64,
    pub evidence: String,
}

impl AppClassification {
    pub fn no_app() -> Self {
        Self {
            promotes_app: false,
            app_name: None,
            is_existing_app: false,
            existing_app_name: None,
            is_new_app: false,
            supporting_post_count: 0,
            language_code: "UNKNOWN".to_string(),
            creator_name: None,
            creator_email: None,
            confidence: 0.0,
            evidence: String::new(),
        }
    }

    pub fn canonical_app_name(&self) -> Option<String> {
        self.existing_app_name
            .as_ref()
            .or(self.app_name.as_ref())
            .map(|name| name.trim().to_string())
            .filter(|name| !name.is_empty())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LanguageClassification {
    pub language_code: String,
    pub confidence: f64,
    pub evidence: String,
}

impl LanguageClassification {
    pub fn normalized(mut self) -> Self {
        self.language_code = self.language_code.trim().to_ascii_uppercase();
        if self.language_code.is_empty() {
            self.language_code = "UNKNOWN".to_string();
        }
        self.confidence = self.confidence.clamp(0.0, 1.0);
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrawledCreator {
    pub snapshot: CreatorSnapshot,
    pub stats: CreatorStats,
    pub latest_content: ExtractedContent,
    pub classification: AppClassification,
    pub scraped_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct QueueJob {
    pub handle: String,
    pub attempts: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueueItem {
    pub handle: String,
    pub discovered_from: Option<String>,
    pub found_at: String,
    pub status: String,
    pub attempts: u32,
    pub last_error: Option<String>,
}
