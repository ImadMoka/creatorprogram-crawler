use std::{fs, path::PathBuf};

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use reqwest::Url;
use serde_json::{Value, json};
use tokio::time::{Duration, sleep};
use tracing::warn;

use crate::{
    config::{SocialProfileScraperConfig, TikTokScraperConfig, TikTokScraperProvider},
    handles::normalize_handle,
    models::{CreatorSnapshot, TiktokPost, TiktokPostKind},
};

const SCRAPER_REQUEST_ATTEMPTS: usize = 3;
const SCRAPER_RETRY_BASE_DELAY_MS: u64 = 500;

#[async_trait]
pub trait TikTokScraper: Send + Sync {
    async fn fetch_creator(&self, handle: &str) -> Result<CreatorSnapshot>;
    async fn fetch_country_code(&self, handle: &str) -> Result<Option<String>>;
}

pub fn scraper_from_config(config: TikTokScraperConfig) -> Result<Box<dyn TikTokScraper>> {
    match config.provider {
        TikTokScraperProvider::SocialProfileScraper(scraper) => Ok(Box::new(
            SocialProfileScraper::new(scraper, config.timeout)?,
        )),
        TikTokScraperProvider::Fixture(path) => Ok(Box::new(FixtureTikTokScraper::new(path))),
    }
}

pub struct SocialProfileScraper {
    client: reqwest::Client,
    config: SocialProfileScraperConfig,
}

impl SocialProfileScraper {
    pub fn new(config: SocialProfileScraperConfig, timeout: std::time::Duration) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .user_agent("creatorprogram-crawler/0.1")
            .build()
            .context("failed to build social profile scraper HTTP client")?;
        Ok(Self { client, config })
    }

    async fn get_json(&self, path: &str, mut params: Vec<(&str, String)>) -> Result<Value> {
        if let Some(region) = &self.config.region {
            params.push(("region", region.clone()));
        }

        let mut last_error = None;
        for attempt in 1..=SCRAPER_REQUEST_ATTEMPTS {
            match self.get_json_once(path, params.clone()).await {
                Ok(value) => return Ok(value),
                Err(error)
                    if attempt < SCRAPER_REQUEST_ATTEMPTS
                        && is_transient_scraper_request_error(&error) =>
                {
                    warn!(
                        path = %path,
                        attempt,
                        max_attempts = SCRAPER_REQUEST_ATTEMPTS,
                        error = %format!("{error:#}"),
                        "retrying transient social profile scraper request failure"
                    );
                    last_error = Some(error);
                    sleep(Duration::from_millis(
                        SCRAPER_RETRY_BASE_DELAY_MS * attempt as u64,
                    ))
                    .await;
                }
                Err(error) => return Err(error),
            }
        }

        Err(last_error.expect("retry loop should keep the last scraper error"))
    }

    async fn get_json_once(&self, path: &str, params: Vec<(&str, String)>) -> Result<Value> {
        let endpoint = format!(
            "{}/{}",
            self.config.base_url.trim_end_matches('/'),
            path.trim_start_matches('/')
        );
        let url =
            Url::parse(&endpoint).with_context(|| format!("invalid scraper URL: {endpoint}"))?;
        let response = self
            .client
            .get(url)
            .query(&params)
            .send()
            .await
            .context("failed to call social profile scraper")?;

        let status = response.status();
        let body = response
            .text()
            .await
            .context("failed to read social profile scraper response body")?;
        if !status.is_success() {
            anyhow::bail!("social profile scraper returned {status}: {body}");
        }

        let value = serde_json::from_str::<Value>(&body)
            .context("social profile scraper returned invalid JSON")?;
        if value.get("success").and_then(Value::as_bool) == Some(false) {
            let message =
                first_string(&value, &["message", "error"]).unwrap_or_else(|| body.clone());
            anyhow::bail!("social profile scraper returned an application error: {message}");
        }
        if value
            .get("status")
            .and_then(Value::as_str)
            .is_some_and(|status| status.eq_ignore_ascii_case("error"))
        {
            let message = first_string(&value, &["message", "error", "error.message"])
                .unwrap_or_else(|| body.clone());
            anyhow::bail!("social profile scraper returned an application error: {message}");
        }

        Ok(value)
    }

    async fn fetch_following_pages(&self, handle: &str) -> Result<Vec<Value>> {
        let mut pages = Vec::new();
        let mut min_time: Option<String> = None;

        for _ in 0..self.config.max_following_pages {
            let mut params = vec![("handle", handle.to_string()), ("trim", "true".to_string())];
            if let Some(cursor) = &min_time {
                params.push(("min_time", cursor.clone()));
            }

            let page = match self.get_json("/v1/tiktok/user/following", params).await {
                Ok(page) => page,
                Err(error) if is_nonfatal_following_error(&error) => {
                    warn!(
                        handle = %handle,
                        error = %format!("{error:#}"),
                        "continuing creator crawl without following graph"
                    );
                    return Ok(pages);
                }
                Err(error) => return Err(error),
            };
            let has_more = first_bool(&page, &["hasMore", "has_more"]).unwrap_or(false);
            min_time = first_string(&page, &["min_time", "minTime"])
                .filter(|cursor| cursor != "0" && !cursor.trim().is_empty());
            pages.push(page);

            if !has_more || min_time.is_none() {
                break;
            }
        }

        Ok(pages)
    }

    async fn fetch_profile_region(&self, handle: &str) -> Result<Value> {
        self.get_json(
            "/v1/tiktok/profile/region",
            vec![("handle", handle.to_string())],
        )
        .await
    }
}

#[async_trait]
impl TikTokScraper for SocialProfileScraper {
    async fn fetch_creator(&self, handle: &str) -> Result<CreatorSnapshot> {
        let normalized = normalize_handle(handle).context("invalid TikTok handle")?;
        let profile = self
            .get_json("/v1/tiktok/profile", vec![("handle", normalized.clone())])
            .await
            .with_context(|| format!("failed to fetch scraper profile for @{normalized}"))?;

        let posts_request = self.get_json(
            "/v1/tiktok/profile/videos",
            vec![
                ("handle", normalized.clone()),
                ("trim", "true".to_string()),
                ("count", "30".to_string()),
                ("include_download_headers", "true".to_string()),
            ],
        );
        let following_request = self.fetch_following_pages(&normalized);
        let region_request = self.fetch_profile_region(&normalized);
        let (posts, following_pages, profile_region) =
            tokio::join!(posts_request, following_request, region_request);

        let posts =
            posts.with_context(|| format!("failed to fetch scraper posts for @{normalized}"))?;
        let following_pages = following_pages
            .with_context(|| format!("failed to fetch scraper following for @{normalized}"))?;
        let profile_region = match profile_region {
            Ok(region) => Some(region),
            Err(error) => {
                warn!(
                    handle = %normalized,
                    error = %format!("{error:#}"),
                    "continuing creator crawl without profile region"
                );
                None
            }
        };

        snapshot_from_social_values(&normalized, profile, posts, following_pages, profile_region)
    }

    async fn fetch_country_code(&self, handle: &str) -> Result<Option<String>> {
        let normalized = normalize_handle(handle).context("invalid TikTok handle")?;
        let region = self
            .fetch_profile_region(&normalized)
            .await
            .with_context(|| format!("failed to fetch scraper profile region for @{normalized}"))?;
        Ok(
            first_string(&region, &["region", "country_code", "countryCode"])
                .and_then(|value| normalize_country_code(&value)),
        )
    }
}

pub struct FixtureTikTokScraper {
    path: PathBuf,
}

impl FixtureTikTokScraper {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

#[async_trait]
impl TikTokScraper for FixtureTikTokScraper {
    async fn fetch_creator(&self, handle: &str) -> Result<CreatorSnapshot> {
        let normalized = normalize_handle(handle).context("invalid TikTok handle")?;
        let path = if self.path.is_dir() {
            self.path.join(format!("{normalized}.json"))
        } else {
            self.path.clone()
        };
        let json = fs::read_to_string(&path)
            .with_context(|| format!("failed to read fixture {}", path.display()))?;
        let value = serde_json::from_str::<Value>(&json)
            .with_context(|| format!("invalid fixture JSON {}", path.display()))?;
        parse_creator_snapshot(&normalized, value)
    }

    async fn fetch_country_code(&self, handle: &str) -> Result<Option<String>> {
        Ok(self.fetch_creator(handle).await?.country_code)
    }
}

fn snapshot_from_social_values(
    fallback_handle: &str,
    profile: Value,
    posts: Value,
    following_pages: Vec<Value>,
    profile_region: Option<Value>,
) -> Result<CreatorSnapshot> {
    let user = profile
        .pointer("/user")
        .or_else(|| profile.pointer("/userInfo/user"))
        .or_else(|| profile.pointer("/author"))
        .or_else(|| profile.pointer("/profile/user"))
        .context("scraper profile did not include a user object")?;

    let handle = first_string(user, &["unique_id", "uniqueId", "username", "handle"])
        .and_then(|value| normalize_handle(&value))
        .unwrap_or_else(|| fallback_handle.to_string());
    let bio = first_string(user, &["signature", "bio", "description"]).unwrap_or_default();
    let display_name = first_string(user, &["nickname", "display_name", "displayName", "name"]);
    let country_code = first_string(user, &["region", "country_code", "countryCode"])
        .or_else(|| first_string(&profile, &["region", "country_code", "countryCode"]))
        .or_else(|| {
            profile_region
                .as_ref()
                .and_then(|region| first_string(region, &["region", "country_code", "countryCode"]))
        })
        .and_then(|value| normalize_country_code(&value));
    let follower_count = parse_profile_follower_count(&profile);
    let following_count = parse_profile_following_count(&profile);

    let mut following = following_pages
        .iter()
        .flat_map(parse_following)
        .collect::<Vec<_>>();
    following.sort();
    following.dedup();

    let mut videos = parse_videos(&posts);
    videos.sort_by(|a, b| b.published_at.cmp(&a.published_at));
    videos.truncate(30);

    Ok(CreatorSnapshot {
        handle,
        display_name,
        country_code,
        bio,
        follower_count,
        following_count,
        following,
        videos,
        raw: json!({
            "provider": "social_profile_scraper",
            "profile": profile,
            "profile_region": profile_region,
            "posts": posts,
            "following_pages": following_pages,
        }),
    })
}

fn parse_creator_snapshot(fallback_handle: &str, value: Value) -> Result<CreatorSnapshot> {
    if value.get("profile").is_some() && value.get("posts").is_some() {
        let profile = value.get("profile").cloned().unwrap_or(Value::Null);
        let posts = value.get("posts").cloned().unwrap_or(Value::Null);
        let following_pages = value
            .get("following_pages")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_else(|| {
                value
                    .get("following")
                    .map(|following| vec![json!({ "userList": following })])
                    .unwrap_or_default()
            });
        let profile_region = value.get("profile_region").cloned();
        return snapshot_from_social_values(
            fallback_handle,
            profile,
            posts,
            following_pages,
            profile_region,
        );
    }

    let root = unwrap_data(value);
    let user = first_object(
        &root,
        &[
            "creator",
            "user",
            "author",
            "profile",
            "userInfo.user",
            "profile.userInfo.user",
        ],
    )
    .unwrap_or(&root);

    let handle = first_string(
        user,
        &[
            "handle",
            "uniqueId",
            "unique_id",
            "username",
            "secUid",
            "nickname",
        ],
    )
    .and_then(|value| normalize_handle(&value))
    .unwrap_or_else(|| fallback_handle.to_string());

    let bio = first_string(
        user,
        &[
            "bio",
            "signature",
            "description",
            "profile.bio",
            "profile.signature",
        ],
    )
    .unwrap_or_default();

    let display_name = first_string(user, &["display_name", "displayName", "nickname", "name"]);
    let country_code = first_string(user, &["region", "country_code", "countryCode"])
        .or_else(|| first_string(&root, &["region", "country_code", "countryCode"]))
        .and_then(|value| normalize_country_code(&value));
    let follower_count = parse_profile_follower_count(&root);
    let following_count = parse_profile_following_count(&root);
    let following = parse_following(&root);
    let mut videos = parse_videos(&root);
    videos.sort_by(|a, b| b.published_at.cmp(&a.published_at));
    videos.truncate(30);

    Ok(CreatorSnapshot {
        handle,
        display_name,
        country_code,
        bio,
        follower_count,
        following_count,
        following,
        videos,
        raw: root,
    })
}

fn normalize_country_code(value: &str) -> Option<String> {
    let normalized = value.trim().to_ascii_uppercase();
    let normalized = match normalized.as_str() {
        "AR" => "SA",
        "BE" => "BY",
        "BS" => "BA",
        "CA" => "ES",
        "CS" => "CZ",
        "DA" => "DK",
        "EL" => "GR",
        "EN" => "GB",
        "ET" => "EE",
        "FA" => "IR",
        "HE" => "IL",
        "HI" => "IN",
        "JA" => "JP",
        "KK" => "KZ",
        "KO" => "KR",
        "MS" => "MY",
        "NB" | "NN" => "NO",
        "SL" => "SI",
        "SQ" => "AL",
        "SR" => "RS",
        "SV" => "SE",
        "TL" => "PH",
        "UK" => "UA",
        "UR" => "PK",
        "VI" => "VN",
        "ZH" => "CN",
        _ => normalized.as_str(),
    }
    .to_string();
    (normalized.len() == 2 && normalized.chars().all(|ch| ch.is_ascii_alphabetic()))
        .then_some(normalized)
}

fn parse_profile_follower_count(root: &Value) -> Option<u64> {
    first_u64(
        root,
        &[
            "follower_count",
            "followerCount",
            "followers",
            "followers_count",
            "stats.follower_count",
            "stats.followerCount",
            "statsV2.followerCount",
            "statsV2.follower_count",
            "user.follower_count",
            "user.followerCount",
            "user.followers",
            "user.stats.follower_count",
            "user.stats.followerCount",
            "userInfo.stats.followerCount",
            "userInfo.stats.follower_count",
            "profile.stats.followerCount",
            "profile.stats.follower_count",
        ],
    )
}

fn parse_profile_following_count(root: &Value) -> Option<u64> {
    first_u64(
        root,
        &[
            "following_count",
            "followingCount",
            "followings",
            "stats.following_count",
            "stats.followingCount",
            "statsV2.followingCount",
            "statsV2.following_count",
            "user.following_count",
            "user.followingCount",
            "user.followings",
            "user.stats.following_count",
            "user.stats.followingCount",
            "userInfo.stats.followingCount",
            "userInfo.stats.following_count",
            "profile.stats.followingCount",
            "profile.stats.following_count",
        ],
    )
}

fn unwrap_data(value: Value) -> Value {
    for key in ["data", "result", "response"] {
        if let Some(inner) = value.get(key) {
            return inner.clone();
        }
    }
    value
}

fn parse_following(root: &Value) -> Vec<String> {
    let arrays = [
        root.pointer("/following"),
        root.pointer("/followings"),
        root.pointer("/followers"),
        root.pointer("/userList"),
        root.pointer("/creator/following"),
        root.pointer("/user/following"),
        root.pointer("/author/following"),
    ];

    let mut handles = Vec::new();
    for array in arrays.into_iter().flatten().filter_map(Value::as_array) {
        for item in array {
            if let Some(handle) = parse_handle_value(item) {
                handles.push(handle);
            }
        }
    }
    handles.sort();
    handles.dedup();
    handles
}

fn parse_handle_value(value: &Value) -> Option<String> {
    if let Some(handle) = value.as_str() {
        return normalize_handle(handle);
    }

    for key in [
        "handle",
        "uniqueId",
        "unique_id",
        "username",
        "nickname",
        "user.uniqueId",
        "user.unique_id",
        "user.username",
    ] {
        if let Some(handle) = first_string(value, &[key])
            && let Some(normalized) = normalize_handle(&handle)
        {
            return Some(normalized);
        }
    }

    None
}

fn parse_videos(root: &Value) -> Vec<TiktokPost> {
    let arrays = [
        root.pointer("/aweme_list"),
        root.pointer("/awemeList"),
        root.pointer("/videos"),
        root.pointer("/posts"),
        root.pointer("/items"),
        root.pointer("/itemList"),
        root.pointer("/creator/videos"),
        root.pointer("/user/videos"),
    ];

    arrays
        .into_iter()
        .flatten()
        .filter_map(Value::as_array)
        .flat_map(|array| array.iter().filter_map(parse_post))
        .collect()
}

fn parse_post(value: &Value) -> Option<TiktokPost> {
    let slide_image_urls = parse_slide_images(value);
    let has_images = !slide_image_urls.is_empty()
        || value.get("imagePost").is_some()
        || value.get("image_post").is_some();
    let kind_hint = first_string(
        value,
        &[
            "kind",
            "type",
            "media_type",
            "mediaType",
            "aweme_type",
            "awemeType",
        ],
    );
    let provisional_kind = if has_images {
        TiktokPostKind::Photo
    } else {
        TiktokPostKind::Video
    };
    let url = first_string(
        value,
        &[
            "url",
            "webVideoUrl",
            "web_video_url",
            "share_url",
            "shareUrl",
            "video_url",
            "videoUrl",
            "share_info.share_url",
            "shareInfo.shareUrl",
        ],
    )
    .or_else(|| build_tiktok_post_url(value, provisional_kind))?;
    let kind = if has_images {
        TiktokPostKind::Photo
    } else {
        TiktokPostKind::from_url_and_type(&url, kind_hint.as_deref())
    };
    let source_url = Some(url.clone());

    Some(TiktokPost {
        id: first_string(value, &["id", "aweme_id", "awemeId", "video.id"]),
        url,
        caption: first_string(value, &["caption", "desc", "description", "text"]),
        views: first_u64(
            value,
            &[
                "views",
                "view_count",
                "viewCount",
                "play_count",
                "playCount",
                "stats.playCount",
                "stats.views",
                "statistics.play_count",
                "statistics.playCount",
            ],
        )
        .unwrap_or(0),
        published_at: first_timestamp(
            value,
            &[
                "published_at",
                "publishedAt",
                "created_at",
                "createdAt",
                "create_time",
                "createTime",
            ],
        ),
        kind,
        is_pinned: first_bool(
            value,
            &[
                "isPinnedItem",
                "isPinned",
                "is_pinned",
                "pinned",
                "is_top",
                "isTop",
                "item_control.isPinned",
            ],
        )
        .unwrap_or(false),
        source_url,
        slide_image_urls,
        visual_image_urls: parse_visual_images(value),
        raw: value.clone(),
    })
}

fn build_tiktok_post_url(value: &Value, kind: TiktokPostKind) -> Option<String> {
    let id = first_string(value, &["id", "aweme_id", "awemeId"])?;
    let handle = first_string(
        value,
        &[
            "author.uniqueId",
            "author.unique_id",
            "author.username",
            "author.handle",
            "uniqueId",
        ],
    )
    .and_then(|handle| normalize_handle(&handle))?;
    let post_type = match kind {
        TiktokPostKind::Video => "video",
        TiktokPostKind::Photo => "photo",
    };
    Some(format!("https://www.tiktok.com/@{handle}/{post_type}/{id}"))
}

fn parse_slide_images(value: &Value) -> Vec<String> {
    let arrays = [
        value.pointer("/slide_image_urls"),
        value.pointer("/slideImageUrls"),
        value.pointer("/images"),
        value.pointer("/image_post/images"),
        value.pointer("/imagePost/images"),
    ];

    let mut urls = Vec::new();
    for array in arrays.into_iter().flatten().filter_map(Value::as_array) {
        for item in array {
            if let Some(url) = item.as_str() {
                urls.push(url.to_string());
                continue;
            }
            if let Some(url) = first_string(
                item,
                &[
                    "url",
                    "image_url",
                    "imageUrl",
                    "display_image",
                    "imageURL.url_list.0",
                    "imageURL.urlList.0",
                    "imageURL.urlList.1",
                    "imageUrl.url_list.0",
                    "imageUrl.urlList.0",
                    "image_url.url_list.0",
                ],
            ) {
                urls.push(url);
            }
            collect_urls_from_value(item, &mut urls);
        }
    }
    urls.sort();
    urls.dedup();
    urls
}

fn parse_visual_images(value: &Value) -> Vec<String> {
    let mut urls = Vec::new();
    for key in [
        "video.cover",
        "video.cover.url_list.0",
        "video.cover.urlList.0",
        "video.originCover",
        "video.origin_cover",
        "video.origin_cover.url_list.0",
        "video.dynamicCover",
        "video.dynamic_cover",
        "video.dynamic_cover.url_list.0",
        "cover",
        "originCover",
        "origin_cover",
        "dynamicCover",
        "dynamic_cover",
    ] {
        if let Some(url) = first_string(value, &[key]) {
            urls.push(url);
        }
        if let Some(url_value) = get_path(value, key) {
            collect_urls_from_value(url_value, &mut urls);
        }
    }
    for url in parse_slide_images(value) {
        urls.push(url);
    }
    urls.sort();
    urls.dedup();
    urls
}

fn collect_urls_from_value(value: &Value, urls: &mut Vec<String>) {
    match value {
        Value::String(url) => urls.push(url.to_string()),
        Value::Array(items) => {
            for item in items {
                collect_urls_from_value(item, urls);
            }
        }
        Value::Object(_) => {
            for key in ["url_list", "urlList"] {
                if let Some(list) = value.get(key).and_then(Value::as_array) {
                    for item in list {
                        if let Some(url) = item.as_str() {
                            urls.push(url.to_string());
                        }
                    }
                }
            }
        }
        _ => {}
    }
}

fn is_nonfatal_following_error(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}").to_ascii_lowercase();
    [
        "operation timed out",
        "request timed out",
        "timed out",
        "deadline has elapsed",
        "private profile",
        "private account",
        "following list is private",
        "followers list is private",
        "returned no tiktok following users",
        "returned no tiktok followers",
    ]
    .iter()
    .any(|needle| message.contains(needle))
}

fn is_transient_scraper_request_error(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}").to_ascii_lowercase();
    [
        "operation timed out",
        "request timed out",
        "deadline has elapsed",
        "error sending request",
        "connection closed",
        "connection reset",
        "connection refused",
        "tcp connect",
        "dns error",
    ]
    .iter()
    .any(|needle| message.contains(needle))
}

fn first_object<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a Value> {
    keys.iter()
        .filter_map(|key| get_path(value, key))
        .find(|candidate| candidate.is_object())
}

fn first_string(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        let value = get_path(value, key)?;
        if let Some(string) = value.as_str() {
            return Some(string.to_string());
        }
        if value.is_number() || value.is_boolean() {
            return Some(value.to_string());
        }
        None
    })
}

fn first_u64(value: &Value, keys: &[&str]) -> Option<u64> {
    keys.iter().find_map(|key| {
        let value = get_path(value, key)?;
        if let Some(number) = value.as_u64() {
            return Some(number);
        }
        value
            .as_str()
            .and_then(|string| string.replace(',', "").parse::<u64>().ok())
    })
}

fn first_bool(value: &Value, keys: &[&str]) -> Option<bool> {
    keys.iter().find_map(|key| {
        let value = get_path(value, key)?;
        if let Some(bool_value) = value.as_bool() {
            return Some(bool_value);
        }
        if let Some(number) = value.as_i64() {
            return Some(number != 0);
        }
        value.as_str().and_then(|string| match string {
            "1" | "true" | "TRUE" | "yes" | "YES" => Some(true),
            "0" | "false" | "FALSE" | "no" | "NO" => Some(false),
            _ => None,
        })
    })
}

fn first_timestamp(value: &Value, keys: &[&str]) -> Option<DateTime<Utc>> {
    keys.iter().find_map(|key| {
        let value = get_path(value, key)?;
        if let Some(timestamp) = value.as_i64() {
            return Utc.timestamp_opt(timestamp, 0).single();
        }
        let string = value.as_str()?;
        DateTime::parse_from_rfc3339(string)
            .map(|dt| dt.with_timezone(&Utc))
            .ok()
            .or_else(|| {
                string
                    .parse::<i64>()
                    .ok()
                    .and_then(|timestamp| Utc.timestamp_opt(timestamp, 0).single())
            })
    })
}

fn get_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = value;
    for segment in path.split('.') {
        current = match current {
            Value::Object(_) => current.get(segment)?,
            Value::Array(items) => items.get(segment.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(current)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_canonical_creator_payload() {
        let snapshot = parse_creator_snapshot(
            "seed",
            json!({
                "handle": "Seed",
                "bio": "building with ExampleApp",
                "following": ["@A", "https://tiktok.com/@B"],
                "videos": [
                    {"url": "https://tiktok.com/@seed/video/1", "views": 10, "kind": "video"},
                    {"url": "https://tiktok.com/@seed/photo/2", "views": 40, "images": [{"url": "https://img/1.png"}]}
                ]
            }),
        )
        .unwrap();

        assert_eq!(snapshot.handle, "seed");
        assert_eq!(snapshot.following, vec!["a", "b"]);
        assert_eq!(snapshot.videos.len(), 2);
        assert_eq!(snapshot.videos[1].kind, TiktokPostKind::Photo);
    }

    #[test]
    fn parses_legacy_profile_posts_and_following_pages_fixture() {
        let snapshot = snapshot_from_social_values(
            "seed",
            json!({
                "userInfo": {
                    "user": {
                        "uniqueId": "SeedCreator",
                        "nickname": "Seed",
                        "signature": "Ambassador for ExampleApp",
                        "secUid": "SEC123"
                    }
                }
            }),
            json!({
                "itemList": [
                    {
                        "id": "100",
                        "desc": "try ExampleApp",
                        "createTime": "1710000000",
                        "isPinnedItem": true,
                        "author": { "uniqueId": "SeedCreator" },
                        "stats": { "playCount": 2500 },
                        "video": {
                            "downloadAddr": "https://video.example/100.mp4",
                            "cover": "https://image.example/cover.jpg"
                        }
                    },
                    {
                        "id": "101",
                        "desc": "slides",
                        "createTime": "1710000100",
                        "author": { "uniqueId": "SeedCreator" },
                        "stats": { "playCount": 5000 },
                        "imagePost": {
                            "images": [
                                { "imageURL": { "urlList": ["https://image.example/1.jpg"] } }
                            ]
                        }
                    }
                ]
            }),
            vec![json!({
                "userList": [
                    { "user": { "uniqueId": "FriendA" } },
                    { "user": { "uniqueId": "friendb" } }
                ],
                "hasMore": false
            })],
            None,
        )
        .unwrap();

        assert_eq!(snapshot.handle, "seedcreator");
        assert_eq!(snapshot.bio, "Ambassador for ExampleApp");
        assert_eq!(snapshot.following, vec!["frienda", "friendb"]);
        assert_eq!(snapshot.videos.len(), 2);
        assert_eq!(snapshot.videos[0].kind, TiktokPostKind::Photo);
        assert_eq!(
            snapshot.videos[0].url,
            "https://www.tiktok.com/@seedcreator/photo/101"
        );
        assert_eq!(
            snapshot.videos[1].source_url.as_deref(),
            Some("https://www.tiktok.com/@seedcreator/video/100")
        );
        assert!(snapshot.videos[1].is_pinned);
        assert_eq!(
            snapshot.videos[1].visual_image_urls,
            vec!["https://image.example/cover.jpg"]
        );
    }

    #[test]
    fn parses_trimmed_social_profile_scraper_payloads() {
        let snapshot = snapshot_from_social_values(
            "seed",
            json!({
                "user": {
                    "unique_id": "SeedCreator",
                    "nickname": "Seed",
                    "signature": "Astra AI notes and study systems"
                },
                "stats": {
                    "followerCount": 1200,
                    "following_count": 31
                },
                "itemList": []
            }),
            json!({
                "aweme_list": [
                    {
                        "aweme_id": "200",
                        "desc": "pinned intro",
                        "create_time": 1710000000,
                        "is_pinned": true,
                        "author": { "unique_id": "SeedCreator" },
                        "statistics": { "play_count": 1234 },
                        "video": {
                            "duration": 12,
                            "cover": { "url_list": ["https://image.example/cover-200.jpg"] },
                            "play_addr": { "url_list": ["https://video.example/play-200.mp4"] },
                            "download_addr": { "url_list": [] },
                            "download_no_watermark_addr": { "url_list": [] },
                            "has_watermark": true
                        }
                    },
                    {
                        "aweme_id": "201",
                        "desc": "new post",
                        "create_time": 1710000100,
                        "is_pinned": false,
                        "author": { "unique_id": "SeedCreator" },
                        "statistics": { "play_count": 5678 },
                        "video": {
                            "duration": 9,
                            "cover": { "url_list": ["https://image.example/cover-201.jpg"] },
                            "play_addr": { "url_list": ["https://video.example/play-201.mp4"] },
                            "download_addr": { "url_list": [] },
                            "download_no_watermark_addr": { "url_list": [] },
                            "has_watermark": true
                        }
                    }
                ],
                "max_cursor": "0",
                "has_more": false
            }),
            vec![json!({
                "followings": [
                    { "unique_id": "FriendA", "nickname": "Friend A", "verified": false },
                    { "unique_id": "friendb", "nickname": "Friend B", "verified": false }
                ],
                "total": 2,
                "min_time": "0",
                "has_more": false
            })],
            Some(json!({ "success": true, "region": "de" })),
        )
        .unwrap();

        assert_eq!(snapshot.handle, "seedcreator");
        assert_eq!(snapshot.follower_count, Some(1200));
        assert_eq!(snapshot.following_count, Some(31));
        assert_eq!(snapshot.country_code.as_deref(), Some("DE"));
        assert_eq!(snapshot.following, vec!["frienda", "friendb"]);
        assert_eq!(snapshot.videos.len(), 2);
        assert_eq!(
            snapshot.videos[0].url,
            "https://www.tiktok.com/@seedcreator/video/201"
        );
        assert_eq!(snapshot.videos[0].views, 5678);
        assert!(!snapshot.videos[0].is_pinned);
        assert_eq!(
            snapshot.videos[0].visual_image_urls,
            vec!["https://image.example/cover-201.jpg"]
        );
        assert_eq!(
            snapshot.videos[0].source_url.as_deref(),
            Some("https://www.tiktok.com/@seedcreator/video/201")
        );
    }

    #[test]
    fn treats_private_or_empty_following_errors_as_nonfatal() {
        let error = anyhow::anyhow!(
            "social profile scraper returned 502 Bad Gateway: {{\"success\":false,\"error\":\"upstream_error\",\"message\":\"Browser Use signer returned no TikTok following users.\"}}"
        );
        assert!(is_nonfatal_following_error(&error));

        let error = anyhow::anyhow!("social profile scraper returned 403: private profile");
        assert!(is_nonfatal_following_error(&error));

        let error = anyhow::anyhow!("failed to call social profile scraper: operation timed out");
        assert!(is_nonfatal_following_error(&error));
        assert!(is_transient_scraper_request_error(&error));
    }

    #[test]
    fn maps_profile_locale_codes_to_country_markets() {
        assert_eq!(normalize_country_code("EN").as_deref(), Some("GB"));
        assert_eq!(normalize_country_code("CA").as_deref(), Some("ES"));
        assert_eq!(normalize_country_code("CS").as_deref(), Some("CZ"));
        assert_eq!(normalize_country_code("UK").as_deref(), Some("UA"));
        assert_eq!(normalize_country_code("SV").as_deref(), Some("SE"));
        assert_eq!(normalize_country_code("de").as_deref(), Some("DE"));
    }
}
