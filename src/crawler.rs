use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use anyhow::{Context, Result};
use chrono::Utc;
use futures::{StreamExt, stream};
use serde::Serialize;
use tokio::time::{Duration, sleep};
use tracing::{error, info, warn};

use crate::{
    db::{Database, FrontierJob, QueueClaimFilters, RecordCreatorOptions, RecordOutcome},
    handles::normalize_handle,
    models::{CrawledCreator, CreatorSnapshot, ExtractedContent, TiktokPost, TiktokPostKind},
    services::{elevenlabs::ElevenLabsService, openai::OpenAiService, tiktok::TikTokScraper},
    stats::calculate_creator_stats,
};

const CLASSIFICATION_VIDEO_LIMIT: usize = 3;
const FRONTIER_DEPTH_LIMIT: u8 = 1;

#[derive(Clone)]
pub struct Crawler {
    db: Database,
    scraper: Arc<dyn TikTokScraper>,
    elevenlabs: ElevenLabsService,
    openai: OpenAiService,
}

#[derive(Debug, Clone)]
pub struct CrawlResult {
    pub handle: String,
    pub skipped_already_scraped: bool,
    pub promoted_app_name: Option<String>,
    pub enqueued_following_count: usize,
}

#[derive(Debug, Clone, Default)]
pub struct QueueRunFilters {
    pub language_code: Option<String>,
    pub app_name: Option<String>,
    pub app_names: Vec<String>,
    pub country_codes: Vec<String>,
    pub handles: Vec<String>,
    pub whitelist_only: bool,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct RunSummary {
    pub processed: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub skipped: usize,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct CountryBackfillSummary {
    pub checked: usize,
    pub updated: usize,
    pub unresolved: usize,
}

impl Crawler {
    pub fn new(
        db: Database,
        scraper: Arc<dyn TikTokScraper>,
        elevenlabs: ElevenLabsService,
        openai: OpenAiService,
    ) -> Self {
        Self {
            db,
            scraper,
            elevenlabs,
            openai,
        }
    }

    pub async fn backfill_parent_countries(
        &self,
        concurrency: usize,
    ) -> Result<CountryBackfillSummary> {
        let handles = self.db.list_queue_parents_needing_country().await?;
        let results = stream::iter(handles)
            .map(|handle| {
                let scraper = self.scraper.clone();
                let db = self.db.clone();
                async move {
                    match scraper.fetch_country_code(&handle).await {
                        Ok(country_code) => {
                            let error = country_code
                                .is_none()
                                .then_some("profile region was missing");
                            db.record_creator_country_lookup(
                                &handle,
                                country_code.as_deref(),
                                error,
                            )
                            .await?;
                            Ok::<bool, anyhow::Error>(country_code.is_some())
                        }
                        Err(error) => {
                            db.record_creator_country_lookup(
                                &handle,
                                None,
                                Some(&format!("{error:#}")),
                            )
                            .await?;
                            Ok(false)
                        }
                    }
                }
            })
            .buffer_unordered(concurrency.clamp(1, 25))
            .collect::<Vec<_>>()
            .await;

        let mut summary = CountryBackfillSummary::default();
        for result in results {
            summary.checked += 1;
            match result {
                Ok(true) => summary.updated += 1,
                Ok(false) => summary.unresolved += 1,
                Err(error) => {
                    summary.unresolved += 1;
                    warn!(error = %format!("{error:#}"), "failed to persist parent country lookup");
                }
            }
        }
        Ok(summary)
    }

    pub async fn crawl_handle(&self, handle: &str, force: bool) -> Result<CrawlResult> {
        self.crawl_handle_with_options(handle, force, RecordCreatorOptions::default())
            .await
    }

    async fn crawl_handle_with_options(
        &self,
        handle: &str,
        force: bool,
        record_options: RecordCreatorOptions,
    ) -> Result<CrawlResult> {
        let handle = normalize_handle(handle).context("invalid TikTok handle")?;
        if !force && self.db.is_scraped(&handle).await? {
            self.db.mark_done(&handle).await?;
            return Ok(CrawlResult {
                handle,
                skipped_already_scraped: true,
                promoted_app_name: None,
                enqueued_following_count: 0,
            });
        }

        info!(handle = %handle, "fetching creator");
        let snapshot = self.scraper.fetch_creator(&handle).await?;
        let stats = calculate_creator_stats(&snapshot.videos);
        let classification_posts = select_classification_posts(&snapshot);
        let mut latest_content = self
            .extract_content_for_posts(&classification_posts)
            .await
            .with_context(|| format!("failed to extract latest content for @{handle}"))?;
        let known_apps = self.db.list_app_names().await?;
        let visual_image_urls = classification_posts
            .iter()
            .flat_map(|post| post.visual_image_urls.iter().cloned())
            .collect::<Vec<_>>();
        let classification = self
            .openai
            .classify_ambassador(
                &snapshot.handle,
                snapshot.display_name.as_deref(),
                &snapshot.bio,
                &latest_content.text,
                &visual_image_urls,
                &known_apps,
            )
            .await
            .with_context(|| format!("failed to classify @{handle}"))?;
        if classification.language_code != "UNKNOWN" {
            latest_content.language_code = classification.language_code.clone();
        }

        let crawled = CrawledCreator {
            snapshot,
            stats,
            latest_content,
            classification,
            scraped_at: Utc::now(),
        };

        let RecordOutcome {
            promoted_app_name,
            enqueued_following_count,
        } = if record_options == RecordCreatorOptions::default() {
            self.db.record_creator(&crawled).await?
        } else {
            self.db
                .record_creator_with_options(&crawled, record_options)
                .await?
        };

        Ok(CrawlResult {
            handle,
            skipped_already_scraped: false,
            promoted_app_name,
            enqueued_following_count,
        })
    }

    pub async fn run_queue(
        &self,
        limit: Option<usize>,
        concurrency: usize,
        watch: bool,
    ) -> Result<RunSummary> {
        self.run_queue_filtered(limit, concurrency, watch, None, None, None)
            .await
    }

    pub async fn run_queue_filtered(
        &self,
        limit: Option<usize>,
        concurrency: usize,
        watch: bool,
        language_code: Option<String>,
        app_name: Option<String>,
        stop_requested: Option<Arc<AtomicBool>>,
    ) -> Result<RunSummary> {
        self.run_queue_with_filters(
            limit,
            concurrency,
            watch,
            QueueRunFilters {
                language_code,
                app_name,
                ..QueueRunFilters::default()
            },
            stop_requested,
        )
        .await
    }

    pub async fn run_queue_with_filters(
        &self,
        limit: Option<usize>,
        concurrency: usize,
        watch: bool,
        filters: QueueRunFilters,
        stop_requested: Option<Arc<AtomicBool>>,
    ) -> Result<RunSummary> {
        let concurrency = concurrency.max(1);
        let mut summary = RunSummary::default();

        loop {
            if stop_requested
                .as_ref()
                .is_some_and(|stop| stop.load(Ordering::Relaxed))
            {
                break;
            }
            if limit.is_some_and(|limit| summary.processed >= limit) {
                break;
            }

            let remaining = limit
                .map(|limit| limit.saturating_sub(summary.processed))
                .unwrap_or(concurrency);
            let batch_size = concurrency.min(remaining).max(1);
            let mut jobs = Vec::with_capacity(batch_size);

            for _ in 0..batch_size {
                if stop_requested
                    .as_ref()
                    .is_some_and(|stop| stop.load(Ordering::Relaxed))
                {
                    break;
                }
                let job = self
                    .db
                    .claim_next_filtered(QueueClaimFilters {
                        language_code: filters.language_code.as_deref(),
                        app_name: filters.app_name.as_deref(),
                        app_names: &filters.app_names,
                        country_codes: &filters.country_codes,
                        handles: &filters.handles,
                        whitelist_only: filters.whitelist_only,
                    })
                    .await?;
                let Some(job) = job else {
                    break;
                };
                jobs.push(job);
            }

            if jobs.is_empty() {
                if watch {
                    sleep(Duration::from_secs(30)).await;
                    continue;
                }
                break;
            }

            let crawler = self.clone();
            let mut results = stream::iter(jobs.into_iter().map(|job| {
                let crawler = crawler.clone();
                async move {
                    let handle = job.handle.clone();
                    let result = crawler.crawl_handle(&handle, false).await;
                    (handle, result)
                }
            }))
            .buffer_unordered(concurrency);

            while let Some((handle, result)) = results.next().await {
                summary.processed += 1;
                match result {
                    Ok(result) => {
                        if result.skipped_already_scraped {
                            summary.skipped += 1;
                        } else {
                            summary.succeeded += 1;
                        }
                        info!(
                            handle = %result.handle,
                            promoted_app = ?result.promoted_app_name,
                            enqueued = result.enqueued_following_count,
                            "finished creator crawl"
                        );
                    }
                    Err(error) => {
                        summary.failed += 1;
                        let error_message = format!("{error:#}");
                        error!(handle = %handle, error = %error_message, "creator crawl failed");
                        self.db.mark_failed(&handle, &error_message).await?;
                    }
                }
            }
        }

        Ok(summary)
    }

    pub async fn run_frontier(
        &self,
        run_id: i64,
        limit: Option<usize>,
        concurrency: usize,
        refresh_seeds: bool,
        stop_requested: Option<Arc<AtomicBool>>,
    ) -> Result<RunSummary> {
        let concurrency = concurrency.max(1);
        let mut summary = RunSummary::default();

        loop {
            if stop_requested
                .as_ref()
                .is_some_and(|stop| stop.load(Ordering::Relaxed))
            {
                break;
            }
            if limit.is_some_and(|limit| summary.processed >= limit) {
                break;
            }

            let remaining = limit
                .map(|limit| limit.saturating_sub(summary.processed))
                .unwrap_or(concurrency);
            let batch_size = concurrency.min(remaining).max(1);
            let mut jobs = Vec::with_capacity(batch_size);

            for _ in 0..batch_size {
                if stop_requested
                    .as_ref()
                    .is_some_and(|stop| stop.load(Ordering::Relaxed))
                {
                    break;
                }
                let Some(job) = self.db.claim_next_frontier_item(run_id).await? else {
                    break;
                };
                jobs.push(job);
            }

            if jobs.is_empty() {
                break;
            }

            let crawler = self.clone();
            let mut results = stream::iter(jobs.into_iter().map(|job| {
                let crawler = crawler.clone();
                async move {
                    let result = crawler
                        .process_frontier_job(job.clone(), refresh_seeds)
                        .await;
                    (job, result)
                }
            }))
            .buffer_unordered(concurrency);

            while let Some((job, result)) = results.next().await {
                summary.processed += 1;
                match result {
                    Ok(result) => {
                        if result.skipped_already_scraped {
                            summary.skipped += 1;
                        } else {
                            summary.succeeded += 1;
                        }
                        info!(
                            run_id = job.run_id,
                            depth = job.depth,
                            handle = %result.handle,
                            promoted_app = ?result.promoted_app_name,
                            "finished frontier creator crawl"
                        );
                    }
                    Err(error) => {
                        summary.failed += 1;
                        let error_message = format!("{error:#}");
                        error!(
                            run_id = job.run_id,
                            depth = job.depth,
                            handle = %job.handle,
                            error = %error_message,
                            "frontier creator crawl failed"
                        );
                        self.db
                            .mark_frontier_item_failed(job.run_id, &job.handle, &error_message)
                            .await?;
                    }
                }
            }
        }

        Ok(summary)
    }

    async fn process_frontier_job(
        &self,
        job: FrontierJob,
        refresh_seeds: bool,
    ) -> Result<CrawlResult> {
        let force = refresh_seeds && job.depth == 0;
        let result = self
            .crawl_handle_with_options(
                &job.handle,
                force,
                RecordCreatorOptions {
                    enqueue_following: false,
                    prune_no_app_children: false,
                },
            )
            .await?;

        if job.depth < FRONTIER_DEPTH_LIMIT {
            let following = self.db.list_following_handles(&job.handle).await?;
            let inserted = self
                .db
                .enqueue_frontier_following(job.run_id, &job.handle, job.depth + 1, &following)
                .await?;
            info!(
                run_id = job.run_id,
                handle = %job.handle,
                depth = job.depth,
                inserted,
                "expanded frontier creator"
            );
        }

        self.db
            .mark_frontier_item_done(job.run_id, &job.handle)
            .await?;
        Ok(result)
    }

    async fn extract_content_for_posts(&self, posts: &[&TiktokPost]) -> Result<ExtractedContent> {
        if posts.is_empty() {
            return Ok(ExtractedContent::empty());
        }

        let mut sections = Vec::new();
        let mut language_code = "UNKNOWN".to_string();

        for (index, post) in posts.iter().enumerate() {
            match self.extract_content_for_post(Some(post)).await {
                Ok(content) => {
                    if language_code == "UNKNOWN" && content.language_code != "UNKNOWN" {
                        language_code = content.language_code.clone();
                    }

                    sections.push(format_post_content_section(
                        index,
                        posts.len(),
                        post,
                        &content.text,
                    ));
                }
                Err(error) => {
                    warn!(
                        post_url = %post.url,
                        error = %format!("{error:#}"),
                        "post content extraction failed; trying next classification post"
                    );
                    if let Some(section) = format_caption_fallback_section(index, posts.len(), post)
                    {
                        sections.push(section);
                    }
                }
            }
        }

        Ok(ExtractedContent {
            text: sections.join("\n\n---\n\n"),
            language_code,
        }
        .normalized())
    }

    async fn extract_content_for_post(
        &self,
        post: Option<&TiktokPost>,
    ) -> Result<ExtractedContent> {
        let Some(latest) = post else {
            return Ok(ExtractedContent::empty());
        };

        match latest.kind {
            TiktokPostKind::Photo => self.extract_photo_post(latest).await,
            TiktokPostKind::Video => self.extract_video_post(latest).await,
        }
    }

    async fn extract_video_post(&self, latest: &TiktokPost) -> Result<ExtractedContent> {
        let source_url = latest.source_url.as_deref().unwrap_or(&latest.url);
        let mut content = self.elevenlabs.transcribe_source_url(source_url).await?;
        if let Some(caption) = latest
            .caption
            .as_deref()
            .filter(|caption| !caption.trim().is_empty())
        {
            content.text = format!("Transcript:\n{}\n\nCaption:\n{}", content.text, caption);
        }
        Ok(content.normalized())
    }

    async fn extract_photo_post(&self, latest: &TiktokPost) -> Result<ExtractedContent> {
        self.openai
            .extract_slideshow_text(&latest.slide_image_urls, latest.caption.as_deref())
            .await
    }
}

fn format_post_content_section(
    index: usize,
    total: usize,
    post: &TiktokPost,
    text: &str,
) -> String {
    format!(
        "Post {} of {}\nURL: {}\nViews: {}\nPinned: {}\n{}",
        index + 1,
        total,
        post.url,
        post.views,
        post.is_pinned,
        text
    )
}

fn format_caption_fallback_section(
    index: usize,
    total: usize,
    post: &TiktokPost,
) -> Option<String> {
    let caption = post
        .caption
        .as_deref()
        .map(str::trim)
        .filter(|caption| !caption.is_empty())?;

    Some(format_post_content_section(
        index,
        total,
        post,
        &format!("Transcript unavailable.\n\nCaption:\n{caption}"),
    ))
}

fn select_classification_posts(snapshot: &CreatorSnapshot) -> Vec<&TiktokPost> {
    snapshot
        .videos
        .iter()
        .filter(|post| post.kind == TiktokPostKind::Video && !post.is_pinned)
        .take(CLASSIFICATION_VIDEO_LIMIT)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::TiktokPostKind;

    fn post(id: &str, kind: TiktokPostKind, is_pinned: bool) -> TiktokPost {
        TiktokPost {
            id: Some(id.to_string()),
            url: format!("https://www.tiktok.com/@creator/video/{id}"),
            caption: None,
            views: 0,
            published_at: None,
            kind,
            is_pinned,
            source_url: Some(format!("https://www.tiktok.com/@creator/video/{id}")),
            slide_image_urls: Vec::new(),
            visual_image_urls: Vec::new(),
            raw: serde_json::Value::Null,
        }
    }

    #[test]
    fn selects_latest_three_non_pinned_videos() {
        let snapshot = CreatorSnapshot {
            handle: "creator".to_string(),
            display_name: None,
            country_code: None,
            bio: String::new(),
            follower_count: None,
            following_count: None,
            following: Vec::new(),
            videos: vec![
                post("pinned", TiktokPostKind::Video, true),
                post("photo", TiktokPostKind::Photo, false),
                post("one", TiktokPostKind::Video, false),
                post("two", TiktokPostKind::Video, false),
                post("three", TiktokPostKind::Video, false),
                post("four", TiktokPostKind::Video, false),
            ],
            raw: serde_json::Value::Null,
        };

        let selected = select_classification_posts(&snapshot);
        let ids = selected
            .iter()
            .filter_map(|post| post.id.as_deref())
            .collect::<Vec<_>>();
        assert_eq!(ids, vec!["one", "two", "three"]);
    }

    #[test]
    fn selected_posts_empty_when_only_pinned_or_photo_posts_exist() {
        let snapshot = CreatorSnapshot {
            handle: "creator".to_string(),
            display_name: None,
            country_code: None,
            bio: String::new(),
            follower_count: None,
            following_count: None,
            following: Vec::new(),
            videos: vec![
                post("pinned", TiktokPostKind::Video, true),
                post("photo", TiktokPostKind::Photo, false),
            ],
            raw: serde_json::Value::Null,
        };

        assert!(select_classification_posts(&snapshot).is_empty());
    }

    #[test]
    fn caption_fallback_preserves_context_when_transcript_fails() {
        let mut post = post("one", TiktokPostKind::Video, false);
        post.caption = Some("Use Astra AI for notes".to_string());

        let section = format_caption_fallback_section(0, 3, &post).unwrap();

        assert!(section.contains("Transcript unavailable."));
        assert!(section.contains("Use Astra AI for notes"));
        assert!(section.contains("Post 1 of 3"));
    }
}
