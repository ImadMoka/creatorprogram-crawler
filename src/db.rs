use std::{fs, path::Path, sync::Arc};

use anyhow::{Context, Result};
use chrono::{Duration as ChronoDuration, Utc};
use rusqlite::{
    Connection, OptionalExtension, Transaction, params, params_from_iter, types::Value,
};
use serde::Serialize;
use serde_json::json;
use tokio::sync::Mutex;

use crate::{
    handles::{normalize_handle, normalize_many},
    models::{CrawledCreator, QueueItem, QueueJob, TiktokPostKind},
};

const STALE_PROCESSING_LOCK_AFTER_MINUTES: i64 = 15;

#[derive(Clone)]
pub struct Database {
    conn: Arc<Mutex<Connection>>,
}

#[derive(Debug, Clone)]
pub struct RecordOutcome {
    pub promoted_app_name: Option<String>,
    pub enqueued_following_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordCreatorOptions {
    pub enqueue_following: bool,
    pub prune_no_app_children: bool,
}

impl Default for RecordCreatorOptions {
    fn default() -> Self {
        Self {
            enqueue_following: true,
            prune_no_app_children: true,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct QueueStatusCount {
    pub status: String,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueueViewItem {
    pub handle: String,
    pub discovered_from: Option<String>,
    pub found_at: String,
    pub status: String,
    pub attempts: u32,
    pub last_error: Option<String>,
    pub expected_app_name: Option<String>,
    pub inferred_app_name: Option<String>,
    pub inferred_language_code: Option<String>,
    pub inferred_country_code: Option<String>,
    pub app_policy: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueueSourceViewItem {
    pub source_handle: String,
    pub item_count: usize,
    pub removable_count: usize,
    pub oldest_found_at: String,
    pub country_code: Option<String>,
    pub app_name: Option<String>,
    pub app_policy: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct FrontierBucketItem {
    pub handle: String,
    pub added_at: String,
    pub source: Option<String>,
    pub promoted_app_name: Option<String>,
    pub language_code: Option<String>,
    pub follower_count: Option<i64>,
    pub following_count: Option<i64>,
    pub follows_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct FrontierRunRecord {
    pub id: i64,
    pub status: String,
    pub created_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub seed_count: usize,
    pub depth_limit: u8,
    pub processed: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub skipped: usize,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FrontierRunItemView {
    pub run_id: i64,
    pub handle: String,
    pub depth: u8,
    pub discovered_from: Option<String>,
    pub found_at: String,
    pub status: String,
    pub attempts: u32,
    pub last_error: Option<String>,
    pub scraped_at: Option<String>,
    pub promoted_app_name: Option<String>,
    pub language_code: Option<String>,
    pub follower_count: Option<i64>,
    pub following_count: Option<i64>,
    pub follows_count: usize,
}

#[derive(Debug, Clone)]
pub struct FrontierJob {
    pub run_id: i64,
    pub handle: String,
    pub depth: u8,
    pub attempts: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct FrontierRunFinish<'a> {
    pub status: &'a str,
    pub processed: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub skipped: usize,
    pub last_error: Option<&'a str>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct QueueClaimFilters<'a> {
    pub language_code: Option<&'a str>,
    pub app_name: Option<&'a str>,
    pub app_names: &'a [String],
    pub country_codes: &'a [String],
    pub handles: &'a [String],
    pub whitelist_only: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct AppSummary {
    pub name: String,
    pub creator_count: usize,
    pub policy: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CreatorListItem {
    pub handle: String,
    pub display_name: Option<String>,
    pub contact_name: Option<String>,
    pub promoted_app_name: Option<String>,
    pub email: Option<String>,
    pub country_code: Option<String>,
    pub follower_count: Option<i64>,
    pub following_count: Option<i64>,
    pub avg_views: f64,
    pub median_views: f64,
    pub most_viral_video_url: Option<String>,
    pub most_viral_video_views: Option<i64>,
    pub language_code: String,
    pub scraped_at: String,
    pub contact_status: String,
    pub contact_priority_at: Option<String>,
    pub contacted_at: Option<String>,
    pub videos_count: usize,
    pub follows_count: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct CreatorListFilters<'a> {
    pub app_mode: Option<&'a str>,
    pub app_names: &'a [String],
    pub language_codes: &'a [String],
    pub country_codes: &'a [String],
    pub contact_statuses: &'a [String],
    pub email_filter: Option<&'a str>,
    pub min_followers: Option<i64>,
    pub max_followers: Option<i64>,
    pub min_following: Option<i64>,
    pub max_following: Option<i64>,
    pub min_median_views: Option<f64>,
    pub max_median_views: Option<f64>,
    pub min_avg_views: Option<f64>,
    pub max_avg_views: Option<f64>,
    pub limit: usize,
    pub offset: usize,
}

impl Default for CreatorListFilters<'_> {
    fn default() -> Self {
        Self {
            app_mode: None,
            app_names: &[],
            language_codes: &[],
            country_codes: &[],
            contact_statuses: &[],
            email_filter: None,
            min_followers: None,
            max_followers: None,
            min_following: None,
            max_following: None,
            min_median_views: None,
            max_median_views: None,
            min_avg_views: None,
            max_avg_views: None,
            limit: 100,
            offset: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LanguageReviewCreator {
    pub handle: String,
    pub bio: String,
    pub latest_content_text: String,
    pub language_code: String,
}

impl Database {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create database directory {}", parent.display())
            })?;
        }

        let conn = Connection::open(path)
            .with_context(|| format!("failed to open SQLite database {}", path.display()))?;
        conn.execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS apps (
                name TEXT PRIMARY KEY,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                policy TEXT NOT NULL DEFAULT 'neutral'
            );

            CREATE TABLE IF NOT EXISTS creators (
                handle TEXT PRIMARY KEY,
                display_name TEXT,
                contact_name TEXT,
                bio TEXT NOT NULL,
                country_code TEXT,
                country_checked_at TEXT,
                country_error TEXT,
                follower_count INTEGER,
                following_count INTEGER,
                avg_views REAL NOT NULL,
                median_views REAL NOT NULL,
                most_viral_video_url TEXT,
                most_viral_video_views INTEGER,
                latest_content_text TEXT NOT NULL,
                promoted_app_name TEXT,
                email TEXT,
                language_code TEXT NOT NULL,
                scraped_at TEXT NOT NULL,
                contact_status TEXT NOT NULL DEFAULT 'unselected',
                contact_priority_at TEXT,
                contacted_at TEXT,
                raw_json TEXT NOT NULL,
                FOREIGN KEY(promoted_app_name) REFERENCES apps(name)
            );

            CREATE TABLE IF NOT EXISTS videos (
                creator_handle TEXT NOT NULL,
                tiktok_url TEXT NOT NULL,
                post_id TEXT,
                kind TEXT NOT NULL,
                views INTEGER NOT NULL,
                caption TEXT,
                published_at TEXT,
                source_url TEXT,
                is_pinned INTEGER NOT NULL DEFAULT 0,
                is_latest INTEGER NOT NULL,
                is_most_viral INTEGER NOT NULL,
                raw_json TEXT NOT NULL,
                PRIMARY KEY (creator_handle, tiktok_url),
                FOREIGN KEY(creator_handle) REFERENCES creators(handle) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS follows (
                creator_handle TEXT NOT NULL,
                follows_handle TEXT NOT NULL,
                PRIMARY KEY (creator_handle, follows_handle),
                FOREIGN KEY(creator_handle) REFERENCES creators(handle) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS queue (
                handle TEXT PRIMARY KEY,
                discovered_from TEXT,
                expected_app_name TEXT,
                found_at TEXT NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                manual_priority INTEGER NOT NULL DEFAULT 0,
                locked_at TEXT,
                scraped_at TEXT,
                last_error TEXT
            );

            CREATE TABLE IF NOT EXISTS frontier_bucket (
                handle TEXT PRIMARY KEY,
                added_at TEXT NOT NULL,
                source TEXT
            );

            CREATE TABLE IF NOT EXISTS frontier_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT,
                seed_count INTEGER NOT NULL DEFAULT 0,
                depth_limit INTEGER NOT NULL DEFAULT 1,
                processed INTEGER NOT NULL DEFAULT 0,
                succeeded INTEGER NOT NULL DEFAULT 0,
                failed INTEGER NOT NULL DEFAULT 0,
                skipped INTEGER NOT NULL DEFAULT 0,
                last_error TEXT
            );

            CREATE TABLE IF NOT EXISTS frontier_items (
                run_id INTEGER NOT NULL,
                handle TEXT NOT NULL,
                depth INTEGER NOT NULL,
                discovered_from TEXT,
                found_at TEXT NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                locked_at TEXT,
                scraped_at TEXT,
                last_error TEXT,
                PRIMARY KEY (run_id, handle),
                FOREIGN KEY(run_id) REFERENCES frontier_runs(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_queue_status_found_at
                ON queue(status, found_at);
            CREATE INDEX IF NOT EXISTS idx_creators_promoted_app
                ON creators(promoted_app_name);
            CREATE INDEX IF NOT EXISTS idx_frontier_items_run_status_depth
                ON frontier_items(run_id, status, depth, found_at);
            "#,
        )
        .context("failed to initialize SQLite schema")?;
        ensure_column(&conn, "videos", "is_pinned", "INTEGER NOT NULL DEFAULT 0")?;
        ensure_column(&conn, "queue", "expected_app_name", "TEXT")?;
        ensure_column(
            &conn,
            "queue",
            "manual_priority",
            "INTEGER NOT NULL DEFAULT 0",
        )?;
        ensure_column(&conn, "apps", "policy", "TEXT NOT NULL DEFAULT 'neutral'")?;
        ensure_column(&conn, "creators", "email", "TEXT")?;
        ensure_column(&conn, "creators", "contact_name", "TEXT")?;
        ensure_column(&conn, "creators", "country_code", "TEXT")?;
        ensure_column(&conn, "creators", "country_checked_at", "TEXT")?;
        ensure_column(&conn, "creators", "country_error", "TEXT")?;
        ensure_column(&conn, "creators", "follower_count", "INTEGER")?;
        ensure_column(&conn, "creators", "following_count", "INTEGER")?;
        ensure_column(
            &conn,
            "creators",
            "contact_status",
            "TEXT NOT NULL DEFAULT 'unselected'",
        )?;
        ensure_column(&conn, "creators", "contact_priority_at", "TEXT")?;
        ensure_column(&conn, "creators", "contacted_at", "TEXT")?;
        conn.execute_batch(
            r#"
            UPDATE creators
            SET country_code = CASE upper(country_code)
                WHEN 'AR' THEN 'SA'
                WHEN 'BE' THEN 'BY'
                WHEN 'BS' THEN 'BA'
                WHEN 'CA' THEN 'ES'
                WHEN 'CS' THEN 'CZ'
                WHEN 'DA' THEN 'DK'
                WHEN 'EL' THEN 'GR'
                WHEN 'EN' THEN 'GB'
                WHEN 'ET' THEN 'EE'
                WHEN 'FA' THEN 'IR'
                WHEN 'HE' THEN 'IL'
                WHEN 'HI' THEN 'IN'
                WHEN 'JA' THEN 'JP'
                WHEN 'KK' THEN 'KZ'
                WHEN 'KO' THEN 'KR'
                WHEN 'MS' THEN 'MY'
                WHEN 'NB' THEN 'NO'
                WHEN 'NN' THEN 'NO'
                WHEN 'SL' THEN 'SI'
                WHEN 'SQ' THEN 'AL'
                WHEN 'SR' THEN 'RS'
                WHEN 'SV' THEN 'SE'
                WHEN 'TL' THEN 'PH'
                WHEN 'UK' THEN 'UA'
                WHEN 'UR' THEN 'PK'
                WHEN 'VI' THEN 'VN'
                WHEN 'ZH' THEN 'CN'
                ELSE upper(country_code)
            END
            WHERE country_code IS NOT NULL;

            UPDATE creators
            SET country_checked_at = NULL,
                country_error = NULL
            WHERE country_code IS NULL
              AND country_error = 'profile region was missing or invalid';

            UPDATE creators
            SET country_code = CASE upper(language_code)
                WHEN 'ENG' THEN 'GB'
                WHEN 'SPA' THEN 'ES'
                WHEN 'ITA' THEN 'IT'
                WHEN 'POL' THEN 'PL'
                WHEN 'DEU' THEN 'DE'
                WHEN 'SWE' THEN 'SE'
                WHEN 'FRA' THEN 'FR'
                WHEN 'POR' THEN 'PT'
                WHEN 'NOR' THEN 'NO'
                WHEN 'NOB' THEN 'NO'
                WHEN 'HRV' THEN 'HR'
                WHEN 'CES' THEN 'CZ'
                WHEN 'SLK' THEN 'SK'
                WHEN 'SLV' THEN 'SI'
                WHEN 'RON' THEN 'RO'
                WHEN 'NLD' THEN 'NL'
                WHEN 'FIN' THEN 'FI'
                WHEN 'DAN' THEN 'DK'
                WHEN 'EST' THEN 'EE'
                WHEN 'TUR' THEN 'TR'
                WHEN 'JPN' THEN 'JP'
                WHEN 'KOR' THEN 'KR'
                WHEN 'VIE' THEN 'VN'
                WHEN 'UKR' THEN 'UA'
                WHEN 'RUS' THEN 'RU'
                WHEN 'SRP' THEN 'RS'
                WHEN 'BUL' THEN 'BG'
                WHEN 'SQI' THEN 'AL'
                WHEN 'IND' THEN 'ID'
                WHEN 'HAT' THEN 'HT'
                ELSE country_code
            END
            WHERE upper(language_code) IN (
                'ENG', 'SPA', 'ITA', 'POL', 'DEU', 'SWE', 'FRA', 'POR',
                'NOR', 'NOB', 'HRV', 'CES', 'SLK', 'SLV', 'RON', 'NLD',
                'FIN', 'DAN', 'EST', 'TUR', 'JPN', 'KOR', 'VIE', 'UKR',
                'RUS', 'SRP', 'BUL', 'SQI', 'IND', 'HAT'
            );
            "#,
        )
        .context("failed to normalize saved creator locale codes")?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub async fn seed_handle(&self, handle: &str, discovered_from: Option<&str>) -> Result<bool> {
        let handle = normalize_handle(handle).context("invalid TikTok handle")?;
        self.enqueue_handle(&handle, discovered_from).await
    }

    pub async fn enqueue_handle(
        &self,
        handle: &str,
        discovered_from: Option<&str>,
    ) -> Result<bool> {
        self.enqueue_handle_with_app(handle, discovered_from, None)
            .await
    }

    pub async fn enqueue_handle_with_app(
        &self,
        handle: &str,
        discovered_from: Option<&str>,
        expected_app_name: Option<&str>,
    ) -> Result<bool> {
        let handle = normalize_handle(handle).context("invalid TikTok handle")?;
        let discovered_from = discovered_from.and_then(normalize_handle);
        let expected_app_name = expected_app_name
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_string);
        let now = Utc::now().to_rfc3339();
        let conn = self.conn.lock().await;
        if let Some(app_name) = &expected_app_name {
            conn.execute(
                r#"
                INSERT INTO apps(name, first_seen_at, last_seen_at)
                VALUES(?1, ?2, ?2)
                ON CONFLICT(name) DO UPDATE SET last_seen_at = excluded.last_seen_at
                "#,
                params![app_name, &now],
            )
            .context("failed to upsert expected app name")?;
        }
        let changed = conn
            .execute(
                r#"
                INSERT OR IGNORE INTO queue (
                    handle,
                    discovered_from,
                    expected_app_name,
                    found_at,
                    status,
                    attempts
                )
                SELECT ?1, ?2, ?3, ?4, 'pending', 0
                WHERE NOT EXISTS (
                    SELECT 1 FROM creators WHERE handle = ?1
                )
                ON CONFLICT(handle) DO UPDATE SET
                    expected_app_name = COALESCE(excluded.expected_app_name, queue.expected_app_name)
                "#,
                params![handle, discovered_from, expected_app_name, now],
            )
            .context("failed to enqueue TikTok handle")?;
        Ok(changed > 0)
    }

    pub async fn prioritize_handles(&self, handles: &[String]) -> Result<usize> {
        let handles = normalize_many(handles);
        let now = Utc::now().to_rfc3339();
        let mut conn = self.conn.lock().await;
        let tx = conn
            .transaction()
            .context("failed to open priority handle transaction")?;
        let mut changed = 0;
        for handle in handles {
            changed += tx
                .execute(
                    r#"
                    INSERT OR IGNORE INTO queue (
                        handle, found_at, status, attempts, manual_priority
                    )
                    SELECT ?1, ?2, 'pending', 0, 1
                    WHERE NOT EXISTS (SELECT 1 FROM creators WHERE handle = ?1)
                    "#,
                    params![&handle, &now],
                )
                .context("failed to enqueue priority handle")?;
            tx.execute(
                r#"
                UPDATE queue
                SET manual_priority = 1,
                    status = 'pending',
                    attempts = CASE WHEN status = 'failed' THEN 0 ELSE attempts END,
                    locked_at = NULL,
                    last_error = NULL
                WHERE handle = ?1
                  AND status IN ('pending', 'retry', 'failed', 'excluded')
                "#,
                params![handle],
            )
            .context("failed to prioritize queue handle")?;
        }
        tx.commit().context("failed to commit priority handles")?;
        Ok(changed)
    }

    pub async fn is_scraped(&self, handle: &str) -> Result<bool> {
        let handle = normalize_handle(handle).context("invalid TikTok handle")?;
        let conn = self.conn.lock().await;
        let exists = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM creators WHERE handle = ?1)",
                params![handle],
                |row| row.get::<_, bool>(0),
            )
            .context("failed to check creator scrape state")?;
        Ok(exists)
    }

    pub async fn claim_next_filtered(
        &self,
        filters: QueueClaimFilters<'_>,
    ) -> Result<Option<QueueJob>> {
        let mut conn = self.conn.lock().await;
        let tx = conn
            .transaction()
            .context("failed to open queue transaction")?;
        let now = Utc::now();
        let now_rfc3339 = now.to_rfc3339();

        tx.execute(
            r#"
            UPDATE queue
            SET status = 'done',
                scraped_at = COALESCE(scraped_at, ?1),
                locked_at = NULL
            WHERE status != 'done'
              AND EXISTS (SELECT 1 FROM creators WHERE creators.handle = queue.handle)
            "#,
            params![&now_rfc3339],
        )
        .context("failed to reconcile queue with scraped creators")?;

        let stale_before =
            (now - ChronoDuration::minutes(STALE_PROCESSING_LOCK_AFTER_MINUTES)).to_rfc3339();
        tx.execute(
            r#"
            UPDATE queue
            SET status = CASE WHEN attempts >= 3 THEN 'failed' ELSE 'retry' END,
                locked_at = NULL,
                last_error = COALESCE(last_error, 'stale processing lock reclaimed')
            WHERE status = 'processing'
              AND locked_at IS NOT NULL
              AND locked_at < ?1
            "#,
            params![stale_before],
        )
        .context("failed to reclaim stale processing queue jobs")?;

        let language_code = normalized_language_filter(filters.language_code);
        let app_name = normalized_specific_app_filter(filters.app_name);
        let app_names = normalized_values(filters.app_names);
        let country_codes = normalized_upper_values(filters.country_codes);
        let handles = normalize_many(filters.handles);
        let mut conditions = vec!["q.status IN ('pending', 'retry')".to_string()];
        if handles.is_empty() {
            conditions.push("COALESCE(policy_app.policy, 'neutral') != 'blacklist'".to_string());
        }
        let mut query_params = Vec::<Value>::new();
        if let Some(language_code) = language_code {
            conditions.push(
                "upper(COALESCE(source.language_code, current.language_code, '')) = ?".to_string(),
            );
            query_params.push(language_code.into());
        }
        if let Some(app_name) = app_name {
            conditions.push("lower(COALESCE(NULLIF(trim(q.expected_app_name), ''), source.promoted_app_name, current.promoted_app_name, '')) = lower(?)".to_string());
            query_params.push(app_name.into());
        }
        push_in_condition(
            &mut conditions,
            &mut query_params,
            "lower(COALESCE(NULLIF(trim(q.expected_app_name), ''), source.promoted_app_name, current.promoted_app_name, ''))",
            &app_names
                .iter()
                .map(|value| value.to_ascii_lowercase())
                .collect::<Vec<_>>(),
        );
        push_in_condition(
            &mut conditions,
            &mut query_params,
            "upper(COALESCE(source.country_code, ''))",
            &country_codes,
        );
        push_in_condition(&mut conditions, &mut query_params, "q.handle", &handles);
        if filters.whitelist_only && app_names.is_empty() {
            conditions.push("COALESCE(policy_app.policy, 'neutral') = 'whitelist'".to_string());
        }
        let sql = format!(
            r#"
            SELECT q.handle, q.attempts
            FROM queue q
            LEFT JOIN creators source ON source.handle = q.discovered_from
            LEFT JOIN creators current ON current.handle = q.handle
            LEFT JOIN apps policy_app
              ON lower(policy_app.name) = lower(COALESCE(NULLIF(trim(q.expected_app_name), ''), source.promoted_app_name, current.promoted_app_name, ''))
            WHERE {}
            ORDER BY
                q.manual_priority DESC,
                CASE WHEN policy_app.policy = 'whitelist' THEN 0 ELSE 1 END ASC,
                q.found_at ASC
            LIMIT 1
            "#,
            conditions.join(" AND ")
        );
        let job = tx
            .query_row(&sql, params_from_iter(query_params), |row| {
                Ok(QueueJob {
                    handle: row.get(0)?,
                    attempts: row.get::<_, i64>(1)? as u32,
                })
            })
            .optional()
            .context("failed to claim queue job")?;

        if let Some(job) = job {
            let attempts = job.attempts + 1;
            tx.execute(
                r#"
                UPDATE queue
                SET status = 'processing',
                    attempts = ?2,
                    locked_at = ?3,
                    last_error = NULL
                WHERE handle = ?1
                "#,
                params![&job.handle, attempts, Utc::now().to_rfc3339()],
            )
            .context("failed to mark queue job as processing")?;
            tx.commit().context("failed to commit queue claim")?;
            Ok(Some(QueueJob { attempts, ..job }))
        } else {
            tx.commit().context("failed to commit empty queue claim")?;
            Ok(None)
        }
    }

    pub async fn mark_done(&self, handle: &str) -> Result<()> {
        let handle = normalize_handle(handle).context("invalid TikTok handle")?;
        let conn = self.conn.lock().await;
        conn.execute(
            r#"
            UPDATE queue
            SET status = 'done',
                scraped_at = ?2,
                locked_at = NULL,
                last_error = NULL
            WHERE handle = ?1
            "#,
            params![handle, Utc::now().to_rfc3339()],
        )
        .context("failed to mark queue job done")?;
        Ok(())
    }

    pub async fn mark_failed(&self, handle: &str, error: &str) -> Result<()> {
        let handle = normalize_handle(handle).context("invalid TikTok handle")?;
        let conn = self.conn.lock().await;
        let attempts = conn
            .query_row(
                "SELECT attempts FROM queue WHERE handle = ?1",
                params![handle],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .context("failed to read failed queue attempts")?
            .unwrap_or(1);
        let status = if attempts >= 3 { "failed" } else { "retry" };
        conn.execute(
            r#"
            UPDATE queue
            SET status = ?2,
                locked_at = NULL,
                last_error = ?3
            WHERE handle = ?1
            "#,
            params![handle, status, truncate(error, 1500)],
        )
        .context("failed to mark queue job failed")?;
        Ok(())
    }

    pub async fn record_creator(&self, creator: &CrawledCreator) -> Result<RecordOutcome> {
        self.record_creator_with_options(creator, RecordCreatorOptions::default())
            .await
    }

    pub async fn record_creator_with_options(
        &self,
        creator: &CrawledCreator,
        options: RecordCreatorOptions,
    ) -> Result<RecordOutcome> {
        let mut conn = self.conn.lock().await;
        let tx = conn
            .transaction()
            .context("failed to open creator record transaction")?;

        let handle =
            normalize_handle(&creator.snapshot.handle).context("invalid creator handle")?;
        let scraped_at = creator.scraped_at.to_rfc3339();
        let app_name = creator.classification.canonical_app_name();
        let contact_name = creator.classification.creator_name.as_deref();
        let email = creator.classification.creator_email.as_deref();
        let country_code = country_code_for_language(&creator.latest_content.language_code)
            .or(creator.snapshot.country_code.as_deref());

        if let Some(app_name) = &app_name {
            tx.execute(
                r#"
                INSERT INTO apps(name, first_seen_at, last_seen_at)
                VALUES(?1, ?2, ?2)
                ON CONFLICT(name) DO UPDATE SET last_seen_at = excluded.last_seen_at
                "#,
                params![app_name, &scraped_at],
            )
            .context("failed to upsert app")?;
        }

        let app_policy = if let Some(app_name) = &app_name {
            tx.query_row(
                "SELECT policy FROM apps WHERE name = ?1",
                params![app_name],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context("failed to read app policy while recording creator")?
            .unwrap_or_else(|| "neutral".to_string())
        } else {
            "neutral".to_string()
        };

        let raw_json = serde_json::to_string(&json!({
            "creator": creator.snapshot.raw,
            "classification": creator.classification,
        }))
        .context("failed to serialize creator raw JSON")?;

        tx.execute(
            r#"
            INSERT INTO creators (
                handle,
                display_name,
                contact_name,
                bio,
                country_code,
                follower_count,
                following_count,
                avg_views,
                median_views,
                most_viral_video_url,
                most_viral_video_views,
                latest_content_text,
                promoted_app_name,
                email,
                language_code,
                scraped_at,
                raw_json
            )
            VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)
            ON CONFLICT(handle) DO UPDATE SET
                display_name = excluded.display_name,
                contact_name = excluded.contact_name,
                bio = excluded.bio,
                country_code = excluded.country_code,
                follower_count = excluded.follower_count,
                following_count = excluded.following_count,
                avg_views = excluded.avg_views,
                median_views = excluded.median_views,
                most_viral_video_url = excluded.most_viral_video_url,
                most_viral_video_views = excluded.most_viral_video_views,
                latest_content_text = excluded.latest_content_text,
                promoted_app_name = excluded.promoted_app_name,
                email = excluded.email,
                language_code = excluded.language_code,
                scraped_at = excluded.scraped_at,
                raw_json = excluded.raw_json
            "#,
            params![
                &handle,
                creator.snapshot.display_name.as_deref(),
                contact_name,
                &creator.snapshot.bio,
                country_code,
                creator.snapshot.follower_count.map(u64_to_i64),
                creator.snapshot.following_count.map(u64_to_i64),
                creator.stats.avg_views,
                creator.stats.median_views,
                creator.stats.most_viral_video_url.as_deref(),
                creator.stats.most_viral_video_views.map(u64_to_i64),
                &creator.latest_content.text,
                app_name.as_deref(),
                email,
                &creator.latest_content.language_code,
                &scraped_at,
                &raw_json,
            ],
        )
        .context("failed to upsert creator")?;

        tx.execute(
            "DELETE FROM videos WHERE creator_handle = ?1",
            params![&handle],
        )
        .context("failed to clear existing videos")?;
        tx.execute(
            "DELETE FROM follows WHERE creator_handle = ?1",
            params![&handle],
        )
        .context("failed to clear existing follows")?;

        let latest_url = creator
            .snapshot
            .videos
            .first()
            .map(|post| post.url.as_str());
        for post in &creator.snapshot.videos {
            let kind = match post.kind {
                TiktokPostKind::Video => "video",
                TiktokPostKind::Photo => "photo",
            };
            let is_latest = Some(post.url.as_str()) == latest_url;
            let is_most_viral = creator
                .stats
                .most_viral_video_url
                .as_deref()
                .is_some_and(|url| url == post.url);
            let raw_json =
                serde_json::to_string(&post.raw).context("failed to serialize video raw JSON")?;

            tx.execute(
                r#"
                INSERT INTO videos (
                    creator_handle,
                    tiktok_url,
                    post_id,
                    kind,
                    views,
                    caption,
                    published_at,
                    source_url,
                    is_pinned,
                    is_latest,
                    is_most_viral,
                    raw_json
                )
                VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                "#,
                params![
                    &handle,
                    &post.url,
                    post.id.as_deref(),
                    kind,
                    u64_to_i64(post.views),
                    post.caption.as_deref(),
                    post.published_at.map(|dt| dt.to_rfc3339()),
                    post.source_url.as_deref(),
                    post.is_pinned as i64,
                    is_latest as i64,
                    is_most_viral as i64,
                    &raw_json,
                ],
            )
            .context("failed to insert video")?;
        }

        let following = normalize_many(&creator.snapshot.following);
        for follows_handle in &following {
            if follows_handle == &handle {
                continue;
            }
            tx.execute(
                r#"
                INSERT OR IGNORE INTO follows(creator_handle, follows_handle)
                VALUES(?1, ?2)
                "#,
                params![&handle, follows_handle],
            )
            .context("failed to insert follow edge")?;
        }

        let mut enqueued_following_count = 0;
        if app_name.is_some() && app_policy != "blacklist" && options.enqueue_following {
            for follows_handle in following {
                if follows_handle == handle {
                    continue;
                }
                let changed = tx
                    .execute(
                        r#"
                        INSERT OR IGNORE INTO queue (
                            handle,
                            discovered_from,
                            expected_app_name,
                            found_at,
                            status,
                            attempts
                        )
                        SELECT ?1, ?2, ?3, ?4, 'pending', 0
                        WHERE NOT EXISTS (
                            SELECT 1 FROM creators WHERE handle = ?1
                        )
                        "#,
                        params![
                            follows_handle,
                            &handle,
                            app_name.as_deref(),
                            Utc::now().to_rfc3339()
                        ],
                    )
                    .context("failed to enqueue followed creator")?;
                enqueued_following_count += changed;
            }
        } else if app_name.is_none() && options.prune_no_app_children {
            prune_queue_children_tx(&tx, &handle)?;
        }

        tx.execute(
            r#"
            UPDATE queue
            SET status = 'done',
                scraped_at = ?2,
                locked_at = NULL,
                last_error = NULL
            WHERE handle = ?1
            "#,
            params![&handle, &scraped_at],
        )
        .context("failed to mark recorded creator queue job done")?;

        tx.commit()
            .context("failed to commit creator record transaction")?;

        Ok(RecordOutcome {
            promoted_app_name: app_name,
            enqueued_following_count,
        })
    }

    pub async fn list_app_names(&self) -> Result<Vec<String>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn
            .prepare("SELECT name FROM apps ORDER BY lower(name)")
            .context("failed to prepare app list query")?;
        let names = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .context("failed to query apps")?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("failed to read app names")?;
        Ok(names)
    }

    pub async fn add_app_name(&self, name: &str) -> Result<bool> {
        let name = name.trim();
        if name.is_empty() {
            anyhow::bail!("app name cannot be empty");
        }
        let now = Utc::now().to_rfc3339();
        let conn = self.conn.lock().await;
        let changed = conn
            .execute(
                r#"
                INSERT OR IGNORE INTO apps(name, first_seen_at, last_seen_at)
                VALUES(?1, ?2, ?2)
                "#,
                params![name, now],
            )
            .context("failed to add app name")?;
        Ok(changed > 0)
    }

    pub async fn set_app_policy(&self, name: &str, policy: &str) -> Result<usize> {
        let name = name.trim();
        if name.is_empty() {
            anyhow::bail!("app name cannot be empty");
        }
        let policy = normalize_app_policy(policy)?;
        let now = Utc::now().to_rfc3339();
        let mut conn = self.conn.lock().await;
        let tx = conn
            .transaction()
            .context("failed to open app policy transaction")?;
        tx.execute(
            r#"
            INSERT INTO apps(name, first_seen_at, last_seen_at, policy)
            VALUES(?1, ?2, ?2, ?3)
            ON CONFLICT(name) DO UPDATE SET
                last_seen_at = excluded.last_seen_at,
                policy = excluded.policy
            "#,
            params![name, &now, policy],
        )
        .context("failed to update app policy")?;

        let relation = r#"lower(COALESCE(
            NULLIF(trim(queue.expected_app_name), ''),
            (SELECT promoted_app_name FROM creators WHERE handle = queue.discovered_from),
            (SELECT promoted_app_name FROM creators WHERE handle = queue.handle),
            ''
        )) = lower(?1)"#;
        let changed = if policy == "blacklist" {
            tx.execute(
                &format!(
                    "UPDATE queue SET status = 'excluded', locked_at = NULL, last_error = 'app blacklisted' WHERE status IN ('pending', 'retry') AND {relation}"
                ),
                params![name],
            )
            .context("failed to exclude blacklisted app queue rows")?
        } else {
            tx.execute(
                &format!(
                    "UPDATE queue SET status = 'pending', last_error = NULL WHERE status = 'excluded' AND last_error = 'app blacklisted' AND {relation}"
                ),
                params![name],
            )
            .context("failed to restore app queue rows")?
        };
        tx.commit().context("failed to commit app policy")?;
        Ok(changed)
    }

    pub async fn set_creator_app(&self, handle: &str, app_name: Option<&str>) -> Result<usize> {
        let handle = normalize_handle(handle).context("invalid TikTok handle")?;
        let app_name = app_name
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_string);
        let now = Utc::now().to_rfc3339();
        let mut conn = self.conn.lock().await;
        let tx = conn
            .transaction()
            .context("failed to open creator app override transaction")?;
        if let Some(app_name) = &app_name {
            tx.execute(
                r#"
                INSERT INTO apps(name, first_seen_at, last_seen_at)
                VALUES(?1, ?2, ?2)
                ON CONFLICT(name) DO UPDATE SET last_seen_at = excluded.last_seen_at
                "#,
                params![app_name, &now],
            )
            .context("failed to upsert app for creator override")?;
        }
        let changed = tx
            .execute(
                "UPDATE creators SET promoted_app_name = ?2 WHERE handle = ?1",
                params![&handle, app_name.as_deref()],
            )
            .context("failed to update creator app classification")?;
        if changed == 0 {
            anyhow::bail!("creator was not found");
        }
        let pruned = if app_name.is_none() {
            prune_queue_children_tx(&tx, &handle)?
        } else {
            0
        };
        tx.commit()
            .context("failed to commit creator app override transaction")?;
        Ok(pruned)
    }

    pub async fn set_creator_contact_status(&self, handle: &str, status: &str) -> Result<()> {
        let handle = normalize_handle(handle).context("invalid TikTok handle")?;
        let status = normalize_contact_status(status)?;
        let now = Utc::now().to_rfc3339();
        let conn = self.conn.lock().await;
        let changed = conn
            .execute(
                r#"
                UPDATE creators
                SET contact_status = ?2,
                    contact_priority_at = CASE
                        WHEN ?2 = 'to_contact' THEN COALESCE(contact_priority_at, ?3)
                        WHEN ?2 = 'unselected' THEN NULL
                        ELSE contact_priority_at
                    END,
                    contacted_at = CASE
                        WHEN ?2 = 'contacted' THEN COALESCE(contacted_at, ?3)
                        WHEN ?2 != 'contacted' THEN NULL
                        ELSE contacted_at
                    END
                WHERE handle = ?1
                "#,
                params![&handle, status, &now],
            )
            .context("failed to update creator contact state")?;
        if changed == 0 {
            anyhow::bail!("creator was not found");
        }
        conn.execute(
            "UPDATE queue SET manual_priority = CASE WHEN ?2 = 'to_contact' THEN 1 ELSE manual_priority END WHERE handle = ?1 AND status IN ('pending', 'retry')",
            params![handle, status],
        )
        .context("failed to prioritize contacted creator in queue")?;
        Ok(())
    }

    pub async fn set_creator_email(&self, handle: &str, email: Option<&str>) -> Result<()> {
        let handle = normalize_handle(handle).context("invalid TikTok handle")?;
        let email = email
            .map(str::trim)
            .filter(|email| !email.is_empty() && !email.eq_ignore_ascii_case("unknown"))
            .map(str::to_ascii_lowercase);
        if email.as_deref().is_some_and(|email| {
            let Some((local, domain)) = email.split_once('@') else {
                return true;
            };
            local.is_empty()
                || domain.is_empty()
                || domain.contains('@')
                || !domain.contains('.')
                || domain.starts_with('.')
                || domain.ends_with('.')
                || email.contains(char::is_whitespace)
        }) {
            anyhow::bail!("email address is not valid");
        }

        let conn = self.conn.lock().await;
        let changed = conn
            .execute(
                "UPDATE creators SET email = ?2 WHERE handle = ?1",
                params![handle, email],
            )
            .context("failed to update creator email")?;
        if changed == 0 {
            anyhow::bail!("creator was not found");
        }
        Ok(())
    }

    pub async fn queue_status_counts(&self) -> Result<Vec<QueueStatusCount>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn
            .prepare("SELECT status, count(*) FROM queue GROUP BY status ORDER BY status")
            .context("failed to prepare queue status count query")?;
        stmt.query_map([], |row| {
            Ok(QueueStatusCount {
                status: row.get(0)?,
                count: row.get::<_, i64>(1)? as usize,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("failed to read queue status counts")
    }

    pub async fn list_queue_view(
        &self,
        status: Option<&str>,
        language_code: Option<&str>,
        app_name: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<QueueViewItem>> {
        let conn = self.conn.lock().await;
        let limit = limit.min(1000) as i64;
        let offset = offset as i64;
        let status = status.map(str::trim).filter(|status| !status.is_empty());
        let language_code = normalized_language_filter(language_code);
        let app_name = normalized_specific_app_filter(app_name);

        let mut stmt = conn
            .prepare(
                r#"
                SELECT
                    q.handle,
                    q.discovered_from,
                    q.found_at,
                    q.status,
                    q.attempts,
                    q.last_error,
                    q.expected_app_name,
                    COALESCE(NULLIF(trim(q.expected_app_name), ''), source.promoted_app_name, current.promoted_app_name),
                    COALESCE(source.language_code, current.language_code),
                    source.country_code,
                    COALESCE(policy_app.policy, 'neutral')
                FROM queue q
                LEFT JOIN creators source ON source.handle = q.discovered_from
                LEFT JOIN creators current ON current.handle = q.handle
                LEFT JOIN apps policy_app
                  ON lower(policy_app.name) = lower(COALESCE(NULLIF(trim(q.expected_app_name), ''), source.promoted_app_name, current.promoted_app_name, ''))
                WHERE (?1 IS NULL OR q.status = ?1)
                  AND (
                      ?2 IS NULL
                      OR upper(COALESCE(source.language_code, current.language_code, '')) = ?2
                  )
                  AND (
                      ?3 IS NULL
                      OR lower(COALESCE(NULLIF(trim(q.expected_app_name), ''), source.promoted_app_name, current.promoted_app_name, '')) = lower(?3)
                  )
                ORDER BY
                    q.manual_priority DESC,
                    CASE WHEN policy_app.policy = 'whitelist' THEN 0 ELSE 1 END ASC,
                    q.found_at ASC
                LIMIT ?4
                OFFSET ?5
                "#,
            )
            .context("failed to prepare queue view query")?;
        stmt.query_map(
            params![status, language_code, app_name, limit, offset],
            |row| {
                Ok(QueueViewItem {
                    handle: row.get(0)?,
                    discovered_from: row.get(1)?,
                    found_at: row.get(2)?,
                    status: row.get(3)?,
                    attempts: row.get::<_, i64>(4)? as u32,
                    last_error: row.get(5)?,
                    expected_app_name: row.get(6)?,
                    inferred_app_name: row.get(7)?,
                    inferred_language_code: row.get(8)?,
                    inferred_country_code: row.get(9)?,
                    app_policy: row.get(10)?,
                })
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("failed to read queue view")
    }

    pub async fn count_queue_view(
        &self,
        status: Option<&str>,
        language_code: Option<&str>,
        app_name: Option<&str>,
    ) -> Result<usize> {
        let conn = self.conn.lock().await;
        let status = status.map(str::trim).filter(|status| !status.is_empty());
        let language_code = normalized_language_filter(language_code);
        let app_name = normalized_specific_app_filter(app_name);
        conn.query_row(
            r#"
            SELECT count(*)
            FROM queue q
            LEFT JOIN creators source ON source.handle = q.discovered_from
            LEFT JOIN creators current ON current.handle = q.handle
            WHERE (?1 IS NULL OR q.status = ?1)
              AND (
                  ?2 IS NULL
                  OR upper(COALESCE(source.language_code, current.language_code, '')) = ?2
              )
              AND (
                  ?3 IS NULL
                  OR lower(COALESCE(NULLIF(trim(q.expected_app_name), ''), source.promoted_app_name, current.promoted_app_name, '')) = lower(?3)
              )
            "#,
            params![status, language_code, app_name],
            |row| row.get::<_, i64>(0),
        )
        .map(|count| count as usize)
        .context("failed to count queue view")
    }

    pub async fn list_queue_source_view(
        &self,
        status: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<QueueSourceViewItem>> {
        let conn = self.conn.lock().await;
        let limit = limit.min(1000) as i64;
        let offset = offset as i64;
        let status = status.map(str::trim).filter(|status| !status.is_empty());
        let mut stmt = conn
            .prepare(
                r#"
                SELECT
                    q.discovered_from,
                    count(*),
                    sum(CASE WHEN q.status != 'done' THEN 1 ELSE 0 END),
                    min(q.found_at),
                    source.country_code,
                    source.promoted_app_name,
                    COALESCE(policy_app.policy, 'neutral')
                FROM queue q
                LEFT JOIN creators source ON source.handle = q.discovered_from
                LEFT JOIN apps policy_app
                  ON lower(policy_app.name) = lower(COALESCE(source.promoted_app_name, ''))
                WHERE q.discovered_from IS NOT NULL
                  AND (?1 IS NULL OR q.status = ?1)
                GROUP BY
                    q.discovered_from,
                    source.country_code,
                    source.promoted_app_name,
                    policy_app.policy
                ORDER BY
                    max(q.manual_priority) DESC,
                    CASE WHEN policy_app.policy = 'whitelist' THEN 0 ELSE 1 END ASC,
                    min(q.found_at) ASC
                LIMIT ?2
                OFFSET ?3
                "#,
            )
            .context("failed to prepare queue source view query")?;
        stmt.query_map(params![status, limit, offset], |row| {
            Ok(QueueSourceViewItem {
                source_handle: row.get(0)?,
                item_count: row.get::<_, i64>(1)? as usize,
                removable_count: row.get::<_, i64>(2)? as usize,
                oldest_found_at: row.get(3)?,
                country_code: row.get(4)?,
                app_name: row.get(5)?,
                app_policy: row.get(6)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("failed to read queue source view")
    }

    pub async fn count_queue_source_view(&self, status: Option<&str>) -> Result<usize> {
        let conn = self.conn.lock().await;
        let status = status.map(str::trim).filter(|status| !status.is_empty());
        conn.query_row(
            r#"
            SELECT count(DISTINCT discovered_from)
            FROM queue
            WHERE discovered_from IS NOT NULL
              AND (?1 IS NULL OR status = ?1)
            "#,
            params![status],
            |row| row.get::<_, i64>(0),
        )
        .map(|count| count as usize)
        .context("failed to count queue source view")
    }

    pub async fn list_languages(&self) -> Result<Vec<String>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn
            .prepare(
                r#"
                SELECT DISTINCT language_code
                FROM creators
                WHERE language_code IS NOT NULL
                  AND language_code != ''
                  AND language_code != 'UNKNOWN'
                ORDER BY language_code
                "#,
            )
            .context("failed to prepare language list query")?;
        stmt.query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("failed to read languages")
    }

    pub async fn list_countries(&self) -> Result<Vec<String>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn
            .prepare(
                r#"
                SELECT DISTINCT upper(country_code)
                FROM creators
                WHERE country_code IS NOT NULL AND trim(country_code) != ''
                ORDER BY upper(country_code)
                "#,
            )
            .context("failed to prepare country list query")?;
        stmt.query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("failed to read countries")
    }

    pub async fn list_queue_parent_countries(&self) -> Result<Vec<String>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn
            .prepare(
                r#"
                SELECT DISTINCT upper(source.country_code)
                FROM queue q
                JOIN creators source ON source.handle = q.discovered_from
                WHERE source.country_code IS NOT NULL
                  AND trim(source.country_code) != ''
                  AND q.status IN ('pending', 'retry')
                ORDER BY upper(source.country_code)
                "#,
            )
            .context("failed to prepare queue parent country list query")?;
        stmt.query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("failed to read queue parent countries")
    }

    pub async fn list_queue_parents_needing_country(&self) -> Result<Vec<String>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn
            .prepare(
                r#"
                SELECT DISTINCT q.discovered_from
                FROM queue q
                JOIN creators source ON source.handle = q.discovered_from
                WHERE q.status IN ('pending', 'retry')
                  AND q.discovered_from IS NOT NULL
                  AND (source.country_code IS NULL OR trim(source.country_code) = '')
                  AND source.country_checked_at IS NULL
                ORDER BY q.discovered_from
                "#,
            )
            .context("failed to prepare parent country backfill query")?;
        stmt.query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("failed to read parents needing country backfill")
    }

    pub async fn record_creator_country_lookup(
        &self,
        handle: &str,
        country_code: Option<&str>,
        error: Option<&str>,
    ) -> Result<()> {
        let handle = normalize_handle(handle).context("invalid TikTok handle")?;
        let country_code = country_code
            .map(str::trim)
            .filter(|code| !code.is_empty())
            .map(str::to_ascii_uppercase);
        let error = error.map(|error| truncate(error, 1000));
        let conn = self.conn.lock().await;
        let changed = conn
            .execute(
                r#"
                UPDATE creators
                SET country_code = ?2,
                    country_checked_at = ?3,
                    country_error = ?4
                WHERE handle = ?1
                "#,
                params![handle, country_code, Utc::now().to_rfc3339(), error],
            )
            .context("failed to record creator country lookup")?;
        if changed == 0 {
            anyhow::bail!("creator was not found");
        }
        Ok(())
    }

    pub async fn remove_queue_items_from_source(&self, source_handle: &str) -> Result<usize> {
        let source_handle =
            normalize_handle(source_handle).context("invalid source TikTok handle")?;
        let conn = self.conn.lock().await;
        conn.execute(
            "DELETE FROM queue WHERE discovered_from = ?1 AND status != 'done'",
            params![source_handle],
        )
        .context("failed to remove queue items from source")
    }

    pub async fn list_app_summaries(&self) -> Result<Vec<AppSummary>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn
            .prepare(
                r#"
                SELECT apps.name, count(creators.handle), apps.policy
                FROM apps
                LEFT JOIN creators ON creators.promoted_app_name = apps.name
                GROUP BY apps.name
                ORDER BY lower(apps.name)
                "#,
            )
            .context("failed to prepare app summary query")?;
        stmt.query_map([], |row| {
            Ok(AppSummary {
                name: row.get(0)?,
                creator_count: row.get::<_, i64>(1)? as usize,
                policy: row.get(2)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("failed to read app summaries")
    }

    pub async fn list_creators(
        &self,
        sort_by: &str,
        sort_dir: &str,
        filters: CreatorListFilters<'_>,
    ) -> Result<Vec<CreatorListItem>> {
        let conn = self.conn.lock().await;
        let order_column = match sort_by {
            "avg_views" => "c.avg_views",
            "median_views" => "c.median_views",
            "followers" | "follower_count" => "c.follower_count",
            "following" | "following_count" => "c.following_count",
            "scraped_at" => "c.scraped_at",
            "contact_priority" => "c.contact_priority_at",
            _ => "c.median_views",
        };
        let order_dir = if sort_dir.eq_ignore_ascii_case("asc") {
            "ASC"
        } else {
            "DESC"
        };
        let app_mode = normalized_app_mode(filters.app_mode);
        let app_names = json_filter_values(filters.app_names, false)?;
        let language_codes = json_filter_values(filters.language_codes, true)?;
        let country_codes = json_filter_values(filters.country_codes, true)?;
        let contact_statuses = json_filter_values(filters.contact_statuses, false)?;
        let email_filter = normalized_email_filter(filters.email_filter);
        let sql = format!(
            r#"
            SELECT
                c.handle,
                c.display_name,
                c.contact_name,
                c.promoted_app_name,
                c.email,
                c.country_code,
                c.follower_count,
                c.following_count,
                c.avg_views,
                c.median_views,
                c.most_viral_video_url,
                c.most_viral_video_views,
                c.language_code,
                c.scraped_at,
                c.contact_status,
                c.contact_priority_at,
                c.contacted_at,
                (SELECT count(*) FROM videos v WHERE v.creator_handle = c.handle),
                (SELECT count(*) FROM follows f WHERE f.creator_handle = c.handle)
            FROM creators c
            WHERE (
                ?1 IS NULL
                OR (?1 = '__WITH_APP__' AND c.promoted_app_name IS NOT NULL AND trim(c.promoted_app_name) != '')
                OR (?1 = '__NO_APP__' AND (c.promoted_app_name IS NULL OR trim(c.promoted_app_name) = ''))
            )
              AND (?2 IS NULL OR EXISTS (SELECT 1 FROM json_each(?2) f WHERE lower(f.value) = lower(c.promoted_app_name)))
              AND (?3 IS NULL OR EXISTS (SELECT 1 FROM json_each(?3) f WHERE upper(f.value) = upper(c.language_code)))
              AND (?4 IS NULL OR EXISTS (SELECT 1 FROM json_each(?4) f WHERE upper(f.value) = upper(c.country_code)))
              AND (?5 IS NULL OR EXISTS (SELECT 1 FROM json_each(?5) f WHERE lower(f.value) = lower(c.contact_status)))
              AND (
                  ?6 IS NULL
                  OR (?6 = 'has' AND c.email IS NOT NULL AND trim(c.email) != '')
                  OR (?6 = 'none' AND (c.email IS NULL OR trim(c.email) = ''))
              )
              AND (?7 IS NULL OR c.follower_count >= ?7)
              AND (?8 IS NULL OR c.follower_count <= ?8)
              AND (?9 IS NULL OR c.following_count >= ?9)
              AND (?10 IS NULL OR c.following_count <= ?10)
              AND (?11 IS NULL OR c.median_views >= ?11)
              AND (?12 IS NULL OR c.median_views <= ?12)
              AND (?13 IS NULL OR c.avg_views >= ?13)
              AND (?14 IS NULL OR c.avg_views <= ?14)
            ORDER BY {order_column} {order_dir}, lower(c.handle) ASC
            LIMIT ?15 OFFSET ?16
            "#
        );
        let mut stmt = conn
            .prepare(&sql)
            .context("failed to prepare creator list query")?;
        stmt.query_map(
            params![
                app_mode,
                app_names,
                language_codes,
                country_codes,
                contact_statuses,
                email_filter,
                filters.min_followers,
                filters.max_followers,
                filters.min_following,
                filters.max_following,
                filters.min_median_views,
                filters.max_median_views,
                filters.min_avg_views,
                filters.max_avg_views,
                filters.limit.clamp(1, 500) as i64,
                filters.offset as i64,
            ],
            |row| {
                Ok(CreatorListItem {
                    handle: row.get(0)?,
                    display_name: row.get(1)?,
                    contact_name: row.get(2)?,
                    promoted_app_name: row.get(3)?,
                    email: row.get(4)?,
                    country_code: row.get(5)?,
                    follower_count: row.get(6)?,
                    following_count: row.get(7)?,
                    avg_views: row.get(8)?,
                    median_views: row.get(9)?,
                    most_viral_video_url: row.get(10)?,
                    most_viral_video_views: row.get(11)?,
                    language_code: row.get(12)?,
                    scraped_at: row.get(13)?,
                    contact_status: row.get(14)?,
                    contact_priority_at: row.get(15)?,
                    contacted_at: row.get(16)?,
                    videos_count: row.get::<_, i64>(17)? as usize,
                    follows_count: row.get::<_, i64>(18)? as usize,
                })
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("failed to read creators")
    }

    pub async fn count_creators(&self, filters: CreatorListFilters<'_>) -> Result<usize> {
        let conn = self.conn.lock().await;
        let app_mode = normalized_app_mode(filters.app_mode);
        let app_names = json_filter_values(filters.app_names, false)?;
        let language_codes = json_filter_values(filters.language_codes, true)?;
        let country_codes = json_filter_values(filters.country_codes, true)?;
        let contact_statuses = json_filter_values(filters.contact_statuses, false)?;
        let email_filter = normalized_email_filter(filters.email_filter);
        conn.query_row(
            r#"
            SELECT count(*)
            FROM creators c
            WHERE (
                ?1 IS NULL
                OR (?1 = '__WITH_APP__' AND c.promoted_app_name IS NOT NULL AND trim(c.promoted_app_name) != '')
                OR (?1 = '__NO_APP__' AND (c.promoted_app_name IS NULL OR trim(c.promoted_app_name) = ''))
            )
              AND (?2 IS NULL OR EXISTS (SELECT 1 FROM json_each(?2) f WHERE lower(f.value) = lower(c.promoted_app_name)))
              AND (?3 IS NULL OR EXISTS (SELECT 1 FROM json_each(?3) f WHERE upper(f.value) = upper(c.language_code)))
              AND (?4 IS NULL OR EXISTS (SELECT 1 FROM json_each(?4) f WHERE upper(f.value) = upper(c.country_code)))
              AND (?5 IS NULL OR EXISTS (SELECT 1 FROM json_each(?5) f WHERE lower(f.value) = lower(c.contact_status)))
              AND (
                  ?6 IS NULL
                  OR (?6 = 'has' AND c.email IS NOT NULL AND trim(c.email) != '')
                  OR (?6 = 'none' AND (c.email IS NULL OR trim(c.email) = ''))
              )
              AND (?7 IS NULL OR c.follower_count >= ?7)
              AND (?8 IS NULL OR c.follower_count <= ?8)
              AND (?9 IS NULL OR c.following_count >= ?9)
              AND (?10 IS NULL OR c.following_count <= ?10)
              AND (?11 IS NULL OR c.median_views >= ?11)
              AND (?12 IS NULL OR c.median_views <= ?12)
              AND (?13 IS NULL OR c.avg_views >= ?13)
              AND (?14 IS NULL OR c.avg_views <= ?14)
            "#,
            params![
                app_mode,
                app_names,
                language_codes,
                country_codes,
                contact_statuses,
                email_filter,
                filters.min_followers,
                filters.max_followers,
                filters.min_following,
                filters.max_following,
                filters.min_median_views,
                filters.max_median_views,
                filters.min_avg_views,
                filters.max_avg_views,
            ],
            |row| row.get::<_, i64>(0),
        )
        .map(|count| count as usize)
        .context("failed to count creators")
    }

    pub async fn list_language_review_creators(
        &self,
        limit: Option<usize>,
    ) -> Result<Vec<LanguageReviewCreator>> {
        let conn = self.conn.lock().await;
        let sql = if limit.is_some() {
            r#"
            SELECT handle, bio, latest_content_text, language_code
            FROM creators
            ORDER BY lower(handle) ASC
            LIMIT ?1
            "#
        } else {
            r#"
            SELECT handle, bio, latest_content_text, language_code
            FROM creators
            ORDER BY lower(handle) ASC
            "#
        };

        let mut stmt = conn
            .prepare(sql)
            .context("failed to prepare language review query")?;
        let map_row = |row: &rusqlite::Row<'_>| {
            Ok(LanguageReviewCreator {
                handle: row.get(0)?,
                bio: row.get(1)?,
                latest_content_text: row.get(2)?,
                language_code: row.get(3)?,
            })
        };

        if let Some(limit) = limit {
            stmt.query_map(params![limit as i64], map_row)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .context("failed to read language review creators")
        } else {
            stmt.query_map([], map_row)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .context("failed to read language review creators")
        }
    }

    pub async fn update_creator_language(&self, handle: &str, language_code: &str) -> Result<()> {
        let handle = normalize_handle(handle).context("invalid TikTok handle")?;
        let language_code = language_code.trim().to_ascii_uppercase();
        let conn = self.conn.lock().await;
        conn.execute(
            "UPDATE creators SET language_code = ?2 WHERE handle = ?1",
            params![handle, language_code],
        )
        .context("failed to update creator language")?;
        Ok(())
    }

    pub async fn list_queue(&self, status: Option<&str>, limit: usize) -> Result<Vec<QueueItem>> {
        let conn = self.conn.lock().await;
        let limit = limit.min(1000) as i64;

        if let Some(status) = status {
            let mut stmt = conn
                .prepare(
                    r#"
                    SELECT handle, discovered_from, found_at, status, attempts, last_error
                    FROM queue
                    WHERE status = ?1
                    ORDER BY
                        CASE
                            WHEN expected_app_name IS NOT NULL
                             AND trim(expected_app_name) != ''
                            THEN 0
                            ELSE 1
                        END ASC,
                        found_at ASC
                    LIMIT ?2
                    "#,
                )
                .context("failed to prepare queue list query")?;
            read_queue_items(stmt.query(params![status, limit])?)
        } else {
            let mut stmt = conn
                .prepare(
                    r#"
                    SELECT handle, discovered_from, found_at, status, attempts, last_error
                    FROM queue
                    ORDER BY
                        CASE
                            WHEN expected_app_name IS NOT NULL
                             AND trim(expected_app_name) != ''
                            THEN 0
                            ELSE 1
                        END ASC,
                        found_at ASC
                    LIMIT ?1
                    "#,
                )
                .context("failed to prepare queue list query")?;
            read_queue_items(stmt.query(params![limit])?)
        }
    }

    pub async fn add_frontier_seed(&self, handle: &str, source: Option<&str>) -> Result<bool> {
        let handle = normalize_handle(handle).context("invalid TikTok handle")?;
        let source = source
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        let now = Utc::now().to_rfc3339();
        let conn = self.conn.lock().await;
        let changed = conn
            .execute(
                r#"
                INSERT OR IGNORE INTO frontier_bucket(handle, added_at, source)
                VALUES(?1, ?2, ?3)
                "#,
                params![handle, now, source],
            )
            .context("failed to add frontier seed")?;
        Ok(changed > 0)
    }

    pub async fn remove_frontier_seed(&self, handle: &str) -> Result<bool> {
        let handle = normalize_handle(handle).context("invalid TikTok handle")?;
        let conn = self.conn.lock().await;
        let changed = conn
            .execute(
                "DELETE FROM frontier_bucket WHERE handle = ?1",
                params![handle],
            )
            .context("failed to remove frontier seed")?;
        Ok(changed > 0)
    }

    pub async fn list_frontier_bucket(&self, limit: usize) -> Result<Vec<FrontierBucketItem>> {
        let conn = self.conn.lock().await;
        let limit = limit.min(1000) as i64;
        let mut stmt = conn
            .prepare(
                r#"
                SELECT
                    b.handle,
                    b.added_at,
                    b.source,
                    c.promoted_app_name,
                    c.language_code,
                    c.follower_count,
                    c.following_count,
                    (SELECT count(*) FROM follows f WHERE f.creator_handle = b.handle)
                FROM frontier_bucket b
                LEFT JOIN creators c ON c.handle = b.handle
                ORDER BY b.added_at ASC, lower(b.handle) ASC
                LIMIT ?1
                "#,
            )
            .context("failed to prepare frontier bucket query")?;
        stmt.query_map(params![limit], |row| {
            Ok(FrontierBucketItem {
                handle: row.get(0)?,
                added_at: row.get(1)?,
                source: row.get(2)?,
                promoted_app_name: row.get(3)?,
                language_code: row.get(4)?,
                follower_count: row.get(5)?,
                following_count: row.get(6)?,
                follows_count: row.get::<_, i64>(7)? as usize,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("failed to read frontier bucket")
    }

    pub async fn count_frontier_bucket(&self) -> Result<usize> {
        let conn = self.conn.lock().await;
        conn.query_row("SELECT count(*) FROM frontier_bucket", [], |row| {
            row.get::<_, i64>(0)
        })
        .map(|count| count as usize)
        .context("failed to count frontier bucket")
    }

    pub async fn create_frontier_run_from_bucket(
        &self,
        depth_limit: u8,
    ) -> Result<FrontierRunRecord> {
        let run_id = {
            let mut conn = self.conn.lock().await;
            let tx = conn
                .transaction()
                .context("failed to open frontier run transaction")?;
            let seed_count = tx
                .query_row("SELECT count(*) FROM frontier_bucket", [], |row| {
                    row.get::<_, i64>(0)
                })
                .context("failed to count frontier bucket seeds")?;
            if seed_count == 0 {
                anyhow::bail!("frontier bucket is empty");
            }

            let now = Utc::now().to_rfc3339();
            tx.execute(
                r#"
                INSERT INTO frontier_runs(
                    status,
                    created_at,
                    started_at,
                    seed_count,
                    depth_limit
                )
                VALUES('running', ?1, ?1, ?2, ?3)
                "#,
                params![&now, seed_count, depth_limit as i64],
            )
            .context("failed to create frontier run")?;
            let run_id = tx.last_insert_rowid();
            tx.execute(
                r#"
                INSERT OR IGNORE INTO frontier_items(
                    run_id,
                    handle,
                    depth,
                    discovered_from,
                    found_at,
                    status,
                    attempts
                )
                SELECT ?1, handle, 0, NULL, ?2, 'pending', 0
                FROM frontier_bucket
                "#,
                params![run_id, &now],
            )
            .context("failed to seed frontier run items")?;
            tx.commit()
                .context("failed to commit frontier run creation")?;
            run_id
        };

        self.get_frontier_run(run_id)
            .await?
            .context("created frontier run was not found")
    }

    pub async fn get_frontier_run(&self, run_id: i64) -> Result<Option<FrontierRunRecord>> {
        let conn = self.conn.lock().await;
        conn.query_row(
            r#"
            SELECT
                id,
                status,
                created_at,
                started_at,
                finished_at,
                seed_count,
                depth_limit,
                processed,
                succeeded,
                failed,
                skipped,
                last_error
            FROM frontier_runs
            WHERE id = ?1
            "#,
            params![run_id],
            read_frontier_run_row,
        )
        .optional()
        .context("failed to read frontier run")
    }

    pub async fn latest_frontier_run(&self) -> Result<Option<FrontierRunRecord>> {
        let conn = self.conn.lock().await;
        conn.query_row(
            r#"
            SELECT
                id,
                status,
                created_at,
                started_at,
                finished_at,
                seed_count,
                depth_limit,
                processed,
                succeeded,
                failed,
                skipped,
                last_error
            FROM frontier_runs
            ORDER BY id DESC
            LIMIT 1
            "#,
            [],
            read_frontier_run_row,
        )
        .optional()
        .context("failed to read latest frontier run")
    }

    pub async fn claim_next_frontier_item(&self, run_id: i64) -> Result<Option<FrontierJob>> {
        let mut conn = self.conn.lock().await;
        let tx = conn
            .transaction()
            .context("failed to open frontier claim transaction")?;
        let now = Utc::now();
        let stale_before =
            (now - ChronoDuration::minutes(STALE_PROCESSING_LOCK_AFTER_MINUTES)).to_rfc3339();
        tx.execute(
            r#"
            UPDATE frontier_items
            SET status = CASE WHEN attempts >= 3 THEN 'failed' ELSE 'retry' END,
                locked_at = NULL,
                last_error = COALESCE(last_error, 'stale processing lock reclaimed')
            WHERE run_id = ?1
              AND status = 'processing'
              AND locked_at IS NOT NULL
              AND locked_at < ?2
            "#,
            params![run_id, stale_before],
        )
        .context("failed to reclaim stale frontier jobs")?;

        let job = tx
            .query_row(
                r#"
                SELECT run_id, handle, depth, attempts
                FROM frontier_items
                WHERE run_id = ?1
                  AND status IN ('pending', 'retry')
                ORDER BY depth ASC, found_at ASC, lower(handle) ASC
                LIMIT 1
                "#,
                params![run_id],
                |row| {
                    Ok(FrontierJob {
                        run_id: row.get(0)?,
                        handle: row.get(1)?,
                        depth: row.get::<_, i64>(2)? as u8,
                        attempts: row.get::<_, i64>(3)? as u32,
                    })
                },
            )
            .optional()
            .context("failed to claim frontier job")?;

        if let Some(job) = job {
            let attempts = job.attempts + 1;
            tx.execute(
                r#"
                UPDATE frontier_items
                SET status = 'processing',
                    attempts = ?3,
                    locked_at = ?4,
                    last_error = NULL
                WHERE run_id = ?1
                  AND handle = ?2
                "#,
                params![run_id, &job.handle, attempts, Utc::now().to_rfc3339()],
            )
            .context("failed to mark frontier job as processing")?;
            tx.commit().context("failed to commit frontier claim")?;
            Ok(Some(FrontierJob { attempts, ..job }))
        } else {
            tx.commit()
                .context("failed to commit empty frontier claim")?;
            Ok(None)
        }
    }

    pub async fn mark_frontier_item_done(&self, run_id: i64, handle: &str) -> Result<()> {
        let handle = normalize_handle(handle).context("invalid TikTok handle")?;
        let conn = self.conn.lock().await;
        conn.execute(
            r#"
            UPDATE frontier_items
            SET status = 'done',
                scraped_at = ?3,
                locked_at = NULL,
                last_error = NULL
            WHERE run_id = ?1
              AND handle = ?2
            "#,
            params![run_id, handle, Utc::now().to_rfc3339()],
        )
        .context("failed to mark frontier item done")?;
        Ok(())
    }

    pub async fn mark_frontier_item_failed(
        &self,
        run_id: i64,
        handle: &str,
        error: &str,
    ) -> Result<()> {
        let handle = normalize_handle(handle).context("invalid TikTok handle")?;
        let conn = self.conn.lock().await;
        let attempts = conn
            .query_row(
                r#"
                SELECT attempts
                FROM frontier_items
                WHERE run_id = ?1
                  AND handle = ?2
                "#,
                params![run_id, &handle],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .context("failed to read failed frontier attempts")?
            .unwrap_or(1);
        let status = if attempts >= 3 { "failed" } else { "retry" };
        conn.execute(
            r#"
            UPDATE frontier_items
            SET status = ?3,
                locked_at = NULL,
                last_error = ?4
            WHERE run_id = ?1
              AND handle = ?2
            "#,
            params![run_id, handle, status, truncate(error, 1500)],
        )
        .context("failed to mark frontier item failed")?;
        Ok(())
    }

    pub async fn enqueue_frontier_following(
        &self,
        run_id: i64,
        discovered_from: &str,
        depth: u8,
        handles: &[String],
    ) -> Result<usize> {
        let discovered_from =
            normalize_handle(discovered_from).context("invalid frontier source handle")?;
        let following = normalize_many(handles);
        let now = Utc::now().to_rfc3339();
        let conn = self.conn.lock().await;
        let mut inserted = 0;
        for handle in following {
            if handle == discovered_from {
                continue;
            }
            let changed = conn
                .execute(
                    r#"
                    INSERT OR IGNORE INTO frontier_items(
                        run_id,
                        handle,
                        depth,
                        discovered_from,
                        found_at,
                        status,
                        attempts
                    )
                    VALUES(?1, ?2, ?3, ?4, ?5, 'pending', 0)
                    "#,
                    params![run_id, handle, depth as i64, &discovered_from, &now],
                )
                .context("failed to enqueue frontier following")?;
            inserted += changed;
        }
        Ok(inserted)
    }

    pub async fn list_following_handles(&self, handle: &str) -> Result<Vec<String>> {
        let handle = normalize_handle(handle).context("invalid TikTok handle")?;
        let conn = self.conn.lock().await;
        let mut stmt = conn
            .prepare(
                r#"
                SELECT follows_handle
                FROM follows
                WHERE creator_handle = ?1
                ORDER BY lower(follows_handle) ASC
                "#,
            )
            .context("failed to prepare following handles query")?;
        stmt.query_map(params![handle], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("failed to read following handles")
    }

    pub async fn finish_frontier_run(
        &self,
        run_id: i64,
        finish: FrontierRunFinish<'_>,
    ) -> Result<()> {
        let status = finish.status.trim();
        if !matches!(status, "completed" | "stopped" | "failed") {
            anyhow::bail!("invalid frontier run status");
        }
        let conn = self.conn.lock().await;
        conn.execute(
            r#"
            UPDATE frontier_runs
            SET status = ?2,
                finished_at = ?3,
                processed = ?4,
                succeeded = ?5,
                failed = ?6,
                skipped = ?7,
                last_error = ?8
            WHERE id = ?1
            "#,
            params![
                run_id,
                status,
                Utc::now().to_rfc3339(),
                finish.processed as i64,
                finish.succeeded as i64,
                finish.failed as i64,
                finish.skipped as i64,
                finish.last_error.map(|error| truncate(error, 1500)),
            ],
        )
        .context("failed to finish frontier run")?;
        Ok(())
    }

    pub async fn frontier_item_status_counts(
        &self,
        run_id: Option<i64>,
    ) -> Result<Vec<QueueStatusCount>> {
        let Some(run_id) = self.resolve_frontier_run_id(run_id).await? else {
            return Ok(Vec::new());
        };
        let conn = self.conn.lock().await;
        let mut stmt = conn
            .prepare(
                r#"
                SELECT status, count(*)
                FROM frontier_items
                WHERE run_id = ?1
                GROUP BY status
                ORDER BY status
                "#,
            )
            .context("failed to prepare frontier status count query")?;
        stmt.query_map(params![run_id], |row| {
            Ok(QueueStatusCount {
                status: row.get(0)?,
                count: row.get::<_, i64>(1)? as usize,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("failed to read frontier status counts")
    }

    pub async fn list_frontier_run_items(
        &self,
        run_id: Option<i64>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<FrontierRunItemView>> {
        let Some(run_id) = self.resolve_frontier_run_id(run_id).await? else {
            return Ok(Vec::new());
        };
        let conn = self.conn.lock().await;
        let limit = limit.min(1000) as i64;
        let offset = offset as i64;
        let mut stmt = conn
            .prepare(
                r#"
                SELECT
                    i.run_id,
                    i.handle,
                    i.depth,
                    i.discovered_from,
                    i.found_at,
                    i.status,
                    i.attempts,
                    i.last_error,
                    i.scraped_at,
                    c.promoted_app_name,
                    c.language_code,
                    c.follower_count,
                    c.following_count,
                    (SELECT count(*) FROM follows f WHERE f.creator_handle = i.handle)
                FROM frontier_items i
                LEFT JOIN creators c ON c.handle = i.handle
                WHERE i.run_id = ?1
                ORDER BY i.depth ASC, i.found_at ASC, lower(i.handle) ASC
                LIMIT ?2
                OFFSET ?3
                "#,
            )
            .context("failed to prepare frontier item query")?;
        stmt.query_map(params![run_id, limit, offset], |row| {
            Ok(FrontierRunItemView {
                run_id: row.get(0)?,
                handle: row.get(1)?,
                depth: row.get::<_, i64>(2)? as u8,
                discovered_from: row.get(3)?,
                found_at: row.get(4)?,
                status: row.get(5)?,
                attempts: row.get::<_, i64>(6)? as u32,
                last_error: row.get(7)?,
                scraped_at: row.get(8)?,
                promoted_app_name: row.get(9)?,
                language_code: row.get(10)?,
                follower_count: row.get(11)?,
                following_count: row.get(12)?,
                follows_count: row.get::<_, i64>(13)? as usize,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("failed to read frontier run items")
    }

    pub async fn count_frontier_run_items(&self, run_id: Option<i64>) -> Result<usize> {
        let Some(run_id) = self.resolve_frontier_run_id(run_id).await? else {
            return Ok(0);
        };
        let conn = self.conn.lock().await;
        conn.query_row(
            "SELECT count(*) FROM frontier_items WHERE run_id = ?1",
            params![run_id],
            |row| row.get::<_, i64>(0),
        )
        .map(|count| count as usize)
        .context("failed to count frontier run items")
    }

    async fn resolve_frontier_run_id(&self, run_id: Option<i64>) -> Result<Option<i64>> {
        if run_id.is_some() {
            return Ok(run_id);
        }
        Ok(self.latest_frontier_run().await?.map(|run| run.id))
    }
}

fn ensure_column(conn: &Connection, table: &str, column: &str, definition: &str) -> Result<()> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info({table})"))
        .with_context(|| format!("failed to inspect SQLite table {table}"))?;
    let exists = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .any(|name| name == column);

    if !exists {
        conn.execute(
            &format!("ALTER TABLE {table} ADD COLUMN {column} {definition}"),
            [],
        )
        .with_context(|| format!("failed to add SQLite column {table}.{column}"))?;
    }

    Ok(())
}

fn prune_queue_children_tx(tx: &Transaction<'_>, discovered_from: &str) -> Result<usize> {
    tx.execute(
        r#"
        DELETE FROM queue
        WHERE discovered_from = ?1
          AND status IN ('pending', 'retry', 'failed')
        "#,
        params![discovered_from],
    )
    .context("failed to prune queued creators discovered from no-app creator")
}

fn normalized_app_mode(app_name: Option<&str>) -> Option<String> {
    let value = app_name.map(str::trim).filter(|value| !value.is_empty())?;
    match value.to_ascii_lowercase().as_str() {
        "__with_app__" | "with_app" | "all_apps" => Some("__WITH_APP__".to_string()),
        "__no_app__" | "no_app" | "none" => Some("__NO_APP__".to_string()),
        _ => None,
    }
}

fn normalize_app_policy(policy: &str) -> Result<&str> {
    match policy.trim().to_ascii_lowercase().as_str() {
        "whitelist" => Ok("whitelist"),
        "neutral" => Ok("neutral"),
        "blacklist" => Ok("blacklist"),
        _ => anyhow::bail!("app policy must be whitelist, neutral, or blacklist"),
    }
}

fn normalize_contact_status(status: &str) -> Result<&str> {
    match status.trim().to_ascii_lowercase().as_str() {
        "unselected" | "none" => Ok("unselected"),
        "to_contact" | "queued" => Ok("to_contact"),
        "contacted" => Ok("contacted"),
        _ => anyhow::bail!("contact status must be unselected, to_contact, or contacted"),
    }
}

fn normalized_values(values: &[String]) -> Vec<String> {
    let mut values = values
        .iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    values.sort_by_key(|value| value.to_ascii_lowercase());
    values.dedup_by(|left, right| left.eq_ignore_ascii_case(right));
    values
}

fn normalized_upper_values(values: &[String]) -> Vec<String> {
    normalized_values(values)
        .into_iter()
        .map(|value| value.to_ascii_uppercase())
        .collect()
}

fn push_in_condition(
    conditions: &mut Vec<String>,
    params: &mut Vec<Value>,
    expression: &str,
    values: &[String],
) {
    if values.is_empty() {
        return;
    }
    let placeholders = std::iter::repeat_n("?", values.len())
        .collect::<Vec<_>>()
        .join(", ");
    conditions.push(format!("{expression} IN ({placeholders})"));
    params.extend(values.iter().cloned().map(Value::from));
}

fn json_filter_values(values: &[String], uppercase: bool) -> Result<Option<String>> {
    let values = if uppercase {
        normalized_upper_values(values)
    } else {
        normalized_values(values)
    };
    if values.is_empty() {
        return Ok(None);
    }
    serde_json::to_string(&values)
        .map(Some)
        .context("failed to serialize list filter")
}

fn normalized_email_filter(email_filter: Option<&str>) -> Option<String> {
    email_filter
        .map(str::trim)
        .filter(|value| !value.is_empty() && !value.eq_ignore_ascii_case("all"))
        .and_then(|value| match value.to_ascii_lowercase().as_str() {
            "has" | "yes" | "true" | "with" => Some("has".to_string()),
            "none" | "no" | "false" | "without" => Some("none".to_string()),
            _ => None,
        })
}

fn normalized_specific_app_filter(app_name: Option<&str>) -> Option<String> {
    app_name
        .map(str::trim)
        .filter(|value| {
            !value.is_empty()
                && !value.eq_ignore_ascii_case("all")
                && !value.eq_ignore_ascii_case("all apps")
                && !value.eq_ignore_ascii_case("__with_app__")
                && !value.eq_ignore_ascii_case("__no_app__")
        })
        .map(str::to_string)
}

fn read_queue_items(mut rows: rusqlite::Rows<'_>) -> Result<Vec<QueueItem>> {
    let mut items = Vec::new();
    while let Some(row) = rows.next().context("failed to read queue row")? {
        items.push(QueueItem {
            handle: row.get(0)?,
            discovered_from: row.get(1)?,
            found_at: row.get(2)?,
            status: row.get(3)?,
            attempts: row.get::<_, i64>(4)? as u32,
            last_error: row.get(5)?,
        });
    }
    Ok(items)
}

fn read_frontier_run_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<FrontierRunRecord> {
    Ok(FrontierRunRecord {
        id: row.get(0)?,
        status: row.get(1)?,
        created_at: row.get(2)?,
        started_at: row.get(3)?,
        finished_at: row.get(4)?,
        seed_count: row.get::<_, i64>(5)? as usize,
        depth_limit: row.get::<_, i64>(6)? as u8,
        processed: row.get::<_, i64>(7)? as usize,
        succeeded: row.get::<_, i64>(8)? as usize,
        failed: row.get::<_, i64>(9)? as usize,
        skipped: row.get::<_, i64>(10)? as usize,
        last_error: row.get(11)?,
    })
}

fn u64_to_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

fn normalized_language_filter(language_code: Option<&str>) -> Option<String> {
    language_code
        .map(str::trim)
        .filter(|value| !value.is_empty() && !value.eq_ignore_ascii_case("all"))
        .map(str::to_ascii_uppercase)
}

fn country_code_for_language(language_code: &str) -> Option<&'static str> {
    match language_code.trim().to_ascii_uppercase().as_str() {
        "ENG" => Some("GB"),
        "SPA" => Some("ES"),
        "ITA" => Some("IT"),
        "POL" => Some("PL"),
        "DEU" => Some("DE"),
        "SWE" => Some("SE"),
        "FRA" => Some("FR"),
        "POR" => Some("PT"),
        "NOR" | "NOB" => Some("NO"),
        "HRV" => Some("HR"),
        "CES" => Some("CZ"),
        "SLK" => Some("SK"),
        "SLV" => Some("SI"),
        "RON" => Some("RO"),
        "NLD" => Some("NL"),
        "FIN" => Some("FI"),
        "DAN" => Some("DK"),
        "EST" => Some("EE"),
        "TUR" => Some("TR"),
        "JPN" => Some("JP"),
        "KOR" => Some("KR"),
        "VIE" => Some("VN"),
        "UKR" => Some("UA"),
        "RUS" => Some("RU"),
        "SRP" => Some("RS"),
        "BUL" => Some("BG"),
        "SQI" => Some("AL"),
        "IND" => Some("ID"),
        "HAT" => Some("HT"),
        _ => None,
    }
}

fn truncate(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }
    value.chars().take(max_chars).collect::<String>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{
        AppClassification, CreatorSnapshot, CreatorStats, ExtractedContent, TiktokPost,
        TiktokPostKind,
    };

    #[test]
    fn maps_classified_languages_to_parent_markets() {
        assert_eq!(country_code_for_language("SLK"), Some("SK"));
        assert_eq!(country_code_for_language("CES"), Some("CZ"));
        assert_eq!(country_code_for_language("SPA"), Some("ES"));
        assert_eq!(country_code_for_language("UNKNOWN"), None);
    }

    #[tokio::test]
    async fn enqueues_following_only_when_creator_promotes_app() {
        let tempdir = tempfile::tempdir().unwrap();
        let db = Database::open(tempdir.path().join("test.sqlite")).unwrap();
        db.seed_handle("@seed", None).await.unwrap();

        let creator = CrawledCreator {
            snapshot: CreatorSnapshot {
                handle: "seed".to_string(),
                display_name: None,
                country_code: None,
                bio: "ExampleApp ambassador".to_string(),
                follower_count: Some(1234),
                following_count: Some(77),
                following: vec!["@next".to_string(), "@next".to_string()],
                videos: vec![TiktokPost {
                    id: None,
                    url: "https://tiktok.test/@seed/video/1".to_string(),
                    caption: None,
                    views: 100,
                    published_at: None,
                    kind: TiktokPostKind::Video,
                    is_pinned: false,
                    source_url: None,
                    slide_image_urls: Vec::new(),
                    visual_image_urls: Vec::new(),
                    raw: serde_json::Value::Null,
                }],
                raw: serde_json::Value::Null,
            },
            stats: CreatorStats {
                avg_views: 100.0,
                median_views: 100.0,
                most_viral_video_url: Some("https://tiktok.test/@seed/video/1".to_string()),
                most_viral_video_views: Some(100),
            },
            latest_content: ExtractedContent {
                text: "Use ExampleApp".to_string(),
                language_code: "EN".to_string(),
            },
            classification: AppClassification {
                promotes_app: true,
                app_name: Some("ExampleApp".to_string()),
                is_existing_app: false,
                existing_app_name: None,
                is_new_app: true,
                supporting_post_count: 2,
                language_code: "ENG".to_string(),
                creator_name: None,
                creator_email: None,
                confidence: 0.95,
                evidence: "bio".to_string(),
            },
            scraped_at: Utc::now(),
        };

        let outcome = db.record_creator(&creator).await.unwrap();
        assert_eq!(outcome.enqueued_following_count, 1);
        assert_eq!(db.list_app_names().await.unwrap(), vec!["ExampleApp"]);
        assert!(db.is_scraped("seed").await.unwrap());
        let creators = db
            .list_creators(
                "followers",
                "desc",
                CreatorListFilters {
                    min_followers: Some(1000),
                    max_followers: Some(2000),
                    min_following: Some(50),
                    max_following: Some(100),
                    ..CreatorListFilters::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(creators[0].follower_count, Some(1234));
        assert_eq!(creators[0].following_count, Some(77));
    }

    #[tokio::test]
    async fn claims_whitelisted_app_queue_items_before_older_regular_items() {
        let tempdir = tempfile::tempdir().unwrap();
        let db = Database::open(tempdir.path().join("test.sqlite")).unwrap();

        db.enqueue_handle("@older_regular", None).await.unwrap();
        db.enqueue_handle_with_app("@newer_app_seed", None, Some("Astra AI"))
            .await
            .unwrap();
        db.set_app_policy("Astra AI", "whitelist").await.unwrap();

        {
            let conn = db.conn.lock().await;
            conn.execute(
                "UPDATE queue SET found_at = '2026-01-01T00:00:00Z' WHERE handle = 'older_regular'",
                [],
            )
            .unwrap();
            conn.execute(
                "UPDATE queue SET found_at = '2026-01-02T00:00:00Z' WHERE handle = 'newer_app_seed'",
                [],
            )
            .unwrap();
        }

        let job = db
            .claim_next_filtered(QueueClaimFilters::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.handle, "newer_app_seed");
    }

    #[tokio::test]
    async fn blacklisted_apps_are_excluded_and_can_be_restored() {
        let tempdir = tempfile::tempdir().unwrap();
        let db = Database::open(tempdir.path().join("test.sqlite")).unwrap();
        db.enqueue_handle_with_app("@blocked_creator", None, Some("Blocked App"))
            .await
            .unwrap();

        assert_eq!(
            db.set_app_policy("Blocked App", "blacklist").await.unwrap(),
            1
        );
        assert!(
            db.claim_next_filtered(QueueClaimFilters::default())
                .await
                .unwrap()
                .is_none()
        );

        assert_eq!(
            db.set_app_policy("Blocked App", "neutral").await.unwrap(),
            1
        );
        let job = db
            .claim_next_filtered(QueueClaimFilters::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.handle, "blocked_creator");
    }

    #[tokio::test]
    async fn explicit_handle_batch_overrides_app_blacklist() {
        let tempdir = tempfile::tempdir().unwrap();
        let db = Database::open(tempdir.path().join("test.sqlite")).unwrap();
        db.enqueue_handle_with_app("@selected_creator", None, Some("Blocked App"))
            .await
            .unwrap();
        db.set_app_policy("Blocked App", "blacklist").await.unwrap();
        let handles = vec!["@selected_creator".to_string()];
        db.prioritize_handles(&handles).await.unwrap();

        let job = db
            .claim_next_filtered(QueueClaimFilters {
                handles: &handles,
                ..QueueClaimFilters::default()
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.handle, "selected_creator");
    }

    #[tokio::test]
    async fn claims_queue_items_by_inferred_app() {
        let tempdir = tempfile::tempdir().unwrap();
        let db = Database::open(tempdir.path().join("test.sqlite")).unwrap();

        let creator = CrawledCreator {
            snapshot: CreatorSnapshot {
                handle: "source".to_string(),
                display_name: None,
                country_code: None,
                bio: String::new(),
                follower_count: None,
                following_count: None,
                following: vec!["astra_candidate".to_string()],
                videos: Vec::new(),
                raw: serde_json::Value::Null,
            },
            stats: CreatorStats {
                avg_views: 0.0,
                median_views: 0.0,
                most_viral_video_url: None,
                most_viral_video_views: None,
            },
            latest_content: ExtractedContent {
                text: String::new(),
                language_code: "ENG".to_string(),
            },
            classification: AppClassification {
                promotes_app: true,
                app_name: Some("Astra AI".to_string()),
                is_existing_app: false,
                existing_app_name: None,
                is_new_app: true,
                supporting_post_count: 2,
                language_code: "ENG".to_string(),
                creator_name: None,
                creator_email: None,
                confidence: 0.9,
                evidence: String::new(),
            },
            scraped_at: Utc::now(),
        };
        db.record_creator(&creator).await.unwrap();
        db.enqueue_handle_with_app("manual_ick_candidate", None, Some("The Ick"))
            .await
            .unwrap();

        let ick_job = db
            .claim_next_filtered(QueueClaimFilters {
                app_name: Some("The Ick"),
                ..QueueClaimFilters::default()
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ick_job.handle, "manual_ick_candidate");

        let astra_job = db
            .claim_next_filtered(QueueClaimFilters {
                app_name: Some("Astra AI"),
                ..QueueClaimFilters::default()
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(astra_job.handle, "astra_candidate");
    }

    #[tokio::test]
    async fn queue_country_filter_uses_the_parent_creator_country() {
        let tempdir = tempfile::tempdir().unwrap();
        let db = Database::open(tempdir.path().join("test.sqlite")).unwrap();
        {
            let conn = db.conn.lock().await;
            conn.execute(
                r#"
                INSERT INTO creators(
                    handle, bio, country_code, avg_views, median_views,
                    latest_content_text, language_code, scraped_at, raw_json
                ) VALUES('parent', '', NULL, 0, 0, '', 'DEU', ?1, '{}')
                "#,
                params![Utc::now().to_rfc3339()],
            )
            .unwrap();
        }
        db.enqueue_handle("@child", Some("parent")).await.unwrap();

        assert_eq!(
            db.list_queue_parents_needing_country().await.unwrap(),
            vec!["parent".to_string()]
        );
        db.record_creator_country_lookup("parent", Some("de"), None)
            .await
            .unwrap();

        assert_eq!(
            db.list_queue_parent_countries().await.unwrap(),
            vec!["DE".to_string()]
        );
        let countries = vec!["DE".to_string()];
        let job = db
            .claim_next_filtered(QueueClaimFilters {
                country_codes: &countries,
                ..QueueClaimFilters::default()
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.handle, "child");
        let sources = db
            .list_queue_source_view(Some("processing"), 10, 0)
            .await
            .unwrap();
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].source_handle, "parent");
        assert_eq!(sources[0].item_count, 1);
        assert_eq!(sources[0].country_code.as_deref(), Some("DE"));
        assert_eq!(
            db.remove_queue_items_from_source("parent").await.unwrap(),
            1
        );
        assert_eq!(db.count_queue_view(None, None, None).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn clearing_creator_app_prunes_unprocessed_discovered_queue_items() {
        let tempdir = tempfile::tempdir().unwrap();
        let db = Database::open(tempdir.path().join("test.sqlite")).unwrap();

        let creator = CrawledCreator {
            snapshot: CreatorSnapshot {
                handle: "source".to_string(),
                display_name: None,
                country_code: None,
                bio: String::new(),
                follower_count: None,
                following_count: None,
                following: vec!["child_a".to_string(), "child_b".to_string()],
                videos: Vec::new(),
                raw: serde_json::Value::Null,
            },
            stats: CreatorStats {
                avg_views: 0.0,
                median_views: 0.0,
                most_viral_video_url: None,
                most_viral_video_views: None,
            },
            latest_content: ExtractedContent {
                text: String::new(),
                language_code: "ENG".to_string(),
            },
            classification: AppClassification {
                promotes_app: true,
                app_name: Some("Astra AI".to_string()),
                is_existing_app: false,
                existing_app_name: None,
                is_new_app: true,
                supporting_post_count: 2,
                language_code: "ENG".to_string(),
                creator_name: None,
                creator_email: None,
                confidence: 0.9,
                evidence: String::new(),
            },
            scraped_at: Utc::now(),
        };
        db.record_creator(&creator).await.unwrap();

        let queued_before = db.list_queue(None, 10).await.unwrap();
        assert_eq!(queued_before.len(), 2);

        let pruned = db.set_creator_app("source", None).await.unwrap();
        assert_eq!(pruned, 2);
        assert!(db.list_queue(None, 10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn frontier_recording_keeps_follow_edges_without_global_queue_enqueue() {
        let tempdir = tempfile::tempdir().unwrap();
        let db = Database::open(tempdir.path().join("test.sqlite")).unwrap();

        let creator = CrawledCreator {
            snapshot: CreatorSnapshot {
                handle: "source".to_string(),
                display_name: None,
                country_code: None,
                bio: String::new(),
                follower_count: None,
                following_count: None,
                following: vec!["child_a".to_string(), "child_b".to_string()],
                videos: Vec::new(),
                raw: serde_json::Value::Null,
            },
            stats: CreatorStats {
                avg_views: 0.0,
                median_views: 0.0,
                most_viral_video_url: None,
                most_viral_video_views: None,
            },
            latest_content: ExtractedContent {
                text: String::new(),
                language_code: "ENG".to_string(),
            },
            classification: AppClassification {
                promotes_app: true,
                app_name: Some("Astra AI".to_string()),
                is_existing_app: false,
                existing_app_name: None,
                is_new_app: true,
                supporting_post_count: 2,
                language_code: "ENG".to_string(),
                creator_name: None,
                creator_email: None,
                confidence: 0.9,
                evidence: String::new(),
            },
            scraped_at: Utc::now(),
        };

        let outcome = db
            .record_creator_with_options(
                &creator,
                RecordCreatorOptions {
                    enqueue_following: false,
                    prune_no_app_children: false,
                },
            )
            .await
            .unwrap();

        assert_eq!(outcome.enqueued_following_count, 0);
        assert!(db.list_queue(None, 10).await.unwrap().is_empty());
        assert_eq!(
            db.list_following_handles("source").await.unwrap(),
            vec!["child_a".to_string(), "child_b".to_string()]
        );
    }

    #[tokio::test]
    async fn frontier_run_seeds_depth_zero_and_expands_to_depth_one() {
        let tempdir = tempfile::tempdir().unwrap();
        let db = Database::open(tempdir.path().join("test.sqlite")).unwrap();

        assert!(db.add_frontier_seed("@seed", Some("test")).await.unwrap());
        assert!(!db.add_frontier_seed("seed", Some("test")).await.unwrap());
        let run = db.create_frontier_run_from_bucket(1).await.unwrap();
        assert_eq!(run.seed_count, 1);

        let seed_job = db.claim_next_frontier_item(run.id).await.unwrap().unwrap();
        assert_eq!(seed_job.handle, "seed");
        assert_eq!(seed_job.depth, 0);

        let inserted = db
            .enqueue_frontier_following(
                run.id,
                "seed",
                1,
                &[
                    "@child_a".to_string(),
                    "@child_b".to_string(),
                    "@child_a".to_string(),
                ],
            )
            .await
            .unwrap();
        assert_eq!(inserted, 2);
        db.mark_frontier_item_done(run.id, "seed").await.unwrap();

        let items = db
            .list_frontier_run_items(Some(run.id), 10, 0)
            .await
            .unwrap();
        let depths = items
            .iter()
            .map(|item| {
                (
                    item.handle.as_str(),
                    item.depth,
                    item.discovered_from.as_deref(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            depths,
            vec![
                ("seed", 0, None),
                ("child_a", 1, Some("seed")),
                ("child_b", 1, Some("seed")),
            ]
        );
    }

    #[tokio::test]
    async fn claims_retry_queue_items_again() {
        let tempdir = tempfile::tempdir().unwrap();
        let db = Database::open(tempdir.path().join("test.sqlite")).unwrap();

        db.enqueue_handle("@retry_me", None).await.unwrap();
        let first_claim = db
            .claim_next_filtered(QueueClaimFilters::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first_claim.handle, "retry_me");
        db.mark_failed("retry_me", "temporary timeout")
            .await
            .unwrap();

        let second_claim = db
            .claim_next_filtered(QueueClaimFilters::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second_claim.handle, "retry_me");
        assert_eq!(second_claim.attempts, 2);
    }

    #[tokio::test]
    async fn reclaims_stale_processing_queue_items() {
        let tempdir = tempfile::tempdir().unwrap();
        let db = Database::open(tempdir.path().join("test.sqlite")).unwrap();

        db.enqueue_handle("@stale_job", None).await.unwrap();
        let first_claim = db
            .claim_next_filtered(QueueClaimFilters::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first_claim.handle, "stale_job");

        {
            let conn = db.conn.lock().await;
            conn.execute(
                "UPDATE queue SET locked_at = '2026-01-01T00:00:00+00:00' WHERE handle = 'stale_job'",
                [],
            )
            .unwrap();
        }

        let second_claim = db
            .claim_next_filtered(QueueClaimFilters::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second_claim.handle, "stale_job");
        assert_eq!(second_claim.attempts, 2);
    }

    #[tokio::test]
    async fn creator_email_is_stored_from_gpt_classification() {
        let tempdir = tempfile::tempdir().unwrap();
        let db = Database::open(tempdir.path().join("test.sqlite")).unwrap();

        let creator = CrawledCreator {
            snapshot: CreatorSnapshot {
                handle: "seed".to_string(),
                display_name: None,
                country_code: None,
                bio: "Business: seed@example.com".to_string(),
                follower_count: None,
                following_count: None,
                following: Vec::new(),
                videos: Vec::new(),
                raw: serde_json::Value::Null,
            },
            stats: CreatorStats {
                avg_views: 0.0,
                median_views: 0.0,
                most_viral_video_url: None,
                most_viral_video_views: None,
            },
            latest_content: ExtractedContent {
                text: String::new(),
                language_code: "UNKNOWN".to_string(),
            },
            classification: AppClassification {
                creator_name: Some("Seed".to_string()),
                creator_email: Some("seed@example.com".to_string()),
                ..AppClassification::no_app()
            },
            scraped_at: Utc::now(),
        };

        db.record_creator(&creator).await.unwrap();
        let creators = db
            .list_creators(
                "median_views",
                "desc",
                CreatorListFilters {
                    email_filter: Some("has"),
                    ..CreatorListFilters::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(creators[0].email.as_deref(), Some("seed@example.com"));

        db.set_creator_email("seed", Some("Updated@Example.com"))
            .await
            .unwrap();
        let creators = db
            .list_creators("median_views", "desc", CreatorListFilters::default())
            .await
            .unwrap();
        assert_eq!(creators[0].email.as_deref(), Some("updated@example.com"));

        db.set_creator_email("seed", Some("unknown")).await.unwrap();
        let creators = db
            .list_creators("median_views", "desc", CreatorListFilters::default())
            .await
            .unwrap();
        assert_eq!(creators[0].email, None);
    }

    #[tokio::test]
    async fn creator_app_filter_separates_any_app_from_no_app() {
        let tempdir = tempfile::tempdir().unwrap();
        let db = Database::open(tempdir.path().join("test.sqlite")).unwrap();

        let mut creator = CrawledCreator {
            snapshot: CreatorSnapshot {
                handle: "with_app".to_string(),
                display_name: None,
                country_code: None,
                bio: String::new(),
                follower_count: None,
                following_count: None,
                following: Vec::new(),
                videos: Vec::new(),
                raw: serde_json::Value::Null,
            },
            stats: CreatorStats {
                avg_views: 0.0,
                median_views: 0.0,
                most_viral_video_url: None,
                most_viral_video_views: None,
            },
            latest_content: ExtractedContent {
                text: String::new(),
                language_code: "UNKNOWN".to_string(),
            },
            classification: AppClassification {
                promotes_app: true,
                app_name: Some("Astra AI".to_string()),
                is_existing_app: false,
                existing_app_name: None,
                is_new_app: true,
                supporting_post_count: 2,
                language_code: "ENG".to_string(),
                creator_name: None,
                creator_email: None,
                confidence: 0.9,
                evidence: String::new(),
            },
            scraped_at: Utc::now(),
        };
        db.record_creator(&creator).await.unwrap();

        creator.snapshot.handle = "without_app".to_string();
        creator.classification = AppClassification::no_app();
        db.record_creator(&creator).await.unwrap();

        let with_app = db
            .list_creators(
                "median_views",
                "desc",
                CreatorListFilters {
                    app_mode: Some("__WITH_APP__"),
                    ..CreatorListFilters::default()
                },
            )
            .await
            .unwrap();
        let no_app = db
            .list_creators(
                "median_views",
                "desc",
                CreatorListFilters {
                    app_mode: Some("__NO_APP__"),
                    ..CreatorListFilters::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(
            with_app
                .iter()
                .map(|item| item.handle.as_str())
                .collect::<Vec<_>>(),
            vec!["with_app"]
        );
        assert_eq!(
            no_app
                .iter()
                .map(|item| item.handle.as_str())
                .collect::<Vec<_>>(),
            vec!["without_app"]
        );
    }
}
