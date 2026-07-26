use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::Result;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{StatusCode, header},
    response::{Html, IntoResponse, Response},
    routing::{delete, get, patch, post},
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::{net::TcpListener, sync::Mutex};
use tracing::{info, warn};

use crate::{
    crawler::{Crawler, QueueRunFilters, RunSummary},
    db::{
        AppSummary, CreatorListFilters, CreatorListItem, Database, FrontierBucketItem,
        FrontierRunFinish, FrontierRunItemView, FrontierRunRecord, QueueSourceViewItem,
        QueueStatusCount, QueueViewItem,
    },
};

#[derive(Clone)]
pub struct WebState {
    db: Database,
    crawler: Crawler,
    runner: Arc<Mutex<RunnerState>>,
    frontier_runner: Arc<Mutex<FrontierRunnerState>>,
}

#[derive(Debug, Default)]
struct RunnerState {
    running: bool,
    stopping: bool,
    stop_requested: Option<Arc<AtomicBool>>,
    started_at: Option<String>,
    finished_at: Option<String>,
    country_codes: Vec<String>,
    app_names: Vec<String>,
    handles: Vec<String>,
    whitelist_only: bool,
    limit: Option<usize>,
    concurrency: usize,
    last_summary: Option<RunSummary>,
    last_error: Option<String>,
}

#[derive(Debug, Default)]
struct FrontierRunnerState {
    running: bool,
    stopping: bool,
    stop_requested: Option<Arc<AtomicBool>>,
    started_at: Option<String>,
    finished_at: Option<String>,
    run_id: Option<i64>,
    limit: Option<usize>,
    concurrency: usize,
    refresh_seeds: bool,
    last_summary: Option<RunSummary>,
    last_error: Option<String>,
}

#[derive(Debug, Serialize)]
struct RunnerStatus {
    running: bool,
    stopping: bool,
    started_at: Option<String>,
    finished_at: Option<String>,
    country_codes: Vec<String>,
    app_names: Vec<String>,
    handles: Vec<String>,
    whitelist_only: bool,
    limit: Option<usize>,
    concurrency: usize,
    last_summary: Option<RunSummary>,
    last_error: Option<String>,
    queue_counts: Vec<QueueStatusCount>,
}

#[derive(Debug, Serialize)]
struct FrontierRunnerStatus {
    running: bool,
    stopping: bool,
    started_at: Option<String>,
    finished_at: Option<String>,
    run_id: Option<i64>,
    limit: Option<usize>,
    concurrency: usize,
    refresh_seeds: bool,
    last_summary: Option<RunSummary>,
    last_error: Option<String>,
    bucket_count: usize,
    latest_run: Option<FrontierRunRecord>,
    item_counts: Vec<QueueStatusCount>,
}

#[derive(Debug, Deserialize)]
struct QueueQuery {
    status: Option<String>,
    language: Option<String>,
    app: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct CreatorQuery {
    sort: Option<String>,
    dir: Option<String>,
    app_mode: Option<String>,
    apps: Option<String>,
    languages: Option<String>,
    countries: Option<String>,
    contact_statuses: Option<String>,
    email: Option<String>,
    min_followers: Option<i64>,
    max_followers: Option<i64>,
    min_following: Option<i64>,
    max_following: Option<i64>,
    min_median_views: Option<f64>,
    max_median_views: Option<f64>,
    min_avg_views: Option<f64>,
    max_avg_views: Option<f64>,
    limit: Option<usize>,
    offset: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct RunRequest {
    concurrency: Option<usize>,
    limit: Option<usize>,
    countries: Option<Vec<String>>,
    apps: Option<Vec<String>>,
    handles: Option<Vec<String>>,
    whitelist_only: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct FrontierRunRequest {
    concurrency: Option<usize>,
    limit: Option<usize>,
    refresh_seeds: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct AddAppRequest {
    name: String,
}

#[derive(Debug, Deserialize)]
struct AppPolicyRequest {
    policy: String,
}

#[derive(Debug, Deserialize)]
struct SeedRequest {
    handle: String,
    app_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FrontierSeedRequest {
    handle: Option<String>,
    handles: Option<Vec<String>>,
    source: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ClassificationOverrideRequest {
    app_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct EmailOverrideRequest {
    email: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ContactStatusRequest {
    status: String,
}

#[derive(Debug, Serialize)]
struct MutationResponse {
    ok: bool,
    changed: bool,
}

#[derive(Debug, Serialize)]
struct QueueSourceMutationResponse {
    ok: bool,
    removed: usize,
}

#[derive(Debug, Serialize)]
struct FrontierMutationResponse {
    ok: bool,
    changed: bool,
    changed_count: usize,
}

#[derive(Debug, Serialize)]
struct AppsResponse {
    apps: Vec<AppSummary>,
}

#[derive(Debug, Serialize)]
struct CreatorsResponse {
    creators: Vec<CreatorListItem>,
    total: usize,
    limit: usize,
    offset: usize,
    has_next: bool,
    has_prev: bool,
}

#[derive(Debug, Serialize)]
struct LanguagesResponse {
    languages: Vec<String>,
}

#[derive(Debug, Serialize)]
struct CountriesResponse {
    countries: Vec<String>,
}

#[derive(Debug, Serialize)]
struct QueueResponse {
    items: Vec<QueueViewItem>,
    counts: Vec<QueueStatusCount>,
    total: usize,
    limit: usize,
    offset: usize,
    has_next: bool,
    has_prev: bool,
}

#[derive(Debug, Serialize)]
struct QueueSourcesResponse {
    items: Vec<QueueSourceViewItem>,
    total: usize,
    limit: usize,
    offset: usize,
    has_next: bool,
    has_prev: bool,
}

#[derive(Debug, Serialize)]
struct FrontierBucketResponse {
    items: Vec<FrontierBucketItem>,
    total: usize,
}

#[derive(Debug, Deserialize)]
struct FrontierItemsQuery {
    run_id: Option<i64>,
    limit: Option<usize>,
    offset: Option<usize>,
}

#[derive(Debug, Serialize)]
struct FrontierItemsResponse {
    run: Option<FrontierRunRecord>,
    items: Vec<FrontierRunItemView>,
    counts: Vec<QueueStatusCount>,
    total: usize,
    limit: usize,
    offset: usize,
    has_next: bool,
    has_prev: bool,
}

pub async fn serve(db: Database, crawler: Crawler, host: String, port: u16) -> Result<()> {
    let state = WebState {
        db,
        crawler,
        runner: Arc::new(Mutex::new(RunnerState {
            concurrency: 10,
            ..RunnerState::default()
        })),
        frontier_runner: Arc::new(Mutex::new(FrontierRunnerState {
            concurrency: 10,
            refresh_seeds: true,
            ..FrontierRunnerState::default()
        })),
    };

    let country_backfill_crawler = state.crawler.clone();
    tokio::spawn(async move {
        match country_backfill_crawler.backfill_parent_countries(10).await {
            Ok(summary) => info!(
                checked = summary.checked,
                updated = summary.updated,
                unresolved = summary.unresolved,
                "finished parent country backfill"
            ),
            Err(error) => warn!(
                error = %format!("{error:#}"),
                "parent country backfill failed"
            ),
        }
    });

    let app = Router::new()
        .route("/", get(index))
        .route("/app.js", get(app_js))
        .route("/lucide.js", get(lucide_js))
        .route("/styles.css", get(styles_css))
        .route("/api/health", get(health))
        .route("/api/queue", get(queue))
        .route("/api/queue/sources", get(queue_sources))
        .route("/api/queue/countries", get(queue_parent_countries))
        .route("/api/queue/source/:handle", delete(remove_queue_source))
        .route("/api/languages", get(languages))
        .route("/api/countries", get(countries))
        .route("/api/apps", get(apps).post(add_app))
        .route("/api/apps/:name/policy", patch(update_app_policy))
        .route("/api/creators", get(creators))
        .route(
            "/api/creators/:handle/classification",
            patch(update_classification),
        )
        .route("/api/creators/:handle/email", patch(update_email))
        .route(
            "/api/creators/:handle/contact",
            patch(update_contact_status),
        )
        .route("/api/seed", post(seed))
        .route(
            "/api/frontier/bucket",
            get(frontier_bucket).post(add_frontier_seed),
        )
        .route("/api/frontier/bucket/:handle", delete(remove_frontier_seed))
        .route("/api/frontier/items", get(frontier_items))
        .route("/api/frontier/run", post(start_frontier_run))
        .route("/api/frontier/run/stop", post(stop_frontier_run))
        .route("/api/frontier/run/status", get(frontier_run_status))
        .route("/api/run", post(start_run))
        .route("/api/run/stop", post(stop_run))
        .route("/api/run/status", get(run_status))
        .with_state(state);

    let addr: SocketAddr = format!("{host}:{port}").parse()?;
    let listener = TcpListener::bind(addr).await?;
    info!(url = %format!("http://{addr}"), "serving crawler dashboard");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn index() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}

async fn app_js() -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )],
        include_str!("../static/app.js"),
    )
}

async fn lucide_js() -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )],
        include_str!("../static/lucide.min.js"),
    )
}

async fn styles_css() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        include_str!("../static/styles.css"),
    )
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({ "ok": true }))
}

async fn queue(
    State(state): State<WebState>,
    Query(query): Query<QueueQuery>,
) -> Result<Json<QueueResponse>, ApiError> {
    let limit = query.limit.unwrap_or(100).clamp(1, 500);
    let offset = query.offset.unwrap_or(0);
    let items = state
        .db
        .list_queue_view(
            query.status.as_deref(),
            query.language.as_deref(),
            query.app.as_deref(),
            limit,
            offset,
        )
        .await?;
    let total = state
        .db
        .count_queue_view(
            query.status.as_deref(),
            query.language.as_deref(),
            query.app.as_deref(),
        )
        .await?;
    let counts = state.db.queue_status_counts().await?;
    Ok(Json(QueueResponse {
        items,
        counts,
        total,
        limit,
        offset,
        has_next: offset.saturating_add(limit) < total,
        has_prev: offset > 0,
    }))
}

async fn queue_sources(
    State(state): State<WebState>,
    Query(query): Query<QueueQuery>,
) -> Result<Json<QueueSourcesResponse>, ApiError> {
    let limit = query.limit.unwrap_or(100).clamp(1, 500);
    let offset = query.offset.unwrap_or(0);
    let items = state
        .db
        .list_queue_source_view(query.status.as_deref(), limit, offset)
        .await?;
    let total = state
        .db
        .count_queue_source_view(query.status.as_deref())
        .await?;
    Ok(Json(QueueSourcesResponse {
        items,
        total,
        limit,
        offset,
        has_next: offset.saturating_add(limit) < total,
        has_prev: offset > 0,
    }))
}

async fn languages(State(state): State<WebState>) -> Result<Json<LanguagesResponse>, ApiError> {
    Ok(Json(LanguagesResponse {
        languages: state.db.list_languages().await?,
    }))
}

async fn countries(State(state): State<WebState>) -> Result<Json<CountriesResponse>, ApiError> {
    Ok(Json(CountriesResponse {
        countries: state.db.list_countries().await?,
    }))
}

async fn queue_parent_countries(
    State(state): State<WebState>,
) -> Result<Json<CountriesResponse>, ApiError> {
    Ok(Json(CountriesResponse {
        countries: state.db.list_queue_parent_countries().await?,
    }))
}

async fn remove_queue_source(
    State(state): State<WebState>,
    Path(handle): Path<String>,
) -> Result<Json<QueueSourceMutationResponse>, ApiError> {
    let removed = state.db.remove_queue_items_from_source(&handle).await?;
    Ok(Json(QueueSourceMutationResponse { ok: true, removed }))
}

async fn apps(State(state): State<WebState>) -> Result<Json<AppsResponse>, ApiError> {
    Ok(Json(AppsResponse {
        apps: state.db.list_app_summaries().await?,
    }))
}

async fn add_app(
    State(state): State<WebState>,
    Json(request): Json<AddAppRequest>,
) -> Result<Json<MutationResponse>, ApiError> {
    let changed = state.db.add_app_name(&request.name).await?;
    Ok(Json(MutationResponse { ok: true, changed }))
}

async fn update_app_policy(
    State(state): State<WebState>,
    Path(name): Path<String>,
    Json(request): Json<AppPolicyRequest>,
) -> Result<Json<MutationResponse>, ApiError> {
    let changed = state.db.set_app_policy(&name, &request.policy).await?;
    Ok(Json(MutationResponse {
        ok: true,
        changed: changed > 0,
    }))
}

async fn creators(
    State(state): State<WebState>,
    Query(query): Query<CreatorQuery>,
) -> Result<Json<CreatorsResponse>, ApiError> {
    let limit = query.limit.unwrap_or(50).clamp(1, 500);
    let offset = query.offset.unwrap_or(0);
    let apps = parse_csv(query.apps.as_deref());
    let languages = parse_csv(query.languages.as_deref());
    let countries = parse_csv(query.countries.as_deref());
    let contact_statuses = parse_csv(query.contact_statuses.as_deref());
    let filters = CreatorListFilters {
        app_mode: query.app_mode.as_deref(),
        app_names: &apps,
        language_codes: &languages,
        country_codes: &countries,
        contact_statuses: &contact_statuses,
        email_filter: query.email.as_deref(),
        min_followers: query.min_followers,
        max_followers: query.max_followers,
        min_following: query.min_following,
        max_following: query.max_following,
        min_median_views: query.min_median_views,
        max_median_views: query.max_median_views,
        min_avg_views: query.min_avg_views,
        max_avg_views: query.max_avg_views,
        limit,
        offset,
    };
    let creators = state
        .db
        .list_creators(
            query.sort.as_deref().unwrap_or("median_views"),
            query.dir.as_deref().unwrap_or("desc"),
            filters,
        )
        .await?;
    let total = state.db.count_creators(filters).await?;
    Ok(Json(CreatorsResponse {
        creators,
        total,
        limit,
        offset,
        has_next: offset.saturating_add(limit) < total,
        has_prev: offset > 0,
    }))
}

async fn update_classification(
    State(state): State<WebState>,
    Path(handle): Path<String>,
    Json(request): Json<ClassificationOverrideRequest>,
) -> Result<Json<MutationResponse>, ApiError> {
    state
        .db
        .set_creator_app(&handle, request.app_name.as_deref())
        .await?;
    Ok(Json(MutationResponse {
        ok: true,
        changed: true,
    }))
}

async fn update_email(
    State(state): State<WebState>,
    Path(handle): Path<String>,
    Json(request): Json<EmailOverrideRequest>,
) -> Result<Json<MutationResponse>, ApiError> {
    state
        .db
        .set_creator_email(&handle, request.email.as_deref())
        .await?;
    Ok(Json(MutationResponse {
        ok: true,
        changed: true,
    }))
}

async fn update_contact_status(
    State(state): State<WebState>,
    Path(handle): Path<String>,
    Json(request): Json<ContactStatusRequest>,
) -> Result<Json<MutationResponse>, ApiError> {
    state
        .db
        .set_creator_contact_status(&handle, &request.status)
        .await?;
    Ok(Json(MutationResponse {
        ok: true,
        changed: true,
    }))
}

async fn seed(
    State(state): State<WebState>,
    Json(request): Json<SeedRequest>,
) -> Result<Json<MutationResponse>, ApiError> {
    let changed = state
        .db
        .enqueue_handle_with_app(&request.handle, None, request.app_name.as_deref())
        .await?;
    Ok(Json(MutationResponse { ok: true, changed }))
}

async fn frontier_bucket(
    State(state): State<WebState>,
) -> Result<Json<FrontierBucketResponse>, ApiError> {
    let items = state.db.list_frontier_bucket(500).await?;
    let total = state.db.count_frontier_bucket().await?;
    Ok(Json(FrontierBucketResponse { items, total }))
}

async fn add_frontier_seed(
    State(state): State<WebState>,
    Json(request): Json<FrontierSeedRequest>,
) -> Result<Json<FrontierMutationResponse>, ApiError> {
    let mut handles = request.handles.unwrap_or_default();
    if let Some(handle) = request.handle {
        handles.push(handle);
    }
    if handles.is_empty() {
        return Err(ApiError::bad_request("at least one handle is required"));
    }

    let mut changed_count = 0;
    for handle in handles {
        if state
            .db
            .add_frontier_seed(&handle, request.source.as_deref())
            .await?
        {
            changed_count += 1;
        }
    }

    Ok(Json(FrontierMutationResponse {
        ok: true,
        changed: changed_count > 0,
        changed_count,
    }))
}

async fn remove_frontier_seed(
    State(state): State<WebState>,
    Path(handle): Path<String>,
) -> Result<Json<MutationResponse>, ApiError> {
    let changed = state.db.remove_frontier_seed(&handle).await?;
    Ok(Json(MutationResponse { ok: true, changed }))
}

async fn frontier_items(
    State(state): State<WebState>,
    Query(query): Query<FrontierItemsQuery>,
) -> Result<Json<FrontierItemsResponse>, ApiError> {
    let limit = query.limit.unwrap_or(100).clamp(1, 500);
    let offset = query.offset.unwrap_or(0);
    let run = if let Some(run_id) = query.run_id {
        state.db.get_frontier_run(run_id).await?
    } else {
        state.db.latest_frontier_run().await?
    };
    let run_id = run.as_ref().map(|run| run.id);
    let items = state
        .db
        .list_frontier_run_items(run_id, limit, offset)
        .await?;
    let total = state.db.count_frontier_run_items(run_id).await?;
    let counts = state.db.frontier_item_status_counts(run_id).await?;
    Ok(Json(FrontierItemsResponse {
        run,
        items,
        counts,
        total,
        limit,
        offset,
        has_next: offset.saturating_add(limit) < total,
        has_prev: offset > 0,
    }))
}

async fn start_run(
    State(state): State<WebState>,
    Json(request): Json<RunRequest>,
) -> Result<Json<RunnerStatus>, ApiError> {
    {
        let frontier_runner = state.frontier_runner.lock().await;
        if frontier_runner.running {
            return Err(ApiError::conflict("frontier crawler is already running"));
        }
    }

    let concurrency = request.concurrency.unwrap_or(10).clamp(1, 25);
    let country_codes = normalize_request_values(request.countries.unwrap_or_default(), true);
    let app_names = normalize_request_values(request.apps.unwrap_or_default(), false);
    let handles = request.handles.unwrap_or_default();
    if !handles.is_empty() {
        state.db.prioritize_handles(&handles).await?;
    }
    let whitelist_only = request
        .whitelist_only
        .unwrap_or(handles.is_empty() && app_names.is_empty());
    let stop_requested = Arc::new(AtomicBool::new(false));

    {
        let mut runner = state.runner.lock().await;
        if runner.running {
            return Err(ApiError::conflict("scraper is already running"));
        }
        runner.running = true;
        runner.stopping = false;
        runner.stop_requested = Some(stop_requested.clone());
        runner.started_at = Some(Utc::now().to_rfc3339());
        runner.finished_at = None;
        runner.country_codes = country_codes.clone();
        runner.app_names = app_names.clone();
        runner.handles = handles.clone();
        runner.whitelist_only = whitelist_only;
        runner.limit = request.limit;
        runner.concurrency = concurrency;
        runner.last_summary = None;
        runner.last_error = None;
    }

    let crawler = state.crawler.clone();
    let runner_state = state.runner.clone();
    tokio::spawn(async move {
        let result = crawler
            .run_queue_with_filters(
                request.limit,
                concurrency,
                false,
                QueueRunFilters {
                    app_names,
                    country_codes,
                    handles,
                    whitelist_only,
                    ..QueueRunFilters::default()
                },
                Some(stop_requested),
            )
            .await;

        let mut runner = runner_state.lock().await;
        runner.running = false;
        runner.stopping = false;
        runner.stop_requested = None;
        runner.finished_at = Some(Utc::now().to_rfc3339());
        match result {
            Ok(summary) => {
                runner.last_summary = Some(summary);
                runner.last_error = None;
            }
            Err(error) => {
                runner.last_summary = None;
                runner.last_error = Some(format!("{error:#}"));
            }
        }
    });

    run_status(State(state)).await
}

async fn start_frontier_run(
    State(state): State<WebState>,
    Json(request): Json<FrontierRunRequest>,
) -> Result<Json<FrontierRunnerStatus>, ApiError> {
    {
        let queue_runner = state.runner.lock().await;
        if queue_runner.running {
            return Err(ApiError::conflict("queue scraper is already running"));
        }
    }
    {
        let frontier_runner = state.frontier_runner.lock().await;
        if frontier_runner.running {
            return Err(ApiError::conflict("frontier crawler is already running"));
        }
    }

    let concurrency = request.concurrency.unwrap_or(10).clamp(1, 25);
    let refresh_seeds = request.refresh_seeds.unwrap_or(true);
    let run = state.db.create_frontier_run_from_bucket(1).await?;
    let stop_requested = Arc::new(AtomicBool::new(false));

    {
        let mut runner = state.frontier_runner.lock().await;
        runner.running = true;
        runner.stopping = false;
        runner.stop_requested = Some(stop_requested.clone());
        runner.started_at = Some(Utc::now().to_rfc3339());
        runner.finished_at = None;
        runner.run_id = Some(run.id);
        runner.limit = request.limit;
        runner.concurrency = concurrency;
        runner.refresh_seeds = refresh_seeds;
        runner.last_summary = None;
        runner.last_error = None;
    }

    let crawler = state.crawler.clone();
    let db = state.db.clone();
    let runner_state = state.frontier_runner.clone();
    let run_id = run.id;
    tokio::spawn(async move {
        let result = crawler
            .run_frontier(
                run_id,
                request.limit,
                concurrency,
                refresh_seeds,
                Some(stop_requested.clone()),
            )
            .await;

        let finish_error = match &result {
            Ok(summary) => {
                let status = if stop_requested.load(Ordering::Relaxed) {
                    "stopped"
                } else {
                    "completed"
                };
                db.finish_frontier_run(
                    run_id,
                    FrontierRunFinish {
                        status,
                        processed: summary.processed,
                        succeeded: summary.succeeded,
                        failed: summary.failed,
                        skipped: summary.skipped,
                        last_error: None,
                    },
                )
                .await
                .err()
            }
            Err(error) => {
                let error_message = format!("{error:#}");
                db.finish_frontier_run(
                    run_id,
                    FrontierRunFinish {
                        status: "failed",
                        processed: 0,
                        succeeded: 0,
                        failed: 0,
                        skipped: 0,
                        last_error: Some(&error_message),
                    },
                )
                .await
                .err()
            }
        };

        let mut runner = runner_state.lock().await;
        runner.running = false;
        runner.stopping = false;
        runner.stop_requested = None;
        runner.finished_at = Some(Utc::now().to_rfc3339());
        match result {
            Ok(summary) => {
                runner.last_summary = Some(summary);
                runner.last_error = finish_error.map(|error| format!("{error:#}"));
            }
            Err(error) => {
                runner.last_summary = None;
                runner.last_error = Some(format!("{error:#}"));
            }
        }
    });

    frontier_run_status(State(state)).await
}

async fn stop_frontier_run(
    State(state): State<WebState>,
) -> Result<Json<FrontierRunnerStatus>, ApiError> {
    {
        let mut runner = state.frontier_runner.lock().await;
        if let Some(stop_requested) = &runner.stop_requested {
            stop_requested.store(true, Ordering::Relaxed);
            runner.stopping = true;
        }
    }
    frontier_run_status(State(state)).await
}

async fn frontier_run_status(
    State(state): State<WebState>,
) -> Result<Json<FrontierRunnerStatus>, ApiError> {
    let snapshot = {
        let runner = state.frontier_runner.lock().await;
        FrontierRunnerStatus {
            running: runner.running,
            stopping: runner.stopping,
            started_at: runner.started_at.clone(),
            finished_at: runner.finished_at.clone(),
            run_id: runner.run_id,
            limit: runner.limit,
            concurrency: runner.concurrency,
            refresh_seeds: runner.refresh_seeds,
            last_summary: runner.last_summary.clone(),
            last_error: runner.last_error.clone(),
            bucket_count: 0,
            latest_run: None,
            item_counts: Vec::new(),
        }
    };
    let mut snapshot = snapshot;
    snapshot.bucket_count = state.db.count_frontier_bucket().await?;
    snapshot.latest_run = if let Some(run_id) = snapshot.run_id {
        state.db.get_frontier_run(run_id).await?
    } else {
        state.db.latest_frontier_run().await?
    };
    snapshot.item_counts = state
        .db
        .frontier_item_status_counts(snapshot.latest_run.as_ref().map(|run| run.id))
        .await?;
    Ok(Json(snapshot))
}

async fn stop_run(State(state): State<WebState>) -> Result<Json<RunnerStatus>, ApiError> {
    {
        let mut runner = state.runner.lock().await;
        if let Some(stop_requested) = &runner.stop_requested {
            stop_requested.store(true, Ordering::Relaxed);
            runner.stopping = true;
        }
    }
    run_status(State(state)).await
}

async fn run_status(State(state): State<WebState>) -> Result<Json<RunnerStatus>, ApiError> {
    let snapshot = {
        let runner = state.runner.lock().await;
        RunnerStatus {
            running: runner.running,
            stopping: runner.stopping,
            started_at: runner.started_at.clone(),
            finished_at: runner.finished_at.clone(),
            country_codes: runner.country_codes.clone(),
            app_names: runner.app_names.clone(),
            handles: runner.handles.clone(),
            whitelist_only: runner.whitelist_only,
            limit: runner.limit,
            concurrency: runner.concurrency,
            last_summary: runner.last_summary.clone(),
            last_error: runner.last_error.clone(),
            queue_counts: Vec::new(),
        }
    };
    let mut snapshot = snapshot;
    snapshot.queue_counts = state.db.queue_status_counts().await?;
    Ok(Json(snapshot))
}

fn parse_csv(value: Option<&str>) -> Vec<String> {
    normalize_request_values(
        value
            .unwrap_or_default()
            .split(',')
            .map(str::to_string)
            .collect(),
        false,
    )
}

fn normalize_request_values(values: Vec<String>, uppercase: bool) -> Vec<String> {
    let mut values = values
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(|value| {
            if uppercase {
                value.to_ascii_uppercase()
            } else {
                value
            }
        })
        .collect::<Vec<_>>();
    values.sort_by_key(|value| value.to_ascii_lowercase());
    values.dedup_by(|left, right| left.eq_ignore_ascii_case(right));
    values
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message: message.into(),
        }
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: format!("{error:#}"),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({
                "ok": false,
                "error": self.message,
            })),
        )
            .into_response()
    }
}
