mod config;
mod crawler;
mod db;
mod handles;
mod models;
mod services;
mod stats;
mod web;

use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use clap::{Parser, Subcommand};
use config::{AppConfig, OpenAiConfig};
use crawler::Crawler;
use db::Database;
use futures::{StreamExt, stream};
use serde::Serialize;
use services::{
    elevenlabs::ElevenLabsService,
    openai::OpenAiService,
    tiktok::{TikTokScraper, scraper_from_config},
};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "creatorprogram-crawler")]
#[command(about = "Crawl creator-program TikTok networks from one seed handle")]
struct Cli {
    #[arg(long, env = "CREATOR_DB_PATH", default_value = "creatorprogram.sqlite")]
    db_path: PathBuf,

    #[arg(long, env = "OPENAI_MODEL")]
    openai_model: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Init,
    Seed {
        handle: String,
        #[arg(long)]
        discovered_from: Option<String>,
    },
    Crawl {
        handle: String,
        #[arg(long)]
        force: bool,
    },
    Run {
        #[arg(long)]
        seed: Option<String>,
        #[arg(long)]
        app: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long, default_value_t = 10)]
        concurrency: usize,
        #[arg(long)]
        watch: bool,
    },
    Queue {
        #[arg(long)]
        status: Option<String>,
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    Serve {
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        #[arg(long, default_value_t = 3000)]
        port: u16,
    },
    ReclassifyLanguages {
        #[arg(long)]
        apply: bool,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long, default_value_t = 8)]
        concurrency: usize,
    },
    Apps {
        #[command(subcommand)]
        command: AppsCommand,
    },
}

#[derive(Debug, Subcommand)]
enum AppsCommand {
    Add { name: String },
    List,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::Init => {
            Database::open(&cli.db_path)?;
            println!("initialized SQLite database at {}", cli.db_path.display());
        }
        Command::Seed {
            handle,
            discovered_from,
        } => {
            let db = Database::open(&cli.db_path)?;
            let inserted = db.seed_handle(&handle, discovered_from.as_deref()).await?;
            if inserted {
                println!("queued {handle}");
            } else {
                println!("{handle} was already queued or scraped");
            }
        }
        Command::Crawl { handle, force } => {
            let crawler = build_crawler(cli.db_path, cli.openai_model)?;
            let result = crawler.crawl_handle(&handle, force).await?;
            if result.skipped_already_scraped {
                println!("@{} was already scraped", result.handle);
            } else {
                println!(
                    "scraped @{}; promoted_app={}; enqueued_following={}",
                    result.handle,
                    result.promoted_app_name.as_deref().unwrap_or("none"),
                    result.enqueued_following_count
                );
            }
        }
        Command::Run {
            seed,
            app,
            limit,
            concurrency,
            watch,
        } => {
            let crawler = build_crawler(cli.db_path.clone(), cli.openai_model)?;
            if let Some(seed) = seed {
                crawler_db(&cli.db_path)?.seed_handle(&seed, None).await?;
            }
            let app = app
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty());
            let summary = if app.is_some() {
                crawler
                    .run_queue_filtered(limit, concurrency, watch, None, app, None)
                    .await?
            } else {
                crawler.run_queue(limit, concurrency, watch).await?
            };
            println!(
                "processed={} succeeded={} skipped={} failed={}",
                summary.processed, summary.succeeded, summary.skipped, summary.failed
            );
        }
        Command::Queue { status, limit } => {
            let db = Database::open(&cli.db_path)?;
            let items = db.list_queue(status.as_deref(), limit).await?;
            println!("{}", serde_json::to_string_pretty(&items)?);
        }
        Command::Serve { host, port } => {
            let (crawler, db) = build_crawler_with_db(cli.db_path, cli.openai_model)?;
            web::serve(db, crawler, host, port).await?;
        }
        Command::ReclassifyLanguages {
            apply,
            limit,
            concurrency,
        } => {
            reclassify_languages(cli.db_path, cli.openai_model, apply, limit, concurrency).await?;
        }
        Command::Apps { command } => {
            let db = Database::open(&cli.db_path)?;
            match command {
                AppsCommand::Add { name } => {
                    let inserted = db.add_app_name(&name).await?;
                    if inserted {
                        println!("added app {name}");
                    } else {
                        println!("app {name} already exists");
                    }
                }
                AppsCommand::List => {
                    for app in db.list_app_names().await? {
                        println!("{app}");
                    }
                }
            }
        }
    }

    Ok(())
}

#[derive(Debug, Serialize)]
struct LanguageReclassificationReport {
    scanned: usize,
    changed: usize,
    applied: bool,
    errors: Vec<LanguageReclassificationError>,
    changes: Vec<LanguageReclassificationChange>,
}

#[derive(Debug, Serialize)]
struct LanguageReclassificationChange {
    handle: String,
    old_language_code: String,
    new_language_code: String,
    confidence: f64,
    evidence: String,
}

#[derive(Debug, Serialize)]
struct LanguageReclassificationError {
    handle: String,
    error: String,
}

async fn reclassify_languages(
    db_path: PathBuf,
    openai_model: Option<String>,
    apply: bool,
    limit: Option<usize>,
    concurrency: usize,
) -> Result<()> {
    let db = Database::open(&db_path)?;
    let openai = OpenAiService::new(OpenAiConfig::from_env(openai_model)?)?;
    let creators = db.list_language_review_creators(limit).await?;
    let scanned = creators.len();
    let concurrency = concurrency.clamp(1, 20);

    let results = stream::iter(creators.into_iter().map(|creator| {
        let openai = openai.clone();
        async move {
            let classification = openai
                .classify_language(&creator.handle, &creator.bio, &creator.latest_content_text)
                .await;
            (creator, classification)
        }
    }))
    .buffer_unordered(concurrency)
    .collect::<Vec<_>>()
    .await;

    let mut changes = Vec::new();
    let mut errors = Vec::new();
    for (creator, classification) in results {
        match classification {
            Ok(classification) => {
                let old_language_code = creator.language_code.trim().to_ascii_uppercase();
                if old_language_code != classification.language_code {
                    changes.push(LanguageReclassificationChange {
                        handle: creator.handle,
                        old_language_code,
                        new_language_code: classification.language_code,
                        confidence: classification.confidence,
                        evidence: classification.evidence,
                    });
                }
            }
            Err(error) => errors.push(LanguageReclassificationError {
                handle: creator.handle,
                error: format!("{error:#}"),
            }),
        }
    }

    changes.sort_by(|left, right| left.handle.cmp(&right.handle));
    errors.sort_by(|left, right| left.handle.cmp(&right.handle));

    if apply {
        for change in &changes {
            db.update_creator_language(&change.handle, &change.new_language_code)
                .await?;
        }
    }

    println!(
        "{}",
        serde_json::to_string_pretty(&LanguageReclassificationReport {
            scanned,
            changed: changes.len(),
            applied: apply,
            errors,
            changes,
        })?
    );

    Ok(())
}

fn build_crawler(db_path: PathBuf, openai_model: Option<String>) -> Result<Crawler> {
    build_crawler_with_db(db_path, openai_model).map(|(crawler, _db)| crawler)
}

fn build_crawler_with_db(
    db_path: PathBuf,
    openai_model: Option<String>,
) -> Result<(Crawler, Database)> {
    let config = AppConfig::from_env(db_path.clone(), openai_model)?;
    let db = Database::open(&config.db_path)?;
    let scraper: Arc<dyn TikTokScraper> = scraper_from_config(config.tiktok)?.into();
    let elevenlabs = ElevenLabsService::new(config.elevenlabs)?;
    let openai = OpenAiService::new(config.openai)?;
    let crawler = Crawler::new(db.clone(), scraper, elevenlabs, openai);
    Ok((crawler, db))
}

fn crawler_db(db_path: &PathBuf) -> Result<Database> {
    Database::open(db_path)
}
