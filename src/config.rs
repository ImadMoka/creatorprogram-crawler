use std::{env, path::PathBuf, time::Duration};

use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub db_path: PathBuf,
    pub openai: OpenAiConfig,
    pub elevenlabs: ElevenLabsConfig,
    pub tiktok: TikTokScraperConfig,
}

#[derive(Debug, Clone)]
pub struct OpenAiConfig {
    pub api_key: String,
    pub model: String,
    pub base_url: String,
    pub max_slides: usize,
}

#[derive(Debug, Clone)]
pub struct ElevenLabsConfig {
    pub api_key: String,
    pub model_id: String,
    pub base_url: String,
}

#[derive(Debug, Clone)]
pub struct TikTokScraperConfig {
    pub provider: TikTokScraperProvider,
    pub timeout: Duration,
}

#[derive(Debug, Clone)]
pub enum TikTokScraperProvider {
    SocialProfileScraper(SocialProfileScraperConfig),
    Fixture(PathBuf),
}

#[derive(Debug, Clone)]
pub struct SocialProfileScraperConfig {
    pub base_url: String,
    pub region: Option<String>,
    pub max_following_pages: usize,
}

impl AppConfig {
    pub fn from_env(db_path: PathBuf, openai_model_override: Option<String>) -> Result<Self> {
        Ok(Self {
            db_path,
            openai: OpenAiConfig::from_env(openai_model_override)?,
            elevenlabs: ElevenLabsConfig::from_env()?,
            tiktok: TikTokScraperConfig::from_env()?,
        })
    }
}

impl OpenAiConfig {
    pub fn from_env(model_override: Option<String>) -> Result<Self> {
        Ok(Self {
            api_key: required_env("OPENAI_API_KEY")?,
            model: model_override
                .filter(|model| !model.trim().is_empty())
                .or_else(|| env::var("OPENAI_MODEL").ok())
                .unwrap_or_else(|| "gpt-5.6".to_string()),
            base_url: env::var("OPENAI_BASE_URL")
                .unwrap_or_else(|_| "https://api.openai.com/v1".to_string()),
            max_slides: env::var("OPENAI_MAX_SLIDES")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(20),
        })
    }
}

impl ElevenLabsConfig {
    fn from_env() -> Result<Self> {
        Ok(Self {
            api_key: required_env("ELEVENLABS_API_KEY")?,
            model_id: env::var("ELEVENLABS_STT_MODEL").unwrap_or_else(|_| "scribe_v2".to_string()),
            base_url: env::var("ELEVENLABS_BASE_URL")
                .unwrap_or_else(|_| "https://api.elevenlabs.io".to_string()),
        })
    }
}

impl TikTokScraperConfig {
    fn from_env() -> Result<Self> {
        let timeout = env::var("TIKTOK_SCRAPER_TIMEOUT_SECONDS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(60));

        if let Ok(path) = env::var("TIKTOK_SCRAPER_FIXTURE_PATH") {
            return Ok(Self {
                provider: TikTokScraperProvider::Fixture(PathBuf::from(path)),
                timeout,
            });
        }

        let base_url = optional_env("TIKTOK_SCRAPER_BASE_URL")
            .or_else(|| optional_env("SOCIAL_PROFILE_SCRAPER_BASE_URL"))
            .context("missing required environment variable TIKTOK_SCRAPER_BASE_URL")?;
        let max_following_pages = env::var("TIKTOK_SCRAPER_MAX_FOLLOWING_PAGES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(10)
            .max(1);

        Ok(Self {
            provider: TikTokScraperProvider::SocialProfileScraper(SocialProfileScraperConfig {
                base_url,
                region: optional_env("TIKTOK_SCRAPER_REGION"),
                max_following_pages,
            }),
            timeout,
        })
    }
}

fn optional_env(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn required_env(name: &str) -> Result<String> {
    env::var(name)
        .with_context(|| format!("missing required environment variable {name}"))
        .map(|value| value.trim().to_string())
        .and_then(|value| {
            if value.is_empty() {
                anyhow::bail!("environment variable {name} is empty");
            }
            Ok(value)
        })
}
