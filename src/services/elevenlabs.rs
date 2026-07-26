use anyhow::{Context, Result};
use reqwest::{
    header::{ACCEPT, CONTENT_TYPE, REFERER},
    multipart,
};
use serde::Deserialize;

use crate::{config::ElevenLabsConfig, models::ExtractedContent};

#[derive(Clone)]
pub struct ElevenLabsService {
    client: reqwest::Client,
    config: ElevenLabsConfig,
}

impl ElevenLabsService {
    pub fn new(config: ElevenLabsConfig) -> Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder()
                .user_agent("creatorprogram-crawler/0.1")
                .build()
                .context("failed to build ElevenLabs HTTP client")?,
            config,
        })
    }

    pub async fn transcribe_source_url(&self, source_url: &str) -> Result<ExtractedContent> {
        match self.transcribe_remote_source_url(source_url).await {
            Ok(content) => Ok(content),
            Err(source_error) => {
                let downloaded = self
                    .download_media(source_url)
                    .await
                    .with_context(|| format!("{source_error:#}; fallback media download failed"))?;
                self.transcribe_uploaded_media(downloaded)
                    .await
                    .with_context(|| format!("{source_error:#}; fallback upload failed"))
            }
        }
    }

    async fn transcribe_remote_source_url(&self, source_url: &str) -> Result<ExtractedContent> {
        let endpoint = format!(
            "{}/v1/speech-to-text",
            self.config.base_url.trim_end_matches('/')
        );
        let form = multipart::Form::new()
            .text("model_id", self.config.model_id.clone())
            .text("source_url", source_url.to_string())
            .text("diarize", "false")
            .text("tag_audio_events", "false");

        self.transcribe_form(endpoint, form).await
    }

    async fn transcribe_uploaded_media(&self, media: DownloadedMedia) -> Result<ExtractedContent> {
        let endpoint = format!(
            "{}/v1/speech-to-text",
            self.config.base_url.trim_end_matches('/')
        );
        let mut part = multipart::Part::bytes(media.bytes).file_name("tiktok-media.mp4");
        if let Some(content_type) = media.content_type {
            part = part
                .mime_str(&content_type)
                .with_context(|| format!("invalid downloaded media content type {content_type}"))?;
        }
        let form = multipart::Form::new()
            .text("model_id", self.config.model_id.clone())
            .part("file", part)
            .text("diarize", "false")
            .text("tag_audio_events", "false");

        self.transcribe_form(endpoint, form).await
    }

    async fn transcribe_form(
        &self,
        endpoint: String,
        form: multipart::Form,
    ) -> Result<ExtractedContent> {
        let response = self
            .client
            .post(endpoint)
            .header("xi-api-key", &self.config.api_key)
            .multipart(form)
            .send()
            .await
            .context("failed to call ElevenLabs speech-to-text")?;
        let status = response.status();
        let body = response
            .text()
            .await
            .context("failed to read ElevenLabs response body")?;
        if !status.is_success() {
            anyhow::bail!("ElevenLabs speech-to-text returned {status}: {body}");
        }

        let response = serde_json::from_str::<ElevenLabsTranscriptResponse>(&body)
            .context("ElevenLabs speech-to-text returned invalid JSON")?;

        Ok(ExtractedContent {
            text: response.text,
            language_code: response
                .language_code
                .unwrap_or_else(|| "UNKNOWN".to_string()),
        }
        .normalized())
    }

    async fn download_media(&self, source_url: &str) -> Result<DownloadedMedia> {
        let response = self
            .client
            .get(source_url)
            .header(ACCEPT, "video/*,audio/*,*/*")
            .header(REFERER, "https://www.tiktok.com/")
            .send()
            .await
            .with_context(|| format!("failed to download media from {source_url}"))?;
        let status = response.status();
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let bytes = response
            .bytes()
            .await
            .context("failed to read downloaded media bytes")?;

        if !status.is_success() {
            anyhow::bail!(
                "media download returned {status} with {} bytes",
                bytes.len()
            );
        }
        if let Some(content_type) = &content_type
            && content_type.starts_with("text/")
        {
            anyhow::bail!("media download returned {content_type}, not audio/video");
        }
        if bytes.is_empty() {
            anyhow::bail!("downloaded media was empty");
        }

        Ok(DownloadedMedia {
            bytes: bytes.to_vec(),
            content_type,
        })
    }
}

#[derive(Debug, Deserialize)]
struct ElevenLabsTranscriptResponse {
    text: String,
    language_code: Option<String>,
}

struct DownloadedMedia {
    bytes: Vec<u8>,
    content_type: Option<String>,
}
