use std::sync::OnceLock;

use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use regex::Regex;
use reqwest::header::{ACCEPT, CONTENT_TYPE, USER_AGENT};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tracing::warn;

use crate::{
    config::OpenAiConfig,
    models::{AppClassification, ExtractedContent, LanguageClassification},
};

const MAX_OPENAI_IMAGE_BYTES: usize = 8 * 1024 * 1024;
const IMAGE_FETCH_USER_AGENT: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36";

#[derive(Clone)]
pub struct OpenAiService {
    client: reqwest::Client,
    config: OpenAiConfig,
}

impl OpenAiService {
    pub fn new(config: OpenAiConfig) -> Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder()
                .user_agent("creatorprogram-crawler/0.1")
                .build()
                .context("failed to build OpenAI HTTP client")?,
            config,
        })
    }

    pub async fn extract_slideshow_text(
        &self,
        slide_image_urls: &[String],
        caption: Option<&str>,
    ) -> Result<ExtractedContent> {
        let mut content = vec![json!({
            "type": "input_text",
            "text": format!(
                "Extract all visible text and summarize important non-text visual context from this TikTok slideshow. Caption: {}",
                caption.unwrap_or("")
            )
        })];

        content.extend(
            self.image_inputs(slide_image_urls, self.config.max_slides, "auto")
                .await,
        );

        let request = json!({
            "model": self.config.model,
            "input": [
                {
                    "role": "system",
                    "content": "You extract creator-video meaning. Return compact text that preserves app names, calls to action, promo language, affiliate language, and the language code of the content."
                },
                {
                    "role": "user",
                    "content": content
                }
            ],
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "slideshow_content",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "text": { "type": "string" },
                            "language_code": { "type": "string" }
                        },
                        "required": ["text", "language_code"],
                        "additionalProperties": false
                    },
                    "strict": true
                }
            }
        });

        let content: SlideshowContent = self.responses_json(request).await?;
        Ok(ExtractedContent {
            text: content.text,
            language_code: content.language_code,
        }
        .normalized())
    }

    pub async fn classify_ambassador(
        &self,
        handle: &str,
        display_name: Option<&str>,
        bio: &str,
        latest_content: &str,
        visual_image_urls: &[String],
        known_apps: &[String],
    ) -> Result<AppClassification> {
        let image_inputs = self.image_inputs(visual_image_urls, 9, "high").await;
        let mut user_content =
            self.classification_user_content(handle, display_name, bio, latest_content, known_apps);
        user_content.extend(image_inputs.clone());

        let request = self.classification_request(user_content);
        let classification = match self.responses_json(request).await {
            Ok(classification) => classification,
            Err(error) if !image_inputs.is_empty() => {
                warn!(
                    error = %format!("{error:#}"),
                    "OpenAI classification failed with visual images; retrying without visuals"
                );
                let request = self.classification_request(self.classification_user_content(
                    handle,
                    display_name,
                    bio,
                    latest_content,
                    known_apps,
                ));
                self.responses_json(request).await?
            }
            Err(error) => return Err(error),
        };

        Ok(apply_bio_ambassador_fallback(
            canonicalize_classification(classification, known_apps),
            bio,
            latest_content,
            known_apps,
        ))
    }

    pub async fn classify_language(
        &self,
        handle: &str,
        bio: &str,
        latest_content: &str,
    ) -> Result<LanguageClassification> {
        let request = json!({
            "model": self.config.model,
            "input": [
                {
                    "role": "system",
                    "content": "You classify the dominant natural language of TikTok creator content for a crawler. Return an ISO 639-3 uppercase language code. Use UNKNOWN only when there is not enough language evidence."
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": json!({
                                "handle": handle,
                                "creator_bio": bio,
                                "saved_posts_text": latest_content,
                                "decision_rules": [
                                    "Use the dominant language used by the creator across bio, captions, and transcripts.",
                                    "Captions and bio can outweigh a single bad speech-to-text line, background music, lyrics, app UI labels, or English hook words.",
                                    "If several languages are truly central, choose the one most useful for filtering this creator audience.",
                                    "Return ISO 639-3 uppercase codes, for example ENG, SPA, DEU, FRA, HRV, POL, POR, ITA, JPN, NLD, RON, CES, SLK."
                                ]
                            }).to_string()
                        }
                    ]
                }
            ],
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "language_classification",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "language_code": { "type": "string" },
                            "confidence": { "type": "number" },
                            "evidence": { "type": "string" }
                        },
                        "required": ["language_code", "confidence", "evidence"],
                        "additionalProperties": false
                    },
                    "strict": true
                }
            }
        });

        let classification: LanguageClassification = self.responses_json(request).await?;
        Ok(classification.normalized())
    }

    fn classification_user_content(
        &self,
        handle: &str,
        display_name: Option<&str>,
        bio: &str,
        latest_content: &str,
        known_apps: &[String],
    ) -> Vec<Value> {
        vec![json!({
            "type": "input_text",
            "text": json!({
                "known_app_names": known_apps,
                "creator_handle": handle,
                "creator_display_name": display_name.unwrap_or("unknown"),
                "creator_bio": bio,
                "classification_posts_text": latest_content,
                "visual_context": "The attached images are screenshots/cover frames from the TikTok post being classified. Use them to identify app UIs, logos, app names, app screens, and visible promotional context.",
                "decision_rules": [
                    "The classification_posts_text contains distinct sections labeled Post 1, Post 2, and Post 3. Count support per post section, not per repeated mention.",
                    "Return promotes_app=true only when the same app is concretely promoted, shown, or discussed in at least 2 distinct post sections among the analyzed recent non-pinned videos.",
                    "Set supporting_post_count to the number of distinct post sections that support the returned app. If supporting_post_count is less than 2, promotes_app must be false and app_name/existing_app_name must be null.",
                    "Bio evidence can help identify or corroborate the app name, including roles like '<App> ambassador' or 'ambassador for <App>', but the bio does not count as one of the 2 required supporting post sections.",
                    "Visible app UI or logo plus spoken/caption context about using it can count for that post section.",
                    "If the app is in known_app_names, return the canonical known name in existing_app_name.",
                    "If the app is clearly promoted but absent from known_app_names, return app_name and is_new_app=true.",
                    "If there is no promoted app, app_name and existing_app_name must be null.",
                    "Return creator_name as the creator's real or preferred first name when it is reasonably supported by the profile display name, bio, or content. Do not return the TikTok handle as a name. Return the literal string 'unknown' when no name is supported.",
                    "Return creator_email as the complete email address visible in the bio or analyzed content. Correct harmless spacing introduced by formatting, but never invent an address. Return the literal string 'unknown' when no email is present.",
                    "Set language_code to the dominant natural language used by the creator in the bio, captions, and transcripts. Use ISO 639-3 uppercase codes such as ENG, SPA, DEU, FRA, HRV, POL. Ignore isolated audio glitches, lyrics, UI labels, and mistranscribed one-off phrases when captions or other posts clearly show another language."
                ]
            }).to_string()
        })]
    }

    fn classification_request(&self, user_content: Vec<Value>) -> Value {
        json!({
            "model": self.config.model,
            "input": [
                {
                    "role": "system",
                    "content": "You classify TikTok creators for a creator-program crawler. Decide whether the creator repeatedly promotes the same app across recent videos. App promotion can appear in transcripts, captions, or visible screenshots. Be conservative: require the same app in at least two distinct post sections before returning a promoted app."
                },
                {
                    "role": "user",
                    "content": user_content
                }
            ],
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "app_ambassador_classification",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "promotes_app": { "type": "boolean" },
                            "app_name": { "type": ["string", "null"] },
                            "is_existing_app": { "type": "boolean" },
                            "existing_app_name": { "type": ["string", "null"] },
                            "is_new_app": { "type": "boolean" },
                            "supporting_post_count": { "type": "integer", "minimum": 0, "maximum": 3 },
                            "language_code": { "type": "string" },
                            "creator_name": { "type": "string" },
                            "creator_email": { "type": "string" },
                            "confidence": { "type": "number" },
                            "evidence": { "type": "string" }
                        },
                        "required": [
                            "promotes_app",
                            "app_name",
                            "is_existing_app",
                            "existing_app_name",
                            "is_new_app",
                            "supporting_post_count",
                            "language_code",
                            "creator_name",
                            "creator_email",
                            "confidence",
                            "evidence"
                        ],
                        "additionalProperties": false
                    },
                    "strict": true
                }
            }
        })
    }

    async fn image_inputs(&self, urls: &[String], limit: usize, detail: &str) -> Vec<Value> {
        let mut inputs = Vec::new();
        for url in urls.iter().take(limit) {
            match self.fetch_image_data_url(url).await {
                Ok(data_url) => inputs.push(json!({
                    "type": "input_image",
                    "image_url": data_url,
                    "detail": detail
                })),
                Err(error) => warn!(
                    image_url = %url,
                    error = %format!("{error:#}"),
                    "skipping visual image that could not be fetched locally"
                ),
            }
        }
        inputs
    }

    async fn fetch_image_data_url(&self, url: &str) -> Result<String> {
        let response = self
            .client
            .get(url)
            .header(
                ACCEPT,
                "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
            )
            .header(USER_AGENT, IMAGE_FETCH_USER_AGENT)
            .send()
            .await
            .with_context(|| format!("failed to fetch visual image from {url}"))?;
        let status = response.status();
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .map(str::trim)
            .filter(|value| value.starts_with("image/"))
            .unwrap_or("image/jpeg")
            .to_string();
        let bytes = response
            .bytes()
            .await
            .context("failed to read visual image bytes")?;

        if !status.is_success() {
            anyhow::bail!(
                "visual image fetch returned {status} with {} bytes",
                bytes.len()
            );
        }
        if bytes.is_empty() {
            anyhow::bail!("visual image fetch returned an empty body");
        }
        if bytes.len() > MAX_OPENAI_IMAGE_BYTES {
            anyhow::bail!(
                "visual image fetch returned {} bytes, above the {} byte cap",
                bytes.len(),
                MAX_OPENAI_IMAGE_BYTES
            );
        }

        Ok(format!(
            "data:{content_type};base64,{}",
            BASE64_STANDARD.encode(bytes)
        ))
    }

    async fn responses_json<T>(&self, body: Value) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let endpoint = format!("{}/responses", self.config.base_url.trim_end_matches('/'));
        let response = self
            .client
            .post(endpoint)
            .bearer_auth(&self.config.api_key)
            .json(&body)
            .send()
            .await
            .context("failed to call OpenAI Responses API")?;

        let status = response.status();
        let body_text = response
            .text()
            .await
            .context("failed to read OpenAI response body")?;
        if !status.is_success() {
            anyhow::bail!("OpenAI Responses API returned {status}: {body_text}");
        }

        let value = serde_json::from_str::<Value>(&body_text)
            .context("OpenAI Responses API returned invalid JSON")?;
        let output_text = extract_output_text(&value).with_context(|| {
            format!(
                "OpenAI response did not contain output text: {}",
                truncate(&body_text, 600)
            )
        })?;
        serde_json::from_str::<T>(&output_text).with_context(|| {
            format!(
                "failed to parse OpenAI structured output as JSON: {}",
                truncate(&output_text, 600)
            )
        })
    }
}

#[derive(Debug, Deserialize)]
struct SlideshowContent {
    text: String,
    language_code: String,
}

fn canonicalize_classification(
    mut classification: AppClassification,
    known_apps: &[String],
) -> AppClassification {
    classification.confidence = classification.confidence.clamp(0.0, 1.0);
    classification.supporting_post_count = classification.supporting_post_count.min(3);
    classification.language_code = normalize_language_code(&classification.language_code);
    classification.creator_name = normalize_optional_model_value(classification.creator_name);
    classification.creator_email = normalize_optional_email(classification.creator_email);

    if !classification.promotes_app {
        return AppClassification {
            evidence: classification.evidence,
            language_code: classification.language_code,
            creator_name: classification.creator_name,
            creator_email: classification.creator_email,
            ..AppClassification::no_app()
        };
    }

    if classification.supporting_post_count < 2 {
        return AppClassification {
            evidence: format!(
                "{} Same app was supported by only {} analyzed post section(s); minimum is 2.",
                classification.evidence.trim(),
                classification.supporting_post_count
            )
            .trim()
            .to_string(),
            language_code: classification.language_code,
            creator_name: classification.creator_name,
            creator_email: classification.creator_email,
            ..AppClassification::no_app()
        };
    }

    let app_candidate = classification
        .existing_app_name
        .as_ref()
        .or(classification.app_name.as_ref())
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty());

    let Some(app_candidate) = app_candidate else {
        return AppClassification {
            promotes_app: false,
            evidence: classification.evidence,
            language_code: classification.language_code,
            creator_name: classification.creator_name,
            creator_email: classification.creator_email,
            ..AppClassification::no_app()
        };
    };

    if let Some(canonical) = known_apps
        .iter()
        .find(|known| known.eq_ignore_ascii_case(&app_candidate))
        .cloned()
    {
        classification.app_name = Some(canonical.clone());
        classification.existing_app_name = Some(canonical);
        classification.is_existing_app = true;
        classification.is_new_app = false;
    } else {
        classification.app_name = Some(app_candidate);
        classification.existing_app_name = None;
        classification.is_existing_app = false;
        classification.is_new_app = true;
    }

    classification
}

fn apply_bio_ambassador_fallback(
    classification: AppClassification,
    bio: &str,
    latest_content: &str,
    known_apps: &[String],
) -> AppClassification {
    if classification.promotes_app {
        return classification;
    }

    let Some(app_candidate) = bio_ambassador_app_candidate(bio) else {
        return classification;
    };
    let supporting_post_count =
        count_post_sections_containing_app(latest_content, &app_candidate).min(3) as u8;
    if supporting_post_count < 2 {
        return classification;
    }

    let canonical_app = known_apps
        .iter()
        .find(|known| known.eq_ignore_ascii_case(&app_candidate))
        .cloned();
    let app_name = canonical_app.unwrap_or(app_candidate.clone());
    let evidence = if classification.evidence.trim().is_empty() {
        format!("Bio explicitly says '{app_candidate} ambassador'.")
    } else {
        format!(
            "{} Bio explicitly says '{app_candidate} ambassador'.",
            classification.evidence.trim()
        )
    };
    let is_existing_app = known_apps
        .iter()
        .any(|known| known.eq_ignore_ascii_case(&app_name));

    AppClassification {
        promotes_app: true,
        app_name: Some(app_name.clone()),
        is_existing_app,
        existing_app_name: is_existing_app.then_some(app_name),
        is_new_app: !is_existing_app,
        supporting_post_count,
        language_code: classification.language_code,
        creator_name: classification.creator_name,
        creator_email: classification.creator_email,
        confidence: classification.confidence.max(0.9),
        evidence,
    }
}

fn normalize_optional_model_value(value: Option<String>) -> Option<String> {
    value.map(|value| value.trim().to_string()).filter(|value| {
        !value.is_empty()
            && !value.eq_ignore_ascii_case("unknown")
            && !value.eq_ignore_ascii_case("n/a")
            && !value.eq_ignore_ascii_case("none")
            && !value.eq_ignore_ascii_case("null")
    })
}

fn normalize_optional_email(value: Option<String>) -> Option<String> {
    normalize_optional_model_value(value)
        .map(|value| value.replace(' ', "").to_ascii_lowercase())
        .filter(|value| value.contains('@') && value.rsplit_once('.').is_some())
}

fn count_post_sections_containing_app(latest_content: &str, app_name: &str) -> usize {
    let normalized_app = normalize_detection_text(app_name);
    if normalized_app.is_empty() {
        return 0;
    }

    latest_content
        .split("\n\n---\n\n")
        .filter(|section| normalize_detection_text(section).contains(&normalized_app))
        .count()
}

fn normalize_detection_text(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || ch.is_ascii_whitespace())
        .collect::<String>()
        .to_ascii_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn bio_ambassador_app_candidate(bio: &str) -> Option<String> {
    bio.lines()
        .filter(|line| contains_ambassador_signal(line))
        .find_map(|line| {
            ambassador_app_after_role(line)
                .or_else(|| ambassador_app_before_role(line))
                .and_then(clean_ambassador_app_candidate)
        })
}

fn contains_ambassador_signal(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    [
        "ambassador",
        "affiliate",
        "partner",
        "promoter",
        "promo code",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn ambassador_app_before_role(line: &str) -> Option<String> {
    static BEFORE_ROLE_RE: OnceLock<Regex> = OnceLock::new();
    let regex = BEFORE_ROLE_RE.get_or_init(|| {
        Regex::new(
            r"(?i)(?P<app>[A-Z0-9][A-Z0-9 &'._+-]{1,80}?)\s+(?:app\s+)?(?:ambassador|affiliate|partner|promoter)\b",
        )
        .expect("bio ambassador before-role regex should compile")
    });
    regex
        .captures(line)
        .and_then(|captures| captures.name("app"))
        .map(|app| app.as_str().to_string())
}

fn ambassador_app_after_role(line: &str) -> Option<String> {
    static AFTER_ROLE_RE: OnceLock<Regex> = OnceLock::new();
    let regex = AFTER_ROLE_RE.get_or_init(|| {
        Regex::new(
            r"(?i)(?:ambassador|affiliate|partner|promoter)\s+(?:for|of|at|with)\s+(?P<app>[A-Z0-9][A-Z0-9 &'._+-]{1,80})",
        )
        .expect("bio ambassador after-role regex should compile")
    });
    regex
        .captures(line)
        .and_then(|captures| captures.name("app"))
        .map(|app| app.as_str().to_string())
}

fn clean_ambassador_app_candidate(candidate: String) -> Option<String> {
    let candidate = candidate
        .trim_matches(|ch: char| {
            ch.is_whitespace()
                || matches!(
                    ch,
                    ':' | ';' | ',' | '.' | '-' | '|' | '(' | ')' | '[' | ']'
                )
        })
        .split("  ")
        .next()
        .unwrap_or_default()
        .trim()
        .to_string();
    if candidate.is_empty() || is_generic_ambassador_candidate(&candidate) {
        None
    } else {
        Some(candidate)
    }
}

fn is_generic_ambassador_candidate(candidate: &str) -> bool {
    matches!(
        candidate.trim().to_ascii_lowercase().as_str(),
        "app" | "brand" | "campus" | "student" | "ugc" | "tiktok" | "main"
    )
}

fn normalize_language_code(language_code: &str) -> String {
    let normalized = language_code.trim().to_ascii_uppercase();
    if normalized.is_empty() {
        "UNKNOWN".to_string()
    } else {
        normalized
    }
}

fn extract_output_text(value: &Value) -> Option<String> {
    if let Some(output_text) = value.get("output_text").and_then(Value::as_str) {
        return Some(output_text.to_string());
    }

    let output = value.get("output")?.as_array()?;
    for item in output {
        let Some(content) = item.get("content").and_then(Value::as_array) else {
            continue;
        };
        for part in content {
            if part.get("type").and_then(Value::as_str) == Some("output_text")
                && let Some(text) = part.get("text").and_then(Value::as_str)
            {
                return Some(text.to_string());
            }
        }
    }

    None
}

fn truncate(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }
    value.chars().take(max_chars).collect::<String>() + "..."
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extracts_output_text_from_responses_shape() {
        let response = json!({
            "output": [{
                "type": "message",
                "content": [{
                    "type": "output_text",
                    "text": "{\"ok\":true}"
                }]
            }]
        });

        assert_eq!(
            extract_output_text(&response),
            Some("{\"ok\":true}".to_string())
        );
    }

    #[test]
    fn canonicalizes_known_app_names() {
        let classification = canonicalize_classification(
            AppClassification {
                promotes_app: true,
                app_name: Some("duolingo".to_string()),
                is_existing_app: false,
                existing_app_name: None,
                is_new_app: true,
                supporting_post_count: 2,
                language_code: "eng".to_string(),
                creator_name: Some("unknown".to_string()),
                creator_email: Some("unknown".to_string()),
                confidence: 1.3,
                evidence: "bio says ambassador".to_string(),
            },
            &["Duolingo".to_string()],
        );

        assert_eq!(classification.app_name, Some("Duolingo".to_string()));
        assert!(classification.is_existing_app);
        assert!(!classification.is_new_app);
        assert_eq!(classification.confidence, 1.0);
        assert_eq!(classification.language_code, "ENG");
    }

    #[test]
    fn rejects_app_classification_with_only_one_supporting_post() {
        let classification = canonicalize_classification(
            AppClassification {
                promotes_app: true,
                app_name: Some("Duolingo".to_string()),
                is_existing_app: true,
                existing_app_name: Some("Duolingo".to_string()),
                is_new_app: false,
                supporting_post_count: 1,
                language_code: "ENG".to_string(),
                creator_name: Some("Jamie".to_string()),
                creator_email: Some("jamie@example.com".to_string()),
                confidence: 0.9,
                evidence: "Only Post 1 shows the app.".to_string(),
            },
            &["Duolingo".to_string()],
        );

        assert!(!classification.promotes_app);
        assert_eq!(classification.app_name, None);
        assert_eq!(classification.supporting_post_count, 0);
        assert_eq!(classification.creator_name.as_deref(), Some("Jamie"));
        assert_eq!(
            classification.creator_email.as_deref(),
            Some("jamie@example.com")
        );
        assert!(classification.evidence.contains("minimum is 2"));
    }

    #[test]
    fn extracts_app_ambassador_signal_from_bio() {
        assert_eq!(
            bio_ambassador_app_candidate("The ick ambassador\nMain acc: @Amina"),
            Some("The ick".to_string())
        );
        assert_eq!(
            bio_ambassador_app_candidate("Ambassador for Pingo AI"),
            Some("Pingo AI".to_string())
        );
        assert_eq!(bio_ambassador_app_candidate("brand ambassador"), None);
    }

    #[test]
    fn bio_ambassador_fallback_requires_two_supporting_posts() {
        let classification = apply_bio_ambassador_fallback(
            AppClassification {
                promotes_app: false,
                app_name: None,
                is_existing_app: false,
                existing_app_name: None,
                is_new_app: false,
                supporting_post_count: 0,
                language_code: "ENG".to_string(),
                creator_name: None,
                creator_email: None,
                confidence: 0.2,
                evidence: "Videos were inconclusive.".to_string(),
            },
            "The ick ambassador",
            "Post 1 of 3\nCaption: The Ick changed my routine\n\n---\n\nPost 2 of 3\nCaption: Still using The Ick today",
            &["The Ick".to_string()],
        );

        assert!(classification.promotes_app);
        assert_eq!(classification.app_name, Some("The Ick".to_string()));
        assert_eq!(
            classification.existing_app_name,
            Some("The Ick".to_string())
        );
        assert!(classification.is_existing_app);
        assert!(!classification.is_new_app);
        assert_eq!(classification.supporting_post_count, 2);
        assert_eq!(classification.language_code, "ENG");
    }

    #[test]
    fn bio_ambassador_fallback_does_not_promote_without_video_support() {
        let classification = apply_bio_ambassador_fallback(
            AppClassification {
                promotes_app: false,
                app_name: None,
                is_existing_app: false,
                existing_app_name: None,
                is_new_app: false,
                supporting_post_count: 0,
                language_code: "ENG".to_string(),
                creator_name: None,
                creator_email: None,
                confidence: 0.2,
                evidence: "Videos were inconclusive.".to_string(),
            },
            "The ick ambassador",
            "Post 1 of 3\nCaption: daily vlog",
            &["The Ick".to_string()],
        );

        assert!(!classification.promotes_app);
        assert_eq!(classification.app_name, None);
    }
}
