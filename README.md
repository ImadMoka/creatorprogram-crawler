# Creator Program Crawler

A local Rust crawler that starts from one TikTok creator handle, fetches their account data, classifies whether they promote an app, and expands into the accounts they follow only when they look like a creator-program ambassador.

## Why SQLite, Not JSON?

JSON is fine for a prototype dump, but it gets painful as soon as 10 workers are claiming handles, deduplicating follows, retrying failures, and checking whether a creator was already scraped. This project uses one local SQLite file instead:

- Still self-contained as a local file managed by the binary.
- Schema is created automatically by the binary.
- SQLite files and sidecars are ignored by Git. Share the database separately when another operator needs the current crawl state.
- Unique constraints handle queue/app/creator de-dupe safely.

## Pipeline

For each creator handle:

1. Fetch creator profile data from the configured TikTok scraper:
   - bio
   - latest 30 videos/photos
   - followed accounts
2. Calculate average views, median views, and most viral post.
3. Extract latest post content:
   - video: ElevenLabs Speech-to-Text via `source_url`
   - photo/slideshow: OpenAI multimodal extraction over slide image URLs
4. Classify app ambassador status and extract contact name/email with OpenAI structured output.
5. Store the creator, videos, follows, app, contact data, profile country, transcript/content, language, and scrape timestamp in SQLite.
6. If the creator promotes an app, normalize followed handles and enqueue only unseen accounts.

## Setup

```bash
cp .env.example .env
```

Fill in:

- `OPENAI_API_KEY`
- `ELEVENLABS_API_KEY`
- `TIKTOK_SCRAPER_BASE_URL`

The TikTok adapter uses the self-hosted Social Profile Scraper Worker:

- `GET /v1/tiktok/profile?handle=<handle>` to fetch public profile data.
- `GET /v1/tiktok/profile/videos?handle=<handle>&trim=true&count=30` to fetch the latest posts.
- `GET /v1/tiktok/user/following?handle=<handle>&trim=true` with `min_time` pagination to fetch followed accounts.

`TIKTOK_SCRAPER_MAX_FOLLOWING_PAGES` defaults to `10`. Increase it if you want a more complete graph and the Worker stays healthy under the extra requests.

Fixture mode still accepts either a canonical local shape or a scraper-like shape with `profile`, `posts`, and `following_pages`.

## Commands

```bash
cargo run -- init
cargo run -- apps add "Example App"
cargo run -- seed @initialcreator
cargo run -- run --concurrency 10 --limit 100
```

Useful inspection commands:

```bash
cargo run -- queue --status pending --limit 25
cargo run -- apps list
cargo run -- crawl @onecreator --force
```

## Dashboard

```bash
cargo run -- serve --port 3000
```

Open `http://127.0.0.1:3000/`. The dashboard has separate Review, Scraper, and Contact Queue workspaces. App policies are persistent: whitelisted app branches are preferred, neutral branches follow, and blacklisted branches are excluded unless a handle is explicitly selected in a manual batch.

## Local Fixture Mode

For development without calling a scraper provider:

```bash
TIKTOK_SCRAPER_FIXTURE_PATH=fixtures/tiktok cargo run -- crawl @creator
```

The fixture path can be a single JSON file or a directory containing `<handle>.json`.

## Design Challenges

- TikTok scraping should stay behind a provider adapter. Raw browser scraping is brittle and more likely to break or violate platform constraints.
- The crawler should expand only from creators classified as promoting an app. Following graphs are noisy, and unrestricted expansion gets expensive quickly.
- Concurrency should start around 10, but provider rate limits should decide the real number.
- App classification should be conservative. False positives poison the queue by expanding from non-program creators.
