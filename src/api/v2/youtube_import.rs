//! YouTube link import (spec 007).
//!
//! Resolves pasted YouTube links — channel URLs, @handles, bare handles,
//! and video links — into `DraftChannel`s the app can create in one go.
//! Server-side because YouTube's RSS feeds now 404 (deprecated) and HTML
//! scraping needs a browser UA; one HTTP round-trip batch instead of one
//! per link client-side.
//!
//! Resolution chain per link:
//!   1. `/channel/UC…` → channel page HTML
//!   2. `@handle`      → channel page HTML → `externalId` → (1) is implicit
//!   3. `watch?v=` / `youtu.be` → oEmbed → author_url → (2)
//!   4. playlist (`list=`) → resolved **with the user's own YouTube
//!      authorization**: the backend looks up the Google session row the
//!      signed-in user (created by Google sign-in, same convention as
//!      `/sync-channels-from-youtube`, whose `session_id` column holds the
//!      access token), renews it if expired, then paginates
//!      `playlistItems` and augments identities with a batched `channels`
//!      call. Users without a synced YouTube account get a per-line error;
//!      the app's `/api/v3/me` exposes `youtubeSynced` for pre-emptive UX.
//!
//! The app groups + creates entities itself (limits/dedupes stay app-side).

use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tower_cookies::Cookies;

use crate::api::v1::auth::renew_token;
use crate::api::v1::user::get_email_from_token;
use crate::errors::AppError;
use crate::InnerState;
use axum::{extract::State, Json};
use chrono::{DateTime, Utc};
use sqlx::Row;

const UA: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolveRequest {
    pub links: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DraftChannel {
    pub channel_id: String,
    pub name: String,
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thumbnail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscriber_count: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video_count: Option<i64>,
}

/// `{success, data}` envelope — the app's HTTP client unwraps `.data`.
#[derive(Debug, Serialize, Deserialize)]
pub struct ResolveResponse {
    pub success: bool,
    pub data: ResolvePayload,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvePayload {
    pub channels: Vec<DraftChannel>,
    /// raw link -> error string, one entry for each unresolvable line
    pub errors: Vec<ResolveError>,
    /// Playlists found in the paste (id + cleaned title) so the app can
    /// name the destination group after them.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub playlists: Vec<PlaylistRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlaylistRef {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolveError {
    pub raw: String,
    pub error: String,
}

/* ------------------------------------------------------------------ */
/* Parsing                                                             */
/* ------------------------------------------------------------------ */

#[derive(Debug, Clone)]
enum ParsedLink {
    ChannelId(String),
    Handle(String),
    VideoId(String),
    PlaylistId(String),
    Invalid(String),
}

fn parse_link(raw_line: &str) -> ParsedLink {
    let raw = raw_line.trim();
    if raw.is_empty() {
        return ParsedLink::Invalid("Empty line".into());
    }
    if raw.starts_with('@') && raw.len() >= 4 && raw[1..].chars().all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_') {
        return ParsedLink::Handle(raw[1..].to_string());
    }
    let with_scheme = if raw.contains("://") { raw.to_string() } else { format!("https://{raw}") };
    let Ok(url) = url::Url::parse(&with_scheme) else {
        return ParsedLink::Invalid("Not a YouTube URL or @handle".into());
    };

    let host = url.host_str().unwrap_or("").to_lowercase();
    if !["youtube.com", "www.youtube.com", "m.youtube.com", "youtu.be", "music.youtube.com"].contains(&host.as_str()) {
        return ParsedLink::Invalid("Not a YouTube URL".into());
    }

    for (_, value) in url.query_pairs() {
        // "PL…" user playlists, "UU…" channel-uploads playlists, "FL…"
        // favorites — all resolvable through playlistItems.
        if value.len() >= 12
            && (value.starts_with("PL") || value.starts_with("UU") || value.starts_with("FL"))
        {
            return ParsedLink::PlaylistId(value.to_string());
        }
    }

    if let Some(pair) = url.query_pairs().find(|(k, _)| k == "v") {
        return ParsedLink::VideoId(pair.1.to_string());
    }

    let path = url.path().to_string();
    if host == "youtu.be" && path.len() > 1 {
        return ParsedLink::VideoId(path[1..].to_string());
    }
    if let Some(rest) = path.rfind("/channel/").map(|i| &path[i + 9..]) {
        if rest.starts_with("UC") {
            return ParsedLink::ChannelId(rest.split('/').next().unwrap_or(rest).to_string());
        }
    }
    if path.len() > 2 && path.starts_with("/@") {
        let handle = path[2..].split('/').next().unwrap_or("").to_string();
        if !handle.is_empty() {
            return ParsedLink::Handle(handle);
        }
    }
    ParsedLink::Invalid("Unsupported YouTube path".into())
}

/* ------------------------------------------------------------------ */
/* HTML helpers (dependency-free string finds)                         */
/* ------------------------------------------------------------------ */

fn find_page_channel_id(html: &str) -> Option<String> {
    for needle in ["\"externalId\":\"", "channel_id=", "itemprop=\"channelId\" content=\""] {
        if let Some(i) = html.find(needle) {
            let rest = &html[i + needle.len()..];
            let end = rest.find(['"', '<', '&', ' ']).unwrap_or(0);
            let cand = &rest[..end.min(rest.len())];
            if cand.starts_with("UC") && cand.len() >= 22 {
                return Some(cand.to_string());
            }
        }
    }
    None
}

fn find_meta_og(html: &str, property: &str) -> Option<String> {
    let needle = format!("property=\"{property}\" content=\"");
    let idx = html.find(&needle)?;
    let rest = &html[idx + needle.len()..];
    let end = rest.find('"')?;
    Some(unescape_html(&rest[..end]))
}

fn unescape_html(s: &str) -> String {
    s.replace("&amp;", "&").replace("&#39;", "'").replace("&quot;", "\"").replace("&lt;", "<").replace("&gt;", ">")
}

/// Finds `"needle":"<captured>"` (JSON-string double-escaped tolerated).
fn find_json_string(html: &str, key: &str) -> Option<String> {
    let needle = format!("\"{key}\"");
    let mut idx = 0;
    while let Some(i) = html[idx..].find(&needle) {
        let at = idx + i;
        let after = &html[at + needle.len()..];
        let rest = after.trim_start();
        if let Some(stripped) = rest.strip_prefix(":") {
            let s = stripped.trim_start();
            if let Some(s) = s.strip_prefix("\"") {
                let mut out = String::new();
                let mut chars = s.chars();
                let mut closed = false;
                while let Some(c) = chars.next() {
                    match c {
                        '\\' => {
                            if let Some(n) = chars.next() {
                                match n {
                                    'n' => out.push('\n'),
                                    '/' => out.push('/'),
                                    '"' => out.push('"'),
                                    '\\' => out.push('\\'),
                                    'u' => {
                                        // Take the next 4 hex digits if present.
                                        let hex: String = chars.by_ref().take(4).collect();
                                        if let Ok(cp) = u32::from_str_radix(&hex, 16) {
                                            if let Some(c) = char::from_u32(cp) {
                                                out.push(c);
                                            }
                                        }
                                    }
                                    other => out.push(other),
                                }
                            }
                        }
                        '"' => {
                            closed = true;
                            break;
                        }
                        other => out.push(other),
                    }
                }
                if closed {
                    return Some(unescape_html(&out));
                }
            }
        }
        idx = at + needle.len();
    }
    None
}

/// Parse YouTube count text such as `3.53M subscribers` / `1,234 videos`.
fn parse_count_text(text: &str) -> Option<i64> {
    let num_part: String = text.chars().take_while(|c| c.is_ascii_digit() || *c == ',' || *c == '.').collect();
    let num_part = num_part.replace(',', "");
    if num_part.is_empty() {
        return None;
    }
    let mult: i64 = if text.contains('K') || text.contains('k') {
        1_000
    } else if text.contains('M') || text.contains('m') {
        1_000_000
    } else if text.contains('B') || text.contains('b') {
        1_000_000_000
    } else {
        1
    };
    num_part.parse::<f64>().ok().map(|v| (v * mult as f64) as i64)
}

/// Extract the human label following a `"simpleText":"…"` occurrence after `after_key`.
fn find_simple_text_after(html: &str, after_key: &str) -> Option<String> {
    let i = html.find(after_key)?;
    let window = &html[i..i + 800.min(html.len() - i)];
    let needle = "\"simpleText\":\"";
    let j = window.find(needle)?;
    let rest = &window[j + needle.len()..];
    let end = rest.find('"')?;
    Some(unescape_html(&rest[..end]))
}

fn count_text_to_number(text: &str) -> Option<i64> {
    parse_count_text(text)
}

/* ------------------------------------------------------------------ */
/* Rough channel html → DraftChannel                                   */
/* ------------------------------------------------------------------ */

async fn fetch_channel_html(client: &Client, channel_url: &str) -> Result<String, String> {
    let res = client
        .get(channel_url)
        .header("accept-language", "en-US,en;q=0.9")
        .send()
        .await
        .map_err(|e| format!("network: {e}"))?;
    if !res.status().is_success() {
        return Err(format!("channel page returned {}", res.status()));
    }
    res.text().await.map_err(|e| format!("body read: {e}"))
}

fn draft_from_html(html: &str, channel_id: String) -> Option<DraftChannel> {
    let name = find_meta_og(html, "og:title")?;
    let thumbnail = find_meta_og(html, "og:image");
    let subscriber_text = find_simple_text_after(html, "\"subscriberCountText\"");
    let video_text = find_simple_text_after(html, "\"videoCountText\"");
    Some(DraftChannel {
        channel_id: channel_id.clone(),
        name,
        url: format!("https://www.youtube.com/channel/{channel_id}"),
        thumbnail,
        subscriber_count: subscriber_text.as_deref().and_then(count_text_to_number),
        video_count: video_text.as_deref().and_then(count_text_to_number),
    })
}

async fn resolve_video(client: &Client, video_id: &str) -> Result<DraftChannel, String> {
    // oEmbed gives the trusted channel identity even on watch pages.
    let url = format!("https://www.youtube.com/oembed?url=https%3A%2F%2Fyoutu.be%2F{video_id}&format=json");
    let res = client.get(&url).header("accept-language", "en").send().await.map_err(|e| format!("network: {e}"))?;
    if !res.status().is_success() {
        return Err(format!("video oEmbed returned {}", res.status()));
    }
    let body: Value = res.json().await.map_err(|e| format!("oEmbed json: {e}"))?;
    let author_name = body["author_name"].as_str().map(|s| s.to_string());
    let author_url = body["author_url"].as_str().map(|s| s.to_string()).unwrap_or_default();
    let thumb = body["thumbnail_url"].as_str().map(|s| s.to_string());

    if let Some(cid) = extract_channel_identifier(&author_url) {
        return finish_channel(client, cid).await;
    }
    if let Some(rest) = author_url.rsplit_once('/').map(|(_, r)| r) {
        if rest.starts_with('@') {
            let handle = &rest[1..];
            let cid = fetch_channel_html(client, &format!("https://www.youtube.com/@{handle}"))
                .await
                .ok()
                .and_then(|h| find_page_channel_id(&h));
            if let Some(cid) = cid {
                let html = fetch_channel_html(client, &format!("https://www.youtube.com/channel/{cid}")).await?;
                let mut draft = draft_from_html(&html, cid).ok_or("channel page incomplete")?;
                if draft.thumbnail.is_none() {
                    draft.thumbnail = thumb;
                }
                if author_name.is_some() && draft.name.is_empty() {
                    draft.name = author_name.unwrap();
                }
                return Ok(draft);
            }
        }
    }
    Err("video channel could not be resolved".into())
}

fn extract_channel_identifier(url: &str) -> Option<String> {
    let idx = url.find("/channel/")?;
    let rest = &url[idx + 9..];
    let end = rest.find(|c: char| !c.is_ascii_alphanumeric() && c != '-' && c != '_').unwrap_or(rest.len());
    Some(rest[..end].to_string()).filter(|s| s.starts_with("UC") && s.len() >= 22)
}

/// Take a UC id all the way to a named DraftChannel via its page HTML.
async fn finish_channel(client: &Client, channel_id: String) -> Result<DraftChannel, String> {
    let url = format!("https://www.youtube.com/channel/{channel_id}");
    let html = fetch_channel_html(client, &url).await?;
    draft_from_html(&html, channel_id).ok_or_else(|| "channel page incomplete".into())
}

/* ------------------------------------------------------------------ */
/* Playlist resolution via the user's synced YouTube account           */
/* ------------------------------------------------------------------ */

/// Minimal serde shapes for the handful of YouTube Data API responses used.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct YtPlaylistItems {
    items: Vec<YtPlaylistItem>,
    next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct YtPlaylistItem {
    #[serde(default)]
    content_details: Option<YtContentDetails>,
    #[serde(default)]
    snippet: Option<YtPlaylistSnippet>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct YtContentDetails {
    video_owner_channel_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct YtPlaylistSnippet {
    /// Preferred: explicit owner channel id (present on current API responses).
    video_owner_channel_id: Option<String>,
    video_owner_channel_title: Option<String>,
}

#[derive(Debug, Deserialize)]
struct YtChannelList {
    items: Vec<YtChannelItem>,
}

#[derive(Debug, Deserialize)]
struct YtChannelItem {
    id: String,
    snippet: Option<YtChannelSnippet>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct YtChannelSnippet {
    title: String,
    #[serde(default)]
    thumbnails: Option<YtThumbnails>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct YtThumbnails {
    default: Option<YtThumb>,
    medium: Option<YtThumb>,
    high: Option<YtThumb>,
}

#[derive(Debug, Deserialize)]
struct YtThumb {
    url: String,
}

#[derive(Debug, Deserialize)]
struct YtPlaylistMeta {
    items: Vec<YtPlaylistMetaItem>,
}

#[derive(Debug, Deserialize)]
struct YtPlaylistMetaItem {
    snippet: Option<YtPlaylistMetaSnippet>,
}

#[derive(Debug, Deserialize)]
struct YtPlaylistMetaSnippet {
    title: Option<String>,
}

/// The user's Google access token. Convention of this codebase (see
/// `/sync-channels-from-youtube`): the access token is stored in the
/// `session_id` column of the `sessions` row with `provider = 'google'`.
/// Expired tokens are renewed first — renewal failures fall through to
/// "not synced" semantics on the caller.
async fn user_google_token(inner: &InnerState, email: &str) -> Option<String> {
    let InnerState { db, oauth_clients, .. } = inner;
    let query = "SELECT * FROM sessions WHERE user_id = (SELECT id FROM users WHERE email = $1 LIMIT 1) and provider = 'google'";

    match sqlx::query(query).bind(email).fetch_one(db).await {
        Ok(bearer) => {
            let expires_at: DateTime<Utc> = bearer.get("expires_at");
            if expires_at >= Utc::now() {
                return Some(bearer.get("session_id"));
            }
            tracing::debug!("youtube-import: google session expired, renewing");
            if renew_token(db.clone(), oauth_clients.google.clone(), email.to_string())
                .await
                .is_err()
            {
                // Stale refresh token (revoked / re-linked elsewhere): treat
                // like "not synced" — the user must sign in with Google again.
                tracing::warn!("youtube-import: google session renewal failed for {email}");
                return None;
            }
            match sqlx::query(query).bind(email).fetch_one(db).await {
                Ok(refreshed) => Some(refreshed.get("session_id")),
                Err(e) => {
                    tracing::warn!("youtube-import: google session re-fetch failed: {e}");
                    None
                }
            }
        }
        Err(e) => {
            tracing::debug!("youtube-import: no google session for {email}: {e}");
            None
        }
    }
}


/// Resolve one playlist id into deduped channel drafts (owner of each video),
/// plus the playlist title when the meta lookup works.
async fn resolve_playlist(
    client: &Client,
    token: &str,
    playlist_id: &str,
) -> Result<(Vec<DraftChannel>, Option<String>), String> {
    // 1) Paginate playlistItems, collecting unique owner channel ids.
    let mut channel_ids: Vec<String> = Vec::new();
    let mut page_token: Option<String> = None;
    let mut pages = 0usize;
    loop {
        let mut url = format!(
            "https://www.googleapis.com/youtube/v3/playlistItems?part=snippet,contentDetails&maxResults=100&playlistId={playlist_id}"
        );
        if let Some(t) = &page_token {
            url.push_str("&pageToken=");
            url.push_str(t);
        }
        let res = client.get(&url).bearer_auth(token).send().await.map_err(|e| format!("network: {e}"))?;
        if !res.status().is_success() {
            let status = res.status();
            let _ = res.text().await;
            let msg = match status.as_u16() {
                403 => "playlist not accessible for this synced account (private or quota)",
                404 | 400 => "playlist not found or invalid",
                _ => "playlist lookup failed",
            };
            return Err(format!("{msg} ({status})"));
        }
        let json: YtPlaylistItems = res.json().await.map_err(|e| format!("playlist json: {e}"))?;
        for item in json.items {
            let owner = item
                .snippet
                .and_then(|s| s.video_owner_channel_id.or(s.video_owner_channel_title))
                .or_else(|| item.content_details.and_then(|c| c.video_owner_channel_id));
            if let Some(id) = owner {
                if id.starts_with("UC") && !channel_ids.contains(&id) {
                    channel_ids.push(id);
                }
            }
        }
        pages += 1;
        match json.next_page_token {
            Some(next) if pages < 5 => page_token = Some(next),
            _ => break,
        }
    }
    if channel_ids.is_empty() {
        return Err("no channels found in that playlist".into());
    }

    // 2) Batched channels call → names + thumbnails.
    let mut drafts: Vec<DraftChannel> = Vec::new();
    for chunk in channel_ids.chunks(50) {
        let url = format!(
            "https://www.googleapis.com/youtube/v3/channels?part=snippet&id={}&maxResults=50",
            chunk.join(",")
        );
        let res = client.get(&url).bearer_auth(token).send().await.map_err(|e| format!("network: {e}"))?;
        if !res.status().is_success() {
            return Err(format!("channel lookup failed ({})", res.status()));
        }
        let json: YtChannelList = res.json().await.map_err(|e| format!("channels json: {e}"))?;
        for item in json.items {
            let (name, thumbnail) = match &item.snippet {
                Some(s) => (
                    s.title.clone(),
                    s.thumbnails
                        .as_ref()
                        .and_then(|t| t.high.as_ref().or(t.medium.as_ref()).or(t.default.as_ref()))
                        .map(|t| t.url.clone()),
                ),
                None => (item.id.clone(), None),
            };
            drafts.push(DraftChannel {
                channel_id: item.id.clone(),
                name,
                url: format!("https://www.youtube.com/channel/{}", item.id),
                thumbnail,
                subscriber_count: None,
                video_count: None,
            });
        }
    }

    // 3) Playlist title (cosmetic — naming the destination group).
    let title = match client
        .get(&format!(
            "https://www.googleapis.com/youtube/v3/playlists?part=snippet&id={playlist_id}"
        ))
        .bearer_auth(token)
        .send()
        .await
    {
        Ok(res) if res.status().is_success() => res
            .json::<YtPlaylistMeta>()
            .await
            .ok()
            .and_then(|m| m.items.into_iter().next())
            .and_then(|i| i.snippet)
            .and_then(|s| s.title),
        _ => None,
    };

    Ok((drafts, title))
}

/* ------------------------------------------------------------------ */
/* Handler                                                             */
/* ------------------------------------------------------------------ */

#[tracing::instrument(name = "Resolve YouTube links", skip(cookies, inner))]
pub async fn resolve(
    cookies: Cookies,
    State(inner): State<InnerState>,
    Json(req): Json<ResolveRequest>,
) -> Result<Json<ResolveResponse>, AppError> {
    let auth_token = cookies
        .get("auth-token")
        .map(|c| c.value().to_string())
        .unwrap_or_default();
    if auth_token.is_empty() {
        return Err(AppError::Authentication(anyhow::anyhow!("Missing auth token")));
    }
    // Token → email (for the Google session lookup later) + validity check
    // (auth middleware already did the heavy lifting).
    let email = get_email_from_token(auth_token).await.map_err(|e| {
        tracing::warn!("youtube-import: token validation failed: {e}");
        e
    })?;

    if req.links.len() > 50 {
        return Err(AppError::Validation("Resolve at most 50 links per request".into()));
    }

    let client = Client::builder()
        .user_agent(UA)
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .map_err(|e| AppError::Unexpected(anyhow::anyhow!(e).context("reqwest build")))?;

    let mut channels: Vec<DraftChannel> = Vec::new();
    let mut errors: Vec<ResolveError> = Vec::new();
    let mut playlists: Vec<PlaylistRef> = Vec::new();
    // Looked up lazily on the first playlist line (most pastes have none).
    let mut google_token: Option<String> = None;

    for raw_line in &req.links {
        match parse_link(raw_line) {
            ParsedLink::ChannelId(id) => match finish_channel(&client, id).await {
                Ok(d) => channels.push(d),
                Err(e) => errors.push(ResolveError { raw: raw_line.clone(), error: e }),
            },
            ParsedLink::Handle(handle) => {
                let html = fetch_channel_html(&client, &format!("https://www.youtube.com/@{handle}")).await;
                let id = html.as_ref().ok().and_then(|h| find_page_channel_id(h));
                match id {
                    Some(id) => match finish_channel(&client, id).await {
                        Ok(d) => channels.push(d),
                        Err(e) => errors.push(ResolveError { raw: raw_line.clone(), error: e }),
                    },
                    None => errors.push(ResolveError {
                        raw: raw_line.clone(),
                        error: html.err().unwrap_or_else(|| "channel not found".into()),
                    }),
                }
            }
            ParsedLink::VideoId(video_id) => {
                // Interruptible failures step down to oEmbed-less paths later.
                match resolve_video(&client, &video_id).await {
                    Ok(d) => channels.push(d),
                    Err(e) => errors.push(ResolveError { raw: raw_line.clone(), error: e }),
                }
            }
            ParsedLink::PlaylistId(playlist_id) => {
                if google_token.is_none() {
                    google_token = user_google_token(&inner, &email).await;
                }
                match google_token.as_ref() {
                    Some(token) => match resolve_playlist(&client, token, &playlist_id).await {
                        Ok((drafts, title)) => {
                            playlists.push(PlaylistRef {
                                id: playlist_id.clone(),
                                name: title.unwrap_or_else(|| playlist_id.clone()),
                            });
                            channels.extend(drafts);
                        }
                        Err(e) => errors.push(ResolveError { raw: raw_line.clone(), error: e }),
                    },
                    None => errors.push(ResolveError {
                        raw: raw_line.clone(),
                        error: "YouTube account not synced or session expired — sign in with Google again to import playlists"
                            .into(),
                    }),
                }
            }
            ParsedLink::Invalid(e) => errors.push(ResolveError { raw: raw_line.clone(), error: e }),
        }
    }

    // de-duplicate by channel id, preserving order.
    let mut seen = std::collections::HashSet::new();
    channels.retain(|c| seen.insert(c.channel_id.clone()));

    Ok(Json(ResolveResponse {
        success: true,
        data: ResolvePayload { channels, errors, playlists },
    }))
}
