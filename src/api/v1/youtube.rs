use axum::{extract::State, Json};
use chrono::{DateTime, Local, Utc};
use serde_json::{json, Value};
use tower_cookies::Cookies;

use crate::{
    api::v1::auth::renew_token, errors::AppError, api::v1::user::{get_email_from_token}, InnerState
};

use serde::{Deserialize, Serialize};
use sqlx::Row;

use crate::api::v1::channel::{save_youtube_channels, YoutubeChannel};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompleteYoutubeChannel {
    pub kind: String,
    pub etag: String,
    pub next_page_token: Option<String>,
    pub page_info: PageInfo,
    pub items: Vec<Item>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Item {
    pub kind: String,
    pub etag: String,
    pub id: String,
    pub snippet: Snippet,
    pub content_details: ContentDetails,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContentDetails {
    pub total_item_count: i64,
    pub new_item_count: i64,
    pub activity_type: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Snippet {
    pub published_at: String,
    pub title: String,
    pub description: String,
    pub resource_id: ResourceId,
    pub channel_id: String,
    pub thumbnails: Thumbnails,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceId {
    pub kind: String,
    pub channel_id: String,
}

#[derive(Serialize, Deserialize)]
pub struct Thumbnails {
    #[serde(rename = "default")]
    pub default: Default,
    pub medium: Default,
    pub high: Default,
}

#[derive(Serialize, Deserialize)]
pub struct Default {
    pub url: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PageInfo {
    pub total_results: i64,
    pub results_per_page: i64,
}

#[tracing::instrument(name = "Sync channels from YouTube API", skip(cookies, inner))]
pub async fn sync_channels_from_youtube(
    cookies: Cookies,
    State(inner): State<InnerState>,
) -> Result<Json<Value>, AppError> {
    tracing::info!("Starting YouTube channel synchronization");
    let InnerState {
        db,
        oauth_clients,
        http_client,
        ..
    } = &inner;

    tracing::debug!("Extracting auth token from cookies");
    let auth_token = cookies
        .get("auth-token")
        .map(|c| c.value().to_string())
        .unwrap_or_default();

    if auth_token.is_empty() {
        tracing::warn!("No auth token found in cookies");
        return Err(AppError::Authentication(anyhow::anyhow!("Missing auth token")));
    }

    tracing::debug!("Getting email from auth token");
    let email = get_email_from_token(auth_token).await?;
    tracing::info!("Syncing YouTube channels for user: {}", email);

    tracing::debug!("Querying session information from database");
    let bearer = sqlx::query(
        "SELECT * FROM sessions WHERE user_id = (SELECT id FROM users WHERE email = $1 LIMIT 1) and provider = 'google'",
    )
    .bind(email.clone())
    .fetch_one(&db.clone())
    .await
    .map_err(|e| {
        tracing::error!("Database error while fetching session for user {}: {:?}", email, e);
        AppError::Database(anyhow::Error::new(e).context("SQLx operation failed"))
    })?;

    let expires_at: DateTime<Utc> = bearer.get("expires_at");
    tracing::debug!("Session expires at: {}", expires_at);

    if expires_at < Local::now() {
        tracing::info!("Session expired, renewing token for user: {}", email);
        let _ = renew_token(db.clone(), oauth_clients.google.clone(), email.clone()).await;
        tracing::debug!("Token renewal completed");
    }

    tracing::debug!("Fetching updated session information");
    let bearer = sqlx::query(
        "SELECT * FROM sessions WHERE user_id = (SELECT id FROM users WHERE email = $1 LIMIT 1) and provider = 'google'",
    )
    .bind(&email)
    .fetch_one(db)
    .await
    .map_err(|e| {
        tracing::error!("Database error while fetching updated session for user {}: {:?}", email, e);
        AppError::Database(anyhow::Error::new(e).context("SQLx operation failed"))
    })?;

    let session_id: String = bearer.get("session_id");
    tracing::debug!("Using session ID for YouTube API requests");

    let req = http_client;
    let mut all_items = Vec::new();
    let mut next_page_token = None;
    let mut page_count = 0;

    tracing::info!("Starting paginated YouTube API requests");
    loop {
        page_count += 1;
        tracing::debug!("Processing page {} of YouTube subscriptions", page_count);
        
        let mut url = format!(
          "https://www.googleapis.com/youtube/v3/subscriptions?mine=true&maxResults=50&part=snippet,contentDetails"
      );

        if let Some(token) = &next_page_token {
            url.push_str(&format!("&pageToken={}", token));
            tracing::debug!("Using page token for pagination");
        }

        tracing::debug!("Sending request to YouTube API: {}", url);
        let channels_req = req
            .get(&url)
            .bearer_auth(&session_id)
            .send()
            .await
            .map_err(|e| {
                tracing::error!("HTTP error while calling YouTube API: {:?}", e);
                AppError::Database(anyhow::Error::new(e).context("YouTube API request failed"))
            })?;

        if !channels_req.status().is_success() {
            tracing::error!("YouTube API error: {:?}, body: {:?}", channels_req.status(), channels_req.text().await);
            return Err(AppError::Validation(String::from("Could not get the channels list")));
        }

        tracing::debug!("Parsing YouTube API response");
        let youtube_channel: CompleteYoutubeChannel = channels_req.json().await.map_err(|err| {
            tracing::error!("Failed to parse YouTube API response: {:?}", err);
            AppError::Validation(String::from("Could not parse the YouTube API response"))
        })?;

        let items_count = youtube_channel.items.len();
        tracing::debug!("Received {} items from YouTube API on page {}", items_count, page_count);
        all_items.extend(youtube_channel.items);

        if let Some(token) = youtube_channel.next_page_token {
            next_page_token = Some(token);
            tracing::debug!("More pages available, continuing pagination");
        } else {
            tracing::info!("Completed pagination, processed {} pages", page_count);
            break;
        }
    }

    tracing::info!("Retrieved {} total YouTube subscriptions", all_items.len());

    tracing::debug!("Converting YouTube API response to simplified channel format");
    let simplified_channels: Vec<YoutubeChannel> = all_items
        .into_iter()
        .enumerate()
        .map(|(index, item)| {
            if index % 10 == 0 {
                tracing::debug!("Processing channel {} of total", index + 1);
            }
            YoutubeChannel {
                id: Some(item.id.clone()),
                name: item.snippet.title.clone(),
                thumbnail: item.snippet.thumbnails.default.url.clone(),
                channel_id: item.snippet.resource_id.channel_id.clone(),
                created_at: Some(Local::now().naive_utc()),
                updated_at: Some(Local::now().naive_utc()),
                url: format!(
                    "{}{}",
                    "@".to_string(),
                    item.snippet.resource_id.channel_id.trim().to_string()
                ),
                new_content: item.content_details.new_item_count > 0,
            }
        })
        .collect();

    tracing::info!("Converted {} channels to simplified format", simplified_channels.len());

    tracing::debug!("Saving YouTube channels to database");
    let _ = save_youtube_channels(cookies, State(inner.clone()), Json(simplified_channels)).await?;
    tracing::info!("YouTube channel synchronization completed successfully");

    Ok(Json(json!({ "success": "true" })))
}
