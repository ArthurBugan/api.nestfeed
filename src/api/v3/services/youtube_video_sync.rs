//! YouTube Video Sync Service
//! 
//! This module handles syncing videos from YouTube API to the local database.
//! Quota-efficient: uses playlistItems.list (1 unit) + videos.list (1 unit) per channel
//! instead of search.list (100 units).

use chrono::{DateTime, Utc};
use futures::stream::{self, StreamExt};
use reqwest::Client;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, Set,
};
use serde::Deserialize;
use tracing::{error, info, warn};

use crate::api::v3::entities::videos;
use crate::errors::AppError;

#[derive(Debug, serde::Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct YoutubeVideoListResponse {
    pub kind: String,
    pub etag: String,
    pub next_page_token: Option<String>,
    pub prev_page_token: Option<String>,
    pub page_info: PageInfo,
    pub items: Vec<YoutubeVideoItem>,
}

#[derive(Debug, serde::Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PageInfo {
    pub total_results: i32,
    pub results_per_page: i32,
}

#[derive(Debug, serde::Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct YoutubeVideoItem {
    pub kind: String,
    pub etag: String,
    pub id: String,
    pub snippet: Option<VideoSnippet>,
    pub content_details: Option<ContentDetails>,
    pub statistics: Option<Statistics>,
}

#[derive(Debug, serde::Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VideoSnippet {
    pub published_at: String,
    pub channel_id: String,
    pub title: String,
    pub description: String,
    pub thumbnails: Thumbnails,
    pub channel_title: String,
}

#[derive(Debug, serde::Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Thumbnails {
    pub default: Thumbnail,
    pub medium: Thumbnail,
    pub high: Thumbnail,
    pub standard: Option<Thumbnail>,
    pub maxres: Option<Thumbnail>,
}

#[derive(Debug, serde::Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Thumbnail {
    pub url: String,
    #[allow(dead_code)]
    pub width: Option<i32>,
    #[allow(dead_code)]
    pub height: Option<i32>,
}

#[derive(Debug, serde::Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContentDetails {
    pub duration: String,
}

#[derive(Debug, serde::Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Statistics {
    pub view_count: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlaylistItemsResponse {
    items: Vec<PlaylistItem>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlaylistItem {
    content_details: PlaylistItemContentDetails,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlaylistItemContentDetails {
    video_id: String,
}

pub struct VideoSyncService {
    db: DatabaseConnection,
    http_client: Client,
}

impl VideoSyncService {
    /// Shared http client comes from app state so connection pools are
    /// reused across requests instead of being rebuilt per call.
    pub fn new(db: DatabaseConnection, http_client: Client) -> Self {
        Self {
            db,
            http_client,
        }
    }

    /// Sync videos for all channels in a group
    #[tracing::instrument(name = "Sync YouTube videos", skip(self, google_token))]
    pub async fn sync_group_videos(
        &self,
        user_id: &str,
        group_id: &str,
        google_token: &str,
        channel_ids: Vec<String>,
    ) -> Result<usize, AppError> {
        let mut total_synced = 0;

        // Channels are independent — sync them concurrently (bounded so we
        // stay well inside YouTube/Google API rate limits and the DB pool).
        // Results are aggregated afterwards so a failure in one channel
        // doesn't stop (or reorder) the others.
        const CONCURRENCY: usize = 4;
        let results: Vec<(String, Result<usize, AppError>)> = stream::iter(channel_ids)
            .map(|channel_id| async move {
                info!("Syncing videos for channel: {}", channel_id);
                (channel_id.clone(), self.sync_channel_videos(user_id, group_id, &channel_id, google_token).await)
            })
            .buffer_unordered(CONCURRENCY)
            .collect()
            .await;

        for (channel_id, result) in results {
            match result {
                Ok(count) => {
                    total_synced += count;
                    info!("Synced {} videos for channel {}", count, channel_id);
                }
                Err(e) => {
                    error!("Failed to sync videos for channel {}: {:?}", channel_id, e);
                }
            }
        }

        info!(
            "Total videos synced for group {}: {}",
            group_id, total_synced
        );
        
        Ok(total_synced)
    }

    async fn sync_channel_videos(
        &self,
        user_id: &str,
        group_id: &str,
        channel_id: &str,
        google_token: &str,
    ) -> Result<usize, AppError> {
        let max_results = 10;
        
        // Step 1: Get existing video IDs from DB for this channel (no API call)
        let existing_videos = self.get_existing_video_ids(channel_id).await?;
        
        // Step 2: Get video IDs from uploads playlist (1 quota unit)
        let playlist_video_ids = self.fetch_uploads_playlist(channel_id, google_token, max_results).await?;
        
        if playlist_video_ids.is_empty() {
            info!("No videos found in uploads playlist for channel: {}", channel_id);
            return Ok(0);
        }
        
        // Step 3: Filter out videos we already have and were updated < 24h ago
        let video_ids_to_fetch: Vec<String> = playlist_video_ids
            .into_iter()
            .filter(|vid| !existing_videos.contains(vid))
            .collect();
        
        if video_ids_to_fetch.is_empty() {
            info!("All videos already synced for channel: {}", channel_id);
            return Ok(0);
        }
        
        info!("Fetching {} new videos for channel {}", video_ids_to_fetch.len(), channel_id);
        
        // Step 4: Fetch video details (1 quota unit for up to 50 videos)
        let videos_response = self.fetch_video_details(&video_ids_to_fetch, google_token).await?;
        
        let mut synced_count = 0;

        for item in videos_response.items {
            if let Some(snippet) = item.snippet {
                let duration_seconds = item
                    .content_details
                    .as_ref()
                    .and_then(|cd| parse_duration(&cd.duration).ok());

                let views_count = item.statistics.as_ref().and_then(|s| {
                    s.view_count.as_ref().and_then(|v| v.parse::<i32>().ok())
                });

                let thumbnail = snippet
                    .thumbnails
                    .maxres
                    .as_ref()
                    .or(snippet.thumbnails.standard.as_ref())
                    .or(Some(&snippet.thumbnails.high))
                    .map(|t| t.url.clone());

                let video_db_id = format!("{}_{}_{}", user_id, channel_id, item.id);
                let video_url = format!("https://www.youtube.com/watch?v={}", item.id);

                let video_active = videos::ActiveModel {
                    id: Set(video_db_id),
                    channel_id: Set(channel_id.to_string()),
                    group_id: Set(group_id.to_string()),
                    user_id: Set(user_id.to_string()),
                    title: Set(snippet.title),
                    description: Set(Some(snippet.description)),
                    thumbnail: Set(thumbnail),
                    url: Set(Some(video_url)),
                    published_at: Set(Some(DateTime::parse_from_rfc3339(&snippet.published_at)
                        .map_err(|e| {
                            error!("Failed to parse published_at: {:?}", e);
                            AppError::BadRequest("Invalid published_at format".to_string())
                        })?
                        .with_timezone(&Utc)
                        .naive_utc())),
                    content_type: Set("youtube".to_string()),
                    external_id: Set(Some(item.id.clone())),
                    duration_seconds: Set(duration_seconds),
                    views_count: Set(views_count),
                    created_at: Set(Some(Utc::now().naive_utc())),
                    updated_at: Set(Some(Utc::now().naive_utc())),
                };

                video_active.insert(&self.db).await.map_err(|e| {
                    error!("Failed to insert video: {:?}", e);
                    AppError::SeaORM(e)
                })?;

                synced_count += 1;
            }
        }

        Ok(synced_count)
    }

    /// Get existing video IDs from database (no API quota)
    async fn get_existing_video_ids(&self, channel_id: &str) -> Result<Vec<String>, AppError> {
        let videos = videos::Entity::find()
            .filter(videos::Column::ChannelId.eq(channel_id))
            .all(&self.db)
            .await
            .map_err(|e| {
                error!("Database error fetching existing videos: {:?}", e);
                AppError::SeaORM(e)
            })?;
        
        Ok(videos.into_iter().filter_map(|v| v.external_id).collect())
    }

    /// Fetch video IDs from channel's uploads playlist (1 quota unit)
    async fn fetch_uploads_playlist(
        &self,
        channel_id: &str,
        token: &str,
        max_results: i32,
    ) -> Result<Vec<String>, AppError> {
        // Channel uploads playlist ID is "UU" + channel_id (without "UC" prefix)
        let uploads_playlist_id = if channel_id.starts_with("UC") {
            format!("UU{}", &channel_id[2..])
        } else {
            format!("UU{}", channel_id)
        };

        let url = format!(
            "https://www.googleapis.com/youtube/v3/playlistItems?playlistId={}&part=contentDetails&maxResults={}",
            uploads_playlist_id,
            max_results.min(50)
        );

        let response = self
            .http_client
            .get(&url)
            .bearer_auth(token)
            .send()
            .await
            .map_err(|e| {
                error!("HTTP error fetching playlist: {:?}", e);
                AppError::ExternalService(anyhow::Error::new(e))
            })?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            error!("YouTube API error: {}", error_text);
            return Err(AppError::ExternalService(anyhow::anyhow!(
                "YouTube API error: {}",
                error_text
            )));
        }

        let data: PlaylistItemsResponse = response.json().await.map_err(|e| {
            error!("Failed to parse playlist response: {:?}", e);
            AppError::ExternalService(anyhow::Error::new(e))
        })?;

        Ok(data.items.into_iter().map(|i| i.content_details.video_id).collect())
    }

    /// Fetch video details by IDs (1 quota unit for up to 50 videos)
    async fn fetch_video_details(
        &self,
        video_ids: &[String],
        token: &str,
    ) -> Result<YoutubeVideoListResponse, AppError> {
        let ids_param = video_ids.join(",");
        let url = format!(
            "https://www.googleapis.com/youtube/v3/videos?id={}&part=snippet,contentDetails,statistics",
            ids_param
        );

        let response = self
            .http_client
            .get(&url)
            .bearer_auth(token)
            .send()
            .await
            .map_err(|e| {
                error!("HTTP error fetching video details: {:?}", e);
                AppError::ExternalService(anyhow::Error::new(e))
            })?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            error!("YouTube API error: {}", error_text);
            return Err(AppError::ExternalService(anyhow::anyhow!(
                "YouTube API error: {}",
                error_text
            )));
        }

        response.json().await.map_err(|e| {
            error!("Failed to parse videos response: {:?}", e);
            AppError::ExternalService(anyhow::Error::new(e))
        })
    }
}

fn parse_duration(duration_str: &str) -> Result<i32, Box<dyn std::error::Error>> {
    let duration_str = duration_str.trim_start_matches("PT");
    
    let mut total_seconds = 0i32;
    let mut current_num = String::new();
    
    for c in duration_str.chars() {
        if c.is_ascii_digit() {
            current_num.push(c);
        } else {
            let num: i32 = current_num.parse()?;
            match c {
                'H' => total_seconds += num * 3600,
                'M' => total_seconds += num * 60,
                'S' => total_seconds += num,
                _ => {}
            }
            current_num.clear();
        }
    }
    
    Ok(total_seconds)
}
