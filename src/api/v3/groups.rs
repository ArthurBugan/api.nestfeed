use anyhow::Result;
use axum::{
    extract::{Path, Query, State},
    Json,
};
use chrono::{NaiveDateTime, Utc};
use sea_orm::{
    sea_query::Expr, ActiveModelTrait, ColumnTrait, Condition, EntityTrait, FromQueryResult, JoinType, Order,
    PaginatorTrait, QueryFilter, QueryOrder, QuerySelect, RelationTrait, Set,
};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use std::collections::HashMap;
use tower_cookies::Cookies;
use uuid::Uuid;

use crate::{
    api::{
        common::{ApiResponse, PaginatedResponse, PaginationInfo, PaginationParams},
        v1::auth::renew_token,
        v1::user::{get_email_from_token, get_user_id_from_token},
        v2::channels::ChannelWithGroup,
        v2::groups::Group,
        v3::entities::{channels, group_members, groups, videos},
        v3::services::youtube_video_sync::VideoSyncService,
    },
    errors::AppError,
    InnerState,
};

#[derive(Debug, Clone, FromQueryResult)]
struct GroupWithCountRow {
    pub id: String,
    pub created_at: Option<NaiveDateTime>,
    pub updated_at: Option<NaiveDateTime>,
    pub name: String,
    pub icon: String,
    pub user_id: String,
    pub description: Option<String>,
    pub category: Option<String>,
    pub parent_id: Option<String>,
    pub nesting_level: Option<i32>,
    pub display_order: Option<f64>,
    pub enable_groupshelf: Option<bool>,
    pub channel_count: Option<i64>,
}

#[tracing::instrument(name = "Get all groups v3 (Optimized)", skip(cookies, inner))]
pub async fn all_groups_v3(
    cookies: Cookies,
    State(inner): State<InnerState>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<PaginatedResponse<Group>>, AppError> {
    let InnerState {
        sea_db,
        redis_cache,
        ..
    } = inner;

    let auth_token = cookies
        .get("auth-token")
        .map(|c| c.value().to_string())
        .unwrap_or_default();

    if auth_token.is_empty() {
        return Err(AppError::Authentication(anyhow::anyhow!("Missing token")));
    }

    let user_id = get_user_id_from_token(auth_token).await?;

    let page = params.page.unwrap_or(1).max(1);
    let limit = params.limit.unwrap_or(10).max(1).min(100);
    let offset = (page - 1) * limit;

    let cache_key = format!(
        "user:{}:groups:{}:{}:{}",
        user_id,
        page,
        limit,
        params.search.clone().unwrap_or_default()
    );

    if let Ok(Some(cached)) = redis_cache
        .get_json::<PaginatedResponse<Group>>(&cache_key)
        .await
    {
        return Ok(Json(cached));
    }

    let base_access = Condition::any()
        .add(groups::Column::UserId.eq(user_id.clone()))
        .add(group_members::Column::UserId.eq(user_id.clone()));

    // Count query remains the same
    let mut count_q = groups::Entity::find()
        .join(JoinType::LeftJoin, groups::Relation::GroupMembers.def())
        .filter(base_access.clone());

    if let Some(search) = &params.search {
        if !search.trim().is_empty() {
            let s = format!("%{}%", search.trim());
            count_q = count_q.filter(
                Condition::any()
                    .add(groups::Column::Name.ilike(s.clone()))
                    .add(groups::Column::Description.ilike(s)),
            );
        }
    }

    let total_result_u64 = count_q
        .select_only()
        .column(groups::Column::Id)
        .distinct()
        .count(&sea_db)
        .await
        .map_err(AppError::SeaORM)?;

    let total_result = total_result_u64.try_into().unwrap();
    let total_pages = ((total_result as f64) / (limit as f64)).ceil() as u32;
    let has_next = page < total_pages;
    let has_prev = page > 1;
    
    let order_expr = Expr::cust(
        "CASE WHEN groups.display_order = 0 OR groups.display_order IS NULL THEN 100 ELSE 0 END",
    );

    let order_expr = Expr::cust(
        "COALESCE(NULLIF(groups.display_order, 0), 999999)",
    );

    // Single query with subquery for channel counts
    let mut data_q = groups::Entity::find()
        .join(JoinType::LeftJoin, groups::Relation::GroupMembers.def())
        .filter(base_access)
        .select_only()
        .column(groups::Column::Id)
        .column(groups::Column::CreatedAt)
        .column(groups::Column::UpdatedAt)
        .column(groups::Column::Name)
        .column(groups::Column::Icon)
        .column(groups::Column::UserId)
        .column(groups::Column::Description)
        .column(groups::Column::Category)
        .column(groups::Column::ParentId)
        .column(groups::Column::NestingLevel)
        .column(groups::Column::DisplayOrder)
        .column(groups::Column::EnableGroupshelf)
        // Use a correlated subquery to get channel count in the same query
        .expr_as(
            Expr::cust(
                format!(
                    "COALESCE((SELECT COUNT(*) FROM channels c WHERE c.group_id = groups.id AND c.user_id = '{}'), 0)",
                    user_id
                )
            ),
            "channel_count"
        )
        .order_by(groups::Column::DisplayOrder, Order::Asc)
        .distinct()
        .limit(limit as u64)
        .offset(offset as u64);

    if let Some(search) = &params.search {
        if !search.trim().is_empty() {
            let s = format!("%{}%", search.trim());
            data_q = data_q.filter(
                Condition::any()
                    .add(groups::Column::Name.ilike(s.clone()))
                    .add(groups::Column::Description.ilike(s)),
            );
        }
    }

let rows: Vec<GroupWithCountRow> = data_q
        .into_model::<GroupWithCountRow>()
        .all(&sea_db)
        .await
        .map_err(AppError::SeaORM)?;

    let out: Vec<Group> = rows
        .into_iter()
        .map(|row| Group {
            id: Some(row.id),
            created_at: row.created_at,
            updated_at: row.updated_at,
            name: row.name,
            icon: row.icon,
            user_id: row.user_id,
            description: row.description,
            category: row.category,
            parent_id: row.parent_id,
            nesting_level: row.nesting_level,
            display_order: row.display_order,
            enable_groupshelf: row.enable_groupshelf,
            channel_count: Some(row.channel_count.unwrap_or(0)),
            channels: Vec::new(),
        })
        .collect();

    let response = PaginatedResponse {
        data: out,
        pagination: PaginationInfo {
            page,
            limit,
            total: total_result,
            total_pages,
            has_next,
            has_prev,
        },
    };

    let _ = redis_cache.set_json(&cache_key, &response, 300).await;

    Ok(Json(response))
}

/// Request to create a new channel in a group
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateChannelRequest {
    pub channel_id: Option<String>, // Optional: if provided, will look up existing channel
    pub name: String,
    pub thumbnail: Option<String>,
    pub url: Option<String>,
    pub content_type: Option<String>,
}

/// Create a new channel in a group
#[tracing::instrument(name = "Create channel in group v3", skip(cookies, inner, payload))]
pub async fn create_channel_in_group(
    cookies: Cookies,
    State(inner): State<InnerState>,
    Path(group_id): Path<String>,
    Json(payload): Json<CreateChannelRequest>,
) -> Result<Json<ApiResponse<ChannelWithGroup>>, AppError> {
    let InnerState { sea_db, db, .. } = inner;

    // Authenticate user
    let auth_token = cookies
        .get("auth-token")
        .map(|c| c.value().to_string())
        .unwrap_or_default();

    if auth_token.is_empty() {
        return Err(AppError::Authentication(anyhow::anyhow!("Missing token")));
    }

    let user_id = get_user_id_from_token(auth_token).await?;

    // Check if channel_id is provided - look up existing channel via GET /api/v2/channels/{id} logic
    let target_group_id = if let Some(channel_id) = payload.channel_id {
        tracing::info!("Looking up existing channel {} to get group data", channel_id);
        
        // Replicate GET /api/v2/channels/{id} logic using SeaORM
        let existing_channel = channels::Entity::find()
            .filter(channels::Column::Id.eq(&channel_id))
            .filter(channels::Column::UserId.eq(&user_id))
            .one(&sea_db)
            .await
            .map_err(AppError::SeaORM)?;
        
        if let Some(channel) = existing_channel {
            if let existing_group_id = channel.group_id {
                tracing::info!(
                    "Channel {} is already in group {}. Using that group.",
                    channel_id,
                    existing_group_id
                );
                existing_group_id
            } else {
                tracing::info!(
                    "Channel {} exists but has no group. Using provided group_id {}.",
                    channel_id,
                    group_id
                );
                group_id
            }
        } else {
            tracing::warn!("Channel {} not found. Using provided group_id.", channel_id);
            group_id
        }
    } else {
        group_id
    };

    // Verify user has access to the target group
    let group = groups::Entity::find()
        .filter(groups::Column::Id.eq(&target_group_id))
        .one(&sea_db)
        .await
        .map_err(AppError::SeaORM)?
        .ok_or_else(|| AppError::NotFound(format!("Group {} not found", target_group_id)))?;

    // Check if user is the owner or a member of the group
    let has_access = group.user_id == user_id || {
        group_members::Entity::find()
            .filter(group_members::Column::GroupId.eq(&target_group_id))
            .filter(group_members::Column::UserId.eq(&user_id))
            .one(&sea_db)
            .await
            .map_err(AppError::SeaORM)?
            .is_some()
    };

    if !has_access {
        return Err(AppError::Permission(anyhow::anyhow!(
            "You don't have permission to add channels to this group"
        )));
    }

    // Generate unique channel ID from user_id and url
    let channel_id = format!("{}/{}", user_id, payload.url.clone().unwrap_or_default());
    let now = Utc::now().fixed_offset();
    
    let content_type = payload.content_type.unwrap_or_else(|| "youtube".to_string());

    // Create the channel
    let new_channel = channels::ActiveModel {
        id: Set(Uuid::new_v4().to_string()),
        group_id: Set(target_group_id.clone()),
        user_id: Set(user_id.clone()),
        name: Set(payload.name),
        thumbnail: Set(payload.thumbnail.unwrap_or_default()),
        created_at: Set(Some(now.naive_utc())),
        updated_at: Set(Some(now.naive_utc())),
        new_content: Set(Some(false)),
        channel_id: Set(Some(channel_id.clone())),
        url: Set(payload.url),
        content_type: Set(Some(content_type)),
    };

    let channel = new_channel.insert(&sea_db).await.map_err(AppError::SeaORM)?;

    // Fetch the created channel with group info
    let channel_with_group = channels::Entity::find()
        .filter(channels::Column::Id.eq(&channel.id))
        .join(JoinType::LeftJoin, channels::Relation::Groups.def())
        .select_only()
        .column_as(channels::Column::Id, "id")
        .column_as(channels::Column::UserId, "user_id")
        .column_as(channels::Column::GroupId, "group_id")
        .column_as(channels::Column::Name, "name")
        .column_as(channels::Column::ChannelId, "channel_id")
        .column_as(channels::Column::Thumbnail, "thumbnail")
        .column_as(channels::Column::CreatedAt, "created_at")
        .column_as(channels::Column::UpdatedAt, "updated_at")
        .column_as(channels::Column::ContentType, "content_type")
        .column_as(channels::Column::Url, "url")
        .column_as(groups::Column::Name, "group_name")
        .column_as(groups::Column::Icon, "group_icon")
        .into_model::<ChannelWithGroup>()
        .one(&sea_db)
        .await
        .map_err(AppError::SeaORM)?
        .ok_or_else(|| AppError::NotFound("Channel not found after creation".to_string()))?;

    tracing::info!(
        "Channel {} created in group {} for user {}",
        channel.id,
        target_group_id,
        user_id
    );

    Ok(Json(ApiResponse::success(channel_with_group)))
}

/// Response structure for video data
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VideoResponse {
    pub id: String,
    pub channel_id: String,
    pub group_id: String,
    pub title: String,
    pub description: Option<String>,
    pub thumbnail: Option<String>,
    pub url: Option<String>,
    pub published_at: Option<NaiveDateTime>,
    pub content_type: String,
    pub external_id: Option<String>,
    pub duration_seconds: Option<i32>,
    pub views_count: Option<i32>,
}

impl From<videos::Model> for VideoResponse {
    fn from(video: videos::Model) -> Self {
        Self {
            id: video.id,
            channel_id: video.channel_id,
            group_id: video.group_id,
            title: video.title,
            description: video.description,
            thumbnail: video.thumbnail,
            url: video.url,
            published_at: video.published_at,
            content_type: video.content_type,
            external_id: video.external_id,
            duration_seconds: video.duration_seconds,
            views_count: video.views_count,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ChannelFilterParams {
    pub channel_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VideoQueryParams {
    pub page: Option<u32>,
    pub limit: Option<u32>,
    pub channel_id: Option<String>,
}

/// Get latest videos from channels in a group with pagination
#[tracing::instrument(name = "Get group videos", skip(cookies, inner))]
pub async fn get_group_videos(
    cookies: Cookies,
    State(inner): State<InnerState>,
    Path(group_id): Path<String>,
    Query(params): Query<VideoQueryParams>,
) -> Result<Json<PaginatedResponse<VideoResponse>>, AppError> {
    let InnerState { sea_db, .. } = inner;

    // Authenticate user
    let auth_token = cookies
        .get("auth-token")
        .map(|c| c.value().to_string())
        .unwrap_or_default();

    if auth_token.is_empty() {
        return Err(AppError::Authentication(anyhow::anyhow!("Missing token")));
    }

    let user_id = get_user_id_from_token(auth_token).await?;

    // Verify user has access to this group
    let group = groups::Entity::find()
        .filter(groups::Column::Id.eq(&group_id))
        .one(&sea_db)
        .await
        .map_err(AppError::SeaORM)?
        .ok_or_else(|| AppError::NotFound(format!("Group {} not found", group_id)))?;

    // Check if user is the owner or a member of the group
    let has_access = group.user_id == user_id || {
        group_members::Entity::find()
            .filter(group_members::Column::GroupId.eq(&group_id))
            .filter(group_members::Column::UserId.eq(&user_id))
            .one(&sea_db)
            .await
            .map_err(AppError::SeaORM)?
            .is_some()
    };

    if !has_access {
        return Err(AppError::Permission(anyhow::anyhow!(
            "You don't have permission to view videos in this group"
        )));
    }

    // Set pagination parameters
    let page = params.page.unwrap_or(1).max(1);
    let limit = params.limit.unwrap_or(20).max(1).min(50);
    let offset = (page - 1) * limit;

    // Build base query with optional channel filter
    let channel_id = params.channel_id;

    // Count total videos for pagination
    let mut count_query = videos::Entity::find()
        .filter(videos::Column::GroupId.eq(&group_id))
        .filter(videos::Column::UserId.eq(&user_id));

    if let Some(ref ch_id) = channel_id {
        count_query = count_query.filter(videos::Column::ChannelId.eq(ch_id));
    }

    let total_count = count_query
        .count(&sea_db)
        .await
        .map_err(AppError::SeaORM)?;

    let total_result: i64 = total_count.try_into().unwrap_or(0);
    let total_pages = ((total_result as f64) / (limit as f64)).ceil() as u32;
    let has_next = page < total_pages;
    let has_prev = page > 1;

    // Fetch videos from all channels in the group
    let mut videos_query = videos::Entity::find()
        .filter(videos::Column::GroupId.eq(&group_id))
        .filter(videos::Column::UserId.eq(&user_id));

    if let Some(ref ch_id) = channel_id {
        videos_query = videos_query.filter(videos::Column::ChannelId.eq(ch_id));
    }

    let videos = videos_query
        .order_by_desc(videos::Column::PublishedAt)
        .limit(limit as u64)
        .offset(offset as u64)
        .all(&sea_db)
        .await
        .map_err(AppError::SeaORM)?;

    let video_responses: Vec<VideoResponse> = videos.into_iter().map(|v| v.into()).collect();

    tracing::info!(
        "Retrieved {} videos for group {} (user {}) - page {} of {}",
        video_responses.len(),
        group_id,
        user_id,
        page,
        total_pages
    );

    let response = PaginatedResponse {
        data: video_responses,
        pagination: PaginationInfo {
            page,
            limit,
            total: total_result,
            total_pages,
            has_next,
            has_prev,
        },
    };

    Ok(Json(response))
}

/// Delete all videos from a group
#[tracing::instrument(name = "Delete group videos", skip(cookies, inner))]
pub async fn delete_group_videos(
    cookies: Cookies,
    State(inner): State<InnerState>,
    Path(group_id): Path<String>,
) -> Result<Json<ApiResponse<String>>, AppError> {
    let InnerState { sea_db, .. } = inner;

    let auth_token = cookies
        .get("auth-token")
        .map(|c| c.value().to_string())
        .unwrap_or_default();

    if auth_token.is_empty() {
        return Err(AppError::Authentication(anyhow::anyhow!("Missing token")));
    }

    let user_id = get_user_id_from_token(auth_token).await?;

    let group = groups::Entity::find()
        .filter(groups::Column::Id.eq(&group_id))
        .one(&sea_db)
        .await
        .map_err(AppError::SeaORM)?
        .ok_or_else(|| AppError::NotFound(format!("Group {} not found", group_id)))?;

    if group.user_id != user_id {
        return Err(AppError::Permission(anyhow::anyhow!(
            "You don't have permission to delete videos in this group"
        )));
    }

    let delete_result = videos::Entity::delete_many()
        .filter(videos::Column::GroupId.eq(&group_id))
        .filter(videos::Column::UserId.eq(&user_id))
        .exec(&sea_db)
        .await
        .map_err(AppError::SeaORM)?;

    let deleted_count = delete_result;

    tracing::info!(
        "Deleted videos for group {} by user {}",
        group_id,
        user_id
    );

    Ok(Json(ApiResponse::success("Videos deleted successfully".to_string())))
}

/// Sync videos from YouTube for all channels in a group
#[tracing::instrument(name = "Sync group videos from YouTube", skip(cookies, inner))]
pub async fn sync_group_videos(
    cookies: Cookies,
    State(inner): State<InnerState>,
    Path(group_id): Path<String>,
) -> Result<Json<ApiResponse<String>>, AppError> {
    let InnerState { sea_db, db, .. } = inner;

    // Authenticate user
    let auth_token = cookies
        .get("auth-token")
        .map(|c| c.value().to_string())
        .unwrap_or_default();

    if auth_token.is_empty() {
        return Err(AppError::Authentication(anyhow::anyhow!("Missing token")));
    }

    let user_id = get_user_id_from_token(auth_token.clone()).await?;
    let email = get_email_from_token(auth_token).await?;

    // Verify user has access to this group
    let group = groups::Entity::find()
        .filter(groups::Column::Id.eq(&group_id))
        .one(&sea_db)
        .await
        .map_err(AppError::SeaORM)?
        .ok_or_else(|| AppError::NotFound(format!("Group {} not found", group_id)))?;

    // Check if user is the owner or a member of the group
    let has_access = group.user_id == user_id || {
        group_members::Entity::find()
            .filter(group_members::Column::GroupId.eq(&group_id))
            .filter(group_members::Column::UserId.eq(&user_id))
            .one(&sea_db)
            .await
            .map_err(AppError::SeaORM)?
            .is_some()
    };

    if !has_access {
        return Err(AppError::Permission(anyhow::anyhow!(
            "You don't have permission to sync videos in this group"
        )));
    }

    // Get user's Google session token
    let session = sqlx::query(
        "SELECT * FROM sessions WHERE user_id = (SELECT id FROM users WHERE email = $1 LIMIT 1) and provider = 'google'",
    )
    .bind(&email)
    .fetch_one(&db)
    .await
    .map_err(|e| {
        tracing::error!("No Google session found for user {}: {:?}", email, e);
        AppError::Validation(String::from("Google session not found. Please connect your Google account."))
    })?;

    let expires_at: chrono::DateTime<chrono::Utc> = session.get("expires_at");
    let mut session_id: String = session.get("session_id");

    // Check if token is expired and renew if necessary
    if expires_at < chrono::Utc::now() {
        tracing::info!("Google session expired for user {}, renewing token...", email);
        
        // Get the OAuth client from state
        let oauth_client = inner.oauth_clients.google.clone();
        
        match renew_token(db.clone(), oauth_client, email.clone()).await {
            Ok(_) => {
                tracing::info!("Token renewed successfully for user {}", email);
                
                // Re-fetch the session to get the new token
                let refreshed_session = sqlx::query(
                    "SELECT * FROM sessions WHERE user_id = (SELECT id FROM users WHERE email = $1 LIMIT 1) and provider = 'google'",
                )
                .bind(&email)
                .fetch_one(&db)
                .await
                .map_err(|e| {
                    tracing::error!("Failed to fetch refreshed session for user {}: {:?}", email, e);
                    AppError::Authentication(anyhow::anyhow!("Failed to retrieve refreshed Google session"))
                })?;
                
                session_id = refreshed_session.get("session_id");
            }
            Err(e) => {
                tracing::error!("Failed to renew token for user {}: {:?}", email, e);
                return Err(AppError::Authentication(anyhow::anyhow!(
                    "Google session expired and token renewal failed. Please reconnect your Google account."
                )));
            }
        }
    }

    // Get all channels in the group with YouTube content type
    let channels_in_group: Vec<channels::Model> = channels::Entity::find()
        .filter(channels::Column::GroupId.eq(&group_id))
        .filter(channels::Column::UserId.eq(&user_id))
        .filter(channels::Column::ContentType.eq("youtube"))
        .all(&sea_db)
        .await
        .map_err(AppError::SeaORM)?;

    tracing::info!("Found {} YouTube channels in group {}", channels_in_group.len(), group_id);

    if channels_in_group.is_empty() {
        return Ok(Json(ApiResponse::success(
            "No YouTube channels found in this group".to_string()
        )));
    }

    // Extract YouTube channel IDs from the format "user_id/youtube_channel_id"
    let channel_ids: Vec<String> = channels_in_group
        .into_iter()
        .filter_map(|ch| {
            ch.channel_id.and_then(|id| {
                // Split on '/' and get the second part (YouTube channel ID)
                id.split('/').nth(1).map(|s| s.to_string())
            })
        })
        .collect();

    if channel_ids.is_empty() {
        return Ok(Json(ApiResponse::success(
            "No valid YouTube channel IDs found in this group".to_string()
        )));
    }

    tracing::info!("Extracted {} YouTube channel IDs: {:?}", channel_ids.len(), channel_ids);

    // Trigger video sync
    let sync_service = VideoSyncService::new(sea_db);
    
    match sync_service.sync_group_videos(&user_id, &group_id, &session_id, channel_ids).await {
        Ok(count) => {
            tracing::info!("Successfully synced {} videos for group {}", count, group_id);
            Ok(Json(ApiResponse::success(format!(
                "Successfully synced {} videos",
                count
            ))))
        }
        Err(e) => {
            tracing::error!("Failed to sync videos for group {}: {:?}", group_id, e);
            Err(e)
        }
    }
}

#[tracing::instrument(name = "Get groups with groupshelf enabled", skip(cookies, inner))]
pub async fn get_groupshelf_groups(
    cookies: Cookies,
    State(inner): State<InnerState>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<PaginatedResponse<Group>>, AppError> {
    let InnerState { sea_db, redis_cache, .. } = inner;

    let auth_token = cookies
        .get("auth-token")
        .map(|c| c.value().to_string())
        .unwrap_or_default();

    if auth_token.is_empty() {
        return Err(AppError::Authentication(anyhow::anyhow!("Missing token")));
    }

    let user_id = get_user_id_from_token(auth_token).await?;

    let page = params.page.unwrap_or(1).max(1);
    let limit = params.limit.unwrap_or(10).max(1).min(100);
    let offset = (page - 1) * limit;

    let cache_key = format!(
        "user:{}:groupshelf:{}:{}",
        user_id,
        page,
        limit,
    );

    if let Ok(Some(cached)) = redis_cache
        .get_json::<PaginatedResponse<Group>>(&cache_key)
        .await
    {
        return Ok(Json(cached));
    }

    let count_q = groups::Entity::find()
        .filter(groups::Column::EnableGroupshelf.eq(true));

    let total_result_u64 = count_q
        .count(&sea_db)
        .await
        .map_err(AppError::SeaORM)?;

    let total_result = total_result_u64.try_into().unwrap();
    let total_pages = ((total_result as f64) / (limit as f64)).ceil() as u32;
    let has_next = page < total_pages;
    let has_prev = page > 1;

    let data_q = groups::Entity::find()
        .filter(groups::Column::EnableGroupshelf.eq(true))
        .order_by(groups::Column::DisplayOrder, Order::Asc)
        .limit(limit as u64)
        .offset(offset as u64);

    let rows = data_q
        .all(&sea_db)
        .await
        .map_err(AppError::SeaORM)?;

    let group_ids: Vec<String> = rows.iter().map(|r| r.id.clone()).collect();

    let channels: Vec<ChannelWithGroup> = if !group_ids.is_empty() {
        channels::Entity::find()
            .filter(channels::Column::GroupId.is_in(group_ids.clone()))
            .join(JoinType::LeftJoin, channels::Relation::Groups.def())
            .select_only()
            .column_as(channels::Column::Id, "id")
            .column_as(channels::Column::UserId, "user_id")
            .column_as(channels::Column::GroupId, "group_id")
            .column_as(channels::Column::Name, "name")
            .column_as(channels::Column::ChannelId, "channel_id")
            .column_as(channels::Column::Thumbnail, "thumbnail")
            .column_as(channels::Column::CreatedAt, "created_at")
            .column_as(channels::Column::UpdatedAt, "updated_at")
            .column_as(channels::Column::ContentType, "content_type")
            .column_as(channels::Column::Url, "url")
            .column_as(groups::Column::Name, "group_name")
            .column_as(groups::Column::Icon, "group_icon")
            .into_model::<ChannelWithGroup>()
            .all(&sea_db)
            .await
            .map_err(AppError::SeaORM)?
    } else {
        Vec::new()
    };

    let channels_by_group: std::collections::HashMap<String, Vec<ChannelWithGroup>> = channels
        .into_iter()
        .fold(std::collections::HashMap::new(), |mut acc, ch| {
            if let Some(gid) = &ch.group_id {
                acc.entry(gid.clone()).or_default().push(ch);
            }
            acc
        });

    let out: Vec<Group> = rows
        .into_iter()
        .map(|row| {
            let row_id = row.id.clone();
            let channel_count = channels_by_group.get(&row_id).map(|c| c.len() as i64).unwrap_or(0);
            Group {
                id: Some(row.id),
                created_at: row.created_at,
                updated_at: row.updated_at,
                name: row.name,
                icon: row.icon,
                user_id: row.user_id,
                description: row.description,
                category: row.category,
                parent_id: row.parent_id,
                nesting_level: row.nesting_level,
                display_order: row.display_order,
                enable_groupshelf: row.enable_groupshelf,
                channel_count: Some(channel_count),
                channels: channels_by_group.get(&row_id).cloned().unwrap_or_default(),
            }
        })
        .collect();

    let response = PaginatedResponse {
        data: out,
        pagination: PaginationInfo {
            page,
            limit,
            total: total_result,
            total_pages,
            has_next,
            has_prev,
        },
    };

    let _ = redis_cache.set_json(&cache_key, &response, 300).await;

    Ok(Json(response))
}

#[tracing::instrument(name = "Copy groupshelf group to user", skip(cookies, inner))]
pub async fn copy_groupshelf_group(
    cookies: Cookies,
    State(inner): State<InnerState>,
    Path(group_id): Path<String>,
) -> Result<Json<ApiResponse<Group>>, AppError> {
    let InnerState { sea_db, redis_cache, .. } = inner;

    let auth_token = cookies
        .get("auth-token")
        .map(|c| c.value().to_string())
        .unwrap_or_default();

    if auth_token.is_empty() {
        return Err(AppError::Authentication(anyhow::anyhow!("Missing token")));
    }

    let user_id = get_user_id_from_token(auth_token).await?;

    let source_group = groups::Entity::find()
        .filter(groups::Column::Id.eq(&group_id))
        .filter(groups::Column::EnableGroupshelf.eq(true))
        .one(&sea_db)
        .await
        .map_err(AppError::SeaORM)?
        .ok_or_else(|| AppError::NotFound("Group not found or groupshelf not enabled".to_string()))?;

    let existing_group = groups::Entity::find()
        .filter(groups::Column::UserId.eq(&user_id))
        .filter(groups::Column::Name.eq(&source_group.name))
        .one(&sea_db)
        .await
        .map_err(AppError::SeaORM)?;

    if existing_group.is_some() {
        return Err(AppError::BadRequest("You already own a group with this name".to_string()));
    }

    let now = Utc::now().fixed_offset();
    let naive_now = now.naive_utc();

    async fn copy_group_with_children(
        sea_db: &sea_orm::DatabaseConnection,
        source_group: &groups::Model,
        user_id: &str,
        now: chrono::NaiveDateTime,
        parent_id_map: &mut std::collections::HashMap<String, String>,
    ) -> Result<String, AppError> {
        let new_group_id = Uuid::new_v4().to_string();

        let new_group = groups::ActiveModel {
            id: Set(new_group_id.clone()),
            name: Set(source_group.name.clone()),
            icon: Set(source_group.icon.clone()),
            user_id: Set(user_id.to_string()),
            description: Set(source_group.description.clone()),
            category: Set(source_group.category.clone()),
            parent_id: Set(source_group.parent_id.clone()),
            nesting_level: Set(source_group.nesting_level),
            display_order: Set(source_group.display_order),
            enable_groupshelf: Set(Some(false)),
            created_at: Set(Some(now)),
            updated_at: Set(Some(now)),
        };

        let _created_group = new_group.insert(sea_db).await.map_err(AppError::SeaORM)?;

        let channels = channels::Entity::find()
            .filter(channels::Column::GroupId.eq(&source_group.id))
            .all(sea_db)
            .await
            .map_err(AppError::SeaORM)?;

        for channel in channels {
            let new_channel_id = Uuid::new_v4().to_string();
            let new_channel = channels::ActiveModel {
                id: Set(new_channel_id),
                group_id: Set(new_group_id.clone()),
                user_id: Set(user_id.to_string()),
                name: Set(channel.name),
                thumbnail: Set(channel.thumbnail),
                channel_id: Set(channel.channel_id),
                content_type: Set(channel.content_type),
                url: Set(channel.url),
                new_content: Set(channel.new_content),
                created_at: Set(Some(now)),
                updated_at: Set(Some(now)),
            };
            new_channel.insert(sea_db).await.map_err(AppError::SeaORM)?;
        }

        parent_id_map.insert(source_group.id.clone(), new_group_id.clone());

        Ok(new_group_id)
    }

    let mut parent_id_map = std::collections::HashMap::new();
    let _root_group_id = copy_group_with_children(&sea_db, &source_group, &user_id, naive_now, &mut parent_id_map).await?;

    let mut to_process: Vec<String> = vec![source_group.id.clone()];
    while let Some(current_parent_id) = to_process.pop() {
        let children = groups::Entity::find()
            .filter(groups::Column::ParentId.eq(&current_parent_id))
            .all(&sea_db)
            .await
            .map_err(AppError::SeaORM)?;

        for child in children {
            if let Some(new_parent_id) = parent_id_map.get(&current_parent_id) {
                let new_child_id = Uuid::new_v4().to_string();

                let new_child = groups::ActiveModel {
                    id: Set(new_child_id.clone()),
                    name: Set(child.name),
                    icon: Set(child.icon),
                    user_id: Set(user_id.clone()),
                    description: Set(child.description),
                    category: Set(child.category),
                    parent_id: Set(Some(new_parent_id.clone())),
                    nesting_level: Set(child.nesting_level),
                    display_order: Set(child.display_order),
                    enable_groupshelf: Set(Some(false)),
                    created_at: Set(Some(naive_now)),
                    updated_at: Set(Some(naive_now)),
                };

                let _created_child = new_child.insert(&sea_db).await.map_err(AppError::SeaORM)?;

                let child_channels = channels::Entity::find()
                    .filter(channels::Column::GroupId.eq(&child.id))
                    .all(&sea_db)
                    .await
                    .map_err(AppError::SeaORM)?;

                for channel in child_channels {
                    let new_channel_id = format!("{}/{}", user_id, Uuid::new_v4().to_string());
                    let new_channel = channels::ActiveModel {
                        id: Set(new_channel_id),
                        group_id: Set(new_child_id.clone()),
                        user_id: Set(user_id.clone()),
                        name: Set(channel.name),
                        thumbnail: Set(channel.thumbnail),
                        channel_id: Set(channel.channel_id),
                        content_type: Set(channel.content_type),
                        url: Set(channel.url),
                        new_content: Set(channel.new_content),
                        created_at: Set(Some(naive_now)),
                        updated_at: Set(Some(naive_now)),
                    };
                    new_channel.insert(&sea_db).await.map_err(AppError::SeaORM)?;
                }

                parent_id_map.insert(child.id.clone(), new_child_id);
                to_process.push(child.id);
            }
        }
    }

    let created_group = groups::Entity::find()
        .filter(groups::Column::Id.eq(parent_id_map.get(&source_group.id).unwrap()))
        .one(&sea_db)
        .await
        .map_err(AppError::SeaORM)?
        .ok_or_else(|| AppError::NotFound("Created group not found".to_string()))?;

    let groupshelf_pattern = format!("user:{}:groupshelf:*", user_id);
    if let Err(e) = redis_cache.del_pattern(&groupshelf_pattern).await {
        tracing::warn!("copy_groupshelf_group: redis DEL groupshelf error: {:?}", e);
    }
    let groups_pattern = format!("user:{}:groups:*", user_id);
    if let Err(e) = redis_cache.del_pattern(&groups_pattern).await {
        tracing::warn!("copy_groupshelf_group: redis DEL groups error: {:?}", e);
    }

    let response_group = Group {
        id: Some(created_group.id),
        created_at: created_group.created_at,
        updated_at: created_group.updated_at,
        name: created_group.name,
        icon: created_group.icon,
        user_id: created_group.user_id,
        description: created_group.description,
        category: created_group.category,
        parent_id: created_group.parent_id,
        nesting_level: created_group.nesting_level,
        display_order: created_group.display_order,
        enable_groupshelf: created_group.enable_groupshelf,
        channel_count: None,
        channels: Vec::new(),
    };

    Ok(Json(ApiResponse::success(response_group)))
}

#[derive(Debug, serde::Serialize, serde::Deserialize, sea_orm::FromQueryResult, Clone)]
pub struct SubgroupFlat {
    pub id: String,
    pub name: String,
    pub icon: String,
    pub description: Option<String>,
    pub category: Option<String>,
    pub parent_id: Option<String>,
    pub nesting_level: Option<i32>,
    pub display_order: Option<f64>,
    pub enable_groupshelf: Option<bool>,
}

#[tracing::instrument(name = "Get subgroups by group ID", skip(cookies, inner))]
pub async fn get_subgroups_by_channel(
    cookies: Cookies,
    State(inner): State<InnerState>,
    Path(group_id): Path<String>,
) -> Result<Json<Vec<SubgroupFlat>>, AppError> {
    let InnerState {
        sea_db,
        redis_cache,
        ..
    } = inner;

    let auth_token = cookies
        .get("auth-token")
        .map(|c| c.value().to_string())
        .unwrap_or_default();

    if auth_token.is_empty() {
        return Err(AppError::Authentication(anyhow::anyhow!("Missing token")));
    }

    let user_id = get_user_id_from_token(auth_token).await?;

    let cache_key = format!("user:{}:subgroups:{}", user_id, group_id);

    if let Ok(Some(cached)) = redis_cache
        .get_json::<Vec<SubgroupFlat>>(&cache_key)
        .await
    {
        return Ok(Json(cached));
    }

    let mut subgroups = Vec::new();
    let mut to_visit = vec![group_id.clone()];

    while let Some(current_parent) = to_visit.pop() {
        let children = groups::Entity::find()
            .filter(groups::Column::UserId.eq(user_id.clone()))
            .filter(groups::Column::ParentId.is_not_null())
            .filter(groups::Column::ParentId.eq(current_parent))
            .select_only()
            .column(groups::Column::Id)
            .column(groups::Column::Name)
            .column(groups::Column::Icon)
            .column(groups::Column::Description)
            .column(groups::Column::Category)
            .column(groups::Column::ParentId)
            .column(groups::Column::NestingLevel)
            .column(groups::Column::DisplayOrder)
            .column(groups::Column::EnableGroupshelf)
            .into_model::<SubgroupFlat>()
            .all(&sea_db)
            .await
            .map_err(AppError::SeaORM)?;

        for child in children {
            subgroups.push(child.clone());
            to_visit.push(child.id.clone());
        }
    }

    let _ = redis_cache.set_json(&cache_key, &subgroups, 300).await;

    Ok(Json(subgroups))
}
