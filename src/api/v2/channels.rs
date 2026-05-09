use crate::api::common::limits::enforce_channel_addition_limit;
use crate::api::common::utils::timeout_query;
use crate::api::common::ApiResponse;
use crate::api::v1::channel::Channel;
use crate::api::v1::user::get_user_id_from_token;
use crate::api::v1::youtube::sync_channels_from_youtube;
use crate::errors::AppError;
use crate::InnerState;
use anyhow::Result;
use axum::extract::{Path, Query, State};
use axum::Json;
use chrono::NaiveDateTime;
use sea_orm::FromQueryResult;
use serde::{Deserialize, Serialize};
use sqlx::{Execute, FromRow, Postgres, QueryBuilder};
use tower_cookies::Cookies;

/// Pagination parameters for channel queries
#[derive(Debug, Deserialize)]
pub struct ChannelPaginationParams {
    pub page: Option<i32>,
    pub limit: Option<i32>,
    pub search: Option<String>,
}

/// Paginated response structure for channels
#[derive(Debug, Serialize, Deserialize)]
pub struct PaginatedChannelsResponse {
    pub data: Vec<ChannelWithGroup>,
    pub pagination: PaginationInfo,
}

/// Pagination metadata
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PaginationInfo {
    pub total: i64,
    pub page: i32,
    pub limit: i32,
    pub total_pages: i32,
}

/// Channel with group name for API responses
#[derive(Debug, Serialize, Deserialize, FromRow, Clone, FromQueryResult)]
#[serde(rename_all = "camelCase")]
pub struct ChannelWithGroup {
    pub id: String,
    pub user_id: String,
    pub group_id: Option<String>,
    pub name: String,
    pub channel_id: String,
    pub thumbnail: Option<String>,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
    pub group_name: Option<String>,
    pub group_icon: Option<String>,
    pub content_type: Option<String>,
    pub url: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PatchChannelRequest {
    pub id: String,
    pub group_id: String,
    pub name: Option<String>,
    pub thumbnail: Option<String>,
    pub url: Option<String>,
    pub content_type: Option<String>,
    pub new_content: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PatchChannelsBatchRequest {
    pub channels: Vec<PatchChannelRequest>,
}

#[tracing::instrument(name = "Get all channels for user", skip(cookies, inner))]
pub async fn all_channels(
    cookies: Cookies,
    State(inner): State<InnerState>,
    Query(params): Query<ChannelPaginationParams>,
) -> Result<Json<PaginatedChannelsResponse>, AppError> {
    let start_time = std::time::Instant::now();
    tracing::info!("Starting all_channels request");

    let fetch_channels_timeout = tokio::time::Duration::from_millis(10000);
    let InnerState { db, .. } = inner.clone();

    let auth_token = cookies
        .get("auth-token")
        .map(|c| c.value().to_string())
        .unwrap_or_default();

    if auth_token.is_empty() {
        tracing::warn!("all_channels: Missing authentication token");
        return Err(AppError::Authentication(anyhow::anyhow!("Missing token")));
    }

    let user_id = match get_user_id_from_token(auth_token).await {
        Ok(user_id) => {
            tracing::debug!("all_channels: Successfully extracted user_id from token");
            user_id
        }
        Err(e) => {
            tracing::error!(
                "all_channels: Failed to extract user_id from token: {:?}",
                e
            );
            return Err(e);
        }
    };

    tracing::info!("all_channels: Fetching channels for user_id: {}", user_id);

    // Set default pagination values
    let page = params.page.unwrap_or(1).max(1);
    let limit = params.limit.unwrap_or(25).max(1).min(100); // Cap at 100 items per page
    let offset = (page - 1) * limit;

    let cache_key = format!(
        "user:{}:channels:{}:{}:{}",
        user_id,
        page,
        limit,
        params.search.clone().unwrap_or_default()
    );

    let redis_cache = &inner.redis_cache;
    if let Ok(cached) = redis_cache
        .get_json::<PaginatedChannelsResponse>(&cache_key)
        .await
    {
        if let Some(cached_response) = cached {
            if cached_response.data.is_empty() {
                tracing::debug!("all_channels: cache hit but empty payload for page {}, skipping cache", page);
            } else {
                tracing::debug!("all_channels: returning cached page {}", page);
                return Ok(Json(cached_response));
            }
        }
    }

    tracing::info!(
        "all_channels: Pagination - page: {}, limit: {}, offset: {}",
        page,
        limit,
        offset
    );

    // Optimized single query with CTEs
    let query = if params.search.is_some() && !params.search.as_ref().unwrap().trim().is_empty() {
        let search_pattern = format!("%{}%", params.search.as_ref().unwrap().trim());
        r#"
            WITH user_channels AS (
                SELECT c.id, c.user_id, c.group_id, c.name, c.channel_id, c.thumbnail, c.created_at, c.updated_at, g.name as group_name, g.icon as group_icon, c.url, c.content_type 
                FROM channels c 
                LEFT JOIN groups g ON g.id = c.group_id 
                WHERE c.user_id = $1 AND (c.content_type = 'youtube' OR c.content_type IS NULL)
            ),
            user_youtube_channels AS (
                SELECT yc.id, yc.user_id, NULL as group_id, yc.name, yc.channel_id, yc.thumbnail, yc.created_at, yc.updated_at, NULL as group_name, NULL as group_icon, yc.url, 'youtube' as content_type 
                FROM youtube_channels yc 
                WHERE yc.user_id = $1 AND NOT EXISTS (SELECT 1 FROM channels c2 WHERE c2.name = yc.name AND c2.user_id = yc.user_id)
            ),
            combined AS (
                SELECT * FROM user_channels 
                UNION ALL 
                SELECT * FROM user_youtube_channels
            ),
            filtered AS (
                SELECT * FROM combined 
                WHERE LOWER(REPLACE(name, ' ', '')) LIKE LOWER(REPLACE($2, ' ', '')) 
                   OR LOWER(REPLACE(COALESCE(group_name,''), ' ', '')) LIKE LOWER(REPLACE($2, ' ', ''))
            )
            SELECT * FROM filtered ORDER BY created_at DESC LIMIT $3 OFFSET $4
        "#
    } else {
        r#"
            WITH user_channels AS (
                SELECT c.id, c.user_id, c.group_id, c.name, c.channel_id, c.thumbnail, c.created_at, c.updated_at, g.name as group_name, g.icon as group_icon, c.url, c.content_type 
                FROM channels c 
                LEFT JOIN groups g ON g.id = c.group_id 
                WHERE c.user_id = $1 AND (c.content_type = 'youtube' OR c.content_type IS NULL)
            ),
            user_youtube_channels AS (
                SELECT yc.id, yc.user_id, NULL as group_id, yc.name, yc.channel_id, yc.thumbnail, yc.created_at, yc.updated_at, NULL as group_name, NULL as group_icon, yc.url, 'youtube' as content_type 
                FROM youtube_channels yc 
                WHERE yc.user_id = $1 AND NOT EXISTS (SELECT 1 FROM channels c2 WHERE c2.name = yc.name AND c2.user_id = yc.user_id)
            )
            SELECT * FROM user_channels UNION ALL SELECT * FROM user_youtube_channels ORDER BY created_at DESC LIMIT $2 OFFSET $3
        "#
    };

    let search_pattern = if params.search.is_some() && !params.search.as_ref().unwrap().trim().is_empty() {
        Some(format!("%{}%", params.search.as_ref().unwrap().trim()))
    } else {
        None
    };

    let channels = match tokio::time::timeout(
        fetch_channels_timeout,
        async {
            if let Some(ref pattern) = search_pattern {
                sqlx::query_as::<_, ChannelWithGroup>(query)
                    .bind(&user_id)
                    .bind(pattern)
                    .bind(limit)
                    .bind(offset)
                    .fetch_all(&db)
                    .await
            } else {
                sqlx::query_as::<_, ChannelWithGroup>(query)
                    .bind(&user_id)
                    .bind(limit)
                    .bind(offset)
                    .fetch_all(&db)
                    .await
            }
        }
    )
    .await
    {
        Ok(Ok(channels)) => {
            tracing::info!(
                "all_channels: Successfully fetched {} channels",
                channels.len()
            );
            channels
        }
        Ok(Err(e)) => {
            tracing::error!(
                "all_channels: Database error while fetching channels: {:?}",
                e
            );
            return Err(AppError::from(e));
        }
        Err(elapsed) => {
            tracing::error!(
                "all_channels: Timeout while fetching channels: {:?}",
                elapsed
            );
            return Err(AppError::Database(anyhow::anyhow!(
                "Database query timeout after {:?}",
                fetch_channels_timeout
            )));
        }
    };

    // Count total channels for pagination - simplified
    let count_query = if search_pattern.is_some() {
        r#"
            SELECT COUNT(*) FROM (
                WITH user_channels AS (
                    SELECT c.id, c.name, c.group_id FROM channels c 
                    WHERE c.user_id = $1 AND (c.content_type = 'youtube' OR c.content_type IS NULL)
                ),
                user_youtube_channels AS (
                    SELECT yc.id, yc.name, NULL as group_id FROM youtube_channels yc 
                    WHERE yc.user_id = $1 AND NOT EXISTS (SELECT 1 FROM channels c2 WHERE c2.name = yc.name AND c2.user_id = yc.user_id)
                )
                SELECT * FROM user_channels UNION ALL SELECT * FROM user_youtube_channels
            ) combined WHERE LOWER(REPLACE(name, ' ', '')) LIKE LOWER(REPLACE($2, ' ', ''))
        "#
    } else {
        r#"
            SELECT (
                SELECT COUNT(*) FROM channels c WHERE c.user_id = $1 AND (c.content_type = 'youtube' OR c.content_type IS NULL)
            ) + (
                SELECT COUNT(*) FROM youtube_channels yc 
                WHERE yc.user_id = $1 AND NOT EXISTS (SELECT 1 FROM channels c2 WHERE c2.name = yc.name AND c2.user_id = yc.user_id)
            )
        "#
    };

    let total_count = match tokio::time::timeout(
        fetch_channels_timeout,
        async {
            if let Some(ref pattern) = search_pattern {
                sqlx::query_scalar::<_, i64>(count_query)
                    .bind(&user_id)
                    .bind(pattern)
                    .fetch_one(&db)
                    .await
            } else {
                sqlx::query_scalar::<_, i64>(count_query)
                    .bind(&user_id)
                    .fetch_one(&db)
                    .await
            }
        }
    )
    .await
    {
        Ok(Ok(count)) => count,
        Ok(Err(e)) => {
            tracing::error!(
                "all_channels: Database error while counting channels: {:?}",
                e
            );
            return Err(AppError::from(e));
        }
        Err(elapsed) => {
            tracing::error!(
                "all_channels: Timeout while counting channels: {:?}",
                elapsed
            );
            return Err(AppError::Database(anyhow::anyhow!(
                "Database count timeout after {:?}",
                fetch_channels_timeout
            )));
        }
    };

    let inner_for_sync = inner.clone();
    let cookies_for_sync = cookies.clone();
    tokio::spawn(async move {
        if let Err(e) = sync_channels_from_youtube(cookies_for_sync, State(inner_for_sync)).await {
            tracing::error!("Error syncing channels from YouTube: {:?}", e);
        }
    });

    let total_pages = ((total_count as f64) / (limit as f64)).ceil() as i32;

    let response = PaginatedChannelsResponse {
        data: channels,
        pagination: PaginationInfo {
            total: total_count,
            page,
            limit,
            total_pages,
        },
    };

    if !response.data.is_empty() {
        if let Err(e) = inner.redis_cache.set_json(&cache_key, &response, 300).await {
            tracing::warn!("all_channels: redis SETEX error: {:?}", e);
        }
    }

    let duration = start_time.elapsed();
    tracing::info!(
        "all_channels: Completed successfully in {:?} - found {} total channels, {} pages",
        duration,
        total_count,
        total_pages
    );
    Ok(Json(response))
}

#[tracing::instrument(name = "Patch channel by ID", skip(cookies, inner))]
pub async fn patch_channel(
    cookies: Cookies,
    State(inner): State<InnerState>,
    Path(channel_id): Path<String>,
    Json(payload): Json<PatchChannelRequest>,
) -> Result<Json<ApiResponse<ChannelWithGroup>>, AppError> {
    let InnerState { db, .. } = inner;
    tracing::info!(
        "patch_channel: Patching channel {} with payload: {:?}",
        channel_id,
        payload
    );

    let auth_token = cookies
        .get("auth-token")
        .map(|c| c.value().to_string())
        .unwrap_or_default();

    if auth_token.is_empty() {
        tracing::warn!("patch_channel: Missing authentication token");
        return Err(AppError::Authentication(anyhow::anyhow!("Missing token")));
    }

    let user_id = match get_user_id_from_token(auth_token).await {
        Ok(user_id) => user_id,
        Err(e) => {
            tracing::error!(
                "patch_channel: Failed to extract user_id from token: {:?}",
                e
            );
            return Err(e);
        }
    };

    enforce_channel_addition_limit(&db.clone(), &user_id, &payload.group_id.clone(), 1).await?;

    // First, check if the channel exists at all
    let channel_exists =
        match sqlx::query_scalar::<_, String>("SELECT id FROM channels WHERE id = $1 and user_id = $2")
            .bind(&channel_id)
            .bind(&user_id)
            .fetch_optional(&db)
            .await
        {
            Ok(Some(_)) => true,
            Ok(None) => false,
            Err(e) => {
                tracing::error!(
                    "patch_channel: Database error while checking channel existence {}: {:?}",
                    channel_id,
                    e
                );
                return Err(AppError::from(e));
            }
        };

    let updated_channel = if channel_exists {
        // If channel exists, verify user ownership/permissions before updating
        let existing_channel_with_permissions = match sqlx::query_as::<_, ChannelWithGroup>(
            "SELECT
                c.*,
                g.name AS group_name,
                g.icon AS group_icon
                FROM channels c
                LEFT JOIN groups g
                ON g.id = c.group_id
                WHERE c.id = $1
                AND (
                    c.user_id = $2
                    OR g.user_id = $2
                    OR EXISTS (
                    SELECT 1
                    FROM group_members gm
                    WHERE gm.group_id = c.group_id
                        AND gm.user_id = $2
                        AND gm.role IN ('admin', 'editor')
                    )
                )",
        )
        .bind(&channel_id)
        .bind(&user_id)
        .fetch_optional(&db)
        .await
        {
            Ok(Some(channel)) => channel,
            Ok(None) => {
                tracing::warn!("patch_channel: Channel {} exists but user {} does not have permissions to modify it.", channel_id, user_id);
                return Err(AppError::Permission(anyhow::anyhow!(format!(
                    "You do not have permission to modify channel {}",
                    channel_id
                ))));
            }
            Err(e) => {
                tracing::error!(
                    "patch_channel: Database error while verifying channel permissions {}: {:?}",
                    channel_id,
                    e
                );
                return Err(AppError::from(e));
            }
        };

        // Update existing channel
        tracing::info!(
            "patch_channel: Updating existing channel {} for user {}",
            channel_id,
            user_id
        );

        let mut builder: QueryBuilder<Postgres> = QueryBuilder::new("UPDATE channels SET ");

        let mut separated = builder.separated(", ");

        separated.push("group_id = ");
        separated.push_bind_unseparated(payload.group_id);

        if let Some(name) = payload.name {
            separated.push("name = ");
            separated.push_bind_unseparated(name);
        }
        if let Some(thumbnail) = payload.thumbnail {
            separated.push("thumbnail = ");
            separated.push_bind_unseparated(thumbnail);
        }
        if let Some(new_content) = payload.new_content {
            separated.push("new_content = ");
            separated.push_bind_unseparated(new_content);
        }
        if let Some(content_type) = payload.content_type {
            separated.push("content_type = ");
            separated.push_bind_unseparated(content_type);
        }

        // Add WHERE clause
        builder.push(" WHERE id = ");
        builder.push_bind(&channel_id);
        builder.push(" RETURNING *");

        let query = builder.build_query_as::<Channel>();

        let update_timeout = tokio::time::Duration::from_millis(5000);

        tracing::debug!("patch_channel: Executing update query: {:?}", query.sql());

        let updated_channel: Channel = timeout_query(update_timeout, query.fetch_one(&db))
            .await
            .inspect_err(|e| tracing::error!("patch_channel: Database error: {:?}", e))?;

        // After successful update, fetch the channel with group info
        let fetched_channel = match sqlx::query_as::<_, ChannelWithGroup>(
            "SELECT
                c.*,
                g.name AS group_name,
                g.icon AS group_icon
            FROM channels c
            LEFT JOIN groups g ON g.id = c.group_id
            WHERE c.id = $1
            AND (
                    c.user_id = $2
                    OR EXISTS (
                        SELECT 1
                        FROM group_members gm
                        WHERE gm.group_id = c.group_id
                        AND gm.user_id = $2
                    )
            )",
        )
        .bind(&updated_channel.id)
        .bind(&user_id)
        .fetch_one(&db)
        .await
        {
            Ok(channel) => channel,
            Err(e) => {
                tracing::error!(
                    "patch_channel: Database error while fetching updated channel {}: {:?}",
                    updated_channel.id.as_deref().unwrap_or_default(),
                    e
                );
                return Err(AppError::from(e));
            }
        };

        fetched_channel
    } else {
        // Create new channel
        tracing::info!("patch_channel: Creating new channel with ID {}", channel_id);

        let cleaned_url = match payload.url {
            Some(ref url) => Some(url.replace("@", "")),
            None => None,
        };

        let create_timeout = tokio::time::Duration::from_millis(10000); // 10 seconds timeout for creation

        let authenticated_user_id = user_id.clone();
        let group_id_from_payload = payload.group_id.clone();

        // 1. Get the actual owner of the group
        let group_owner_id: String = match tokio::time::timeout(
            create_timeout,
            sqlx::query_scalar::<_, String>("SELECT user_id FROM groups WHERE id = $1")
                .bind(&group_id_from_payload)
                .fetch_one(&db),
        )
        .await
        {
            Ok(Ok(owner_id)) => owner_id,
            Ok(Err(e)) => {
                tracing::error!(
                    "patch_channel: Database error fetching group owner: {:?}",
                    e
                );
                return Err(AppError::Database(anyhow::Error::from(e)));
            }
            Err(elapsed) => {
                tracing::error!("patch_channel: Timeout fetching group owner: {:?}", elapsed);
                return Err(AppError::Timeout(elapsed));
            }
        };

        let mut effective_user_id_for_channel_creation = authenticated_user_id.clone();

        // 2. Check if the authenticated user is an admin/editor of the group
        let member_role: Option<String> = match tokio::time::timeout(
            create_timeout,
            sqlx::query_scalar::<_, String>(
                "SELECT role FROM group_members WHERE group_id = $1 AND user_id = $2",
            )
            .bind(&group_id_from_payload)
            .bind(&authenticated_user_id)
            .fetch_optional(&db),
        )
        .await
        {
            Ok(Ok(role_opt)) => role_opt,
            Ok(Err(e)) => {
                tracing::error!(
                    "create_channel: Database error fetching group member role: {:?}",
                    e
                );
                return Err(AppError::Database(anyhow::Error::from(e)));
            }
            Err(elapsed) => {
                tracing::error!(
                    "create_channel: Timeout fetching group member role: {:?}",
                    elapsed
                );
                return Err(AppError::Timeout(elapsed));
            }
        };

        if let Some(role) = member_role {
            if role == "admin" || role == "editor" {
                // If admin/editor, create channel on behalf of the group owner
                effective_user_id_for_channel_creation = group_owner_id.clone();
            } else {
                // If just a member, they must be the owner to create a channel
                if authenticated_user_id != group_owner_id {
                    return Err(AppError::Permission(anyhow::anyhow!(
                        "You do not have permission to add channels to this group."
                    )));
                }
            }
        } else {
            // If not a member at all, they must be the owner to create a channel
            if authenticated_user_id != group_owner_id {
                return Err(AppError::Permission(anyhow::anyhow!(
                    "You do not have permission to add channels to this group."
                )));
            }
        }

        let new_channel = match tokio::time::timeout(
            create_timeout,
            sqlx::query_as::<_, Channel>(
                "INSERT INTO channels (id, user_id, group_id, name, thumbnail, channel_id, new_content, url, content_type) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING *"
            )
            .bind(&channel_id)
            .bind(&effective_user_id_for_channel_creation) // Use the determined effective_user_id
            .bind(payload.group_id)
            .bind(payload.name)
            .bind(payload.thumbnail)
            .bind(format!("{}/{}", effective_user_id_for_channel_creation, channel_id)) // Also update this format string
            .bind(false)
            .bind(cleaned_url)
            .bind(payload.content_type)
            .fetch_one(&db),
        )
        .await
        {
            Ok(Ok(channel)) => {
                tracing::debug!(
                    "patch_channel: Successfully created new channel with ID: {}",
                    channel.id.as_deref().unwrap_or_default()
                );
                channel
            }
            Ok(Err(e)) => {
                tracing::error!(
                    "patch_channel: Database error while creating channel: {:?}",
                    e
                );
                return Err(AppError::from(e));
            }
            Err(elapsed) => {
                tracing::error!(
                    "patch_channel: Timeout while creating channel: {:?}",
                    elapsed
                );
                return Err(AppError::Database(anyhow::anyhow!(
                    "Database query timeout after {:?}",
                    create_timeout
                )));
            }
        };

        // After successful insertion, fetch the channel with group info
        let fetched_channel = sqlx::query_as::<_, ChannelWithGroup>(
            "SELECT c.*, g.name as group_name, g.icon as group_icon
            FROM channels c
            LEFT JOIN groups g
            ON g.id = c.group_id
            WHERE c.id = $1 AND c.user_id = $2",
        )
        .bind(&new_channel.id)
        .bind(&effective_user_id_for_channel_creation)
        .fetch_one(&db)
        .await?;

        fetched_channel
    };

    let group_pattern = format!("user:{}:groups:*", user_id);
    if let Err(e) = inner.redis_cache.del_pattern(&group_pattern).await {
        tracing::warn!("patch_channel: redis DEL group error: {:?}", e);
    }

    let channels_pattern = format!("user:{}:channels:*", user_id);
    if let Err(e) = inner.redis_cache.del_pattern(&channels_pattern).await {
        tracing::warn!("patch_channel: redis DEL channels error: {:?}", e);
    }

    let animes_pattern = format!("user:{}:animes:*", user_id);
    if let Err(e) = inner.redis_cache.del_pattern(&animes_pattern).await {
        tracing::warn!("patch_channel: redis DEL animes error: {:?}", e);
    }
    Ok(Json(ApiResponse::success(updated_channel)))
}

#[tracing::instrument(name = "Get all channels by group ID for user", skip(inner))]
pub async fn all_channels_by_group_id(
    State(inner): State<InnerState>,
    Path(group_id): Path<String>,
    user_id: String,
) -> Result<Vec<ChannelWithGroup>, AppError> {
    let fetch_channels_timeout = tokio::time::Duration::from_millis(10000);
    let InnerState { db, .. } = inner;

    tracing::info!(
        "all_channels_by_group_id: Fetching channels for user_id: {} and group_id: {}",
        user_id,
        group_id
    );

    let cache_key = format!("user:{}:group:{}:channels", user_id, group_id);
    if let Ok(cached) = inner
        .redis_cache
        .get_json::<Vec<ChannelWithGroup>>(&cache_key)
        .await
    {
        if let Some(cached_response) = cached {
            tracing::debug!(
                "all_channels_by_group_id: returning cached group channels {}",
                &group_id
            );
            return Ok(cached_response);
        }
    }

    let channels = match tokio::time::timeout(
        fetch_channels_timeout,
        sqlx::query_as::<_, ChannelWithGroup>(
            "SELECT c.*, g.name as group_name, g.icon as group_icon FROM channels c INNER JOIN users u ON u.id = c.user_id LEFT JOIN groups g ON g.id = c.group_id LEFT JOIN group_members gm ON g.id = gm.group_id WHERE (g.user_id = $1 OR gm.user_id = $1) AND c.group_id = $2 ORDER BY c.created_at DESC"
        )
        .bind(&user_id)
        .bind(&group_id)
        .fetch_all(&db),
    )
    .await
    {
        Ok(Ok(channels)) => {
            tracing::info!(
                "all_channels_by_group_id: Successfully fetched {} channels for group {}",
                channels.len(),
                group_id
            );
            channels
        }
        Ok(Err(e)) => {
            tracing::error!(
                "all_channels_by_group_id: Database error while fetching channels for group {}: {:?}",
                group_id,
                e
            );
            return Err(AppError::from(e));
        }
        Err(elapsed) => {
            tracing::error!(
                "all_channels_by_group_id: Timeout while fetching channels for group {}: {:?}",
                group_id,
                elapsed
            );
            return Err(AppError::Database(anyhow::anyhow!(
                "Database query timeout after {:?} for group {}",
                fetch_channels_timeout,
                group_id
            )));
        }
    };

    if !channels.is_empty() {
        if let Err(e) = inner.redis_cache.set_json(&cache_key, &channels, 300).await {
            tracing::warn!("all_channels_by_group_id: redis SETEX error: {:?}", e);
        }
    }
    Ok(channels)
}

#[tracing::instrument(name = "Get count of channels by channel ID for user", skip(db))]
pub async fn all_count_by_channel_id(
    db: &sqlx::PgPool,
    group_id: &str,
    user_id: &str,
) -> Result<i64, AppError> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
            FROM channels c
            WHERE c.group_id = $1
            AND EXISTS (
                SELECT 1
                FROM groups g
                LEFT JOIN group_members gm
                ON gm.group_id = g.id
                AND gm.user_id = $2
                WHERE g.id = c.group_id
                AND (
                    g.user_id = $2
                    OR gm.user_id IS NOT NULL
                )
            )",
    )
    .bind(group_id)
    .bind(user_id)
    .fetch_one(db)
    .await?;

    Ok(count)
}

#[tracing::instrument(name = "Delete all channels by group ID for user")]
async fn delete_channels_by_group_id(
    db: &sqlx::PgPool,
    group_id: &str,
    user_id: &str,
) -> Result<(), AppError> {
    sqlx::query!(
        "DELETE FROM channels WHERE group_id = $1 AND user_id = $2",
        group_id,
        user_id
    )
    .execute(db)
    .await?;

    Ok(())
}

#[tracing::instrument(name = "Patch multiple channels in batch", skip(cookies, inner))]
pub async fn patch_channels_batch(
    cookies: Cookies,
    State(inner): State<InnerState>,
    Path(group_id): Path<String>,
    Json(payload): Json<PatchChannelsBatchRequest>,
) -> Result<Json<ApiResponse<Vec<ChannelWithGroup>>>, AppError> {
    let InnerState {
        db,
        sea_db,
        email_client,
        oauth_clients,
        redis_cache,
    } = inner;

    let auth_token = cookies
        .get("auth-token")
        .map(|c| c.value().to_string())
        .unwrap_or_default();

    if auth_token.is_empty() {
        tracing::warn!("patch_channels_batch: Missing authentication token");
        return Err(AppError::Authentication(anyhow::anyhow!("Missing token")));
    }

    let user_id = match get_user_id_from_token(auth_token).await {
        Ok(user_id) => user_id,
        Err(e) => {
            tracing::error!(
                "patch_channels_batch: Failed to extract user_id from token: {:?}",
                e
            );
            return Err(e);
        }
    };
    delete_channels_by_group_id(&db.clone(), &group_id, &user_id).await?;

    let mut updated_channels = Vec::new();

    for channel_request in payload.channels {
        let channel_id_for_patch = channel_request.id.clone();
        let mut patched_channel = patch_channel(
            cookies.clone(),
            State(InnerState {
                db: db.clone(),
                sea_db: sea_db.clone(),
                email_client: email_client.clone(),
                oauth_clients: oauth_clients.clone(),
                redis_cache: redis_cache.clone(),
            }),
            Path(channel_id_for_patch),
            Json(channel_request),
        )
        .await?;
        updated_channels.push(patched_channel.data.as_mut().unwrap().clone());
    }

    // Invalidate user's group cache
    let groups_pattern = format!("user:{}:group:*", user_id);
    if let Err(e) = redis_cache.del_pattern(&groups_pattern).await {
        tracing::warn!("patch_channels_batch: redis DEL groups error: {:?}", e);
    }

    let channels_pattern = format!("user:{}:channels:*", user_id);
    if let Err(e) = redis_cache.del_pattern(&channels_pattern).await {
        tracing::warn!("patch_channels_batch: redis DEL channels error: {:?}", e);
    }

    let animes_pattern = format!("user:{}:animes:*", user_id);
    if let Err(e) = redis_cache.del_pattern(&animes_pattern).await {
        tracing::warn!("patch_channels_batch: redis DEL animes error: {:?}", e);
    }
    Ok(Json(ApiResponse::success(updated_channels)))
}

#[tracing::instrument(name = "Get channel by ID", skip(cookies, inner))]
pub async fn get_channel_by_id(
    cookies: Cookies,
    State(inner): State<InnerState>,
    Path(channel_id): Path<String>,
) -> Result<Json<ApiResponse<ChannelWithGroup>>, AppError> {
    let start_time = std::time::Instant::now();
    tracing::info!(
        "Starting get_channel_by_id request for channel_id: {}",
        channel_id
    );

    let fetch_channel_timeout = tokio::time::Duration::from_millis(5000); // 5 seconds timeout
    let InnerState { db, .. } = inner;

    let auth_token = cookies
        .get("auth-token")
        .map(|c| c.value().to_string())
        .unwrap_or_default();

    if auth_token.is_empty() {
        tracing::warn!("get_channel_by_id: Missing authentication token");
        return Err(AppError::Authentication(anyhow::anyhow!("Missing token")));
    }

    let user_id = match get_user_id_from_token(auth_token).await {
        Ok(user_id) => {
            tracing::debug!("get_channel_by_id: Successfully extracted user_id from token");
            user_id
        }
        Err(e) => {
            tracing::error!(
                "get_channel_by_id: Failed to extract user_id from token: {:?}",
                e
            );
            return Err(e);
        }
    };

    tracing::info!(
        "get_channel_by_id: Fetching channel {} for user_id: {}",
        channel_id,
        user_id
    );

    let cache_key = format!("user:{}:channel:{}", user_id, channel_id);
    if let Ok(cached) = inner
        .redis_cache
        .get_json::<ChannelWithGroup>(&cache_key)
        .await
    {
        if let Some(cached_response) = cached {
            tracing::debug!("get_channel_by_id: returning cached channel {}", channel_id);
            return Ok(Json(ApiResponse::success(cached_response)));
        }
    }

    let channel = match tokio::time::timeout(
        fetch_channel_timeout,
        sqlx::query_as::<_, ChannelWithGroup>(
            "SELECT c.*, g.name as group_name, g.icon as group_icon
             FROM channels c
             INNER JOIN users u ON u.id = c.user_id
             LEFT JOIN groups g ON g.id = c.group_id
             WHERE c.id = $1 AND u.id = $2",
        )
        .bind(&channel_id)
        .bind(&user_id)
        .fetch_optional(&db),
    )
    .await
    {
        Ok(Ok(Some(channel))) => {
            tracing::info!(
                "get_channel_by_id: Successfully fetched channel {}",
                channel_id
            );
            channel
        }
        Ok(Ok(None)) => {
            tracing::warn!(
                "get_channel_by_id: Channel {} not found for user {}",
                channel_id,
                user_id
            );
            return Err(AppError::NotFound(format!(
                "Channel {} not found",
                channel_id
            )));
        }
        Ok(Err(e)) => {
            tracing::error!(
                "get_channel_by_id: Database error while fetching channel {}: {:?}",
                channel_id,
                e
            );
            return Err(AppError::from(e));
        }
        Err(elapsed) => {
            tracing::error!(
                "get_channel_by_id: Timeout while fetching channel {}: {:?}",
                channel_id,
                elapsed
            );
            return Err(AppError::Database(anyhow::anyhow!(
                "Database query timeout after {:?}",
                fetch_channel_timeout
            )));
        }
    };

    tracing::info!(
        "Finished get_channel_by_id request for channel_id: {} in {:?}",
        channel_id,
        start_time.elapsed()
    );

    if let Err(e) = inner.redis_cache.set_json(&cache_key, &channel, 300).await {
        tracing::warn!("get_channel_by_id: redis SETEX error: {:?}", e);
    }
    Ok(Json(ApiResponse::success(channel)))
}

#[tracing::instrument(name = "Delete channel by ID", skip(cookies, inner), fields(channel_id = %channel_id))]
pub async fn delete_channel(
    cookies: Cookies,
    State(inner): State<InnerState>,
    Path(channel_id): Path<String>,
) -> Result<Json<ApiResponse<String>>, Json<ApiResponse<String>>> {
    let start_time = std::time::Instant::now();
    tracing::info!(
        "Starting delete_channel request for channel_id: {}",
        channel_id
    );

    let delete_channel_timeout = tokio::time::Duration::from_millis(5000); // 5 seconds timeout
    let InnerState { db, redis_cache, .. } = inner;

    let auth_token = cookies
        .get("auth-token")
        .map(|c| c.value().to_string())
        .unwrap_or_default();

    if auth_token.is_empty() {
        tracing::warn!("delete_channel: Missing authentication token");
        return Err(Json(ApiResponse::error("Missing token".to_string())));
    }

    let user_id = match get_user_id_from_token(auth_token).await {
        Ok(user_id) => {
            tracing::debug!("delete_channel: Successfully extracted user_id from token");
            user_id
        }
        Err(e) => {
            tracing::error!(
                "delete_channel: Failed to extract user_id from token: {:?}",
                e
            );
            return Err(Json(ApiResponse::error(format!(
                "Failed to extract user_id from token: {:?}",
                e
            ))));
        }
    };

    tracing::info!(
        "delete_channel: Deleting channel {} for user_id: {}",
        channel_id,
        user_id
    );

    let result: Result<ApiResponse<String>, ApiResponse<String>> = match tokio::time::timeout(
        delete_channel_timeout,
        sqlx::query!(
            "DELETE FROM channels WHERE id = $1 AND user_id = $2",
            &channel_id,
            &user_id
        )
        .execute(&db),
    )
    .await
    {
        Ok(Ok(query_result)) => {
            if query_result.rows_affected() == 0 {
                tracing::warn!(
                    "delete_channel: Channel {} not found for user {}",
                    channel_id,
                    user_id
                );
                return Err(Json(ApiResponse::error(format!(
                    "Channel {} not found",
                    channel_id
                ))));
            }
            tracing::info!(
                "delete_channel: Successfully deleted channel {}",
                channel_id
            );
            redis_cache
                .del_pattern(&format!("user:{}:group:*", user_id))
                .await
                .ok();
            Ok(ApiResponse::success(format!(
                "Channel {} deleted successfully",
                channel_id
            )))
        }
        Ok(Err(e)) => {
            tracing::error!(
                "delete_channel: Database error while deleting channel {}: {:?}",
                channel_id,
                e
            );
            return Err(Json(ApiResponse::error(format!("Database error: {:?}", e))));
        }
        Err(elapsed) => {
            tracing::error!(
                "delete_channel: Timeout while deleting channel {}: {:?}",
                channel_id,
                elapsed
            );

            return Err(Json(ApiResponse::error(format!(
                "Timeout while deleting channel {}: {:?}",
                channel_id, elapsed
            ))));
        }
    };

    let channels_pattern = format!("user:{}:channels:*", user_id);
    if let Err(e) = redis_cache.del_pattern(&channels_pattern).await {
        tracing::warn!("delete_channel: redis DEL channels error: {:?}", e);
    }

    let animes_pattern = format!("user:{}:animes:*", user_id);
    if let Err(e) = redis_cache.del_pattern(&animes_pattern).await {
        tracing::warn!("delete_channel: redis DEL animes error: {:?}", e);
    }
    Ok(Json(result.unwrap()))
}
