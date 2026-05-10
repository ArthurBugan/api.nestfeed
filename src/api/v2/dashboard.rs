use crate::api::common::ApiResponse;
use crate::api::v1::user::get_user_id_from_token;
use crate::errors::AppError;
use crate::InnerState;
use anyhow::Result;
use axum::extract::State;
use axum::Json;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use tower_cookies::Cookies;

#[derive(Debug, Serialize, Deserialize, FromRow, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DashboardTotalResponse {
    groups: i64,
    channels: i64,
    youtube_channels: i64,
    shared_groups: i64,
    anime_channels: i64,
    websites: i64,
}

pub async fn get_dashboard_total(
    cookies: Cookies,
    State(inner): State<InnerState>,
) -> Result<Json<ApiResponse<DashboardTotalResponse>>, AppError> {
    let InnerState { db, .. } = inner;

    let auth_token = cookies
        .get("auth-token")
        .map(|c| c.value().to_string())
        .unwrap_or_default();

    let user_id = match get_user_id_from_token(auth_token).await {
        Ok(user_id) => {
            tracing::debug!("dashboard_total: Successfully extracted user_id from token");
            user_id
        }
        Err(e) => {
            tracing::error!(
                "dashboard_total: Failed to extract user_id from token: {:?}",
                e
            );
            return Err(AppError::from(e));
        }
    };

    let cache_key = format!("user:{}:dashboard_total", user_id);

    if let Ok(cached) = inner.redis_cache.get_json::<DashboardTotalResponse>(&cache_key).await {
        if let Some(counts) = cached {
            return Ok(Json(ApiResponse::success(counts)));
        }
    }

    match sqlx::query_as::<_, DashboardTotalResponse>(
        r#"
        SELECT
            (SELECT COUNT(*) FROM groups WHERE user_id = $1) as groups,
            (SELECT COUNT(*) FROM channels WHERE user_id = $1) as channels,
            (SELECT COUNT(*) FROM youtube_channels WHERE user_id = $1) as youtube_channels,
            (SELECT COUNT(*) FROM crunchyroll_channels) as anime_channels,
            (SELECT COUNT(*) FROM channels WHERE user_id = $1 AND content_type = 'website') as websites,
            (
                SELECT COUNT(DISTINCT sl.group_id)
                FROM share_links sl
                JOIN groups g ON g.id = sl.group_id
                WHERE g.user_id = $1
            ) AS shared_groups
        "#,
    )
    .bind(user_id)
    .fetch_one(&db)
    .await
    {
        Ok(counts) => {
            if let Err(e) = inner.redis_cache.set_json(&cache_key, &counts, 300).await {
                tracing::warn!("dashboard_total: redis SETEX error: {:?}", e);
            }
            Ok(Json(ApiResponse::success(counts)))
        },
        Err(e) => {
            tracing::error!("Failed to fetch dashboard totals: {:?}", e);
            Err(AppError::from(e))
        }
    }
}