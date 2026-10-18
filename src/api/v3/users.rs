use anyhow::Result;
use axum::{extract::State, Json};
use chrono::{DateTime, FixedOffset, NaiveDateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::{ColumnTrait, EntityTrait, FromQueryResult, QueryFilter};
use serde::{Deserialize, Serialize};
use tower_cookies::Cookies;

use crate::{
    api::{
        common::ApiResponse,
        v1::user::get_user_id_from_token,
        v3::entities::{
            groups, subscription_plans, subscription_plans_users, users, youtube_channels,
        },
    },
    errors::AppError,
    InnerState,
};

#[derive(Debug, Serialize, Deserialize, FromQueryResult)]
#[serde(rename_all = "camelCase")]
pub struct User {
    pub id: String,
    pub email: String,
    pub username: String,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
    pub plan_name: String,
    pub max_channels: i32,
    pub max_groups: i32,
    pub can_create_subgroups: bool,
    pub price_monthly: Decimal,
    pub price_yearly: Decimal,
    pub subscription_start_date: DateTime<FixedOffset>,
    pub subscription_end_date: DateTime<FixedOffset>,
    pub group_count: i32,
    pub channel_count: i32,
    pub can_add_channel: bool,
    pub can_add_group: bool,
    /// True when the user has a Google (YouTube) session linked, so
    /// playlist imports can be resolved with their own authorization
    /// (spec 007/008 — replaces client API keys).
    pub youtube_synced: bool,
}

#[tracing::instrument(name = "Get current user data", skip(cookies, inner))]
pub async fn me(
    cookies: Cookies,
    State(inner): State<InnerState>,
) -> Result<Json<ApiResponse<User>>, AppError> {
    let db = &inner.sea_db;

    // 1. Auth token
    let auth_token = cookies
        .get("auth-token")
        .and_then(|cookie| Some(cookie.value().to_string()))
        .ok_or_else(|| {
            tracing::warn!("No auth token found in cookies");
            AppError::Authentication(anyhow::anyhow!("No authentication token"))
        })?;

    let user_id = get_user_id_from_token(auth_token.to_string()).await?;

    // 2. Load user + subscription plans in ONE query
    let user = match users::Entity::find()
        .filter(users::Column::Id.eq(user_id.clone()))
        .one(db)
        .await
        .map_err(AppError::SeaORM)?
    {
        Some(u) => u,
        None => {
            return Err(AppError::Permission(anyhow::anyhow!(
                "You do not have permission to modify this user"
            )))
        }
    };

    // 3. Count groups for user
    let group_count =
        sqlx::query_scalar::<_, i32>("SELECT COUNT(*) FROM groups WHERE user_id = $1")
            .bind(&user_id)
            .fetch_one(&inner.db)
            .await
            .unwrap_or(0);

    let channels_count =
        sqlx::query_scalar::<_, i32>("SELECT COUNT(*) FROM channels WHERE user_id = $1")
            .bind(&user_id)
            .fetch_one(&inner.db)
            .await
            .unwrap_or(0);

    // YouTube sync: a Google session row (created by Google sign-in) is what
    // lets the backend resolve playlists with the user's own authorization.
    let youtube_synced = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM sessions WHERE user_id = $1 AND provider = 'google'",
    )
    .bind(&user_id)
    .fetch_one(&inner.db)
    .await
    .unwrap_or(0)
        > 0;

    let subscription_plans_users = subscription_plans_users::Entity::find()
        .filter(subscription_plans_users::Column::UserId.eq(user_id))
        .find_also_related(subscription_plans::Entity)
        .all(db)
        .await
        .map_err(AppError::SeaORM)?;

    let current = subscription_plans_users.into_iter().find(|(sub_user, _)| {
        sub_user
            .ended_at
            .map_or(true, |end_date| end_date > Utc::now().fixed_offset())
    });

    if let Some((active_subscription, Some(plan))) = current {
        println!(
            "Current plan: {:?}, active subscription: {:?}",
            plan, active_subscription
        );

        return Ok(Json(ApiResponse::success(User {
            id: user.id,
            email: user.email,
            username: user.display_name.unwrap_or_default(),
            created_at: user.created_at.unwrap_or_default(),
            updated_at: user.updated_at.unwrap_or_default(),

            plan_name: plan.name,
            max_channels: plan.max_channels,
            max_groups: plan.max_groups,
            can_create_subgroups: plan.can_create_subgroups,
            price_monthly: plan.price_monthly,
            price_yearly: plan.price_yearly,

            subscription_start_date: active_subscription.started_at.unwrap_or_default(),
            subscription_end_date: active_subscription.ended_at.unwrap_or_default(),

            group_count,
            channel_count: channels_count,

            can_add_channel: channels_count < plan.max_channels,
            can_add_group: group_count < plan.max_groups,
            youtube_synced,
        })));
    }

    Ok(Json(ApiResponse::error(
        "No active subscription".to_string(),
    )))
}
