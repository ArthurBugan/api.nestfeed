use anyhow::Result;
use axum::{
    extract::{Path, Query, State},
    Json,
};
use sea_orm::{
    ColumnTrait, EntityTrait, JoinType, Order,
    QueryFilter, QueryOrder, QuerySelect, RelationTrait, sea_query::Expr,
};
use tower_cookies::Cookies;

use crate::{
    api::{
        v1::user::get_user_id_from_token,
        v2::animes::{
            AnimePaginationParams, PaginatedAnimesResponse, PaginationInfo, UnifiedAnime,
        },
        v3::entities::{channels, crunchyroll_channels, groups},
    },
    errors::AppError,
    InnerState,
};

#[tracing::instrument(name = "Get all animes", skip(cookies, inner))]
pub async fn all_animes_v3(
    cookies: Cookies,
    State(inner): State<InnerState>,
    Query(params): Query<AnimePaginationParams>,
) -> Result<Json<PaginatedAnimesResponse>, AppError> {
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
    let limit = params.limit.unwrap_or(25).max(1).min(100);
    let offset = (page - 1) * limit;

    let cache_key = format!(
        "user:{}:animes:{}:{}:{}",
        user_id,
        page,
        limit,
        params.search.clone().unwrap_or_default()
    );

    if let Ok(Some(cached)) = redis_cache
        .get_json::<PaginatedAnimesResponse>(&cache_key)
        .await
    {
        return Ok(Json(cached));
    }

    let search_pattern = params.search.as_ref().and_then(|s| {
        if s.trim().is_empty() { None } else { Some(format!("%{}%", s.trim())) }
    });

    let user_animes: Vec<UnifiedAnime> = {
        let mut q = channels::Entity::find()
            .filter(channels::Column::UserId.eq(user_id.clone()))
            .filter(channels::Column::ContentType.eq("anime"))
            .join(JoinType::LeftJoin, channels::Relation::Groups.def())
            .select_only()
            .column(channels::Column::Id)
            .column(channels::Column::UserId)
            .column(channels::Column::GroupId)
            .column(channels::Column::Name)
            .expr_as(Expr::cust("COALESCE(channels.channel_id, '')"), "channel_id")
            .column(channels::Column::Thumbnail)
            .column(channels::Column::CreatedAt)
            .column(channels::Column::UpdatedAt)
            .column_as(groups::Column::Name, "group_name")
            .column_as(groups::Column::Icon, "group_icon")
            .column(channels::Column::Url)
            .column(channels::Column::ContentType)
            .expr_as(Expr::cust("NULL"), "average_rating")
            .expr_as(Expr::cust("NULL"), "launch_year")
            .order_by(channels::Column::CreatedAt, Order::Desc);

        if let Some(ref pattern) = search_pattern {
            q = q.filter(channels::Column::Name.ilike(pattern));
        }

        q.into_model::<UnifiedAnime>()
            .all(&sea_db)
            .await
            .map_err(AppError::SeaORM)?
    };

    let crunchy_animes: Vec<UnifiedAnime> = {
        let mut q = crunchyroll_channels::Entity::find()
            .select_only()
            .column(crunchyroll_channels::Column::Id)
            .expr_as(Expr::cust("NULL"), "user_id")
            .expr_as(Expr::cust("NULL"), "group_id")
            .column_as(crunchyroll_channels::Column::Title, "name")
            .expr_as(Expr::cust("COALESCE(crunchyroll_channels.channel_id, '')"), "channel_id")
            .column_as(crunchyroll_channels::Column::PosterImageUrl, "thumbnail")
            .expr_as(Expr::cust("NULL"), "created_at")
            .expr_as(Expr::cust("NULL"), "updated_at")
            .column_as(crunchyroll_channels::Column::Title, "group_name")
            .expr_as(Expr::cust("NULL"), "group_icon")
            .column_as(crunchyroll_channels::Column::Id, "url")
            .expr_as(Expr::cust("'anime'"), "content_type")
            .column_as(crunchyroll_channels::Column::AverageRating, "average_rating")
            .column(crunchyroll_channels::Column::LaunchYear)
            .filter(crunchyroll_channels::Column::Title.is_not_null())
            .filter(Expr::cust(format!(
                "NOT EXISTS (SELECT 1 FROM channels c2 WHERE c2.content_type = 'anime' AND c2.user_id = '{}' AND c2.name = crunchyroll_channels.title)",
                user_id
            )))
            .order_by(crunchyroll_channels::Column::LaunchYear, Order::Desc)
            .order_by(crunchyroll_channels::Column::AverageRating, Order::Desc);

        if let Some(ref pattern) = search_pattern {
            q = q.filter(crunchyroll_channels::Column::Title.ilike(pattern));
        }

        q.into_model::<UnifiedAnime>()
            .all(&sea_db)
            .await
            .map_err(AppError::SeaORM)?
    };

    let mut combined = Vec::with_capacity(user_animes.len() + crunchy_animes.len());
    combined.extend(user_animes);
    combined.extend(crunchy_animes);

    combined.sort_by(|a, b| {
        let ay = a.launch_year.unwrap_or(0);
        let by = b.launch_year.unwrap_or(0);
        let year_cmp = by.cmp(&ay);
        if year_cmp != std::cmp::Ordering::Equal {
            return year_cmp;
        }
        let ar = a.average_rating.unwrap_or(0.0);
        let br = b.average_rating.unwrap_or(0.0);
        br.partial_cmp(&ar).unwrap_or(std::cmp::Ordering::Equal)
    });

    let total_count = combined.len() as i64;
    let start = offset as usize;
    let end = (start + limit as usize).min(combined.len());
    let page_data = if start < combined.len() { combined[start..end].to_vec() } else { Vec::new() };

    let total_pages = ((total_count as f64) / (limit as f64)).ceil() as i32;

    let response = PaginatedAnimesResponse {
        data: page_data,
        pagination: PaginationInfo {
            total: total_count,
            page,
            limit,
            total_pages,
        },
    };

    let _ = redis_cache.set_json(&cache_key, &response, 300).await;

    Ok(Json(response))
}

#[derive(Debug, serde::Serialize, serde::Deserialize, sea_orm::FromQueryResult)]
#[serde(rename_all = "camelCase")]
pub struct AnimeDetailResponse {
    pub id: String,
    pub name: String,
    pub content_type: Option<String>,
    pub channel_id: Option<String>,
    pub thumbnail: Option<String>,
    pub url: Option<String>,
    pub group_name: Option<String>,
    pub group_icon: Option<String>,
    pub average_rating: Option<f64>,
    pub launch_year: Option<i32>,
}

#[tracing::instrument(name = "Get anime by ID", skip(cookies, inner))]
pub async fn get_anime_v3(
    cookies: Cookies,
    State(inner): State<InnerState>,
    Path(id): Path<String>,
) -> Result<Json<AnimeDetailResponse>, AppError> {
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

    let cache_key = format!("user:{}:anime:{}", user_id, id);

    if let Ok(Some(cached)) = redis_cache
        .get_json::<AnimeDetailResponse>(&cache_key)
        .await
    {
        return Ok(Json(cached));
    }

    if let Some(anime) = channels::Entity::find()
        .filter(channels::Column::Id.eq(id.clone()))
        .filter(channels::Column::UserId.eq(user_id.clone()))
        .filter(channels::Column::ContentType.eq("anime"))
        .join(JoinType::LeftJoin, channels::Relation::Groups.def())
        .select_only()
        .column(channels::Column::Id)
        .column(channels::Column::Name)
        .column(channels::Column::ContentType)
        .column(channels::Column::ChannelId)
        .column(channels::Column::Thumbnail)
        .column(channels::Column::Url)
        .column_as(groups::Column::Name, "group_name")
        .column_as(groups::Column::Icon, "group_icon")
        .expr_as(Expr::cust("NULL"), "average_rating")
        .expr_as(Expr::cust("NULL"), "launch_year")
        .into_model::<AnimeDetailResponse>()
        .one(&sea_db)
        .await
        .map_err(AppError::SeaORM)?
    {
        let response = AnimeDetailResponse {
            id: anime.id.clone(),
            name: anime.name.clone(),
            content_type: anime.content_type.clone(),
            channel_id: anime.channel_id.clone(),
            thumbnail: anime.thumbnail.clone(),
            url: anime.url.clone(),
            group_name: anime.group_name.clone(),
            group_icon: anime.group_icon.clone(),
            average_rating: anime.average_rating.clone(),
            launch_year: anime.launch_year.clone(),
        };

        let _ = redis_cache.set_json(&cache_key, &response, 300).await;
        return Ok(Json(response));
    }

    if let Some(anime) = crunchyroll_channels::Entity::find()
        .filter(crunchyroll_channels::Column::Id.eq(id.clone()))
        .select_only()
        .column(crunchyroll_channels::Column::Id)
        .column_as(crunchyroll_channels::Column::Title, "name")
        .expr_as(Expr::cust("'anime'"), "content_type")
        .expr_as(Expr::cust("COALESCE(crunchyroll_channels.channel_id, '')"), "channel_id")
        .column_as(crunchyroll_channels::Column::PosterImageUrl, "thumbnail")
        .column_as(crunchyroll_channels::Column::Id, "url")
        .expr_as(Expr::cust("NULL"), "group_name")
        .expr_as(Expr::cust("NULL"), "group_icon")
        .column_as(crunchyroll_channels::Column::AverageRating, "average_rating")
        .column(crunchyroll_channels::Column::LaunchYear)
        .into_model::<AnimeDetailResponse>()
        .one(&sea_db)
        .await
        .map_err(AppError::SeaORM)?
    {
        let response = AnimeDetailResponse {
            id: anime.id.clone(),
            name: anime.name.clone(),
            content_type: anime.content_type.clone(),
            channel_id: anime.channel_id.clone(),
            thumbnail: anime.thumbnail.clone(),
            url: anime.url.clone(),
            group_name: anime.group_name.clone(),
            group_icon: anime.group_icon.clone(),
            average_rating: anime.average_rating.clone(),
            launch_year: anime.launch_year.clone(),
        };

        let _ = redis_cache.set_json(&cache_key, &response, 300).await;
        return Ok(Json(response));
    }

    Err(AppError::NotFound(format!("Anime with id {} not found", id)))
}
