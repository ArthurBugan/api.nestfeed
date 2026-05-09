use crate::InnerState;
use crate::errors::AppError;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode}; // StatusCode might still be used for direct responses if any
use axum::response::{Response};
use axum::Json;
use base64::engine::general_purpose;
use base64::Engine;

use sqlx::{FromRow};
use url::Url;

const DEFAULT_CACHE_CONTROL_HEADER_VALUE: &str =
    "public, max-age=300, s-maxage=300, state-while-revalidate=300, stale-if-error=300";

#[derive(serde::Deserialize, serde::Serialize, FromRow)]
#[serde(rename_all = "camelCase")]
pub struct Link {
    pub id: String,
    pub target_url: String,
}

#[derive(serde::Deserialize, FromRow)]
#[serde(rename_all = "camelCase")]
pub struct LinkTarget {
    pub target_url: String,
}

#[derive(serde::Serialize, FromRow)]
#[serde(rename_all = "camelCase")]
pub struct CounterLinkStatistics {
    pub amount: Option<i64>,
    pub referer: Option<String>,
    pub user_agent: Option<String>,
}

fn generate_id() -> String {
    let random_number = rand::random::<u32>();
    general_purpose::URL_SAFE_NO_PAD.encode(random_number.to_string())
}

pub async fn redirect(
    State(inner): State<InnerState>,
    Path(requested_link): Path<String>,
    headers: HeaderMap,
) -> Result<Response, AppError> { // Changed return type
    let InnerState { db, .. } = inner;

    let link = sqlx::query_as::<_, Link>(r#" select id, target_url from links where id = $1"#)
        .bind(&requested_link)
        .fetch_optional(&db)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("Link '{}' not found", requested_link)))?;

    tracing::info!(
        "Redirecting link id {} to {}",
        requested_link,
        link.target_url
    );

    let referer_header = headers
        .get("referer")
        .and_then(|value| value.to_str().ok())
        .map(String::from);

    let user_agent_header = headers
        .get("user-agent")
        .and_then(|value| value.to_str().ok())
        .map(String::from);

    let insert_statistics_timeout = tokio::time::Duration::from_millis(10000);

    tokio::time::timeout(
        insert_statistics_timeout,
        sqlx::query(
            r#"
                insert into link_statistics(link_id, referer, user_agent)
                values($1, $2, $3)
                "#,
        )
        .bind(&requested_link) // Ensure requested_link is owned or cloned if needed here
        .bind(referer_header)
        .bind(user_agent_header)
        .execute(&db),
    )
    .await??; // First ? for Timeout, second for sqlx::Error (both map to AppError)
    
    Response::builder()
        .status(StatusCode::TEMPORARY_REDIRECT)
        .header("Location", link.target_url)
        .header("Cache-Control", DEFAULT_CACHE_CONTROL_HEADER_VALUE)
        .body(Body::empty())
        .map_err(|e| AppError::Unexpected(anyhow::Error::new(e).context("Failed to build redirect response")))
}

pub async fn create_link(
    State(inner): State<InnerState>,
    Json(new_link_payload): Json<LinkTarget>,
) -> Result<Json<Link>, AppError> { // Changed return type
    let InnerState { db, .. } = inner;

    let url = Url::parse(&new_link_payload.target_url)? // Uses From<url::ParseError> for AppError
        .to_string();

    let new_link_id = generate_id();
    let query_timeout = tokio::time::Duration::from_millis(10000);

    let new_link = tokio::time::timeout(
        query_timeout,
        sqlx::query_as::<_, Link>(
            r#"INSERT INTO links (id, target_url) VALUES ($1, $2) RETURNING id, target_url"#,
        )
        .bind(new_link_id)
        .bind(url)
        .fetch_one(&db),
    )
    .await??;

    Ok(Json(new_link))
}

pub async fn update_link(
    State(inner): State<InnerState>,
    Path(link_id): Path<String>,
    Json(update_link_payload): Json<LinkTarget>,
) -> Result<Json<Link>, AppError> { // Changed return type
    let InnerState { db, .. } = inner;

    let url = Url::parse(&update_link_payload.target_url)?
        .to_string();

    let query_timeout = tokio::time::Duration::from_millis(10000);

    let link = tokio::time::timeout(
        query_timeout,
        sqlx::query_as::<_, Link>(
            r#"update links set target_url = $1 where id = $2 returning id, target_url"#,
        )
        .bind(url)
        .bind(link_id)
        .fetch_one(&db),
    )
    .await??;

    Ok(Json(link))
}

pub async fn get_link_statistics(
    State(inner): State<InnerState>,
    Path(link_id): Path<String>,
) -> Result<Json<Vec<CounterLinkStatistics>>, AppError> { // Changed return type
    let InnerState { db, .. } = inner;

    let query_timeout = tokio::time::Duration::from_millis(10000);

    let statistics = tokio::time::timeout(
        query_timeout,
        sqlx::query_as::<_, CounterLinkStatistics>(r#"select count(*) as amount, referer, user_agent from link_statistics group by link_id, referer, user_agent having link_id = $1"#)
            .bind(link_id)
            .fetch_all(&db),
    )
    .await??;

    Ok(Json(statistics))
}
