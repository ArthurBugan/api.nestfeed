use axum::{
    extract::{Query, State},
    response::{IntoResponse, Redirect},
    Json,
};
use axum::http::HeaderMap;
use oauth2::{reqwest::async_http_client, AuthorizationCode, TokenResponse};
use serde::{Deserialize, Serialize};
use tower_cookies::Cookies;

use crate::{
    api::{
        common::{utils::setup_auth_cookie, ApiResponse},
        v1::{
            login::generate_token,
            oauth::{fetch_user_profile, update_user_session, AuthRequest, OAuthProvider, Session},
            user::{create_user, get_email_from_token, get_user_id_from_email, User},
        },
    },
    errors::AppError,
    InnerState,
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SocialLoginSessionStatus {
    pub connected: bool,
    pub provider: String,
    pub expired: bool,
    pub expires_at: String,
}

#[derive(Debug, Deserialize)]
pub struct AuthQueryParams {
    pub origin: Option<String>,
}

fn is_mobile_browser(headers: &HeaderMap) -> bool {
    if let Some(user_agent) = headers.get("user-agent") {
        if let Ok(ua) = user_agent.to_str() {
            let ua_lower = ua.to_lowercase();
            return ua_lower.contains("mobile")
                || ua_lower.contains("android")
                || ua_lower.contains("iphone")
                || ua_lower.contains("ipad")
                || ua_lower.contains("ipod");
        }
    }
    false
}

#[tracing::instrument(name = "Discord OAuth callback", skip(cookies, inner, query), fields(code_length = query.code.len()))]
pub async fn discord_callback(
    cookies: Cookies,
    headers: HeaderMap,
    State(inner): State<InnerState>,
    Query(query): Query<AuthRequest>,
) -> Result<impl IntoResponse, AppError> {
    tracing::info!("Processing Discord OAuth callback");

    let InnerState {
        db, oauth_clients, redis_cache, ..
    } = inner;

    tracing::debug!("Exchanging authorization code for access token");
    let token = oauth_clients
        .discord
        .exchange_code(AuthorizationCode::new(query.code))
        .request_async(async_http_client)
        .await
        .map_err(|e| {
            tracing::error!("Failed to exchange Discord authorization code: {:?}", e);
            AppError::ExternalService(anyhow::anyhow!(
                "Failed to exchange authorization code: {}",
                e
            ))
        })?;

    let access_token = &token.access_token().secret();
    let refresh_token = token.refresh_token().map(|t| t.secret()).map_or("", |v| v);

    tracing::debug!("Successfully obtained Discord access token");

    let user_profile = fetch_user_profile(access_token, &OAuthProvider::Discord).await?;
    tracing::info!("Fetched Discord user profile for: {}", user_profile.email);

    let expires_at = crate::api::v1::auth::calculate_token_expiry(token.expires_in()).await;

    let auth_token = cookies
        .get("auth-token")
        .map(|c| c.value().to_string())
        .unwrap_or_default();

    let original_email = user_profile.email.clone();

    let email = if auth_token.is_empty() {
        tracing::info!("No auth-token found, using Discord profile email: {}", original_email);
        original_email.clone()
    } else {
        tracing::info!("Auth-token found, using token to get email: {}", auth_token);
        get_email_from_token(auth_token).await?
    };

    let user_id = match get_user_id_from_email(&db, &email).await {
        Ok(id) => id,
        Err(AppError::NotFound(_)) => {
            tracing::info!("User not found for email {}. Creating user from Discord callback.", email);
            let new_user = User {
                id: None,
                email: email.clone(),
                encrypted_password: None,
                ..Default::default()
            };
            let mut transaction = db.begin().await.map_err(|e| {
                AppError::Database(anyhow::Error::from(e).context("Failed to start transaction"))
            })?;
            let id = create_user(&mut transaction, new_user).await?;
            transaction.commit().await.map_err(|e| {
                AppError::Database(anyhow::Error::from(e).context("Failed to commit transaction"))
            })?;
            id
        }
        Err(e) => return Err(e),
    };

    tracing::info!("Updating user session for: {:?} and original_email {:?}", email, original_email);

    update_user_session(
        &db,
        &email,
        access_token,
        expires_at,
        refresh_token,
        &OAuthProvider::Discord,
        &original_email,
    )
    .await?;

    tracing::info!(
        "Discord OAuth callback completed successfully for: {}",
        email
    );

    // Redirect to your frontend with success
    let frontend_url =
        std::env::var("GROUPIFY_HOST").unwrap_or_else(|_| "https://nestfeed.app".to_string());

    tracing::debug!("Generating JWT token for user: {} and original_email: {}", email, original_email);

    let user_id = get_user_id_from_email(&db, &email).await?;

    let token = generate_token(
        &email,
        &user_id,
    )?;
    tracing::debug!("JWT token generated successfully");

    tracing::debug!("Retrieving GROUPIFY_HOST environment variable");
    let domain = std::env::var("GROUPIFY_HOST").map_err(|e| {
        tracing::error!("GROUPIFY_HOST environment variable not set: {:?}", e);
        AppError::Unexpected(anyhow::anyhow!(e).context("GROUPIFY_HOST env var not set"))
    })?;

    // Use the utility function instead of duplicated code
    setup_auth_cookie(&token, &domain, &cookies);

    let channels_pattern = format!("user:{}:channels:*", user_id);
    if let Err(e) = redis_cache.del_pattern(&channels_pattern).await {
        tracing::warn!("discord_callback: redis DEL channels error: {:?}", e);
    }

    // Check if we're in development mode
    let is_development = std::env::var("ENVIRONMENT")
        .unwrap_or_else(|_| "production".to_string())
        .to_lowercase()
        == "development";

    let protocol = if is_development { "http" } else { "https" };

    let redirect_url = if is_mobile_browser(&headers) {
        format!("{}://{}/oauth?token={}", protocol, domain, token)
    } else {
        let mut url = format!(
            "{}://{}/dashboard?auth=success&provider=discord",
            protocol, frontend_url
        );

        if let Some(state) = query.state {
            let parts: Vec<&str> = state.splitn(2, '-').collect();
            if parts.len() == 2 {
                let origin_value = parts[1];
                url = format!("{}{}", url, format!("&origin={}", origin_value));
            }
        }
        url
    };

    tracing::info!(
        "Discord OAuth callback completed successfully for: {}",
        user_profile.email
    );
    Ok(Redirect::to(&redirect_url))
}

#[tracing::instrument(name = "Discord login initiation")]
pub async fn discord_login(
    State(inner): State<InnerState>,
    Query(params): Query<AuthQueryParams>,
) -> Result<impl IntoResponse, AppError> {
    tracing::info!("Initiating Discord OAuth login");

    let auth_url = crate::api::v1::oauth::generate_auth_url(
        &inner.oauth_clients.discord,
        &OAuthProvider::Discord,
        params.origin,
    );

    tracing::debug!("Generated Discord auth URL: {}", auth_url);
    Ok(Redirect::to(&auth_url))
}

#[tracing::instrument(name = "Check Discord session", skip(inner, cookies))]
pub async fn check_discord_session(
    State(inner): State<InnerState>,
    cookies: Cookies,
) -> Result<Json<ApiResponse<SocialLoginSessionStatus>>, AppError> {
    tracing::info!("Checking Discord session status");

    let InnerState { db, .. } = inner;

    let auth_token = cookies
        .get("auth-token")
        .and_then(|cookie| Some(cookie.value().to_string()))
        .ok_or_else(|| {
            tracing::warn!("No auth token found in cookies");
            AppError::Authentication(anyhow::anyhow!("No authentication token"))
        })?;

    let email = crate::api::v1::user::get_email_from_token(auth_token).await?;

    tracing::info!("Checking Discord session for user: {}", email);

    let session: Option<Session> = sqlx::query_as::<_, Session>(
        "SELECT * FROM sessions WHERE user_id = (SELECT id FROM users WHERE email = $1 LIMIT 1) AND provider = 'discord'",
    )
    .bind(&email)
    .fetch_optional(&db)
    .await
    .map_err(|e| {
        tracing::error!("Failed to query session: {:?}", e);
        AppError::Database(anyhow::Error::new(e).context("SQLx operation failed"))
    })?;

    match session {
        Some(session) => {
            let is_expired = session.expires_at < chrono::Utc::now();
            tracing::info!(
                "Discord session found for user: {}, expired: {}",
                email,
                is_expired
            );

            Ok(Json(ApiResponse::success(SocialLoginSessionStatus {
                connected: true,
                provider: "discord".to_string(),
                expired: is_expired,
                expires_at: session.expires_at.to_rfc3339(),
            })))
        }
        None => {
            tracing::info!("No Discord session found for user: {}", email);
            Ok(Json(ApiResponse::error(
                "No Discord session found".to_string(),
            )))
        }
    }
}

#[tracing::instrument(name = "Disconnect Discord session", skip(inner, cookies))]
pub async fn disconnect_discord(
    State(inner): State<InnerState>,
    cookies: Cookies,
) -> Result<Json<ApiResponse<String>>, AppError> {
    tracing::info!("Disconnecting Discord session");

    let InnerState { db, .. } = inner;

    let auth_token = cookies
        .get("auth-token")
        .and_then(|cookie| Some(cookie.value().to_string()))
        .ok_or_else(|| {
            tracing::warn!("No auth token found in cookies");
            AppError::Authentication(anyhow::anyhow!("No authentication token"))
        })?;

    let email = crate::api::v1::user::get_email_from_token(auth_token).await?;

    sqlx::query(
        "DELETE FROM sessions WHERE user_id = (SELECT id FROM users WHERE email = $1 LIMIT 1) AND provider = 'discord'",
    )
    .bind(&email)
    .execute(&db)
    .await
    .map_err(|e| {
        tracing::error!("Failed to delete Discord session: {:?}", e);
        AppError::Database(anyhow::Error::new(e).context("SQLx operation failed"))
    })?;

    tracing::info!("Discord session disconnected for user: {}", email);
    Ok(Json(ApiResponse::success(
        "Discord session disconnected successfully".to_string(),
    )))
}
