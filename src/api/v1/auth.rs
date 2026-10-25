use axum::{
    extract::{Query, State},
    response::{IntoResponse, Redirect},
    Json,
};
use axum::http::HeaderMap;
use chrono::{Duration, Local};

use oauth2::{
    basic::BasicClient, reqwest::async_http_client, AuthorizationCode, RefreshToken,
    RequestTokenError, TokenResponse,
};
use serde_json::{json, Value};
use sqlx::{PgPool, Row};

use tower_cookies::Cookies;

use crate::{
    api::{common::{utils::setup_auth_cookie, ApiResponse}, v1::{
        discord_auth::SocialLoginSessionStatus, login::generate_token, oauth::{
            fetch_user_profile, generate_auth_url, update_user_session, AuthRequest, OAuthProvider,
            Session,
        }, user::{create_user, get_email_from_token, get_user_id_from_email, User}
    }},
    errors::AppError,
    InnerState,
};

use serde::Deserialize;

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

#[tracing::instrument(name = "Google OAuth callback", skip(inner, query), fields(code_length = query.code.len()))]
pub async fn google_callback(
    cookies: Cookies,
    headers: HeaderMap,
    State(inner): State<InnerState>,
    Query(query): Query<AuthRequest>,
) -> Result<impl IntoResponse, AppError> {
    tracing::info!("Processing Google OAuth callback");

    let InnerState {
        db,
        oauth_clients,
        redis_cache,
        http_client,
        ..
    } = inner;
    let domain = std::env::var("GROUPIFY_HOST").expect("GROUPIFY_HOST must be set.");

    tracing::debug!(
        "Domain: {}, Authorization code received (length: {})",
        domain,
        query.code.len()
    );

    tracing::debug!("Exchanging authorization code for access token");
    let token = oauth_clients
        .google
        .exchange_code(AuthorizationCode::new(query.code))
        .request_async(async_http_client)
        .await
        .map_err(|e| {
            tracing::error!("Failed to exchange authorization code: {:?}", e);
            tracing::error!("map error {:?}", e);
            match e {
                RequestTokenError::ServerResponse(server_response) => {
                    // Extract the error response from the server
                    let error_description = server_response.error_description();
                    let error_code = server_response.error();

                    tracing::error!("Server response error: {:?}", error_code);
                    println!("Server response error: {:?}", error_code);
                    if let Some(description) = error_description {
                        tracing::error!("Error description: {:?}", description);
                        println!("Error description: {:?}", description);
                    }
                }
                RequestTokenError::Request(request_error) => {
                    // This could be a network or other I/O error
                    tracing::error!("Request error during token exchange: {:?}", request_error);
                    println!("Request error: {:?}", request_error);
                }
                RequestTokenError::Parse(parse_error, response) => {
                    // Error occurred while parsing the response
                    tracing::error!("Parse error: {:?}, Response: {:?}", parse_error, response);
                    println!("Parse error: {:?}", parse_error);
                    println!("Response body: {:?}", response);
                }
                _ => {
                    // Fallback for other kinds of errors (if any)
                    tracing::error!("Unexpected OAuth error: {:?}", e);
                    println!("An unexpected error occurred: {:?}", e);
                }
            }

            AppError::ExternalService(anyhow::anyhow!("Failed to exchange authorization code"))
        })?;


    let auth_token = cookies
        .get("auth-token")
        .map(|c| c.value().to_string())
        .unwrap_or_default();

    tracing::info!("Successfully exchanged authorization code for token");
    tracing::info!("Passou do oauth {:?}", token);

    tracing::debug!("Fetching user profile from Google");
    let profile =
        fetch_user_profile(&token.access_token().secret(), &OAuthProvider::Google, &http_client).await?;

    tracing::info!("Retrieved user profile for email: {}", profile.email);

    let original_email = profile.email.clone();

    let email = if auth_token.is_empty() {
        tracing::info!("No auth-token found, using Google profile email: {}", original_email);
        original_email.clone()
    } else {
        tracing::info!("Auth-token found, using token to get email");
        get_email_from_token(auth_token).await?
    };

    let max_age = calculate_token_expiry(token.expires_in()).await;
    tracing::debug!("Calculated token expiry: {:?}", max_age);

    let refresh_token = token.refresh_token().ok_or_else(|| {
        tracing::error!("No refresh token received from OAuth provider");
        AppError::Validation(String::from("Token not found"))
    })?;

    let user_id = match get_user_id_from_email(&db, &email).await {
        Ok(id) => id,
        Err(AppError::NotFound(_)) => {
            tracing::info!("User not found for email {}. Creating user from Google callback.", email);
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

    tracing::debug!("Generating JWT token for user: {}", email);
    let access_token = generate_token(&email, &user_id)?;
    tracing::debug!("JWT token generated successfully");

    // Use the utility function instead of duplicated code
    setup_auth_cookie(&access_token, &domain, &cookies);

    update_user_session(
        &db,
        &email,
        token.access_token().secret(),
        max_age,
        refresh_token.secret(),
        &OAuthProvider::Google,
        &original_email,
    )
    .await?;

    let user_id = get_user_id_from_email(&db, &email).await?;
    let channels_pattern = format!("user:{}:channels:*", user_id);
    if let Err(e) = redis_cache.del_pattern(&channels_pattern).await {
        tracing::warn!("google_callback: redis DEL channels error: {:?}", e);
    }

        // Check if we're in development mode
    let is_development = std::env::var("ENVIRONMENT")
        .unwrap_or_else(|_| "production".to_string())
        .to_lowercase()
        == "development";

    let protocol = if is_development { "http" } else { "https" };

    let redirect_url = if is_mobile_browser(&headers) {
        format!("{}://{}/oauth?token={}", protocol, domain, access_token)
    } else {
        let mut url = format!(
            "{}://{}/dashboard/groups?auth=success&provider=google",
            protocol, domain
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

    tracing::info!("Redirecting user to: {}", redirect_url);

    Ok(Redirect::to(redirect_url.as_str()))
}

#[tracing::instrument(name = "Calculate token expiry", fields(expires_in_seconds = expires_in.as_ref().map(|d| d.as_secs())))]
pub async fn calculate_token_expiry(
    expires_in: Option<std::time::Duration>,
) -> chrono::NaiveDateTime {
    tracing::debug!("Calculating token expiry time");

    match expires_in {
        Some(secs) => {
            let duration = Duration::seconds(secs.as_secs().try_into().unwrap_or(0));
            let expiry = Local::now().naive_local() + duration;
            tracing::debug!(
                "Token will expire at: {:?} (in {} seconds)",
                expiry,
                secs.as_secs()
            );
            expiry
        }
        None => {
            let expiry = Local::now().naive_local();
            tracing::warn!("No expiry time provided, using current time: {:?}", expiry);
            expiry
        }
    }
}

#[tracing::instrument(name = "Renew OAuth token", skip(db, oauth_client), fields(email = %email))]
pub async fn renew_token(
    db: PgPool,
    oauth_client: BasicClient,
    email: String,
) -> Result<(), AppError> {
    tracing::info!("Renewing OAuth token for user: {}", email);

    tracing::debug!("Fetching refresh token from database");
    let refresh_token = fetch_refresh_token(&db, &email).await?;

    tracing::debug!("Exchanging refresh token for new access token");
    let token = oauth_client
        .exchange_refresh_token(&refresh_token)
        .request_async(async_http_client)
        .await
        .map_err(|err| {
            tracing::error!("Failed to exchange refresh token: {:?}", err);
            tracing::error!("Request error: {:?}", err);
            AppError::ExternalService(anyhow::anyhow!("Failed to exchange refresh token"))
        })?;

    tracing::info!("Successfully renewed token for user: {}", email);
    let max_age = calculate_token_expiry(token.expires_in()).await;

    tracing::debug!("Updating session with new token");
    update_user_session(
        &db,
        &email,
        token.access_token().secret(),
        max_age,
        refresh_token.secret(),
        &OAuthProvider::Google,
        &email,
    )
    .await?;

    tracing::info!("Token renewal completed successfully for user: {}", email);
    Ok(())
}

#[tracing::instrument(name = "Fetch refresh token", skip(db), fields(email = %email))]
async fn fetch_refresh_token(db: &sqlx::PgPool, email: &str) -> Result<RefreshToken, AppError> {
    tracing::debug!("Fetching refresh token from database for user: {}", email);

    let refresh_token: String = sqlx::query("SELECT refresh_token FROM sessions WHERE user_id = (SELECT id FROM users WHERE email = $1 LIMIT 1)")
      .bind(email)
      .fetch_one(db)
      .await
      .map_err(|e| {
          tracing::error!("Failed to fetch refresh token: {:?}", e);
          AppError::Database(anyhow::Error::new(e).context("SQLx operation failed"))
      })?
      .get("refresh_token");

    tracing::debug!("Successfully retrieved refresh token for user: {}", email);
    Ok(RefreshToken::new(refresh_token))
}

#[tracing::instrument(name = "Check Google session", skip(inner, cookies))]
pub async fn check_google_session(
    State(inner): State<InnerState>,
    cookies: Cookies,
) -> Result<Json<ApiResponse<SocialLoginSessionStatus>>, AppError> {
    tracing::info!("Checking Google session validity");

    let InnerState { db, .. } = inner;

    tracing::debug!("Extracting auth token from cookies");
    let auth_token = cookies
        .get("auth-token")
        .map(|c| c.value().to_string())
        .unwrap_or_default();

    if auth_token.clone().len() == 0 {
        tracing::warn!("No auth token found in cookies");
        return Err(AppError::Validation(String::from("No auth token found")));
    }

    tracing::debug!("Auth token found, extracting email");
    let email = get_email_from_token(auth_token).await;

    let email = match email {
        Ok(email) => {
            tracing::debug!("Successfully extracted email from token: {}", email);
            email
        }
        Err(e) => {
            tracing::error!("Failed to extract email from token: {:?}", e);
            return Err(AppError::Validation(String::from("Invalid auth token")));
        }
    };

    tracing::debug!("Checking session existence in database");
    let session: Option<Session> = sqlx::query_as::<_, Session>(
        "SELECT * FROM sessions WHERE user_id = (SELECT id FROM users WHERE email = $1 LIMIT 1) AND provider = 'google'",
    )
    .bind(&email)
    .fetch_optional(&db)
    .await
    .map_err(|e| {
        tracing::error!("Failed to query session: {:?}", e);
        AppError::Database(anyhow::Error::new(e).context("SQLx operation failed"))
    })?;

    tracing::debug!("Session query result: {:?}", session);
    tracing::info!("session {:?}", session);

    if session.is_none() {
        tracing::info!("No Google session found for user: {}", email);
        return Ok(Json(ApiResponse::error(
            "No Google session found".to_string(),
        )));
    }

    let session = session.unwrap();
    tracing::info!("Valid session found for user: {}", email);
    Ok(Json(ApiResponse::success(SocialLoginSessionStatus {
        connected: true,
        provider: "google".to_string(),
        expired: false,
        expires_at: session.expires_at.to_rfc3339(),
    })))
}

#[tracing::instrument(name = "Disconnect Google session", skip(inner, cookies))]
pub async fn disconnect_google(
    State(inner): State<InnerState>,
    cookies: Cookies,
) -> Result<Json<ApiResponse<String>>, AppError> {
    tracing::info!("Disconnecting Google session");

    let InnerState { db, .. } = inner;

    let auth_token = cookies
        .get("auth-token")
        .and_then(|cookie| Some(cookie.value().to_string()))
        .ok_or_else(|| {
            tracing::warn!("No auth token found in cookies");
            AppError::Authentication(anyhow::anyhow!("No authentication token"))
        })?;

    let email = get_email_from_token(auth_token).await?;

    sqlx::query(
        "DELETE FROM sessions WHERE user_id = (SELECT id FROM users WHERE email = $1 LIMIT 1) AND provider = 'google'",
    )
    .bind(&email)
    .execute(&db)
    .await
    .map_err(|e| {
        tracing::error!("Failed to delete Google session: {:?}", e);
        AppError::Database(anyhow::Error::new(e).context("SQLx operation failed"))
    })?;

    tracing::info!("Google session disconnected for user: {}", email);
    Ok(Json(ApiResponse::success(
        "Google session disconnected successfully".to_string(),
    )))
}

#[tracing::instrument(name = "Google login initiation")]
pub async fn google_login(
    State(inner): State<InnerState>,
    Query(params): Query<AuthQueryParams>,
) -> Result<impl IntoResponse, AppError> {
    tracing::info!("Initiating Google OAuth login");

    let auth_url = generate_auth_url(
        &inner.oauth_clients.google,
        &OAuthProvider::Google,
        params.origin,
    );

    tracing::debug!("Generated Google auth URL: {}", auth_url);
    Ok(Redirect::to(&auth_url))
}

#[tracing::instrument(name = "Get user email from token", skip(cookies))]
pub async fn me(
    cookies: Cookies,
) -> Result<Json<Value>, AppError> {
    tracing::info!("Getting user email from token");

    tracing::debug!("Extracting auth token from cookies");
    let auth_token = cookies
        .get("auth-token")
        .map(|c| c.value().to_string())
        .unwrap_or_default();

    if auth_token.is_empty() {
        tracing::warn!("No auth token found in cookies");
        return Err(AppError::Validation(String::from("No auth token found")));
    }

    tracing::debug!("Auth token found, extracting email");
    let email = get_email_from_token(auth_token).await;

    match email {
        Ok(email) => {
            tracing::debug!("Successfully extracted email from token: {}", email);
            Ok(Json(json!({ "email": email })))
        }
        Err(e) => {
            tracing::error!("Failed to extract email from token: {:?}", e);
            Err(AppError::Validation(String::from("Invalid auth token")))
        }
    }
}
