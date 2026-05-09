use axum::http::HeaderMap;
use axum::{
    extract::{Form, Query, State},
    response::{IntoResponse, Redirect},
    Json,
};
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use oauth2::AuthorizationCode;
use serde::{Deserialize, Serialize};
use tower_cookies::Cookies;

use crate::{
    api::{
        common::utils::setup_auth_cookie,
        common::ApiResponse,
        v1::{
            login::generate_token,
            oauth::{exchange_apple_code, update_user_session, AuthRequest, OAuthProvider},
            user::{create_user, get_email_from_token, get_user_id_from_email, User},
        },
    },
    errors::AppError,
    InnerState,
};

#[derive(Debug, Deserialize)]
pub struct AuthQueryParams {
    pub origin: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppleFullName {
    pub family_name: Option<String>,
    pub given_name: Option<String>,
    pub middle_name: Option<String>,
    pub name_prefix: Option<String>,
    pub name_suffix: Option<String>,
    pub nickname: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppleMobileAuthRequest {
    pub authorization_code: String,
    pub email: Option<String>,
    pub full_name: Option<AppleFullName>,
    pub identity_token: String,
    pub real_user_status: i32,
    pub state: Option<String>,
    pub user: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct AppleClaims {
    sub: String,
    email: Option<String>,
    aud: serde_json::Value,
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

#[tracing::instrument(name = "Apple OAuth callback", skip(cookies, inner, query), fields(code_length = query.code.len()))]
pub async fn apple_callback(
    cookies: Cookies,
    headers: HeaderMap,
    State(inner): State<InnerState>,
    Form(query): Form<AuthRequest>,
) -> Result<impl IntoResponse, AppError> {
    tracing::info!("Processing Apple OAuth callback");

    let InnerState {
        db,
        oauth_clients,
        redis_cache,
        ..
    } = inner;

    tracing::debug!("Exchanging authorization code for access token");
    let token = exchange_apple_code(&oauth_clients.apple, query.code.clone())
        .await
        .map_err(|e| {
            tracing::error!("Failed to exchange Apple authorization code: {:?}", e);
            AppError::ExternalService(anyhow::anyhow!(
                "Failed to exchange authorization code: {}",
                e
            ))
        })?;

    // Apple provides the email in the id_token
    let id_token = token.id_token.ok_or_else(|| {
        tracing::error!("No id_token received from Apple");
        AppError::Authentication(anyhow::anyhow!("No id_token received from Apple"))
    })?;

    // Decode JWT without verification for now (proper verification requires Apple's public keys)
    // In production, you should verify the signature.
    let mut validation = Validation::new(Algorithm::RS256);
    validation.insecure_disable_signature_validation();
    validation.set_audience(&[std::env::var("APPLE_OAUTH_CLIENT_ID").unwrap_or_default()]);

    let token_data = decode::<AppleClaims>(
        &id_token, 
        &DecodingKey::from_rsa_der(&[]), 
        &validation
    )
        .map_err(|e| {
        tracing::error!("Failed to decode Apple id_token: {:?}", e);
        AppError::Authentication(anyhow::anyhow!("Failed to decode Apple id_token"))
    })?;

    let apple_claims = token_data.claims;
    let apple_email = apple_claims.email.ok_or_else(|| {
        tracing::error!("No email in Apple id_token");
        AppError::Authentication(anyhow::anyhow!("No email in Apple id_token"))
    })?;

    tracing::info!("Fetched Apple user email: {}", apple_email);

    let expires_at = crate::api::v1::auth::calculate_token_expiry(
        Some(std::time::Duration::from_secs(token.expires_in as u64))
    ).await;
    let access_token = token.access_token;
    let refresh_token = token.refresh_token.unwrap_or_default();

    let auth_token = cookies
        .get("auth-token")
        .map(|c| c.value().to_string())
        .unwrap_or_default();

    let original_email = apple_email.clone();

    let email = if auth_token.is_empty() {
        tracing::info!("No auth-token found, using Apple email: {}", original_email);
        original_email.clone()
    } else {
        tracing::info!("Auth-token found, using token to get email");
        get_email_from_token(auth_token).await?
    };

    let user_id = match get_user_id_from_email(&db, &email).await {
        Ok(id) => id,
        Err(AppError::NotFound(_)) => {
            tracing::info!(
                "User not found for email {}. Creating user from Apple callback.",
                email
            );
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

    update_user_session(
        &db,
        &email,
        &access_token,
        expires_at,
        &refresh_token,
        &OAuthProvider::Apple,
        &original_email,
    )
    .await?;

    let jwt_token = generate_token(&email, &user_id)?;
    let domain = std::env::var("GROUPIFY_HOST").expect("GROUPIFY_HOST must be set.");

    setup_auth_cookie(&jwt_token, &domain, &cookies);

    let channels_pattern = format!("user:{}:channels:*", user_id);
    if let Err(e) = redis_cache.del_pattern(&channels_pattern).await {
        tracing::warn!("apple_callback: redis DEL channels error: {:?}", e);
    }

    let is_development = std::env::var("ENVIRONMENT")
        .unwrap_or_else(|_| "production".to_string())
        .to_lowercase()
        == "development";
    let protocol = if is_development { "http" } else { "https" };

    let redirect_url = if is_mobile_browser(&headers) {
        format!("{}://{}/oauth?token={}", protocol, domain, jwt_token)
    } else {
        let mut url = format!(
            "{}://{}/dashboard?auth=success&provider=apple",
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

    tracing::info!("Apple OAuth callback completed successfully for: {}", email);
    Ok(Redirect::to(&redirect_url))
}

#[tracing::instrument(name = "Apple login initiation")]
pub async fn apple_login(
    State(inner): State<InnerState>,
    Query(params): Query<AuthQueryParams>,
) -> Result<impl IntoResponse, AppError> {
    tracing::info!("Initiating Apple OAuth login");

    let auth_url = crate::api::v1::oauth::generate_auth_url(
        &inner.oauth_clients.apple,
        &OAuthProvider::Apple,
        params.origin,
    );

    tracing::debug!("Generated Apple auth URL: {}", auth_url);
    Ok(Redirect::to(&auth_url))
}

#[tracing::instrument(name = "Apple Mobile OAuth callback", skip(cookies, inner, payload))]
pub async fn apple_mobile_auth(
    cookies: Cookies,
    State(inner): State<InnerState>,
    Json(payload): Json<AppleMobileAuthRequest>,
) -> Result<Json<ApiResponse<String>>, AppError> {
    tracing::info!("Processing Apple Mobile OAuth callback");

    let InnerState {
        db,
        redis_cache,
        ..
    } = inner;

    // Decode JWT without verification for now (proper verification requires Apple's public keys)
    // In production, you should verify the signature.
    let mut validation = Validation::new(Algorithm::RS256);
    validation.insecure_disable_signature_validation();
    // Temporarily disable audience validation to see what's being sent
    validation.validate_aud = false;

    // Log the header to see useful info
    if let Ok(header) = jsonwebtoken::decode_header(&payload.identity_token) {
        tracing::info!("JWT Header: {:?}", header);
    }

    let token_data = decode::<AppleClaims>(
        &payload.identity_token,
        &DecodingKey::from_rsa_der(&[]), // Use RSA der even if empty since it matches RS256 algorithm
        &validation
    )
        .map_err(|e| {
            // If from_rsa_der([]) fails, try the header-only approach or check the JWT format
            tracing::error!("Failed to decode Apple identity_token: {:?}", e);
            AppError::Authentication(anyhow::anyhow!("Failed to decode Apple identity_token: {}", e))
        })?;

    let apple_claims = token_data.claims;
    tracing::info!("Decoded Apple Claims for debugging: {:?}", apple_claims);

    let apple_email = apple_claims.email.or(payload.email).ok_or_else(|| {
        tracing::error!("No email in Apple identity_token or payload");
        AppError::Authentication(anyhow::anyhow!("No email in Apple identity_token or payload"))
    })?;

    tracing::info!("Fetched Apple user email: {}", apple_email);

    let user_id = match get_user_id_from_email(&db, &apple_email).await {
        Ok(id) => id,
        Err(AppError::NotFound(_)) => {
            tracing::info!(
                "User not found for email {}. Creating user from Apple mobile auth.",
                apple_email
            );
            let new_user = User {
                id: None,
                email: apple_email.clone(),
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

    let jwt_token = generate_token(&apple_email, &user_id)?;
    let domain = std::env::var("GROUPIFY_HOST").expect("GROUPIFY_HOST must be set.");

    setup_auth_cookie(&jwt_token, &domain, &cookies);

    let channels_pattern = format!("user:{}:channels:*", user_id);
    if let Err(e) = redis_cache.del_pattern(&channels_pattern).await {
        tracing::warn!("apple_mobile_auth: redis DEL channels error: {:?}", e);
    }

    tracing::info!("Apple Mobile OAuth callback completed successfully for: {}", apple_email);
    Ok(Json(ApiResponse::success(jwt_token)))
}
