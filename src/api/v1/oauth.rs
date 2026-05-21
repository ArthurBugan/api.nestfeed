use chrono::{DateTime, Utc};
use oauth2::{
    basic::BasicClient, AuthUrl, ClientId, ClientSecret, RedirectUrl, Scope, TokenResponse,
    TokenUrl,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::FromRow;
use time::OffsetDateTime;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use base64::{Engine as _, engine::general_purpose};

use crate::errors::AppError;

#[derive(Debug, Clone)]
pub enum OAuthProvider {
    Google,
    Discord,
    Apple,
}

impl OAuthProvider {
    pub fn as_str(&self) -> &'static str {
        match self {
            OAuthProvider::Google => "google",
            OAuthProvider::Discord => "discord",
            OAuthProvider::Apple => "apple",
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct AuthRequest {
    pub code: String,
    pub state: Option<String>,
}

#[derive(Debug, Deserialize, FromRow)]
pub struct Session {
    pub id: i32,
    pub user_id: String,
    pub session_id: String,
    pub refresh_token: String,
    pub expires_at: DateTime<Utc>,
    pub original_email: Option<String>,
}

#[derive(Deserialize, sqlx::FromRow, Clone)]
pub struct UserProfile {
    pub email: String,
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
}

#[derive(Clone, Debug)]
pub struct OAuthClients {
    pub google: BasicClient,
    pub discord: BasicClient,
    pub apple: BasicClient,
}

#[derive(Debug, Deserialize)]
pub struct AppleTokenResponse {
    pub access_token: String,
    pub token_type: String,
    pub expires_in: i64,
    pub refresh_token: Option<String>,
    pub id_token: Option<String>,
}

#[tracing::instrument(name = "Exchange Apple authorization code", skip(_client, code))]
pub async fn exchange_apple_code(
    _client: &BasicClient,
    code: String,
) -> Result<AppleTokenResponse, AppError> {
    let token_url = "https://appleid.apple.com/auth/token";

    let client_id = std::env::var("APPLE_OAUTH_CLIENT_ID")
        .map_err(|_| AppError::ExternalService(anyhow::anyhow!("APPLE_OAUTH_CLIENT_ID not set")))?;

    let team_id = std::env::var("APPLE_TEAM_ID")
        .map_err(|_| AppError::ExternalService(anyhow::anyhow!("APPLE_TEAM_ID not set")))?;

    let key_id = std::env::var("APPLE_KEY_ID")
        .map_err(|_| AppError::ExternalService(anyhow::anyhow!("APPLE_KEY_ID not set")))?;

    let private_key = std::env::var("APPLE_PRIVATE_KEY")
        .map_err(|_| AppError::ExternalService(anyhow::anyhow!("APPLE_PRIVATE_KEY not set")))?;

    let client_secret = generate_apple_client_secret(&team_id, &client_id, &key_id, &private_key)?;

    let redirect_url = std::env::var("APPLE_REDIRECT_URL")
        .unwrap_or_else(|_| "https://coolify.nestfeed.app/api/auth/apple_callback".to_string());

    let params = serde_json::json!({
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_url,
        "client_id": client_id,
        "client_secret": client_secret,
    });

    let http_client = Client::new();
    let response = http_client
        .post(token_url)
        .header("Content-Type", "application/json")
        .json(&params)
        .send()
        .await
        .map_err(|e| {
            tracing::error!("Failed to send Apple token request: {:?}", e);
            AppError::ExternalService(anyhow::anyhow!("Failed to exchange authorization code"))
        })?;

    let token_response: AppleTokenResponse = response.json().await.map_err(|e| {
        tracing::error!("Failed to parse Apple token response: {:?}", e);
        AppError::ExternalService(anyhow::anyhow!("Invalid token response"))
    })?;

    tracing::debug!("Successfully exchanged Apple authorization code");
    Ok(token_response)
}

#[tracing::instrument(name = "Build Google OAuth client", skip(client_id, client_secret))]
pub fn build_google_oauth_client(client_id: String, client_secret: String) -> BasicClient {
    tracing::info!("Building Google OAuth client");

    let redirect_url = std::env::var("GOOGLE_REDIRECT_URL")
        .unwrap_or_else(|_| "https://coolify.nestfeed.app/api/auth/google_callback".to_string());

    tracing::debug!("Using Google redirect URL: {}", redirect_url);

    BasicClient::new(
        ClientId::new(client_id),
        Some(ClientSecret::new(client_secret)),
        AuthUrl::new("https://accounts.google.com/o/oauth2/v2/auth".to_string())
            .expect("Invalid Google authorization endpoint URL"),
        Some(
            TokenUrl::new("https://www.googleapis.com/oauth2/v3/token".to_string())
                .expect("Invalid Google token endpoint URL"),
        ),
    )
    .set_redirect_uri(RedirectUrl::new(redirect_url).unwrap())
}

#[tracing::instrument(name = "Build Discord OAuth client", skip(client_id, client_secret))]
pub fn build_discord_oauth_client(client_id: String, client_secret: String) -> BasicClient {
    tracing::info!("Building Discord OAuth client");

    let redirect_url = std::env::var("DISCORD_REDIRECT_URL")
        .unwrap_or_else(|_| "https://coolify.nestfeed.app/api/auth/discord_callback".to_string());

    tracing::debug!("Using Discord redirect URL: {}", redirect_url);

    BasicClient::new(
        ClientId::new(client_id),
        Some(ClientSecret::new(client_secret)),
        AuthUrl::new("https://discord.com/api/oauth2/authorize".to_string())
            .expect("Invalid Discord authorization endpoint URL"),
        Some(
            TokenUrl::new("https://discord.com/api/oauth2/token".to_string())
                .expect("Invalid Discord token endpoint URL"),
        ),
    )
    .set_redirect_uri(RedirectUrl::new(redirect_url).unwrap())
}

#[derive(Debug, Serialize)]
struct AppleClientSecretClaims {
    iss: String,
    iat: i64,
    exp: i64,
    aud: String,
    sub: String,
}

#[tracing::instrument(name = "Generate Apple client secret", skip(private_key_base64))]
pub fn generate_apple_client_secret(
    team_id: &str,
    client_id: &str,
    key_id: &str,
    private_key_base64: &str,
) -> Result<String, AppError> {
    let iat = Utc::now().timestamp();
    let exp = iat + (180 * 24 * 60 * 60); // 180 days (max allowed by Apple)

    let claims = AppleClientSecretClaims {
        iss: team_id.to_string(),
        iat,
        exp,
        aud: "https://appleid.apple.com".to_string(),
        sub: client_id.to_string(),
    };

    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some(key_id.to_string());

    // Decode base64 private key
    let private_key_bytes = general_purpose::STANDARD
        .decode(private_key_base64)
        .map_err(|e| {
            tracing::error!("Failed to decode Apple private key from base64: {:?}", e);
            AppError::ExternalService(anyhow::anyhow!("Invalid base64 Apple private key: {}", e))
        })?;

    let key = EncodingKey::from_ec_pem(&private_key_bytes).map_err(|e| {
        tracing::error!("Failed to create encoding key from Apple private key: {:?}", e);
        AppError::ExternalService(anyhow::anyhow!("Invalid Apple private key PEM: {}", e))
    })?;

    encode(&header, &claims, &key).map_err(|e| {
        tracing::error!("Failed to encode Apple client secret JWT: {:?}", e);
        AppError::ExternalService(anyhow::anyhow!("Failed to generate Apple client secret: {}", e))
    })
}

#[tracing::instrument(name = "Build Apple OAuth client", skip(client_id, team_id, key_id, private_key))]
pub fn build_apple_oauth_client(
    client_id: String,
    team_id: String,
    key_id: String,
    private_key: String,
) -> BasicClient {
    tracing::info!("Building Apple OAuth client");

    let client_secret = generate_apple_client_secret(&team_id, &client_id, &key_id, &private_key)
        .expect("Failed to generate Apple client secret");

    let redirect_url = std::env::var("APPLE_REDIRECT_URL")
        .unwrap_or_else(|_| "https://coolify.nestfeed.app/api/auth/apple_callback".to_string());

    tracing::debug!("Using Apple redirect URL: {}", redirect_url);

    BasicClient::new(
        ClientId::new(client_id),
        Some(ClientSecret::new(client_secret)),
        AuthUrl::new("https://appleid.apple.com/auth/authorize".to_string())
            .expect("Invalid Apple authorization endpoint URL"),
        Some(
            TokenUrl::new("https://appleid.apple.com/auth/token".to_string())
                .expect("Invalid Apple token endpoint URL"),
        ),
    )
    .set_redirect_uri(RedirectUrl::new(redirect_url).unwrap())
}

#[tracing::instrument(name = "Generate OAuth authorization URL")]
pub fn generate_auth_url(
    client: &BasicClient,
    provider: &OAuthProvider,
    origin: Option<String>,
) -> String {
    let csrf_token = oauth2::CsrfToken::new_random();
    let state_with_origin = if let Some(origin_value) = origin {
        format!("{}-{}", csrf_token.secret(), origin_value)
    } else {
        csrf_token.secret().to_string()
    };

    let mut auth_request = client.authorize_url(|| oauth2::CsrfToken::new(state_with_origin));

    match provider {
        OAuthProvider::Google => {
            auth_request = auth_request
                .add_scope(Scope::new("email".to_string()))
                .add_scope(Scope::new("openid".to_string()))
                .add_scope(Scope::new("https://www.googleapis.com/auth/youtube.readonly".to_string()))
                .add_scope(Scope::new("profile".to_string()))
                .set_response_type(&oauth2::ResponseType::new("code".to_string()))
                .add_extra_param("prompt", "consent")
                .add_extra_param("access_type", "offline");
        }
        OAuthProvider::Discord => {
            auth_request = auth_request
                .add_scope(Scope::new("identify".to_string()))
                .add_scope(Scope::new("email".to_string()));
        }
        OAuthProvider::Apple => {
            auth_request = auth_request
                .add_scope(Scope::new("name".to_string()))
                .add_scope(Scope::new("email".to_string()))
                .add_extra_param("response_mode", "form_post");
        }
    }

    let (auth_url, _csrf_token) = auth_request.url();
    auth_url.to_string()
}

#[tracing::instrument(name = "Fetch user profile", skip(access_token), fields(provider = %provider.as_str()))]
pub async fn fetch_user_profile(
    access_token: &str,
    provider: &OAuthProvider,
) -> Result<UserProfile, AppError> {
    if let OAuthProvider::Apple = provider {
        // Apple profile is typically in the id_token, not a separate endpoint.
        // For now, we'll return a placeholder or handle it in the callback.
        return Err(AppError::ExternalService(anyhow::anyhow!("Apple profile must be extracted from id_token")));
    }
    let client = Client::new();

    let (url, auth_header) = match provider {
        OAuthProvider::Google => (
            "https://www.googleapis.com/oauth2/v2/userinfo",
            format!("Bearer {}", access_token),
        ),
        OAuthProvider::Discord => (
            "https://discord.com/api/users/@me",
            format!("Bearer {}", access_token),
        ),
        OAuthProvider::Apple => (
            "https://appleid.apple.com/auth/userinfo",
            format!("Bearer {}", access_token),
        ),
    };

    tracing::debug!("Fetching user profile from: {}", url);

    let response = client
        .get(url)
        .header("Authorization", auth_header)
        .send()
        .await
        .map_err(|e| {
            AppError::ExternalService(anyhow::anyhow!("Failed to fetch user profile: {}", e))
        })?;

    if !response.status().is_success() {
        tracing::error!("Failed to fetch user profile: HTTP {}", response.status());
        return Err(AppError::ExternalService(anyhow::anyhow!(
            "Failed to fetch user profile: HTTP {}",
            response.status()
        )));
    }

    let user_data: Value = response.json().await.map_err(|e| {
        AppError::ExternalService(anyhow::anyhow!("Failed to fetch user profile: {}", e))
    })?;

    tracing::debug!("Received user data: {:?}", user_data);

    let profile = match provider {
        OAuthProvider::Google => UserProfile {
            email: user_data["email"]
                .as_str()
                .ok_or_else(|| {
                    AppError::ExternalService(anyhow::anyhow!("No email in Google profile"))
                })?
                .to_string(),
            display_name: user_data["name"].as_str().map(|s| s.to_string()),
            avatar_url: user_data["picture"].as_str().map(|s| s.to_string()),
        },
        OAuthProvider::Discord => UserProfile {
            email: user_data["email"]
                .as_str()
                .ok_or_else(|| {
                    AppError::ExternalService(anyhow::anyhow!("No email in Discord profile"))
                })?
                .to_string(),
            display_name: user_data["username"].as_str().map(|s| s.to_string()),
            avatar_url: user_data["avatar"].as_str().map(|avatar_hash| {
                let user_id = user_data["id"].as_str().unwrap_or("0");
                format!(
                    "https://cdn.discordapp.com/avatars/{}/{}.png",
                    user_id, avatar_hash
                )
            }),
        },
        OAuthProvider::Apple => UserProfile {
            email: user_data["email"]
                .as_str()
                .ok_or_else(|| {
                    AppError::ExternalService(anyhow::anyhow!("No email in Apple profile"))
                })?
                .to_string(),
            display_name: None, // Apple UserInfo doesn't usually provide name
            avatar_url: None,
        },
    };

    tracing::info!("Successfully fetched profile for: {}", profile.email);
    Ok(profile)
}

#[tracing::instrument(name = "Update user session", skip(db, access_token, refresh_token), fields(email = %email, provider = %provider.as_str()))]
pub async fn update_user_session(
    db: &sqlx::PgPool,
    email: &str,
    access_token: &str,
    expires_at: chrono::NaiveDateTime,
    refresh_token: &str,
    provider: &OAuthProvider,
    original_email: &str,
) -> Result<(), AppError> {
    tracing::info!("Updating user session for provider: {}", provider.as_str());

    // First, get or create the user
    let user_id = get_or_create_user(db, email, provider).await?;

    let offset_dt = OffsetDateTime::from_unix_timestamp(
        expires_at.and_utc().timestamp()
    ).unwrap();

    let _ = sqlx::query!(
        r#"
        INSERT INTO sessions (user_id, session_id, refresh_token, expires_at, provider, original_email)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (user_id, provider) DO UPDATE
        SET 
            session_id = EXCLUDED.session_id,
            refresh_token = EXCLUDED.refresh_token,
            expires_at = EXCLUDED.expires_at
        "#,
        user_id,
        access_token,
        refresh_token,
        offset_dt,
        provider.as_str(),
        original_email,
    )
    .execute(db)
    .await
    .map_err(|e| AppError::Database(anyhow::anyhow!("Failed to upsert user session: {}", e)))?;

    tracing::info!("Successfully updated session for user: {}", email);
    Ok(())
}

#[tracing::instrument(name = "Get or create user", skip(db), fields(email = %email, provider = %provider.as_str()))]
async fn get_or_create_user(
    db: &sqlx::PgPool,
    email: &str,
    provider: &OAuthProvider,
) -> Result<String, AppError> {
    // Try to find existing user
    let user = sqlx::query_scalar::<_, String>("SELECT id FROM users WHERE email = $1")
        .bind(email)
        .fetch_optional(db)
        .await
        .map_err(|e| {
            AppError::Database(anyhow::Error::from(e).context("Failed to fetch user by email"))
        })?;

    if let Some(user_id) = user {
        tracing::debug!("Found existing user: {}", user_id);
        return Ok(user_id);
    }

    // Create new user
    let user_id = uuid::Uuid::new_v4().to_string();

    sqlx::query!(
        r#"
        INSERT INTO users (id, email, is_sso_user, created_at, updated_at)
        VALUES ($1, $2, true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        "#,
        user_id,
        email
    )
    .execute(db)
    .await
    .map_err(|e| AppError::Database(anyhow::Error::from(e).context("Failed to create user")))?;

    tracing::info!("Created new user: {} for email: {}", user_id, email);
    Ok(user_id)
}
