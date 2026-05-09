use anyhow::Result;
use axum::extract::State;
use axum::Json;
use rand::distr::Alphanumeric;
use rand::*;
use serde_json::{json, Value};
use sqlx::{Executor, Postgres, Transaction};
use tower_cookies::Cookies;
use std::collections::HashMap;

use crate::api::common::ApiResponse;
use crate::api::common::utils::setup_auth_cookie;
use crate::api::v1::login::generate_token;
use crate::api::v1::user::{create_user, User};
use crate::InnerState;
use crate::errors::AppError; // Added

use crate::email::EmailClient;

#[tracing::instrument(name = "Subscribe new user", skip(inner, user), fields(user_email = %user.email))]
pub async fn subscribe(
    State(inner): State<InnerState>,
    cookies: Cookies,
    Json(user): Json<User>,
) -> Result<Json<ApiResponse<String>>,  AppError> {
    tracing::info!("Starting subscription process for user: {}", user.email);
    let InnerState {
        email_client, db, ..
    } = inner;

    tracing::debug!("Beginning database transaction");
    let mut transaction = db.begin().await?;

    tracing::debug!("Creating user record in database");
    let user_id = create_user(&mut transaction, user.clone()).await?;
    tracing::info!("User created with ID: {}", user_id);

    tracing::debug!("Generating subscription token");
    let subscription_token = generate_subscription_token();
    tracing::debug!("Subscription token generated with length: {}", subscription_token.len());

    tracing::debug!("Storing subscription token in database");
    store_token(&mut transaction, &user_id, &subscription_token).await?;

    tracing::debug!("Committing database transaction");
    transaction.commit().await?;
    tracing::info!("Database transaction committed successfully");

    tracing::debug!("Sending confirmation email to user: {}", user.email);
    let _resp = send_confirmation_email(&email_client, user.clone(), &subscription_token).await?;
    tracing::info!("Confirmation email sent successfully to: {}", user.email);

    let domain = std::env::var("GROUPIFY_HOST").map_err(|e| {
        tracing::error!("GROUPIFY_HOST environment variable not set: {:?}", e);
        AppError::Unexpected(anyhow::anyhow!(e).context("GROUPIFY_HOST env var not set"))
    })?;

    let access_token = generate_token(&user.email, &user_id)?;
    setup_auth_cookie(&access_token, &domain, &cookies);    
    Ok(Json(ApiResponse::success("Subscription process completed successfully".to_owned())))
}

#[tracing::instrument(name = "Generate subscription token")]
pub fn generate_subscription_token() -> String {
    tracing::debug!("Generating new subscription token");
    let mut rng = rand::rng();
    let token = std::iter::repeat_with(|| rng.sample(Alphanumeric))
        .map(char::from)
        .take(56)
        .collect();
    tracing::debug!("Subscription token generated successfully");
    token
}

#[tracing::instrument(
    name = "Send a confirmation email to a new subscriber",
    skip(email_client, user, subscription_token),
    fields(user_email = %user.email, subscription_token_length = subscription_token.len())
)]
pub async fn send_confirmation_email(
    email_client: &EmailClient,
    user: User,
    subscription_token: &str,
) -> Result<reqwest::Response, AppError> {
    tracing::info!("Preparing confirmation email for user: {}", user.email);
    
    let confirmation_link = format!(
        "{}/subscriptions/confirm/{}",
        &String::from("https://groupify.dev"),
        subscription_token
    );
    tracing::debug!("Generated confirmation link: {}", confirmation_link);

    let template_id = 2;
    tracing::debug!("Using email template ID: {}", template_id);

    let mut template_model = HashMap::new();
    template_model.insert("product_name".to_owned(), "Groupify".to_owned());
    template_model.insert("action_url".to_owned(), confirmation_link);
    template_model.insert("support_email".to_owned(), "admin@groupify.dev".to_owned());
    template_model.insert(
        "login_url".to_owned(),
        "https://groupify.dev/login".to_owned(),
    );

    tracing::debug!("Sending email via email client");
    let resp = email_client
        .send_email(&user.email, template_model, template_id)
        .await?;

    tracing::info!("Confirmation email sent successfully to: {}", user.email);
    Ok(resp)
}

#[tracing::instrument(
    name = "Store subscription token in the database",
    skip(subscription_token, transaction, subscriber_id),
    fields(subscriber_id = %subscriber_id, subscription_token_length = subscription_token.len())
)]
pub async fn store_token(
    transaction: &mut Transaction<'_, Postgres>,
    subscriber_id: &str,
    subscription_token: &str,
) -> Result<(), AppError> {
    tracing::info!("Storing subscription token for subscriber: {}", subscriber_id);
    
    tracing::debug!("Executing database update to store token");
    let query = sqlx::query_as::<_, User>(r#" UPDATE users SET confirmation_token = $1, updated_at = CURRENT_TIMESTAMP, confirmation_sent_at = CURRENT_TIMESTAMP WHERE id = $2"#)
        .bind(&subscription_token)
        .bind(subscriber_id);

    transaction.execute(query).await?;
    tracing::info!("Subscription token stored successfully for subscriber: {}", subscriber_id);
    Ok(())
}
