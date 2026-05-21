use crate::api::v1::subscriptions::generate_subscription_token;
use crate::api::v1::user::{
    get_password_confirmation_token_from_user, get_stored_credentials, User,
};
use crate::errors::AppError;
use anyhow::Context;
use std::collections::HashMap;

use crate::email::EmailClient;
use crate::InnerState;
use argon2::password_hash::SaltString;
use argon2::{Algorithm, Argon2, Params, PasswordHash, PasswordHasher, PasswordVerifier, Version};
use axum::extract::{Path, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::{Executor, PgPool, Postgres, Transaction};
use argon2::password_hash::rand_core::OsRng;

#[derive(Deserialize)]
pub struct Credentials {
    pub email: String,
    pub password: String,
}

#[derive(Deserialize)]
pub struct PasswordChange {
    pub password: String,
    pub password_confirmation: String,
}

#[tracing::instrument(name = "Validate user credentials", skip(credentials, pool), fields(email = %credentials.email))]
pub async fn validate_credentials(
    credentials: &Credentials,
    pool: &PgPool,
) -> Result<String, AppError> {
    tracing::info!(
        "Starting credential validation for user: {}",
        credentials.email
    );

    let mut user_id = None;
    let mut expected_password_hash = String::from(
        "$argon2id$v=19$m=15000,t=2,p=1$\
        gZiV/M1gPc22ElAH/Jh1Hw$\
        CWOrkoo7oJBQ/iyh7uJ0LO2aLEfrHwTWllSAxT0zRno"
            .to_string(),
    );

    tracing::debug!("Using default password hash as fallback to prevent timing attacks");

    tracing::debug!("Fetching stored credentials from database");
    match get_stored_credentials(&credentials.email, pool).await {
        Ok(user) => {
            // If the Result is Ok, user will contain the User
            tracing::debug!("Successfully retrieved user credentials from database");
            user_id = user.id;

            if user.encrypted_password.is_none() {
                tracing::warn!("User {} has no encrypted password", credentials.email);
                return Err(AppError::NotFound(
                    "Please use Google or Discord to login.".to_string(),
                ));
            }

            expected_password_hash = user.encrypted_password.unwrap();
            tracing::debug!("User found with ID: {:?}", user_id);
        }
        Err(error) => {
            // If the Result is Err, error will contain the Error
            tracing::warn!("Failed to retrieve user credentials: {:?}", error);
            tracing::info!("Error: {:?}", error);
        }
    }

    tracing::debug!("Verifying password hash");
    verify_password_hash(&expected_password_hash, &credentials.password)?;
    tracing::info!("Password verification successful");

    match user_id {
        Some(id) => {
            tracing::info!(
                "Credential validation successful for user: {}",
                credentials.email
            );
            Ok(id)
        }
        None => {
            tracing::warn!(
                "Credential validation failed - user not found: {}",
                credentials.email
            );
            Err(AppError::Authentication(anyhow::anyhow!(
                "Unknown username."
            )))
        }
    }
}

#[tracing::instrument(name = "Initiate password reset", skip(inner), fields(email = %user.email))]
pub async fn forget_password(
    State(inner): State<InnerState>,
    Json(user): Json<User>,
) -> Result<Json<Value>, AppError> {
    tracing::info!("Starting password reset process for user: {}", user.email);

    let InnerState {
        email_client, db, ..
    } = inner;

    tracing::debug!("Beginning database transaction");
    let mut transaction = db
        .begin()
        .await
        .context("Failed to begin database transaction.")?;

    tracing::debug!("Retrieving user credentials from database");
    let user_id_obj = get_stored_credentials(&user.email, &db).await?;
    tracing::debug!("User found with ID: {:?}", user_id_obj.id);

    tracing::debug!("Generating password reset token");
    let subscription_token = generate_subscription_token();
    tracing::debug!(
        "Password reset token generated (length: {})",
        subscription_token.len()
    );

    tracing::debug!("Storing reset token in database");
    store_token(&mut transaction, &user_id_obj.id, &subscription_token).await?;

    tracing::debug!("Committing database transaction");
    transaction
        .commit()
        .await
        .context("Failed to commit database transaction.")?;
    tracing::info!("Password reset token stored successfully");

    tracing::debug!("Sending password reset email");
    send_forget_password_email(&email_client, user.clone(), &subscription_token).await?;
    tracing::info!("Password reset email sent successfully to: {}", user.email);

    tracing::info!(
        "Password reset process completed successfully for user: {}",
        user.email
    );
    return Ok(Json(json!({ "success": true })));
}

#[tracing::instrument(
    name = "Send password reset email",
    skip(email_client, forget_password_token),
    fields(email = %user.email, token_length = forget_password_token.len())
)]
pub async fn send_forget_password_email(
    email_client: &EmailClient,
    user: User,
    forget_password_token: &str,
) -> Result<reqwest::Response, AppError> {
    tracing::info!("Preparing password reset email for user: {}", user.email);

    let confirmation_link = format!(
        "{}/forget-password/confirm/{}",
        &String::from("https://nestfeed.app"),
        forget_password_token
    );
    tracing::debug!("Generated confirmation link: {}", confirmation_link);

    let template_id = 3;
    tracing::debug!("Using email template ID: {}", template_id);

    tracing::debug!("Building email template model");
    let mut template_model = HashMap::new();
    template_model.insert("product_name".to_owned(), "Groupify".to_owned());
    template_model.insert("action_url".to_owned(), confirmation_link);
    template_model.insert("support_email".to_owned(), "admin@nestfeed.app".to_owned());
    template_model.insert(
        "login_url".to_owned(),
        "https://nestfeed.app/login".to_owned(),
    );

    tracing::debug!("Sending email via email client");
    let resp = email_client
        .send_email(&user.email, template_model, template_id)
        .await
        .context("Failed to send forget password email.")?;

    tracing::info!("Password reset email sent successfully to: {}", user.email);
    Ok(resp)
}

#[tracing::instrument(
    name = "Verify password hash",
    skip(expected_password_hash, password_candidate),
    fields(hash_length = expected_password_hash.len(), password_length = password_candidate.len())
)]
fn verify_password_hash(
    expected_password_hash: &str,
    password_candidate: &str,
) -> Result<(), AppError> {
    tracing::debug!("Starting password hash verification");
    tracing::debug!(
        "Expected hash length: {}, candidate password length: {}",
        expected_password_hash.len(),
        password_candidate.len()
    );

    tracing::debug!("Parsing password hash in PHC format");
    let expected_password_hash = PasswordHash::new(expected_password_hash)
        .context("Failed to parse hash in PHC string format.")?;

    tracing::debug!("Verifying password using Argon2");
    let result = Argon2::default()
        .verify_password(password_candidate.as_bytes(), &expected_password_hash)
        .context("Invalid password.")
        .map_err(AppError::Authentication);

    match result {
        Ok(_) => {
            tracing::debug!("Password verification successful");
            Ok(())
        }
        Err(ref e) => {
            tracing::warn!("Password verification failed: {:?}", e);
            result
        }
    }
}

#[tracing::instrument(
    name = "Change user password",
    skip(inner, password_change, forget_password_token),
    fields(token_length = forget_password_token.len())
)]
pub async fn change_password(
    State(inner): State<InnerState>,
    Path(forget_password_token): Path<String>,
    Json(password_change): Json<PasswordChange>,
) -> Result<Json<Value>, AppError> {
    tracing::info!("Starting password change process");
    tracing::debug!("Reset token length: {}", forget_password_token.len());

    let InnerState { db, .. } = inner;

    tracing::debug!("Validating password reset token");
    let subscriber_id =
        get_password_confirmation_token_from_user(&db, forget_password_token).await?;
    tracing::debug!(
        "Password reset token validated for user ID: {:?}",
        subscriber_id
    );

    tracing::debug!("Validating password confirmation match");
    if password_change.password != password_change.password_confirmation {
        tracing::warn!("Password confirmation mismatch");
        return Err(AppError::Validation(
            anyhow::anyhow!("Passwords are different").to_string(),
        ));
    }
    tracing::debug!("Password confirmation validated successfully");

    tracing::debug!("Computing new password hash");
    let password_hash = compute_password_hash(password_change.password).await?;
    tracing::debug!("New password hash computed successfully");

    tracing::debug!("Updating password in database");
    let result = sqlx::query_as::<_, User>(
        r#"UPDATE users
        SET encrypted_password = $1,
        updated_at = CURRENT_TIMESTAMP,
        recovery_token = null,
        recovery_sent_at = null
        WHERE id = $2
        "#,
    )
    .bind(&password_hash)
    .bind(&subscriber_id)
    .fetch_optional(&db)
    .await
    .context("Failed to update password in database.")?;

    match result {
        Some(_) => tracing::info!(
            "Password updated successfully for user ID: {:?}",
            subscriber_id
        ),
        None => tracing::warn!("No user found to update for ID: {:?}", subscriber_id),
    }

    let success = String::from("Password changed");
    tracing::info!("Password change process completed successfully");

    Ok(Json(json!({"data": success.to_string()})))
}

#[tracing::instrument(name = "Compute password hash", skip(password), fields(password_length = password.len()))]
pub async fn compute_password_hash(password: String) -> Result<String, AppError> {
    tracing::debug!("Starting password hash computation");
    tracing::debug!("Password length: {}", password.len());

    tracing::debug!("Generating random salt");
    let salt = SaltString::generate(OsRng);
    tracing::debug!("Salt generated successfully");

    tracing::debug!("Creating Argon2 parameters (memory: 15000, iterations: 2, parallelism: 1)");
    let params = Params::new(15000, 2, 1, None).map_err(|e| {
        tracing::error!("Failed to create Argon2 parameters: {:?}", e);
        AppError::Unexpected(anyhow::Error::new(e).context("Failed to create Argon2 params"))
    })?;

    tracing::debug!("Initializing Argon2 hasher with Argon2id algorithm");
    let hasher = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);

    tracing::debug!("Hashing password with Argon2");
    let password_hash = hasher
        .hash_password(password.as_bytes(), &salt)
        .map_err(|e| {
            tracing::error!("Failed to hash password: {:?}", e);
            AppError::Unexpected(anyhow::Error::new(e).context("Failed to hash password"))
        })?
        .to_string();

    tracing::debug!(
        "Password hash computed successfully (length: {})",
        password_hash.len()
    );
    tracing::info!("Password hash computation completed");
    Ok(password_hash)
}

#[tracing::instrument(
    name = "Store password reset token",
    skip(subscription_token, transaction),
    fields(subscriber_id = ?subscriber_id, token_length = subscription_token.len())
)]
pub async fn store_token(
    transaction: &mut Transaction<'_, Postgres>,
    subscriber_id: &Option<String>,
    subscription_token: &str,
) -> Result<(), AppError> {
    tracing::info!(
        "Storing password reset token for user ID: {:?}",
        subscriber_id
    );
    tracing::debug!("Token length: {}", subscription_token.len());

    tracing::debug!("Preparing database update query");
    let query = sqlx::query_as::<_, User>(r#" UPDATE users SET recovery_token = $1, recovery_sent_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE id = $2"#)
        .bind(&subscription_token)
        .bind(subscriber_id);

    tracing::debug!("Executing token storage query");
    let result = transaction
        .execute(query)
        .await
        .context("Failed to store token in database.")?;

    tracing::debug!(
        "Token storage query executed, rows affected: {}",
        result.rows_affected()
    );

    if result.rows_affected() == 0 {
        tracing::warn!(
            "No rows affected when storing token for user ID: {:?}",
            subscriber_id
        );
    } else {
        tracing::info!(
            "Password reset token stored successfully for user ID: {:?}",
            subscriber_id
        );
    }

    Ok(())
}
