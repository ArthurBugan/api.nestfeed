use axum::{Json, extract::State};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, error, info};
use chrono::{Utc};
use sea_orm::{ActiveModelTrait, ColumnTrait, EntityTrait, QueryFilter, Set, TransactionTrait};
use tower_cookies::Cookies;
use crate::{InnerState, api::common::ApiResponse, errors::AppError, api::{v1::user::get_user_id_from_token, v3::entities::{subscription_plans, subscription_plans_users, users}}};

#[derive(Debug, Deserialize, Serialize)]
pub struct CreateCheckoutSessionRequest {
    pub plan_name: String,
    pub user_id: String,
}

#[derive(Debug, Serialize, Clone)]
pub struct CheckoutSessionResponse {
    pub session_id: String,
    pub checkout_url: String,
}

// Dodo webhook types
#[derive(Debug, Deserialize, Serialize)]
pub struct DodoCustomer {
    pub customer_id: String,
    pub email: String,
    pub name: String,
    #[serde(default)]
    pub metadata: Option<HashMap<String, serde_json::Value>>,
    #[serde(default)]
    pub phone_number: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DodoSubscriptionData {
    pub subscription_id: String,
    pub status: String,
    pub created_at: String,
    #[serde(default)]
    pub expires_at: Option<String>,
    pub next_billing_date: String,
    pub product_id: String,
    pub quantity: i32,
    pub customer: DodoCustomer,
    #[serde(default)]
    pub metadata: Option<HashMap<String, serde_json::Value>>,
    #[serde(default)]
    pub custom_field_responses: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DodoWebhookPayload {
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(default)]
    pub business_id: Option<String>,
    pub data: DodoSubscriptionData,
    pub timestamp: String,
}

/// Create a Dodo checkout session for subscription plans
#[tracing::instrument(name = "Create Dodo checkout session", skip(inner))]
pub async fn create_checkout_session(
    State(inner): State<InnerState>,
    Json(payload): Json<CreateCheckoutSessionRequest>,
) -> Result<Json<ApiResponse<CheckoutSessionResponse>>, AppError> {
    info!("Creating checkout session for user: {} - plan: {}", payload.user_id, payload.plan_name);
    
    // Determine product ID based on plan name
    let product_id = match payload.plan_name.as_str() {
        "Basic" => "pdt_0NbiyF78XxSPn9p43kVNX",
        "Pro" => "pdt_0NbiyEerr9uUphnRBJ3ph",
        _ => {
            error!("Unknown plan name: {}", payload.plan_name);
            return Err(AppError::BadRequest(format!("Unknown plan: {}", payload.plan_name)));
        }
    };

    // Create Dodo client
    let client = reqwest::Client::new();
    
    // Prepare the request to Dodo API
    let request_body = serde_json::json!({
        "product_cart": [{
            "product_id": product_id,
            "quantity": 1
        }],
        "metadata": {
            "user_id": payload.user_id,
            "plan_name": payload.plan_name
        }
    });

    // Get Dodo API credentials from environment
    let dodo_api_key = std::env::var("DODO_API_KEY")
        .map_err(|_| AppError::BadRequest("DODO_API_KEY not set".to_string()))?;
    
    let environment = std::env::var("DODO_ENVIRONMENT")
        .unwrap_or_else(|_| "test_mode".to_string());

    let url = std::env::var("DODO_URL")
        .unwrap_or_else(|_| "test_mode".to_string());
    
    let checkout_url = std::env::var("DODO_CHECKOUT")
        .unwrap_or_else(|_| format!("{}{}", url, "/checkouts"));

    let response = client
        .post(format!("{}{}", url, "/checkouts"))
        .header("Authorization", format!("Bearer {}", dodo_api_key))
        .header("Content-Type", "application/json")
        .header("Dodo-Environment", environment)
        .json(&request_body)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to create Dodo checkout session: {}", e);
            AppError::BadRequest("Failed to create checkout session".to_string())
        })?;

    let response_status = response.status();
    if !response_status.is_success() {
        let error_text = response.text().await.unwrap_or_default();
        error!("Dodo API error: {} - {}", response_status, error_text);
        return Err(AppError::BadRequest("Failed to create checkout session with Dodo".to_string()));
    }

    let dodo_response: HashMap<String, serde_json::Value> = response.json().await.map_err(|e| {
        error!("Failed to parse Dodo response: {}", e);
        AppError::BadRequest("Failed to parse checkout session response".to_string())
    })?;

    debug!("Dodo checkout session response: {:?}", dodo_response);

    let session_id = dodo_response.get("session_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            error!("Session ID not found in Dodo response: {:?}", dodo_response);
            AppError::BadRequest("Session ID not found in response".to_string())
        })?
        .to_string();

    let checkout_url = format!("{}/session/{}", checkout_url, session_id);

    info!("Successfully created checkout session: {} for user: {}", session_id, payload.user_id);

    Ok(Json(ApiResponse::success(CheckoutSessionResponse {
        session_id,
        checkout_url,
    })))
}

/// Helper function to activate subscription from Dodo webhook data
#[tracing::instrument(name = "Activate subscription from Dodo webhook", skip(inner, payload))]
async fn activate_subscription_from_dodo_webhook(
    inner: &InnerState,
    payload: &DodoWebhookPayload,
) -> Result<(), AppError> {
    // Extract user_id from metadata
    let user_id = payload.data.metadata.as_ref()
        .and_then(|m| m.get("user_id"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            error!("User ID not found in Dodo webhook metadata: {:?}", payload.data.metadata);
            AppError::BadRequest("User ID not found in webhook metadata".to_string())
        })?;
    
    // Extract plan_name from metadata and map to database plan names
    let dodo_plan_name = payload.data.metadata.as_ref()
        .and_then(|m| m.get("plan_name"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            error!("Plan name not found in Dodo webhook metadata: {:?}", payload.data.metadata);
            AppError::BadRequest("Plan name not found in webhook metadata".to_string())
        })?;
    
    // Map Dodo plan names to database plan names
    let db_plan_name = match dodo_plan_name {
        "Basic" => "Groupify Basic Monthly",
        "Pro" => "Groupify Pro Monthly",
        _ => {
            error!("Unknown Dodo plan name: {}", dodo_plan_name);
            return Err(AppError::BadRequest(format!("Unknown plan: {}", dodo_plan_name)));
        }
    };
    
    info!("Activating subscription for user: {} - Dodo plan: {} -> DB plan: {}", user_id, dodo_plan_name, db_plan_name);
    
    let db = &inner.sea_db;
    let txn = db.begin().await.map_err(AppError::SeaORM)?;

    // 1. Find user
    let user = match users::Entity::find()
        .filter(users::Column::Id.eq(user_id))
        .one(&txn)
        .await
        .map_err(AppError::SeaORM)?
    {
        Some(u) => u,
        None => {
            error!("User not found for user_id: {}", user_id);
            return Err(AppError::NotFound(format!(
                "User not found for user_id: {}",
                user_id
            )));
        }
    };

    // 2. Find subscription plan
    let subscription_plan = match subscription_plans::Entity::find()
        .filter(subscription_plans::Column::Name.eq(db_plan_name))
        .one(&txn)
        .await
        .map_err(AppError::SeaORM)?
    {
        Some(p) => p,
        None => {
            error!("Subscription plan not found for DB plan: {}", db_plan_name);
            return Err(AppError::NotFound(format!(
                "Subscription plan not found for plan: {}",
                db_plan_name
            )));
        }
    };

    // 3. Check for existing active subscriptions and end them
    let existing_subscriptions = subscription_plans_users::Entity::find()
        .filter(subscription_plans_users::Column::UserId.eq(user.id.clone()))
        .filter(subscription_plans_users::Column::EndedAt.is_null()) // Only active subscriptions
        .all(&txn)
        .await
        .map_err(AppError::SeaORM)?;

    // End all existing active subscriptions
    for mut existing_sub in existing_subscriptions {
        info!("Ending existing subscription {} for user {}", existing_sub.id, user.id);
        
        let mut active_model: subscription_plans_users::ActiveModel = existing_sub.into();
        active_model.ended_at = Set(Some(Utc::now().fixed_offset()));
        
        active_model.update(&txn).await.map_err(AppError::SeaORM)?;
    }

    // 4. Create new subscription_plans_users entry
    info!(
        "Assigning plan {} to user {} from Dodo subscription",
        subscription_plan.name, user.email
    );
    

    let new_subscription_user = subscription_plans_users::ActiveModel {
        user_id: Set(user.id.clone()),
        subscription_plan_id: Set(subscription_plan.id),
        started_at: Set(Some(chrono::DateTime::parse_from_rfc3339(&payload.data.created_at)
            .map_err(|e| {
                error!("Failed to parse created_at timestamp: {}", e);
                AppError::BadRequest("Invalid timestamp format".to_string())
            })?)),
        ended_at: Set(Some(chrono::DateTime::parse_from_rfc3339(&payload.data.next_billing_date)
            .map_err(|e| {
                error!("Failed to parse next_billing_date timestamp: {}", e);
                AppError::BadRequest("Invalid next_billing_date format".to_string())
            })?)),
        created_at: Set(Some(Utc::now().fixed_offset())),
        updated_at: Set(Some(Utc::now().fixed_offset())),
        external_subscription_id: Set(Some(payload.data.subscription_id.clone())),
        external_customer_id: Set(Some(payload.data.customer.customer_id.clone())),
        ..Default::default()
    };

    new_subscription_user
        .insert(&txn)
        .await
        .map_err(AppError::SeaORM)?;

    txn.commit().await.map_err(AppError::SeaORM)?;
    
    info!("Subscription successfully activated for user {} from Dodo webhook", user_id);

    Ok(())
}

/// Handle Dodo webhook for subscription activation
#[tracing::instrument(name = "Handle Dodo subscription webhook", skip(inner))]
pub async fn handle_dodo_subscription_webhook(
    State(inner): State<InnerState>,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<ApiResponse<String>>, AppError> {
    let event_type = payload.get("type").and_then(|v| v.as_str()).unwrap_or("").to_string();
    info!("Received Dodo webhook event: {}", event_type);

    match event_type.as_str() {
        "subscription.active" | "subscription.renewed" => {
            let dodo_payload: DodoWebhookPayload = serde_json::from_value(payload)
                .map_err(|e| {
                    error!("Failed to deserialize {} payload: {}", event_type, e);
                    AppError::BadRequest("Invalid webhook payload".to_string())
                })?;
            activate_subscription_from_dodo_webhook(&inner, &dodo_payload).await?;
            Ok(Json(ApiResponse::success("Subscription processed successfully".to_string())))
        }
        "payment.succeeded" => {
            let data = payload.get("data").ok_or_else(|| {
                AppError::BadRequest("Missing data field in payment webhook".to_string())
            })?;

            let subscription_id = data.get("subscription_id").and_then(|v| v.as_str());
            let customer_id = data.get("customer").and_then(|c| c.get("customer_id")).and_then(|v| v.as_str());

            if let Some(sub_id) = subscription_id {
                if let Some(cus_id) = customer_id {
                    let db = &inner.sea_db;

                    if let Some(existing_sub) = subscription_plans_users::Entity::find()
                        .filter(subscription_plans_users::Column::ExternalSubscriptionId.eq(sub_id))
                        .one(db)
                        .await
                        .map_err(AppError::SeaORM)?
                    {
                        if existing_sub.external_customer_id.is_none() {
                            let mut active_model: subscription_plans_users::ActiveModel = existing_sub.into();
                            active_model.external_customer_id = Set(Some(cus_id.to_string()));
                            active_model.update(db).await.map_err(AppError::SeaORM)?;
                            info!("Updated external_customer_id for subscription {}", sub_id);
                        }
                    } else {
                        let user_id = data.get("metadata").and_then(|m| m.get("user_id")).and_then(|v| v.as_str());
                        let plan_name = data.get("metadata").and_then(|m| m.get("plan_name")).and_then(|v| v.as_str());

                        if let (Some(uid), Some(pname)) = (user_id, plan_name) {
                            let db_plan_name = match pname {
                                "Basic" => "Groupify Basic Monthly",
                                "Pro" => "Groupify Pro Monthly",
                                _ => {
                                    error!("Unknown plan name from payment webhook: {}", pname);
                                    return Err(AppError::BadRequest(format!("Unknown plan: {}", pname)));
                                }
                            };

                            let user = crate::api::v3::entities::users::Entity::find()
                                .filter(crate::api::v3::entities::users::Column::Id.eq(uid))
                                .one(db)
                                .await
                                .map_err(AppError::SeaORM)?
                                .ok_or_else(|| AppError::NotFound("User not found".to_string()))?;

                            let plan = crate::api::v3::entities::subscription_plans::Entity::find()
                                .filter(crate::api::v3::entities::subscription_plans::Column::Name.eq(db_plan_name))
                                .one(db)
                                .await
                                .map_err(AppError::SeaORM)?
                                .ok_or_else(|| AppError::NotFound("Plan not found".to_string()))?;

                            let created_at_str = data.get("created_at").and_then(|v| v.as_str()).unwrap_or("");
                            let created_at = chrono::DateTime::parse_from_rfc3339(created_at_str).ok();

                            let new_sub = subscription_plans_users::ActiveModel {
                                user_id: Set(user.id.clone()),
                                subscription_plan_id: Set(plan.id),
                                started_at: Set(created_at.map(|dt| dt.fixed_offset())),
                                ended_at: Set(None),
                                created_at: Set(Some(Utc::now().fixed_offset())),
                                updated_at: Set(Some(Utc::now().fixed_offset())),
                                external_subscription_id: Set(Some(sub_id.to_string())),
                                external_customer_id: Set(Some(cus_id.to_string())),
                                ..Default::default()
                            };

                            new_sub.insert(db).await.map_err(AppError::SeaORM)?;
                            info!("Created subscription from payment.succeeded webhook for user {}", uid);
                        }
                    }
                }
            }

            Ok(Json(ApiResponse::success("Payment processed".to_string())))
        }
        _ => {
            info!("Ignoring Dodo event type: {}", event_type);
            Ok(Json(ApiResponse::success("Event ignored".to_string())))
        }
    }
}

/// Cancel the user's subscription
#[tracing::instrument(name = "Cancel user subscription", skip(inner, cookies))]
pub async fn cancel_subscription(
    cookies: Cookies,
    State(inner): State<InnerState>,
) -> Result<Json<ApiResponse<String>>, AppError> {
    let db = &inner.sea_db;
    
    let auth_token = cookies
        .get("auth-token")
        .map(|c| c.value().to_string())
        .ok_or_else(|| {
            tracing::warn!("cancel_subscription: Missing authentication token");
            AppError::Authentication(anyhow::anyhow!("Missing auth token"))
        })?;

    let user_id = get_user_id_from_token(auth_token).await?;
    
    info!("Canceling subscription for user: {}", user_id);
    
    // Find active subscription
    let active_subscriptions = subscription_plans_users::Entity::find()
        .filter(subscription_plans_users::Column::UserId.eq(user_id.clone()))
        .filter(subscription_plans_users::Column::EndedAt.is_null())
        .all(db)
        .await
        .map_err(AppError::SeaORM)?;
    
    if active_subscriptions.is_empty() {
        return Ok(Json(ApiResponse::success("No active subscription to cancel".to_string())));
    }
    
    // Get the external subscription ID from the first active subscription
    let external_subscription_id = active_subscriptions
        .first()
        .and_then(|s| s.external_subscription_id.clone())
        .ok_or_else(|| {
            error!("No external subscription ID found for user: {}", user_id);
            AppError::BadRequest("No external subscription ID found".to_string())
        })?;
    
    info!("Calling Dodo API to cancel subscription: {}", external_subscription_id);
    
    // Call Dodo API to cancel subscription
    let client = reqwest::Client::new();
    
    let dodo_api_key = std::env::var("DODO_API_KEY")
        .map_err(|_| AppError::BadRequest("DODO_API_KEY not set".to_string()))?;
    
    let environment = std::env::var("DODO_ENVIRONMENT")
        .unwrap_or_else(|_| "test_mode".to_string());
    let environment_for_get = environment.clone();
    
    let url = std::env::var("DODO_URL")
        .unwrap_or_else(|_| "test_mode".to_string());

    let dodo_cancel_url = format!("{}/subscriptions/{}", url, external_subscription_id);
    
    let request_body = serde_json::json!({
        "cancel_at_next_billing_date": true
    });
    
    let response = client
        .patch(&dodo_cancel_url)
        .header("Authorization", format!("Bearer {}", dodo_api_key))
        .header("Content-Type", "application/json")
        .header("Dodo-Environment", environment)
        .json(&request_body)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to call Dodo cancel API: {}", e);
            AppError::ExternalService(anyhow::anyhow!("Failed to cancel subscription with Dodo"))
        })?;
    
    if !response.status().is_success() {
        let error_text = response.text().await.unwrap_or_default();
        error!("Dodo cancel API error: {}", error_text);
        return Err(AppError::ExternalService(anyhow::anyhow!(
            "Failed to cancel subscription: {}",
            error_text
        )));
    }
    
    info!("Dodo API subscription canceled successfully");
    
    // Fetch subscription details from Dodo to get the expiration date
    let dodo_get_url = format!("{}/subscriptions/{}", url, external_subscription_id);
    
    let get_response = client
        .get(&dodo_get_url)
        .header("Authorization", format!("Bearer {}", dodo_api_key))
        .header("Content-Type", "application/json")
        .header("Dodo-Environment", environment_for_get)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to call Dodo get subscription API: {}", e);
            AppError::ExternalService(anyhow::anyhow!("Failed to get subscription details from Dodo"))
        })?;
    
    let subscription_details: serde_json::Value = get_response.json().await.map_err(|e| {
        error!("Failed to parse Dodo get subscription response: {}", e);
        AppError::ExternalService(anyhow::anyhow!("Failed to parse subscription details"))
    })?;

    info!("Dodo get subscription response: {:?}", subscription_details);
    
    let expires_at = subscription_details
        .get("next_billing_date")
        .and_then(|v: &serde_json::Value| v.as_str())
        .ok_or_else(|| {
            error!("expires_at not found in Dodo response: {:?}", subscription_details);
            AppError::BadRequest("expires_at not found in Dodo response".to_string())
        })?;
    
    let expires_at_date = chrono::DateTime::parse_from_rfc3339(expires_at)
        .map_err(|e| {
            error!("Failed to parse expires_at: {}", e);
            AppError::BadRequest("Invalid expires_at format".to_string())
        })?;
    
    // End all active subscriptions in local database with the expiration date
    for mut subscription in active_subscriptions {
        info!("Ending local subscription {} for user {} at {}", subscription.id, user_id, expires_at);
        
        let mut active_model: subscription_plans_users::ActiveModel = subscription.into();
        active_model.ended_at = Set(Some(expires_at_date));
        active_model.update(db).await.map_err(AppError::SeaORM)?;
    }
    
    info!("Successfully canceled subscription for user: {}", user_id);
    
    Ok(Json(ApiResponse::success("Subscription canceled successfully".to_string())))
}