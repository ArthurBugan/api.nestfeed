use axum::{Json, extract::State};
use serde::Serialize;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use tower_cookies::Cookies;
use tracing::error;

use crate::{
    InnerState,
    api::{common::ApiResponse, v1::user::get_user_id_from_token, v3::entities::subscription_plans_users},
    errors::AppError,
};

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InvoiceItem {
    pub payment_id: String,
    pub total_amount: i32,
    pub currency: String,
    pub status: Option<String>,
    pub created_at: String,
    pub customer_name: String,
    pub customer_email: String,
    pub subscription_id: Option<String>,
    pub invoice_url: Option<String>,
    pub refund_status: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InvoicesResponse {
    pub items: Vec<InvoiceItem>,
}

#[tracing::instrument(name = "Get Dodo invoice history", skip(cookies, inner))]
pub async fn get_invoice_history(
    cookies: Cookies,
    State(inner): State<InnerState>,
) -> Result<Json<ApiResponse<InvoicesResponse>>, AppError> {
    let InnerState { sea_db, .. } = inner;

    let auth_token = cookies
        .get("auth-token")
        .map(|c| c.value().to_string())
        .unwrap_or_default();

    if auth_token.is_empty() {
        return Err(AppError::Authentication(anyhow::anyhow!("Missing token")));
    }

    let user_id = get_user_id_from_token(auth_token).await?;

    let dodo_api_key = std::env::var("DODO_API_KEY")
        .map_err(|_| AppError::BadRequest("DODO_API_KEY not set".to_string()))?;

    let environment = std::env::var("DODO_ENVIRONMENT")
        .unwrap_or_else(|_| "test_mode".to_string());

    let base_url = match environment.as_str() {
        "live_mode" => "https://live.dodopayments.com",
        _ => "https://test.dodopayments.com",
    };

    let active_sub = subscription_plans_users::Entity::find()
        .filter(subscription_plans_users::Column::UserId.eq(user_id.clone()))
        .filter(subscription_plans_users::Column::EndedAt.is_null())
        .one(&sea_db)
        .await
        .map_err(AppError::SeaORM)?;

    let customer_id = active_sub
        .as_ref()
        .and_then(|s| s.external_customer_id.clone());

    let items = if let Some(ref cus_id) = customer_id {
        let client = reqwest::Client::new();
        let response = client
            .get(format!("{}/payments", base_url))
            .header("Authorization", format!("Bearer {}", dodo_api_key))
            .header("Content-Type", "application/json")
            .header("Dodo-Environment", &environment)
            .query(&[("customer_id", cus_id)])
            .send()
            .await
            .map_err(|e| {
                error!("Failed to call Dodo payments API: {}", e);
                AppError::ExternalService(anyhow::anyhow!("Failed to fetch invoices from Dodo"))
            })?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            error!("Dodo payments API error: {}", error_text);
            return Err(AppError::ExternalService(anyhow::anyhow!(
                "Dodo API error: {}",
                error_text
            )));
        }

        let dodo_response: serde_json::Value = response.json().await.map_err(|e| {
            error!("Failed to parse Dodo payments response: {}", e);
            AppError::BadRequest("Failed to parse Dodo response".to_string())
        })?;

        dodo_response
            .get("items")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|item| InvoiceItem {
                        payment_id: item
                            .get("payment_id")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        total_amount: item
                            .get("total_amount")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(0) as i32,
                        currency: item
                            .get("currency")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        status: item
                            .get("status")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
                        created_at: item
                            .get("created_at")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        customer_name: item
                            .get("customer")
                            .and_then(|c| c.get("name"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        customer_email: item
                            .get("customer")
                            .and_then(|c| c.get("email"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        subscription_id: item
                            .get("subscription_id")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
                        invoice_url: item
                            .get("invoice_url")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
                        refund_status: item
                            .get("refund_status")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    } else {
        Vec::new()
    };

    Ok(Json(ApiResponse::success(InvoicesResponse { items })))
}
