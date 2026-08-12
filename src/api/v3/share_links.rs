use anyhow::Result;
use axum::{
    extract::{Path, Query, State},
    Json,
};
use chrono::{NaiveDateTime, Utc};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, DatabaseConnection, EntityTrait, FromQueryResult,
    JoinType, Order, PaginatorTrait, QueryFilter, QueryOrder, QuerySelect, RelationTrait, Set,
};
use serde::{Deserialize, Serialize};
use tower_cookies::Cookies;
use uuid::Uuid;

use crate::{
    api::{
        common::{ApiResponse, PaginatedResponse, PaginationInfo},
        v1::user::get_user_id_from_token,
        v3::entities::{group_members, groups, share_links},
    },
    errors::AppError,
    InnerState,
};

#[derive(Debug, Clone, Copy)]
pub enum GroupPermission {
    Owner,
    Editor,
    Admin,
    Member,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateShareLinkRequest {
    pub group_id: String,
    pub link_code: String,
    pub link_type: String,
    pub permission: Option<String>,
    pub expires_at: Option<NaiveDateTime>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateShareLinkRequest {
    pub link_code: Option<String>,
    pub link_type: Option<String>,
    pub permission: Option<String>,
    pub expires_at: Option<NaiveDateTime>,
}

#[derive(Debug, Serialize, Deserialize, FromQueryResult, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ShareLinkResponse {
    pub id: String,
    pub group_id: String,
    pub link_code: String,
    pub link_type: String,
    pub permission: Option<String>,
    pub created_at: Option<NaiveDateTime>,
    pub expires_at: Option<NaiveDateTime>,
    pub group_name: Option<String>,
    pub group_icon: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ListShareLinksParams {
    pub page: Option<u32>,
    pub limit: Option<u32>,
    pub search: Option<String>,
}

#[tracing::instrument(name = "List share links for user", skip(cookies, inner))]
pub async fn list_share_links(
    cookies: Cookies,
    State(inner): State<InnerState>,
    Query(params): Query<ListShareLinksParams>,
) -> Result<Json<PaginatedResponse<ShareLinkResponse>>, AppError> {
    let InnerState { sea_db, redis_cache, .. } = inner;

    let auth_token = cookies
        .get("auth-token")
        .map(|c| c.value().to_string())
        .unwrap_or_default();

    if auth_token.is_empty() {
        return Err(AppError::Authentication(anyhow::anyhow!("Missing token")));
    }

    let user_id = get_user_id_from_token(auth_token).await?;

    let page = params.page.unwrap_or(1).max(1);
    let limit = params.limit.unwrap_or(20).max(1).min(100);
    let offset = (page - 1) * limit;

    let cache_key = format!(
        "user:{}:share_links:{}:{}:{}",
        user_id,
        page,
        limit,
        params.search.clone().unwrap_or_default()
    );

    if let Ok(Some(cached)) = redis_cache
        .get_json::<PaginatedResponse<ShareLinkResponse>>(&cache_key)
        .await
    {
        return Ok(Json(cached));
    }

    let base_access = Condition::any()
        .add(groups::Column::UserId.eq(user_id.clone()))
        .add(group_members::Column::UserId.eq(user_id.clone()));

    // Count total records
    let mut count_q = share_links::Entity::find()
        .join(JoinType::LeftJoin, share_links::Relation::Groups.def())
        .join(JoinType::LeftJoin, groups::Relation::GroupMembers.def())
        .filter(base_access.clone());

    if let Some(search) = &params.search {
        if !search.trim().is_empty() {
            let s = format!("%{}%", search.trim());
            count_q = count_q.filter(
                Condition::any()
                    .add(share_links::Column::LinkCode.ilike(s.clone()))
                    .add(share_links::Column::LinkType.ilike(s.clone())),
            );
        }
    }

    let total_result_u64 = count_q
        .select_only()
        .column(share_links::Column::Id)
        .distinct()
        .count(&sea_db)
        .await
        .map_err(AppError::SeaORM)?;

    let total_result = total_result_u64.try_into().unwrap();
    let total_pages = ((total_result as f64) / (limit as f64)).ceil() as u32;
    let has_next = page < total_pages;
    let has_prev = page > 1;

    // Query data
    let mut data_q = share_links::Entity::find()
        .join(JoinType::LeftJoin, share_links::Relation::Groups.def())
        .join(JoinType::LeftJoin, groups::Relation::GroupMembers.def())
        .filter(base_access)
        .select_only()
        .column(share_links::Column::Id)
        .column(share_links::Column::GroupId)
        .column(share_links::Column::LinkCode)
        .column(share_links::Column::LinkType)
        .column(share_links::Column::Permission)
        .column(share_links::Column::CreatedAt)
        .column(share_links::Column::ExpiresAt)
        .column_as(groups::Column::Name, "group_name")
        .column_as(groups::Column::Icon, "group_icon")
        .limit(limit as u64)
        .offset(offset as u64)
        .order_by(share_links::Column::CreatedAt, Order::Desc);

    if let Some(search) = &params.search {
        if !search.trim().is_empty() {
            let s = format!("%{}%", search.trim());
            data_q = data_q.filter(
                Condition::any()
                    .add(share_links::Column::LinkCode.ilike(s.clone()))
                    .add(share_links::Column::LinkType.ilike(s.clone())),
            );
        }
    }

    let share_links_result: Vec<ShareLinkResponse> = data_q
        .into_model::<ShareLinkResponse>()
        .all(&sea_db)
        .await
        .map_err(AppError::SeaORM)?;

    let response = PaginatedResponse {
        data: share_links_result,
        pagination: PaginationInfo {
            page,
            limit,
            total: total_result,
            total_pages,
            has_next,
            has_prev,
        },
    };

    let _ = redis_cache
        .set_json(&cache_key, &response, 300)
        .await;

    Ok(Json(response))
}

#[tracing::instrument(name = "Create share link", skip(cookies, inner, payload))]
pub async fn create_share_link(
    cookies: Cookies,
    State(inner): State<InnerState>,
    Json(payload): Json<CreateShareLinkRequest>,
) -> Result<Json<ApiResponse<ShareLinkResponse>>, AppError> {
    let InnerState { sea_db, redis_cache, .. } = inner;

    let auth_token = cookies
        .get("auth-token")
        .map(|c| c.value().to_string())
        .unwrap_or_default();

    if auth_token.is_empty() {
        return Err(AppError::Authentication(anyhow::anyhow!("Missing token")));
    }

    let user_id = get_user_id_from_token(auth_token).await?;

    let permission = check_group_permission(&sea_db, &payload.group_id, &user_id).await?;

    match permission {
        GroupPermission::Owner | GroupPermission::Admin | GroupPermission::Editor => {}
        GroupPermission::Member => {
            return Err(AppError::Permission(anyhow::anyhow!(
                "You do not have permission to create share links in this group"
            )))
        }
    }

    let id = Uuid::new_v4().to_string();
    let created_at = Some(Utc::now().naive_utc());

    let new_share_link = share_links::ActiveModel {
        id: Set(id.clone()),
        group_id: Set(payload.group_id.clone()),
        link_code: Set(payload.link_code.clone()),
        link_type: Set(payload.link_type.clone()),
        permission: Set(payload.permission),
        created_at: Set(created_at),
        expires_at: Set(payload.expires_at),
        ..Default::default()
    };

    new_share_link.insert(&sea_db).await.map_err(AppError::SeaORM)?;

    let share_link_with_group = share_links::Entity::find()
        .filter(share_links::Column::Id.eq(id.clone()))
        .join(JoinType::LeftJoin, share_links::Relation::Groups.def())
        .select_only()
        .column(share_links::Column::Id)
        .column(share_links::Column::GroupId)
        .column(share_links::Column::LinkCode)
        .column(share_links::Column::LinkType)
        .column(share_links::Column::Permission)
        .column(share_links::Column::CreatedAt)
        .column(share_links::Column::ExpiresAt)
        .column_as(groups::Column::Name, "group_name")
        .column_as(groups::Column::Icon, "group_icon")
        .into_model::<ShareLinkResponse>()
        .one(&sea_db)
        .await
        .map_err(AppError::SeaORM)?
        .ok_or_else(|| AppError::NotFound("Failed to retrieve created share link".to_string()))?;

    redis_cache
        .del_pattern(&format!("user:{}:share_links:*", user_id))
        .await
        .ok();

    redis_cache
        .del_pattern(&format!("user:{}:group:*", user_id))
        .await
        .ok();

    Ok(Json(ApiResponse::success(share_link_with_group)))
}

#[tracing::instrument(name = "Update share link", skip(cookies, inner, payload))]
pub async fn update_share_link(
    cookies: Cookies,
    State(inner): State<InnerState>,
    Path(share_link_id): Path<String>,
    Json(payload): Json<UpdateShareLinkRequest>,
) -> Result<Json<ApiResponse<ShareLinkResponse>>, AppError> {
    let InnerState { sea_db, redis_cache, .. } = inner;

    let auth_token = cookies
        .get("auth-token")
        .map(|c| c.value().to_string())
        .unwrap_or_default();

    if auth_token.is_empty() {
        return Err(AppError::Authentication(anyhow::anyhow!("Missing token")));
    }

    let user_id = get_user_id_from_token(auth_token).await?;

    let existing = share_links::Entity::find()
        .filter(share_links::Column::Id.eq(share_link_id.clone()))
        .join(JoinType::LeftJoin, share_links::Relation::Groups.def())
        .join(JoinType::LeftJoin, groups::Relation::GroupMembers.def())
        .filter(
            Condition::any()
                .add(groups::Column::UserId.eq(user_id.clone()))
                .add(
                    Condition::all()
                        .add(group_members::Column::UserId.eq(user_id.clone()))
                        .add(group_members::Column::Role.is_in(vec!["admin", "editor"])),
                ),
        )
        .one(&sea_db)
        .await
        .map_err(AppError::SeaORM)?;

    let share_link = match existing {
        Some(sl) => sl,
        None => {
            return Err(AppError::Permission(anyhow::anyhow!(
                "You do not have permission to update this share link"
            )))
        }
    };

    let mut active: share_links::ActiveModel = share_link.into();

    if let Some(link_code) = payload.link_code {
        active.link_code = Set(link_code);
    }
    if let Some(link_type) = payload.link_type {
        active.link_type = Set(link_type);
    }
    if let Some(permission) = payload.permission {
        active.permission = Set(Some(permission));
    }
    if let Some(expires_at) = payload.expires_at {
        active.expires_at = Set(Some(expires_at));
    }

    active.update(&sea_db).await.map_err(AppError::SeaORM)?;

    let updated_share_link = share_links::Entity::find()
        .filter(share_links::Column::Id.eq(share_link_id.clone()))
        .join(JoinType::LeftJoin, share_links::Relation::Groups.def())
        .select_only()
        .column(share_links::Column::Id)
        .column(share_links::Column::GroupId)
        .column(share_links::Column::LinkCode)
        .column(share_links::Column::LinkType)
        .column(share_links::Column::Permission)
        .column(share_links::Column::CreatedAt)
        .column(share_links::Column::ExpiresAt)
        .column_as(groups::Column::Name, "group_name")
        .column_as(groups::Column::Icon, "group_icon")
        .into_model::<ShareLinkResponse>()
        .one(&sea_db)
        .await
        .map_err(AppError::SeaORM)?
        .ok_or_else(|| AppError::NotFound("Failed to retrieve updated share link".to_string()))?;

    redis_cache
        .del_pattern(&format!("user:{}:share_links:*", user_id))
        .await
        .ok();

    redis_cache
        .del_pattern(&format!("user:{}:group:*", user_id))
        .await
        .ok();

    Ok(Json(ApiResponse::success(updated_share_link)))
}

#[tracing::instrument(name = "Delete share link", skip(cookies, inner))]
pub async fn delete_share_link(
    cookies: Cookies,
    State(inner): State<InnerState>,
    Path(share_link_id): Path<String>,
) -> Result<Json<ApiResponse<String>>, AppError> {
    let InnerState { sea_db, redis_cache, .. } = inner;

    let auth_token = cookies
        .get("auth-token")
        .map(|c| c.value().to_string())
        .unwrap_or_default();

    if auth_token.is_empty() {
        return Err(AppError::Authentication(anyhow::anyhow!("Missing token")));
    }

    let user_id = get_user_id_from_token(auth_token).await?;

    let existing = share_links::Entity::find()
        .filter(share_links::Column::Id.eq(share_link_id.clone()))
        .join(JoinType::LeftJoin, share_links::Relation::Groups.def())
        .join(JoinType::LeftJoin, groups::Relation::GroupMembers.def())
        .filter(
            Condition::any()
                .add(groups::Column::UserId.eq(user_id.clone()))
                .add(
                    Condition::all()
                        .add(group_members::Column::UserId.eq(user_id.clone()))
                        .add(group_members::Column::Role.is_in(vec!["admin", "editor"])),
                ),
        )
        .one(&sea_db)
        .await
        .map_err(AppError::SeaORM)?;

    if existing.is_none() {
        return Err(AppError::Permission(anyhow::anyhow!(
            "You do not have permission to delete this share link"
        )));
    }

    let result = share_links::Entity::delete_many()
        .filter(share_links::Column::Id.eq(share_link_id.clone()))
        .exec(&sea_db)
        .await
        .map_err(AppError::SeaORM)?;

    if result.rows_affected == 0 {
        return Err(AppError::NotFound(format!(
            "Share link {} not found",
            share_link_id
        )));
    }

    redis_cache
        .del_pattern(&format!("user:{}:share_links:*", user_id))
        .await
        .ok();

    redis_cache
        .del_pattern(&format!("user:{}:group:*", user_id))
        .await
        .ok();

    Ok(Json(ApiResponse::success(format!(
        "Share link {} deleted successfully",
        share_link_id
    ))))
}

pub async fn check_group_permission(
    db: &DatabaseConnection,
    group_id: &str,
    user_id: &str,
) -> Result<GroupPermission, AppError> {
    let is_owner = groups::Entity::find()
        .filter(groups::Column::Id.eq(group_id))
        .filter(groups::Column::UserId.eq(user_id))
        .one(db)
        .await
        .map_err(AppError::SeaORM)?
        .is_some();

    if is_owner {
        return Ok(GroupPermission::Owner);
    }

    let is_admin = group_members::Entity::find()
        .filter(group_members::Column::GroupId.eq(group_id))
        .filter(group_members::Column::UserId.eq(user_id))
        .filter(group_members::Column::Role.is_in(["admin"]))
        .one(db)
        .await
        .map_err(AppError::SeaORM)?
        .is_some();

    if is_admin {
        return Ok(GroupPermission::Admin);
    }

    let is_editor = group_members::Entity::find()
        .filter(group_members::Column::GroupId.eq(group_id))
        .filter(group_members::Column::UserId.eq(user_id))
        .filter(group_members::Column::Role.is_in(["editor"]))
        .one(db)
        .await
        .map_err(AppError::SeaORM)?
        .is_some();

    if is_editor {
        return Ok(GroupPermission::Editor);
    }

    let is_member = group_members::Entity::find()
        .filter(group_members::Column::GroupId.eq(group_id))
        .filter(group_members::Column::UserId.eq(user_id))
        .one(db)
        .await
        .map_err(AppError::SeaORM)?
        .is_some();

    if is_member {
        return Ok(GroupPermission::Member);
    }

    Err(AppError::Permission(anyhow::anyhow!(
        "You do not have permission to access this group"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db;
    use crate::api::common::cache::RedisCache;
    use crate::email::EmailClient;
    use crate::api::v1::oauth::{build_google_oauth_client, build_discord_oauth_client, build_apple_oauth_client, OAuthClients};
    use deadpool_redis::{Config as RedisConfig, Runtime};
    use sea_orm::Database;

    async fn setup_test_state() -> InnerState {
        dotenvy::dotenv().ok();
        let db = init_db().await.expect("Failed to init DB");
        let sea_db = Database::connect(std::env::var("DATABASE_URL").expect("DATABASE_URL not set"))
            .await
            .expect("Failed to connect to SeaORM database");
        
        let redis_cfg = RedisConfig::from_url(std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string()));
        let redis_pool = redis_cfg.create_pool(Some(Runtime::Tokio1)).expect("Failed to create Redis pool");
        let redis_cache = RedisCache { pool: redis_pool };
        
        let email_client = EmailClient::new(
            std::env::var("EMAIL_BASE_URL").unwrap_or_else(|_| "http://localhost".to_string()),
            std::env::var("EMAIL").unwrap_or_else(|_| "test@test.com".to_string()),
        );
        
        let google_oauth_client = build_google_oauth_client(
            std::env::var("GOOGLE_OAUTH_CLIENT_ID").unwrap_or_else(|_| "test_id".to_string()),
            std::env::var("GOOGLE_OAUTH_CLIENT").unwrap_or_else(|_| "test_secret".to_string()),
        );
        let discord_oauth_client = build_discord_oauth_client(
            std::env::var("DISCORD_OAUTH_CLIENT_ID").unwrap_or_else(|_| "test_id".to_string()),
            std::env::var("DISCORD_OAUTH_CLIENT").unwrap_or_else(|_| "test_secret".to_string()),
        );
        
        let oauth_clients = OAuthClients {
            google: google_oauth_client,
            discord: discord_oauth_client,
            apple: build_apple_oauth_client(
                std::env::var("APPLE_OAUTH_CLIENT_ID").unwrap_or_else(|_| "test_id".to_string()),
                std::env::var("APPLE_TEAM_ID").unwrap_or_else(|_| "test_team".to_string()),
                std::env::var("APPLE_KEY_ID").unwrap_or_else(|_| "test_key".to_string()),
                std::env::var("APPLE_PRIVATE_KEY").unwrap_or_else(|_| "test_key".to_string()),
            ),
        };

        InnerState {
            db,
            sea_db,
            redis_cache,
            email_client,
            oauth_clients,
        }
    }

    /*
    #[tokio::test]
    async fn test_check_group_permission_owner() {
        let state = setup_test_state().await;
        
        // This test assumes a user owns a group
        // You need to replace with actual test data IDs
        let group_id = "test-group-id";
        let user_id = "test-user-id";
        
        let result = check_group_permission(&state.sea_db, group_id, user_id).await;
        
        // Assert based on your test data setup
        // If user owns the group, should return Owner
        // If not, will return error or different permission
    }

    #[tokio::test]
    async fn test_list_share_links_unauthorized() {
        let _state = setup_test_state().await;
        
        // Create mock cookies without auth token
        // This would require mocking the Cookies object
        // For now, this is a placeholder showing the test structure
    }

    #[tokio::test]
    async fn test_create_share_link_success() {
        let _state = setup_test_state().await;
        
        // Test creating a share link
        // Requires:
        // 1. A valid auth token in cookies
        // 2. User with Owner/Admin/Editor permission on the group
        // 3. Valid CreateShareLinkRequest payload
        
        let _payload = CreateShareLinkRequest {
            group_id: "test-group-id".to_string(),
            link_code: "test-code-123".to_string(),
            link_type: "view".to_string(),
            permission: Some("read".to_string()),
            expires_at: None,
        };
        
        // Create mock cookies with valid auth token
        // Call create_share_link function
        // Assert response contains created share link with correct data
    }

    #[tokio::test]
    async fn test_create_share_link_permission_denied() {
        let _state = setup_test_state().await;
        
        // Test that a member (not owner/admin/editor) cannot create share links
        // Should return AppError::Permission
    }

    #[tokio::test]
    async fn test_update_share_link_success() {
        let _state = setup_test_state().await;
        
        // Test updating an existing share link
        // Requires:
        // 1. Existing share link ID
        // 2. User with permission (owner/admin/editor of the group)
        // 3. UpdateShareLinkRequest with fields to update
        
        let _share_link_id = "existing-share-link-id".to_string();
        let _payload = UpdateShareLinkRequest {
            link_code: Some("updated-code".to_string()),
            link_type: None,
            permission: Some("write".to_string()),
            expires_at: None,
        };
        
        // Call update_share_link function
        // Assert response contains updated data
    }

    #[tokio::test]
    async fn test_update_share_link_not_found() {
        let _state = setup_test_state().await;
        
        // Test updating a non-existent share link
        // Should return AppError::NotFound or AppError::Permission
    }

    #[tokio::test]
    async fn test_delete_share_link_success() {
        let _state = setup_test_state().await;
        
        // Test deleting a share link
        // Requires:
        // 1. Existing share link ID
        // 2. User with permission (owner/admin/editor of the group)
        
        let _share_link_id = "share-link-to-delete".to_string();
        
        // Call delete_share_link function
        // Assert success message in response
        // Verify share link no longer exists in database
    }

    #[tokio::test]
    async fn test_delete_share_link_not_found() {
        let _state = setup_test_state().await;
        
        // Test deleting a non-existent share link
        // Should return AppError::NotFound
    }

    #[tokio::test]
    async fn test_list_share_links_pagination() {
        let _state = setup_test_state().await;
        
        // Test pagination parameters
        let _params = ListShareLinksParams {
            page: Some(1),
            limit: Some(10),
            search: None,
        };
        
        // Call list_share_links with pagination
        // Assert correct number of results returned
        // Assert cache is set correctly
    }

    #[tokio::test]
    async fn test_list_share_links_search() {
        let _state = setup_test_state().await;
        
        // Test search functionality
        let _params = ListShareLinksParams {
            page: Some(1),
            limit: Some(20),
            search: Some("test".to_string()),
        };
        
        // Call list_share_links with search
        // Assert only matching results returned
    }

    #[tokio::test]
    async fn test_cache_invalidation_on_create() {
        let _state = setup_test_state().await;
        
        // Test that creating a share link invalidates the cache
        // 1. List share links (should cache results)
        // 2. Create a new share link
        // 3. List share links again (should not use stale cache)
    }

    #[tokio::test]
    async fn test_cache_invalidation_on_update() {
        let _state = setup_test_state().await;
        
        // Test that updating a share link invalidates the cache
        // Similar to create test
    }

    #[tokio::test]
    async fn test_cache_invalidation_on_delete() {
        let _state = setup_test_state().await;
        
        // Test that deleting a share link invalidates the cache
        // Similar to create test
    }
    */
}
