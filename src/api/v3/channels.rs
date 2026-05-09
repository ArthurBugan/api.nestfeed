use anyhow::Result;
use axum::{
    extract::{Path, State},
    Json,
};
use std::collections::HashSet;
use tower_cookies::Cookies;

use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, DatabaseConnection, EntityTrait, ExprTrait, 
    Iterable, QueryFilter, QuerySelect, RelationTrait, Set, TransactionTrait, JoinType
};
use sea_orm::sea_query::Expr;
use crate::api::common::limits::enforce_channel_addition_limit;

use crate::{
    api::{
        common::ApiResponse,
        v1::user::get_user_id_from_token,
        v2::channels::{ChannelWithGroup, PatchChannelsBatchRequest},
        v3::entities::{channels, group_members, groups},
    },
    errors::AppError,
    InnerState,
};

#[derive(Debug, Clone, Copy)]
pub enum GroupChannelPermission {
    Owner,
    Editor,
    Admin,
}

#[tracing::instrument(name = "Patch v3 multiple channels in batch (Optimized)", skip(cookies, inner))]
pub async fn patch_channels_batch(
    cookies: Cookies,
    State(inner): State<InnerState>,
    Path(group_id): Path<String>,
    Json(payload): Json<PatchChannelsBatchRequest>,
) -> Result<Json<ApiResponse<Vec<ChannelWithGroup>>>, AppError> {
    let InnerState {
        db,
        sea_db,
        redis_cache,
        ..
    } = inner;

    let auth_token = cookies
        .get("auth-token")
        .map(|c| c.value().to_string())
        .unwrap_or_default();

    if auth_token.is_empty() {
        return Err(AppError::Authentication(anyhow::anyhow!("Missing token")));
    }

    let user_id = get_user_id_from_token(auth_token).await?;
    let incoming_ids: Vec<String> = payload.channels.iter().map(|c| c.id.clone()).collect();

    let permission = check_group_channel_permission(&sea_db, &group_id, &user_id).await?;

    // Check permission
    match permission {
        GroupChannelPermission::Owner | GroupChannelPermission::Editor | GroupChannelPermission::Admin => {},
        _ => return Err(AppError::Permission(anyhow::anyhow!("Insufficient permissions"))),
    }

    enforce_channel_addition_limit(&db.clone(), &user_id, &group_id, payload.channels.len() as i64).await?;

    // Start transaction for atomic operations
    let txn = sea_db.begin().await.map_err(AppError::SeaORM)?;

    // Step 1: Delete missing channels (single batch delete)
    delete_missing_channels_by_group_id_txn(&txn, &group_id, &user_id, &incoming_ids).await?;

    // Step 2: Check which channels already exist with a single query
    let existing_channels: Vec<String> = channels::Entity::find()
        .select_only()
        .column(channels::Column::Id)
        .filter(channels::Column::Id.is_in(incoming_ids.clone()))
        .filter(channels::Column::GroupId.eq(&group_id))
        .filter(channels::Column::UserId.eq(&user_id))
        .into_tuple()
        .all(&txn)
        .await
        .map_err(AppError::SeaORM)?;

    let existing_ids_set: HashSet<String> = existing_channels.into_iter().collect();

    // Step 3: Split channels into existing (to update) and new (to insert)
    let (existing_channels_to_update, new_channels_to_insert): (Vec<_>, Vec<_>) = payload
        .channels
        .into_iter()
        .partition(|req| existing_ids_set.contains(&req.id));

    // Step 4: Update existing channels using update_many for each field change
    // Since each channel can have different values, we need to update each one individually
    // But we do it all within the transaction for atomicity
    for req in existing_channels_to_update {
        let cleaned_url = req.url.map(|u| u.replace("@", ""));
        
        let update_result = channels::Entity::update_many()
            .col_expr(channels::Column::Name, Expr::value(req.name.unwrap_or_default()))
            .col_expr(channels::Column::Thumbnail, Expr::value(req.thumbnail.unwrap_or_default()))
            .col_expr(channels::Column::NewContent, Expr::value(Some(req.new_content.unwrap_or(false))))
            .col_expr(channels::Column::Url, Expr::value(cleaned_url))
            .col_expr(channels::Column::ContentType, Expr::value(req.content_type))
            .col_expr(channels::Column::GroupId, Expr::value(req.group_id))
            .filter(channels::Column::Id.eq(&req.id))
            .filter(channels::Column::GroupId.eq(&group_id))
            .filter(channels::Column::UserId.eq(&user_id))
            .exec(&txn)
            .await
            .map_err(AppError::SeaORM)?;

        tracing::debug!("Updated channel {}: {} rows affected", req.id, update_result.rows_affected);
    }

    // Step 5: Insert new channels in batch
    if !new_channels_to_insert.clone().is_empty() {
        let new_active_models: Vec<channels::ActiveModel> = new_channels_to_insert.clone()
            .into_iter()
            .map(|req| {
                let channel_id = req.id.clone();
                let cleaned_url = req.url.map(|u| u.replace("@", ""));
                
                channels::ActiveModel {
                    id: Set(channel_id.clone()),
                    user_id: Set(user_id.clone()),
                    group_id: Set(req.group_id),
                    name: Set(req.name.unwrap_or_default()),
                    thumbnail: Set(req.thumbnail.unwrap_or_default()),
                    channel_id: Set(Some(format!("{}/{}", user_id, channel_id))),
                    new_content: Set(Some(req.new_content.unwrap_or(false))),
                    url: Set(cleaned_url),
                    content_type: Set(req.content_type),
                    ..Default::default()
                }
            })
            .collect();

        channels::Entity::insert_many(new_active_models)
            .exec(&txn)
            .await
            .map_err(AppError::SeaORM)?;
        
        tracing::info!("Inserted {} new channels", new_channels_to_insert.clone().len());
    }

    // Step 6: Commit transaction
    txn.commit().await.map_err(AppError::SeaORM)?;

    // Step 7: Fetch all updated channels in a single query
    let updated_channels: Vec<ChannelWithGroup> = channels::Entity::find()
        .filter(channels::Column::Id.is_in(incoming_ids))
        .filter(channels::Column::GroupId.eq(group_id))
        .filter(channels::Column::UserId.eq(user_id.clone()))
        .join(
            JoinType::LeftJoin,
            channels::Relation::Groups.def(),
        )
        .select_only()
        .columns(channels::Column::iter())
        .column_as(groups::Column::Name, "group_name")
        .column_as(groups::Column::Icon, "group_icon")
        .into_model::<ChannelWithGroup>()
        .all(&sea_db)
        .await
        .map_err(AppError::SeaORM)?;

    // Step 8: Batch Redis cache invalidation using del_pattern
    let patterns = vec![
        format!("user:{}:group:*", user_id),
        format!("user:{}:channels:*", user_id),
        format!("user:{}:animes:*", user_id),
    ];
    
    for pattern in patterns {
        redis_cache.del_pattern(&pattern).await.map_err(|e| AppError::ExternalService(e))?;
    }

    Ok(Json(ApiResponse::success(updated_channels)))
}

#[tracing::instrument(name = "Delete missing channels in transaction", skip(txn, group_id, user_id, incoming_channel_ids))]
async fn delete_missing_channels_by_group_id_txn(
    txn: &sea_orm::DatabaseTransaction,
    group_id: &str,
    user_id: &str,
    incoming_channel_ids: &[String],
) -> Result<(), AppError> {
    // Get existing IDs with a more efficient query
    let existing_ids: Vec<String> = channels::Entity::find()
        .select_only()
        .column(channels::Column::Id)
        .filter(channels::Column::GroupId.eq(group_id))
        .filter(channels::Column::UserId.eq(user_id))
        .into_tuple()
        .all(txn)
        .await
        .map_err(AppError::SeaORM)?;

    let incoming: HashSet<&String> = incoming_channel_ids.iter().collect();

    let to_delete: Vec<String> = existing_ids
        .into_iter()
        .filter(|id| !incoming.contains(id))
        .collect();

    if !to_delete.is_empty() {
        tracing::info!(
            "Found {} stale channels to delete for group {}",
            to_delete.len(),
            group_id
        );
        
        let result = channels::Entity::delete_many()
            .filter(channels::Column::Id.is_in(to_delete))
            .filter(channels::Column::GroupId.eq(group_id))
            .exec(txn)
            .await
            .map_err(AppError::SeaORM)?;

        tracing::info!(
            "Deleted {} stale channels for group {}",
            result.rows_affected,
            group_id
        );
    }

    Ok(())
}

pub async fn check_group_channel_permission(
    db: &DatabaseConnection,
    group_id: &str,
    user_id: &str,
) -> Result<GroupChannelPermission, AppError> {
    // Use a single query with UNION to check all permissions at once
    let is_owner = groups::Entity::find()
        .filter(groups::Column::Id.eq(group_id))
        .filter(groups::Column::UserId.eq(user_id))
        .one(db)
        .await
        .map_err(AppError::SeaORM)?
        .is_some();

    if is_owner {
        return Ok(GroupChannelPermission::Owner);
    }

    // Batch permission check with is_in
    let member_record = group_members::Entity::find()
        .filter(group_members::Column::GroupId.eq(group_id))
        .filter(group_members::Column::UserId.eq(user_id))
        .filter(group_members::Column::Role.is_in(["editor", "admin"]))
        .one(db)
        .await
        .map_err(AppError::SeaORM)?;

    match member_record {
        Some(record) => {
            match record.role.as_str() {
                "editor" => Ok(GroupChannelPermission::Editor),
                "admin" => Ok(GroupChannelPermission::Admin),
                _ => Err(AppError::Permission(anyhow::anyhow!("Invalid role"))),
            }
        }
        None => Err(AppError::Permission(anyhow::anyhow!(
            "You do not have permission to manage channels in this group"
        ))),
    }
}
