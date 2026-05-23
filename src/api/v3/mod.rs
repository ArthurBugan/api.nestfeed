pub mod entities;
pub mod animes;
pub mod channels;
pub mod groups;
pub mod sales;
pub mod services;
pub mod share_links;
pub mod users;
pub mod payments;
pub mod invoices;
pub mod blog;
pub mod proxy;
pub mod websites;

use axum::{middleware, Router};
use axum::routing::{delete, get, patch, post};
use tower_cookies::CookieManagerLayer;

use crate::InnerState;
use crate::api::common::middleware::auth_middleware;


/// Creates the V2 API router
#[tracing::instrument(name = "create_v3_router", skip(state))]
pub fn create_v3_router(state: InnerState) -> Router<InnerState> {
    tracing::info!("Creating V3 API router");
        
    Router::new()
        .route("/api/v3/health", get(|| async { "v3 health check ok!" }))
        .route("/api/v3/animes", get(animes::all_animes_v3))
        .route("/api/v3/animes/{id}", get(animes::get_anime_v3))
        .route("/api/v3/websites", get(websites::all_websites_v3))
        .route("/api/v3/groups", get(groups::all_groups_v3))
        .route("/api/v3/groups/shelf", get(groups::get_groupshelf_groups))
        .route("/api/v3/groups/shelf/copy/{group_id}", post(groups::copy_groupshelf_group))
        .route("/api/v3/groups/subgroups/{group_id}", get(groups::get_subgroups_by_channel))
        .route("/api/v3/groups/{group_id}/channels", post(groups::create_channel_in_group))
        .route("/api/v3/groups/{group_id}/videos", get(groups::get_group_videos))
        .route("/api/v3/groups/{group_id}/videos", delete(groups::delete_group_videos))
        .route("/api/v3/groups/{group_id}/videos/sync", post(groups::sync_group_videos))
        .route("/api/v3/channels/{channel_id}/batch", patch(channels::patch_channels_batch))
        .route("/api/v3/share-links", get(share_links::list_share_links))
        .route("/api/v3/share-links", post(share_links::create_share_link))
        .route("/api/v3/share-links/{share_link_id}", patch(share_links::update_share_link))
        .route("/api/v3/share-links/{share_link_id}", delete(share_links::delete_share_link))
        .route("/api/v3/me", get(users::me))
        .route("/api/v3/proxy/fetch-url", post(proxy::fetch_url_metadata))
        .route("/api/v3/payments", post(payments::create_checkout_session))
        .route("/api/v3/payments/cancel", post(payments::cancel_subscription))
        .route("/api/v3/invoices/history", get(invoices::get_invoice_history))
        .layer(CookieManagerLayer::new())
        .layer(middleware::from_fn(auth_middleware))
        .with_state(state)
}