mod authentication;
mod db;
mod email;
mod errors;

mod api;
mod system;

use crate::api::v1::routes::create_v1_routes;
use crate::api::v2::create_v2_router;
use crate::api::v3::create_v3_router;
use crate::email::EmailClient;

use crate::db::init_db;

use crate::system::create_system_router;
use crate::api::common::tracing::{make_custom_span, on_custom_request, on_custom_response, on_custom_failure};
use crate::api::common::body_logger::log_request_response_body;

use anyhow::Result;
use axum::extract::FromRef;
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderMap, HeaderName};
use axum::{Extension, Router};
use bytes::BytesMut;
use hyper::Method;
use sea_orm::{Database, DatabaseConnection};
use sqlx::PgPool;
use std::error::Error;
use time::Duration;
use tower::ServiceBuilder;
use tower_cookies::CookieManagerLayer;
use tower_http::cors::CorsLayer;
use tower_sessions::{Expiry, MemoryStore, SessionManagerLayer};

use std::sync::{Arc, RwLock};
use tower_http::trace::TraceLayer;
use tracing::Level;

use axum_otel::{AxumOtelOnFailure, AxumOtelOnResponse, AxumOtelSpanCreator};
use tower_http::request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer};
use tracing_otel_extra::{
    get_resource, init_env_filter, init_logger_provider, init_meter_provider, init_tracer_provider, init_tracing_subscriber
};

use opentelemetry::KeyValue;
use deadpool_redis::{Config as RedisConfig, Runtime};
use crate::api::common::cache::RedisCache;

struct AppState {
    inner: InnerState,
}

use crate::api::v1::oauth::{build_google_oauth_client, build_discord_oauth_client, build_apple_oauth_client, OAuthClients};

#[derive(Clone, Debug)]
struct InnerState {
    pub db: PgPool,
    pub sea_db: DatabaseConnection,
    pub email_client: EmailClient,
    pub oauth_clients: OAuthClients,
    pub redis_cache: RedisCache,
}

#[derive(Default)]
pub struct HeaderAppState {
    pub headers: HeaderMap,
    pub body: BytesMut,
}

impl FromRef<AppState> for InnerState {
    fn from_ref(app_state: &AppState) -> InnerState {
        app_state.inner.clone()
    }
}

pub type SharedState = Arc<RwLock<HeaderAppState>>;



#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();
    let service_name = "api-groupify";
    let resource = get_resource(service_name, &[KeyValue::new("environment", "production")]);

    let tracer_provider = init_tracer_provider(&resource, 1.0)?;
    let meter_provider = init_meter_provider(&resource, 30)?;
    let env_filter = init_env_filter(&Level::DEBUG);
    let logger_provider = init_logger_provider(&resource)?;

    let _guard = init_tracing_subscriber(
        service_name,
        env_filter,
        vec![Box::new(tracing_subscriber::fmt::layer())],
        tracer_provider,
        meter_provider,
        Some(logger_provider),
    )?;

    tracing::info!("Starting Groupify API server");

    let shared_state = Arc::new(RwLock::new(HeaderAppState::default()));

    let email_client = EmailClient::new(
        std::env::var("EMAIL_BASE_URL")?,
        std::env::var("EMAIL")?,
    );

    let database_url = std::env::var("DATABASE_URL")?;
    let db = init_db().await?;
    let sea_db = Database::connect(&database_url).await?;
    let cfg = RedisConfig::from_url(std::env::var("REDIS_URL").unwrap());
    let redis_pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();
    let redis_cache = RedisCache { pool: redis_pool };

    let session_store = MemoryStore::default();
    let session = SessionManagerLayer::new(session_store)
        .with_secure(false)
        .with_expiry(Expiry::OnInactivity(Duration::days(120)));

    let oauth_id = std::env::var("GOOGLE_OAUTH_CLIENT_ID")?;
    let oauth_secret = std::env::var("GOOGLE_OAUTH_CLIENT")?;

    let google_oauth_client = build_google_oauth_client(oauth_id.clone(), oauth_secret);
    let discord_oauth_client = build_discord_oauth_client(
        std::env::var("DISCORD_OAUTH_CLIENT_ID")?,
        std::env::var("DISCORD_OAUTH_CLIENT")?,
    );
    let apple_oauth_client = build_apple_oauth_client(
        std::env::var("APPLE_OAUTH_CLIENT_ID")?,
        std::env::var("APPLE_TEAM_ID")?,
        std::env::var("APPLE_KEY_ID")?,
        std::env::var("APPLE_PRIVATE_KEY")?,
    );

    let app_state = InnerState {
        db,
        sea_db,
        email_client,
        oauth_clients: OAuthClients {
            google: google_oauth_client,
            discord: discord_oauth_client,
            apple: apple_oauth_client,
        },
        redis_cache,
    };

   let origins = [
        "chrome-extension://dmdgaegnpjnnkcbdngfgkhlehlccbija".parse().unwrap(),
        "chrome-extension://jbifilepodgklfkblilibnbbbncjphde".parse().unwrap(),
        "https://localhost".parse().unwrap(),
        "http://localhost".parse().unwrap(),
        "http://localhost:3000".parse().unwrap(),
        "https://localhost:3000".parse().unwrap(),
        "https://nestfeed.app".parse().unwrap(),
        "https://coolify.nestfeed.app".parse().unwrap(),
        "https://www.youtube.com".parse().unwrap(),
        "https://youtube.com".parse().unwrap(),
    ];

    let cors = CorsLayer::new()
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::OPTIONS,
            Method::PUT,
            Method::DELETE,
            Method::PATCH,
        ])
        .allow_headers([CONTENT_TYPE, HeaderName::from_static("x-correlation-id"), HeaderName::from_static("x-request-id")])
        .allow_origin(origins)
        .allow_credentials(true);

    // Build the main application with versioned routes
    tracing::info!("Building application router with versioned routes");

    let app = Router::new()
        .merge(create_system_router(app_state.clone()).with_state(app_state.clone()))
        .merge(create_v1_routes(app_state.clone()).with_state(app_state.clone()))
        .merge(create_v2_router(app_state.clone()).with_state(app_state.clone()))
        .merge(create_v3_router(app_state.clone()).with_state(app_state.clone()))
        // Apply middleware layers
        .layer(axum::middleware::from_fn(log_request_response_body))
        .layer(cors)
        .layer(CookieManagerLayer::new())
        .layer(session)
        .layer(
            ServiceBuilder::new()
                .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
                .layer(
                    TraceLayer::new_for_http()
                        .make_span_with(make_custom_span)
                        .on_request(on_custom_request)
                        .on_response(on_custom_response)
                        .on_failure(on_custom_failure),
                )
                .layer(PropagateRequestIdLayer::x_request_id()),
        )
        .layer(
            ServiceBuilder::new()
                .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
                .layer(
                    TraceLayer::new_for_http()
                        .make_span_with(AxumOtelSpanCreator::new().level(Level::DEBUG))
                        .on_response(AxumOtelOnResponse::new().level(Level::DEBUG))
                        .on_failure(AxumOtelOnFailure::new().level(Level::ERROR)),
                )
                .layer(PropagateRequestIdLayer::x_request_id()),
        )
        .layer(Extension(shared_state));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3010")
        .await
        .expect("Could not initialize TcpListener");

    tracing::info!(
        "Server listening on {} with versioned API routes",
        listener
            .local_addr()
            .expect("Could not convert listener address to local address")
    );

    tracing::info!("Available API versions:");
    tracing::info!("  - Legacy routes: / (for backward compatibility)");
    tracing::info!("  - V2 API: /api/v2/* (coming soon)");
    tracing::info!("  - System: /health, /metrics");

    axum::serve(listener, app)
        .await
        .expect("Could not successfully connect");

    Ok(())
}
