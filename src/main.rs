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
use crate::api::common::metrics::http_metrics;

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

use tower_http::request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer};
use tracing_otel_extra::{
    get_resource, init_env_filter, init_logger_provider, init_meter_provider, init_tracing_subscriber
};

use opentelemetry::global;
use opentelemetry::KeyValue;
use opentelemetry_otlp::SpanExporter;
use opentelemetry_sdk::trace::{RandomIdGenerator, Sampler, SdkTracerProvider};
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

fn otlp_env_var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.trim().is_empty())
}

fn traces_endpoint_from_env() -> Option<String> {
    if let Some(endpoint) = otlp_env_var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
        .or_else(|| otlp_env_var("OTEL_EXPORTER_OTLP_ENDPOINT"))
    {
        return Some(endpoint);
    }
    otlp_env_var("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
        .or_else(|| otlp_env_var("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"))
        .map(|endpoint| match endpoint.rsplit_once("/v1/") {
            Some((base, _)) => format!("{base}/v1/traces"),
            None => endpoint,
        })
}

fn traces_protocol_from_env() -> String {
    otlp_env_var("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")
        .or_else(|| otlp_env_var("OTEL_EXPORTER_OTLP_PROTOCOL"))
        .or_else(|| otlp_env_var("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL"))
        .or_else(|| otlp_env_var("OTEL_EXPORTER_OTLP_LOGS_PROTOCOL"))
        .unwrap_or_else(|| "grpc".to_string())
        .trim()
        .to_ascii_lowercase()
}



#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();
    let service_name = "api-groupify";
    let resource = get_resource(service_name, &[KeyValue::new("environment", "production")]);

    global::set_text_map_propagator(opentelemetry_sdk::propagation::TraceContextPropagator::new());

    use opentelemetry_otlp::WithExportConfig as _;

    let span_exporter_endpoint = traces_endpoint_from_env();
    let span_exporter_protocol = traces_protocol_from_env();
    let span_exporter = if span_exporter_protocol.starts_with("http") {
        if span_exporter_protocol == "http/json" {
            tracing::warn!(
                "protocole http/json is not supported by opentelemetry-otlp build features, falling back to http/protobuf"
            );
        }
        let builder = SpanExporter::builder()
            .with_http()
            .with_protocol(opentelemetry_otlp::Protocol::HttpBinary);
        let builder = match span_exporter_endpoint {
            Some(ref endpoint) => builder.with_endpoint(endpoint.clone()),
            None => builder,
        };
        builder.build()?
    } else {
        let builder = SpanExporter::builder().with_tonic();
        let builder = match span_exporter_endpoint {
            Some(ref endpoint) => builder.with_endpoint(endpoint.clone()),
            None => builder,
        };
        builder.build()?
    };
    tracing::info!(
        endpoint = span_exporter_endpoint.as_deref().unwrap_or("default (localhost:4317)"),
        protocol = span_exporter_protocol.as_str(),
        "OTLP span exporter initialized"
    );

    let tracer_provider = SdkTracerProvider::builder()
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(1.0))))
        .with_id_generator(RandomIdGenerator::default())
        .with_resource(resource.clone())
        .with_batch_exporter(span_exporter)
        .build();
    global::set_tracer_provider(tracer_provider.clone());

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
        .layer(axum::middleware::from_fn(http_metrics))
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

#[cfg(test)]
mod tests {
    use opentelemetry::global;
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::{InMemorySpanExporterBuilder, SdkTracerProvider};
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::Registry;

    #[tokio::test]
    async fn exports_trace_spans() {
        let exporter = InMemorySpanExporterBuilder::new().build();
        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter.clone())
            .build();
        global::set_tracer_provider(provider.clone());

        let subscriber = Registry::default().with(
            tracing_otel_extra::otel::tracing_opentelemetry::OpenTelemetryLayer::new(
                provider.tracer("api-groupify"),
            ),
        );

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("http_request", method = "GET", uri = "/api/v1/test");
            let _guard = span.enter();
            tracing::info!(status = 200, "request complete");
        });

        provider.force_flush().unwrap();

        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].name, "http_request");
    }
}
