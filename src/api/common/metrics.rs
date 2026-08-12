//! OpenTelemetry HTTP metrics middleware.
//!
//! Records HTTP request metrics (request count, latency, body sizes, active
//! requests) into the global OpenTelemetry meter provider, which is set up in
//! `main.rs` via `init_meter_provider` and pushed to the configured OTLP
//! collector.

use axum::body::Body;
use axum::extract::MatchedPath;
use axum::http::{Request, header};
use axum::middleware::Next;
use axum::response::Response;
use once_cell::sync::Lazy;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram, UpDownCounter};
use opentelemetry::KeyValue;
use std::time::Instant;

static REQUESTS_TOTAL: Lazy<Counter<u64>> = Lazy::new(|| {
    global::meter("groupify-http")
        .u64_counter("http.server.requests")
        .with_description("Total number of HTTP server requests")
        .with_unit("{request}")
        .build()
});

static REQUEST_DURATION: Lazy<Histogram<f64>> = Lazy::new(|| {
    global::meter("groupify-http")
        .f64_histogram("http.server.duration")
        .with_description("Duration of HTTP server requests")
        .with_unit("s")
        .with_boundaries(vec![
            0.0, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0,
        ])
        .build()
});

static REQUEST_BODY_SIZE: Lazy<Histogram<u64>> = Lazy::new(|| {
    global::meter("groupify-http")
        .u64_histogram("http.server.request.body.size")
        .with_description("Size of HTTP server request bodies")
        .with_unit("By")
        .build()
});

static RESPONSE_BODY_SIZE: Lazy<Histogram<u64>> = Lazy::new(|| {
    global::meter("groupify-http")
        .u64_histogram("http.server.response.body.size")
        .with_description("Size of HTTP server response bodies")
        .with_unit("By")
        .build()
});

static ACTIVE_REQUESTS: Lazy<UpDownCounter<i64>> = Lazy::new(|| {
    global::meter("groupify-http")
        .i64_up_down_counter("http.server.active_requests")
        .with_description("Number of active HTTP server requests")
        .with_unit("{request}")
        .build()
});

fn content_length(headers: &Request<Body>) -> Option<u64> {
    headers
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
}

pub async fn http_metrics(request: Request<Body>, next: Next) -> Response {
    let start = Instant::now();

    let method = request.method().clone();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(|path| path.as_str().to_owned())
        .unwrap_or_else(|| "unmatched".to_owned());
    let request_body_size = content_length(&request);

    ACTIVE_REQUESTS.add(1, &[]);

    let response = next.run(request).await;

    ACTIVE_REQUESTS.add(-1, &[]);

    let status = response.status().as_u16() as i64;
    let attributes = [
        KeyValue::new("http.request.method", method.to_string()),
        KeyValue::new("http.response.status_code", status),
        KeyValue::new("http.route", route),
    ];

    REQUESTS_TOTAL.add(1, &attributes);
    REQUEST_DURATION.record(start.elapsed().as_secs_f64(), &attributes);

    if let Some(size) = request_body_size {
        REQUEST_BODY_SIZE.record(size, &attributes);
    }

    let response_body_size = response
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    if let Some(size) = response_body_size {
        RESPONSE_BODY_SIZE.record(size, &attributes);
    }

    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, body::Body, routing::get};
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
    use std::time::Duration;
    use tower::ServiceExt;

        fn metric_total(
        scope_metrics: &[&opentelemetry_sdk::metrics::data::ScopeMetrics],
        name: &str,
    ) -> u64 {
        let mut total = 0;
        for scope in scope_metrics {
            for metric in scope.metrics() {
                if metric.name() != name {
                    continue;
                }
                let sum = match metric.data() {
                    opentelemetry_sdk::metrics::data::AggregatedMetrics::U64(
                        opentelemetry_sdk::metrics::data::MetricData::Sum(sum),
                    ) => sum,
                    _ => continue,
                };
                for point in sum.data_points() {
                    total += point.value();
                }
            }
        }
        total
    }

    #[tokio::test]
    async fn records_http_server_metrics() {
        let exporter = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(exporter.clone())
            .with_interval(Duration::from_secs(3600))
            .build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        global::set_meter_provider(provider.clone());

        let app = Router::new()
            .route("/hello", get(|| async { "world" }))
            .layer(axum::middleware::from_fn(http_metrics));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/hello")
                    .header(header::CONTENT_LENGTH, 1)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 200);

        provider.force_flush().unwrap();

        let finished = exporter.get_finished_metrics().unwrap();
        let scope_metrics: Vec<_> = finished
            .iter()
            .flat_map(|rm| rm.scope_metrics())
            .collect();

        assert_eq!(metric_total(&scope_metrics, "http.server.requests"), 1);
        assert!(scope_metrics
            .iter()
            .any(|scope| scope.metrics().any(|m| m.name() == "http.server.duration")));
        assert_eq!(
            metric_total(&scope_metrics, "http.server.active_requests"),
            0
        );
    }
}
