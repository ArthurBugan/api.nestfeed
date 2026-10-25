use axum::{extract::State, Json};
use serde::{Deserialize, Serialize};
use tracing::info;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use crate::{InnerState, api::common::ApiResponse, errors::AppError};
use tower_cookies::Cookies;
use crate::api::v1::user::get_user_id_from_token;

#[derive(Debug, Deserialize)]
pub struct FetchUrlRequest {
    pub url: String,
    pub group_id: Option<String>,
    pub content_type: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelMetadata {
    pub id: String,
    pub name: String,
    pub thumbnail: Option<String>,
    pub group_id: Option<String>,
    pub content_type: String,
    pub url: String,
    pub new_content: bool,
}

/// Fetch metadata from any URL
#[tracing::instrument(name = "Fetch URL metadata", skip(cookies, state))]
pub async fn fetch_url_metadata(
    cookies: Cookies,
    State(state): State<InnerState>,
    Json(payload): Json<FetchUrlRequest>,
) -> Result<Json<ApiResponse<ChannelMetadata>>, AppError> {
    let auth_token = cookies
        .get("auth-token")
        .map(|c| c.value().to_string())
        .unwrap_or_default();

    if auth_token.is_empty() {
        return Err(AppError::Authentication(anyhow::anyhow!("Missing token")));
    }

    let _user_id = get_user_id_from_token(auth_token).await?;

    let mut url = payload.url.clone();
    if !url.starts_with("http://") && !url.starts_with("https://") {
        url = format!("https://{}", url);
    }

    let client = &state.http_client;

    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|e| AppError::ExternalService(anyhow::anyhow!("Failed to fetch URL: {}", e)))?;

    let html = response
        .text()
        .await
        .map_err(|e| AppError::ExternalService(anyhow::anyhow!("Failed to read response: {}", e)))?;

    let title: String = extract_meta(&html, "title")
        .unwrap_or_else(|| "Unknown".to_string());

    let name: String = extract_meta(&html, "og:site_name")
        .unwrap_or_else(|| title.clone());

    let thumbnail: Option<String> = extract_best_image(&html, &url);

    let id = create_id_from_url(&url);

    Ok(Json(ApiResponse::success(ChannelMetadata {
        id,
        name,
        thumbnail,
        group_id: payload.group_id,
        content_type: payload.content_type.unwrap_or_else(|| "website".to_string()),
        url,
        new_content: false,
    })))
}

fn get_base_url(url: &str) -> String {
    if let Ok(parsed) = url::Url::parse(url) {
        let host = parsed.host_str().unwrap_or("");
        let scheme = parsed.scheme();
        if let Some(port) = parsed.port() {
            return format!("{}://{}:{}", scheme, host, port);
        }
        return format!("{}://{}", scheme, host);
    }
    url.to_string()
}

fn extract_meta(html: &str, name: &str) -> Option<String> {
    let lower = html.to_lowercase();
    let search = name.to_lowercase();
    
    if search == "title" {
        if let Some(start) = lower.find("<title>") {
            if let Some(end) = lower[start..].find("</title>") {
                let title_start = start + 7;
                return Some(html[title_start..title_start + end - 7].to_string());
            }
        }
        if let Some(start) = lower.find("<title ") {
            if let Some(end) = lower[start..].find("</title>") {
                let title_start = start + 7;
                return Some(html[title_start..title_start + end - 7].to_string());
            }
        }
        return None;
    }
    
    let patterns = vec![
        format!("property=\"{}\"", search),
        format!("property='{}'", search),
        format!("name=\"{}\"", search),
        format!("name='{}'", search),
    ];
    
    let mut search_start = 0;
    while search_start < lower.len() {
        if let Some(meta_pos) = lower[search_start..].find("<meta") {
            let abs_pos = search_start + meta_pos;
            let line_end = lower[abs_pos..].find('>').map(|e| e + abs_pos + 1).unwrap_or(lower.len());
            let tag = &lower[abs_pos..line_end.min(lower.len())];
            
            let mut has_target = false;
            for pattern in &patterns {
                if tag.contains(pattern) {
                    has_target = true;
                    break;
                }
            }
            
            if has_target {
                for quote in &["\"", "'"] {
                    let content_key = format!("content={}", quote);
                    if let Some(cs) = tag.find(&content_key) {
                        let after_content = &tag[cs + content_key.len()..];
                        if after_content.starts_with(quote) {
                            let inner_start = 1;
                            if let Some(inner_end) = after_content[inner_start..].find(quote) {
                                let value = &after_content[inner_start..inner_start + inner_end];
                                return Some(value.to_string());
                            }
                        }
                    }
                }
            }
            
            search_start = line_end;
        } else {
            break;
        }
    }
    None
}

fn extract_og_property(html: &str, property: &str) -> Option<String> {
    extract_meta(html, property)
}

fn create_id_from_url(url: &str) -> String {
    let mut hasher = DefaultHasher::new();
    url.hash(&mut hasher);
    format!("{:x}", hasher.finish())
}

fn extract_best_image(html: &str, url: &str) -> Option<String> {
    let base = get_base_url(url);
    let lower = html.to_lowercase();
    
    let image_meta_tags = vec!["og:image", "og:image:url", "twitter:image", "twitter:image:src"];
    
    for tag in image_meta_tags {
        if let Some(img) = extract_meta(html, tag) {
            if !img.is_empty() {
                let resolved = resolve_url(&base, &img);
                return Some(resolved);
            }
        }
    }
    
    Some(format!("{}/favicon.ico", base))
}

fn resolve_url(base: &str, path: &str) -> String {
    if path.starts_with("http://") || path.starts_with("https://") {
        return path.to_string();
    }
    if path.starts_with("//") {
        return format!("https:{}", path);
    }
    if path.starts_with('/') {
        return format!("{}{}", base, path);
    }
    format!("{}/{}", base, path)
}
