use axum::{extract::{Query, State}, Json};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use tracing::{debug, error, info, warn};

#[derive(Debug, Deserialize, Clone)]
pub struct BlogQueryParams {
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub category: Option<String>,
    #[serde(default)]
    pub page: Option<i32>,
    #[serde(default)]
    pub featured: Option<bool>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub search: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BlogPost {
    pub id: i32,
    pub status: String,
    pub sort: Option<i32>,
    pub date_created: String,
    pub date_updated: String,
    pub image: String,
    pub slug: String,
    pub title: String,
    pub description: String,
    #[serde(rename = "readTime")]
    pub read_time: String,
    pub category: String,
    pub featured: bool,
    pub content: String,
    pub author: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
pub struct BlogResponse {
    pub data: Vec<BlogPost>,
    pub total: usize,
}

/// Fetch blog posts from Directus CMS
#[tracing::instrument(name = "Get v3 get_blog_posts")]
pub async fn get_blog_posts(
    Query(params): Query<BlogQueryParams>,
    State(state): State<crate::InnerState>,
) -> Result<Json<BlogResponse>, axum::http::StatusCode> {
    let directus_url = std::env::var("DIRECTUS_URL")
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let client = state.http_client;
    let mut query_params = HashMap::new();

    // Build filter query for Directus
    let mut filters = Vec::new();

    if let Some(status) = params.status {
        filters.push(format!("{{\"status\":{{\"_eq\":\"{}\"}}}}", status));
    } else {
        filters.push(format!("{{\"status\":{{\"_eq\":\"published\"}}}}"));
    }

    if let Some(category) = params.category {
        filters.push(format!("{{\"category\":{{\"_eq\":\"{}\"}}}}", category));
    }

    if let Some(featured) = params.featured {
        filters.push(format!("{{\"featured\":{{\"_eq\":{}}}}}", featured));
    }

    // Add search filter if provided
    if let Some(search_term) = params.search {
        filters.push(format!(
            "{{\"_or\":[{{\"title\":{{\"_icontains\":\"{}\"}}}},{{\"description\":{{\"_icontains\":\"{}\"}}}}]}}",
            search_term, search_term
        ));
    }

    if !filters.is_empty() {
        query_params.insert("filter", format!("{{\"_and\":[{}]}}", filters.join(",")));
    }

    if let Some(limit) = params.limit {
        query_params.insert("limit", limit.to_string());
    }

    // Add pagination - Directus uses page parameter for pagination
    if let Some(page) = params.page {
        query_params.insert("page", page.to_string());
    }

    // Add sorting by date_created descending
    query_params.insert("sort", "-date_created".to_string());

    let response: reqwest::Response = client
        .get(format!("{}/items/posts", directus_url))
        .query(&query_params)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch blog posts from Directus: {}", e);
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        })?;

    if !response.status().is_success() {
        error!(
            "Directus API returned error status: {} for blog posts request. Query params: {:?}",
            response.status(),
            query_params
        );
        return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
    }

    let directus_response: Value = response.json().await.map_err(|e| {
        error!("Failed to parse Directus JSON response: {}", e);
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Extract data from Directus response
    let posts_data = directus_response["data"].as_array().ok_or_else(|| {
        error!(
            "Directus response missing or invalid 'data' field: {:?}",
            directus_response
        );
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let posts: Vec<BlogPost> = posts_data
        .iter()
        .filter_map(|post| {
            let mut blog_post: BlogPost = match serde_json::from_value(post.clone()) {
                Ok(post) => post,
                Err(e) => {
                    warn!(
                        "Failed to parse blog post from Directus response: {}. Post data: {:?}",
                        e, post
                    );
                    return None;
                }
            };

            // Prefix image URL with Directus assets endpoint
            if !blog_post.image.is_empty() && !blog_post.image.starts_with("http") {
                blog_post.image = format!(
                    "{}/assets/{}",
                    directus_url, blog_post.image
                );
            }

            Some(blog_post)
        })
        .collect();

    let total_count_response = client
        .get(format!("{}/items/posts?aggregate[count]=*", directus_url))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch filtered post count from Directus: {}", e);
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let total_count: usize = if total_count_response.status().is_success() {
        let count_data: Value = total_count_response.json().await.map_err(|e| {
            error!("Failed to parse total count JSON response: {}", e);
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        })?;

        info!("Total count data: {:?}", count_data);
        
        // Handle both string and number formats from Directus
        match count_data["data"][0]["count"].as_str() {
            Some(count_str) => count_str.parse().unwrap_or(0),
            None => count_data["data"][0]["count"]
                .as_u64()
                .unwrap_or(0) as usize,
        }
    } else {
        warn!("Failed to get total post count from Directus, using filtered count as fallback");
        posts.len()
    };

    Ok(Json(BlogResponse {
        data: posts,
        total: total_count,
    }))
}

/// Fetch a single blog post by slug from Directus CMS
pub async fn get_blog_post_by_slug(
    axum::extract::Path(slug): axum::extract::Path<String>,
    State(state): State<crate::InnerState>,
) -> Result<Json<BlogPost>, axum::http::StatusCode> {
    let directus_url = std::env::var("DIRECTUS_URL")
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let client = state.http_client;

    let response = client
        .get(format!(
            "{}/items/posts?filter[slug][_eq]={}",
            directus_url, slug
        ))
        .send()
        .await
        .map_err(|e| {
            error!(
                "Failed to fetch blog post by slug '{}' from Directus: {}",
                slug, e
            );
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        })?;

    if !response.status().is_success() {
        warn!(
            "Blog post with slug '{}' not found in Directus. Status: {}",
            slug,
            response.status()
        );
        return Err(axum::http::StatusCode::NOT_FOUND);
    }

    let directus_response: Value = response.json().await.map_err(|e| {
        error!(
            "Failed to parse Directus JSON response for slug '{}': {}",
            slug, e
        );
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let posts_data = directus_response["data"].as_array().ok_or_else(|| {
        error!(
            "Directus response missing or invalid 'data' field: {:?}",
            directus_response
        );
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let post_data = posts_data.first().ok_or_else(|| {
        error!(
            "Directus response for slug '{}' has no posts: {:?}",
            slug, directus_response
        );
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let mut blog_post: BlogPost = serde_json::from_value(post_data.clone()).map_err(|e| {
        error!(
            "Failed to parse blog post data for slug '{}': {}. Post data: {:?}",
            slug, e, post_data
        );
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Prefix image URL with Directus assets endpoint
    if !blog_post.image.is_empty() && !blog_post.image.starts_with("http") {
        blog_post.image = format!(
            "{}/assets/{}",
            directus_url, blog_post.image
        );
    }

    Ok(Json(blog_post))
}
