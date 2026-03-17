// Licensed under Apache License, Version 2.0.

//! HTTP client wrapper for the Cassandra admin API.
//!
//! Provides a typed client that encapsulates base URL and common HTTP patterns
//! used by all nodetool-equivalent commands.

use anyhow::{Context, Result};
use serde_json::Value;

/// Client for the Cassandra admin HTTP API.
pub struct AdminClient {
    client: reqwest::blocking::Client,
    base_url: String,
}

impl AdminClient {
    /// Create a new admin client pointing at `http://{host}:{port}`.
    pub fn new(host: &str, port: u16) -> Self {
        Self {
            client: reqwest::blocking::Client::new(),
            base_url: format!("http://{}:{}", host, port),
        }
    }

    /// Base URL (e.g. `http://127.0.0.1:9090`).
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// GET a JSON response from `{base_url}{path}`.
    pub fn get(&self, path: &str) -> Result<Value> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self
            .client
            .get(&url)
            .send()
            .with_context(|| format!("Failed to connect to admin API at {}", url))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().unwrap_or_default();
            anyhow::bail!("HTTP {} from {}: {}", status, url, body);
        }

        resp.json::<Value>()
            .with_context(|| format!("Failed to parse JSON from {}", url))
    }

    /// GET raw text from `{base_url}{path}`.
    pub fn get_text(&self, path: &str) -> Result<String> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self
            .client
            .get(&url)
            .send()
            .with_context(|| format!("Failed to connect to admin API at {}", url))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().unwrap_or_default();
            anyhow::bail!("HTTP {} from {}: {}", status, url, body);
        }

        resp.text()
            .with_context(|| format!("Failed to read response from {}", url))
    }

    /// POST JSON to `{base_url}{path}` and return the response body.
    pub fn post_json(&self, path: &str, body: &Value) -> Result<Value> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self
            .client
            .post(&url)
            .json(body)
            .send()
            .with_context(|| format!("Failed to connect to admin API at {}", url))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let err_body = resp.text().unwrap_or_default();
            anyhow::bail!("HTTP {} from {}: {}", status, url, err_body);
        }

        resp.json::<Value>()
            .with_context(|| format!("Failed to parse JSON from {}", url))
    }

    /// POST with no body to `{base_url}{path}`.
    pub fn post_empty(&self, path: &str) -> Result<Value> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self
            .client
            .post(&url)
            .send()
            .with_context(|| format!("Failed to connect to admin API at {}", url))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let err_body = resp.text().unwrap_or_default();
            anyhow::bail!("HTTP {} from {}: {}", status, url, err_body);
        }

        resp.json::<Value>()
            .with_context(|| format!("Failed to parse JSON from {}", url))
    }

    /// DELETE to `{base_url}{path}`.
    pub fn delete(&self, path: &str) -> Result<Value> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self
            .client
            .delete(&url)
            .send()
            .with_context(|| format!("Failed to connect to admin API at {}", url))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let err_body = resp.text().unwrap_or_default();
            anyhow::bail!("HTTP {} from {}: {}", status, url, err_body);
        }

        resp.json::<Value>()
            .with_context(|| format!("Failed to parse JSON from {}", url))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        let client = AdminClient::new("127.0.0.1", 9090);
        assert_eq!(client.base_url(), "http://127.0.0.1:9090");
    }

    #[test]
    fn test_client_custom_host() {
        let client = AdminClient::new("10.0.0.1", 8080);
        assert_eq!(client.base_url(), "http://10.0.0.1:8080");
    }

    #[test]
    fn test_get_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        let result = client.get("/health");
        assert!(result.is_err());
    }
}
