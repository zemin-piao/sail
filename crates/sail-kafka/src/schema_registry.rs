use std::collections::HashMap;
use std::sync::RwLock;

use arrow::datatypes::Schema as ArrowSchema;
use datafusion::common::{DataFusionError, Result};

use crate::error::KafkaError;

#[derive(Debug, Clone)]
pub struct SchemaRegistryConfig {
    pub url: String,
    pub basic_auth: Option<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct RegisteredSchema {
    pub schema_id: u32,
    pub schema_json: String,
    pub schema_type: String,
}

pub struct SchemaRegistryClient {
    config: SchemaRegistryConfig,
    client: reqwest::Client,
    cache: RwLock<HashMap<u32, RegisteredSchema>>,
}

impl SchemaRegistryClient {
    pub fn new(config: SchemaRegistryConfig) -> Result<Self> {
        let client = reqwest::Client::builder()
            .build()
            .map_err(|e| {
                DataFusionError::from(KafkaError::SchemaRegistry(format!(
                    "failed to create HTTP client: {e}"
                )))
            })?;
        Ok(Self {
            config,
            client,
            cache: RwLock::new(HashMap::new()),
        })
    }

    pub async fn get_latest_version(&self, subject: &str) -> Result<RegisteredSchema> {
        let url = format!(
            "{}/subjects/{}/versions/latest",
            self.config.url.trim_end_matches('/'),
            subject
        );
        let response = self.send_request(&url).await?;
        let json: serde_json::Value = serde_json::from_str(&response).map_err(|e| {
            DataFusionError::from(KafkaError::SchemaRegistry(format!(
                "failed to parse schema registry response: {e}"
            )))
        })?;

        let schema_id = json["id"]
            .as_u64()
            .ok_or_else(|| {
                DataFusionError::from(KafkaError::SchemaRegistry(
                    "missing 'id' in schema registry response".to_string(),
                ))
            })? as u32;
        let schema_json = json["schema"]
            .as_str()
            .ok_or_else(|| {
                DataFusionError::from(KafkaError::SchemaRegistry(
                    "missing 'schema' in schema registry response".to_string(),
                ))
            })?
            .to_string();
        let schema_type = json["schemaType"]
            .as_str()
            .unwrap_or("AVRO")
            .to_string();

        let registered = RegisteredSchema {
            schema_id,
            schema_json,
            schema_type,
        };

        if let Ok(mut cache) = self.cache.write() {
            cache.insert(schema_id, registered.clone());
        }

        Ok(registered)
    }

    pub async fn get_schema_by_id(&self, id: u32) -> Result<RegisteredSchema> {
        if let Ok(cache) = self.cache.read() {
            if let Some(cached) = cache.get(&id) {
                return Ok(cached.clone());
            }
        }

        let url = format!(
            "{}/schemas/ids/{}",
            self.config.url.trim_end_matches('/'),
            id
        );
        let response = self.send_request(&url).await?;
        let json: serde_json::Value = serde_json::from_str(&response).map_err(|e| {
            DataFusionError::from(KafkaError::SchemaRegistry(format!(
                "failed to parse schema registry response: {e}"
            )))
        })?;

        let schema_json = json["schema"]
            .as_str()
            .ok_or_else(|| {
                DataFusionError::from(KafkaError::SchemaRegistry(
                    "missing 'schema' in schema registry response".to_string(),
                ))
            })?
            .to_string();
        let schema_type = json["schemaType"]
            .as_str()
            .unwrap_or("AVRO")
            .to_string();

        let registered = RegisteredSchema {
            schema_id: id,
            schema_json,
            schema_type,
        };

        if let Ok(mut cache) = self.cache.write() {
            cache.insert(id, registered.clone());
        }

        Ok(registered)
    }

    async fn send_request(&self, url: &str) -> Result<String> {
        let mut request = self.client.get(url);
        if let Some((user, pass)) = &self.config.basic_auth {
            request = request.basic_auth(user, Some(pass));
        }
        let response = request.send().await.map_err(|e| {
            DataFusionError::from(KafkaError::SchemaRegistry(format!(
                "HTTP request to schema registry failed: {e}"
            )))
        })?;

        if !response.status().is_success() {
            return Err(DataFusionError::from(KafkaError::SchemaRegistry(
                format!(
                    "schema registry returned HTTP {}: {}",
                    response.status(),
                    url
                ),
            )));
        }

        response.text().await.map_err(|e| {
            DataFusionError::from(KafkaError::SchemaRegistry(format!(
                "failed to read schema registry response: {e}"
            )))
        })
    }
}
