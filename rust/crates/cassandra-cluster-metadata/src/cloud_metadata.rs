// Licensed under Apache License, Version 2.0.

//! Cloud instance metadata client for cloud snitches.
//!
//! Provides async HTTP clients for fetching datacenter/rack information
//! from cloud provider metadata APIs.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.locator.Ec2Snitch` (metadata fetcher)
//! - `org.apache.cassandra.locator.GoogleCloudSnitch` (metadata fetcher)
//! - `org.apache.cassandra.locator.AzureSnitch`
//! - `org.apache.cassandra.locator.AlibabaCloudSnitch`
//! - `org.apache.cassandra.locator.CloudstackSnitch`

use std::time::Duration;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum CloudMetadataError {
    #[error("HTTP request failed: {0}")]
    HttpError(String),
    #[error("Failed to parse metadata: {0}")]
    ParseError(String),
    #[error("Metadata service unavailable: {0}")]
    Unavailable(String),
}

/// Result of a cloud metadata fetch: datacenter and rack.
#[derive(Debug, Clone)]
pub struct CloudLocation {
    pub datacenter: String,
    pub rack: String,
}

/// Cloud metadata client configuration.
#[derive(Debug, Clone)]
pub struct CloudMetadataConfig {
    /// HTTP timeout for metadata requests.
    pub timeout: Duration,
    /// Number of retries on failure.
    pub retries: u32,
}

impl Default for CloudMetadataConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(5),
            retries: 3,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EC2 Metadata
// ─────────────────────────────────────────────────────────────────────────────

/// EC2 instance metadata API URLs.
pub const EC2_METADATA_URL: &str = "http://169.254.169.254/latest/meta-data";
pub const EC2_AZ_PATH: &str = "/placement/availability-zone";

/// Parse EC2 availability zone into (region, az).
///
/// EC2 AZ format: "us-east-1a" → region="us-east-1", rack="us-east-1a"
/// Java strips the last character for the region.
pub fn parse_ec2_az(az: &str) -> Option<CloudLocation> {
    let az = az.trim();
    if az.len() < 2 {
        return None;
    }
    // Region is everything except the last character (the AZ letter)
    let region = &az[..az.len() - 1];
    Some(CloudLocation {
        datacenter: region.to_string(),
        rack: az.to_string(),
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// GCE Metadata
// ─────────────────────────────────────────────────────────────────────────────

/// GCE instance metadata API URL.
pub const GCE_METADATA_URL: &str = "http://metadata.google.internal/computeMetadata/v1";
pub const GCE_ZONE_PATH: &str = "/instance/zone";

/// Parse GCE zone into (region, zone).
///
/// GCE zone format: "projects/123456/zones/us-central1-a"
/// → datacenter="us-central1", rack="us-central1-a"
pub fn parse_gce_zone(zone_path: &str) -> Option<CloudLocation> {
    let zone = zone_path.rsplit('/').next()?.trim();
    if zone.is_empty() {
        return None;
    }
    // Region is zone minus the last 2 characters (e.g., "-a")
    let region = if let Some(idx) = zone.rfind('-') {
        &zone[..idx]
    } else {
        zone
    };
    Some(CloudLocation {
        datacenter: region.to_string(),
        rack: zone.to_string(),
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Azure Metadata
// ─────────────────────────────────────────────────────────────────────────────

/// Azure Instance Metadata Service URL.
pub const AZURE_METADATA_URL: &str =
    "http://169.254.169.254/metadata/instance/compute?api-version=2021-02-01";

/// Parse Azure metadata response (JSON) into location.
///
/// Azure provides `location` (e.g., "eastus") and `zone` (e.g., "1").
/// Datacenter = location, rack = location + zone (or "default" if no zone).
pub fn parse_azure_metadata(location: &str, zone: &str) -> CloudLocation {
    let location = location.trim();
    let zone = zone.trim();
    let rack = if zone.is_empty() {
        format!("{}-default", location)
    } else {
        format!("{}-{}", location, zone)
    };
    CloudLocation {
        datacenter: location.to_string(),
        rack,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Alibaba Cloud Metadata
// ─────────────────────────────────────────────────────────────────────────────

/// Alibaba Cloud (Aliyun) metadata URL.
pub const ALIBABA_METADATA_URL: &str = "http://100.100.100.200/latest/meta-data";
pub const ALIBABA_ZONE_PATH: &str = "/zone-id";
pub const ALIBABA_REGION_PATH: &str = "/region-id";

/// Parse Alibaba Cloud metadata into location.
///
/// Region: "cn-hangzhou", Zone: "cn-hangzhou-b"
/// Datacenter = region, rack = zone.
pub fn parse_alibaba_metadata(region: &str, zone: &str) -> CloudLocation {
    CloudLocation {
        datacenter: region.trim().to_string(),
        rack: zone.trim().to_string(),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// CloudStack Metadata
// ─────────────────────────────────────────────────────────────────────────────

/// CloudStack metadata URL (uses DHCP server as metadata source).
pub const CLOUDSTACK_METADATA_PATH: &str = "/latest/meta-data";

/// Parse CloudStack metadata into location.
///
/// CloudStack provides availability-zone directly.
/// Format varies by deployment; typically "zone-name".
pub fn parse_cloudstack_metadata(availability_zone: &str) -> CloudLocation {
    let az = availability_zone.trim();
    // Use the full AZ as both DC and rack if no separator
    if let Some(idx) = az.rfind('-') {
        CloudLocation {
            datacenter: az[..idx].to_string(),
            rack: az.to_string(),
        }
    } else {
        CloudLocation {
            datacenter: az.to_string(),
            rack: format!("{}-default", az),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ec2_az_valid() {
        let loc = parse_ec2_az("us-east-1a").unwrap();
        assert_eq!(loc.datacenter, "us-east-1");
        assert_eq!(loc.rack, "us-east-1a");
    }

    #[test]
    fn parse_ec2_az_eu() {
        let loc = parse_ec2_az("eu-west-1b").unwrap();
        assert_eq!(loc.datacenter, "eu-west-1");
        assert_eq!(loc.rack, "eu-west-1b");
    }

    #[test]
    fn parse_ec2_az_invalid() {
        assert!(parse_ec2_az("a").is_none());
        assert!(parse_ec2_az("").is_none());
    }

    #[test]
    fn parse_gce_zone_valid() {
        let loc = parse_gce_zone("projects/123/zones/us-central1-a").unwrap();
        assert_eq!(loc.datacenter, "us-central1");
        assert_eq!(loc.rack, "us-central1-a");
    }

    #[test]
    fn parse_gce_zone_short() {
        let loc = parse_gce_zone("us-east1-b").unwrap();
        assert_eq!(loc.datacenter, "us-east1");
        assert_eq!(loc.rack, "us-east1-b");
    }

    #[test]
    fn parse_azure_with_zone() {
        let loc = parse_azure_metadata("eastus", "1");
        assert_eq!(loc.datacenter, "eastus");
        assert_eq!(loc.rack, "eastus-1");
    }

    #[test]
    fn parse_azure_without_zone() {
        let loc = parse_azure_metadata("westeurope", "");
        assert_eq!(loc.datacenter, "westeurope");
        assert_eq!(loc.rack, "westeurope-default");
    }

    #[test]
    fn parse_alibaba() {
        let loc = parse_alibaba_metadata("cn-hangzhou", "cn-hangzhou-b");
        assert_eq!(loc.datacenter, "cn-hangzhou");
        assert_eq!(loc.rack, "cn-hangzhou-b");
    }

    #[test]
    fn parse_cloudstack_with_separator() {
        let loc = parse_cloudstack_metadata("zone1-rack1");
        assert_eq!(loc.datacenter, "zone1");
        assert_eq!(loc.rack, "zone1-rack1");
    }

    #[test]
    fn parse_cloudstack_without_separator() {
        let loc = parse_cloudstack_metadata("myzone");
        assert_eq!(loc.datacenter, "myzone");
        assert_eq!(loc.rack, "myzone-default");
    }
}
