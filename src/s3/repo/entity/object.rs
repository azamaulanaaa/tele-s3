use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "s3_object")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub bucket_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub version_id: String,
    /// Whether this is the current version for the key.
    pub is_latest: bool,
    /// Whether this version is a delete marker.
    pub is_delete_marker: bool,
    pub size: u32,
    pub last_modified: chrono::DateTime<chrono::Utc>,
    pub content_type: Option<String>,
    pub etag: Option<String>,
    /// x-amz-meta-* headers as a JSON object of string key/value pairs.
    pub user_metadata: serde_json::Value,
    /// Object tagging set as a JSON array of {key, value} pairs.
    pub tags: serde_json::Value,
    /// Client-provided checksums as a JSON object keyed by algorithm
    /// (e.g. {"sha256": "<base64>"}).
    pub checksums: serde_json::Value,
    pub content: serde_json::Value,
    #[sea_orm(belongs_to, from = "bucket_id", to = "id")]
    pub bucket: HasOne<super::bucket::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
