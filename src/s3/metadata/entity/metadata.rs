use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "s3_metadata")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub bucket: String,
    #[sea_orm(primary_key)]
    pub key: String,
    pub size: u32,
    pub last_modified: chrono::DateTime<chrono::Utc>,
    pub content_type: Option<String>,
    pub etag: Option<String>,
    pub content: serde_json::Value,
}

impl ActiveModelBehavior for ActiveModel {}
