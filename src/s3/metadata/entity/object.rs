use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "s3_object")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub bucket_id: String,
    #[sea_orm(primary_key)]
    pub id: String,
    pub size: u32,
    pub last_modified: chrono::DateTime<chrono::Utc>,
    pub content_type: Option<String>,
    pub etag: Option<String>,
    pub content: serde_json::Value,
    #[sea_orm(belongs_to, from = "bucket_id", to = "id")]
    pub bucket: HasOne<super::bucket::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
