use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "s3_multipart_upload_state")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub bucket_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub object_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub upload_id: String,
    pub content_type: Option<String>,
    pub content: serde_json::Value,
    #[sea_orm(belongs_to, from = "bucket_id", to = "id")]
    pub bucket: HasOne<super::bucket::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
