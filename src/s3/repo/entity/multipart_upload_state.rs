use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "s3_multipart_upload_state")]
pub(crate) struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub bucket_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub object_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub upload_id: String,
    pub content_type: Option<String>,
    /// The `x-amz-meta-*` headers captured at `CreateMultipartUpload`. The
    /// code replays them onto the completed object.
    pub user_metadata: serde_json::Value,
    /// The tag set captured at `CreateMultipartUpload`. It is `[]` when
    /// none exists. The code replays it onto the completed object. The
    /// column is nullable. Databases created before this column existed
    /// still sync. The code reads missing values as `[]`.
    pub tags: Option<serde_json::Value>,
    pub content: serde_json::Value,
    #[sea_orm(belongs_to, from = "bucket_id", to = "id")]
    pub bucket: HasOne<super::bucket::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
