use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "s3_blob")]
pub(crate) struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    pub size: u32,
    /// Number of metadata references that point at this blob. The backend
    /// deletes the blob only when this count reaches zero.
    pub refs: i64,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl ActiveModelBehavior for ActiveModel {}
