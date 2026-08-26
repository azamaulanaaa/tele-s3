use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "s3_blob")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    /// Size of the backend blob in bytes.
    pub size: u32,
    /// Number of metadata references pointing at this blob. A blob is
    /// eligible for backend deletion only when this reaches zero.
    pub refs: i64,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl ActiveModelBehavior for ActiveModel {}
