use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "s3_bucket")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    pub region: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    #[sea_orm(has_many)]
    pub object: HasMany<super::object::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
