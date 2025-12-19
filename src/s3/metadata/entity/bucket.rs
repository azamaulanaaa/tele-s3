use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "s3_bucket")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: String,
    #[sea_orm(has_many)]
    pub metadata: HasMany<super::object::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
