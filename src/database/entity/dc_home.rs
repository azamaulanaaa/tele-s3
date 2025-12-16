use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "dc_home")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub dc_id: i32,
}

impl ActiveModelBehavior for ActiveModel {}
