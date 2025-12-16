use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "update_state")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i32,
    pub pts: i32,
    pub qts: i32,
    pub date: i32,
    pub seq: i32,
}

impl ActiveModelBehavior for ActiveModel {}
