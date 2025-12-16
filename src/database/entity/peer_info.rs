use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "peer_info")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub peer_id: i32,
    pub hash: Option<i32>,
    pub subtype: Option<i32>,
}

impl ActiveModelBehavior for ActiveModel {}
