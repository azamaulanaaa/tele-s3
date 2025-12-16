use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "peer_info")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub peer_id: i64,
    pub hash: Option<i64>,
    pub subtype: Option<u8>,
}

impl ActiveModelBehavior for ActiveModel {}
