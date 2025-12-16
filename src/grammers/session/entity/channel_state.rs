use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "channel_state")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub peer_id: i64,
    pub pts: i32,
}

impl ActiveModelBehavior for ActiveModel {}
