use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "dc_option")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub dc_id: i32,
    pub ipv4: String,
    pub ipv6: String,
    pub auth_key: Option<Vec<u8>>,
}

impl ActiveModelBehavior for ActiveModel {}
