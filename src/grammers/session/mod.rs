use std::{default::Default, ops::Deref};

use grammers_client::session::{
    Session,
    types::{
        ChannelKind, ChannelState, DcOption, PeerAuth, PeerId, PeerInfo, PeerKind, UpdateState,
        UpdatesState,
    },
};
pub use sea_orm::DbErr;
use sea_orm::{
    ActiveModelTrait, ActiveValue::NotSet, ColumnTrait, DatabaseConnection, EntityTrait, ExprTrait,
    QueryFilter, Set, TransactionTrait,
};

mod entity;

#[derive(Debug, thiserror::Error)]
pub enum SessionStorageError {
    #[error("SeaOrm : {0}")]
    SeaOrm(#[from] DbErr),
    #[error("IO : {0}")]
    IO(#[from] std::io::Error),
}

pub struct SessionStorage {
    inner: DatabaseConnection,
}

impl SessionStorage {
    pub async fn init(connection: DatabaseConnection) -> Result<Self, SessionStorageError> {
        Self::sync_table(&connection).await?;

        Ok(Self { inner: connection })
    }

    async fn sync_table(connection: &DatabaseConnection) -> Result<(), DbErr> {
        connection
            .get_schema_registry(concat!(module_path!(), "::entity"))
            .sync(connection)
            .await?;

        Ok(())
    }

    fn run_blocking<F, R>(&self, future: F) -> R
    where
        F: std::future::Future<Output = R> + Send,
        R: Send,
    {
        tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(future))
    }
}

impl Deref for SessionStorage {
    type Target = DatabaseConnection;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[repr(u8)]
enum PeerSubtype {
    UserSelf = 1,
    UserBot = 2,
    UserSelfBot = 3,
    Megagroup = 4,
    Broadcast = 8,
    Gigagroup = 12,
}


impl Session for SessionStorage {
    fn home_dc_id(&self) -> i32 {
        self.run_blocking(async {
            entity::dc_home::Entity::find()
                .one(&self.inner)
                .await
                .map(|v| v.map(|v| v.dc_id))
        })
        .ok()
        .flatten()
        .unwrap_or(DEFAULT_DC)
    }

    fn set_home_dc_id(&self, dc_id: i32) {
        let _: Result<(), DbErr> = self.run_blocking(async {
            let txn = self.begin().await?;

            let _ = entity::dc_home::Entity::delete_many().exec(&txn).await?;
            let _ = entity::dc_home::ActiveModel { dc_id: Set(dc_id) }
                .insert(&txn)
                .await?;

            txn.commit().await?;

            Ok(())
        });
    }

    fn dc_option(&self, dc_id: i32) -> Option<grammers_client::session::types::DcOption> {
        let result: Result<Option<_>, DbErr> = self.run_blocking(async {
            let dc_option = entity::dc_option::Entity::find()
                .filter(entity::dc_option::Column::DcId.eq(dc_id))
                .one(&self.inner)
                .await?;
            let dc_option = dc_option.map(|v| DcOption {
                id: v.dc_id,
                ipv4: v.ipv4.parse().unwrap(),
                ipv6: v.ipv6.parse().unwrap(),
                auth_key: v.auth_key.map(|v| v.try_into().unwrap()),
            });

            Ok(dc_option)
        });
        result.ok().flatten()
    }

    fn set_dc_option(&self, dc_option: &DcOption) {
        let _: Result<(), DbErr> = self.run_blocking(async {
            let txn = self.begin().await?;

            entity::dc_option::Entity::delete_many()
                .filter(entity::dc_option::Column::DcId.eq(dc_option.id))
                .exec(&txn)
                .await?;
            entity::dc_option::ActiveModel {
                dc_id: Set(dc_option.id),
                ipv4: Set(dc_option.ipv4.to_string()),
                ipv6: Set(dc_option.ipv6.to_string()),
                auth_key: Set(dc_option.auth_key.map(|v| v.to_vec())),
            }
            .insert(&txn)
            .await?;

            txn.commit().await?;

            Ok(())
        });
    }

    fn peer(&self, peer: PeerId) -> Option<PeerInfo> {
        let peer_info: Result<Option<_>, DbErr> = self.run_blocking(async {
            let peer_info = match peer.kind() {
                PeerKind::UserSelf => {
                    entity::peer_info::Entity::find()
                        .filter(
                            entity::peer_info::Column::Subtype
                                .into_expr()
                                .bit_and(PeerSubtype::UserSelf as i32),
                        )
                        .one(&self.inner)
                        .await?
                }
                _ => {
                    entity::peer_info::Entity::find()
                        .filter(entity::peer_info::Column::PeerId.eq(peer.bot_api_dialog_id()))
                        .one(&self.inner)
                        .await?
                }
            };
            let peer_info = peer_info.map(|v| match peer.kind() {
                PeerKind::User | PeerKind::UserSelf => PeerInfo::User {
                    id: PeerId::user(v.peer_id.into()).bare_id(),
                    auth: v.hash.map(|v| PeerAuth::from_hash(v.into())),
                    bot: v.subtype.map(|v| v & PeerSubtype::UserBot as u8 != 0),
                    is_self: v.subtype.map(|v| v & PeerSubtype::UserSelf as u8 != 0),
                },
                PeerKind::Chat => PeerInfo::Chat { id: peer.bare_id() },
                PeerKind::Channel => PeerInfo::Channel {
                    id: peer.bare_id(),
                    auth: v.hash.map(|v| PeerAuth::from_hash(v.into())),
                    kind: v.subtype.and_then(|s| {
                        if (s & PeerSubtype::Gigagroup as u8) == PeerSubtype::Gigagroup as u8 {
                            Some(ChannelKind::Gigagroup)
                        } else if s & PeerSubtype::Broadcast as u8 != 0 {
                            Some(ChannelKind::Broadcast)
                        } else if s & PeerSubtype::Megagroup as u8 != 0 {
                            Some(ChannelKind::Megagroup)
                        } else {
                            None
                        }
                    }),
                },
            });

            Ok(peer_info)
        });

        peer_info.ok().flatten()
    }

    fn cache_peer(&self, peer: &PeerInfo) {
        let subtype = match peer {
            PeerInfo::User { bot, is_self, .. } => {
                match (bot.unwrap_or_default(), is_self.unwrap_or_default()) {
                    (true, true) => Some(PeerSubtype::UserSelfBot),
                    (true, false) => Some(PeerSubtype::UserBot),
                    (false, true) => Some(PeerSubtype::UserSelf),
                    (false, false) => None,
                }
            }
            PeerInfo::Chat { .. } => None,
            PeerInfo::Channel { kind, .. } => kind.map(|kind| match kind {
                ChannelKind::Megagroup => PeerSubtype::Megagroup,
                ChannelKind::Broadcast => PeerSubtype::Broadcast,
                ChannelKind::Gigagroup => PeerSubtype::Gigagroup,
            }),
        };

        let hash = {
            let mut hash = None;

            if peer.auth() != PeerAuth::default() {
                hash = Some(peer.auth().hash())
            }

            hash
        };

        let _: Result<(), DbErr> = self.run_blocking(async {
            let txn = self.begin().await?;

            entity::peer_info::Entity::delete_many()
                .filter(entity::peer_info::Column::PeerId.eq(peer.id().bot_api_dialog_id()))
                .exec(&txn)
                .await?;
            entity::peer_info::ActiveModel {
                peer_id: Set(peer.id().bot_api_dialog_id()),
                subtype: Set(subtype.map(|v| v as u8)),
                hash: Set(hash),
            }
            .insert(&txn)
            .await?;

            txn.commit().await?;

            Ok(())
        });
    }

    fn updates_state(&self) -> UpdatesState {
        let result: Result<_, DbErr> = self.run_blocking(async {
            let txn = self.begin().await?;

            let update_state = entity::update_state::Entity::find().one(&txn).await?;
            let channels = entity::channel_state::Entity::find().all(&txn).await?;

            txn.commit().await?;

            let mut update_state = update_state
                .map(|v| UpdatesState {
                    pts: v.pts,
                    qts: v.qts,
                    date: v.date,
                    seq: v.seq,
                    channels: Vec::new(),
                })
                .unwrap_or_default();
            update_state.channels = channels
                .into_iter()
                .map(|v| ChannelState {
                    id: v.peer_id,
                    pts: v.pts,
                })
                .collect();

            Ok(update_state)
        });
        result.ok().unwrap_or_default()
    }

    fn set_update_state(&self, update: UpdateState) {
        let _: Result<(), DbErr> = self.run_blocking(async {
            let txn = self.begin().await?;

            match update {
                UpdateState::All(updates_state) => {
                    entity::update_state::Entity::delete_many()
                        .exec(&txn)
                        .await?;
                    entity::update_state::ActiveModel {
                        id: NotSet,
                        pts: Set(updates_state.pts),
                        qts: Set(updates_state.qts),
                        date: Set(updates_state.date),
                        seq: Set(updates_state.seq),
                    }
                    .insert(&txn)
                    .await?;

                    entity::channel_state::Entity::delete_many()
                        .exec(&txn)
                        .await?;
                    let channel_states = updates_state
                        .channels
                        .into_iter()
                        .map(|v| entity::channel_state::ActiveModel {
                            peer_id: Set(v.id),
                            pts: Set(v.pts),
                        })
                        .collect::<Vec<_>>();
                    entity::channel_state::Entity::insert_many(channel_states)
                        .exec(&txn)
                        .await?;
                }
                UpdateState::Primary { pts, date, seq } => {
                    let update_state = entity::update_state::Entity::find().one(&txn).await?;

                    if update_state.is_some() {
                        entity::update_state::ActiveModel {
                            id: NotSet,
                            pts: Set(pts),
                            date: Set(date),
                            seq: Set(seq),
                            qts: NotSet,
                        }
                        .update(&txn)
                        .await?;
                    } else {
                        entity::update_state::ActiveModel {
                            id: NotSet,
                            pts: Set(pts),
                            date: Set(date),
                            seq: Set(seq),
                            qts: Set(0),
                        }
                        .insert(&txn)
                        .await?;
                    }
                }
                UpdateState::Secondary { qts } => {
                    let update_state = entity::update_state::Entity::find().one(&txn).await?;

                    if update_state.is_some() {
                        entity::update_state::ActiveModel {
                            id: NotSet,
                            pts: NotSet,
                            date: NotSet,
                            seq: NotSet,
                            qts: Set(qts),
                        }
                        .update(&txn)
                        .await?;
                    } else {
                        entity::update_state::ActiveModel {
                            id: NotSet,
                            pts: Set(0),
                            date: Set(0),
                            seq: Set(0),
                            qts: Set(qts),
                        }
                        .insert(&txn)
                        .await?;
                    }
                }
                UpdateState::Channel { id, pts } => {
                    entity::channel_state::Entity::delete_many()
                        .filter(entity::channel_state::Column::PeerId.eq(id))
                        .exec(&txn)
                        .await?;
                    entity::channel_state::ActiveModel {
                        peer_id: Set(id),
                        pts: Set(pts),
                    }
                    .insert(&txn)
                    .await?;
                }
            }

            txn.commit().await?;

            Ok(())
        });
    }
}
