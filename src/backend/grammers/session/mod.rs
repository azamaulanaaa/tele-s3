use std::{
    default::Default,
    net::{Ipv4Addr, SocketAddrV4, SocketAddrV6},
    ops::Deref,
};

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
use tracing::{error, instrument, warn};

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
    #[instrument(skip(connection), err)]
    pub async fn init(connection: DatabaseConnection) -> Result<Self, SessionStorageError> {
        Self::sync_table(&connection).await?;

        Ok(Self { inner: connection })
    }

    #[instrument(skip(connection), err)]
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

const DEFAULT_DC: i32 = 2;
const KNOWN_DC_OPTIONS: [DcOption; 5] = [
    DcOption {
        id: 1,
        ipv4: SocketAddrV4::new(Ipv4Addr::new(149, 154, 175, 53), 443),
        ipv6: SocketAddrV6::new(
            Ipv4Addr::new(149, 154, 175, 53).to_ipv6_compatible(),
            443,
            0,
            0,
        ),
        auth_key: None,
    },
    DcOption {
        id: 2,
        ipv4: SocketAddrV4::new(Ipv4Addr::new(149, 154, 167, 41), 443),
        ipv6: SocketAddrV6::new(
            Ipv4Addr::new(149, 154, 167, 41).to_ipv6_compatible(),
            443,
            0,
            0,
        ),
        auth_key: None,
    },
    DcOption {
        id: 3,
        ipv4: SocketAddrV4::new(Ipv4Addr::new(149, 154, 175, 100), 443),
        ipv6: SocketAddrV6::new(
            Ipv4Addr::new(149, 154, 175, 100).to_ipv6_compatible(),
            443,
            0,
            0,
        ),
        auth_key: None,
    },
    DcOption {
        id: 4,
        ipv4: SocketAddrV4::new(Ipv4Addr::new(149, 154, 167, 92), 443),
        ipv6: SocketAddrV6::new(
            Ipv4Addr::new(149, 154, 167, 92).to_ipv6_compatible(),
            443,
            0,
            0,
        ),
        auth_key: None,
    },
    DcOption {
        id: 5,
        ipv4: SocketAddrV4::new(Ipv4Addr::new(91, 108, 56, 104), 443),
        ipv6: SocketAddrV6::new(
            Ipv4Addr::new(91, 108, 56, 104).to_ipv6_compatible(),
            443,
            0,
            0,
        ),
        auth_key: None,
    },
];

impl Session for SessionStorage {
    #[instrument(skip(self), level = "debug", ret)]
    fn home_dc_id(&self) -> i32 {
        let result = self.run_blocking(async {
            entity::dc_home::Entity::find()
                .one(&self.inner)
                .await
                .map(|v| v.map(|v| v.dc_id))
        });

        match result {
            Ok(Some(v)) => v,
            Ok(None) => {
                warn!("No Home DC found, using default");
                DEFAULT_DC
            }
            Err(e) => {
                error!(error = ?e, "Failed to query Home DC");

                DEFAULT_DC
            }
        }
    }

    #[instrument(skip(self), level = "debug")]
    fn set_home_dc_id(&self, dc_id: i32) {
        let result: Result<(), DbErr> = self.run_blocking(async {
            let txn = self.begin().await?;

            let _ = entity::dc_home::Entity::delete_many().exec(&txn).await?;
            let _ = entity::dc_home::ActiveModel { dc_id: Set(dc_id) }
                .insert(&txn)
                .await?;

            txn.commit().await?;

            Ok(())
        });

        if let Err(e) = result {
            error!(
                error = ?e,
                dc_id = dc_id,
                "Failed to set Home DC"
            );
        }
    }

    #[instrument(skip(self), level = "debug", ret)]
    fn dc_option(&self, dc_id: i32) -> Option<grammers_client::session::types::DcOption> {
        let result: Result<Option<_>, DbErr> = self.run_blocking(async {
            let dc_option = entity::dc_option::Entity::find()
                .filter(entity::dc_option::Column::DcId.eq(dc_id))
                .one(&self.inner)
                .await?;

            Ok(dc_option)
        });

        let dc_option = match result {
            Ok(Some(v)) => Some(v),
            Ok(None) => {
                warn!("DC Option not found, find from default list.");

                None
            }
            Err(e) => {
                error!(
                    error = ?e,
                    dc_id = dc_id,
                    "Failed to query DC Option. find from default list."
                );

                None
            }
        };

        let dc_option = dc_option
            .map(|v| {
                let ipv4 = v.ipv4.parse();
                let ipv6 = v.ipv6.parse();

                let (ipv4, ipv6) = match (ipv4, ipv6) {
                    (Ok(v4), Ok(v6)) => (v4, v6),
                    _ => {
                        error!(dc_id, "Corrupted IP address in database. Ignoring row.");

                        return None;
                    }
                };

                Some(DcOption {
                    id: v.dc_id,
                    ipv4,
                    ipv6,
                    auth_key: v.auth_key.map(|v| v.try_into().unwrap()),
                })
            })
            .flatten();

        if dc_option.is_none() {
            let fallback = KNOWN_DC_OPTIONS
                .iter()
                .find(|dc_option| dc_option.id == dc_id)
                .cloned();

            if fallback.is_none() {
                warn!(dc_id, "DC Option found nowhere (neither DB nor Defaults).");
            }

            return fallback;
        }

        dc_option
    }

    #[instrument(skip(self), level = "debug")]
    fn set_dc_option(&self, dc_option: &DcOption) {
        let result: Result<(), DbErr> = self.run_blocking(async {
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

        if let Err(e) = result {
            error!(
                error = ?e,
                "Failed to set DC Option"
            );
        }
    }

    #[instrument(skip(self), level = "debug", ret)]
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

            Ok(peer_info)
        });

        let peer_info = match peer_info {
            Ok(Some(v)) => v,
            Ok(None) => {
                warn!("Peer not found");

                return None;
            }
            Err(e) => {
                error!(
                    error = ?e,
                    "Failed to query Peer Info"
                );

                return None;
            }
        };

        let peer_info = match peer.kind() {
            PeerKind::User | PeerKind::UserSelf => PeerInfo::User {
                id: PeerId::user(peer_info.peer_id.into()).bare_id(),
                auth: peer_info.hash.map(|v| PeerAuth::from_hash(v.into())),
                bot: peer_info
                    .subtype
                    .map(|v| v & PeerSubtype::UserBot as u8 != 0),
                is_self: peer_info
                    .subtype
                    .map(|v| v & PeerSubtype::UserSelf as u8 != 0),
            },
            PeerKind::Chat => PeerInfo::Chat { id: peer.bare_id() },
            PeerKind::Channel => PeerInfo::Channel {
                id: peer.bare_id(),
                auth: peer_info.hash.map(|v| PeerAuth::from_hash(v.into())),
                kind: peer_info.subtype.and_then(|s| {
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
        };

        Some(peer_info)
    }

    #[instrument(skip(self), level = "debug")]
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

        let result: Result<(), DbErr> = self.run_blocking(async {
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

        if let Err(e) = result {
            error!(
                error = ?e,
                "Failed to set Cache Peer"
            );
        }
    }

    #[instrument(skip(self), level = "debug", ret)]
    fn updates_state(&self) -> UpdatesState {
        let result: Result<_, DbErr> = self.run_blocking(async {
            let txn = self.begin().await?;

            let update_state = entity::update_state::Entity::find().one(&txn).await?;
            let channels = entity::channel_state::Entity::find().all(&txn).await?;

            txn.commit().await?;

            Ok((update_state, channels))
        });

        let (update_state, channels) = match result {
            Ok(v) => v,
            Err(e) => {
                error!(
                    error = ?e,
                    "Failed to query Update State"
                );

                return Default::default();
            }
        };

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

        update_state
    }

    #[instrument(skip(self, update_state), level = "debug")]
    fn set_update_state(&self, update_state: UpdateState) {
        let result: Result<(), DbErr> = self.run_blocking(async {
            let txn = self.begin().await?;

            match update_state {
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

        if let Err(e) = result {
            error!(
                error = ?e,
                "Failed to set Update State"
            );
        }
    }
}
