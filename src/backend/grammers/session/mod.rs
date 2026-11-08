use std::net::{Ipv4Addr, SocketAddrV4, SocketAddrV6};

use grammers_session::{
    BoxFuture, Session,
    types::{
        ChannelKind, ChannelState, DcOption, PeerAuth, PeerId, PeerInfo, PeerKind, UpdateState,
        UpdatesState,
    },
};
use redb::{ReadableDatabase, ReadableTable};
use tracing::{error, instrument};

use crate::storage::{
    CHANNEL_STATE, ChannelStateRow, DC_HOME, DC_OPTIONS, DcHomeRow, DcOptionRow, PEER_INFO,
    PeerInfoRow, Store, StoreError, UPDATE_STATE, UpdateStateRow, decode, encode,
};

#[derive(Debug, thiserror::Error)]
pub(super) enum SessionStorageError {
    #[error("Store : {0}")]
    Store(#[from] StoreError),
}

impl From<redb::DatabaseError> for SessionStorageError {
    fn from(err: redb::DatabaseError) -> Self {
        Self::Store(StoreError::from(err))
    }
}

impl From<redb::TransactionError> for SessionStorageError {
    fn from(err: redb::TransactionError) -> Self {
        Self::Store(StoreError::from(err))
    }
}

impl From<redb::TableError> for SessionStorageError {
    fn from(err: redb::TableError) -> Self {
        Self::Store(StoreError::from(err))
    }
}

impl From<redb::StorageError> for SessionStorageError {
    fn from(err: redb::StorageError) -> Self {
        Self::Store(StoreError::from(err))
    }
}

impl From<redb::CommitError> for SessionStorageError {
    fn from(err: redb::CommitError) -> Self {
        Self::Store(StoreError::from(err))
    }
}

impl From<serde_json::Error> for SessionStorageError {
    fn from(err: serde_json::Error) -> Self {
        Self::Store(StoreError::from(err))
    }
}

/// Telegram MTProto session state over the shared redb store.
pub(super) struct SessionStorage {
    store: Store,
}

impl SessionStorage {
    #[instrument(skip(store), level = "debug", err)]
    pub(super) async fn init(store: Store) -> Result<Self, SessionStorageError> {
        Ok(Self { store })
    }

    fn run_sync<T>(
        &self,
        f: impl FnOnce() -> Result<T, SessionStorageError>,
    ) -> Result<T, SessionStorageError> {
        match tokio::runtime::Handle::try_current() {
            Ok(handle)
                if matches!(
                    handle.runtime_flavor(),
                    tokio::runtime::RuntimeFlavor::MultiThread
                ) =>
            {
                tokio::task::block_in_place(f)
            }
            _ => f(),
        }
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
    type Error = SessionStorageError;

    #[instrument(skip(self), level = "debug", ret)]
    fn home_dc_id(&self) -> Result<i32, Self::Error> {
        let result = self.run_sync(|| {
            let txn = self.store.read()?;
            let table = txn.open_table(DC_HOME)?;
            let row = table
                .get(0)?
                .map(|value| decode::<DcHomeRow>(value.value()));
            match row {
                Some(Ok(row)) => Ok(Some(row.dc_id)),
                Some(Err(err)) => Err(err.into()),
                None => Ok(None),
            }
        });

        match result {
            Ok(Some(v)) => Ok(v),
            Ok(None) => Ok(DEFAULT_DC),
            Err(e) => {
                error!(operation = "query_home_dc", error.type = "session_store", error.message = ?e, "Failed to query Home DC");

                Err(e)
            }
        }
    }

    #[instrument(skip(self), level = "debug")]
    fn set_home_dc_id(&self, dc_id: i32) -> BoxFuture<'_, Result<(), Self::Error>> {
        Box::pin(async move {
            self.store
                .run(move |db| {
                    let txn = db.begin_write()?;
                    {
                        let mut table = txn.open_table(DC_HOME)?;
                        let row = DcHomeRow { dc_id };
                        let bytes = encode(&row)?;
                        table.insert(0, bytes.as_slice())?;
                    }
                    txn.commit()?;

                    Ok(())
                })
                .await
        })
    }

    #[instrument(skip(self), level = "debug", ret)]
    fn dc_option(&self, dc_id: i32) -> Result<Option<DcOption>, Self::Error> {
        let row: Option<DcOptionRow> = self.run_sync(|| {
            let txn = self.store.read()?;
            let table = txn.open_table(DC_OPTIONS)?;
            let Some(value) = table.get(dc_id)? else {
                return Ok(None);
            };
            Ok(Some(decode(value.value())?))
        })?;

        let dc_option = row.and_then(|v| {
            let ipv4 = v.ipv4.parse();
            let ipv6 = v.ipv6.parse();

            let (ipv4, ipv6) = if let (Ok(v4), Ok(v6)) = (ipv4, ipv6) {
                (v4, v6)
            } else {
                error!(
                    dc_id,
                    error.type = "corrupt_dc_option",
                    "Corrupted IP address in database. Ignoring row."
                );

                return None;
            };

            let auth_key = match v.auth_key {
                None => None,
                Some(bytes) => {
                    if let Ok(key) = bytes.try_into() {
                        Some(key)
                    } else {
                        error!(
                            dc_id,
                            error.type = "corrupt_dc_option",
                            "Corrupted auth key in database. Ignoring row."
                        );

                        return None;
                    }
                }
            };

            Some(DcOption {
                id: v.dc_id,
                ipv4,
                ipv6,
                auth_key,
            })
        });

        if dc_option.is_none() {
            let fallback = KNOWN_DC_OPTIONS
                .iter()
                .find(|dc_option| dc_option.id == dc_id)
                .cloned();

            return Ok(fallback);
        }

        Ok(dc_option)
    }

    #[instrument(skip(self), level = "debug")]
    fn set_dc_option(&self, dc_option: &DcOption) -> BoxFuture<'_, Result<(), Self::Error>> {
        let dc_option = dc_option.clone();
        Box::pin(async move {
            self.store
                .run(move |db| {
                    let txn = db.begin_write()?;
                    {
                        let mut table = txn.open_table(DC_OPTIONS)?;
                        let row = DcOptionRow {
                            dc_id: dc_option.id,
                            ipv4: dc_option.ipv4.to_string(),
                            ipv6: dc_option.ipv6.to_string(),
                            auth_key: dc_option.auth_key.map(|v| v.to_vec()),
                        };
                        let bytes = encode(&row)?;
                        table.insert(row.dc_id, bytes.as_slice())?;
                    }
                    txn.commit()?;

                    Ok(())
                })
                .await
        })
    }

    #[instrument(skip(self), level = "debug", ret)]
    fn peer(&self, peer: PeerId) -> BoxFuture<'_, Result<Option<PeerInfo>, Self::Error>> {
        Box::pin(async move {
            let dialog_id = peer.bot_api_dialog_id();
            let row = self
                .store
                .run(
                    move |db| -> Result<Option<PeerInfoRow>, SessionStorageError> {
                        let txn = db.begin_read()?;
                        let table = txn.open_table(PEER_INFO)?;
                        let value = match dialog_id {
                            Some(dialog_id) => table.get(dialog_id)?,
                            None => {
                                let mut found = None;
                                for entry in table.iter()? {
                                    let (_, value) = entry?;
                                    let row: PeerInfoRow = decode(value.value())?;
                                    if row
                                        .subtype
                                        .is_some_and(|v| v & PeerSubtype::UserSelf as u8 != 0)
                                    {
                                        found = Some(value);
                                        break;
                                    }
                                }
                                found
                            }
                        };

                        let Some(value) = value else {
                            return Ok(None);
                        };
                        Ok(Some(decode::<PeerInfoRow>(value.value())?))
                    },
                )
                .await?;

            let Some(row) = row else {
                return Ok(None);
            };

            Ok(Some(match peer.kind() {
                PeerKind::User => PeerInfo::User {
                    id: PeerId::user_unchecked(row.peer_id).bare_id_unchecked(),
                    auth: row.hash.map(PeerAuth::from_hash),
                    bot: row.subtype.map(|v| v & PeerSubtype::UserBot as u8 != 0),
                    is_self: row.subtype.map(|v| v & PeerSubtype::UserSelf as u8 != 0),
                },
                PeerKind::Chat => PeerInfo::Chat {
                    id: peer.bare_id_unchecked(),
                },
                PeerKind::Channel => PeerInfo::Channel {
                    id: peer.bare_id_unchecked(),
                    auth: row.hash.map(PeerAuth::from_hash),
                    kind: row.subtype.and_then(|s| {
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
            }))
        })
    }

    #[instrument(skip(self), level = "debug")]
    fn cache_peer(&self, peer: &PeerInfo) -> BoxFuture<'_, Result<(), Self::Error>> {
        let peer = peer.clone();
        Box::pin(async move {
            // Merge with any previously-cached info, as upstream's
            // `PeerInfo::extend_info` does: cached fields win, so a sparse
            // update never clears a known value.
            let peer = if let Some(mut existing) = self.peer(peer.id()).await? {
                existing.extend_info(&peer);
                existing
            } else {
                peer
            };

            let subtype = match &peer {
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

            let hash = peer.auth().map(grammers_session::types::PeerAuth::hash);
            let row = PeerInfoRow {
                peer_id: peer.id().bot_api_dialog_id_unchecked(),
                subtype: subtype.map(|v| v as u8),
                hash,
            };

            self.store
                .run(move |db| {
                    let txn = db.begin_write()?;
                    {
                        let mut table = txn.open_table(PEER_INFO)?;
                        let bytes = encode(&row)?;
                        table.insert(row.peer_id, bytes.as_slice())?;
                    }
                    txn.commit()?;

                    Ok(())
                })
                .await
        })
    }

    #[instrument(skip(self), level = "debug", ret)]
    fn updates_state(&self) -> BoxFuture<'_, Result<UpdatesState, Self::Error>> {
        Box::pin(async move {
            let (update_state, channels) = self
                .store
                .run(
                    |db| -> Result<
                        (Option<UpdateStateRow>, Vec<ChannelStateRow>),
                        SessionStorageError,
                    > {
                        let txn = db.begin_read()?;
                        let updates = txn.open_table(UPDATE_STATE)?;
                        let channels = txn.open_table(CHANNEL_STATE)?;

                        let update_state = updates
                            .get(0)?
                            .map(|value| decode::<UpdateStateRow>(value.value()))
                            .transpose()?;
                        let channels = channels
                            .iter()?
                            .map(|entry| {
                                let (_, value) = entry?;
                                decode::<ChannelStateRow>(value.value())
                            })
                            .collect::<Result<Vec<_>, _>>()?;

                        Ok((update_state, channels))
                    },
                )
                .await?;

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
        })
    }

    #[instrument(skip(self, update_state), level = "debug")]
    fn set_update_state(
        &self,
        update_state: UpdateState,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        Box::pin(async move {
            self.store
                .run(move |db| {
                    let txn = db.begin_write()?;
                    match update_state {
                        UpdateState::All(updates_state) => {
                            {
                                let mut table = txn.open_table(UPDATE_STATE)?;
                                let row = UpdateStateRow {
                                    pts: updates_state.pts,
                                    qts: updates_state.qts,
                                    date: updates_state.date,
                                    seq: updates_state.seq,
                                };
                                let bytes = encode(&row)?;
                                table.insert(0, bytes.as_slice())?;
                            }
                            {
                                let mut table = txn.open_table(CHANNEL_STATE)?;
                                table.retain(|_, _| false)?;
                                for channel in updates_state.channels {
                                    let row = ChannelStateRow {
                                        peer_id: channel.id,
                                        pts: channel.pts,
                                    };
                                    let bytes = encode(&row)?;
                                    table.insert(row.peer_id, bytes.as_slice())?;
                                }
                            }
                        }
                        UpdateState::Primary { pts, date, seq } => {
                            let current = {
                                let table = txn.open_table(UPDATE_STATE)?;
                                table
                                    .get(0)?
                                    .map(|value| decode::<UpdateStateRow>(value.value()))
                                    .transpose()?
                            };
                            let row = match current {
                                Some(mut row) => {
                                    row.pts = pts;
                                    row.date = date;
                                    row.seq = seq;
                                    row
                                }
                                None => UpdateStateRow {
                                    pts,
                                    qts: 0,
                                    date,
                                    seq,
                                },
                            };
                            let bytes = encode(&row)?;
                            let mut table = txn.open_table(UPDATE_STATE)?;
                            table.insert(0, bytes.as_slice())?;
                        }
                        UpdateState::Secondary { qts } => {
                            let current = {
                                let table = txn.open_table(UPDATE_STATE)?;
                                table
                                    .get(0)?
                                    .map(|value| decode::<UpdateStateRow>(value.value()))
                                    .transpose()?
                            };
                            let row = match current {
                                Some(mut row) => {
                                    row.qts = qts;
                                    row
                                }
                                None => UpdateStateRow {
                                    pts: 0,
                                    qts,
                                    date: 0,
                                    seq: 0,
                                },
                            };
                            let bytes = encode(&row)?;
                            let mut table = txn.open_table(UPDATE_STATE)?;
                            table.insert(0, bytes.as_slice())?;
                        }
                        UpdateState::Channel { id, pts } => {
                            let row = ChannelStateRow { peer_id: id, pts };
                            let bytes = encode(&row)?;
                            let mut table = txn.open_table(CHANNEL_STATE)?;
                            table.insert(row.peer_id, bytes.as_slice())?;
                        }
                    }
                    txn.commit()?;

                    Ok(())
                })
                .await
        })
    }
}
