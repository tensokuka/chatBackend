use std::{
    collections::HashSet,
    hash::RandomState,
    sync::Arc,
};

use crate::utils::{Consumer, push_context, render_notification_content, send_message_push};
use anyhow::Result;
use async_trait::async_trait;
use lapin::{Channel, Connection, message::Delivery};
use revolt_database::{
    Database, Member, MessageFlagsValue,
    events::rabbit::*,
    util::{
        bulk_permissions::BulkDatabasePermissionQuery,
        push_filter::{PushCandidate, filter_push_recipients},
    },
};
use revolt_models::v0::{MessageFlags, PushNotification};
use revolt_result::ToRevoltError;

#[derive(Clone)]
#[allow(unused)]
pub struct MassMessageConsumer {
    db: Database,
    connection: Arc<Connection>,
    channel: Arc<Channel>,
}

impl MassMessageConsumer {
    /// Push to the users mentioned by @everyone or a role who want it
    async fn notify(
        &self,
        push: &PushNotification,
        users: impl Iterator<Item = &String>,
        skip: &HashSet<String>,
    ) -> Result<()> {
        let Some(context) = push_context(push) else {
            return Ok(());
        };

        let candidates: Vec<PushCandidate> = users
            .filter(|id| !skip.contains(*id))
            .map(|id| PushCandidate {
                user_id: id.clone(),
                mentioned: true,
            })
            .collect();

        let targets = filter_push_recipients(&self.db, &context, &candidates).await?;
        debug!("targets: {:?}", targets);
        send_message_push(self, &self.db, push, &targets).await
    }
}

#[async_trait]
impl Consumer for MassMessageConsumer {
    async fn create(db: Database, connection: Arc<Connection>, channel: Arc<Channel>) -> Self {
        Self {
            db,
            connection,
            channel,
        }
    }

    fn channel(&self) -> &Arc<Channel> {
        &self.channel
    }

    /// This consumer handles adding mentions for all the users affected by a mass mention ping, and then sends out push notifications.
    async fn consume(&self, delivery: Delivery) -> Result<()> {
        let mut payload: MassMessageSentPayload = serde_json::from_slice(&delivery.data)?;
        let config = revolt_config::config().await;

        for push in payload.notifications.iter_mut() {
            if let Ok(body) = render_notification_content(push, &self.db)
                .await
                .to_internal_error()
            {
                push.raw_body = Some(push.body.clone());
                push.body = body;
            }
        }

        debug!("Received mass message event");

        // We should only ever receive clumped messages from a single channel, so it's safe to reuse this many times.
        let mut query: Option<BulkDatabasePermissionQuery<'_>> = None;
        let query_db = self.db.clone();

        for push in payload.notifications {
            if query.is_none() {
                query = Some(
                    BulkDatabasePermissionQuery::from_server_id(&query_db, &payload.server_id)
                        .await
                        .from_channel_id(push.channel.id().to_string()) // wrong channel model, so fetch the right one
                        .await,
                );
            }

            let existing_mentions: HashSet<String, RandomState> =
                if let Some(ref mentions) = push.message.mentions {
                    HashSet::from_iter(mentions.iter().cloned())
                } else {
                    HashSet::new()
                };

            // Users mentioned by name, and users who chose All for the
            // channel, are notified by the message consumer
            let mut skip = existing_mentions.clone();
            skip.extend(
                self.db
                    .fetch_users_notified_of_all(push.channel.id(), Some(&payload.server_id))
                    .await?,
            );

            // KNOWN QUIRK: if you mention @online and role(s), the offline members with the role(s) wont get pinged
            if let Some(ref query) = query {
                let flags = MessageFlagsValue(push.message.flags);
                if flags.has(MessageFlags::MentionsEveryone) {
                    let mut db_query = self
                        .db
                        .fetch_all_members_chunked(&payload.server_id)
                        .await?;

                    let mut exhausted = false;
                    let ack_chnl = vec![push.channel.id().to_string()];
                    loop {
                        let mut chunk: Vec<Member> = vec![];
                        for _ in 0..config.pushd.mass_mention_chunk_size {
                            if let Some(member) = db_query.next().await {
                                chunk.push(member);
                            } else {
                                exhausted = true;
                                break;
                            }
                        }

                        let userids: Vec<String> =
                            chunk.iter().map(|member| member.id.user.clone()).collect();

                        debug!("Userids in chunk: {:?}", userids);

                        if let Err(err) = self
                            .db
                            .add_mention_to_many_unreads(push.channel.id(), &userids, &ack_chnl)
                            .await
                        {
                            revolt_config::capture_error(&err);
                        }

                        self.notify(&push, userids.iter(), &skip).await?;

                        if exhausted {
                            break;
                        }
                    }
                } else if let Some(roles) = &push.message.role_mentions {
                    // role mentions
                    let mut role_members = self
                        .db
                        .fetch_all_members_with_roles_chunked(&payload.server_id, roles)
                        .await?;

                    let mut chunk = vec![];
                    let mut exhausted = false;

                    while !exhausted {
                        chunk.clear();

                        for _ in 0..config.pushd.mass_mention_chunk_size {
                            if let Some(member) = role_members.next().await {
                                chunk.push(member);
                            } else {
                                exhausted = true;
                                break;
                            }
                        }

                        let mut q = query.clone().members(&chunk);
                        let viewing_members: Vec<String> = q
                            .members_can_see_channel()
                            .await
                            .iter()
                            .filter_map(|(uid, viewable)| viewable.then(|| uid.clone()))
                            .collect();

                        debug!("viewing members: {:?}", viewing_members);

                        self.notify(&push, viewing_members.iter(), &skip).await?;
                    }
                }
            }
        }

        Ok(())
    }
}
