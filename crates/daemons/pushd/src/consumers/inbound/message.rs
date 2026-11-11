use std::{collections::HashSet, sync::Arc};

use crate::utils::{Consumer, push_context, render_notification_content, send_message_push};
use anyhow::Result;
use async_trait::async_trait;
use lapin::{Channel, Connection, message::Delivery};
use log::debug;
use revolt_database::{
    Database, MessageFlagsValue,
    events::rabbit::*,
    util::{
        bulk_permissions::BulkDatabasePermissionQuery,
        push_filter::{PushCandidate, filter_push_recipients},
    },
};
use revolt_models::v0::{MessageFlags, PushNotification};

#[derive(Clone)]
#[allow(unused)]
pub struct MessageConsumer {
    db: Database,
    connection: Arc<Connection>,
    channel: Arc<Channel>,
}

impl MessageConsumer {
    /// Users who chose All for this server channel and can see it. Everyone
    /// else mentioned by @everyone or a role is handled by the mass mention
    /// consumer, which leaves these users out.
    async fn users_notified_of_all(
        &self,
        push: &PushNotification,
        server_id: &str,
        known: &HashSet<String>,
    ) -> Result<Vec<PushCandidate>> {
        let channel_id = push.channel.id();
        let user_ids: Vec<String> = self
            .db
            .fetch_users_notified_of_all(channel_id, Some(server_id))
            .await?
            .into_iter()
            .filter(|id| !known.contains(id))
            .collect();

        if user_ids.is_empty() {
            return Ok(vec![]);
        }

        let server = self.db.fetch_server(server_id).await?;
        let channel = self.db.fetch_channel(channel_id).await?;
        let members = self.db.fetch_members(server_id, &user_ids).await?;
        let can_see = BulkDatabasePermissionQuery::new(&self.db, server)
            .channel(&channel)
            .members(&members)
            .members_can_see_channel()
            .await;

        let everyone = MessageFlagsValue(push.message.flags).has(MessageFlags::MentionsEveryone);
        let roles: HashSet<&String> = push.message.role_mentions.iter().flatten().collect();

        Ok(members
            .iter()
            .filter(|member| can_see.get(&member.id.user).copied().unwrap_or(false))
            .map(|member| PushCandidate {
                user_id: member.id.user.clone(),
                mentioned: everyone || member.roles.iter().any(|role| roles.contains(role)),
            })
            .collect())
    }
}

#[async_trait]
impl Consumer for MessageConsumer {
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

    /// Works out who to notify of a message and queues their pushes
    async fn consume(&self, delivery: Delivery) -> Result<()> {
        let mut payload: MessageSentPayload = serde_json::from_slice(&delivery.data)?;

        if let Ok(body) = render_notification_content(&payload.notification, &self.db).await {
            payload.notification.raw_body = Some(payload.notification.body);
            payload.notification.body = body;
        }

        debug!("Received message event on origin");

        let push = &payload.notification;
        let Some(context) = push_context(push) else {
            return Ok(());
        };

        // DM and group members, or users mentioned by name
        let mentions: HashSet<&String> = push.message.mentions.iter().flatten().collect();
        let mut candidates: Vec<PushCandidate> = payload
            .users
            .iter()
            .map(|id| PushCandidate {
                user_id: id.clone(),
                mentioned: mentions.contains(id),
            })
            .collect();

        if let Some(server_id) = context.server_id {
            let known: HashSet<String> = payload.users.iter().cloned().collect();
            candidates.extend(self.users_notified_of_all(push, server_id, &known).await?);
        }

        let targets = filter_push_recipients(&self.db, &context, &candidates).await?;
        send_message_push(self, &self.db, push, &targets).await
    }
}
