use std::collections::{HashMap, HashSet};

use anyhow::Result;
use log::debug;
use redis_kiss::get_connection;
use revolt_database::{
    Database, NotificationLevel, events::rabbit::*, util::push_filter::PushContext,
};
use revolt_models::v0::{Channel, PushNotification};

use super::Consumer;

/// After a push from a channel makes a sound, further pushes from it within
/// this many seconds replace it quietly
const QUIET_REPEAT_SECONDS: u64 = 60;

/// Where a message was sent, for the push filter; `None` for channels that
/// never notify
pub fn push_context(push: &PushNotification) -> Option<PushContext<'_>> {
    let (server_id, default_level) = match &push.channel {
        Channel::TextChannel { server, .. } => (Some(server.as_str()), NotificationLevel::Mention),
        Channel::DirectMessage { .. } | Channel::Group { .. } => (None, NotificationLevel::All),
        _ => return None,
    };

    Some(PushContext {
        channel_id: push.channel.id(),
        server_id,
        default_level,
        author_id: &push.message.author,
    })
}

/// Whether this user already had a push from this channel that made a sound
/// in the last minute. If not, this push will make one and starts the window.
async fn is_quiet_repeat(user_id: &str, channel_id: &str) -> bool {
    let Ok(conn) = get_connection().await else {
        return false;
    };
    let mut conn = conn.into_inner();

    // SET NX only succeeds when no window is open
    let opened: Result<Option<String>, _> = redis_kiss::redis::cmd("SET")
        .arg(format!("push_window:{user_id}:{channel_id}"))
        .arg(1)
        .arg("NX")
        .arg("EX")
        .arg(QUIET_REPEAT_SECONDS)
        .query_async(&mut conn)
        .await;

    matches!(opened, Ok(None))
}

/// Queue a message push to every push-enabled session of these users
pub async fn send_message_push<C: Consumer>(
    consumer: &C,
    db: &Database,
    push: &PushNotification,
    users: &[String],
) -> Result<()> {
    if users.is_empty() {
        return Ok(());
    }

    let sessions = db.fetch_sessions_with_subscription(users).await?;
    let config = revolt_config::config().await;
    let channel_id = push.channel.id();

    // Decided once per user, so all their devices agree
    let mut quiet: HashMap<String, bool> = HashMap::new();
    let with_push: HashSet<&String> = sessions
        .iter()
        .filter(|s| s.subscription.is_some())
        .map(|s| &s.user_id)
        .collect();
    for user_id in with_push {
        quiet.insert(user_id.clone(), is_quiet_repeat(user_id, channel_id).await);
    }

    for session in sessions {
        let Some(sub) = session.subscription else {
            continue;
        };

        let mut notification = push.clone();
        notification.quiet = quiet.get(&session.user_id).copied().unwrap_or(false);
        debug!(
            "Pushing to session {} of {} (quiet: {})",
            session.id, session.user_id, notification.quiet
        );

        let mut sendable = PayloadToService {
            notification: PayloadKind::MessageNotification(notification),
            token: sub.auth,
            user_id: session.user_id,
            session_id: session.id,
            extras: HashMap::new(),
        };

        let routing_key = match sub.endpoint.as_str() {
            "apn" => &config.pushd.apn.queue,
            "fcm" => &config.pushd.fcm.queue,
            endpoint => {
                sendable.extras.insert("p256dh".to_string(), sub.p256dh);
                sendable
                    .extras
                    .insert("endpoint".to_string(), endpoint.to_string());

                &config.pushd.vapid.queue
            }
        };

        let payload = serde_json::to_string(&sendable)?;
        consumer
            .publish_message(payload.as_bytes(), &config.pushd.exchange, routing_key)
            .await?;
    }

    Ok(())
}
