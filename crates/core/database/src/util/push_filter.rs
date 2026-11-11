//! Who a message should be pushed to, from each user's notification
//! settings, status and whether one of their devices is in use

use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

use revolt_result::Result;

use crate::{Database, NotificationLevel, NotificationPrefs, Presence, RelationshipStatus, User};

/// Someone who may be notified of a message
#[derive(Debug, Clone)]
pub struct PushCandidate {
    pub user_id: String,
    /// Mentioned by name, by a role they have, or by @everyone
    pub mentioned: bool,
}

/// Where a message was sent
pub struct PushContext<'a> {
    pub channel_id: &'a str,
    pub server_id: Option<&'a str>,
    /// Level for users who haven't chosen one: All in DMs and groups,
    /// Mention in servers
    pub default_level: NotificationLevel,
    pub author_id: &'a str,
}

#[derive(Debug, PartialEq, Eq)]
enum Decision {
    Skip,
    Push,
    /// Push unless one of their devices is in use
    PushIfAway,
}

fn decide(
    context: &PushContext<'_>,
    candidate: &PushCandidate,
    prefs: Option<&NotificationPrefs>,
    user: Option<&User>,
    now: i64,
) -> Decision {
    if candidate.user_id == context.author_id {
        return Decision::Skip;
    }

    // Channel and server choices, as the client applies them
    if let Some(prefs) = prefs {
        if prefs.is_muted(context.channel_id, context.server_id, now) {
            return Decision::Skip;
        }
    }

    let level = prefs
        .map(|prefs| prefs.level(context.channel_id, context.server_id, context.default_level))
        .unwrap_or(context.default_level);

    match level {
        NotificationLevel::None => return Decision::Skip,
        NotificationLevel::Mention if !candidate.mentioned => return Decision::Skip,
        _ => {}
    }

    let Some(user) = user else {
        return Decision::Skip;
    };

    if user.relationship_with(context.author_id) == RelationshipStatus::Blocked {
        return Decision::Skip;
    }

    match user.status.as_ref().and_then(|status| status.presence.as_ref()) {
        Some(Presence::Busy) => Decision::Skip,
        Some(Presence::Focus) if !candidate.mentioned => Decision::Skip,
        // Chosen by hand to say they're away
        Some(Presence::Idle) => Decision::Push,
        _ => Decision::PushIfAway,
    }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

/// The candidates who should get a push for this message
pub async fn filter_push_recipients(
    db: &Database,
    context: &PushContext<'_>,
    candidates: &[PushCandidate],
) -> Result<Vec<String>> {
    // One entry per user; being mentioned anywhere counts
    let mut merged: HashMap<&str, PushCandidate> = HashMap::new();
    for candidate in candidates {
        merged
            .entry(&candidate.user_id)
            .and_modify(|c| c.mentioned |= candidate.mentioned)
            .or_insert_with(|| candidate.clone());
    }

    let ids: Vec<String> = merged.keys().map(|id| id.to_string()).collect();
    if ids.is_empty() {
        return Ok(vec![]);
    }

    let prefs: HashMap<String, NotificationPrefs> = db
        .fetch_notification_prefs(&ids)
        .await?
        .into_iter()
        .map(|prefs| (prefs.id.clone(), prefs))
        .collect();

    let users: HashMap<String, User> = db
        .fetch_users(&ids)
        .await?
        .into_iter()
        .map(|user| (user.id.clone(), user))
        .collect();

    let now = now_millis();
    let mut push = vec![];
    let mut if_away = vec![];
    for (id, candidate) in &merged {
        match decide(context, candidate, prefs.get(*id), users.get(*id), now) {
            Decision::Skip => {}
            Decision::Push => push.push(id.to_string()),
            Decision::PushIfAway => if_away.push(id.to_string()),
        }
    }

    let active: HashSet<String> = revolt_presence::filter_active(&if_away).await;
    push.extend(if_away.into_iter().filter(|id| !active.contains(id)));

    debug!(
        "Push for {} in {}: {} of {} candidates ({} have a device in use)",
        context.author_id,
        context.channel_id,
        push.len(),
        merged.len(),
        active.len()
    );

    Ok(push)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Relationship, UserStatus};

    fn user(presence: Option<Presence>) -> User {
        User {
            id: "me".to_string(),
            username: "me".to_string(),
            discriminator: "0001".to_string(),
            status: presence.map(|presence| UserStatus {
                presence: Some(presence),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn context(default_level: NotificationLevel) -> PushContext<'static> {
        PushContext {
            channel_id: "C",
            server_id: Some("S"),
            default_level,
            author_id: "author",
        }
    }

    fn candidate(mentioned: bool) -> PushCandidate {
        PushCandidate {
            user_id: "me".to_string(),
            mentioned,
        }
    }

    fn prefs(json: &str) -> NotificationPrefs {
        NotificationPrefs::from_settings("me", json)
    }

    #[test]
    fn follows_channel_and_server_choices() {
        let ctx = context(NotificationLevel::Mention);
        let me = user(None);

        // Default for servers is mentions only
        assert_eq!(decide(&ctx, &candidate(false), None, Some(&me), 0), Decision::Skip);
        assert_eq!(decide(&ctx, &candidate(true), None, Some(&me), 0), Decision::PushIfAway);

        // All on the server, but None on this channel
        let p = prefs(r#"{"server":{"S":"all"},"channel":{"C":"none"}}"#);
        assert_eq!(decide(&ctx, &candidate(true), Some(&p), Some(&me), 0), Decision::Skip);

        // All on the channel
        let p = prefs(r#"{"channel":{"C":"all"}}"#);
        assert_eq!(decide(&ctx, &candidate(false), Some(&p), Some(&me), 0), Decision::PushIfAway);

        // Muted server, even when mentioned
        let p = prefs(r#"{"channel":{"C":"all"},"server_mutes":{"S":{}}}"#);
        assert_eq!(decide(&ctx, &candidate(true), Some(&p), Some(&me), 0), Decision::Skip);

        // A mute that has ended
        let p = prefs(r#"{"channel_mutes":{"C":{"until":10}}}"#);
        assert_eq!(decide(&ctx, &candidate(true), Some(&p), Some(&me), 11), Decision::PushIfAway);
    }

    #[test]
    fn follows_status() {
        let ctx = context(NotificationLevel::All);

        let busy = user(Some(Presence::Busy));
        assert_eq!(decide(&ctx, &candidate(true), None, Some(&busy), 0), Decision::Skip);

        let focus = user(Some(Presence::Focus));
        assert_eq!(decide(&ctx, &candidate(false), None, Some(&focus), 0), Decision::Skip);
        assert_eq!(decide(&ctx, &candidate(true), None, Some(&focus), 0), Decision::PushIfAway);

        let idle = user(Some(Presence::Idle));
        assert_eq!(decide(&ctx, &candidate(false), None, Some(&idle), 0), Decision::Push);

        let invisible = user(Some(Presence::Invisible));
        assert_eq!(decide(&ctx, &candidate(false), None, Some(&invisible), 0), Decision::PushIfAway);
    }

    #[test]
    fn skips_author_and_blocked_users() {
        let ctx = context(NotificationLevel::All);

        let mut author = candidate(true);
        author.user_id = "author".to_string();
        assert_eq!(decide(&ctx, &author, None, Some(&user(None)), 0), Decision::Skip);

        let mut me = user(None);
        me.relations = Some(vec![Relationship {
            id: "author".to_string(),
            status: RelationshipStatus::Blocked,
        }]);
        assert_eq!(decide(&ctx, &candidate(true), None, Some(&me), 0), Decision::Skip);
    }
}
