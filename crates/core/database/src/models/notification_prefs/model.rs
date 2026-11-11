use std::collections::HashMap;

use serde::Deserialize;

auto_derived!(
    /// A user's per-channel and per-server notification choices, copied from
    /// the `notifications` key of their synced settings so the push service
    /// can read and query them
    #[derive(Default)]
    pub struct NotificationPrefs {
        /// User id
        #[serde(rename = "_id")]
        pub id: String,
        /// Channels and servers set to notify for every message
        #[serde(default)]
        pub all: Vec<String>,
        /// Channels and servers set to notify for mentions only
        #[serde(default)]
        pub mention: Vec<String>,
        /// Channels and servers set to never notify
        #[serde(default)]
        pub none: Vec<String>,
        /// Muted channels and servers, with when the mute ends in Unix
        /// milliseconds, or `None` if it doesn't
        #[serde(default)]
        pub muted: HashMap<String, Option<i64>>,
    }
);

/// How much a user wants to hear about a channel
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationLevel {
    All,
    Mention,
    None,
}

/// The `notifications` settings as the web client stores them
#[derive(Deserialize, Default)]
#[serde(default)]
struct SyncedNotificationOptions {
    server: HashMap<String, Option<String>>,
    channel: HashMap<String, Option<String>>,
    server_mutes: HashMap<String, Option<SyncedMute>>,
    channel_mutes: HashMap<String, Option<SyncedMute>>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct SyncedMute {
    until: Option<f64>,
}

impl NotificationPrefs {
    /// Read the client's synced `notifications` value; anything that can't be
    /// read is ignored, as the client does
    pub fn from_settings(user_id: &str, json: &str) -> NotificationPrefs {
        let options: SyncedNotificationOptions = serde_json::from_str(json).unwrap_or_default();
        let mut prefs = NotificationPrefs {
            id: user_id.to_string(),
            ..Default::default()
        };

        for (id, state) in options.server.into_iter().chain(options.channel) {
            match state.as_deref() {
                Some("all") => prefs.all.push(id),
                Some("mention") => prefs.mention.push(id),
                Some("none") => prefs.none.push(id),
                _ => {}
            }
        }

        for (id, mute) in options.server_mutes.into_iter().chain(options.channel_mutes) {
            if let Some(mute) = mute {
                prefs.muted.insert(id, mute.until.map(|until| until as i64));
            }
        }

        prefs.all.sort();
        prefs.mention.sort();
        prefs.none.sort();
        prefs
    }

    fn level_of(&self, id: &str) -> Option<NotificationLevel> {
        if self.all.iter().any(|x| x == id) {
            Some(NotificationLevel::All)
        } else if self.mention.iter().any(|x| x == id) {
            Some(NotificationLevel::Mention)
        } else if self.none.iter().any(|x| x == id) {
            Some(NotificationLevel::None)
        } else {
            None
        }
    }

    fn is_muted_id(&self, id: &str, now: i64) -> bool {
        match self.muted.get(id) {
            Some(None) => true,
            Some(Some(until)) => *until > now,
            None => false,
        }
    }

    /// The channel's own choice, else the server's, else the default
    pub fn level(
        &self,
        channel_id: &str,
        server_id: Option<&str>,
        default: NotificationLevel,
    ) -> NotificationLevel {
        self.level_of(channel_id)
            .or_else(|| server_id.and_then(|id| self.level_of(id)))
            .unwrap_or(default)
    }

    /// Whether the channel, or the server it's in, is muted at `now` (Unix ms)
    pub fn is_muted(&self, channel_id: &str, server_id: Option<&str>, now: i64) -> bool {
        self.is_muted_id(channel_id, now) || server_id.is_some_and(|id| self.is_muted_id(id, now))
    }
}

#[cfg(test)]
mod tests {
    use super::{NotificationLevel, NotificationPrefs};

    #[test]
    fn reads_client_settings() {
        let prefs = NotificationPrefs::from_settings(
            "user",
            r#"{
                "server": {"S1": "all", "S2": "none", "S3": "mention"},
                "channel": {"C1": "none", "C2": "all", "C3": null, "C4": "bogus"},
                "server_mutes": {"S4": {}},
                "channel_mutes": {"C5": {"until": 2000}, "C6": null}
            }"#,
        );

        // A channel's own choice wins over its server's
        assert_eq!(prefs.level("C1", Some("S1"), NotificationLevel::Mention), NotificationLevel::None);
        assert_eq!(prefs.level("C2", Some("S2"), NotificationLevel::Mention), NotificationLevel::All);
        // Otherwise the server's, then the default
        assert_eq!(prefs.level("C9", Some("S1"), NotificationLevel::Mention), NotificationLevel::All);
        assert_eq!(prefs.level("C3", Some("S9"), NotificationLevel::Mention), NotificationLevel::Mention);
        assert_eq!(prefs.level("C4", None, NotificationLevel::All), NotificationLevel::All);

        // Server mutes cover their channels; timed mutes end
        assert!(prefs.is_muted("C9", Some("S4"), 0));
        assert!(prefs.is_muted("C5", None, 1999));
        assert!(!prefs.is_muted("C5", None, 2000));
        assert!(!prefs.is_muted("C6", None, 0));
    }

    #[test]
    fn ignores_unreadable_settings() {
        let prefs = NotificationPrefs::from_settings("user", "not json");
        assert!(prefs.all.is_empty() && prefs.muted.is_empty());
    }
}
