use revolt_result::Result;

use crate::NotificationPrefs;

#[cfg(feature = "mongodb")]
mod mongodb;
mod reference;

#[async_trait]
pub trait AbstractNotificationPrefs: Sync + Send {
    /// Replace a user's notification preferences
    async fn set_notification_prefs(&self, prefs: &NotificationPrefs) -> Result<()>;

    /// Fetch the preferences of the given users; users without any are left out
    async fn fetch_notification_prefs(&self, user_ids: &[String])
        -> Result<Vec<NotificationPrefs>>;

    /// Users whose choice for a channel is All: set on the channel itself, or
    /// on its server without the channel overriding it
    async fn fetch_users_notified_of_all(
        &self,
        channel_id: &str,
        server_id: Option<&str>,
    ) -> Result<Vec<String>>;

    /// Delete a user's notification preferences
    async fn delete_notification_prefs(&self, user_id: &str) -> Result<()>;
}
