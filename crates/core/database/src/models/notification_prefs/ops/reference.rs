use revolt_result::Result;

use crate::{NotificationLevel, NotificationPrefs, ReferenceDb};

use super::AbstractNotificationPrefs;

#[async_trait]
impl AbstractNotificationPrefs for ReferenceDb {
    async fn set_notification_prefs(&self, prefs: &NotificationPrefs) -> Result<()> {
        self.notification_prefs
            .lock()
            .await
            .insert(prefs.id.clone(), prefs.clone());
        Ok(())
    }

    async fn fetch_notification_prefs(
        &self,
        user_ids: &[String],
    ) -> Result<Vec<NotificationPrefs>> {
        let prefs = self.notification_prefs.lock().await;
        Ok(user_ids
            .iter()
            .filter_map(|id| prefs.get(id).cloned())
            .collect())
    }

    async fn fetch_users_notified_of_all(
        &self,
        channel_id: &str,
        server_id: Option<&str>,
    ) -> Result<Vec<String>> {
        let prefs = self.notification_prefs.lock().await;
        Ok(prefs
            .values()
            .filter(|prefs| {
                prefs.level(channel_id, server_id, NotificationLevel::None)
                    == NotificationLevel::All
            })
            .map(|prefs| prefs.id.clone())
            .collect())
    }

    async fn delete_notification_prefs(&self, user_id: &str) -> Result<()> {
        self.notification_prefs.lock().await.remove(user_id);
        Ok(())
    }
}
