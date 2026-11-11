use bson::{to_document, Document};
use futures::StreamExt;
use mongodb::options::{FindOptions, ReplaceOptions};
use revolt_result::Result;

use crate::{MongoDb, NotificationPrefs};

use super::AbstractNotificationPrefs;

static COL: &str = "notification_prefs";

#[async_trait]
impl AbstractNotificationPrefs for MongoDb {
    async fn set_notification_prefs(&self, prefs: &NotificationPrefs) -> Result<()> {
        let document = to_document(prefs).map_err(|_| create_database_error!("to_document", COL))?;
        self.col::<Document>(COL)
            .replace_one(doc! { "_id": &prefs.id }, document)
            .with_options(ReplaceOptions::builder().upsert(true).build())
            .await
            .map(|_| ())
            .map_err(|_| create_database_error!("replace_one", COL))
    }

    async fn fetch_notification_prefs(
        &self,
        user_ids: &[String],
    ) -> Result<Vec<NotificationPrefs>> {
        if user_ids.is_empty() {
            return Ok(vec![]);
        }

        query!(self, find, COL, doc! { "_id": { "$in": user_ids } })
    }

    async fn fetch_users_notified_of_all(
        &self,
        channel_id: &str,
        server_id: Option<&str>,
    ) -> Result<Vec<String>> {
        let mut filter = vec![doc! { "all": channel_id }];
        if let Some(server_id) = server_id {
            filter.push(doc! {
                "all": server_id,
                "mention": { "$ne": channel_id },
                "none": { "$ne": channel_id },
            });
        }

        Ok(self
            .col::<Document>(COL)
            .find(doc! { "$or": filter })
            .with_options(FindOptions::builder().projection(doc! { "_id": 1 }).build())
            .await
            .map_err(|_| create_database_error!("find", COL))?
            .filter_map(|doc| async move {
                doc.ok()
                    .and_then(|doc| doc.get_str("_id").ok().map(|id| id.to_string()))
            })
            .collect()
            .await)
    }

    async fn delete_notification_prefs(&self, user_id: &str) -> Result<()> {
        query!(self, delete_one_by_id, COL, user_id).map(|_| ())
    }
}
