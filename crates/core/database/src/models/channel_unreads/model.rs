auto_derived!(
    /// Channel Unread
    pub struct ChannelUnread {
        /// Composite key pointing to a user's view of a channel
        #[serde(rename = "_id")]
        pub id: ChannelCompositeKey,

        /// Id of the last message read in this channel by a user
        #[serde(skip_serializing_if = "Option::is_none")]
        pub last_id: Option<String>,
        /// Array of message ids that mention the user
        #[serde(skip_serializing_if = "Option::is_none")]
        pub mentions: Option<Vec<String>>,
    }

    /// Composite primary key consisting of channel and user id
    #[derive(Hash)]
    pub struct ChannelCompositeKey {
        /// Channel Id
        pub channel: String,
        /// User Id
        pub user: String,
    }
);

#[cfg(test)]
mod tests {
    // Used by database_test!
    use lapin::{options::ExchangeDeclareOptions, types::FieldTable, ExchangeKind};

    #[tokio::test]
    async fn remove_mentions_of_deleted_messages() {
        database_test!(|db| async move {
            let ids = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
            let users = ids(&["alice", "bob"]);
            for user in &users {
                db.acknowledge_message("chan", user, "m0").await.unwrap();
            }

            db.add_mention_to_many_unreads("chan", &users, &ids(&["m1", "m2", "m3"]))
                .await
                .unwrap();
            db.add_mention_to_unread("other", "alice", &ids(&["m2"]))
                .await
                .unwrap();

            db.remove_mentions("chan", &ids(&["m2", "m3"]))
                .await
                .unwrap();

            for user in &users {
                let unread = db.fetch_unread(user, "chan").await.unwrap().unwrap();
                assert_eq!(unread.mentions, Some(ids(&["m1"])));
            }

            // Other channels keep their mentions
            let other = db.fetch_unread("alice", "other").await.unwrap().unwrap();
            assert_eq!(other.mentions, Some(ids(&["m2"])));
        });
    }
}
