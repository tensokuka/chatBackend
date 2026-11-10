use std::{collections::HashMap, sync::Arc};

use crate::utils::Consumer;

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use base64::{
    Engine as _,
    engine::{self},
};
use lapin::{Channel as AMQPChannel, Connection, message::Delivery};
use log::{info, warn};
use revolt_database::{Database, events::rabbit::*, util::format_display_name};
use web_push::{
    ContentEncoding, IsahcWebPushClient, SubscriptionInfo, SubscriptionKeys, Urgency,
    VapidSignatureBuilder, WebPushClient, WebPushError, WebPushMessageBuilder,
};

/// Host of a push endpoint, for logs (the full endpoint is a credential)
fn url_host(endpoint: &str) -> &str {
    endpoint
        .split_once("://")
        .map_or(endpoint, |(_, rest)| rest)
        .split('/')
        .next()
        .unwrap_or_default()
}

#[derive(Clone)]
#[allow(unused)]
pub struct VapidOutboundConsumer {
    db: Database,
    connection: Arc<Connection>,
    channel: Arc<AMQPChannel>,
    client: IsahcWebPushClient,
    pkey: Arc<Vec<u8>>,
}

#[async_trait]
impl Consumer for VapidOutboundConsumer {
    async fn create(db: Database, connection: Arc<Connection>, channel: Arc<AMQPChannel>) -> Self {
        let config = revolt_config::config().await;

        if config.pushd.vapid.private_key.is_empty() || config.pushd.vapid.public_key.is_empty() {
            panic!("no Vapid keys present");
        }

        let web_push_private_key = Arc::new(
            engine::general_purpose::URL_SAFE_NO_PAD
                .decode(config.pushd.vapid.private_key)
                .expect("valid `VAPID_PRIVATE_KEY`"),
        );

        Self {
            db,
            connection,
            channel,
            client: IsahcWebPushClient::new().unwrap(),
            pkey: web_push_private_key,
        }
    }

    fn channel(&self) -> &Arc<AMQPChannel> {
        &self.channel
    }

    async fn consume(&self, delivery: Delivery) -> Result<()> {
        let payload: PayloadToService = serde_json::from_slice(&delivery.data)?;

        let subscription = SubscriptionInfo {
            endpoint: payload
                .extras
                .get("endpoint")
                .ok_or_else(|| anyhow!("missing endpoint"))?
                .clone(),
            keys: SubscriptionKeys {
                auth: payload.token,
                p256dh: payload
                    .extras
                    .get("p256dh")
                    .ok_or_else(|| anyhow!("missing p256dh"))?
                    .clone(),
            },
        };

        let payload_body = match payload.notification {
            PayloadKind::FRReceived(alert) => {
                let name = alert
                    .from_user
                    .display_name
                    .or(Some(format!(
                        "{}#{}",
                        alert.from_user.username, alert.from_user.discriminator
                    )))
                    .clone()
                    .ok_or_else(|| anyhow!("missing name"))?;

                let mut body = HashMap::new();
                body.insert("body", format!("{} sent you a friend request", name));

                serde_json::to_string(&body)?
            }
            PayloadKind::FRAccepted(alert) => {
                let name = alert
                    .accepted_user
                    .display_name
                    .or(Some(format!(
                        "{}#{}",
                        alert.accepted_user.username, alert.accepted_user.discriminator
                    )))
                    .clone()
                    .ok_or_else(|| anyhow!("missing name"))?;

                let mut body = HashMap::new();
                body.insert("body", format!("{} accepted your friend request", name));

                serde_json::to_string(&body)?
            }
            PayloadKind::Generic(alert) => serde_json::to_string(&alert)?,
            PayloadKind::MessageNotification(alert) => serde_json::to_string(&alert)?,
            PayloadKind::DmCallStartEnd(alert) => {
                let initiator_name = if let Some(server_id) =
                    self.db.fetch_channel(&alert.channel_id).await?.server()
                {
                    format_display_name(&self.db, &alert.initiator_id, Some(server_id)).await
                } else {
                    format_display_name(&self.db, &alert.initiator_id, None).await
                }?;

                let channel = self.db.fetch_channel(&alert.channel_id).await?;
                let mut body = HashMap::new();

                match channel {
                    revolt_database::Channel::DirectMessage { .. } => {
                        body.insert("body", format!("{} is calling you", initiator_name));
                    }
                    revolt_database::Channel::Group { name, .. } => {
                        body.insert(
                            "body",
                            format!("{} is calling your group, {}", initiator_name, name),
                        );
                    }
                    _ => bail!("Invalid DmCallStart/End channel type"),
                }

                serde_json::to_string(&body)?
            }
            PayloadKind::BadgeUpdate(_) => {
                bail!("Vapid cannot handle badge updates and they should not be sent here.");
            }
        };

        // Apple's push service expects a `sub` contact claim; others accept it
        let config = revolt_config::config().await;
        let mut signature = VapidSignatureBuilder::from_pem(
            std::io::Cursor::new(self.pkey.as_ref()),
            &subscription,
        )?;
        signature.add_claim("sub", config.hosts.app.clone());
        let signature = signature.build()?;

        let mut builder = WebPushMessageBuilder::new(&subscription);
        builder.set_vapid_signature(signature);

        // aes128gcm (RFC 8291) is the standard every current browser supports;
        // the legacy aesgcm draft can be accepted by a push service and then
        // fail to decrypt on the device, which nothing here would notice
        builder.set_payload(ContentEncoding::Aes128Gcm, payload_body.as_bytes());
        builder.set_urgency(Urgency::High);

        let msg = builder.build()?;

        let host = url_host(&subscription.endpoint);
        match self.client.send(msg).await {
            Ok(()) => {
                info!(
                    "Web push accepted by {host} for session {}",
                    payload.session_id
                );
            }
            // The subscription is no longer valid, so stop sending to it
            Err(
                err @ (WebPushError::Unauthorized
                | WebPushError::EndpointNotValid
                | WebPushError::EndpointNotFound),
            ) => {
                warn!(
                    "Web push rejected by {host} for session {} ({err}), removing subscription",
                    payload.session_id
                );
                if let Err(err) = self
                    .db
                    .remove_push_subscription_by_session_id(&payload.session_id)
                    .await
                {
                    revolt_config::capture_error(&err);
                }
            }
            Err(err) => {
                bail!(
                    "Web push to {host} for session {} failed: {err:?}",
                    payload.session_id
                );
            }
        };

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::url_host;

    #[test]
    fn url_host_keeps_only_the_host() {
        assert_eq!(
            url_host("https://web.push.apple.com/QBMbCXS8d-L8bhiMvDCn02y"),
            "web.push.apple.com"
        );
        assert_eq!(
            url_host("https://fcm.googleapis.com/fcm/send/abc:def"),
            "fcm.googleapis.com"
        );
        assert_eq!(url_host("no-scheme/path"), "no-scheme");
    }
}
