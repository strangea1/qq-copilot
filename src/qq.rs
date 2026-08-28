use std::{
    sync::Arc,
    time::{Duration, Instant},
};

#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{
    sync::Mutex,
    time::{MissedTickBehavior, interval_at, sleep, timeout},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{
    config::QqConfig,
    db::{Database, GatewayResume},
    service::BridgeService,
};

const OP_DISPATCH: u64 = 0;
const OP_HEARTBEAT: u64 = 1;
const OP_IDENTIFY: u64 = 2;
const OP_RESUME: u64 = 6;
const OP_RECONNECT: u64 = 7;
const OP_INVALID_SESSION: u64 = 9;
const OP_HELLO: u64 = 10;
const OP_HEARTBEAT_ACK: u64 = 11;

#[derive(Debug, Clone)]
pub struct SendReceipt {
    pub message_id: String,
}

#[derive(Debug, Clone)]
pub struct ApprovalButtons {
    pub markdown: String,
    pub allow_button_data: String,
    pub deny_button_data: String,
}

#[derive(Debug, Clone)]
pub struct ChoiceButton {
    pub label: String,
    pub button_data: String,
}

#[derive(Debug, Clone)]
pub struct ChoiceButtons {
    pub markdown: String,
    pub button_id_prefix: String,
    pub choices: Vec<ChoiceButton>,
}

#[async_trait]
pub trait QqMessenger: Send + Sync {
    async fn send_text(
        &self,
        user_openid: &str,
        content: &str,
        reply_to_message_id: Option<&str>,
        message_sequence: u32,
    ) -> Result<SendReceipt>;

    async fn send_approval_buttons(
        &self,
        user_openid: &str,
        buttons: &ApprovalButtons,
    ) -> Result<SendReceipt>;

    async fn send_choice_buttons(
        &self,
        user_openid: &str,
        buttons: &ChoiceButtons,
        reply_to_message_id: Option<&str>,
        message_sequence: u32,
    ) -> Result<SendReceipt>;

    async fn send_typing(&self, user_openid: &str, duration_seconds: u8) -> Result<()>;

    async fn acknowledge_interaction(&self, interaction_id: &str, code: u8) -> Result<()>;
}

pub struct QqClient {
    config: QqConfig,
    app_secret: String,
    http: Client,
    token: Mutex<Option<CachedToken>>,
}

struct CachedToken {
    value: String,
    refresh_at: Instant,
}

impl QqClient {
    pub fn new(config: QqConfig, app_secret: String) -> Result<Self> {
        let http = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .user_agent(concat!("qq-copilot-remote/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("failed to build QQ HTTP client")?;
        Ok(Self {
            config,
            app_secret,
            http,
            token: Mutex::new(None),
        })
    }

    pub async fn gateway_url(&self) -> Result<String> {
        let token = self.access_token().await?;
        let response = self
            .http
            .get(&self.config.gateway_endpoint)
            .header("Authorization", format!("QQBot {token}"))
            .send()
            .await
            .context("failed to request QQ Gateway URL")?;
        if !response.status().is_success() {
            bail!(
                "QQ Gateway URL request failed with HTTP {}",
                response.status()
            );
        }
        let body: Value = response
            .json()
            .await
            .context("QQ Gateway URL response was not valid JSON")?;
        body.get("url")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .context("QQ Gateway URL response omitted url")
    }

    pub async fn access_token(&self) -> Result<String> {
        let mut cached = self.token.lock().await;
        if let Some(token) = cached.as_ref()
            && Instant::now() < token.refresh_at
        {
            return Ok(token.value.clone());
        }

        let body = json!({
            "appId": self.config.app_id,
            "clientSecret": self.app_secret,
        });
        let response = self
            .http
            .post(&self.config.token_endpoint)
            .json(&body)
            .send()
            .await
            .context("failed to request QQ App Access Token")?;
        if !response.status().is_success() {
            bail!(
                "QQ App Access Token request failed with HTTP {}",
                response.status()
            );
        }
        let body: Value = response
            .json()
            .await
            .context("QQ App Access Token response was not valid JSON")?;
        let value = body
            .get("access_token")
            .or_else(|| body.get("accessToken"))
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .context("QQ App Access Token response omitted access_token")?
            .to_owned();
        let expires_in = parse_u64(
            body.get("expires_in")
                .or_else(|| body.get("expiresIn"))
                .context("QQ App Access Token response omitted expires_in")?,
        )
        .context("QQ App Access Token expires_in was invalid")?;
        let valid_for = expires_in.saturating_sub(self.config.token_refresh_skew_seconds);
        if valid_for == 0 {
            bail!("QQ App Access Token expires too soon");
        }
        *cached = Some(CachedToken {
            value: value.clone(),
            refresh_at: Instant::now() + Duration::from_secs(valid_for),
        });
        Ok(value)
    }

    async fn invalidate_token(&self) {
        *self.token.lock().await = None;
    }

    async fn send_once(
        &self,
        token: &str,
        user_openid: &str,
        body: &Value,
    ) -> Result<reqwest::Response> {
        let endpoint = format!(
            "{}/v2/users/{}/messages",
            self.config.api_base.trim_end_matches('/'),
            urlencoding::encode(user_openid)
        );
        self.http
            .post(endpoint)
            .header("Authorization", format!("QQBot {token}"))
            .json(body)
            .send()
            .await
            .context("failed to send QQ C2C message")
    }

    async fn send_message_body(&self, user_openid: &str, body: &Value) -> Result<SendReceipt> {
        for attempt in 0..2 {
            let token = self.access_token().await?;
            let response = self.send_once(&token, user_openid, body).await?;
            if response.status() == StatusCode::UNAUTHORIZED && attempt == 0 {
                self.invalidate_token().await;
                continue;
            }
            if !response.status().is_success() {
                let status = response.status();
                let error = response.json::<QqApiError>().await.ok();
                if let Some(error) = error {
                    bail!(
                        "QQ message delivery failed with HTTP {status}, code {}",
                        error.code.unwrap_or_else(|| "unknown".to_owned())
                    );
                }
                bail!("QQ message delivery failed with HTTP {status}");
            }
            let body: Value = response
                .json()
                .await
                .context("QQ message response was not valid JSON")?;
            let message_id = body
                .get("id")
                .or_else(|| body.get("message_id"))
                .and_then(Value::as_str)
                .context("QQ message response omitted message ID")?
                .to_owned();
            return Ok(SendReceipt { message_id });
        }
        bail!("QQ message delivery remained unauthorized after token refresh")
    }

    async fn acknowledge_interaction_once(
        &self,
        token: &str,
        interaction_id: &str,
        code: u8,
    ) -> Result<reqwest::Response> {
        let endpoint = format!(
            "{}/interactions/{}",
            self.config.api_base.trim_end_matches('/'),
            urlencoding::encode(interaction_id)
        );
        self.http
            .put(endpoint)
            .header("Authorization", format!("QQBot {token}"))
            .json(&json!({"code": code}))
            .send()
            .await
            .context("failed to acknowledge QQ interaction")
    }
}

#[async_trait]
impl QqMessenger for QqClient {
    async fn send_text(
        &self,
        user_openid: &str,
        content: &str,
        reply_to_message_id: Option<&str>,
        message_sequence: u32,
    ) -> Result<SendReceipt> {
        let mut body = json!({
            "content": content,
            "msg_type": 0,
        });
        if let Some(message_id) = reply_to_message_id {
            body["msg_id"] = Value::String(message_id.to_owned());
            body["msg_seq"] = Value::from(message_sequence);
        }
        self.send_message_body(user_openid, &body).await
    }

    async fn send_approval_buttons(
        &self,
        user_openid: &str,
        buttons: &ApprovalButtons,
    ) -> Result<SendReceipt> {
        let body = approval_buttons_payload(buttons);
        self.send_message_body(user_openid, &body).await
    }

    async fn send_choice_buttons(
        &self,
        user_openid: &str,
        buttons: &ChoiceButtons,
        reply_to_message_id: Option<&str>,
        message_sequence: u32,
    ) -> Result<SendReceipt> {
        let mut body = choice_buttons_payload(buttons)?;
        if let Some(message_id) = reply_to_message_id {
            body["msg_id"] = Value::String(message_id.to_owned());
            body["msg_seq"] = Value::from(message_sequence);
        }
        self.send_message_body(user_openid, &body).await
    }

    async fn send_typing(&self, user_openid: &str, duration_seconds: u8) -> Result<()> {
        if duration_seconds == 0 || duration_seconds > 60 {
            bail!("QQ typing duration must be between 1 and 60 seconds");
        }
        let body = json!({
            "msg_type": 6,
            "input_notify": {
                "input_type": 1,
                "input_second": duration_seconds
            }
        });
        for attempt in 0..2 {
            let token = self.access_token().await?;
            let response = self.send_once(&token, user_openid, &body).await?;
            if response.status() == StatusCode::UNAUTHORIZED && attempt == 0 {
                self.invalidate_token().await;
                continue;
            }
            if !response.status().is_success() {
                bail!("QQ typing indicator failed with HTTP {}", response.status());
            }
            return Ok(());
        }
        bail!("QQ typing indicator remained unauthorized after token refresh")
    }

    async fn acknowledge_interaction(&self, interaction_id: &str, code: u8) -> Result<()> {
        for attempt in 0..2 {
            let token = self.access_token().await?;
            let response = self
                .acknowledge_interaction_once(&token, interaction_id, code)
                .await?;
            if response.status() == StatusCode::UNAUTHORIZED && attempt == 0 {
                self.invalidate_token().await;
                continue;
            }
            if !response.status().is_success() {
                bail!(
                    "QQ interaction acknowledgement failed with HTTP {}",
                    response.status()
                );
            }
            return Ok(());
        }
        bail!("QQ interaction acknowledgement remained unauthorized after token refresh")
    }
}

fn approval_buttons_payload(buttons: &ApprovalButtons) -> Value {
    json!({
        "msg_type": 2,
        "markdown": {
            "content": buttons.markdown,
        },
        "keyboard": {
            "content": {
                "rows": [{
                    "buttons": [
                        {
                            "id": "approve",
                            "render_data": {
                                "label": "批准一次",
                                "visited_label": "已批准",
                                "style": 1
                            },
                            "action": {
                                "type": 1,
                                "permission": {
                                    "type": 2
                                },
                                "data": buttons.allow_button_data,
                                "unsupport_tips": "请使用 /allow 审批码"
                            }
                        },
                        {
                            "id": "deny",
                            "render_data": {
                                "label": "拒绝",
                                "visited_label": "已拒绝",
                                "style": 0
                            },
                            "action": {
                                "type": 1,
                                "permission": {
                                    "type": 2
                                },
                                "data": buttons.deny_button_data,
                                "unsupport_tips": "请使用 /deny 审批码"
                            }
                        }
                    ]
                }]
            }
        }
    })
}

fn choice_buttons_payload(buttons: &ChoiceButtons) -> Result<Value> {
    if buttons.choices.is_empty() || buttons.choices.len() > 25 {
        bail!("QQ choice keyboard must contain 1 to 25 buttons");
    }
    if !matches!(buttons.button_id_prefix.as_str(), "choice" | "session") {
        bail!("QQ choice keyboard button prefix is invalid");
    }
    let rows: Vec<Value> = buttons
        .choices
        .chunks(5)
        .enumerate()
        .map(|(row_index, choices)| {
            let buttons: Vec<Value> = choices
                .iter()
                .enumerate()
                .map(|(column_index, choice)| {
                    let position = row_index * 5 + column_index;
                    json!({
                        "id": format!("{}_{position}", buttons.button_id_prefix),
                        "render_data": {
                            "label": truncate_chars(&choice.label, 10),
                            "visited_label": "已选择",
                            "style": 1
                        },
                        "action": {
                            "type": 1,
                            "permission": {
                                "type": 2
                            },
                            "data": choice.button_data,
                            "unsupport_tips": "请直接回复选项文本"
                        }
                    })
                })
                .collect();
            json!({"buttons": buttons})
        })
        .collect();
    Ok(json!({
        "msg_type": 2,
        "markdown": {
            "content": buttons.markdown,
        },
        "keyboard": {
            "content": {
                "rows": rows
            }
        }
    }))
}

fn truncate_chars(value: &str, maximum: usize) -> String {
    value.chars().take(maximum).collect()
}

#[derive(Debug, Deserialize)]
struct QqApiError {
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    code: Option<String>,
}

pub async fn run_gateway(
    qq: Arc<QqClient>,
    database: Database,
    service: Arc<BridgeService>,
) -> Result<()> {
    database.set_gateway_runtime("starting", false)?;
    let mut retry_seconds = 1_u64;
    loop {
        database.set_gateway_runtime("connecting", false)?;
        let resume = database.gateway_resume()?;
        let gateway_url = match resume
            .as_ref()
            .and_then(|resume| resume.resume_gateway_url.clone())
        {
            Some(url) => url,
            None => match qq.gateway_url().await {
                Ok(url) => url,
                Err(error) => {
                    tracing::error!(error = %error, "failed to obtain QQ Gateway URL");
                    sleep(Duration::from_secs(retry_seconds)).await;
                    retry_seconds = (retry_seconds * 2).min(60);
                    continue;
                }
            },
        };

        match run_gateway_connection(
            &gateway_url,
            qq.as_ref(),
            &database,
            service.as_ref(),
            resume,
        )
        .await
        {
            Ok(()) => retry_seconds = 1,
            Err(error) => {
                database.set_gateway_runtime("reconnecting", false)?;
                tracing::warn!(error = %error, "QQ Gateway disconnected");
                sleep(Duration::from_secs(retry_seconds)).await;
                retry_seconds = (retry_seconds * 2).min(60);
            }
        }
    }
}

async fn run_gateway_connection(
    gateway_url: &str,
    qq: &QqClient,
    database: &Database,
    service: &BridgeService,
    resume: Option<GatewayResume>,
) -> Result<()> {
    let (mut socket, _) = connect_async(gateway_url)
        .await
        .context("failed to connect to QQ Gateway")?;
    let hello_message = timeout(Duration::from_secs(15), socket.next())
        .await
        .context("QQ Gateway did not send Hello in time")?
        .context("QQ Gateway closed before Hello")?
        .context("failed to read QQ Gateway Hello")?;
    let hello = parse_gateway_message(hello_message)?;
    if hello.op != OP_HELLO {
        bail!("QQ Gateway first payload was not Hello");
    }
    let heartbeat_interval_ms = hello
        .d
        .as_ref()
        .and_then(|value| value.get("heartbeat_interval"))
        .and_then(Value::as_u64)
        .context("QQ Gateway Hello omitted heartbeat_interval")?;
    if heartbeat_interval_ms < 1000 {
        bail!("QQ Gateway heartbeat interval is unexpectedly short");
    }

    let token = qq.access_token().await?;
    let mut active_session_id = resume.as_ref().map(|state| state.session_id.clone());
    let mut sequence = resume.as_ref().map(|state| state.sequence);
    let auth = if let Some(resume) = &resume {
        json!({
            "op": OP_RESUME,
            "d": {
                "token": format!("QQBot {token}"),
                "session_id": resume.session_id,
                "seq": resume.sequence,
            }
        })
    } else {
        json!({
            "op": OP_IDENTIFY,
            "d": {
                "token": format!("QQBot {token}"),
                "intents": qq.config.intents,
                "shard": [0, 1],
                "properties": {
                    "$os": "windows",
                    "$browser": "qq-copilot-remote",
                    "$device": "qq-copilot-remote"
                }
            }
        })
    };
    socket
        .send(Message::Text(auth.to_string().into()))
        .await
        .context("failed to authenticate QQ Gateway")?;

    let start = tokio::time::Instant::now() + Duration::from_millis(heartbeat_interval_ms);
    let mut heartbeat = interval_at(start, Duration::from_millis(heartbeat_interval_ms));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut heartbeat_acknowledged = true;

    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                if !heartbeat_acknowledged {
                    bail!("QQ Gateway did not acknowledge the previous heartbeat");
                }
                let payload = json!({"op": OP_HEARTBEAT, "d": sequence});
                socket
                    .send(Message::Text(payload.to_string().into()))
                    .await
                    .context("failed to send QQ Gateway heartbeat")?;
                heartbeat_acknowledged = false;
            }
            message = socket.next() => {
                let message = message
                    .context("QQ Gateway closed the connection")?
                    .context("failed to read QQ Gateway payload")?;
                if matches!(message, Message::Ping(_)) {
                    continue;
                }
                if matches!(message, Message::Close(_)) {
                    bail!("QQ Gateway sent Close");
                }
                let payload = parse_gateway_message(message)?;
                if let Some(received_sequence) = payload.s {
                    sequence = Some(received_sequence);
                    if let Some(session_id) = active_session_id.as_deref() {
                        database.save_gateway_resume(session_id, received_sequence, None)?;
                    }
                }
                match payload.op {
                    OP_DISPATCH => {
                        match payload.t.as_deref() {
                            Some("READY") => {
                                let data = payload.d.context("READY omitted data")?;
                                let session_id = data
                                    .get("session_id")
                                    .and_then(Value::as_str)
                                    .context("READY omitted session_id")?
                                    .to_owned();
                                let resume_url = data
                                    .get("resume_gateway_url")
                                    .and_then(Value::as_str);
                                let current_sequence = sequence.unwrap_or(0);
                                database.save_gateway_resume(
                                    &session_id,
                                    current_sequence,
                                    resume_url,
                                )?;
                                active_session_id = Some(session_id);
                                database.set_gateway_runtime("connected", true)?;
                                tracing::info!("QQ Gateway session is ready");
                            }
                            Some("RESUMED") => {
                                database.set_gateway_runtime("connected", true)?;
                                tracing::info!("QQ Gateway session resumed");
                            }
                            Some("C2C_MESSAGE_CREATE") => {
                                let event: C2cMessage = serde_json::from_value(
                                    payload.d.context("C2C message omitted data")?
                                )
                                .context("invalid C2C message payload")?;
                                if let Err(error) = service
                                    .handle_inbound_message(
                                        &event.id,
                                        &event.author.user_openid,
                                        &event.content,
                                    )
                                    .await
                                {
                                    tracing::error!(
                                        message_id = %event.id,
                                        error = %error,
                                        "failed to process C2C message"
                                    );
                                }
                            }
                            Some("INTERACTION_CREATE") => {
                                let outer_event_id = payload
                                    .id
                                    .as_deref()
                                    .context("interaction event omitted outer event ID")?;
                                let event: C2cInteraction = serde_json::from_value(
                                    payload.d.context("interaction event omitted data")?
                                )
                                .context("invalid C2C interaction payload")?;
                                if let Err(error) = service
                                    .handle_interaction(
                                        outer_event_id,
                                        &event.id,
                                        &event.application_id,
                                        &event.user_openid,
                                        &event.scene,
                                        event.chat_type,
                                        event.interaction_type,
                                        event.data.interaction_type,
                                        &event.data.resolved.button_id,
                                        &event.data.resolved.button_data,
                                    )
                                    .await
                                {
                                    tracing::error!(
                                        interaction_id = %event.id,
                                        error = %error,
                                        "failed to process C2C interaction"
                                    );
                                }
                            }
                            _ => {}
                        }
                    }
                    OP_HEARTBEAT_ACK => {
                        heartbeat_acknowledged = true;
                        database.set_gateway_runtime("connected", true)?;
                    }
                    OP_RECONNECT => bail!("QQ Gateway requested reconnect"),
                    OP_INVALID_SESSION => {
                        database.clear_gateway_resume()?;
                        bail!("QQ Gateway rejected the resumable session");
                    }
                    OP_HELLO => {}
                    other => {
                        tracing::debug!(op = other, "ignored QQ Gateway opcode");
                    }
                }
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct GatewayPayload {
    #[serde(default)]
    id: Option<String>,
    op: u64,
    #[serde(default)]
    d: Option<Value>,
    #[serde(default)]
    s: Option<u64>,
    #[serde(default)]
    t: Option<String>,
}

#[derive(Debug, Deserialize)]
struct C2cMessage {
    id: String,
    content: String,
    author: C2cAuthor,
}

#[derive(Debug, Deserialize)]
struct C2cAuthor {
    user_openid: String,
}

#[derive(Debug, Deserialize)]
struct C2cInteraction {
    application_id: String,
    chat_type: u8,
    data: C2cInteractionData,
    id: String,
    scene: String,
    #[serde(rename = "type")]
    interaction_type: u8,
    user_openid: String,
}

#[derive(Debug, Deserialize)]
struct C2cInteractionData {
    resolved: C2cInteractionResolved,
    #[serde(rename = "type")]
    interaction_type: u8,
}

#[derive(Debug, Deserialize)]
struct C2cInteractionResolved {
    button_data: String,
    button_id: String,
}

fn parse_gateway_message(message: Message) -> Result<GatewayPayload> {
    match message {
        Message::Text(text) => {
            serde_json::from_str(text.as_ref()).context("invalid QQ Gateway JSON payload")
        }
        Message::Binary(bytes) => {
            serde_json::from_slice(&bytes).context("invalid binary QQ Gateway JSON payload")
        }
        other => Err(anyhow!("unexpected QQ Gateway frame type: {other:?}")),
    }
}

fn parse_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

fn deserialize_optional_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;
    Ok(value.map(|value| match value {
        Value::String(value) => value,
        other => other.to_string(),
    }))
}

#[derive(Debug, Clone, Serialize)]
pub struct MockSentMessage {
    pub kind: String,
    pub user_openid: String,
    pub content: String,
    pub reply_to_message_id: Option<String>,
    pub message_sequence: u32,
}

#[cfg(test)]
pub struct MockQqMessenger {
    messages: Mutex<Vec<MockSentMessage>>,
    acknowledgements: Mutex<Vec<(String, u8)>>,
    fail_sends: AtomicBool,
}

#[cfg(test)]
impl MockQqMessenger {
    pub fn new() -> Self {
        Self {
            messages: Mutex::new(Vec::new()),
            acknowledgements: Mutex::new(Vec::new()),
            fail_sends: AtomicBool::new(false),
        }
    }

    pub async fn messages(&self) -> Vec<MockSentMessage> {
        self.messages.lock().await.clone()
    }

    pub async fn acknowledgements(&self) -> Vec<(String, u8)> {
        self.acknowledgements.lock().await.clone()
    }

    pub fn set_send_failure(&self, fail: bool) {
        self.fail_sends.store(fail, Ordering::SeqCst);
    }
}

#[cfg(test)]
impl Default for MockQqMessenger {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[async_trait]
impl QqMessenger for MockQqMessenger {
    async fn send_text(
        &self,
        user_openid: &str,
        content: &str,
        reply_to_message_id: Option<&str>,
        message_sequence: u32,
    ) -> Result<SendReceipt> {
        if self.fail_sends.load(Ordering::SeqCst) {
            bail!("mock QQ delivery failure");
        }
        let mut messages = self.messages.lock().await;
        messages.push(MockSentMessage {
            kind: "text".to_owned(),
            user_openid: user_openid.to_owned(),
            content: content.to_owned(),
            reply_to_message_id: reply_to_message_id.map(str::to_owned),
            message_sequence,
        });
        Ok(SendReceipt {
            message_id: format!("message-{}", messages.len()),
        })
    }

    async fn send_approval_buttons(
        &self,
        user_openid: &str,
        buttons: &ApprovalButtons,
    ) -> Result<SendReceipt> {
        if self.fail_sends.load(Ordering::SeqCst) {
            bail!("mock QQ button delivery failure");
        }
        let mut messages = self.messages.lock().await;
        messages.push(MockSentMessage {
            kind: "approval_buttons".to_owned(),
            user_openid: user_openid.to_owned(),
            content: buttons.markdown.clone(),
            reply_to_message_id: None,
            message_sequence: 0,
        });
        Ok(SendReceipt {
            message_id: format!("message-{}", messages.len()),
        })
    }

    async fn send_choice_buttons(
        &self,
        user_openid: &str,
        buttons: &ChoiceButtons,
        reply_to_message_id: Option<&str>,
        message_sequence: u32,
    ) -> Result<SendReceipt> {
        if self.fail_sends.load(Ordering::SeqCst) {
            bail!("mock QQ choice delivery failure");
        }
        let mut messages = self.messages.lock().await;
        messages.push(MockSentMessage {
            kind: "choice_buttons".to_owned(),
            user_openid: user_openid.to_owned(),
            content: buttons.markdown.clone(),
            reply_to_message_id: reply_to_message_id.map(str::to_owned),
            message_sequence,
        });
        Ok(SendReceipt {
            message_id: format!("message-{}", messages.len()),
        })
    }

    async fn send_typing(&self, user_openid: &str, duration_seconds: u8) -> Result<()> {
        if self.fail_sends.load(Ordering::SeqCst) {
            bail!("mock QQ typing failure");
        }
        self.messages.lock().await.push(MockSentMessage {
            kind: "typing".to_owned(),
            user_openid: user_openid.to_owned(),
            content: duration_seconds.to_string(),
            reply_to_message_id: None,
            message_sequence: 0,
        });
        Ok(())
    }

    async fn acknowledge_interaction(&self, interaction_id: &str, code: u8) -> Result<()> {
        self.acknowledgements
            .lock()
            .await
            .push((interaction_id.to_owned(), code));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn c2c_approval_buttons_defer_identity_to_server_validation() {
        let payload = approval_buttons_payload(&ApprovalButtons {
            markdown: "approval".to_owned(),
            allow_button_data: "allow-token".to_owned(),
            deny_button_data: "deny-token".to_owned(),
        });
        let buttons = payload["keyboard"]["content"]["rows"][0]["buttons"]
            .as_array()
            .expect("buttons");
        assert_eq!(buttons.len(), 2);
        assert!(
            buttons
                .iter()
                .all(|button| button["action"]["permission"]["type"] == 2)
        );
        assert!(
            buttons
                .iter()
                .all(|button| button["action"]["permission"]["specify_user_ids"].is_null())
        );
    }

    #[test]
    fn choice_keyboard_limits_labels_and_uses_server_validated_callbacks() {
        let payload = choice_buttons_payload(&ChoiceButtons {
            markdown: "choose".to_owned(),
            button_id_prefix: "choice".to_owned(),
            choices: vec![
                ChoiceButton {
                    label: "这是一个超过十字符的选项标签".to_owned(),
                    button_data: "choice-a".to_owned(),
                },
                ChoiceButton {
                    label: "第二项".to_owned(),
                    button_data: "choice-b".to_owned(),
                },
            ],
        })
        .expect("choice payload");
        let buttons = payload["keyboard"]["content"]["rows"][0]["buttons"]
            .as_array()
            .expect("buttons");
        assert_eq!(
            buttons[0]["render_data"]["label"]
                .as_str()
                .expect("label")
                .chars()
                .count(),
            10
        );
        assert!(
            buttons
                .iter()
                .all(|button| button["action"]["permission"]["type"] == 2)
        );
    }
}
