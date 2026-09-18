use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{Arc, Mutex as StdMutex, RwLock as StdRwLock},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{
    sync::{Mutex, RwLock},
    task::JoinHandle,
    time::{Instant, sleep},
};
use uuid::Uuid;

use crate::{
    ahp_store::{
        AhpApprovalRecord, AhpInputRecord, AhpProjectionRecord, AhpSessionMenuCandidate, AhpStatus,
        MAX_TRACKED_AHP_SESSIONS, NewAhpApproval, NewAhpInput,
    },
    config::{AhpAuthorizedTarget, AhpToolNotificationMode, AppConfig},
    db::{
        ApprovalRecord, BeginDelivery, BindOutcome, Database, NewApproval, NewDelivery,
        NewQuestion, QuestionRecord,
    },
    protocol::{
        AhpAdapterRegistration, AhpAuthorizationSnapshot, AhpCommandPollResult, AhpEventKind,
        AhpHostDescriptor, AhpManagedTarget, AhpPrepareTargetCommand, AhpPublishedEvent,
        AhpSessionDescriptor, AhpTargetKind, ApprovalState, BridgeRequest, PermissionDecision,
        PermissionResult, RiskLevel, StopDecision,
    },
    qq::{ApprovalButtons, ChoiceButton, ChoiceButtons, QqMessenger, SendReceipt},
    security::{
        Classifier, PolicyAction, canonical_json, contains_secret_value, path_is_within,
        redact_json, redact_text, sha256_hex, validate_agent_content, workspace_hash,
    },
};

const POLL_INTERVAL: Duration = Duration::from_millis(200);
const MAX_WAIT_FOR_MESSAGE_SECONDS: u64 = 300;
const INBOUND_MESSAGE_TTL_SECONDS: u64 = 1800;
const AHP_MANAGED_COMMAND_TIMEOUT_SECONDS: u64 = 960;
const AHP_NEW_REMOVED_MESSAGE: &str = "QQ 已移除 /new 和 /new advanced。请在 PC 上创建 Session、登录并选择模型，再发送 /sessions 和 /switch <编号> 使用已有 Session。";

#[derive(Clone, Copy)]
enum InboundMessage<'a> {
    Text(&'a str),
    Voice(Option<&'a str>),
}

struct AhpSessionMenuEntry {
    session: AhpSessionDescriptor,
    last_used_at: i64,
    foreground: bool,
    protected: bool,
    state: String,
    workspace: String,
    host_label: String,
    host_status: String,
}

struct AhpSessionMenu {
    entries: Vec<AhpSessionMenuEntry>,
    total_count: usize,
    protected_count: usize,
    tracked_count: usize,
    limit: usize,
}

pub struct BridgeService {
    authorization: Arc<StdRwLock<RuntimeAuthorization>>,
    config_path: PathBuf,
    database: Database,
    qq: Arc<dyn QqMessenger>,
    tool_notification_mode: Arc<RwLock<AhpToolNotificationMode>>,
    final_delivery_lock: Mutex<()>,
    offline_replay_lock: Arc<Mutex<()>>,
    typing_tasks: Mutex<HashMap<String, JoinHandle<()>>>,
    foreground_switch_intent: Arc<StdMutex<u64>>,
}

struct RuntimeAuthorization {
    config: Arc<AppConfig>,
    file_version: Option<String>,
    adapter_identity: Option<(String, String)>,
    adapter_revision: Option<String>,
    last_error: Option<String>,
    restart_required: bool,
}

#[derive(Clone, Serialize)]
struct AuthorizationStatus {
    revision: String,
    adapter_revision: Option<String>,
    adapter_connected: bool,
    target_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_error: Option<String>,
    restart_required: bool,
}

impl RuntimeAuthorization {
    fn snapshot(&self) -> Result<AhpAuthorizationSnapshot> {
        Ok(AhpAuthorizationSnapshot {
            revision: self.config.authorization_revision()?,
            authorized_targets: self.config.ahp.effective_authorized_targets(),
        })
    }

    fn status(&self, adapter_connected: bool) -> Result<AuthorizationStatus> {
        Ok(AuthorizationStatus {
            revision: self.config.authorization_revision()?,
            adapter_revision: self.adapter_revision.clone(),
            adapter_connected,
            target_count: self.config.ahp.effective_authorized_targets().len(),
            last_error: self.last_error.clone(),
            restart_required: self.restart_required,
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DeliveryResult {
    pub sent: bool,
    pub delivery_id: Uuid,
    pub qq_message_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AhpTextEventData {
    message_id: String,
    content: String,
    #[serde(default)]
    complete: bool,
    #[serde(default)]
    historical: bool,
    #[serde(default)]
    final_response: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AhpToolEventData {
    tool_call_id: String,
    tool_name: String,
    status: String,
    summary: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AhpApprovalPendingData {
    approval_key: String,
    stage: String,
    tool_call_id: String,
    tool_name: String,
    summary: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AhpApprovalResolvedData {
    approval_key: String,
    approved: bool,
    #[serde(default)]
    client_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AhpInputPendingData {
    input_key: String,
    request_id: String,
    prompt: String,
    #[serde(default)]
    choices: Vec<String>,
    #[serde(default)]
    allow_freeform: bool,
    selection_mode: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AhpInputResolvedData {
    input_key: String,
    outcome: String,
    #[serde(default)]
    client_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AhpTurnEventData {
    #[serde(default)]
    summary: Option<String>,
}

struct OfflineReplayBatch {
    delivery_key: String,
    content: String,
    event_ids: Vec<String>,
}

impl BridgeService {
    pub fn new(
        config: Arc<AppConfig>,
        config_path: PathBuf,
        database: Database,
        qq: Arc<dyn QqMessenger>,
    ) -> Self {
        let tool_notification_mode = config.ahp.tool_notification_mode;
        Self {
            authorization: Arc::new(StdRwLock::new(RuntimeAuthorization {
                config,
                file_version: None,
                adapter_identity: None,
                adapter_revision: None,
                last_error: None,
                restart_required: false,
            })),
            config_path,
            database,
            qq,
            tool_notification_mode: Arc::new(RwLock::new(tool_notification_mode)),
            final_delivery_lock: Mutex::new(()),
            offline_replay_lock: Arc::new(Mutex::new(())),
            typing_tasks: Mutex::new(HashMap::new()),
            foreground_switch_intent: Arc::new(StdMutex::new(0)),
        }
    }

    pub fn database(&self) -> &Database {
        &self.database
    }

    fn task_clone(&self) -> Self {
        Self {
            authorization: self.authorization.clone(),
            config_path: self.config_path.clone(),
            database: self.database.clone(),
            qq: self.qq.clone(),
            tool_notification_mode: self.tool_notification_mode.clone(),
            final_delivery_lock: Mutex::new(()),
            offline_replay_lock: self.offline_replay_lock.clone(),
            typing_tasks: Mutex::new(HashMap::new()),
            foreground_switch_intent: self.foreground_switch_intent.clone(),
        }
    }

    fn config(&self) -> Arc<AppConfig> {
        self.authorization
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .config
            .clone()
    }

    fn authorization_status(&self) -> Result<AuthorizationStatus> {
        let authorization = self
            .authorization
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.authorization_status_for(&authorization)
    }

    fn authorization_status_for(
        &self,
        authorization: &RuntimeAuthorization,
    ) -> Result<AuthorizationStatus> {
        let adapter = self
            .database
            .ahp_status(authorization.config.ahp.adapter_stale_seconds)?
            .adapter;
        let adapter_connected = adapter.is_some_and(|adapter| {
            adapter.state == "connected"
                && authorization.adapter_identity.as_ref().is_some_and(
                    |(adapter_id, adapter_instance_id)| {
                        adapter_id == &adapter.adapter_id
                            && adapter_instance_id == &adapter.adapter_instance_id
                    },
                )
        });
        authorization.status(adapter_connected)
    }

    fn refresh_authorization(&self) -> Result<AuthorizationStatus> {
        let mut authorization = self
            .authorization
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let file = std::fs::read(&self.config_path);
        let version = match &file {
            Ok(bytes) => sha256_hex(bytes),
            Err(error) => format!("read-error:{:?}", error.kind()),
        };
        if authorization.file_version.as_deref() == Some(version.as_str()) {
            return self.authorization_status_for(&authorization);
        }
        let mut restart_required = false;
        let updated = (|| -> Result<Option<AppConfig>> {
            let bytes = file.context("failed to read authorization configuration")?;
            let content = std::str::from_utf8(&bytes)
                .context("authorization configuration is not valid UTF-8")?;
            let candidate = AppConfig::parse(content)?;
            let updated = authorization
                .config
                .with_authorization_additions(&candidate)
                .inspect_err(|_| restart_required = true)?;
            let changed = updated.authorization_revision()?
                != authorization.config.authorization_revision()?;
            Ok(changed.then_some(updated))
        })();
        authorization.file_version = Some(version);
        match updated {
            Ok(config) => {
                if let Some(config) = config {
                    authorization.config = Arc::new(config);
                }
                authorization.last_error = None;
                authorization.restart_required = false;
            }
            Err(error) => {
                let error = format!("{error:#}");
                let restart_required =
                    restart_required || error.starts_with("authorization_reload_requires_restart:");
                tracing::warn!(
                    error,
                    restart_required,
                    "authorization reload rejected; keeping the last accepted configuration"
                );
                authorization.last_error = Some(error);
                authorization.restart_required = restart_required;
            }
        }
        self.authorization_status_for(&authorization)
    }

    fn register_ahp_adapter(&self, registration: &AhpAdapterRegistration) -> Result<Value> {
        self.refresh_authorization()?;
        let mut authorization = self
            .authorization
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut result = serde_json::to_value(self.database.ahp_register_adapter(registration)?)
            .context("failed to serialize AHP adapter registration")?;
        authorization.adapter_identity = Some((
            registration.adapter_id.clone(),
            registration.adapter_instance_id.clone(),
        ));
        authorization.adapter_revision = None;
        result["authorization"] = serde_json::to_value(authorization.snapshot()?)?;
        Ok(result)
    }

    fn poll_ahp_commands(
        &self,
        adapter_id: &str,
        adapter_instance_id: &str,
        applied_revision: Option<&str>,
    ) -> Result<AhpCommandPollResult> {
        if let Some(revision) = applied_revision
            && (revision.len() != 64 || !revision.bytes().all(|byte| byte.is_ascii_hexdigit()))
        {
            bail!("invalid applied authorization revision");
        }
        let mut authorization = self
            .authorization
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let commands = self.database.ahp_poll_commands(
            adapter_id,
            adapter_instance_id,
            authorization.config.ahp.command_lease_seconds,
        )?;
        let identity = (adapter_id.to_owned(), adapter_instance_id.to_owned());
        if authorization.adapter_identity.as_ref() != Some(&identity) {
            authorization.adapter_identity = Some(identity);
            authorization.adapter_revision = None;
        }
        let snapshot = authorization.snapshot()?;
        if let Some(revision) = applied_revision
            && authorization.adapter_revision.as_deref() != Some(snapshot.revision.as_str())
        {
            authorization.adapter_revision = Some(revision.to_owned());
        }
        Ok(AhpCommandPollResult {
            commands,
            authorization: snapshot,
        })
    }

    fn ahp_status_value(&self) -> Result<Value> {
        let mut status = serde_json::to_value(
            self.database
                .ahp_status(self.config().ahp.adapter_stale_seconds)?,
        )
        .context("failed to serialize AHP status")?;
        status["authorization"] = serde_json::to_value(self.authorization_status()?)?;
        Ok(status)
    }

    fn begin_foreground_switch(&self) -> u64 {
        let mut state = self
            .foreground_switch_intent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *state = state.wrapping_add(1);
        *state
    }

    fn switch_session_by_button(
        &self,
        button_data: &str,
        allowed_session_uris: &[String],
    ) -> (
        u64,
        Result<Option<crate::ahp_store::AhpSessionSwitchSubmission>>,
    ) {
        let mut state = self
            .foreground_switch_intent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *state = state.wrapping_add(1);
        let intent = *state;
        let submission = self
            .database
            .ahp_switch_session_by_button(button_data, allowed_session_uris);
        (intent, submission)
    }

    fn unbind_foreground_session(&self) -> Result<bool> {
        let mut state = self
            .foreground_switch_intent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *state = state.wrapping_add(1);
        self.database.ahp_unbind_session()
    }

    fn detach_session(&self, session_uri: &str) -> Result<bool> {
        let mut state = self
            .foreground_switch_intent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let was_foreground = self
            .database
            .ahp_binding_for_session(session_uri)?
            .is_some_and(|binding| binding.foreground);
        if !was_foreground {
            return self.database.ahp_detach_session(session_uri);
        }
        *state = state.wrapping_add(1);
        self.database.ahp_detach_session(session_uri)
    }

    fn track_session_for_message(&self, endpoint_id: &str, session_uri: &str) -> Result<()> {
        self.database
            .ahp_track_session(endpoint_id, session_uri)
            .map(|_| ())
    }

    fn enqueue_foreground_message(&self, message_id: &str, content: &str) -> Result<i64> {
        self.database.ahp_enqueue_message(message_id, content)
    }

    fn enqueue_targeted_message(
        &self,
        message_id: &str,
        session_uri: &str,
        content: &str,
    ) -> Result<i64> {
        self.database
            .ahp_enqueue_message_to_session(message_id, session_uri, content)
    }

    fn commit_foreground_switch(
        &self,
        intent: u64,
        endpoint_id: &str,
        session_uri: &str,
    ) -> Result<crate::protocol::AhpBindingRecord> {
        let state = self
            .foreground_switch_intent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *state != intent {
            bail!("Session switch request was superseded by a newer choice");
        }
        self.database.ahp_bind_session(endpoint_id, session_uri)
    }

    pub async fn dispatch(&self, request: BridgeRequest) -> Result<Value> {
        match request {
            BridgeRequest::Health => Ok(json!({
                "status": "ok",
                "owner_bound": self.database.owner()?.is_some(),
            })),
            BridgeRequest::RegisterPrompt { session_id, cwd } => {
                self.register_prompt(&session_id, &cwd).await
            }
            BridgeRequest::PreTool {
                session_id,
                tool_use_id,
                tool_name,
                tool_input,
                cwd,
            } => serde_json::to_value(
                self.pre_tool(&session_id, &tool_use_id, &tool_name, &tool_input, &cwd)
                    .await?,
            )
            .context("failed to serialize permission result"),
            BridgeRequest::PostTool {
                session_id,
                tool_use_id,
                tool_name,
            } => self.post_tool(&session_id, &tool_use_id, &tool_name).await,
            BridgeRequest::Stop {
                session_id,
                stop_hook_active,
            } => {
                let result = self.database.stop_session(&session_id, stop_hook_active)?;
                if result.decision == StopDecision::Allow && result.reason.is_some() {
                    let message = format!(
                        "[Copilot 会话结束]\n会话未确认最终回复投递，Stop 兜底已耗尽。\n会话: {}",
                        self.session_label_or_unknown(&session_id)?
                    );
                    if let Err(error) = self
                        .send_if_owner(
                            "stop_fallback",
                            Some(&session_id),
                            &format!("stop-fallback:{session_id}"),
                            &message,
                            None,
                        )
                        .await
                    {
                        tracing::error!(error = %error, "failed to send Stop fallback notice");
                    }
                }
                serde_json::to_value(result).context("failed to serialize Stop result")
            }
            BridgeRequest::SendProgress {
                session_id,
                session_label,
                content,
            } => serde_json::to_value(
                self.send_progress(session_id.as_deref(), session_label.as_deref(), &content)
                    .await?,
            )
            .context("failed to serialize delivery"),
            BridgeRequest::SendFinal {
                session_id,
                session_label,
                content,
                idempotency_key,
            } => serde_json::to_value(
                self.send_final(
                    session_id.as_deref(),
                    session_label.as_deref(),
                    &content,
                    &idempotency_key,
                )
                .await?,
            )
            .context("failed to serialize delivery"),
            BridgeRequest::AskUser {
                session_id,
                session_label,
                question,
                choices,
                allow_freeform,
                timeout_seconds,
            } => {
                self.ask_user(
                    session_id.as_deref(),
                    session_label.as_deref(),
                    &question,
                    &choices,
                    allow_freeform,
                    timeout_seconds,
                )
                .await
            }
            BridgeRequest::WaitForMessage {
                session_id,
                session_label,
                timeout_seconds,
            } => {
                self.wait_for_message(
                    session_id.as_deref(),
                    session_label.as_deref(),
                    timeout_seconds,
                )
                .await
            }
            BridgeRequest::Status => {
                let mut status = serde_json::to_value(self.database.status_snapshot()?)
                    .context("failed to serialize status")?;
                if self.config().ahp.enabled {
                    status["ahp"] = self.ahp_status_value()?;
                }
                Ok(status)
            }
            BridgeRequest::NewBindCode => {
                let code = self
                    .database
                    .create_binding_code(self.config().bridge.bind_code_ttl_seconds)?;
                Ok(json!({
                    "binding_code": code,
                    "expires_in_seconds": self.config().bridge.bind_code_ttl_seconds,
                }))
            }
            BridgeRequest::SetEmergencyEnabled { enabled } => {
                self.database.set_owner_enabled(enabled)?;
                Ok(json!({"enabled": enabled}))
            }
            BridgeRequest::AhpAdapterRegister { registration } => {
                self.require_ahp_enabled()?;
                validate_ahp_registration(&registration)?;
                self.register_ahp_adapter(&registration)
            }
            BridgeRequest::AhpCatalogReplace {
                adapter_id,
                adapter_instance_id,
                hosts,
                sessions,
                removed_session_uris,
                full_snapshot,
            } => {
                self.require_ahp_enabled()?;
                validate_ahp_catalog(&hosts, &sessions, &removed_session_uris)?;
                self.database.ahp_replace_catalog_scoped(
                    &adapter_id,
                    &adapter_instance_id,
                    &hosts,
                    &sessions,
                    &removed_session_uris,
                    full_snapshot,
                )?;
                if full_snapshot {
                    self.reconcile_recent_ahp_sessions(&sessions)?;
                }
                Ok(json!({"accepted": true}))
            }
            BridgeRequest::AhpBindingReady {
                adapter_id,
                adapter_instance_id,
                binding_id,
                endpoint_id,
                host_instance_id,
                binding_generation,
                session_uri,
                chat_uri,
                last_server_sequence,
            } => {
                self.require_ahp_enabled()?;
                validate_identifier("binding_id", &binding_id)?;
                self.database.ahp_binding_ready(
                    &adapter_id,
                    &adapter_instance_id,
                    &binding_id,
                    &endpoint_id,
                    &host_instance_id,
                    binding_generation,
                    &session_uri,
                    &chat_uri,
                    last_server_sequence,
                )?;
                Ok(json!({"accepted": true}))
            }
            BridgeRequest::AhpBindingFailed {
                adapter_id,
                adapter_instance_id,
                binding_id,
                binding_generation,
                reason_code,
            } => {
                self.require_ahp_enabled()?;
                validate_identifier("binding_id", &binding_id)?;
                validate_short_code("reason_code", &reason_code)?;
                self.database.ahp_binding_failed(
                    &adapter_id,
                    &adapter_instance_id,
                    &binding_id,
                    binding_generation,
                    &reason_code,
                )?;
                Ok(json!({"accepted": true}))
            }
            BridgeRequest::AhpPublishEvents {
                adapter_id,
                adapter_instance_id,
                binding_id,
                binding_generation,
                events,
            } => {
                self.require_ahp_enabled()?;
                validate_identifier("binding_id", &binding_id)?;
                validate_ahp_events(&events)?;
                let events: Vec<_> = events
                    .into_iter()
                    .map(|mut event| {
                        event.data = redact_json(&event.data);
                        event
                    })
                    .collect();
                let pending_event_ids = self.database.ahp_publish_events(
                    &adapter_id,
                    &adapter_instance_id,
                    &binding_id,
                    binding_generation,
                    &events,
                )?;
                let pending_event_ids: HashSet<_> = pending_event_ids.into_iter().collect();
                let mut processed = 0_u32;
                for event in events
                    .iter()
                    .filter(|event| pending_event_ids.contains(&event.event_id))
                {
                    if let Err(error) = self.handle_ahp_event(&adapter_id, event).await {
                        tracing::error!(
                            event_id = %event.event_id,
                            kind = event.kind.as_str(),
                            error = %error,
                            "failed to project AHP event"
                        );
                        continue;
                    }
                    self.database.ahp_mark_event_processed(&event.event_id)?;
                    processed = processed.saturating_add(1);
                }
                Ok(json!({"accepted": pending_event_ids.len(), "processed": processed}))
            }
            BridgeRequest::AhpPollCommands {
                adapter_id,
                adapter_instance_id,
                timeout_seconds,
                applied_authorization_revision,
            } => {
                self.require_ahp_enabled()?;
                if timeout_seconds == 0 || timeout_seconds > self.config().ahp.poll_seconds {
                    bail!(
                        "AHP poll timeout must be between 1 and {} seconds",
                        self.config().ahp.poll_seconds
                    );
                }
                let deadline = Instant::now() + Duration::from_secs(timeout_seconds);
                let initial_revision = self.authorization_status()?.revision;
                let mut applied_revision = applied_authorization_revision.as_deref();
                loop {
                    self.refresh_authorization()?;
                    let result = self.poll_ahp_commands(
                        &adapter_id,
                        &adapter_instance_id,
                        applied_revision.take(),
                    )?;
                    if !result.commands.is_empty()
                        || result.authorization.revision != initial_revision
                        || applied_authorization_revision
                            .as_deref()
                            .is_some_and(|revision| revision != result.authorization.revision)
                        || Instant::now() >= deadline
                    {
                        break serde_json::to_value(result)
                            .context("failed to serialize AHP commands");
                    }
                    sleep(POLL_INTERVAL).await;
                }
            }
            BridgeRequest::AhpAckCommand {
                adapter_id,
                adapter_instance_id,
                command_id,
                outcome,
                error_code,
                result,
            } => {
                self.require_ahp_enabled()?;
                if let Some(error_code) = error_code.as_deref() {
                    validate_short_code("error_code", error_code)?;
                }
                self.database.ahp_ack_command(
                    &adapter_id,
                    &adapter_instance_id,
                    command_id,
                    outcome,
                    error_code.as_deref(),
                    result.as_ref(),
                )?;
                Ok(json!({"accepted": true}))
            }
            BridgeRequest::AhpSessions => {
                self.require_ahp_enabled()?;
                self.ahp_status_value()
            }
            BridgeRequest::AhpReloadTargets => {
                let status = self.refresh_authorization()?;
                if let Some(error) = status.last_error.as_deref() {
                    bail!("{error}");
                }
                serde_json::to_value(status).context("failed to serialize authorization status")
            }
            BridgeRequest::AhpBindSession {
                endpoint_id,
                session_uri,
            } => {
                self.require_ahp_enabled()?;
                self.validate_ahp_session_workspace(&endpoint_id, &session_uri)?;
                let switch_intent = self.begin_foreground_switch();
                serde_json::to_value(self.commit_foreground_switch(
                    switch_intent,
                    &endpoint_id,
                    &session_uri,
                )?)
                .context("failed to serialize AHP binding")
            }
            BridgeRequest::AhpUnbindSession => {
                self.require_ahp_enabled()?;
                Ok(json!({"unbound": self.unbind_foreground_session()?}))
            }
        }
    }

    pub async fn handle_inbound_message(
        &self,
        message_id: &str,
        user_openid: &str,
        content: &str,
    ) -> Result<()> {
        self.handle_inbound(message_id, user_openid, InboundMessage::Text(content))
            .await
    }

    pub async fn handle_inbound_voice_message(
        &self,
        message_id: &str,
        user_openid: &str,
        transcript: Option<&str>,
    ) -> Result<()> {
        self.handle_inbound(message_id, user_openid, InboundMessage::Voice(transcript))
            .await
    }

    async fn handle_inbound(
        &self,
        message_id: &str,
        user_openid: &str,
        message: InboundMessage<'_>,
    ) -> Result<()> {
        if message_id.trim().is_empty() || user_openid.trim().is_empty() {
            bail!("QQ message omitted a stable identity");
        }
        if !self.database.record_inbound(message_id, user_openid)? {
            return Ok(());
        }

        let command = match message {
            InboundMessage::Text(content) => content.trim(),
            InboundMessage::Voice(transcript) => transcript.unwrap_or_default().trim(),
        };
        let owner = self.database.owner()?;
        if owner.is_none() {
            if matches!(message, InboundMessage::Voice(_)) {
                self.database
                    .mark_inbound_kind(message_id, "voice_ignored")?;
                return Ok(());
            }
            let mut fields = command.split_whitespace();
            let verb = fields.next().unwrap_or_default();
            let code = fields.next().unwrap_or_default();
            if verb.eq_ignore_ascii_case("/bind") && !code.is_empty() && fields.next().is_none() {
                match self
                    .database
                    .try_bind_owner(user_openid, code, message_id)?
                {
                    BindOutcome::Bound => {
                        self.database.mark_inbound_kind(message_id, "bind")?;
                        let _ = self
                            .send_if_owner(
                                "command_reply",
                                None,
                                &format!("inbound:{message_id}:bind"),
                                "绑定成功。该 QQ 用户现为唯一 Owner。",
                                Some(message_id),
                            )
                            .await;
                    }
                    BindOutcome::AlreadyBound | BindOutcome::InvalidOrExpired => {
                        self.database.mark_inbound_kind(message_id, "ignored")?;
                    }
                }
            } else {
                self.database.mark_inbound_kind(message_id, "ignored")?;
            }
            return Ok(());
        }

        let owner = owner.expect("checked owner");
        if !owner.enabled {
            self.database.mark_inbound_kind(message_id, "disabled")?;
            return Ok(());
        }
        if owner.user_openid != user_openid {
            self.database.mark_inbound_kind(message_id, "non_owner")?;
            self.database
                .audit_security_event("non_owner_message", "ignored", Some(message_id))?;
            return Ok(());
        }

        let response = if is_removed_session_creation_request(command) {
            self.database
                .mark_inbound_kind(message_id, "ahp_new_removed")?;
            Some(AHP_NEW_REMOVED_MESSAGE.to_owned())
        } else {
            match message {
                InboundMessage::Text(_) if self.config().ahp.enabled => {
                    self.process_ahp_owner_message(message_id, command).await?
                }
                InboundMessage::Text(_) => self.process_owner_command(message_id, command).await?,
                InboundMessage::Voice(transcript) => {
                    self.process_owner_voice_message(message_id, transcript)
                        .await?
                }
            }
        };
        if let Some(response) = response {
            self.send_if_owner(
                "command_reply",
                None,
                &format!("inbound:{message_id}:reply"),
                &response,
                Some(message_id),
            )
            .await?;
        }
        if self.config().ahp.enabled {
            self.replay_pending_ahp_projections().await?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn handle_interaction(
        &self,
        outer_event_id: &str,
        interaction_id: &str,
        application_id: &str,
        user_openid: &str,
        scene: &str,
        chat_type: u8,
        interaction_type: u8,
        data_type: u8,
        button_id: &str,
        button_data: &str,
    ) -> Result<()> {
        validate_identifier("interaction_id", interaction_id)?;
        validate_identifier("outer_event_id", outer_event_id)?;
        let expected_approved = match button_id {
            "approve" => Some(true),
            "deny" => Some(false),
            _ => None,
        };
        let is_choice = button_id.starts_with("choice_");
        let is_session_switch = button_id.starts_with("session_");
        let is_removed_creation = ["target_", "model_", "approval_", "nw_"]
            .iter()
            .any(|prefix| button_id.starts_with(prefix))
            || is_removed_session_creation_request(button_data);
        let structurally_valid = self.config().qq.approval_buttons_enabled
            && application_id == self.config().qq.app_id
            && scene == "c2c"
            && chat_type == 2
            && interaction_type == 11
            && data_type == 11
            && (expected_approved.is_some()
                || is_choice
                || is_session_switch
                || is_removed_creation)
            && !button_data.is_empty()
            && button_data.len() <= 200;
        if !structurally_valid {
            self.acknowledge_interaction(interaction_id, 4).await?;
            self.database.audit_security_event(
                "invalid_qq_interaction",
                "rejected",
                Some(outer_event_id),
            )?;
            return Ok(());
        }

        if !self.database.record_qq_interaction(
            interaction_id,
            outer_event_id,
            user_openid,
            button_data,
        )? {
            self.acknowledge_interaction(interaction_id, 3).await?;
            return Ok(());
        }
        let owner = self.database.owner()?;
        if owner
            .as_ref()
            .is_none_or(|owner| !owner.enabled || owner.user_openid != user_openid)
        {
            self.acknowledge_interaction(interaction_id, 4).await?;
            self.database.audit_security_event(
                "non_owner_interaction",
                "rejected",
                Some(outer_event_id),
            )?;
            return Ok(());
        }

        let (ack_code, confirmation) = if is_removed_creation {
            (4, Some(AHP_NEW_REMOVED_MESSAGE.to_owned()))
        } else if let Some(expected_approved) = expected_approved {
            match self.database.ahp_submit_approval_by_button(
                button_data,
                expected_approved,
                interaction_id,
            ) {
                Ok(Some(submission)) if submission.accepted => (
                    0,
                    Some(format!(
                        "审批 {} 已通过按钮提交给 Agent Host。",
                        submission.record.short_code
                    )),
                ),
                Ok(Some(_)) | Ok(None) => (3, None),
                Err(error) => {
                    tracing::warn!(
                        interaction_id,
                        error = %error,
                        "QQ approval button was rejected"
                    );
                    (1, None)
                }
            }
        } else if is_choice {
            match self
                .database
                .ahp_submit_input_by_button(button_data, interaction_id)
            {
                Ok(Some(submission)) if submission.accepted => (
                    0,
                    Some(format!(
                        "问题 {} 已选择“{}”。",
                        submission.record.short_code, submission.answer
                    )),
                ),
                Ok(Some(_)) | Ok(None) => (3, None),
                Err(error) => {
                    tracing::warn!(
                        interaction_id,
                        error = %error,
                        "QQ input button was rejected"
                    );
                    (1, None)
                }
            }
        } else {
            let allowed_session_uris = self
                .database
                .ahp_list_sessions()?
                .into_iter()
                .filter(|session| ahp_session_matches_workspace(&self.config(), session))
                .map(|session| session.session_uri)
                .collect::<Vec<_>>();
            let (switch_intent, submission) =
                self.switch_session_by_button(button_data, &allowed_session_uris);
            match submission {
                Ok(Some(submission)) if submission.accepted => {
                    if let Some(workspace) =
                        ahp_session_target_display(&self.config(), &submission.session)
                    {
                        (
                            0,
                            Some(format!(
                                "前台已切换到 {}：{}\n目录: {}\nBinding: {}",
                                submission
                                    .session
                                    .short_code
                                    .as_deref()
                                    .unwrap_or("[unknown]"),
                                submission.session.title,
                                workspace,
                                submission.binding.state
                            )),
                        )
                    } else {
                        (3, None)
                    }
                }
                Ok(Some(_)) | Ok(None) => (3, None),
                Err(error) => {
                    match self
                        .database
                        .ahp_consume_session_switch_button(button_data, &allowed_session_uris)
                    {
                        Ok(Some(session)) => {
                            let service = self.task_clone();
                            let interaction_id = interaction_id.to_owned();
                            tokio::spawn(async move {
                                let result =
                                    service.switch_ahp_session(session, switch_intent).await;
                                let (key, message) = match result {
                                    Ok(message) => {
                                        (format!("switch-button:{interaction_id}"), message)
                                    }
                                    Err(error) => (
                                        format!("switch-button:{interaction_id}:error"),
                                        format!("Session 切换失败：{error}"),
                                    ),
                                };
                                let _ = service
                                    .send_if_owner(
                                        "ahp_button_confirmation",
                                        None,
                                        &key,
                                        &message,
                                        None,
                                    )
                                    .await;
                            });
                            (0, Some("离线目标正在连接，切换请求已提交。".to_owned()))
                        }
                        Ok(None) | Err(_) => {
                            tracing::warn!(
                                interaction_id,
                                error = %error,
                                "QQ Session switch button was rejected"
                            );
                            (1, None)
                        }
                    }
                }
            }
        };
        self.acknowledge_interaction(interaction_id, ack_code)
            .await?;
        if let Some(confirmation) = confirmation {
            self.try_send_ahp_projection(
                "ahp_button_confirmation",
                &format!("interaction:{interaction_id}:submitted"),
                &confirmation,
            )
            .await;
        }
        Ok(())
    }

    async fn acknowledge_interaction(&self, interaction_id: &str, code: u8) -> Result<()> {
        self.qq
            .acknowledge_interaction(interaction_id, code)
            .await?;
        self.database.set_qq_interaction_ack(interaction_id, code)
    }

    async fn register_prompt(&self, session_id: &str, cwd: &Path) -> Result<Value> {
        validate_identifier("session_id", session_id)?;
        self.ensure_workspace(cwd)?;
        let session = self
            .database
            .register_session(session_id, &workspace_hash(cwd))?;
        let workspace = cwd
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("[workspace]");
        let notification = format!(
            "[Copilot 会话开始]\n工作区: {workspace}\n会话: {}",
            session.short_code
        );
        if let Err(error) = self
            .send_if_owner(
                "session_start",
                Some(session_id),
                &format!("session:{session_id}:started"),
                &notification,
                None,
            )
            .await
        {
            tracing::warn!(error = %error, "session notification was not delivered");
        }
        Ok(json!({
            "registered": true,
            "session_label": session.short_code,
        }))
    }

    async fn pre_tool(
        &self,
        session_id: &str,
        tool_use_id: &str,
        tool_name: &str,
        tool_input: &Value,
        cwd: &Path,
    ) -> Result<PermissionResult> {
        validate_identifier("session_id", session_id)?;
        validate_identifier("tool_use_id", tool_use_id)?;
        validate_identifier("tool_name", tool_name)?;
        self.ensure_workspace(cwd)?;
        let Some(session) = self.database.session_by_id(session_id)? else {
            return Ok(deny("Unknown Agent session"));
        };
        if !matches!(session.state.as_str(), "active" | "waiting") {
            return Ok(deny("Agent session is not active"));
        }
        let config = self.config();
        let protected_paths = [
            self.config_path.clone(),
            config.bridge.database_path.clone(),
        ];
        let classifier = Classifier::new(
            &config.policy,
            &config.bridge.workspace_roots,
            &protected_paths,
        );
        let classification = classifier.classify(tool_name, tool_input, cwd);
        match classification.action {
            PolicyAction::AutoAllow => {
                return Ok(PermissionResult {
                    decision: PermissionDecision::Allow,
                    reason: classification.reason,
                });
            }
            PolicyAction::Deny => {
                self.database.audit_security_event(
                    "tool_denied_by_policy",
                    &classification.reason,
                    None,
                )?;
                return Ok(deny(&classification.reason));
            }
            PolicyAction::RequireApproval => {}
        }

        let idempotency_key = sha256_hex(
            format!(
                "{session_id}\0{tool_use_id}\0{}",
                canonical_json(&classification.redacted_input)
            )
            .as_bytes(),
        );
        let expires_at = Utc::now().timestamp()
            + i64::try_from(self.config().bridge.approval_ttl_seconds)
                .context("approval TTL exceeds supported range")?;
        let approval = self.database.begin_approval(NewApproval {
            approval_id: Uuid::new_v4(),
            idempotency_key,
            session_id: session_id.to_owned(),
            tool_use_id: tool_use_id.to_owned(),
            tool_name: tool_name.to_owned(),
            input_hash: classification.input_hash,
            redacted_summary: classification.summary,
            risk: classification.risk,
            expires_at,
        })?;

        if approval.record.state == ApprovalState::Pending {
            let message = approval_message(&approval.record, &session.short_code);
            if let Err(error) = self
                .send_owner(
                    "approval",
                    Some(session_id),
                    &format!("approval:{}", approval.record.approval_id),
                    &message,
                    None,
                )
                .await
            {
                self.database.deny_approval_delivery_failure(
                    approval.record.approval_id,
                    "qq_delivery_failed",
                )?;
                tracing::error!(
                    approval_id = %approval.record.approval_id,
                    error = %error,
                    "approval notification failed; denying tool"
                );
                return Ok(deny("QQ approval notification failed; the tool was denied"));
            }
        }

        self.wait_for_approval(approval.record).await
    }

    async fn wait_for_approval(&self, approval: ApprovalRecord) -> Result<PermissionResult> {
        let maximum_wait = Duration::from_secs(self.config().bridge.bridge_wait_seconds);
        let deadline = Instant::now() + maximum_wait;
        loop {
            let current = self
                .database
                .approval_by_id(approval.approval_id)?
                .context("approval disappeared while waiting")?;
            match current.state {
                ApprovalState::Allowed => {
                    return Ok(PermissionResult {
                        decision: PermissionDecision::Allow,
                        reason: "Approved once by the bound QQ owner".to_owned(),
                    });
                }
                ApprovalState::Pending if Utc::now().timestamp() < current.expires_at => {}
                ApprovalState::Pending | ApprovalState::Expired => {
                    self.database.expire_approval(current.approval_id)?;
                    return Ok(deny("QQ approval expired"));
                }
                ApprovalState::Denied => return Ok(deny("Denied by the bound QQ owner")),
                ApprovalState::Completed | ApprovalState::UnknownFailure => {
                    return Ok(deny("This approval has already reached a terminal state"));
                }
            }
            if Instant::now() >= deadline {
                self.database.expire_approval(current.approval_id)?;
                return Ok(deny("QQ approval timed out"));
            }
            sleep(POLL_INTERVAL).await;
        }
    }

    async fn post_tool(
        &self,
        session_id: &str,
        tool_use_id: &str,
        tool_name: &str,
    ) -> Result<Value> {
        validate_identifier("session_id", session_id)?;
        validate_identifier("tool_use_id", tool_use_id)?;
        let approval = self.database.approval_for_tool(session_id, tool_use_id)?;
        let completed = self.database.complete_approval(session_id, tool_use_id)?;
        if completed
            && approval
                .as_ref()
                .is_some_and(|approval| approval.risk == RiskLevel::High)
        {
            let approval = approval.expect("checked approval");
            let message = format!(
                "[Copilot 工具完成]\n会话: {}\n工具: {}\n审批码: {}",
                self.session_label_or_unknown(session_id)?,
                tool_name,
                approval.short_code
            );
            if let Err(error) = self
                .send_if_owner(
                    "tool_completed",
                    Some(session_id),
                    &format!("approval:{}:completed", approval.approval_id),
                    &message,
                    None,
                )
                .await
            {
                tracing::warn!(error = %error, "tool completion notice was not delivered");
            }
        }
        Ok(json!({"recorded": true, "approved_tool_completed": completed}))
    }

    async fn send_progress(
        &self,
        session_id: Option<&str>,
        session_label: Option<&str>,
        content: &str,
    ) -> Result<DeliveryResult> {
        let session = self.database.resolve_session(session_id, session_label)?;
        let content = validate_agent_content(content, self.config().bridge.message_total_chars)
            .map_err(anyhow::Error::msg)?;
        let content_hash = sha256_hex(content.as_bytes());
        self.send_owner(
            "progress",
            Some(&session.session_id),
            &format!("progress:{}:{content_hash}", session.session_id),
            &format!("[Copilot 进度 {}]\n{content}", session.short_code),
            None,
        )
        .await
    }

    async fn send_final(
        &self,
        session_id: Option<&str>,
        session_label: Option<&str>,
        content: &str,
        idempotency_key: &str,
    ) -> Result<DeliveryResult> {
        if idempotency_key.trim().is_empty() || idempotency_key.len() > 256 {
            bail!("idempotency_key must contain 1 to 256 bytes");
        }
        let _guard = self.final_delivery_lock.lock().await;
        let session = self.database.resolve_session(session_id, session_label)?;
        let delivery_key = format!("final:{idempotency_key}");
        if session.final_sent {
            let existing = self.database.delivery_by_idempotency_key(&delivery_key)?;
            if let Some(record) = existing {
                return self
                    .wait_for_existing_delivery(BeginDelivery {
                        record,
                        created: false,
                    })
                    .await;
            }
            bail!("a final response was already sent for the current turn");
        }
        let content = validate_agent_content(content, self.config().bridge.message_total_chars)
            .map_err(anyhow::Error::msg)?;
        let delivery = self
            .send_owner(
                "final",
                Some(&session.session_id),
                &delivery_key,
                &format!("[Copilot 最终回复 {}]\n{content}", session.short_code),
                None,
            )
            .await?;
        if delivery.sent {
            self.database.set_final_sent(&session.session_id)?;
        }
        Ok(delivery)
    }

    async fn ask_user(
        &self,
        session_id: Option<&str>,
        session_label: Option<&str>,
        question: &str,
        choices: &[String],
        allow_freeform: bool,
        timeout_seconds: u64,
    ) -> Result<Value> {
        if timeout_seconds == 0 || timeout_seconds > self.config().bridge.question_ttl_seconds {
            bail!(
                "timeout_seconds must be between 1 and {}",
                self.config().bridge.question_ttl_seconds
            );
        }
        if choices.len() > 10 {
            bail!("at most 10 choices are accepted");
        }
        if choices.is_empty() && !allow_freeform {
            bail!("a question without choices must allow free-form answers");
        }
        let question = validate_agent_content(question, 1000).map_err(anyhow::Error::msg)?;
        let mut safe_choices = Vec::with_capacity(choices.len());
        for choice in choices {
            safe_choices.push(validate_agent_content(choice, 200).map_err(anyhow::Error::msg)?);
        }
        let session = self.database.resolve_session(session_id, session_label)?;
        let question_hash = sha256_hex(
            canonical_json(&json!({
                "question": question,
                "choices": safe_choices,
                "allow_freeform": allow_freeform,
            }))
            .as_bytes(),
        );
        let record = self.database.create_question(NewQuestion {
            question_id: Uuid::new_v4(),
            session_id: session.session_id.clone(),
            question_hash,
            choices_json: serde_json::to_string(&safe_choices)?,
            allow_freeform,
            expires_at: Utc::now().timestamp()
                + i64::try_from(timeout_seconds).context("question timeout is too large")?,
        })?;
        let message = question_message(&record, &session.short_code, &question, &safe_choices);
        if let Err(error) = self
            .send_owner(
                "question",
                Some(&session.session_id),
                &format!("question:{}", record.question_id),
                &message,
                None,
            )
            .await
        {
            self.database.expire_question(record.question_id)?;
            return Err(error).context("question delivery failed");
        }

        let deadline = Instant::now() + Duration::from_secs(timeout_seconds);
        loop {
            let current = self
                .database
                .question_by_id(record.question_id)?
                .context("question disappeared while waiting")?;
            match current.state.as_str() {
                "answered" => {
                    let answer = current.answer.context("answered question omitted answer")?;
                    self.database.consume_question_answer(record.question_id)?;
                    return Ok(json!({
                        "status": "answered",
                        "answer": answer,
                        "question_code": current.short_code,
                    }));
                }
                "cancelled" => return Ok(json!({"status": "cancelled"})),
                "expired" => return Ok(json!({"status": "timeout"})),
                "pending" => {}
                state => bail!("question entered unknown state {state}"),
            }
            if Instant::now() >= deadline || Utc::now().timestamp() >= current.expires_at {
                self.database.expire_question(record.question_id)?;
                return Ok(json!({"status": "timeout"}));
            }
            sleep(POLL_INTERVAL).await;
        }
    }

    async fn wait_for_message(
        &self,
        session_id: Option<&str>,
        session_label: Option<&str>,
        timeout_seconds: u64,
    ) -> Result<Value> {
        if timeout_seconds == 0 || timeout_seconds > MAX_WAIT_FOR_MESSAGE_SECONDS {
            bail!("timeout_seconds must be between 1 and {MAX_WAIT_FOR_MESSAGE_SECONDS}");
        }
        let session = self.database.resolve_session(session_id, session_label)?;
        self.database
            .set_session_waiting(&session.session_id, true)?;
        let deadline = Instant::now() + Duration::from_secs(timeout_seconds);
        loop {
            if self.database.take_cancel_requested(&session.session_id)? {
                return Ok(json!({"status": "cancelled"}));
            }
            if let Some((message_id, content)) =
                self.database.take_next_message(&session.session_id)?
            {
                self.database
                    .set_session_waiting(&session.session_id, false)?;
                return Ok(json!({
                    "status": "message",
                    "message_id": message_id,
                    "content": content,
                }));
            }
            if Instant::now() >= deadline {
                self.database
                    .set_session_waiting(&session.session_id, false)?;
                return Ok(json!({"status": "timeout"}));
            }
            sleep(POLL_INTERVAL).await;
        }
    }

    async fn process_ahp_owner_message(
        &self,
        message_id: &str,
        command: &str,
    ) -> Result<Option<String>> {
        if command.chars().count() > 4_000 || contains_secret_value(command) {
            self.database
                .mark_inbound_kind(message_id, "ahp_message_rejected")?;
            return Ok(Some("消息过长或包含疑似 Secret，已拒绝。".to_owned()));
        }
        let mut parts = command.splitn(3, char::is_whitespace);
        let verb = parts.next().unwrap_or_default().to_ascii_lowercase();
        let argument = parts.next().unwrap_or_default().trim();
        let remainder = parts.next().unwrap_or_default().trim();
        match verb.as_str() {
            "/allow" | "/deny" if !argument.is_empty() && remainder.is_empty() => {
                let approved = verb == "/allow";
                let decision = self
                    .database
                    .ahp_submit_approval(argument, approved, message_id)?;
                self.database
                    .mark_inbound_kind(message_id, "ahp_approval")?;
                Ok(Some(match decision {
                    None => format!("审批码 {} 不存在。", argument.to_ascii_uppercase()),
                    Some(submission) if submission.accepted => format!(
                        "审批 {} 已提交给 Agent Host，等待权威状态确认。",
                        submission.record.short_code
                    ),
                    Some(submission) if submission.record.state == "allowed" => {
                        format!("审批 {} 已批准。", submission.record.short_code)
                    }
                    Some(submission) if submission.record.state == "denied" => {
                        format!("审批 {} 已拒绝。", submission.record.short_code)
                    }
                    Some(submission) => {
                        format!(
                            "审批 {} 已处于 {} 状态。",
                            submission.record.short_code, submission.record.state
                        )
                    }
                }))
            }
            "/cancel" if argument.is_empty() && remainder.is_empty() => {
                match self.database.ahp_enqueue_cancel(message_id) {
                    Ok(_) => {
                        self.database.mark_inbound_kind(message_id, "ahp_cancel")?;
                        Ok(Some("取消请求已排队。".to_owned()))
                    }
                    Err(error) => {
                        tracing::warn!(error = %error, "AHP cancel request rejected");
                        self.database
                            .mark_inbound_kind(message_id, "ahp_cancel_rejected")?;
                        Ok(Some("当前没有可取消的已绑定 AHP 会话。".to_owned()))
                    }
                }
            }
            "/cancel" if !argument.is_empty() && remainder.is_empty() => {
                let Some(session) = self.database.ahp_session_by_code(argument)? else {
                    self.database
                        .mark_inbound_kind(message_id, "ahp_cancel_invalid")?;
                    return Ok(Some(format!(
                        "Session 编号 {} 不存在，发送 /sessions 查看。",
                        argument.to_ascii_uppercase()
                    )));
                };
                if !ahp_session_matches_workspace(&self.config(), &session) {
                    self.database
                        .mark_inbound_kind(message_id, "ahp_cancel_forbidden")?;
                    return Ok(Some("该 Session 不属于配置的目标目录。".to_owned()));
                }
                match self
                    .database
                    .ahp_enqueue_cancel_for_session(message_id, &session.session_uri)
                {
                    Ok(_) => {
                        self.database
                            .mark_inbound_kind(message_id, "ahp_cancel_target")?;
                        Ok(Some(format!(
                            "Session {} 的取消请求已排队。",
                            session.short_code.as_deref().unwrap_or("[unknown]")
                        )))
                    }
                    Err(error) => {
                        tracing::warn!(error = %error, "targeted AHP cancel request rejected");
                        self.database
                            .mark_inbound_kind(message_id, "ahp_cancel_target_rejected")?;
                        Ok(Some("指定 Session 当前没有可取消的活动 Turn。".to_owned()))
                    }
                }
            }
            "/ask" if !argument.is_empty() => {
                let content = if remainder.is_empty() {
                    argument.to_owned()
                } else {
                    format!("{argument} {remainder}")
                };
                self.queue_ahp_owner_message(message_id, &content)
            }
            "/send" if !argument.is_empty() && !remainder.is_empty() => {
                let Some(session) = self.database.ahp_session_by_code(argument)? else {
                    self.database
                        .mark_inbound_kind(message_id, "ahp_send_invalid")?;
                    return Ok(Some(format!(
                        "Session 编号 {} 不存在，发送 /sessions 查看。",
                        argument.to_ascii_uppercase()
                    )));
                };
                if !ahp_session_matches_workspace(&self.config(), &session) {
                    self.database
                        .mark_inbound_kind(message_id, "ahp_send_forbidden")?;
                    return Ok(Some("该 Session 不属于配置的目标目录。".to_owned()));
                }
                if let Err(error) =
                    self.track_session_for_message(&session.endpoint_id, &session.session_uri)
                {
                    tracing::warn!(error = %error, "target AHP Session could not be tracked");
                    self.database
                        .mark_inbound_kind(message_id, "ahp_send_capacity_rejected")?;
                    return Ok(Some(
                        "无法加入该 Session：5 个后台槽位均正在运行、排队或等待交互。".to_owned(),
                    ));
                }
                match self.enqueue_targeted_message(message_id, &session.session_uri, remainder) {
                    Ok(_) => {
                        self.database
                            .mark_inbound_kind(message_id, "ahp_send_target")?;
                        Ok(Some(format!(
                            "消息已发送到 Session {}，前台 Session 未改变。",
                            session.short_code.as_deref().unwrap_or("[unknown]")
                        )))
                    }
                    Err(error) => {
                        tracing::warn!(error = %error, "targeted AHP message rejected");
                        self.database
                            .mark_inbound_kind(message_id, "ahp_send_target_rejected")?;
                        Ok(Some(
                            "目标 Session 的 Adapter Binding 尚未就绪。".to_owned(),
                        ))
                    }
                }
            }
            "/answer" if !argument.is_empty() && !remainder.is_empty() => {
                let input = self.database.ahp_input_by_code(argument)?;
                let Some(input) = input else {
                    self.database
                        .mark_inbound_kind(message_id, "ahp_answer_invalid")?;
                    return Ok(Some(format!(
                        "问题码 {} 不存在。",
                        argument.to_ascii_uppercase()
                    )));
                };
                let accepted =
                    self.database
                        .ahp_submit_input(&input.input_key, remainder, message_id)?;
                self.database.mark_inbound_kind(message_id, "ahp_answer")?;
                Ok(Some(if accepted {
                    format!("问题 {} 的回答已提交。", input.short_code)
                } else {
                    format!("问题 {} 已由另一端处理。", input.short_code)
                }))
            }
            "/sessions" if argument.is_empty() && remainder.is_empty() => {
                self.database
                    .mark_inbound_kind(message_id, "ahp_sessions")?;
                Ok(Some(format_ahp_sessions(&self.ahp_session_menu()?)))
            }
            "/new" => {
                self.database
                    .mark_inbound_kind(message_id, "ahp_new_removed")?;
                Ok(Some(AHP_NEW_REMOVED_MESSAGE.to_owned()))
            }
            "/switch" if argument.is_empty() && remainder.is_empty() => {
                self.database
                    .mark_inbound_kind(message_id, "ahp_switch_menu")?;
                self.send_ahp_session_switch_menu(message_id).await
            }
            "/switch" if !argument.is_empty() && remainder.is_empty() => {
                let session = self.database.ahp_session_by_code(argument)?;
                let Some(session) = session else {
                    self.database
                        .mark_inbound_kind(message_id, "ahp_switch_invalid")?;
                    return Ok(Some(format!(
                        "Session 编号 {} 不存在，发送 /sessions 查看。",
                        argument.to_ascii_uppercase()
                    )));
                };
                if !ahp_session_matches_workspace(&self.config(), &session) {
                    self.database
                        .mark_inbound_kind(message_id, "ahp_switch_forbidden")?;
                    return Ok(Some("该 Session 不属于配置的目标目录。".to_owned()));
                }
                let switch_intent = self.begin_foreground_switch();
                match self.switch_ahp_session(session, switch_intent).await {
                    Ok(binding) => {
                        self.database.mark_inbound_kind(message_id, "ahp_switch")?;
                        Ok(Some(binding))
                    }
                    Err(error) => {
                        tracing::warn!(error = %error, "AHP Session switch rejected");
                        self.database
                            .mark_inbound_kind(message_id, "ahp_switch_rejected")?;
                        Ok(Some(
                            "无法切换：5 个后台槽位均正在运行、排队或等待交互。".to_owned(),
                        ))
                    }
                }
            }
            "/detach" if !argument.is_empty() && remainder.is_empty() => {
                let Some(session) = self.database.ahp_session_by_code(argument)? else {
                    self.database
                        .mark_inbound_kind(message_id, "ahp_detach_invalid")?;
                    return Ok(Some(format!(
                        "Session 编号 {} 不存在，发送 /sessions 查看。",
                        argument.to_ascii_uppercase()
                    )));
                };
                if !ahp_session_matches_workspace(&self.config(), &session) {
                    self.database
                        .mark_inbound_kind(message_id, "ahp_detach_forbidden")?;
                    return Ok(Some("该 Session 不属于配置的目标目录。".to_owned()));
                }
                match self.detach_session(&session.session_uri) {
                    Ok(true) => {
                        self.database.mark_inbound_kind(message_id, "ahp_detach")?;
                        Ok(Some(format!(
                            "Session {} 已进入安全解绑队列。",
                            session.short_code.as_deref().unwrap_or("[unknown]")
                        )))
                    }
                    Ok(false) => {
                        self.database
                            .mark_inbound_kind(message_id, "ahp_detach_not_tracked")?;
                        Ok(Some("该 Session 当前未被后台监控。".to_owned()))
                    }
                    Err(error) => {
                        tracing::warn!(error = %error, "AHP Session detach rejected");
                        self.database
                            .mark_inbound_kind(message_id, "ahp_detach_rejected")?;
                        Ok(Some(
                            "该 Session 正在运行、排队或等待交互，不能安全解绑。".to_owned(),
                        ))
                    }
                }
            }
            "/status" if argument.is_empty() && remainder.is_empty() => {
                self.database.mark_inbound_kind(message_id, "ahp_status")?;
                let mode = self.current_tool_notification_mode().await;
                let status = format_ahp_status(
                    &self
                        .database
                        .ahp_status(self.config().ahp.adapter_stale_seconds)?,
                    mode,
                );
                let authorization = self.authorization_status()?;
                let reload_error = authorization.last_error.map_or_else(String::new, |error| {
                    format!(
                        "\n授权热更新失败: {error}{}",
                        if authorization.restart_required {
                            "；请等待空闲后重启"
                        } else {
                            ""
                        }
                    )
                });
                Ok(Some(format!(
                    "{status}\n授权版本: {}\nAdapter 已应用: {}{reload_error}",
                    authorization.revision,
                    authorization
                        .adapter_revision
                        .as_deref()
                        .unwrap_or("尚未确认")
                )))
            }
            "/notify" if remainder.is_empty() => {
                if argument.is_empty() {
                    self.database
                        .mark_inbound_kind(message_id, "ahp_notify_status")?;
                    return Ok(Some(format_notification_mode(
                        self.current_tool_notification_mode().await,
                    )));
                }
                let Ok(mode) = argument.parse::<AhpToolNotificationMode>() else {
                    self.database
                        .mark_inbound_kind(message_id, "ahp_notify_invalid")?;
                    return Ok(Some(
                        "未知通知模式。使用 /notify approval_only、/notify compact 或 /notify full。"
                            .to_owned(),
                    ));
                };
                match self.set_tool_notification_mode(mode).await {
                    Ok(changed) => {
                        self.database.mark_inbound_kind(
                            message_id,
                            if changed {
                                "ahp_notify_changed"
                            } else {
                                "ahp_notify_unchanged"
                            },
                        )?;
                        Ok(Some(format!(
                            "通知模式{} {}，已写入配置；后续事件立即生效，无需重启。\n{}",
                            if changed { "已切换为" } else { "已经是" },
                            mode.as_str(),
                            notification_mode_description(mode)
                        )))
                    }
                    Err(error) => {
                        let current = self.current_tool_notification_mode().await;
                        tracing::warn!(
                            error = %error,
                            requested_mode = mode.as_str(),
                            "notification mode update failed"
                        );
                        self.database
                            .mark_inbound_kind(message_id, "ahp_notify_failed")?;
                        Ok(Some(format!(
                            "通知模式切换失败，仍保持 {}。请检查本机 Bridge 日志和配置文件权限。",
                            current.as_str()
                        )))
                    }
                }
            }
            "/help" if argument.is_empty() && remainder.is_empty() => {
                self.database.mark_inbound_kind(message_id, "ahp_help")?;
                Ok(Some(ahp_help_text(
                    self.current_tool_notification_mode().await,
                )))
            }
            _ if verb.starts_with('/') => {
                self.database
                    .mark_inbound_kind(message_id, "ahp_unknown_command")?;
                Ok(Some(
                    "未识别命令。普通文本会进入共享对话；发送 /help 查看控制命令。".to_owned(),
                ))
            }
            _ => {
                if let Some(input) = self.database.ahp_pending_input()? {
                    let accepted =
                        self.database
                            .ahp_submit_input(&input.input_key, command, message_id)?;
                    self.database.mark_inbound_kind(message_id, "ahp_answer")?;
                    return Ok(Some(if accepted {
                        format!(
                            "问题 {} 的回答已提交给 Agent Host，等待确认。",
                            input.short_code
                        )
                    } else {
                        format!("问题 {} 已由另一端处理。", input.short_code)
                    }));
                }
                self.queue_ahp_owner_message(message_id, command)
            }
        }
    }

    async fn process_owner_voice_message(
        &self,
        message_id: &str,
        transcript: Option<&str>,
    ) -> Result<Option<String>> {
        if !self.config().qq.voice_input_enabled {
            self.database
                .mark_inbound_kind(message_id, "voice_disabled")?;
            return Ok(Some(
                "语音输入尚未启用，请在配置中设置 qq.voice_input_enabled = true。".to_owned(),
            ));
        }
        let Some(transcript) = transcript.map(str::trim).filter(|value| !value.is_empty()) else {
            self.database
                .mark_inbound_kind(message_id, "voice_asr_missing")?;
            return Ok(Some(
                "未获取到 QQ 内置语音识别结果，请重新录制较短、清晰的语音，或改发文字。".to_owned(),
            ));
        };
        if !self.config().ahp.enabled {
            self.database
                .mark_inbound_kind(message_id, "voice_unsupported_mode")?;
            return Ok(Some(
                "语音输入仅支持 AHP 共享会话；Legacy 控制命令请使用文字发送。".to_owned(),
            ));
        }
        if transcript.chars().count() > 4_000 || contains_secret_value(transcript) {
            self.database
                .mark_inbound_kind(message_id, "ahp_voice_rejected")?;
            return Ok(Some(
                "语音识别结果过长或包含疑似 Secret，已拒绝。".to_owned(),
            ));
        }
        if let Some(input) = self.database.ahp_pending_input()? {
            let accepted =
                self.database
                    .ahp_submit_input(&input.input_key, transcript, message_id)?;
            self.database
                .mark_inbound_kind(message_id, "ahp_voice_answer")?;
            return Ok(Some(if accepted {
                format!(
                    "语音回答已提交给 Agent Host，等待确认（问题 {}）：\n{transcript}",
                    input.short_code
                )
            } else {
                format!("问题 {} 已由另一端处理。", input.short_code)
            }));
        }
        match self.enqueue_foreground_message(message_id, transcript) {
            Ok(_) => {
                self.database
                    .mark_inbound_kind(message_id, "ahp_voice_message")?;
                Ok(Some(format!("已识别并发送到共享会话：\n{transcript}")))
            }
            Err(error) => {
                tracing::warn!(error = %error, "AHP voice message rejected");
                self.database
                    .mark_inbound_kind(message_id, "ahp_voice_rejected")?;
                Ok(Some(
                    "共享 AHP Session 尚未绑定或 Adapter 未就绪，请先在本机完成绑定。".to_owned(),
                ))
            }
        }
    }

    fn queue_ahp_owner_message(&self, message_id: &str, content: &str) -> Result<Option<String>> {
        match self.enqueue_foreground_message(message_id, content) {
            Ok(_) => {
                self.database.mark_inbound_kind(message_id, "ahp_message")?;
                let session_code = if let Some(binding) = self.database.ahp_binding()? {
                    self.database
                        .ahp_session_by_uri(&binding.session_uri)?
                        .and_then(|session| session.short_code)
                        .unwrap_or_else(|| "[unknown]".to_owned())
                } else {
                    "[unknown]".to_owned()
                };
                Ok(Some(format!(
                    "消息已进入前台 Session {session_code} 的队列。"
                )))
            }
            Err(error) => {
                tracing::warn!(error = %error, "AHP shared message rejected");
                self.database
                    .mark_inbound_kind(message_id, "ahp_message_rejected")?;
                Ok(Some(
                    "共享 AHP Session 尚未绑定或 Adapter 未就绪，请先在本机完成绑定。".to_owned(),
                ))
            }
        }
    }

    async fn current_tool_notification_mode(&self) -> AhpToolNotificationMode {
        *self.tool_notification_mode.read().await
    }

    async fn set_tool_notification_mode(&self, mode: AhpToolNotificationMode) -> Result<bool> {
        let mut current = self.tool_notification_mode.write().await;
        let mut config = AppConfig::load(&self.config_path)?;
        let changed = *current != mode || config.ahp.tool_notification_mode != mode;
        if config.ahp.tool_notification_mode != mode {
            config.ahp.tool_notification_mode = mode;
            config.save(&self.config_path)?;
        }
        *current = mode;
        Ok(changed)
    }

    fn managed_target_for_session(
        &self,
        session: &AhpSessionDescriptor,
    ) -> Option<AhpManagedTarget> {
        self.config()
            .ahp
            .effective_authorized_targets()
            .iter()
            .find(|target| session_matches_authorized_target(target, session))
            .map(authorized_target_to_managed_target)
    }

    async fn prepare_ahp_session_target(
        &self,
        session: &AhpSessionDescriptor,
    ) -> Result<AhpSessionDescriptor> {
        let target = self
            .managed_target_for_session(session)
            .context("AHP Session is no longer associated with a registered target")?;
        let command = AhpPrepareTargetCommand {
            target,
            retain_connection: true,
        };
        let command_id = self.database.ahp_enqueue_prepare_target(
            &format!("prepare-switch:{}:{}", session.session_uri, Uuid::new_v4()),
            &serde_json::to_value(&command)?,
        )?;
        let status = self
            .wait_for_ahp_command_terminal(
                command_id,
                Duration::from_secs(AHP_MANAGED_COMMAND_TIMEOUT_SECONDS),
            )
            .await?;
        if status.state != "acked" {
            bail!(
                "目标准备失败：{}",
                status.error_code.as_deref().unwrap_or("unknown")
            );
        }
        self.wait_for_ahp_session_catalog_entry(&session.session_uri, Duration::from_secs(15))
            .await?
            .or_else(|| {
                self.database
                    .ahp_session_by_uri(&session.session_uri)
                    .ok()
                    .flatten()
            })
            .context("AHP Session disappeared after target refresh")
    }

    async fn switch_ahp_session(
        &self,
        session: AhpSessionDescriptor,
        switch_intent: u64,
    ) -> Result<String> {
        let status = self
            .database
            .ahp_status(self.config().ahp.adapter_stale_seconds)?;
        let mut session = if session_host_is_online(&session, &status.hosts) {
            session
        } else {
            self.prepare_ahp_session_target(&session).await?
        };
        if !ahp_session_matches_workspace(&self.config(), &session) {
            bail!("该 Session 不属于配置的目标目录");
        }
        if !session_host_is_online(
            &session,
            &self
                .database
                .ahp_status(self.config().ahp.adapter_stale_seconds)?
                .hosts,
        ) {
            session = self.prepare_ahp_session_target(&session).await?;
        }
        let binding = self.commit_foreground_switch(
            switch_intent,
            &session.endpoint_id,
            &session.session_uri,
        )?;
        Ok(format!(
            "正在切换到 {}：{}\n目录: {}\nGeneration: {}",
            session.short_code.as_deref().unwrap_or("[unknown]"),
            session.title,
            ahp_session_target_display(&self.config(), &session)
                .unwrap_or_else(|| session.session_uri.clone()),
            binding.generation
        ))
    }

    async fn wait_for_ahp_session_catalog_entry(
        &self,
        session_uri: &str,
        timeout: Duration,
    ) -> Result<Option<AhpSessionDescriptor>> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(session) = self.database.ahp_session_by_uri(session_uri)? {
                return Ok(Some(session));
            }
            if Instant::now() >= deadline {
                return Ok(None);
            }
            sleep(Duration::from_millis(200)).await;
        }
    }

    async fn wait_for_ahp_command_terminal(
        &self,
        command_id: i64,
        timeout: Duration,
    ) -> Result<crate::ahp_store::AhpCommandStatusRecord> {
        let deadline = Instant::now() + timeout;
        loop {
            let status = self
                .database
                .ahp_command_status(command_id)?
                .context("AHP command disappeared")?;
            if matches!(status.state.as_str(), "acked" | "rejected" | "failed") {
                return Ok(status);
            }
            if Instant::now() >= deadline {
                bail!("AHP target preparation timed out");
            }
            sleep(Duration::from_millis(200)).await;
        }
    }

    async fn handle_ahp_event(&self, adapter_id: &str, event: &AhpPublishedEvent) -> Result<()> {
        let session_label = self.ahp_session_label(&event.session_uri)?;
        match event.kind {
            AhpEventKind::UserMessage => {
                let data: AhpTextEventData = serde_json::from_value(event.data.clone())?;
                if data.historical {
                    return Ok(());
                }
                if event.origin_client_id.as_deref() == Some(adapter_id) {
                    return Ok(());
                }
                self.try_deliver_ahp_event_projection(
                    &event.event_id,
                    "ahp_user_message",
                    &format!("{session_label}\n[PC]\n{}", data.content),
                )
                .await;
                if let Some(turn_id) = event.turn_id.as_deref() {
                    self.start_ahp_typing(&event.session_uri, turn_id).await;
                }
            }
            AhpEventKind::AssistantMessage => {
                let data: AhpTextEventData = serde_json::from_value(event.data.clone())?;
                if !data.complete || data.historical {
                    return Ok(());
                }
                let final_response = data.final_response.unwrap_or(data.complete);
                if final_response {
                    self.stop_ahp_typing(&event.session_uri, event.turn_id.as_deref())
                        .await;
                }
                self.try_deliver_ahp_event_projection(
                    &event.event_id,
                    "ahp_assistant_message",
                    &format!("{session_label}\n{}", data.content),
                )
                .await;
                if final_response {
                    self.try_send_ahp_focus_button(
                        &event.session_uri,
                        &format!("assistant:{}", event.event_id),
                        &session_label,
                    )
                    .await;
                }
            }
            AhpEventKind::ToolStatus => {
                let data: AhpToolEventData = serde_json::from_value(event.data.clone())?;
                if !should_notify_tool(self.current_tool_notification_mode().await, &data.status) {
                    return Ok(());
                }
                self.try_deliver_ahp_event_projection(
                    &event.event_id,
                    "ahp_tool_status",
                    &format!(
                        "{session_label}\n[工具 {}]\n状态: {}\n{}",
                        data.tool_name, data.status, data.summary
                    ),
                )
                .await;
                if let Some(turn_id) = event.turn_id.as_deref() {
                    self.start_ahp_typing(&event.session_uri, turn_id).await;
                }
            }
            AhpEventKind::ApprovalPending => {
                self.stop_ahp_typing(&event.session_uri, event.turn_id.as_deref())
                    .await;
                let data: AhpApprovalPendingData = serde_json::from_value(event.data.clone())?;
                let chat_uri = event
                    .chat_uri
                    .clone()
                    .context("AHP approval event omitted chat URI")?;
                let turn_id = event
                    .turn_id
                    .clone()
                    .context("AHP approval event omitted turn ID")?;
                let approval = self.database.ahp_begin_approval(&NewAhpApproval {
                    approval_key: data.approval_key,
                    stage: data.stage,
                    session_uri: event.session_uri.clone(),
                    chat_uri,
                    turn_id,
                    tool_call_id: data.tool_call_id,
                    tool_name: data.tool_name,
                    summary: data.summary,
                    expires_at: Utc::now().timestamp()
                        + i64::try_from(self.config().bridge.approval_ttl_seconds)?,
                })?;
                if approval.created {
                    self.send_ahp_approval_notification(&approval.record, &session_label)
                        .await;
                }
            }
            AhpEventKind::ApprovalResolved => {
                let data: AhpApprovalResolvedData = serde_json::from_value(event.data.clone())?;
                if let Some(record) = self.database.ahp_resolve_approval(
                    &data.approval_key,
                    data.approved,
                    data.client_id.as_deref(),
                )? {
                    self.try_deliver_ahp_event_projection(
                        &event.event_id,
                        "ahp_approval_resolved",
                        &format!(
                            "{session_label}\n审批 {} 已由 {} {}。",
                            record.short_code,
                            data.client_id.as_deref().unwrap_or("Agent Host"),
                            if data.approved { "批准" } else { "拒绝" }
                        ),
                    )
                    .await;
                    if let Some(turn_id) = event.turn_id.as_deref() {
                        self.start_ahp_typing(&event.session_uri, turn_id).await;
                    }
                }
            }
            AhpEventKind::InputPending => {
                self.stop_ahp_typing(&event.session_uri, event.turn_id.as_deref())
                    .await;
                let data: AhpInputPendingData = serde_json::from_value(event.data.clone())?;
                let input = self.database.ahp_begin_input(&NewAhpInput {
                    input_key: data.input_key,
                    session_uri: event.session_uri.clone(),
                    chat_uri: event
                        .chat_uri
                        .clone()
                        .context("AHP input event omitted chat URI")?,
                    request_id: data.request_id,
                    prompt: data.prompt,
                    choices: data.choices,
                    allow_freeform: data.allow_freeform,
                    selection_mode: data.selection_mode,
                    expires_at: Utc::now().timestamp()
                        + i64::try_from(self.config().bridge.question_ttl_seconds)?,
                })?;
                if input.created {
                    self.send_ahp_input_notification(&input.record, &session_label)
                        .await;
                }
            }
            AhpEventKind::InputResolved => {
                let data: AhpInputResolvedData = serde_json::from_value(event.data.clone())?;
                if let Some(resolution) = self
                    .database
                    .ahp_resolve_input(&data.input_key, &data.outcome, data.client_id.as_deref())?
                    .filter(|resolution| resolution.transitioned)
                {
                    let resolved_by_qq = match data.client_id.as_deref() {
                        Some(client_id) => client_id == adapter_id,
                        None => resolution
                            .decided_by_surface
                            .as_deref()
                            .is_some_and(|surface| surface.starts_with("qq_")),
                    };
                    if !resolved_by_qq {
                        let resolution_message = if data.client_id.is_some() {
                            format!("问题 {} 已在 PC 端处理。", resolution.record.short_code)
                        } else {
                            format!(
                                "问题 {} 已由 Agent Host 处理。",
                                resolution.record.short_code
                            )
                        };
                        self.try_deliver_ahp_event_projection(
                            &event.event_id,
                            "ahp_input_resolved",
                            &format!("{session_label}\n{resolution_message}"),
                        )
                        .await;
                    }
                    if let Some(turn_id) = event.turn_id.as_deref() {
                        self.start_ahp_typing(&event.session_uri, turn_id).await;
                    }
                }
            }
            AhpEventKind::TurnCancelled
            | AhpEventKind::TurnFailed
            | AhpEventKind::HostDisconnected => {
                self.stop_ahp_typing(&event.session_uri, event.turn_id.as_deref())
                    .await;
                let data: AhpTurnEventData = serde_json::from_value(event.data.clone())?;
                let default_summary = match event.kind {
                    AhpEventKind::TurnCancelled => "当前 Turn 已取消",
                    AhpEventKind::TurnFailed => "当前 Turn 执行失败",
                    AhpEventKind::HostDisconnected => "VS Code Agent Host 已断开",
                    _ => unreachable!("matched event kind"),
                };
                self.try_deliver_ahp_event_projection(
                    &event.event_id,
                    "ahp_turn_state",
                    &format!(
                        "{session_label}\n{}",
                        data.summary.as_deref().unwrap_or(default_summary)
                    ),
                )
                .await;
            }
            AhpEventKind::TurnStarted => {
                if let Some(turn_id) = event.turn_id.as_deref() {
                    self.start_ahp_typing(&event.session_uri, turn_id).await;
                }
                self.warn_on_shared_workspace_turns(event, &session_label)
                    .await?;
            }
            AhpEventKind::TurnCompleted => {
                self.stop_ahp_typing(&event.session_uri, event.turn_id.as_deref())
                    .await;
            }
            AhpEventKind::SessionSnapshot | AhpEventKind::ChatSnapshot => {}
        }
        Ok(())
    }

    async fn warn_on_shared_workspace_turns(
        &self,
        event: &AhpPublishedEvent,
        session_label: &str,
    ) -> Result<()> {
        let Some(current_session) = self.database.ahp_session_by_uri(&event.session_uri)? else {
            return Ok(());
        };
        let config = self.config();
        let Some(current_workspace) = ahp_session_target_workspace(&config, &current_session)
        else {
            return Ok(());
        };
        for binding in self.database.ahp_bindings()? {
            if binding.session_uri == event.session_uri || binding.active_turn_id.is_none() {
                continue;
            }
            let Some(other_session) = self.database.ahp_session_by_uri(&binding.session_uri)?
            else {
                continue;
            };
            let Some(other_workspace) = ahp_session_target_workspace(&config, &other_session)
            else {
                continue;
            };
            if other_workspace != current_workspace {
                continue;
            }
            let other_label = self.ahp_session_label(&binding.session_uri)?;
            let warning_key = sha256_hex(
                format!(
                    "{}:{}:{}",
                    event.event_id,
                    binding.binding_id,
                    binding.active_turn_id.as_deref().unwrap_or_default()
                )
                .as_bytes(),
            );
            self.try_send_ahp_projection(
                "ahp_workspace_conflict",
                &format!("ahp-workspace-conflict:{warning_key}"),
                &format!(
                    "⚠ 同工作区并发警告\n{session_label} 与 {other_label} 正在同时执行 Turn。\n\
                     两个 Agent 可能互相覆盖文件、干扰测试或 Git 索引；写任务建议使用独立 Git worktree。"
                ),
            )
            .await;
        }
        Ok(())
    }

    async fn try_send_ahp_projection(&self, kind: &str, key: &str, content: &str) {
        if let Err(error) = self.send_if_owner(kind, None, key, content, None).await {
            tracing::warn!(kind, error = %error, "AHP event was stored but not delivered to QQ");
        }
    }

    async fn start_ahp_typing(&self, session_uri: &str, turn_id: &str) {
        if !self.config().ahp.typing_indicator_enabled {
            return;
        }
        let owner = match self.database.owner() {
            Ok(Some(owner)) if owner.enabled => owner,
            _ => return,
        };
        let mut tasks = self.typing_tasks.lock().await;
        let task_key = format!("{session_uri}\0{turn_id}");
        if let Some(task) = tasks.remove(&task_key) {
            task.abort();
        }
        let qq = self.qq.clone();
        let user_openid = owner.user_openid;
        let duration = self.config().ahp.typing_duration_seconds;
        let refresh = self.config().ahp.typing_refresh_seconds;
        let task = tokio::spawn(async move {
            loop {
                if let Err(error) = qq.send_typing(&user_openid, duration).await {
                    tracing::debug!(error = %error, "QQ typing indicator stopped");
                    break;
                }
                sleep(Duration::from_secs(u64::from(refresh))).await;
            }
        });
        tasks.insert(task_key, task);
    }

    async fn stop_ahp_typing(&self, session_uri: &str, turn_id: Option<&str>) {
        let mut tasks = self.typing_tasks.lock().await;
        if let Some(turn_id) = turn_id {
            if let Some(task) = tasks.remove(&format!("{session_uri}\0{turn_id}")) {
                task.abort();
            }
        } else {
            let prefix = format!("{session_uri}\0");
            let matching: Vec<_> = tasks
                .keys()
                .filter(|key| key.starts_with(&prefix))
                .cloned()
                .collect();
            for key in matching {
                if let Some(task) = tasks.remove(&key) {
                    task.abort();
                }
            }
        }
    }

    async fn try_deliver_ahp_event_projection(&self, event_id: &str, kind: &str, content: &str) {
        match self.database.ahp_queue_projection(event_id, kind, content) {
            Ok(true) => {}
            Ok(false) => return,
            Err(error) => {
                tracing::error!(event_id, error = %error, "failed to queue AHP projection");
                return;
            }
        };
        match self
            .send_if_owner(
                kind,
                None,
                &format!("ahp-event-projection:{event_id}"),
                content,
                None,
            )
            .await
        {
            Ok(_) => {
                if let Err(error) = self
                    .database
                    .ahp_mark_projections_delivered(&[event_id.to_owned()])
                {
                    tracing::error!(
                        event_id,
                        error = %error,
                        "failed to mark AHP projection delivered"
                    );
                }
            }
            Err(error) => {
                let _ = self
                    .database
                    .ahp_projection_failed(event_id, "qq_delivery_failed");
                tracing::warn!(kind, error = %error, "AHP projection queued for later delivery");
            }
        }
    }

    fn claim_pending_ahp_replay(&self) -> Result<Option<OfflineReplayBatch>> {
        let pending = self.database.ahp_pending_projections(20)?;
        if pending.is_empty() {
            return Ok(None);
        }
        if let Some(replay_key) = pending[0].replay_key.clone() {
            if pending
                .iter()
                .any(|projection| projection.replay_key.as_deref() != Some(&replay_key))
            {
                bail!("claimed AHP projection batch has inconsistent replay keys");
            }
            let mut content = format_offline_replay_content(&pending);
            if content.chars().count() > self.config().bridge.message_total_chars {
                content = truncate_for_qq(&content, self.config().bridge.message_total_chars);
            }
            return Ok(Some(OfflineReplayBatch {
                delivery_key: replay_key,
                content,
                event_ids: pending
                    .into_iter()
                    .map(|projection| projection.event_id)
                    .collect(),
            }));
        }

        let mut selected: Vec<AhpProjectionRecord> = Vec::new();
        let mut content = String::from("[QQ 离线期间未实时送达]\n");
        for projection in pending {
            let section = format!("\n[{}]\n{}", projection.kind, projection.content.trim());
            if content.chars().count() + section.chars().count()
                > self.config().bridge.message_total_chars
            {
                break;
            }
            content.push_str(&section);
            selected.push(projection);
        }
        if selected.is_empty() {
            let projection = self.database.ahp_pending_projections(1)?.pop();
            let Some(projection) = projection else {
                return Ok(None);
            };
            let available = self.config().bridge.message_total_chars.saturating_sub(80);
            if available == 0 {
                return Ok(None);
            }
            content = format!(
                "[QQ 离线期间未实时送达]\n[{}]\n{}",
                projection.kind,
                truncate_for_qq(&projection.content, available)
            );
            selected.push(projection);
        }
        let delivery_key = offline_replay_delivery_key(&selected);
        if !self
            .database
            .ahp_claim_projection_replay(&selected, &delivery_key)?
        {
            return Ok(None);
        }
        Ok(Some(OfflineReplayBatch {
            delivery_key,
            content,
            event_ids: selected
                .into_iter()
                .map(|projection| projection.event_id)
                .collect(),
        }))
    }

    async fn replay_pending_ahp_projections(&self) -> Result<()> {
        let _guard = self.offline_replay_lock.lock().await;
        let Some(batch) = self.claim_pending_ahp_replay()? else {
            return Ok(());
        };
        match self
            .send_if_owner(
                "ahp_offline_replay",
                None,
                &batch.delivery_key,
                &batch.content,
                None,
            )
            .await
        {
            Ok(_) => {
                self.database
                    .ahp_mark_projections_delivered(&batch.event_ids)?;
                Ok(())
            }
            Err(error) => {
                let terminal = self
                    .database
                    .delivery_by_idempotency_key(&batch.delivery_key)?
                    .is_some_and(|delivery| offline_replay_delivery_is_terminal(&delivery.status));
                if terminal {
                    self.database
                        .ahp_mark_projections_delivered(&batch.event_ids)?;
                    tracing::warn!(
                        replay_key = %batch.delivery_key,
                        error = %error,
                        "offline AHP replay outcome is terminal; refusing to resend"
                    );
                    return Ok(());
                }
                self.database
                    .ahp_release_projection_replay(&batch.delivery_key, "offline_replay_failed")?;
                Err(error)
            }
        }
    }

    async fn send_ahp_approval_notification(
        &self,
        approval: &AhpApprovalRecord,
        session_label: &str,
    ) {
        if self.config().qq.approval_buttons_enabled {
            match self
                .send_ahp_approval_buttons(approval, session_label)
                .await
            {
                Ok(()) => {
                    self.try_send_ahp_focus_button(
                        &approval.session_uri,
                        &format!("approval:{}", approval.approval_key),
                        session_label,
                    )
                    .await;
                    return;
                }
                Err(error) => {
                    tracing::warn!(
                        approval_key = %approval.approval_key,
                        error = %error,
                        "QQ approval buttons failed; falling back to plain text"
                    );
                }
            }
        }
        self.try_send_ahp_projection(
            "ahp_approval",
            &format!("ahp-approval:{}:text", approval.approval_key),
            &format_ahp_approval(approval, session_label),
        )
        .await;
        self.try_send_ahp_focus_button(
            &approval.session_uri,
            &format!("approval:{}", approval.approval_key),
            session_label,
        )
        .await;
    }

    async fn send_ahp_approval_buttons(
        &self,
        approval: &AhpApprovalRecord,
        session_label: &str,
    ) -> Result<()> {
        let owner = self.database.owner()?.context("no QQ owner is bound")?;
        if !owner.enabled {
            bail!("QQ remote control is disabled by the local emergency switch");
        }

        let delivery = self.database.begin_delivery(NewDelivery {
            delivery_id: Uuid::new_v4(),
            idempotency_key: format!("ahp-approval:{}:buttons", approval.approval_key),
            kind: "ahp_approval_buttons".to_owned(),
            session_id: None,
        })?;
        if !delivery.created {
            self.wait_for_existing_delivery(delivery).await?;
            return Ok(());
        }
        let markdown = format!(
            "{}\n## 工具{}审批 {}\n工具：{}\n{}\n\n按钮不可用时：`/allow {}` 或 `/deny {}`",
            escape_qq_markdown(session_label),
            if approval.stage == "result" {
                "结果复核"
            } else {
                "执行前"
            },
            escape_qq_markdown(&approval.short_code),
            escape_qq_markdown(&approval.tool_name),
            escape_qq_markdown(&approval.summary),
            escape_qq_markdown(&approval.short_code),
            escape_qq_markdown(&approval.short_code),
        );
        let result = self
            .qq
            .send_approval_buttons(
                &owner.user_openid,
                &ApprovalButtons {
                    markdown,
                    allow_button_data: approval.allow_button_data.clone(),
                    deny_button_data: approval.deny_button_data.clone(),
                },
            )
            .await;
        match result {
            Ok(receipt) => {
                self.database
                    .record_sent_message(delivery.record.delivery_id, 1)?;
                self.database.finish_delivery(
                    delivery.record.delivery_id,
                    "sent",
                    Some(&receipt.message_id),
                    None,
                )?;
                Ok(())
            }
            Err(error) => {
                self.database.finish_delivery(
                    delivery.record.delivery_id,
                    "in_doubt",
                    None,
                    Some("qq_button_delivery_error"),
                )?;
                Err(error)
            }
        }
    }

    async fn send_ahp_input_notification(&self, input: &AhpInputRecord, session_label: &str) {
        if self.config().qq.approval_buttons_enabled
            && input.selection_mode == "single"
            && !input.buttons.is_empty()
        {
            match self.send_ahp_input_buttons(input, session_label).await {
                Ok(()) => {
                    self.try_send_ahp_focus_button(
                        &input.session_uri,
                        &format!("input:{}", input.input_key),
                        session_label,
                    )
                    .await;
                    return;
                }
                Err(error) => {
                    tracing::warn!(
                        input_key = %input.input_key,
                        error = %error,
                        "QQ input buttons failed; falling back to plain text"
                    );
                }
            }
        }
        self.try_send_ahp_projection(
            "ahp_input",
            &format!("ahp-input:{}:text", input.input_key),
            &format_ahp_input(input, session_label),
        )
        .await;
        self.try_send_ahp_focus_button(
            &input.session_uri,
            &format!("input:{}", input.input_key),
            session_label,
        )
        .await;
    }

    async fn send_ahp_input_buttons(
        &self,
        input: &AhpInputRecord,
        session_label: &str,
    ) -> Result<()> {
        let owner = self.database.owner()?.context("no QQ owner is bound")?;
        if !owner.enabled {
            bail!("QQ remote control is disabled by the local emergency switch");
        }
        let delivery = self.database.begin_delivery(NewDelivery {
            delivery_id: Uuid::new_v4(),
            idempotency_key: format!("ahp-input:{}:buttons", input.input_key),
            kind: "ahp_input_buttons".to_owned(),
            session_id: None,
        })?;
        if !delivery.created {
            self.wait_for_existing_delivery(delivery).await?;
            return Ok(());
        }
        let choices = input
            .buttons
            .iter()
            .map(|button| ChoiceButton {
                label: button.label.clone(),
                button_data: button.button_data.clone(),
            })
            .collect();
        let options = input
            .choices
            .iter()
            .enumerate()
            .map(|(index, choice)| format!("{}. {}", index + 1, escape_qq_markdown(choice)))
            .collect::<Vec<_>>()
            .join("\n");
        let result = self
            .qq
            .send_choice_buttons(
                &owner.user_openid,
                &ChoiceButtons {
                    markdown: format!(
                        "{}\n## Agent 提问 {}\n{}\n\n{}\n\n按钮不可用时使用 `/answer {} <文本>`。",
                        escape_qq_markdown(session_label),
                        escape_qq_markdown(&input.short_code),
                        escape_qq_markdown(&input.prompt),
                        options,
                        escape_qq_markdown(&input.short_code)
                    ),
                    button_id_prefix: "choice".to_owned(),
                    choices,
                },
                None,
                0,
            )
            .await;
        match result {
            Ok(receipt) => {
                self.database
                    .record_sent_message(delivery.record.delivery_id, 1)?;
                self.database.finish_delivery(
                    delivery.record.delivery_id,
                    "sent",
                    Some(&receipt.message_id),
                    None,
                )?;
                Ok(())
            }
            Err(error) => {
                self.database.finish_delivery(
                    delivery.record.delivery_id,
                    "in_doubt",
                    None,
                    Some("qq_input_button_delivery_error"),
                )?;
                Err(error)
            }
        }
    }

    async fn try_send_ahp_focus_button(&self, session_uri: &str, key: &str, session_label: &str) {
        if !self.config().qq.approval_buttons_enabled {
            return;
        }
        if let Err(error) = self
            .send_ahp_focus_button(session_uri, key, session_label)
            .await
        {
            tracing::warn!(error = %error, "AHP Session focus button was not delivered");
        }
    }

    async fn send_ahp_focus_button(
        &self,
        session_uri: &str,
        key: &str,
        session_label: &str,
    ) -> Result<()> {
        if self
            .database
            .ahp_binding()?
            .is_some_and(|binding| binding.session_uri == session_uri)
        {
            return Ok(());
        }
        let owner = self.database.owner()?.context("no QQ owner is bound")?;
        if !owner.enabled {
            bail!("QQ remote control is disabled by the local emergency switch");
        }
        let buttons = self.database.ahp_create_session_switch_buttons(
            &[session_uri.to_owned()],
            self.config().bridge.question_ttl_seconds,
        )?;
        let button = buttons
            .into_iter()
            .next()
            .context("AHP Session disappeared while creating a focus button")?;
        let code = button
            .session
            .short_code
            .clone()
            .unwrap_or_else(|| "Session".to_owned());
        let delivery = self.database.begin_delivery(NewDelivery {
            delivery_id: Uuid::new_v4(),
            idempotency_key: format!("ahp-session-focus:{key}"),
            kind: "ahp_session_focus_button".to_owned(),
            session_id: None,
        })?;
        if !delivery.created {
            self.wait_for_existing_delivery(delivery).await?;
            return Ok(());
        }
        let result = self
            .qq
            .send_choice_buttons(
                &owner.user_openid,
                &ChoiceButtons {
                    markdown: format!(
                        "{}\n任务仍在其原 Session 中，可按需切换前台查看或继续对话。",
                        escape_qq_markdown(session_label)
                    ),
                    button_id_prefix: "session".to_owned(),
                    choices: vec![ChoiceButton {
                        label: format!("切换到 {code}"),
                        button_data: button.button_data,
                    }],
                },
                None,
                0,
            )
            .await;
        match result {
            Ok(receipt) => {
                self.database
                    .record_sent_message(delivery.record.delivery_id, 1)?;
                self.database.finish_delivery(
                    delivery.record.delivery_id,
                    "sent",
                    Some(&receipt.message_id),
                    None,
                )?;
                Ok(())
            }
            Err(error) => {
                self.database.finish_delivery(
                    delivery.record.delivery_id,
                    "in_doubt",
                    None,
                    Some("qq_session_focus_delivery_error"),
                )?;
                Err(error)
            }
        }
    }

    fn ahp_session_menu(&self) -> Result<AhpSessionMenu> {
        let config = self.config();
        let status = self.database.ahp_status(config.ahp.adapter_stale_seconds)?;
        Ok(select_ahp_session_menu(
            &config,
            &status.bindings,
            &status.hosts,
            self.database.ahp_session_menu_candidates()?,
        ))
    }

    async fn send_ahp_session_switch_menu(&self, message_id: &str) -> Result<Option<String>> {
        let menu = self.ahp_session_menu()?;
        if menu.entries.is_empty() {
            return Ok(Some("目标目录中没有可展示的 AHP Session。".to_owned()));
        }
        if menu.entries.len() > 100 {
            return Ok(Some(
                "受保护的 Session 超过 QQ 单条命令的 100 个按钮上限，不能隐藏活动会话。请等待部分任务结束，或使用 /switch <编号> 直接切换；不会停止任务或删除历史。"
                    .to_owned(),
            ));
        }
        let session_uris: Vec<_> = menu
            .entries
            .iter()
            .map(|entry| entry.session.session_uri.clone())
            .collect();
        let buttons = self.database.ahp_create_session_switch_buttons(
            &session_uris,
            self.config().bridge.question_ttl_seconds,
        )?;
        let owner = self.database.owner()?.context("no QQ owner is bound")?;
        if !owner.enabled {
            bail!("QQ remote control is disabled by the local emergency switch");
        }

        let page_count = buttons.len().div_ceil(25);
        for (page_index, page) in buttons.chunks(25).enumerate() {
            let delivery = self.database.begin_delivery(NewDelivery {
                delivery_id: Uuid::new_v4(),
                idempotency_key: format!("ahp-session-switch-menu:{message_id}:{}", page_index + 1),
                kind: "ahp_session_switch_buttons".to_owned(),
                session_id: None,
            })?;
            if !delivery.created {
                self.wait_for_existing_delivery(delivery).await?;
                continue;
            }
            let first_entry = page_index * 25;
            let entries = &menu.entries[first_entry..first_entry + page.len()];
            let session_lines = entries
                .iter()
                .enumerate()
                .map(|(index, entry)| format_ahp_session_entry(entry, first_entry + index, true))
                .collect::<Vec<_>>()
                .join("\n\n");
            let choices = page
                .iter()
                .map(|button| ChoiceButton {
                    label: format!(
                        "{} {}",
                        button.session.short_code.as_deref().unwrap_or("Session"),
                        session_menu_text(&button.session.title)
                            .chars()
                            .take(4)
                            .collect::<String>()
                    ),
                    button_data: button.button_data.clone(),
                })
                .collect();
            let page_label = if page_count > 1 {
                format!("（{}/{page_count}）", page_index + 1)
            } else {
                String::new()
            };
            let receipt = self
                .qq
                .send_choice_buttons(
                    &owner.user_openid,
                    &ChoiceButtons {
                        markdown: format!(
                            "## 切换会话{page_label}\n\n{}\n\n{session_lines}\n\n点击下方“编号 + 标题”按钮，或发送 `/switch <编号>`。  \n切换不会停止其他任务；按钮有效期 {} 秒。",
                            format_ahp_session_menu_summary(&menu).replace('\n', "  \n"),
                            self.config().bridge.question_ttl_seconds
                        ),
                        button_id_prefix: "session".to_owned(),
                        choices,
                    },
                    Some(message_id),
                    u32::try_from(page_index + 1)?,
                )
                .await;
            match receipt {
                Ok(receipt) => {
                    self.database
                        .record_sent_message(delivery.record.delivery_id, 1)?;
                    self.database.finish_delivery(
                        delivery.record.delivery_id,
                        "sent",
                        Some(&receipt.message_id),
                        None,
                    )?;
                }
                Err(error) => {
                    self.database.finish_delivery(
                        delivery.record.delivery_id,
                        "in_doubt",
                        None,
                        Some("qq_session_switch_delivery_error"),
                    )?;
                    return Err(error);
                }
            }
        }
        Ok(None)
    }

    async fn process_owner_command(
        &self,
        message_id: &str,
        command: &str,
    ) -> Result<Option<String>> {
        let mut parts = command.splitn(3, char::is_whitespace);
        let verb = parts.next().unwrap_or_default().to_ascii_lowercase();
        let argument = parts.next().unwrap_or_default().trim();
        let remainder = parts.next().unwrap_or_default().trim();
        match verb.as_str() {
            "/allow" | "/deny" if !argument.is_empty() && remainder.is_empty() => {
                let allow = verb == "/allow";
                let decision = self.database.decide_approval(argument, allow, message_id)?;
                self.database.mark_inbound_kind(message_id, "approval")?;
                Ok(Some(match decision {
                    None => format!("审批码 {} 不存在。", argument.to_ascii_uppercase()),
                    Some(decision) if decision.state == ApprovalState::Allowed => {
                        format!("审批 {} 已批准一次。", decision.short_code)
                    }
                    Some(decision) if decision.state == ApprovalState::Denied => {
                        format!("审批 {} 已拒绝。", decision.short_code)
                    }
                    Some(decision) if decision.state == ApprovalState::Expired => {
                        format!("审批 {} 已过期。", decision.short_code)
                    }
                    Some(decision) => {
                        format!("审批 {} 已处于终态，未重复执行。", decision.short_code)
                    }
                }))
            }
            "/answer" if !argument.is_empty() && !remainder.is_empty() => {
                let question = self.database.question_by_code(argument)?;
                let Some(question) = question else {
                    self.database
                        .mark_inbound_kind(message_id, "answer_invalid")?;
                    return Ok(Some(format!(
                        "问题码 {} 不存在。",
                        argument.to_ascii_uppercase()
                    )));
                };
                if question.state != "pending" || question.expires_at <= Utc::now().timestamp() {
                    self.database
                        .mark_inbound_kind(message_id, "answer_expired")?;
                    return Ok(Some(format!(
                        "问题 {} 已过期或已回答。",
                        question.short_code
                    )));
                }
                if remainder.chars().count() > 2000 || contains_secret_value(remainder) {
                    self.database
                        .mark_inbound_kind(message_id, "answer_rejected")?;
                    return Ok(Some("回答过长或包含疑似 Secret，已拒绝。".to_owned()));
                }
                let choices: Vec<String> = serde_json::from_str(&question.choices_json)
                    .context("stored question choices are invalid")?;
                if !question.allow_freeform && !choices.iter().any(|choice| choice == remainder) {
                    self.database
                        .mark_inbound_kind(message_id, "answer_invalid")?;
                    return Ok(Some(format!(
                        "问题 {} 只接受以下选项: {}",
                        question.short_code,
                        choices.join(" / ")
                    )));
                }
                let accepted =
                    self.database
                        .answer_question(question.question_id, remainder, message_id)?;
                self.database.mark_inbound_kind(message_id, "answer")?;
                Ok(Some(if accepted {
                    format!("问题 {} 的回答已提交。", question.short_code)
                } else {
                    format!("问题 {} 已由其他消息处理。", question.short_code)
                }))
            }
            "/cancel" if !argument.is_empty() && remainder.is_empty() => {
                let cancelled = self.database.cancel_session(argument, message_id)?;
                self.database.mark_inbound_kind(message_id, "cancel")?;
                Ok(Some(if cancelled {
                    format!("会话 {} 已取消。", argument.to_ascii_uppercase())
                } else {
                    format!("会话 {} 不存在或已结束。", argument.to_ascii_uppercase())
                }))
            }
            "/message" if !argument.is_empty() && !remainder.is_empty() => {
                if contains_secret_value(remainder) {
                    self.database
                        .mark_inbound_kind(message_id, "message_rejected")?;
                    return Ok(Some("消息包含疑似 Secret，已拒绝。".to_owned()));
                }
                let session = self.database.resolve_session(None, Some(argument))?;
                self.database.queue_message(
                    message_id,
                    &session.session_id,
                    remainder,
                    INBOUND_MESSAGE_TTL_SECONDS,
                )?;
                Ok(Some(format!("消息已排队给会话 {}。", session.short_code)))
            }
            "/status" if argument.is_empty() && remainder.is_empty() => {
                self.database.mark_inbound_kind(message_id, "status")?;
                Ok(Some(format_status(&self.database.status_snapshot()?)))
            }
            "/help" if argument.is_empty() && remainder.is_empty() => {
                self.database.mark_inbound_kind(message_id, "help")?;
                Ok(Some(help_text()))
            }
            "/bind" => {
                self.database.mark_inbound_kind(message_id, "bind_replay")?;
                Ok(Some("Owner 已绑定；绑定码不可重复使用。".to_owned()))
            }
            _ => {
                self.database
                    .mark_inbound_kind(message_id, "unknown_command")?;
                Ok(Some(
                    "未识别命令。审批只接受显式 /allow 或 /deny；发送 /help 查看帮助。".to_owned(),
                ))
            }
        }
    }

    async fn send_if_owner(
        &self,
        kind: &str,
        session_id: Option<&str>,
        idempotency_key: &str,
        content: &str,
        reply_to_message_id: Option<&str>,
    ) -> Result<DeliveryResult> {
        match self.database.owner()? {
            Some(owner) if owner.enabled => {
                self.send_owner(
                    kind,
                    session_id,
                    idempotency_key,
                    content,
                    reply_to_message_id,
                )
                .await
            }
            Some(_) => bail!("QQ remote control is disabled by the local emergency switch"),
            None => bail!("no QQ owner is bound"),
        }
    }

    async fn send_owner(
        &self,
        kind: &str,
        session_id: Option<&str>,
        idempotency_key: &str,
        content: &str,
        reply_to_message_id: Option<&str>,
    ) -> Result<DeliveryResult> {
        let owner = self.database.owner()?.context("no QQ owner is bound")?;
        if !owner.enabled {
            bail!("QQ remote control is disabled by the local emergency switch");
        }
        let safe_content = redact_text(content);
        if safe_content.chars().count() > self.config().bridge.message_total_chars {
            bail!("QQ message exceeds the configured per-delivery length limit");
        }
        let chunks = split_message(&safe_content, self.config().bridge.message_chunk_chars);
        let delivery = self.database.begin_delivery(NewDelivery {
            delivery_id: Uuid::new_v4(),
            idempotency_key: idempotency_key.to_owned(),
            kind: kind.to_owned(),
            session_id: session_id.map(str::to_owned),
        })?;
        if !delivery.created {
            return self.wait_for_existing_delivery(delivery).await;
        }

        let now = Utc::now().timestamp();
        let minute_count = self.database.sent_delivery_count_since(now - 60)?;
        let day_start = Utc::now()
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .context("failed to compute UTC day start")?
            .and_utc()
            .timestamp();
        let day_count = self.database.sent_delivery_count_since(day_start)?;
        let chunk_count = u32::try_from(chunks.len()).context("too many message chunks")?;
        if minute_count.saturating_add(chunk_count) > self.config().bridge.messages_per_minute
            || day_count.saturating_add(chunk_count) > self.config().bridge.messages_per_day
        {
            self.database.finish_delivery(
                delivery.record.delivery_id,
                "failed",
                None,
                Some("rate_limited"),
            )?;
            bail!("QQ message rate limit would be exceeded");
        }

        let mut message_ids = Vec::with_capacity(chunks.len());
        for (index, chunk) in chunks.iter().enumerate() {
            let sequence = u32::try_from(index + 1).context("message sequence overflow")?;
            match self
                .qq
                .send_text(&owner.user_openid, chunk, reply_to_message_id, sequence)
                .await
            {
                Ok(SendReceipt { message_id }) => {
                    if let Err(error) = self
                        .database
                        .record_sent_message(delivery.record.delivery_id, sequence)
                    {
                        self.database.finish_delivery(
                            delivery.record.delivery_id,
                            "in_doubt",
                            Some(&message_id),
                            Some("audit_write_error"),
                        )?;
                        return Err(error).context(
                            "QQ message was sent but its rate-limit event could not be recorded",
                        );
                    }
                    message_ids.push(message_id);
                }
                Err(error) => {
                    self.database.finish_delivery(
                        delivery.record.delivery_id,
                        "in_doubt",
                        message_ids.first().map(String::as_str),
                        Some("qq_delivery_error"),
                    )?;
                    return Err(error);
                }
            }
        }
        let first_message_id = message_ids.first().cloned();
        self.database.finish_delivery(
            delivery.record.delivery_id,
            "sent",
            first_message_id.as_deref(),
            None,
        )?;
        Ok(DeliveryResult {
            sent: true,
            delivery_id: delivery.record.delivery_id,
            qq_message_id: first_message_id,
        })
    }

    async fn wait_for_existing_delivery(&self, delivery: BeginDelivery) -> Result<DeliveryResult> {
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut current = delivery.record;
        loop {
            match current.status.as_str() {
                "sent" => {
                    return Ok(DeliveryResult {
                        sent: true,
                        delivery_id: current.delivery_id,
                        qq_message_id: current.qq_message_id,
                    });
                }
                "failed" => bail!(
                    "previous delivery failed: {}",
                    current.error_code.as_deref().unwrap_or("unknown")
                ),
                "in_doubt" => bail!("previous delivery outcome is uncertain; refusing to resend"),
                "pending" if Instant::now() < deadline => {
                    sleep(POLL_INTERVAL).await;
                    current = self
                        .database
                        .delivery_by_id(current.delivery_id)?
                        .context("delivery disappeared while waiting")?;
                }
                "pending" => bail!("delivery is still pending"),
                state => bail!("delivery entered unknown state {state}"),
            }
        }
    }

    fn ensure_workspace(&self, cwd: &Path) -> Result<()> {
        if !cwd.is_absolute() {
            bail!("hook cwd must be absolute");
        }
        if !self
            .config()
            .bridge
            .workspace_roots
            .iter()
            .any(|root| path_is_within(cwd, root))
        {
            bail!("hook cwd is outside the configured workspace roots");
        }
        Ok(())
    }

    fn session_label_or_unknown(&self, session_id: &str) -> Result<String> {
        Ok(self
            .database
            .session_by_id(session_id)?
            .map_or_else(|| "[unknown]".to_owned(), |session| session.short_code))
    }

    fn reconcile_recent_ahp_sessions(&self, sessions: &[AhpSessionDescriptor]) -> Result<()> {
        let foreground_session_uri = self
            .database
            .ahp_binding()?
            .map(|binding| binding.session_uri);
        let mut candidates: Vec<_> = sessions
            .iter()
            .filter(|session| ahp_session_matches_workspace(&self.config(), session))
            .collect();
        candidates.sort_by(|left, right| right.modified_at.cmp(&left.modified_at));
        if let Some(foreground_session_uri) = foreground_session_uri
            && let Some(index) = candidates
                .iter()
                .position(|session| session.session_uri == foreground_session_uri)
        {
            let foreground = candidates.remove(index);
            candidates.insert(0, foreground);
        }

        let mut seen = HashSet::new();
        for session in candidates
            .into_iter()
            .filter(|session| seen.insert(session.session_uri.as_str()))
            .take(MAX_TRACKED_AHP_SESSIONS)
        {
            if let Err(error) = self
                .database
                .ahp_track_session(&session.endpoint_id, &session.session_uri)
            {
                tracing::warn!(
                    session_code = session.short_code.as_deref().unwrap_or("[unknown]"),
                    error = %error,
                    "recent AHP Session could not be tracked"
                );
            }
        }
        Ok(())
    }

    fn ahp_session_label(&self, session_uri: &str) -> Result<String> {
        let Some(session) = self.database.ahp_session_by_uri(session_uri)? else {
            return Ok("[Session unknown]".to_owned());
        };
        Ok(format!(
            "[{} · {}]",
            session.short_code.as_deref().unwrap_or("[unknown]"),
            session.title
        ))
    }

    fn require_ahp_enabled(&self) -> Result<()> {
        if !self.config().ahp.enabled {
            bail!("AHP mode is disabled");
        }
        Ok(())
    }

    fn validate_ahp_session_workspace(&self, endpoint_id: &str, session_uri: &str) -> Result<()> {
        validate_identifier("endpoint_id", endpoint_id)?;
        validate_identifier("session_uri", session_uri)?;
        if self.config().ahp.effective_authorized_targets().is_empty() {
            bail!("AHP target workspaces are not configured");
        }
        let session = self
            .database
            .ahp_list_sessions()?
            .into_iter()
            .find(|session| {
                session.endpoint_id == endpoint_id && session.session_uri == session_uri
            })
            .context("AHP session is not present in the current catalogue")?;
        if ahp_session_target_display(&self.config(), &session).is_none() {
            bail!("AHP session does not target a configured workspace");
        }
        Ok(())
    }
}

fn validate_ahp_registration(registration: &AhpAdapterRegistration) -> Result<()> {
    validate_identifier("adapter_id", &registration.adapter_id)?;
    validate_identifier("adapter_instance_id", &registration.adapter_instance_id)?;
    validate_short_code("adapter version", &registration.version)?;
    if registration.supported_protocols.is_empty() || registration.supported_protocols.len() > 16 {
        bail!("AHP adapter must declare 1 to 16 protocol versions");
    }
    for protocol in &registration.supported_protocols {
        validate_short_code("AHP protocol version", protocol)?;
    }
    Ok(())
}

fn validate_ahp_catalog(
    hosts: &[AhpHostDescriptor],
    sessions: &[AhpSessionDescriptor],
    removed_session_uris: &[String],
) -> Result<()> {
    if hosts.len() > 32 || sessions.len() > 2_000 || removed_session_uris.len() > 2_000 {
        bail!("AHP catalogue exceeds the configured safety limit");
    }
    for host in hosts {
        validate_identifier("endpoint_id", &host.endpoint_id)?;
        validate_identifier("host_instance_id", &host.host_instance_id)?;
        validate_short_code("advertised_protocol", &host.advertised_protocol)?;
        if let Some(protocol) = host.selected_protocol.as_deref() {
            validate_short_code("selected_protocol", protocol)?;
        }
    }
    for session in sessions {
        validate_identifier("session_uri", &session.session_uri)?;
        validate_identifier("session endpoint_id", &session.endpoint_id)?;
        validate_identifier("session host_instance_id", &session.host_instance_id)?;
        if session.title.chars().count() > 500
            || session.provider.len() > 128
            || session.workspace_uris.len() > 16
        {
            bail!("AHP session metadata exceeds the safety limit");
        }
        for uri in &session.workspace_uris {
            validate_identifier("workspace URI", uri)?;
        }
    }
    for session_uri in removed_session_uris {
        validate_identifier("removed session_uri", session_uri)?;
        if sessions
            .iter()
            .any(|session| session.session_uri == *session_uri)
        {
            bail!("AHP catalogue cannot add and remove the same Session");
        }
    }
    Ok(())
}

fn validate_ahp_events(events: &[AhpPublishedEvent]) -> Result<()> {
    if events.is_empty() || events.len() > 256 {
        bail!("AHP event batch must contain 1 to 256 events");
    }
    for event in events {
        validate_identifier("event_id", &event.event_id)?;
        validate_identifier("host_instance_id", &event.host_instance_id)?;
        validate_identifier("event session_uri", &event.session_uri)?;
        if let Some(chat_uri) = event.chat_uri.as_deref() {
            validate_identifier("event chat_uri", chat_uri)?;
        }
        if let Some(turn_id) = event.turn_id.as_deref() {
            validate_identifier("event turn_id", turn_id)?;
        }
        if let Some(client_id) = event.origin_client_id.as_deref() {
            validate_identifier("event origin_client_id", client_id)?;
        }
        if canonical_json(&event.data).len() > 256 * 1024 {
            bail!("AHP event payload exceeds 256 KiB");
        }
        validate_ahp_event_payload(event)?;
    }
    Ok(())
}

fn validate_ahp_event_payload(event: &AhpPublishedEvent) -> Result<()> {
    match event.kind {
        AhpEventKind::UserMessage | AhpEventKind::AssistantMessage => {
            let data: AhpTextEventData =
                serde_json::from_value(event.data.clone()).context("invalid AHP text event")?;
            validate_identifier("AHP message_id", &data.message_id)?;
            if data.content.chars().count() > 100_000 {
                bail!("AHP message content exceeds the safety limit");
            }
        }
        AhpEventKind::ToolStatus => {
            let data: AhpToolEventData =
                serde_json::from_value(event.data.clone()).context("invalid AHP tool event")?;
            validate_identifier("AHP tool_call_id", &data.tool_call_id)?;
            validate_identifier("AHP tool_name", &data.tool_name)?;
            validate_short_code("AHP tool status", &data.status)?;
            if data.summary.chars().count() > 4_000 {
                bail!("AHP tool summary exceeds the safety limit");
            }
        }
        AhpEventKind::ApprovalPending => {
            let data: AhpApprovalPendingData =
                serde_json::from_value(event.data.clone()).context("invalid AHP approval event")?;
            validate_identifier("AHP approval_key", &data.approval_key)?;
            validate_identifier("AHP tool_call_id", &data.tool_call_id)?;
            validate_identifier("AHP tool_name", &data.tool_name)?;
            if !matches!(data.stage.as_str(), "parameter" | "result")
                || data.summary.chars().count() > 4_000
                || event.chat_uri.is_none()
                || event.turn_id.is_none()
            {
                bail!("AHP approval event is invalid");
            }
        }
        AhpEventKind::ApprovalResolved => {
            let data: AhpApprovalResolvedData = serde_json::from_value(event.data.clone())
                .context("invalid AHP approval resolution")?;
            validate_identifier("AHP approval_key", &data.approval_key)?;
        }
        AhpEventKind::InputPending => {
            let data: AhpInputPendingData =
                serde_json::from_value(event.data.clone()).context("invalid AHP input event")?;
            validate_identifier("AHP input_key", &data.input_key)?;
            validate_identifier("AHP request_id", &data.request_id)?;
            if data.prompt.chars().count() > 4_000
                || data.choices.len() > 20
                || data
                    .choices
                    .iter()
                    .any(|choice| choice.chars().count() > 500)
                || event.chat_uri.is_none()
                || !matches!(data.selection_mode.as_str(), "none" | "single" | "multi")
            {
                bail!("AHP input event exceeds the safety limit");
            }
        }
        AhpEventKind::InputResolved => {
            let data: AhpInputResolvedData = serde_json::from_value(event.data.clone())
                .context("invalid AHP input resolution")?;
            validate_identifier("AHP input_key", &data.input_key)?;
            if !matches!(data.outcome.as_str(), "answered" | "declined" | "cancelled") {
                bail!("AHP input outcome is invalid");
            }
        }
        AhpEventKind::TurnCancelled | AhpEventKind::TurnFailed | AhpEventKind::HostDisconnected => {
            let data: AhpTurnEventData =
                serde_json::from_value(event.data.clone()).context("invalid AHP turn event")?;
            if data
                .summary
                .as_ref()
                .is_some_and(|summary| summary.chars().count() > 4_000)
            {
                bail!("AHP turn summary exceeds the safety limit");
            }
        }
        AhpEventKind::SessionSnapshot
        | AhpEventKind::ChatSnapshot
        | AhpEventKind::TurnStarted
        | AhpEventKind::TurnCompleted => {}
    }
    Ok(())
}

fn validate_short_code(name: &str, value: &str) -> Result<()> {
    if value.trim().is_empty()
        || value.len() > 128
        || value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
    {
        bail!("{name} is invalid");
    }
    Ok(())
}

fn should_notify_tool(mode: AhpToolNotificationMode, status: &str) -> bool {
    match mode {
        AhpToolNotificationMode::Full => true,
        AhpToolNotificationMode::Compact => matches!(status, "completed" | "cancelled"),
        AhpToolNotificationMode::ApprovalOnly => false,
    }
}

fn format_offline_replay_content(projections: &[AhpProjectionRecord]) -> String {
    let mut content = String::from("[QQ 离线期间未实时送达]\n");
    for projection in projections {
        content.push_str(&format!(
            "\n[{}]\n{}",
            projection.kind,
            projection.content.trim()
        ));
    }
    content
}

fn offline_replay_delivery_key(projections: &[AhpProjectionRecord]) -> String {
    let mut identities: Vec<_> = projections
        .iter()
        .map(|projection| format!("{}:{}", projection.event_id, projection.replay_generation))
        .collect();
    identities.sort_unstable();
    format!(
        "ahp-offline-replay:{}",
        sha256_hex(identities.join("\0").as_bytes())
    )
}

fn offline_replay_delivery_is_terminal(status: &str) -> bool {
    matches!(status, "pending" | "sent" | "in_doubt")
}

fn deny(reason: &str) -> PermissionResult {
    PermissionResult {
        decision: PermissionDecision::Deny,
        reason: reason.to_owned(),
    }
}

fn validate_identifier(name: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() || value.len() > 512 || value.chars().any(char::is_control) {
        bail!("{name} is invalid");
    }
    Ok(())
}

fn approval_message(approval: &ApprovalRecord, session_label: &str) -> String {
    format!(
        "[Copilot 工具审批]\n会话: {session_label}\n风险: {}\n{}\n有效期: {}\n\n批准: /allow {}\n拒绝: /deny {}",
        approval.risk.as_str(),
        approval.redacted_summary,
        chrono::DateTime::from_timestamp(approval.expires_at, 0)
            .map(|time| time.format("%Y-%m-%d %H:%M:%S UTC").to_string())
            .unwrap_or_else(|| "[invalid]".to_owned()),
        approval.short_code,
        approval.short_code
    )
}

fn question_message(
    question: &QuestionRecord,
    session_label: &str,
    prompt: &str,
    choices: &[String],
) -> String {
    let choices = if choices.is_empty() {
        String::new()
    } else {
        format!(
            "\n选项:\n{}",
            choices
                .iter()
                .enumerate()
                .map(|(index, choice)| format!("{}. {choice}", index + 1))
                .collect::<Vec<_>>()
                .join("\n")
        )
    };
    format!(
        "[Copilot 提问]\n会话: {session_label}\n{prompt}{choices}\n\n回答: /answer {} <文本>",
        question.short_code
    )
}

fn split_message(content: &str, max_chars: usize) -> Vec<String> {
    if content.chars().count() <= max_chars {
        return vec![content.to_owned()];
    }
    let mut chunks = Vec::new();
    let mut current = String::new();
    for line in content.split_inclusive('\n') {
        if current.chars().count() + line.chars().count() <= max_chars {
            current.push_str(line);
            continue;
        }
        if !current.is_empty() {
            chunks.push(std::mem::take(&mut current));
        }
        let mut remaining = line;
        while remaining.chars().count() > max_chars {
            let split_byte = remaining
                .char_indices()
                .nth(max_chars)
                .map(|(index, _)| index)
                .unwrap_or(remaining.len());
            chunks.push(remaining[..split_byte].to_owned());
            remaining = &remaining[split_byte..];
        }
        current.push_str(remaining);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

fn format_status(status: &crate::db::StatusSnapshot) -> String {
    let sessions = if status.sessions.is_empty() {
        "无会话".to_owned()
    } else {
        status
            .sessions
            .iter()
            .map(|session| {
                format!(
                    "{}: {}{}",
                    session.session_label,
                    session.state,
                    if session.final_sent {
                        ", final已发送"
                    } else {
                        ""
                    }
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    let approvals = if status.pending_approvals.is_empty() {
        "无待审批".to_owned()
    } else {
        status
            .pending_approvals
            .iter()
            .map(|approval| {
                format!(
                    "{} / {} / {}",
                    approval.approval_code, approval.session_label, approval.tool_name
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    format!("Owner: 已启用\n会话:\n{sessions}\n待审批:\n{approvals}")
}

fn format_ahp_status(status: &AhpStatus, mode: AhpToolNotificationMode) -> String {
    let adapter = status.adapter.as_ref().map_or_else(
        || "未连接".to_owned(),
        |adapter| format!("{} ({})", adapter.state, adapter.version),
    );
    let bindings = if status.bindings.is_empty() {
        "未绑定".to_owned()
    } else {
        status
            .bindings
            .iter()
            .map(|binding| {
                let session = status
                    .sessions
                    .iter()
                    .find(|session| session.session_uri == binding.session_uri);
                let code = session
                    .and_then(|session| session.short_code.as_deref())
                    .unwrap_or("[unknown]");
                let title = session.map_or("[unknown]", |session| session.title.as_str());
                format!(
                    "{}{} · {} / {}{}",
                    if binding.foreground { "* " } else { "  " },
                    code,
                    title,
                    binding.state,
                    if binding.active_turn_id.is_some() {
                        " / 运行中"
                    } else {
                        ""
                    }
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    format!(
        "AHP Adapter: {adapter}\n监控 Session ({}/{}):\n{bindings}\n通知模式: {}\n可见 Host: {}\n可见 Session: {}\n待处理命令: {}\n待审批/待回答: {}/{}\nQQ 待补发事件: {}",
        status.bindings.len(),
        MAX_TRACKED_AHP_SESSIONS,
        mode.as_str(),
        status.hosts.len(),
        status.sessions.len(),
        status.pending_commands,
        status.pending_approvals,
        status.pending_inputs,
        status.pending_projections
    )
}

fn format_ahp_approval(approval: &AhpApprovalRecord, session_label: &str) -> String {
    format!(
        "{session_label}\n[工具{}审批 {}]\n工具: {}\n{}\n\n批准一次: /allow {}\n拒绝: /deny {}",
        if approval.stage == "result" {
            "结果复核"
        } else {
            "执行前"
        },
        approval.short_code,
        approval.tool_name,
        approval.summary,
        approval.short_code,
        approval.short_code
    )
}

fn escape_qq_markdown(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        if matches!(
            character,
            '\\' | '*' | '_' | '`' | '[' | ']' | '(' | ')' | '#' | '>' | '|'
        ) {
            escaped.push('\\');
        }
        escaped.push(character);
    }
    escaped
}

fn truncate_for_qq(value: &str, maximum: usize) -> String {
    if value.chars().count() <= maximum {
        return value.to_owned();
    }
    if maximum <= 3 {
        return ".".repeat(maximum);
    }
    format!(
        "{}...",
        value
            .chars()
            .take(maximum.saturating_sub(3))
            .collect::<String>()
    )
}

fn format_ahp_input(input: &AhpInputRecord, session_label: &str) -> String {
    let choices = if input.choices.is_empty() {
        String::new()
    } else {
        format!("\n选项: {}", input.choices.join(" / "))
    };
    format!(
        "{session_label}\n[Agent 提问 {}]\n{}{}\n\n使用 /answer {} <文本> 显式回答；仅前台 Session 的唯一待输入可直接回复。",
        input.short_code, input.prompt, choices, input.short_code
    )
}

fn is_removed_session_creation_request(content: &str) -> bool {
    let verb = content
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    verb == "/new"
        || ["newt_", "newm_", "newa_", "newx_", "nw_"]
            .iter()
            .any(|prefix| verb.starts_with(prefix))
}

fn ahp_help_text(mode: AhpToolNotificationMode) -> String {
    format!(
        "AHP 多 Session 共享会话命令:
【消息】
普通文本：发送到前台 Session；仅优先回答前台 Session 的待澄清问题
QQ 语音：使用内置 ASR；不会执行控制命令
/ask <文本>：忽略待回答问题，排队为新消息
/send <编号> <文本>：发送到指定 Session，不改变前台

【Session】
在 PC 上创建 Session、登录并选择模型，再从 QQ 选择已有 Session
/sessions：按最近使用显示候选 Session，前台和活动会话始终保留
/switch：显示同一批候选的前台切换按钮
/switch <编号>：切换前台，其他任务继续后台运行
/detach <编号>：安全停止后台监控
/cancel：取消前台 Session 的当前 Turn
/cancel <编号>：取消指定 Session 的当前 Turn
最多同时监控 {} 个最近活跃 Session；活动、排队或待交互 Session 不会被 LRU 淘汰

【审批与回答】
/allow <审批码>：单次批准
/deny <审批码>：拒绝
/answer <问题码> <文本>：显式回答 Agent 问题

【通知】
/notify：查看当前模式
/notify approval_only：仅审批和最终回复
/notify compact：审批、工具终态和最终回复
/notify full：审批、全部工具状态和最终回复
当前模式: {}

【状态】
/status：查看连接、绑定、通知模式和待处理状态
/help：显示本帮助",
        MAX_TRACKED_AHP_SESSIONS,
        mode.as_str()
    )
}

fn format_notification_mode(mode: AhpToolNotificationMode) -> String {
    format!(
        "当前通知模式: {}\n{}\n\n切换命令:\n/notify approval_only\n/notify compact\n/notify full\n切换后立即生效并持久化，无需重启。",
        mode.as_str(),
        notification_mode_description(mode)
    )
}

fn notification_mode_description(mode: AhpToolNotificationMode) -> &'static str {
    match mode {
        AhpToolNotificationMode::ApprovalOnly => {
            "仅发送工具审批和完整 Assistant 最终回复，不发送普通工具状态。"
        }
        AhpToolNotificationMode::Compact => {
            "发送工具完成/取消状态；工具审批和完整 Assistant 最终回复照常发送。"
        }
        AhpToolNotificationMode::Full => {
            "发送工具全部状态变化；工具审批和完整 Assistant 最终回复照常发送。"
        }
    }
}

fn select_ahp_session_menu(
    config: &AppConfig,
    bindings: &[crate::protocol::AhpBindingRecord],
    hosts: &[AhpHostDescriptor],
    candidates: Vec<AhpSessionMenuCandidate>,
) -> AhpSessionMenu {
    const IN_PROGRESS: u32 = 1 << 3;
    const INPUT_NEEDED: u32 = 1 << 4;
    let mut entries: Vec<_> = candidates
        .into_iter()
        .filter_map(|candidate| {
            let session = candidate.session;
            let workspace = ahp_session_target_display(config, &session)?;
            let binding = bindings
                .iter()
                .find(|binding| binding.session_uri == session.session_uri);
            let foreground = binding.is_some_and(|binding| binding.foreground);
            let active = session.status & IN_PROGRESS != 0
                || binding.is_some_and(|binding| binding.active_turn_id.is_some());
            let queued = binding.map_or(0, |binding| binding.queued_message_count);
            let waiting_input = candidate.pending_inputs || session.status & INPUT_NEEDED != 0;
            let protected = foreground
                || active
                || queued != 0
                || candidate.pending_commands
                || candidate.pending_approvals
                || waiting_input
                || binding.is_some_and(|binding| binding.state == "binding");
            let role = if foreground {
                "前台"
            } else if binding.is_some() {
                "后台"
            } else {
                "未监控"
            };
            let activity = if candidate.pending_approvals {
                "待审批"
            } else if waiting_input {
                "待回答/确认"
            } else if active {
                if binding.is_some() {
                    "运行中"
                } else {
                    "忙碌"
                }
            } else if binding.is_some_and(|binding| binding.state == "binding") {
                "连接中"
            } else if binding.is_some_and(|binding| binding.state != "bound") {
                "待恢复"
            } else if session.status & 2 != 0 {
                "失败"
            } else if ahp_session_is_idle(&session) {
                "空闲"
            } else {
                "状态未知"
            };
            let mut state = format!("{role} · {activity}");
            if queued != 0 {
                state.push_str(&format!(" · 排队 {queued} 条"));
            }
            if candidate.pending_commands {
                state.push_str(" · 命令待处理");
            }
            Some(AhpSessionMenuEntry {
                last_used_at: candidate.last_used_at.max(session_modified_at(&session)),
                foreground,
                protected,
                state,
                workspace,
                host_label: session
                    .host_label
                    .clone()
                    .unwrap_or_else(|| infer_session_host_label(&session)),
                host_status: describe_session_host_status(&session, hosts),
                session,
            })
        })
        .collect();
    entries.sort_by(|left, right| {
        right
            .last_used_at
            .cmp(&left.last_used_at)
            .then_with(|| left.session.session_uri.cmp(&right.session.session_uri))
    });
    let total_count = entries.len();
    let protected_count = entries.iter().filter(|entry| entry.protected).count();
    let limit = config.ahp.session_list_limit;
    let mut idle_slots = limit.saturating_sub(protected_count);
    entries.retain(|entry| {
        if entry.protected {
            true
        } else if idle_slots > 0 {
            idle_slots -= 1;
            true
        } else {
            false
        }
    });
    entries.sort_by_key(|entry| !entry.foreground);
    AhpSessionMenu {
        entries,
        total_count,
        protected_count,
        tracked_count: bindings.len(),
        limit,
    }
}

fn session_modified_at(session: &AhpSessionDescriptor) -> i64 {
    match chrono::DateTime::parse_from_rfc3339(&session.modified_at) {
        Ok(modified_at) => modified_at.timestamp_millis(),
        Err(error) => {
            tracing::warn!(
                session_code = session.short_code.as_deref().unwrap_or("[unknown]"),
                error = %error,
                "invalid Session modification time; using creation time for menu ordering"
            );
            chrono::DateTime::parse_from_rfc3339(&session.created_at)
                .map(|created_at| created_at.timestamp_millis())
                .unwrap_or_else(|error| {
                    tracing::warn!(error = %error, "invalid Session creation time for menu ordering");
                    0
                })
        }
    }
}

fn session_menu_text(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect()
}

fn format_ahp_session_menu_summary(menu: &AhpSessionMenu) -> String {
    let mut summary = format!(
        "显示 {}/{} 个 · 常规保留 {} 个\n监控 {}/{} 个 · 前台置顶，其余按最近使用排序",
        menu.entries.len(),
        menu.total_count,
        menu.limit,
        menu.tracked_count,
        MAX_TRACKED_AHP_SESSIONS,
    );
    let hidden = menu.total_count - menu.entries.len();
    if hidden != 0 {
        summary.push_str(&format!(
            "\n已收起 {hidden} 个较旧会话，仅精简 QQ 列表，不删除历史；仍可按原编号切换。"
        ));
    }
    if menu.entries.len() > menu.limit {
        summary.push_str(&format!(
            "\n前台、活动或待处理会话共 {} 个，已全部保留，不受列表上限影响。",
            menu.protected_count,
        ));
    }
    summary
}

fn format_ahp_session_entry(entry: &AhpSessionMenuEntry, index: usize, markdown: bool) -> String {
    let code = entry.session.short_code.as_deref().unwrap_or("[unknown]");
    let title = truncate_for_qq(&session_menu_text(&entry.session.title), 64);
    let recent = chrono::DateTime::<Utc>::from_timestamp_millis(entry.last_used_at)
        .filter(|_| entry.last_used_at > 0)
        .map(|time| {
            time.with_timezone(&chrono::Local)
                .format("%m-%d %H:%M")
                .to_string()
        })
        .unwrap_or_else(|| "未知".to_owned());
    let text = |value: &str| {
        let value = session_menu_text(value);
        if markdown {
            escape_qq_markdown(&value)
        } else {
            value
        }
    };
    let heading = if markdown {
        format!("{}. **{} · {}**", index + 1, text(code), text(&title))
    } else {
        format!("{}. [{}] {}", index + 1, code, title)
    };
    let line_break = if markdown { "  \n" } else { "\n" };
    format!(
        "{heading}{line_break}{} · {}{line_break}位置：{} · {}{line_break}最近使用：{recent}",
        entry.state,
        entry.host_status,
        text(&entry.host_label),
        text(&entry.workspace),
    )
}

fn format_ahp_sessions(menu: &AhpSessionMenu) -> String {
    if menu.entries.is_empty() {
        return "目标目录中没有可绑定的 AHP Session。".to_owned();
    }
    let lines = menu
        .entries
        .iter()
        .enumerate()
        .map(|(index, entry)| format_ahp_session_entry(entry, index, false))
        .collect::<Vec<_>>()
        .join("\n\n");
    format!(
        "【会话列表】\n{}\n\n{lines}\n\n/switch <编号> 切换前台\n/send <编号> <文本> 定向发送；切换不会停止其他任务。",
        format_ahp_session_menu_summary(menu),
    )
}

fn ahp_session_matches_workspace(config: &AppConfig, session: &AhpSessionDescriptor) -> bool {
    config
        .ahp
        .effective_authorized_targets()
        .iter()
        .any(|target| session_matches_authorized_target(target, session))
}

fn ahp_session_target_workspace<'a>(
    config: &'a AppConfig,
    session: &AhpSessionDescriptor,
) -> Option<&'a Path> {
    config.ahp.shared_workspaces.iter().find_map(|configured| {
        session
            .workspace_uris
            .iter()
            .any(|workspace_uri| {
                url::Url::parse(workspace_uri)
                    .ok()
                    .and_then(|url| url.to_file_path().ok())
                    .is_some_and(|path| {
                        path_is_within(&path, configured) && path_is_within(configured, &path)
                    })
            })
            .then_some(configured.as_path())
    })
}

fn ahp_session_target_display(
    config: &AppConfig,
    session: &AhpSessionDescriptor,
) -> Option<String> {
    config
        .ahp
        .effective_authorized_targets()
        .iter()
        .find(|target| session_matches_authorized_target(target, session))
        .map(|target| target.display_workspace())
}

fn authorized_target_to_managed_target(target: &AhpAuthorizedTarget) -> AhpManagedTarget {
    match target {
        AhpAuthorizedTarget::Local { path } => AhpManagedTarget::Local {
            path: path.display().to_string(),
        },
        AhpAuthorizedTarget::Ssh {
            alias,
            path,
            user,
            host,
            port,
            host_key_fingerprints,
        } => AhpManagedTarget::Ssh {
            alias: alias.clone(),
            path: path.clone(),
            user: user.clone(),
            host: host.clone(),
            port: *port,
            host_key_fingerprints: host_key_fingerprints.clone(),
        },
    }
}

fn infer_session_host_label(session: &AhpSessionDescriptor) -> String {
    match session.target_kind {
        Some(AhpTargetKind::Local) => "local".to_owned(),
        Some(AhpTargetKind::Ssh) => session
            .ssh_alias
            .as_deref()
            .map(|alias| format!("ssh:{alias}"))
            .unwrap_or_else(|| "ssh".to_owned()),
        None => "host".to_owned(),
    }
}

fn describe_session_host_status(
    session: &AhpSessionDescriptor,
    hosts: &[AhpHostDescriptor],
) -> String {
    if let Some(host) = hosts
        .iter()
        .find(|host| host.endpoint_id == session.endpoint_id)
    {
        match host.state {
            crate::protocol::AhpHostState::Connected => "在线".to_owned(),
            crate::protocol::AhpHostState::ReadOnly => "只读".to_owned(),
            crate::protocol::AhpHostState::Incompatible => "协议不兼容".to_owned(),
            crate::protocol::AhpHostState::Unreachable => "离线/缓存".to_owned(),
        }
    } else {
        "离线/缓存".to_owned()
    }
}

fn session_host_is_online(session: &AhpSessionDescriptor, hosts: &[AhpHostDescriptor]) -> bool {
    hosts.iter().any(|host| {
        host.endpoint_id == session.endpoint_id
            && matches!(
                host.state,
                crate::protocol::AhpHostState::Connected | crate::protocol::AhpHostState::ReadOnly
            )
    })
}

fn session_matches_authorized_target(
    target: &AhpAuthorizedTarget,
    session: &AhpSessionDescriptor,
) -> bool {
    match target {
        AhpAuthorizedTarget::Local { path } => {
            if session.target_kind == Some(AhpTargetKind::Local)
                && session.target_path.as_deref().is_some_and(|target_path| {
                    let target_path = PathBuf::from(target_path);
                    path_is_within(path, &target_path) && path_is_within(&target_path, path)
                })
            {
                return true;
            }
            session.workspace_uris.iter().any(|workspace_uri| {
                url::Url::parse(workspace_uri)
                    .ok()
                    .and_then(|url| url.to_file_path().ok())
                    .is_some_and(|workspace| {
                        path_is_within(&workspace, path) && path_is_within(path, &workspace)
                    })
            })
        }
        AhpAuthorizedTarget::Ssh { alias, path, .. } => {
            if session.target_kind == Some(AhpTargetKind::Ssh)
                && session.ssh_alias.as_deref() == Some(alias.as_str())
                && session.target_path.as_deref() == Some(path.as_str())
            {
                return true;
            }
            session.workspace_uris.iter().any(|workspace_uri| {
                parse_remote_workspace_target(workspace_uri).is_some_and(
                    |(workspace_alias, workspace_path)| {
                        workspace_alias == *alias && workspace_path == *path
                    },
                ) || (session.ssh_alias.as_deref() == Some(alias.as_str())
                    && parse_remote_file_workspace(workspace_uri)
                        .is_some_and(|workspace_path| workspace_path == *path))
            })
        }
    }
}

fn parse_remote_workspace_target(value: &str) -> Option<(String, String)> {
    let url = url::Url::parse(value).ok()?;
    if url.scheme() != "vscode-remote" {
        return None;
    }
    let host = urlencoding::decode(url.host_str()?).ok()?;
    let alias = host.strip_prefix("ssh-remote+")?;
    let path = normalize_posix_path(&urlencoding::decode(url.path()).ok()?)?;
    Some((alias.to_owned(), path))
}

fn parse_remote_file_workspace(value: &str) -> Option<String> {
    let url = url::Url::parse(value).ok()?;
    (url.scheme() == "file")
        .then(|| normalize_posix_path(url.path()))
        .flatten()
}

fn normalize_posix_path(value: &str) -> Option<String> {
    if !value.starts_with('/') {
        return None;
    }
    let mut parts = Vec::new();
    for part in value.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            other => parts.push(other),
        }
    }
    Some(format!("/{}", parts.join("/")))
}

fn ahp_session_is_idle(session: &AhpSessionDescriptor) -> bool {
    const IDLE: u32 = 1;
    const IN_PROGRESS: u32 = 1 << 3;
    session.status & IDLE != 0 && session.status & IN_PROGRESS == 0
}

fn help_text() -> String {
    [
        "QQ Copilot 远程监管命令:",
        "/allow <审批码>",
        "/deny <审批码>",
        "/answer <问题码> <文本>",
        "/message <会话码> <文本>",
        "/cancel <会话码>",
        "/status",
        "/help",
        "自由文本不会被解释为批准。",
    ]
    .join("\n")
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::{
        config::AppConfig,
        protocol::{BridgeRequest, PermissionResult},
        qq::MockQqMessenger,
    };

    use super::*;

    fn test_session_menu_candidate(number: usize, workspace: &Path) -> AhpSessionMenuCandidate {
        let mut session = test_ahp_session(number, workspace, 1);
        let time = chrono::DateTime::parse_from_rfc3339("2026-09-01T00:00:00Z").expect("time")
            + chrono::Duration::minutes(i64::try_from(number).expect("number"));
        session.created_at = time.to_rfc3339();
        session.modified_at = time.to_rfc3339();
        session.short_code = Some(format!("A{number:04}"));
        AhpSessionMenuCandidate {
            session,
            last_used_at: 0,
            pending_commands: false,
            pending_approvals: false,
            pending_inputs: false,
        }
    }

    #[test]
    fn qq_session_menu_applies_exact_lru_limit_before_rendering() {
        let fixture = Fixture::new_ahp();
        let mut config = fixture.service.config().as_ref().clone();
        for count in [0, 1, 9, 10, 11, 150] {
            let candidates = (1..=count)
                .map(|number| test_session_menu_candidate(number, &fixture.workspace))
                .collect();
            let menu = select_ahp_session_menu(&config, &[], &[test_ahp_host()], candidates);
            assert_eq!(menu.entries.len(), count.min(10));
            assert_eq!(menu.total_count, count);
            let codes: Vec<_> = menu
                .entries
                .iter()
                .map(|entry| entry.session.short_code.clone().expect("code"))
                .collect();
            let expected: Vec<_> = (1..=count)
                .rev()
                .take(10)
                .map(|number| format!("A{number:04}"))
                .collect();
            assert_eq!(codes, expected);
        }
        config.ahp.session_list_limit = 2;
        let mut candidates: Vec<_> = (1..=5)
            .map(|number| test_session_menu_candidate(number, &fixture.workspace))
            .collect();
        candidates[0].last_used_at = Utc::now().timestamp_millis();
        let outside = fixture._directory.path().join("unauthorized");
        candidates.push(test_session_menu_candidate(500, &outside));
        let menu = select_ahp_session_menu(&config, &[], &[], candidates);
        assert_eq!(
            menu.total_count, 5,
            "unauthorized entries must not consume the limit"
        );
        assert_eq!(menu.entries.len(), 2);
        assert_eq!(menu.entries[0].session.short_code.as_deref(), Some("A0001"));
        assert_eq!(menu.entries[1].session.short_code.as_deref(), Some("A0005"));
    }

    #[test]
    fn qq_session_menu_compares_instants_and_preserves_all_protected_sessions() {
        let fixture = Fixture::new_ahp();
        let mut config = fixture.service.config().as_ref().clone();
        config.ahp.session_list_limit = 1;
        let mut earlier = test_session_menu_candidate(1, &fixture.workspace);
        earlier.session.modified_at = "2026-09-18T01:00:00+08:00".to_owned();
        let mut later = test_session_menu_candidate(2, &fixture.workspace);
        later.session.modified_at = "2026-09-17T18:00:00Z".to_owned();
        let menu = select_ahp_session_menu(&config, &[], &[], vec![earlier, later]);
        assert_eq!(menu.entries[0].session.short_code.as_deref(), Some("A0002"));

        config.ahp.session_list_limit = 10;
        let foreground = fixture
            .service
            .database()
            .ahp_binding()
            .expect("query")
            .expect("binding");
        let mut queued = foreground.clone();
        queued.session_uri = "copilot:/session-2".to_owned();
        queued.foreground = false;
        queued.queued_message_count = 1;
        let mut active = queued.clone();
        active.session_uri = "copilot:/session-3".to_owned();
        active.queued_message_count = 0;
        active.active_turn_id = Some("menu-turn".to_owned());
        let mut candidates: Vec<_> = (1..=20)
            .map(|number| test_session_menu_candidate(number, &fixture.workspace))
            .collect();
        candidates[3].session.status = 8;
        candidates[4].session.status = 24;
        candidates[5].pending_approvals = true;
        candidates[6].pending_inputs = true;
        candidates[7].pending_commands = true;
        let menu = select_ahp_session_menu(&config, &[foreground, queued, active], &[], candidates);
        assert_eq!(menu.protected_count, 8);
        assert_eq!(menu.entries.len(), 10);
        for number in [1, 2, 3, 4, 5, 6, 7, 8, 19, 20] {
            assert!(
                menu.entries
                    .iter()
                    .any(|entry| entry.session.short_code.as_deref()
                        == Some(format!("A{number:04}").as_str()))
            );
        }
        assert!(menu.entries[0].foreground);
        let mut candidates: Vec<_> = (1..=15)
            .map(|number| test_session_menu_candidate(number, &fixture.workspace))
            .collect();
        for candidate in candidates.iter_mut().take(12) {
            candidate.session.status = 24;
        }
        let menu = select_ahp_session_menu(&config, &[], &[], candidates);
        assert_eq!(menu.entries.len(), 12);
        assert!(menu.entries.iter().all(|entry| entry.protected));
        assert!(format_ahp_sessions(&menu).contains("已全部保留"));
        assert!(format_ahp_sessions(&menu).contains("收起 3 个"));
    }

    #[test]
    fn qq_session_menu_formats_readable_bounded_entries_and_escapes_markdown() {
        let fixture = Fixture::new_ahp();
        let config = fixture.service.config();
        let mut candidates: Vec<_> = (1..=10)
            .map(|number| test_session_menu_candidate(number, &fixture.workspace))
            .collect();
        for candidate in &mut candidates {
            candidate.session.title = "长标题".repeat(100);
        }
        let mut host = test_ahp_host();
        host.state = crate::protocol::AhpHostState::Unreachable;
        let menu = select_ahp_session_menu(&config, &[], &[host], candidates);
        let text = format_ahp_sessions(&menu);
        assert!(text.starts_with("【会话列表】\n显示 10/10 个"));
        assert!(text.contains("\n未监控 · 空闲 · 离线/缓存\n位置：local · "));
        assert!(text.contains(&fixture.workspace.display().to_string()));
        assert!(text.contains("最近使用："));
        assert_eq!(text.matches("\n\n").count(), 11);
        assert!(text.chars().count() < config.bridge.message_total_chars);
        assert!(!text.contains(" | "));
        let mut candidate = test_session_menu_candidate(1, &fixture.workspace);
        candidate.session.title = "第一行\n**bold** [name](target)\t末尾".to_owned();
        let menu = select_ahp_session_menu(&config, &[], &[], vec![candidate]);
        let markdown = format_ahp_session_entry(&menu.entries[0], 0, true);
        assert!(markdown.starts_with("1. **A0001 · 第一行 "));
        assert!(markdown.contains(r"\*\*bold\*\* \[name\]\(target\)"));
        assert_eq!(markdown.lines().count(), 4);
        assert_eq!(markdown.matches("  \n").count(), 3);
    }

    #[tokio::test]
    async fn qq_session_menu_commands_share_candidates_without_deleting_or_touching_sessions() {
        let fixture = Fixture::new_ahp();
        let database = fixture.service.database();
        let sessions: Vec<_> = (1..=12)
            .map(|number| test_session_menu_candidate(number, &fixture.workspace).session)
            .collect();
        database
            .ahp_replace_catalog(
                "adapter-stable",
                "adapter-run-1",
                &[test_ahp_host()],
                &sessions,
            )
            .expect("catalogue");
        let expected = fixture.service.ahp_session_menu().expect("menu");
        assert_eq!(expected.entries.len(), 10);
        assert!(
            !expected
                .entries
                .iter()
                .any(|entry| entry.session.session_uri == "copilot:/session-2")
        );
        let usage_before: HashMap<_, _> = database
            .ahp_session_menu_candidates()
            .expect("usage")
            .into_iter()
            .map(|candidate| (candidate.session.session_uri, candidate.last_used_at))
            .collect();
        let bindings_before = serde_json::to_value(database.ahp_bindings().expect("bindings"))
            .expect("bindings JSON");
        let listing = fixture
            .service
            .process_ahp_owner_message("menu-list", "/sessions")
            .await
            .expect("listing")
            .expect("text");
        assert!(listing.contains("显示 10/12 个"));
        assert!(listing.contains("收起 2 个"));
        assert!(
            fixture
                .service
                .process_ahp_owner_message("menu-switch", "/switch")
                .await
                .expect("switch menu")
                .is_none()
        );
        let menus = fixture.qq.choice_menus().await;
        assert_eq!(menus.len(), 1);
        assert_eq!(menus[0].choices.len(), 10);
        for (entry, button) in expected.entries.iter().zip(&menus[0].choices) {
            let code = entry.session.short_code.as_deref().expect("code");
            assert!(listing.contains(&format!("[{code}]")));
            assert!(menus[0].markdown.contains(&format!("**{code} · ")));
            assert!(button.label.starts_with(&format!("{code} ")));
            assert!(button.label.chars().count() <= 10);
        }
        assert_eq!(
            database.ahp_list_sessions().expect("full catalogue").len(),
            12
        );
        assert_eq!(
            serde_json::to_value(database.ahp_bindings().expect("bindings"))
                .expect("bindings JSON"),
            bindings_before
        );
        let usage_after: HashMap<_, _> = database
            .ahp_session_menu_candidates()
            .expect("usage")
            .into_iter()
            .map(|candidate| (candidate.session.session_uri, candidate.last_used_at))
            .collect();
        assert_eq!(usage_before, usage_after);

        let hidden = database
            .ahp_session_by_uri("copilot:/session-2")
            .expect("session")
            .expect("retained");
        let code = hidden.short_code.expect("stable code");
        fixture
            .service
            .process_ahp_owner_message("menu-use-hidden", &format!("/switch {code}"))
            .await
            .expect("switch by hidden code");
        let recent = fixture.service.ahp_session_menu().expect("updated LRU");
        assert_eq!(recent.entries[0].session.session_uri, "copilot:/session-2");
        assert!(recent.entries[0].foreground);
        let mut updated = sessions;
        updated[2].modified_at = (Utc::now() + chrono::Duration::minutes(1)).to_rfc3339();
        database
            .ahp_replace_catalog(
                "adapter-stable",
                "adapter-run-1",
                &[test_ahp_host()],
                &updated,
            )
            .expect("PC activity");
        assert!(
            fixture
                .service
                .ahp_session_menu()
                .expect("PC LRU")
                .entries
                .iter()
                .any(|entry| entry.session.session_uri == "copilot:/session-3")
        );
    }

    fn test_ssh_target(path: &str) -> AhpAuthorizedTarget {
        AhpAuthorizedTarget::Ssh {
            alias: "dev".to_owned(),
            path: path.to_owned(),
            user: "developer".to_owned(),
            host: "dev.example.test".to_owned(),
            port: 22,
            host_key_fingerprints: vec!["SHA256:test".to_owned()],
        }
    }

    fn add_test_ssh_authorization(fixture: &Fixture, path: &str) {
        let mut config = (*fixture.service.config()).clone();
        config.ahp.authorized_targets.push(test_ssh_target(path));
        config
            .save(&fixture.service.config_path)
            .expect("save SSH authorization");
        assert!(
            fixture
                .service
                .refresh_authorization()
                .expect("refresh SSH authorization")
                .last_error
                .is_none()
        );
    }

    async fn restore_test_authorization(fixture: &Fixture, content: &[u8], revision: &str) {
        fs::write(&fixture.service.config_path, content).expect("restore exact accepted file");
        let restored = fixture
            .service
            .dispatch(BridgeRequest::AhpReloadTargets)
            .await
            .expect("restore accepted authorization");
        assert_eq!(restored["revision"], revision);
        assert!(restored.get("last_error").is_none());
        assert_eq!(restored["restart_required"], false);
    }

    #[tokio::test]
    async fn authorization_append_updates_ssh_routing_without_resetting_active_bindings() {
        let fixture = Fixture::new_ahp_with_ssh();
        add_test_ssh_authorization(&fixture, "/repo-one");
        let database = fixture.service.database();
        let original_binding = database
            .ahp_binding()
            .expect("binding")
            .expect("foreground");
        database
            .ahp_publish_events(
                "adapter-stable",
                "adapter-run-1",
                &original_binding.binding_id,
                original_binding.generation,
                &[AhpPublishedEvent {
                    event_id: sha256_hex(b"authorization-active-turn"),
                    host_instance_id: "host-1".to_owned(),
                    server_sequence: Some(2),
                    session_uri: original_binding.session_uri.clone(),
                    chat_uri: original_binding.chat_uri.clone(),
                    turn_id: Some("authorization-turn".to_owned()),
                    kind: AhpEventKind::TurnStarted,
                    origin_client_id: None,
                    occurred_at: "2026-09-16T00:00:00Z".to_owned(),
                    data: json!({}),
                }],
            )
            .expect("start existing Turn");
        database
            .ahp_begin_input(&NewAhpInput {
                input_key: "authorization-pending-input".to_owned(),
                session_uri: original_binding.session_uri.clone(),
                chat_uri: original_binding.chat_uri.clone().expect("chat URI"),
                request_id: "authorization-question".to_owned(),
                prompt: "Continue?".to_owned(),
                choices: Vec::new(),
                allow_freeform: true,
                selection_mode: "none".to_owned(),
                expires_at: Utc::now().timestamp() + 600,
            })
            .expect("pending input");

        let mut remote = test_ahp_session(2, &fixture.workspace, 1);
        remote.endpoint_id = "remote-endpoint".to_owned();
        remote.host_instance_id = "remote-host".to_owned();
        remote.title = "New remote directory".to_owned();
        remote.workspace_uris = vec!["vscode-remote://ssh-remote+dev/repo-two".to_owned()];
        remote.ssh_alias = Some("dev".to_owned());
        remote.target_kind = Some(AhpTargetKind::Ssh);
        remote.target_path = Some("/repo-two".to_owned());
        let mut remote_host = test_ahp_host();
        remote_host.endpoint_id = remote.endpoint_id.clone();
        remote_host.host_instance_id = remote.host_instance_id.clone();
        remote_host.ssh_alias = remote.ssh_alias.clone();
        remote_host.target_kind = remote.target_kind;
        remote_host.target_path = remote.target_path.clone();
        database
            .ahp_replace_catalog(
                "adapter-stable",
                "adapter-run-1",
                &[test_ahp_host(), remote_host],
                &[test_ahp_session(1, &fixture.workspace, 1), remote.clone()],
            )
            .expect("catalogue with unauthorized remote directory");
        let pending_command = database
            .ahp_enqueue_cancel("pending-before-auth-reload")
            .expect("queue existing Turn cancellation");
        let before = serde_json::to_value(database.ahp_binding().expect("binding"))
            .expect("serialize binding");
        let listing = fixture
            .service
            .process_ahp_owner_message("before-auth-append", "/sessions")
            .await
            .expect("list Sessions")
            .expect("listing");
        assert!(!listing.contains(&remote.title));
        assert!(
            fixture
                .service
                .managed_target_for_session(&remote)
                .is_none()
        );
        let old_revision = fixture
            .service
            .authorization_status()
            .expect("authorization status")
            .revision;
        let mut config = (*fixture.service.config()).clone();
        config
            .ahp
            .authorized_targets
            .push(test_ssh_target("/repo-two"));
        config
            .save(&fixture.service.config_path)
            .expect("append SSH directory");

        let poll: AhpCommandPollResult = serde_json::from_value(
            fixture
                .service
                .dispatch(BridgeRequest::AhpPollCommands {
                    adapter_id: "adapter-stable".to_owned(),
                    adapter_instance_id: "adapter-run-1".to_owned(),
                    timeout_seconds: 1,
                    applied_authorization_revision: Some(old_revision.clone()),
                })
                .await
                .expect("poll refreshes authorization"),
        )
        .expect("poll result");
        assert_eq!(poll.commands.len(), 1);
        assert_eq!(poll.commands[0].command_id, pending_command);
        assert_eq!(poll.commands[0].binding_id, original_binding.binding_id);
        assert_ne!(poll.authorization.revision, old_revision);
        assert!(
            poll.authorization
                .authorized_targets
                .contains(&test_ssh_target("/repo-two"))
        );
        assert_eq!(
            serde_json::to_value(database.ahp_binding().expect("binding")).expect("binding JSON"),
            before
        );
        assert_eq!(database.ahp_status(60).expect("status").pending_inputs, 1);
        let status = fixture
            .service
            .dispatch(BridgeRequest::Status)
            .await
            .expect("status");
        assert_eq!(
            status["ahp"]["authorization"]["revision"],
            poll.authorization.revision
        );
        assert_eq!(
            status["ahp"]["authorization"]["adapter_revision"],
            old_revision
        );
        assert!(
            fixture
                .service
                .process_ahp_owner_message("after-auth-append", "/sessions")
                .await
                .expect("list Sessions")
                .expect("listing")
                .contains(&remote.title)
        );
        assert!(matches!(
            fixture.service.managed_target_for_session(&remote),
            Some(AhpManagedTarget::Ssh { path, .. }) if path == "/repo-two"
        ));
        let remote = database
            .ahp_session_by_uri(&remote.session_uri)
            .expect("remote Session")
            .expect("catalogued Session");
        let code = remote.short_code.as_deref().expect("short code");
        let reply = fixture
            .service
            .process_ahp_owner_message("send-after-auth-append", &format!("/send {code} continue"))
            .await
            .expect("send to newly authorized Session")
            .expect("send reply");
        assert!(reply.contains("前台 Session 未改变"));
        assert_eq!(
            serde_json::to_value(database.ahp_binding().expect("binding")).expect("binding JSON"),
            before
        );
        fixture
            .service
            .process_ahp_owner_message("switch-after-auth-append", &format!("/switch {code}"))
            .await
            .expect("switch to newly authorized Session");
        assert_eq!(
            database
                .ahp_binding()
                .expect("binding")
                .expect("foreground")
                .session_uri,
            remote.session_uri
        );
    }

    #[tokio::test]
    async fn authorization_poll_detects_additions_while_waiting_without_commands() {
        let fixture = Fixture::new_ahp_with_ssh();
        add_test_ssh_authorization(&fixture, "/repo-one");
        let previous_file = fs::read(&fixture.service.config_path).expect("previous file");
        let revision = fixture
            .service
            .authorization_status()
            .expect("authorization status")
            .revision;
        let service = fixture.service.clone();
        let applied_revision = revision.clone();
        let poll = tokio::spawn(async move {
            service
                .dispatch(BridgeRequest::AhpPollCommands {
                    adapter_id: "adapter-stable".to_owned(),
                    adapter_instance_id: "adapter-run-1".to_owned(),
                    timeout_seconds: 5,
                    applied_authorization_revision: Some(applied_revision),
                })
                .await
        });
        let deadline = Instant::now() + Duration::from_secs(2);
        while fixture
            .service
            .authorization_status()
            .expect("authorization status")
            .adapter_revision
            .is_none()
        {
            assert!(Instant::now() < deadline, "poll did not start");
            sleep(Duration::from_millis(10)).await;
        }
        let mut config = (*fixture.service.config()).clone();
        config
            .ahp
            .authorized_targets
            .push(test_ssh_target("/repo-two"));
        config
            .save(&fixture.service.config_path)
            .expect("append while polling");
        let latest_file = fs::read(&fixture.service.config_path).expect("latest file");
        let explicit_reload = fixture
            .service
            .dispatch(BridgeRequest::AhpReloadTargets)
            .await
            .expect("explicit reload shares the polling state");
        let result = tokio::time::timeout(Duration::from_secs(2), poll)
            .await
            .expect("authorization change wakes the poll")
            .expect("poll task")
            .expect("poll result");
        assert!(result["commands"].as_array().expect("commands").is_empty());
        assert_ne!(result["authorization"]["revision"], revision);
        assert_eq!(
            result["authorization"]["revision"],
            explicit_reload["revision"]
        );
        let latest_revision = explicit_reload["revision"]
            .as_str()
            .expect("latest revision");
        fs::write(&fixture.service.config_path, previous_file).expect("restore older file");
        assert!(
            fixture
                .service
                .dispatch(BridgeRequest::AhpReloadTargets)
                .await
                .is_err()
        );
        assert_eq!(
            fixture
                .service
                .authorization_status()
                .expect("status")
                .revision,
            latest_revision
        );
        restore_test_authorization(&fixture, &latest_file, latest_revision).await;
    }

    #[tokio::test]
    async fn authorization_rejections_preserve_last_good_state_and_report_safe_errors() {
        let fixture = Fixture::new_ahp_with_ssh();
        add_test_ssh_authorization(&fixture, "/repo-one");
        let accepted_file = fs::read(&fixture.service.config_path).expect("accepted file");
        let config = fixture.service.config();
        let revision = config.authorization_revision().expect("revision");
        let binding_id = fixture.ahp_binding_id();
        fs::write(
            &fixture.service.config_path,
            "ipc_token = \"sensitive-test-content",
        )
        .expect("write malformed configuration");
        let error = fixture
            .service
            .dispatch(BridgeRequest::AhpReloadTargets)
            .await
            .expect_err("malformed config rejected");
        assert!(!error.to_string().contains("sensitive-test-content"));
        assert!(Arc::ptr_eq(&config, &fixture.service.config()));
        let malformed = fixture.service.authorization_status().expect("status");
        assert!(malformed.last_error.is_some());
        assert!(!malformed.restart_required);
        fixture
            .service
            .refresh_authorization()
            .expect("cached rejection");
        assert!(Arc::ptr_eq(&config, &fixture.service.config()));
        fixture
            .service
            .enqueue_foreground_message("message-despite-bad-config", "continue")
            .expect("last good config remains usable");
        let poll = fixture
            .service
            .dispatch(BridgeRequest::AhpPollCommands {
                adapter_id: "adapter-stable".to_owned(),
                adapter_instance_id: "adapter-run-1".to_owned(),
                timeout_seconds: 1,
                applied_authorization_revision: Some(revision.clone()),
            })
            .await
            .expect("bad disk config does not poison polling");
        assert_eq!(poll["authorization"]["revision"], revision);
        assert_eq!(poll["commands"][0]["kind"], "send_message");
        restore_test_authorization(&fixture, &accepted_file, &revision).await;

        for change in ["remove", "identity", "fingerprint", "fixed-setting"] {
            let mut candidate = (*config).clone();
            if change == "remove" {
                candidate
                    .ahp
                    .authorized_targets
                    .retain(|target| !matches!(target, AhpAuthorizedTarget::Ssh { .. }));
            } else if change == "fixed-setting" {
                candidate.bridge.message_chunk_chars += 1;
            } else {
                let target = candidate
                    .ahp
                    .authorized_targets
                    .iter_mut()
                    .find(|target| matches!(target, AhpAuthorizedTarget::Ssh { .. }))
                    .expect("SSH target");
                if let AhpAuthorizedTarget::Ssh {
                    host,
                    host_key_fingerprints,
                    ..
                } = target
                {
                    if change == "identity" {
                        *host = "different.example.test".to_owned();
                    } else {
                        *host_key_fingerprints = vec!["SHA256:changed".to_owned()];
                    }
                }
            }
            candidate
                .save(&fixture.service.config_path)
                .expect("save rejected candidate");
            assert!(
                fixture
                    .service
                    .dispatch(BridgeRequest::AhpReloadTargets)
                    .await
                    .is_err(),
                "{change} must require restart"
            );
            let status = fixture
                .service
                .authorization_status()
                .expect("reload status");
            assert!(status.restart_required, "{change}");
            assert!(status.last_error.is_some());
            assert_eq!(status.revision, revision);
            assert_eq!(fixture.ahp_binding_id(), binding_id);
            assert!(Arc::ptr_eq(&config, &fixture.service.config()));
            restore_test_authorization(&fixture, &accepted_file, &revision).await;
        }
        let accepted = fixture.service.config();
        fixture
            .service
            .refresh_authorization()
            .expect("unchanged file");
        assert!(Arc::ptr_eq(&accepted, &fixture.service.config()));
    }

    #[tokio::test]
    async fn authorization_append_updates_local_hooks_and_shared_task_snapshots() {
        let fixture = Fixture::new(true);
        let workspace = fixture._directory.path().join("added-workspace");
        fs::create_dir_all(&workspace).expect("added workspace");
        let workspace = fs::canonicalize(workspace).expect("canonical workspace");
        let file = workspace.join("example.txt");
        fs::write(&file, "example").expect("test file");
        let task = fixture.service.task_clone();
        assert!(fixture.service.ensure_workspace(&workspace).is_err());
        assert!(task.ensure_workspace(&workspace).is_err());
        let mut config = (*fixture.service.config()).clone();
        config.bridge.workspace_roots.push(workspace.clone());
        config
            .save(&fixture.service.config_path)
            .expect("append local root");
        fixture
            .service
            .dispatch(BridgeRequest::AhpReloadTargets)
            .await
            .expect("reload local authorization");
        fixture
            .service
            .ensure_workspace(&workspace)
            .expect("new live root");
        task.ensure_workspace(&workspace)
            .expect("existing task sees new root");
        fixture
            .service
            .register_prompt("added-local-session", &workspace)
            .await
            .expect("register local Session in new root");
        let result = fixture
            .service
            .pre_tool(
                "added-local-session",
                "read-added-root",
                "view",
                &json!({"path": file}),
                &workspace,
            )
            .await
            .expect("classify read in added root");
        assert_eq!(
            result.decision,
            PermissionDecision::Allow,
            "{}",
            result.reason
        );
    }

    #[tokio::test]
    async fn authorization_reload_checks_adapter_freshness_even_with_a_matching_ack() {
        let fixture = Fixture::new_ahp();
        let revision = fixture
            .service
            .authorization_status()
            .expect("authorization status")
            .revision;
        fixture
            .service
            .poll_ahp_commands("adapter-stable", "adapter-run-1", Some(&revision))
            .expect("acknowledge current revision");
        let connected = fixture
            .service
            .dispatch(BridgeRequest::AhpReloadTargets)
            .await
            .expect("connected authorization status");
        assert_eq!(connected["adapter_connected"], true);
        assert_eq!(connected["revision"], connected["adapter_revision"]);
        assert_eq!(connected["target_count"], 1);
        let config = fixture.service.config();
        let mut content = fs::read_to_string(&fixture.service.config_path).expect("read config");
        content.push('\n');
        fs::write(&fixture.service.config_path, content)
            .expect("change file without changing authorization");
        fixture
            .service
            .dispatch(BridgeRequest::AhpReloadTargets)
            .await
            .expect("same revision is not reapplied");
        assert!(Arc::ptr_eq(&config, &fixture.service.config()));
        fixture
            .service
            .database()
            .connection()
            .expect("database")
            .execute("UPDATE ahp_adapter SET last_seen_at = 0", [])
            .expect("expire adapter heartbeat");
        let stale = fixture
            .service
            .dispatch(BridgeRequest::AhpReloadTargets)
            .await
            .expect("cached file still checks adapter freshness");
        assert_eq!(stale["revision"], stale["adapter_revision"]);
        assert_eq!(stale["adapter_connected"], false);
        fixture
            .service
            .database()
            .ahp_register_adapter(&AhpAdapterRegistration {
                adapter_id: "adapter-stable".to_owned(),
                adapter_instance_id: "different-adapter-instance".to_owned(),
                version: "0.1.0".to_owned(),
                supported_protocols: vec!["1.0.0".to_owned()],
            })
            .expect("different active instance cannot inherit the previous acknowledgement");
        let different_instance = fixture
            .service
            .dispatch(BridgeRequest::AhpReloadTargets)
            .await
            .expect("validate acknowledgement owner");
        assert_eq!(different_instance["adapter_connected"], false);
    }

    #[tokio::test]
    async fn authorization_acknowledgements_require_the_current_adapter_instance() {
        let fixture = Fixture::new_ahp();
        let registration = AhpAdapterRegistration {
            adapter_id: "adapter-stable".to_owned(),
            adapter_instance_id: "adapter-run-1".to_owned(),
            version: "0.1.0".to_owned(),
            supported_protocols: vec!["1.0.0".to_owned()],
        };
        let registered = fixture
            .service
            .dispatch(BridgeRequest::AhpAdapterRegister {
                registration: registration.clone(),
            })
            .await
            .expect("register adapter with authorization");
        let revision = registered["authorization"]["revision"]
            .as_str()
            .expect("revision")
            .to_owned();
        assert!(registered["authorization"]["authorized_targets"].is_array());
        assert!(
            fixture
                .service
                .authorization_status()
                .expect("status")
                .adapter_revision
                .is_none()
        );
        fixture
            .service
            .poll_ahp_commands("adapter-stable", "adapter-run-1", Some(&revision))
            .expect("record valid acknowledgement");
        assert_eq!(
            fixture
                .service
                .authorization_status()
                .expect("status")
                .adapter_revision
                .as_deref(),
            Some(revision.as_str())
        );
        fixture
            .service
            .poll_ahp_commands("adapter-stable", "adapter-run-1", Some(&"0".repeat(64)))
            .expect("late old poll must not regress a current acknowledgement");
        assert_eq!(
            fixture
                .service
                .authorization_status()
                .expect("status")
                .adapter_revision
                .as_deref(),
            Some(revision.as_str())
        );
        let mut replacement = registration;
        replacement.adapter_instance_id = "adapter-run-2".to_owned();
        fixture
            .service
            .dispatch(BridgeRequest::AhpAdapterRegister {
                registration: replacement,
            })
            .await
            .expect("replacement registration clears acknowledgement");
        assert!(
            fixture
                .service
                .authorization_status()
                .expect("status")
                .adapter_revision
                .is_none()
        );
        assert!(
            fixture
                .service
                .dispatch(BridgeRequest::AhpPollCommands {
                    adapter_id: "adapter-stable".to_owned(),
                    adapter_instance_id: "adapter-run-1".to_owned(),
                    timeout_seconds: 1,
                    applied_authorization_revision: Some(revision),
                })
                .await
                .is_err()
        );
        assert!(
            fixture
                .service
                .authorization_status()
                .expect("status")
                .adapter_revision
                .is_none()
        );
    }

    #[test]
    fn split_message_preserves_unicode_and_limits_chunks() {
        let content = "你好世界\nabcdef";
        let chunks = split_message(content, 4);
        assert_eq!(chunks.concat(), content);
        assert!(chunks.iter().all(|chunk| chunk.chars().count() <= 4));
    }

    #[test]
    fn qq_truncation_includes_ellipsis_in_the_limit() {
        assert_eq!(truncate_for_qq("abcdef", 6), "abcdef");
        assert_eq!(truncate_for_qq("abcdef", 5), "ab...");
        assert_eq!(truncate_for_qq("abcdef", 3), "...");
        assert_eq!(truncate_for_qq("abcdef", 2), "..");
        assert_eq!(truncate_for_qq("abcdef", 0), "");
        assert!(truncate_for_qq("你好世界", 3).chars().count() <= 3);
    }

    #[test]
    fn pending_replay_delivery_is_terminal_for_at_most_once_delivery() {
        assert!(offline_replay_delivery_is_terminal("pending"));
        assert!(offline_replay_delivery_is_terminal("sent"));
        assert!(offline_replay_delivery_is_terminal("in_doubt"));
        assert!(!offline_replay_delivery_is_terminal("failed"));
    }

    #[test]
    fn tool_notification_modes_filter_states() {
        assert!(!should_notify_tool(
            AhpToolNotificationMode::Compact,
            "streaming"
        ));
        assert!(!should_notify_tool(
            AhpToolNotificationMode::Compact,
            "running"
        ));
        assert!(should_notify_tool(
            AhpToolNotificationMode::Compact,
            "completed"
        ));
        assert!(should_notify_tool(
            AhpToolNotificationMode::Compact,
            "cancelled"
        ));
        assert!(should_notify_tool(AhpToolNotificationMode::Full, "running"));
        for status in ["streaming", "running", "completed", "cancelled"] {
            assert!(!should_notify_tool(
                AhpToolNotificationMode::ApprovalOnly,
                status
            ));
        }
    }

    #[tokio::test]
    async fn approval_only_mode_sends_approvals_and_final_reply_without_tool_status() {
        let fixture =
            Fixture::new_ahp_with_notification_mode(AhpToolNotificationMode::ApprovalOnly);
        let events = vec![
            AhpPublishedEvent {
                event_id: sha256_hex(b"approval-only-tool"),
                host_instance_id: "host-1".to_owned(),
                server_sequence: Some(2),
                session_uri: "copilot:/session-1".to_owned(),
                chat_uri: Some("ahp-chat://default/session-1".to_owned()),
                turn_id: Some("turn-1".to_owned()),
                kind: AhpEventKind::ToolStatus,
                origin_client_id: None,
                occurred_at: "2026-08-27T00:00:01Z".to_owned(),
                data: json!({
                    "tool_call_id": "tool-without-approval",
                    "tool_name": "View",
                    "status": "completed",
                    "summary": "Read README without approval"
                }),
            },
            AhpPublishedEvent {
                event_id: sha256_hex(b"approval-only-approval"),
                host_instance_id: "host-1".to_owned(),
                server_sequence: Some(3),
                session_uri: "copilot:/session-1".to_owned(),
                chat_uri: Some("ahp-chat://default/session-1".to_owned()),
                turn_id: Some("turn-1".to_owned()),
                kind: AhpEventKind::ApprovalPending,
                origin_client_id: None,
                occurred_at: "2026-08-27T00:00:02Z".to_owned(),
                data: json!({
                    "approval_key": "approval-only-request",
                    "stage": "parameter",
                    "tool_call_id": "tool-requiring-approval",
                    "tool_name": "Terminal",
                    "summary": "Run deployment"
                }),
            },
            AhpPublishedEvent {
                event_id: sha256_hex(b"approval-only-final"),
                host_instance_id: "host-1".to_owned(),
                server_sequence: Some(4),
                session_uri: "copilot:/session-1".to_owned(),
                chat_uri: Some("ahp-chat://default/session-1".to_owned()),
                turn_id: Some("turn-1".to_owned()),
                kind: AhpEventKind::AssistantMessage,
                origin_client_id: None,
                occurred_at: "2026-08-27T00:00:03Z".to_owned(),
                data: json!({
                    "message_id": "approval-only-assistant",
                    "content": "Deployment complete",
                    "complete": true,
                    "historical": false
                }),
            },
        ];

        fixture
            .service
            .dispatch(BridgeRequest::AhpPublishEvents {
                adapter_id: "adapter-stable".to_owned(),
                adapter_instance_id: "adapter-run-1".to_owned(),
                binding_id: fixture.ahp_binding_id(),
                binding_generation: 1,
                events,
            })
            .await
            .expect("publish AHP events");

        let messages = fixture.qq.messages().await;
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].kind, "approval_buttons");
        assert!(messages[0].content.contains("Run deployment"));
        assert!(messages[0].content.contains("Shared"));
        assert_eq!(messages[1].kind, "text");
        assert!(messages[1].content.contains("Deployment complete"));
        assert!(messages[1].content.contains("Shared"));
        assert!(
            messages
                .iter()
                .all(|message| !message.content.contains("Read README without approval"))
        );
    }

    #[tokio::test]
    async fn assistant_progress_is_delivered_without_stopping_typing_or_adding_focus_button() {
        let fixture = Fixture::new_ahp();
        let base_event = AhpPublishedEvent {
            event_id: sha256_hex(b"assistant-progress-turn"),
            host_instance_id: "host-1".to_owned(),
            server_sequence: Some(2),
            session_uri: "copilot:/session-1".to_owned(),
            chat_uri: Some("ahp-chat://default/session-1".to_owned()),
            turn_id: Some("turn-progress".to_owned()),
            kind: AhpEventKind::TurnStarted,
            origin_client_id: None,
            occurred_at: "2026-08-27T00:00:01Z".to_owned(),
            data: json!({}),
        };
        fixture
            .service
            .dispatch(BridgeRequest::AhpPublishEvents {
                adapter_id: "adapter-stable".to_owned(),
                adapter_instance_id: "adapter-run-1".to_owned(),
                binding_id: fixture.ahp_binding_id(),
                binding_generation: 1,
                events: vec![
                    base_event.clone(),
                    AhpPublishedEvent {
                        event_id: sha256_hex(b"assistant-progress-part"),
                        server_sequence: Some(3),
                        kind: AhpEventKind::AssistantMessage,
                        data: json!({
                            "message_id": "turn:turn-progress:assistant:progress",
                            "content": "Inspecting the implementation.",
                            "complete": true,
                            "historical": false,
                            "final_response": false
                        }),
                        ..base_event.clone()
                    },
                ],
            })
            .await
            .expect("deliver progress response part");
        assert_eq!(fixture.service.typing_tasks.lock().await.len(), 1);
        let progress_messages = fixture.qq.messages().await;
        assert!(progress_messages.iter().any(|message| {
            message.kind == "text" && message.content.contains("Inspecting the implementation.")
        }));
        assert!(
            progress_messages
                .iter()
                .all(|message| message.kind != "choice_buttons")
        );

        fixture
            .service
            .dispatch(BridgeRequest::AhpPublishEvents {
                adapter_id: "adapter-stable".to_owned(),
                adapter_instance_id: "adapter-run-1".to_owned(),
                binding_id: fixture.ahp_binding_id(),
                binding_generation: 1,
                events: vec![AhpPublishedEvent {
                    event_id: sha256_hex(b"assistant-final-part"),
                    server_sequence: Some(4),
                    kind: AhpEventKind::AssistantMessage,
                    data: json!({
                        "message_id": "turn:turn-progress:assistant:final",
                        "content": "Implementation complete.",
                        "complete": true,
                        "historical": false,
                        "final_response": true
                    }),
                    ..base_event
                }],
            })
            .await
            .expect("deliver final response part");
        assert!(fixture.service.typing_tasks.lock().await.is_empty());
        let messages = fixture.qq.messages().await;
        assert_eq!(
            messages
                .iter()
                .filter(|message| {
                    message.kind == "text"
                        && message.content.contains("Inspecting the implementation.")
                })
                .count(),
            1
        );
        assert_eq!(
            messages
                .iter()
                .filter(|message| {
                    message.kind == "text" && message.content.contains("Implementation complete.")
                })
                .count(),
            1
        );
        assert_eq!(
            messages
                .iter()
                .filter(|message| message.kind == "choice_buttons")
                .count(),
            0
        );
    }

    #[tokio::test]
    async fn background_session_final_reply_adds_focus_button() {
        let fixture = Fixture::new_ahp();
        fixture
            .service
            .database()
            .ahp_replace_catalog(
                "adapter-stable",
                "adapter-run-1",
                &[test_ahp_host()],
                &[
                    test_ahp_session(1, &fixture.workspace, 1),
                    test_ahp_session(2, &fixture.workspace, 1),
                ],
            )
            .expect("catalogue");
        let background = fixture
            .service
            .database()
            .ahp_track_session("endpoint-1", "copilot:/session-2")
            .expect("track background session");
        let bind_command = fixture
            .service
            .database()
            .ahp_poll_commands("adapter-stable", "adapter-run-1", 60)
            .expect("bind command");
        fixture
            .service
            .database()
            .ahp_ack_command(
                "adapter-stable",
                "adapter-run-1",
                bind_command[0].command_id,
                crate::protocol::AhpCommandOutcome::Applied,
                None,
                None,
            )
            .expect("ack bind");
        fixture
            .service
            .database()
            .ahp_binding_ready(
                "adapter-stable",
                "adapter-run-1",
                &background.binding_id,
                "endpoint-1",
                "host-1",
                background.generation,
                "copilot:/session-2",
                "ahp-chat://default/session-2",
                1,
            )
            .expect("background ready");

        fixture
            .service
            .dispatch(BridgeRequest::AhpPublishEvents {
                adapter_id: "adapter-stable".to_owned(),
                adapter_instance_id: "adapter-run-1".to_owned(),
                binding_id: background.binding_id,
                binding_generation: background.generation,
                events: vec![AhpPublishedEvent {
                    event_id: sha256_hex(b"background-final"),
                    host_instance_id: "host-1".to_owned(),
                    server_sequence: Some(2),
                    session_uri: "copilot:/session-2".to_owned(),
                    chat_uri: Some("ahp-chat://default/session-2".to_owned()),
                    turn_id: Some("turn-background".to_owned()),
                    kind: AhpEventKind::AssistantMessage,
                    origin_client_id: None,
                    occurred_at: "2026-09-02T08:00:00Z".to_owned(),
                    data: json!({
                        "message_id": "turn:turn-background:assistant:final",
                        "content": "Background task complete.",
                        "complete": true,
                        "historical": false,
                        "final_response": true
                    }),
                }],
            })
            .await
            .expect("publish background final reply");

        let messages = fixture.qq.messages().await;
        assert_eq!(
            messages
                .iter()
                .filter(|message| {
                    message.kind == "text" && message.content.contains("Background task complete.")
                })
                .count(),
            1
        );
        assert_eq!(
            messages
                .iter()
                .filter(|message| {
                    message.kind == "choice_buttons"
                        && message.content.contains("任务仍在其原 Session")
                })
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn qq_input_resolution_confirmation_is_silent_with_or_without_action_origin() {
        let fixture = Fixture::new_ahp();
        let text_input = fixture
            .service
            .database()
            .ahp_begin_input(&NewAhpInput {
                input_key: "input-qq-text".to_owned(),
                session_uri: "copilot:/session-1".to_owned(),
                chat_uri: "ahp-chat://default/session-1".to_owned(),
                request_id: "request-qq-text".to_owned(),
                prompt: "Enter a value".to_owned(),
                choices: Vec::new(),
                allow_freeform: true,
                selection_mode: "none".to_owned(),
                expires_at: Utc::now().timestamp() + 600,
            })
            .expect("begin QQ text input");
        assert!(
            fixture
                .service
                .database()
                .ahp_submit_input(&text_input.record.input_key, "value", "qq-text-message")
                .expect("submit QQ text input")
        );
        fixture
            .service
            .dispatch(BridgeRequest::AhpPublishEvents {
                adapter_id: "adapter-stable".to_owned(),
                adapter_instance_id: "adapter-run-1".to_owned(),
                binding_id: fixture.ahp_binding_id(),
                binding_generation: 1,
                events: vec![AhpPublishedEvent {
                    event_id: sha256_hex(b"input-qq-text-resolved"),
                    host_instance_id: "host-1".to_owned(),
                    server_sequence: Some(5),
                    session_uri: "copilot:/session-1".to_owned(),
                    chat_uri: Some("ahp-chat://default/session-1".to_owned()),
                    turn_id: Some("turn-qq-text".to_owned()),
                    kind: AhpEventKind::InputResolved,
                    origin_client_id: None,
                    occurred_at: "2026-08-27T00:00:05Z".to_owned(),
                    data: json!({
                        "input_key": "input-qq-text",
                        "outcome": "answered",
                        "client_id": null
                    }),
                }],
            })
            .await
            .expect("confirm QQ text input without origin");

        let button_input = fixture
            .service
            .database()
            .ahp_begin_input(&NewAhpInput {
                input_key: "input-qq-button".to_owned(),
                session_uri: "copilot:/session-1".to_owned(),
                chat_uri: "ahp-chat://default/session-1".to_owned(),
                request_id: "request-qq-button".to_owned(),
                prompt: "Choose a value".to_owned(),
                choices: vec!["one".to_owned(), "two".to_owned()],
                allow_freeform: false,
                selection_mode: "single".to_owned(),
                expires_at: Utc::now().timestamp() + 600,
            })
            .expect("begin QQ button input");
        let first_button = button_input
            .record
            .buttons
            .first()
            .expect("input choice button");
        let button_submission = fixture
            .service
            .database()
            .ahp_submit_input_by_button(&first_button.button_data, "qq-button-interaction")
            .expect("submit QQ button input")
            .expect("button exists");
        assert!(button_submission.accepted);
        fixture
            .service
            .dispatch(BridgeRequest::AhpPublishEvents {
                adapter_id: "adapter-stable".to_owned(),
                adapter_instance_id: "adapter-run-1".to_owned(),
                binding_id: fixture.ahp_binding_id(),
                binding_generation: 1,
                events: vec![AhpPublishedEvent {
                    event_id: sha256_hex(b"input-qq-button-resolved"),
                    host_instance_id: "host-1".to_owned(),
                    server_sequence: Some(6),
                    session_uri: "copilot:/session-1".to_owned(),
                    chat_uri: Some("ahp-chat://default/session-1".to_owned()),
                    turn_id: Some("turn-qq-button".to_owned()),
                    kind: AhpEventKind::InputResolved,
                    origin_client_id: Some("adapter-stable".to_owned()),
                    occurred_at: "2026-08-27T00:00:06Z".to_owned(),
                    data: json!({
                        "input_key": "input-qq-button",
                        "outcome": "answered",
                        "client_id": "adapter-stable"
                    }),
                }],
            })
            .await
            .expect("confirm QQ button input");

        let messages = fixture.qq.messages().await;
        assert!(
            messages.iter().all(|message| message.kind != "text"),
            "QQ-originated input confirmations must not emit a resolution message"
        );
    }

    #[tokio::test]
    async fn pc_input_resolution_is_notified_once() {
        let fixture = Fixture::new_ahp();
        fixture
            .service
            .database()
            .ahp_begin_input(&NewAhpInput {
                input_key: "input-pc".to_owned(),
                session_uri: "copilot:/session-1".to_owned(),
                chat_uri: "ahp-chat://default/session-1".to_owned(),
                request_id: "request-pc".to_owned(),
                prompt: "Enter a value".to_owned(),
                choices: Vec::new(),
                allow_freeform: true,
                selection_mode: "none".to_owned(),
                expires_at: Utc::now().timestamp() + 600,
            })
            .expect("begin PC input");
        let resolved = AhpPublishedEvent {
            event_id: sha256_hex(b"input-pc-resolved"),
            host_instance_id: "host-1".to_owned(),
            server_sequence: Some(7),
            session_uri: "copilot:/session-1".to_owned(),
            chat_uri: Some("ahp-chat://default/session-1".to_owned()),
            turn_id: Some("turn-pc".to_owned()),
            kind: AhpEventKind::InputResolved,
            origin_client_id: Some("vscode".to_owned()),
            occurred_at: "2026-08-27T00:00:07Z".to_owned(),
            data: json!({
                "input_key": "input-pc",
                "outcome": "answered",
                "client_id": "vscode"
            }),
        };
        fixture
            .service
            .dispatch(BridgeRequest::AhpPublishEvents {
                adapter_id: "adapter-stable".to_owned(),
                adapter_instance_id: "adapter-run-1".to_owned(),
                binding_id: fixture.ahp_binding_id(),
                binding_generation: 1,
                events: vec![resolved.clone()],
            })
            .await
            .expect("resolve input from PC");
        fixture
            .service
            .dispatch(BridgeRequest::AhpPublishEvents {
                adapter_id: "adapter-stable".to_owned(),
                adapter_instance_id: "adapter-run-1".to_owned(),
                binding_id: fixture.ahp_binding_id(),
                binding_generation: 1,
                events: vec![AhpPublishedEvent {
                    event_id: sha256_hex(b"input-pc-resolved-replay"),
                    server_sequence: Some(8),
                    ..resolved
                }],
            })
            .await
            .expect("ignore repeated PC resolution");

        let messages = fixture.qq.messages().await;
        assert_eq!(
            messages
                .iter()
                .filter(|message| {
                    message.kind == "text" && message.content.contains("已在 PC 端处理")
                })
                .count(),
            1
        );
        assert!(
            messages
                .iter()
                .all(|message| !message.content.contains("已由另一端处理"))
        );
    }

    #[tokio::test]
    async fn notification_mode_commands_persist_and_apply_without_restart() {
        let fixture =
            Fixture::new_ahp_with_notification_mode(AhpToolNotificationMode::ApprovalOnly);

        let initial = fixture
            .service
            .process_ahp_owner_message("notify-status", "/notify")
            .await
            .expect("read notification mode")
            .expect("notification mode response");
        assert!(initial.contains("当前通知模式: approval_only"));

        let changed = fixture
            .service
            .process_ahp_owner_message("notify-full", "/notify full")
            .await
            .expect("change notification mode")
            .expect("notification mode change response");
        assert!(changed.contains("已切换为 full"));
        assert!(changed.contains("立即生效"));
        assert_eq!(
            AppConfig::load(&fixture.service.config_path)
                .expect("reload persisted config")
                .ahp
                .tool_notification_mode,
            AhpToolNotificationMode::Full
        );

        let help = fixture
            .service
            .process_ahp_owner_message("notify-help", "/help")
            .await
            .expect("read help")
            .expect("help response");
        assert!(help.contains("/notify approval_only"));
        assert!(help.contains("/notify compact"));
        assert!(help.contains("/notify full"));
        assert!(help.contains("当前模式: full"));

        let status = fixture
            .service
            .process_ahp_owner_message("notify-bridge-status", "/status")
            .await
            .expect("read status")
            .expect("status response");
        assert!(status.contains("通知模式: full"));

        fixture
            .service
            .dispatch(BridgeRequest::AhpPublishEvents {
                adapter_id: "adapter-stable".to_owned(),
                adapter_instance_id: "adapter-run-1".to_owned(),
                binding_id: fixture.ahp_binding_id(),
                binding_generation: 1,
                events: vec![AhpPublishedEvent {
                    event_id: sha256_hex(b"full-mode-running-tool"),
                    host_instance_id: "host-1".to_owned(),
                    server_sequence: Some(2),
                    session_uri: "copilot:/session-1".to_owned(),
                    chat_uri: Some("ahp-chat://default/session-1".to_owned()),
                    turn_id: Some("turn-1".to_owned()),
                    kind: AhpEventKind::ToolStatus,
                    origin_client_id: None,
                    occurred_at: "2026-09-02T00:00:00Z".to_owned(),
                    data: json!({
                        "tool_call_id": "tool-running",
                        "tool_name": "Terminal",
                        "status": "running",
                        "summary": "Run tests"
                    }),
                }],
            })
            .await
            .expect("publish full-mode tool event");
        assert!(
            fixture
                .qq
                .messages()
                .await
                .iter()
                .any(|message| message.content.contains("Run tests"))
        );

        let invalid = fixture
            .service
            .process_ahp_owner_message("notify-invalid", "/notify noisy")
            .await
            .expect("reject invalid notification mode")
            .expect("invalid mode response");
        assert!(invalid.contains("未知通知模式"));
        assert_eq!(
            fixture.service.current_tool_notification_mode().await,
            AhpToolNotificationMode::Full
        );

        let reverted = fixture
            .service
            .process_ahp_owner_message("notify-approval-only", "/notify approval_only")
            .await
            .expect("restore approval-only mode")
            .expect("approval-only response");
        assert!(reverted.contains("已切换为 approval_only"));
        assert_eq!(
            AppConfig::load(&fixture.service.config_path)
                .expect("reload restored config")
                .ahp
                .tool_notification_mode,
            AhpToolNotificationMode::ApprovalOnly
        );
    }

    #[tokio::test]
    async fn approval_round_trip_allows_exactly_the_pending_tool() {
        let fixture = Fixture::new(true);
        let session_label = fixture.register_session().await;
        let service = fixture.service.clone();
        let workspace = fixture.workspace.clone();
        let pending = tokio::spawn(async move {
            service
                .dispatch(BridgeRequest::PreTool {
                    session_id: "session-1".to_owned(),
                    tool_use_id: "tool-use-1".to_owned(),
                    tool_name: "apply_patch".to_owned(),
                    tool_input: json!({
                        "patch": "*** Begin Patch\n*** Add File: src/new.rs\n+fn main() {}\n*** End Patch"
                    }),
                    cwd: workspace,
                })
                .await
        });
        let approval_code = fixture.wait_for_pending_approval().await;
        fixture
            .service
            .handle_inbound_message(
                "owner-message-1",
                "owner-openid",
                &format!("/allow {approval_code}"),
            )
            .await
            .expect("owner approval");
        let result: PermissionResult = serde_json::from_value(
            pending
                .await
                .expect("task joined")
                .expect("pre-tool result"),
        )
        .expect("permission result");
        assert_eq!(result.decision, PermissionDecision::Allow);
        assert!(session_label.starts_with('S'));
    }

    #[tokio::test]
    async fn voice_transcript_is_queued_without_executing_control_command() {
        use crate::protocol::AhpCommandKind;

        let fixture = Fixture::new_ahp_with_voice_input(true);
        fixture
            .service
            .handle_inbound_voice_message("voice-message-1", "owner-openid", Some("/allow ABC123"))
            .await
            .expect("voice message");

        let commands = fixture
            .service
            .database()
            .ahp_poll_commands("adapter-stable", "adapter-run-1", 60)
            .expect("commands");
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].kind, AhpCommandKind::SendMessage);
        assert_eq!(commands[0].data["content"], "/allow ABC123");
        assert!(
            fixture
                .qq
                .messages()
                .await
                .last()
                .expect("voice reply")
                .content
                .contains("已识别并发送")
        );
    }

    #[tokio::test]
    async fn voice_transcript_answers_pending_freeform_question() {
        use crate::protocol::AhpCommandKind;

        let fixture = Fixture::new_ahp_with_voice_input(true);
        let input = fixture
            .service
            .database()
            .ahp_begin_input(&NewAhpInput {
                input_key: "voice-input-1".to_owned(),
                session_uri: "copilot:/session-1".to_owned(),
                chat_uri: "ahp-chat://default/session-1".to_owned(),
                request_id: "voice-request-1".to_owned(),
                prompt: "Choose or enter a response".to_owned(),
                choices: vec!["选项 A".to_owned(), "选项 B".to_owned()],
                allow_freeform: true,
                selection_mode: "single".to_owned(),
                expires_at: Utc::now().timestamp() + 600,
            })
            .expect("pending input");
        fixture
            .service
            .handle_inbound_voice_message("voice-answer-1", "owner-openid", Some("今天是星期几？"))
            .await
            .expect("voice answer");

        let commands = fixture
            .service
            .database()
            .ahp_poll_commands("adapter-stable", "adapter-run-1", 60)
            .expect("commands");
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].kind, AhpCommandKind::CompleteInput);
        assert_eq!(commands[0].data["answer"], "今天是星期几？");
        assert_eq!(
            fixture
                .service
                .database()
                .ahp_input_by_code(&input.record.short_code)
                .expect("input")
                .expect("stored input")
                .state,
            "submitted"
        );
        assert!(
            fixture
                .qq
                .messages()
                .await
                .last()
                .expect("voice reply")
                .content
                .contains("等待确认")
        );
    }

    #[tokio::test]
    async fn voice_without_builtin_asr_prompts_owner_without_queueing() {
        let fixture = Fixture::new_ahp_with_voice_input(true);
        fixture
            .service
            .handle_inbound_voice_message("voice-message-2", "owner-openid", None)
            .await
            .expect("voice message");

        assert!(
            fixture
                .qq
                .messages()
                .await
                .last()
                .expect("voice reply")
                .content
                .contains("未获取到 QQ 内置语音识别结果")
        );
        assert!(
            fixture
                .service
                .database()
                .ahp_poll_commands("adapter-stable", "adapter-run-1", 60)
                .expect("commands")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn voice_input_is_disabled_by_default() {
        let fixture = Fixture::new_ahp();
        fixture
            .service
            .handle_inbound_voice_message(
                "voice-message-3",
                "owner-openid",
                Some("检查当前项目状态"),
            )
            .await
            .expect("voice message");

        assert!(
            fixture
                .qq
                .messages()
                .await
                .last()
                .expect("voice reply")
                .content
                .contains("语音输入尚未启用")
        );
        assert!(
            fixture
                .service
                .database()
                .ahp_poll_commands("adapter-stable", "adapter-run-1", 60)
                .expect("commands")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn non_owner_voice_input_is_ignored() {
        let fixture = Fixture::new_ahp_with_voice_input(true);
        fixture
            .service
            .handle_inbound_voice_message(
                "voice-message-4",
                "attacker-openid",
                Some("检查当前项目状态"),
            )
            .await
            .expect("voice message");

        assert!(fixture.qq.messages().await.is_empty());
        assert!(
            fixture
                .service
                .database()
                .ahp_poll_commands("adapter-stable", "adapter-run-1", 60)
                .expect("commands")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn approval_delivery_without_owner_fails_closed() {
        let fixture = Fixture::new(false);
        fixture.register_session().await;
        let result = fixture
            .service
            .dispatch(BridgeRequest::PreTool {
                session_id: "session-1".to_owned(),
                tool_use_id: "tool-use-1".to_owned(),
                tool_name: "apply_patch".to_owned(),
                tool_input: json!({
                    "patch": "*** Begin Patch\n*** Add File: src/new.rs\n+fn main() {}\n*** End Patch"
                }),
                cwd: fixture.workspace.clone(),
            })
            .await
            .expect("pre-tool result");
        let result: PermissionResult = serde_json::from_value(result).expect("permission result");
        assert_eq!(result.decision, PermissionDecision::Deny);
    }

    #[tokio::test]
    async fn non_owner_cannot_decide_pending_approval() {
        let fixture = Fixture::new(true);
        fixture.register_session().await;
        let service = fixture.service.clone();
        let workspace = fixture.workspace.clone();
        let pending = tokio::spawn(async move {
            service
                .dispatch(BridgeRequest::PreTool {
                    session_id: "session-1".to_owned(),
                    tool_use_id: "tool-use-1".to_owned(),
                    tool_name: "apply_patch".to_owned(),
                    tool_input: json!({
                        "patch": "*** Begin Patch\n*** Add File: src/new.rs\n+fn main() {}\n*** End Patch"
                    }),
                    cwd: workspace,
                })
                .await
        });
        let approval_code = fixture.wait_for_pending_approval().await;
        let before = fixture.qq.messages().await.len();
        fixture
            .service
            .handle_inbound_message(
                "attacker-message",
                "attacker-openid",
                &format!("/allow {approval_code}"),
            )
            .await
            .expect("ignored attacker message");
        assert_eq!(fixture.qq.messages().await.len(), before);
        fixture
            .service
            .handle_inbound_message(
                "owner-message",
                "owner-openid",
                &format!("/deny {approval_code}"),
            )
            .await
            .expect("owner denial");
        let result: PermissionResult = serde_json::from_value(
            pending
                .await
                .expect("task joined")
                .expect("pre-tool result"),
        )
        .expect("permission result");
        assert_eq!(result.decision, PermissionDecision::Deny);
    }

    #[tokio::test]
    async fn final_delivery_redacts_secret_patterns() {
        let fixture = Fixture::new(true);
        fixture.register_session().await;
        fixture
            .service
            .dispatch(BridgeRequest::SendFinal {
                session_id: Some("session-1".to_owned()),
                session_label: None,
                content: "Result: Authorization: Bearer abcdefghijklmnopqrstuvwxyz".to_owned(),
                idempotency_key: "session-1:turn-1:final".to_owned(),
            })
            .await
            .expect("final delivery");
        let messages = fixture.qq.messages().await;
        let final_message = messages.last().expect("sent message");
        assert!(final_message.content.contains("[REDACTED]"));
        assert!(!final_message.content.contains("abcdefghijklmnopqrstuvwxyz"));
    }

    #[tokio::test]
    async fn final_delivery_is_idempotent_and_limited_to_once_per_turn() {
        let fixture = Fixture::new(true);
        fixture.register_session().await;
        let request = BridgeRequest::SendFinal {
            session_id: Some("session-1".to_owned()),
            session_label: None,
            content: "Done".to_owned(),
            idempotency_key: "session-1:turn-1:final".to_owned(),
        };
        fixture
            .service
            .dispatch(request.clone())
            .await
            .expect("first final delivery");
        let sent_after_first = fixture.qq.messages().await.len();
        fixture
            .service
            .dispatch(request)
            .await
            .expect("idempotent final replay");
        assert_eq!(fixture.qq.messages().await.len(), sent_after_first);

        let duplicate = fixture
            .service
            .dispatch(BridgeRequest::SendFinal {
                session_id: Some("session-1".to_owned()),
                session_label: None,
                content: "Done again".to_owned(),
                idempotency_key: "session-1:turn-1:different".to_owned(),
            })
            .await
            .expect_err("second distinct final must be rejected");
        assert!(duplicate.to_string().contains("already sent"));
        assert_eq!(fixture.qq.messages().await.len(), sent_after_first);
    }

    #[tokio::test]
    async fn qq_approval_button_is_acknowledged_and_replay_safe() {
        let fixture = Fixture::new_ahp();
        let approval = fixture
            .service
            .database()
            .ahp_begin_approval(&NewAhpApproval {
                approval_key: "approval-button-1".to_owned(),
                stage: "parameter".to_owned(),
                session_uri: "copilot:/session-1".to_owned(),
                chat_uri: "ahp-chat://default/session-1".to_owned(),
                turn_id: "turn-1".to_owned(),
                tool_call_id: "tool-1".to_owned(),
                tool_name: "Terminal".to_owned(),
                summary: "Run tests".to_owned(),
                expires_at: Utc::now().timestamp() + 600,
            })
            .expect("approval");
        fixture
            .service
            .send_ahp_approval_notification(&approval.record, "[ABCDE · Shared]")
            .await;
        let messages = fixture.qq.messages().await;
        assert!(
            messages
                .iter()
                .any(|message| message.kind == "approval_buttons")
        );
        assert!(
            messages
                .iter()
                .all(|message| message.kind != "choice_buttons")
        );

        fixture
            .service
            .handle_interaction(
                "outer-event-1",
                "interaction-1",
                "app-1",
                "owner-openid",
                "c2c",
                2,
                11,
                11,
                "approve",
                &approval.record.allow_button_data,
            )
            .await
            .expect("button interaction");
        fixture
            .service
            .handle_interaction(
                "outer-event-1",
                "interaction-1",
                "app-1",
                "owner-openid",
                "c2c",
                2,
                11,
                11,
                "approve",
                &approval.record.allow_button_data,
            )
            .await
            .expect("duplicate interaction");
        assert_eq!(
            fixture.qq.acknowledgements().await,
            vec![
                ("interaction-1".to_owned(), 0),
                ("interaction-1".to_owned(), 3)
            ]
        );
        let submitted = fixture
            .service
            .database()
            .ahp_approval_by_code(&approval.record.short_code)
            .expect("query approval")
            .expect("approval exists");
        assert_eq!(submitted.state, "submitted");
        let commands = fixture
            .service
            .database()
            .ahp_poll_commands("adapter-stable", "adapter-run-1", 60)
            .expect("approval command");
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].kind,
            crate::protocol::AhpCommandKind::ApproveTool
        );
    }

    #[tokio::test]
    async fn qq_single_choice_button_completes_pending_input_once() {
        let fixture = Fixture::new_ahp();
        let input = fixture
            .service
            .database()
            .ahp_begin_input(&NewAhpInput {
                input_key: "input-button-1".to_owned(),
                session_uri: "copilot:/session-1".to_owned(),
                chat_uri: "ahp-chat://default/session-1".to_owned(),
                request_id: "request-1".to_owned(),
                prompt: "Choose environment".to_owned(),
                choices: vec!["test".to_owned(), "production".to_owned()],
                allow_freeform: false,
                selection_mode: "single".to_owned(),
                expires_at: Utc::now().timestamp() + 600,
            })
            .expect("input");
        fixture
            .service
            .send_ahp_input_notification(&input.record, "[ABCDE · Shared]")
            .await;
        assert_eq!(
            fixture
                .qq
                .messages()
                .await
                .last()
                .expect("choice message")
                .kind,
            "choice_buttons"
        );
        let first_button = input.record.buttons.first().expect("first choice");
        fixture
            .service
            .handle_interaction(
                "outer-input-1",
                "interaction-input-1",
                "app-1",
                "owner-openid",
                "c2c",
                2,
                11,
                11,
                "choice_0",
                &first_button.button_data,
            )
            .await
            .expect("choice interaction");
        fixture
            .service
            .handle_interaction(
                "outer-input-1",
                "interaction-input-1",
                "app-1",
                "owner-openid",
                "c2c",
                2,
                11,
                11,
                "choice_0",
                &first_button.button_data,
            )
            .await
            .expect("duplicate choice interaction");
        assert_eq!(
            fixture.qq.acknowledgements().await,
            vec![
                ("interaction-input-1".to_owned(), 0),
                ("interaction-input-1".to_owned(), 3)
            ]
        );
        let submitted = fixture
            .service
            .database()
            .ahp_input_by_code(&input.record.short_code)
            .expect("query input")
            .expect("input exists");
        assert_eq!(submitted.state, "submitted");
        let commands = fixture
            .service
            .database()
            .ahp_poll_commands("adapter-stable", "adapter-run-1", 60)
            .expect("input command");
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].kind,
            crate::protocol::AhpCommandKind::CompleteInput
        );
    }

    #[tokio::test]
    async fn failed_projection_replays_on_next_owner_message() {
        let fixture = Fixture::new_ahp();
        fixture.qq.set_send_failure(true);
        fixture
            .service
            .dispatch(BridgeRequest::AhpPublishEvents {
                adapter_id: "adapter-stable".to_owned(),
                adapter_instance_id: "adapter-run-1".to_owned(),
                binding_id: fixture.ahp_binding_id(),
                binding_generation: 1,
                events: vec![AhpPublishedEvent {
                    event_id: sha256_hex(b"offline-assistant-event"),
                    host_instance_id: "host-1".to_owned(),
                    server_sequence: Some(2),
                    session_uri: "copilot:/session-1".to_owned(),
                    chat_uri: Some("ahp-chat://default/session-1".to_owned()),
                    turn_id: Some("turn-1".to_owned()),
                    kind: AhpEventKind::AssistantMessage,
                    origin_client_id: None,
                    occurred_at: "2026-08-27T00:00:01Z".to_owned(),
                    data: json!({
                        "message_id": "assistant-1",
                        "content": "missed response",
                        "complete": true,
                        "historical": false
                    }),
                }],
            })
            .await
            .expect("publish offline event");
        assert_eq!(
            fixture
                .service
                .database()
                .ahp_status(60)
                .expect("status")
                .pending_projections,
            1
        );

        fixture.qq.set_send_failure(false);
        fixture
            .service
            .handle_inbound_message("owner-sync-message", "owner-openid", "/status")
            .await
            .expect("trigger replay");
        let last = fixture
            .qq
            .messages()
            .await
            .last()
            .cloned()
            .expect("replay delivery");
        assert!(last.content.contains("missed response"));
        assert!(last.content.contains("离线期间未实时送达"));
        assert_eq!(last.reply_to_message_id, None);
        assert_eq!(
            fixture
                .service
                .database()
                .ahp_status(60)
                .expect("status")
                .pending_projections,
            0
        );
        fixture
            .service
            .handle_inbound_message("owner-sync-message-2", "owner-openid", "/status")
            .await
            .expect("trigger second owner response");
        let replay_count = fixture
            .qq
            .messages()
            .await
            .iter()
            .filter(|message| message.content.contains("离线期间未实时送达"))
            .count();
        assert_eq!(replay_count, 1);
        let before_duplicate = fixture.qq.messages().await.len();
        fixture
            .service
            .try_deliver_ahp_event_projection(
                &sha256_hex(b"offline-assistant-event"),
                "ahp_assistant_message",
                "missed response",
            )
            .await;
        assert_eq!(fixture.qq.messages().await.len(), before_duplicate);
    }

    #[tokio::test]
    async fn concurrent_owner_messages_send_one_offline_replay() {
        let fixture = Fixture::new_ahp();
        queue_failed_assistant_projection(
            &fixture,
            b"concurrent-offline-event",
            "concurrent missed response",
            3,
        )
        .await;
        fixture.qq.set_send_failure(false);

        let first = fixture.service.task_clone();
        let second = fixture.service.task_clone();
        let (first_result, second_result) = tokio::join!(
            first.handle_inbound_message("concurrent-sync-1", "owner-openid", "/status"),
            second.handle_inbound_message("concurrent-sync-2", "owner-openid", "/status")
        );
        first_result.expect("first concurrent response");
        second_result.expect("second concurrent response");

        let messages = fixture.qq.messages().await;
        assert_eq!(
            messages
                .iter()
                .filter(|message| message.content.contains("离线期间未实时送达"))
                .count(),
            1
        );
        assert_eq!(
            fixture
                .service
                .database()
                .ahp_status(60)
                .expect("status")
                .pending_projections,
            0
        );
    }

    #[tokio::test]
    async fn sent_offline_replay_is_not_resent_after_restart_window() {
        let fixture = Fixture::new_ahp();
        queue_failed_assistant_projection(
            &fixture,
            b"restart-window-offline-event",
            "restart window missed response",
            4,
        )
        .await;
        fixture.qq.set_send_failure(false);

        let replay = fixture
            .service
            .claim_pending_ahp_replay()
            .expect("claim replay")
            .expect("pending replay");
        fixture
            .service
            .send_if_owner(
                "ahp_offline_replay",
                None,
                &replay.delivery_key,
                &replay.content,
                None,
            )
            .await
            .expect("simulate replay sent before projection update");
        queue_failed_assistant_projection(
            &fixture,
            b"new-event-after-replay-send",
            "newer missed response",
            5,
        )
        .await;
        fixture.qq.set_send_failure(false);
        assert_eq!(
            fixture
                .service
                .database()
                .ahp_status(60)
                .expect("status")
                .pending_projections,
            2
        );

        fixture
            .service
            .handle_inbound_message("restart-window-sync", "owner-openid", "/status")
            .await
            .expect("recover sent replay");
        assert_eq!(
            fixture
                .service
                .database()
                .ahp_status(60)
                .expect("status")
                .pending_projections,
            1
        );
        fixture
            .service
            .handle_inbound_message("restart-window-sync-2", "owner-openid", "/status")
            .await
            .expect("replay newer batch");
        let messages = fixture.qq.messages().await;
        assert_eq!(
            messages
                .iter()
                .filter(|message| message.content.contains("离线期间未实时送达"))
                .count(),
            2
        );
        assert_eq!(
            messages
                .iter()
                .filter(|message| message.content.contains("restart window missed response"))
                .count(),
            1
        );
        assert_eq!(
            fixture
                .service
                .database()
                .ahp_status(60)
                .expect("status")
                .pending_projections,
            0
        );
    }

    #[tokio::test]
    async fn uncertain_offline_replay_is_not_retried() {
        let fixture = Fixture::new_ahp();
        queue_failed_assistant_projection(
            &fixture,
            b"uncertain-offline-replay",
            "uncertain missed response",
            5,
        )
        .await;

        fixture
            .service
            .replay_pending_ahp_projections()
            .await
            .expect("uncertain replay is terminal");
        assert_eq!(
            fixture
                .service
                .database()
                .ahp_status(60)
                .expect("status")
                .pending_projections,
            0
        );

        fixture.qq.set_send_failure(false);
        fixture
            .service
            .handle_inbound_message("uncertain-sync", "owner-openid", "/status")
            .await
            .expect("subsequent owner response");
        assert_eq!(
            fixture
                .qq
                .messages()
                .await
                .iter()
                .filter(|message| message.content.contains("离线期间未实时送达"))
                .count(),
            0
        );
    }

    #[tokio::test]
    async fn definitively_failed_replay_uses_a_new_generation() {
        let fixture = Fixture::new_ahp();
        queue_failed_assistant_projection(
            &fixture,
            b"retryable-offline-replay",
            "retryable missed response",
            6,
        )
        .await;
        fixture.qq.set_send_failure(false);
        let first = fixture
            .service
            .claim_pending_ahp_replay()
            .expect("claim first replay")
            .expect("first replay");
        let failed = fixture
            .service
            .database()
            .begin_delivery(NewDelivery {
                delivery_id: Uuid::new_v4(),
                idempotency_key: first.delivery_key.clone(),
                kind: "ahp_offline_replay".to_owned(),
                session_id: None,
            })
            .expect("begin failed replay");
        fixture
            .service
            .database()
            .finish_delivery(
                failed.record.delivery_id,
                "failed",
                None,
                Some("rate_limited"),
            )
            .expect("finish failed replay");

        fixture
            .service
            .replay_pending_ahp_projections()
            .await
            .expect_err("definitive failure should be retryable");
        let second = fixture
            .service
            .claim_pending_ahp_replay()
            .expect("claim second replay")
            .expect("second replay");
        assert_ne!(first.delivery_key, second.delivery_key);
        fixture
            .service
            .replay_pending_ahp_projections()
            .await
            .expect("second generation replay");
        assert_eq!(
            fixture
                .qq
                .messages()
                .await
                .iter()
                .filter(|message| message.content.contains("离线期间未实时送达"))
                .count(),
            1
        );
        assert_eq!(
            fixture
                .service
                .database()
                .ahp_status(60)
                .expect("status")
                .pending_projections,
            0
        );
    }

    #[tokio::test]
    async fn turn_lifecycle_starts_and_stops_typing_indicator() {
        let fixture = Fixture::new_ahp();
        let started = AhpPublishedEvent {
            event_id: sha256_hex(b"typing-start"),
            host_instance_id: "host-1".to_owned(),
            server_sequence: Some(2),
            session_uri: "copilot:/session-1".to_owned(),
            chat_uri: Some("ahp-chat://default/session-1".to_owned()),
            turn_id: Some("turn-typing".to_owned()),
            kind: AhpEventKind::TurnStarted,
            origin_client_id: Some("vscode".to_owned()),
            occurred_at: "2026-08-27T00:00:01Z".to_owned(),
            data: json!({}),
        };
        fixture
            .service
            .handle_ahp_event("adapter-stable", &started)
            .await
            .expect("start typing");
        sleep(Duration::from_millis(20)).await;
        assert!(
            fixture
                .qq
                .messages()
                .await
                .iter()
                .any(|message| message.kind == "typing")
        );
        assert_eq!(fixture.service.typing_tasks.lock().await.len(), 1);

        fixture
            .service
            .handle_ahp_event(
                "adapter-stable",
                &AhpPublishedEvent {
                    event_id: sha256_hex(b"typing-stop"),
                    kind: AhpEventKind::TurnCompleted,
                    server_sequence: Some(3),
                    ..started
                },
            )
            .await
            .expect("stop typing");
        assert!(fixture.service.typing_tasks.lock().await.is_empty());
    }

    #[tokio::test]
    async fn switch_menu_lists_directories_and_switches_across_workspaces() {
        let fixture = Fixture::new_ahp_multi_workspace();
        let host = AhpHostDescriptor {
            endpoint_id: "endpoint-1".to_owned(),
            host_instance_id: "host-1".to_owned(),
            pid: 42,
            advertised_protocol: "1.0.0".to_owned(),
            selected_protocol: Some("1.0.0".to_owned()),
            state: crate::protocol::AhpHostState::Connected,
            host_label: Some("local".to_owned()),
            ssh_alias: None,
            target_kind: Some(AhpTargetKind::Local),
            target_path: Some(fixture.workspace.display().to_string()),
            endpoint_type: Some("socket".to_owned()),
            editor_client_tools_available: Some(true),
            last_seen_at: Some(Utc::now().timestamp()),
        };
        let workspace_uri = url::Url::from_file_path(&fixture.workspace)
            .expect("workspace URI")
            .to_string();
        let other_workspace = fixture
            .other_workspace
            .as_ref()
            .expect("other target workspace");
        let other_workspace_uri = url::Url::from_file_path(other_workspace)
            .expect("other workspace URI")
            .to_string();
        let outside_workspace = fixture._directory.path().join("outside-workspace");
        fs::create_dir(&outside_workspace).expect("outside workspace");
        let outside_workspace_uri = url::Url::from_file_path(&outside_workspace)
            .expect("outside workspace URI")
            .to_string();
        let sessions = vec![
            AhpSessionDescriptor {
                short_code: None,
                endpoint_id: "endpoint-1".to_owned(),
                host_instance_id: "host-1".to_owned(),
                session_uri: "copilot:/session-1".to_owned(),
                provider: "copilot".to_owned(),
                title: "First".to_owned(),
                status: 1,
                workspace_uris: vec![workspace_uri.clone()],
                created_at: "2026-08-27T00:00:00Z".to_owned(),
                modified_at: "2026-08-27T00:00:00Z".to_owned(),
                host_label: Some("local".to_owned()),
                ssh_alias: None,
                target_kind: Some(AhpTargetKind::Local),
                target_path: Some(fixture.workspace.display().to_string()),
                editor_client_tools_available: Some(true),
                host_state: None,
                host_last_seen_at: None,
            },
            AhpSessionDescriptor {
                short_code: None,
                endpoint_id: "endpoint-1".to_owned(),
                host_instance_id: "host-1".to_owned(),
                session_uri: "copilot:/session-2".to_owned(),
                provider: "copilot".to_owned(),
                title: "Second".to_owned(),
                status: 1,
                workspace_uris: vec![other_workspace_uri],
                created_at: "2026-08-27T00:01:00Z".to_owned(),
                modified_at: "2026-08-27T00:01:00Z".to_owned(),
                host_label: Some("local".to_owned()),
                ssh_alias: None,
                target_kind: Some(AhpTargetKind::Local),
                target_path: Some(other_workspace.display().to_string()),
                editor_client_tools_available: Some(true),
                host_state: None,
                host_last_seen_at: None,
            },
            AhpSessionDescriptor {
                short_code: None,
                endpoint_id: "endpoint-1".to_owned(),
                host_instance_id: "host-1".to_owned(),
                session_uri: "copilot:/outside-session".to_owned(),
                provider: "copilot".to_owned(),
                title: "Outside".to_owned(),
                status: 1,
                workspace_uris: vec![outside_workspace_uri],
                created_at: "2026-08-27T00:02:00Z".to_owned(),
                modified_at: "2026-08-27T00:02:00Z".to_owned(),
                host_label: Some("local".to_owned()),
                ssh_alias: None,
                target_kind: Some(AhpTargetKind::Local),
                target_path: Some(outside_workspace.display().to_string()),
                editor_client_tools_available: Some(true),
                host_state: None,
                host_last_seen_at: None,
            },
            AhpSessionDescriptor {
                short_code: None,
                endpoint_id: "endpoint-1".to_owned(),
                host_instance_id: "host-1".to_owned(),
                session_uri: "copilot:/busy-session".to_owned(),
                provider: "copilot".to_owned(),
                title: "Busy".to_owned(),
                status: 1 << 3,
                workspace_uris: vec![
                    url::Url::from_file_path(other_workspace)
                        .expect("busy workspace URI")
                        .to_string(),
                ],
                created_at: "2026-08-27T00:03:00Z".to_owned(),
                modified_at: "2026-08-27T00:03:00Z".to_owned(),
                host_label: Some("local".to_owned()),
                ssh_alias: None,
                target_kind: Some(AhpTargetKind::Local),
                target_path: Some(other_workspace.display().to_string()),
                editor_client_tools_available: Some(true),
                host_state: None,
                host_last_seen_at: None,
            },
        ];
        fixture
            .service
            .database()
            .ahp_replace_catalog(
                "adapter-stable",
                "adapter-run-1",
                std::slice::from_ref(&host),
                &sessions,
            )
            .expect("catalogue");
        let listing = fixture
            .service
            .process_ahp_owner_message("session-list-message", "/sessions")
            .await
            .expect("session list")
            .expect("session list response");
        assert!(listing.contains(&fixture.workspace.display().to_string()));
        assert!(listing.contains(&other_workspace.display().to_string()));
        assert!(listing.contains("First"));
        assert!(listing.contains("Second"));
        assert!(listing.contains("Busy"));
        assert!(listing.contains("忙碌"));
        assert!(!listing.contains("Outside"));
        assert!(
            fixture
                .service
                .process_ahp_owner_message("switch-menu-message", "/switch")
                .await
                .expect("switch menu")
                .is_none()
        );
        let menu = fixture
            .qq
            .messages()
            .await
            .last()
            .cloned()
            .expect("menu message");
        assert_eq!(menu.kind, "choice_buttons");
        assert_eq!(
            menu.reply_to_message_id.as_deref(),
            Some("switch-menu-message")
        );
        assert!(menu.content.contains("workspace"));
        assert!(menu.content.contains("other-workspace"));
        assert!(menu.content.contains("忙碌"));
        assert!(!menu.content.contains("Outside"));

        let session_uris = vec![
            "copilot:/session-1".to_owned(),
            "copilot:/session-2".to_owned(),
        ];
        let buttons = fixture
            .service
            .database()
            .ahp_create_session_switch_buttons(&session_uris, 600)
            .expect("switch buttons");
        let second = buttons
            .iter()
            .find(|button| button.session.session_uri == "copilot:/session-2")
            .expect("second Session");
        fixture
            .service
            .handle_interaction(
                "outer-switch-1",
                "interaction-switch-1",
                "app-1",
                "owner-openid",
                "c2c",
                2,
                11,
                11,
                "session_1",
                &second.button_data,
            )
            .await
            .expect("switch interaction");
        assert_eq!(
            fixture.qq.acknowledgements().await,
            vec![("interaction-switch-1".to_owned(), 0)]
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        let binding = loop {
            let binding = fixture
                .service
                .database()
                .ahp_binding()
                .expect("binding")
                .expect("bound");
            if binding.session_uri == "copilot:/session-2" {
                break binding;
            }
            assert!(Instant::now() < deadline, "switch did not finish");
            sleep(Duration::from_millis(20)).await;
        };
        assert_eq!(binding.session_uri, "copilot:/session-2");
        assert_eq!(binding.state, "binding");
        let bindings = fixture
            .service
            .database()
            .ahp_bindings()
            .expect("tracked bindings");
        assert_eq!(bindings.len(), 2);
        assert!(
            bindings
                .iter()
                .any(|binding| binding.session_uri == "copilot:/session-1")
        );
        assert_eq!(
            fixture
                .service
                .database()
                .ahp_status(60)
                .expect("status")
                .binding
                .expect("single binding")
                .session_uri,
            "copilot:/session-2"
        );
    }

    #[tokio::test]
    async fn recent_catalogue_sessions_are_auto_tracked_without_changing_foreground() {
        let fixture = Fixture::new_ahp();
        let sessions: Vec<_> = (1..=6)
            .map(|number| test_ahp_session(number, &fixture.workspace, 1))
            .collect();
        fixture
            .service
            .dispatch(BridgeRequest::AhpCatalogReplace {
                adapter_id: "adapter-stable".to_owned(),
                adapter_instance_id: "adapter-run-1".to_owned(),
                hosts: vec![test_ahp_host()],
                sessions,
                removed_session_uris: vec![],
                full_snapshot: true,
            })
            .await
            .expect("replace catalogue");

        let bindings = fixture
            .service
            .database()
            .ahp_bindings()
            .expect("tracked bindings");
        assert_eq!(bindings.len(), MAX_TRACKED_AHP_SESSIONS);
        assert_eq!(
            bindings
                .iter()
                .find(|binding| binding.foreground)
                .expect("foreground")
                .session_uri,
            "copilot:/session-1"
        );
        assert!(
            bindings
                .iter()
                .any(|binding| binding.session_uri == "copilot:/session-6")
        );
        assert!(
            bindings
                .iter()
                .all(|binding| binding.session_uri != "copilot:/session-2")
        );
    }

    #[tokio::test]
    async fn removed_new_commands_and_tokens_never_reach_agent_host() {
        let fixture = Fixture::new_ahp();
        fixture
            .service
            .database()
            .ahp_begin_input(&NewAhpInput {
                input_key: "pending-during-removed-new".to_owned(),
                session_uri: "copilot:/session-1".to_owned(),
                chat_uri: "ahp-chat://default/session-1".to_owned(),
                request_id: "pending-new-request".to_owned(),
                prompt: "Enter a value".to_owned(),
                choices: Vec::new(),
                allow_freeform: true,
                selection_mode: "none".to_owned(),
                expires_at: Utc::now().timestamp() + 600,
            })
            .expect("begin pending input");
        let requests = [
            "/new",
            "/new advanced",
            " /NEW ADVANCED ",
            "/new unexpected arguments",
            "newt_OLDTARGET",
            "newm_OLDMODEL",
            "newa_OLDAPPROVAL",
            "newx_OLDOPTION",
            "nw_OLDWIZARD",
        ];
        for (index, request) in requests.iter().enumerate() {
            fixture
                .service
                .handle_inbound_message(&format!("removed-new-{index}"), "owner-openid", request)
                .await
                .expect("reject retired request");
        }
        let messages = fixture.qq.messages().await;
        assert_eq!(messages.len(), requests.len());
        assert!(messages.iter().all(|message| {
            message.content.contains("PC")
                && message.content.contains("/sessions")
                && message.content.contains("/switch")
        }));
        let database = fixture.service.database();
        assert!(
            database
                .ahp_poll_commands("adapter-stable", "adapter-run-1", 60)
                .expect("no Host commands")
                .is_empty()
        );
        let status = database.ahp_status(60).expect("status");
        assert_eq!(status.sessions.len(), 1);
        assert_eq!(status.bindings.len(), 1);
        assert_eq!(status.pending_inputs, 1);
        assert_eq!(
            status.binding.expect("foreground").session_uri,
            "copilot:/session-1"
        );
        assert!(!ahp_help_text(AhpToolNotificationMode::Compact).contains("/new"));
        assert!(
            !format_ahp_status(
                &database.ahp_status(60).expect("status"),
                AhpToolNotificationMode::Compact
            )
            .contains("创建向导")
        );
    }

    #[tokio::test]
    async fn removed_new_does_not_capture_subsequent_plain_messages() {
        let fixture = Fixture::new_ahp();
        fixture
            .service
            .handle_inbound_message("removed-before-text", "owner-openid", "/new")
            .await
            .expect("reject retired command");
        fixture
            .service
            .handle_inbound_message("normal-after-new", "owner-openid", "continue existing task")
            .await
            .expect("send ordinary text");
        let commands = fixture
            .service
            .database()
            .ahp_poll_commands("adapter-stable", "adapter-run-1", 60)
            .expect("message command");
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].kind,
            crate::protocol::AhpCommandKind::SendMessage
        );
        assert_eq!(commands[0].data["content"], "continue existing task");
        assert_eq!(commands[0].binding_id, fixture.ahp_binding_id());
    }

    #[tokio::test]
    async fn removed_new_voice_requests_never_reach_agent_host() {
        let fixture = Fixture::new_ahp_with_voice_input(true);
        for (index, transcript) in ["/new", "/new advanced", "newt_OLDTARGET"]
            .iter()
            .enumerate()
        {
            fixture
                .service
                .handle_inbound_voice_message(
                    &format!("removed-new-voice-{index}"),
                    "owner-openid",
                    Some(transcript),
                )
                .await
                .expect("reject retired voice request");
        }
        assert!(
            fixture
                .qq
                .messages()
                .await
                .iter()
                .all(|message| message.content.contains("PC"))
        );
        assert!(
            fixture
                .service
                .database()
                .ahp_poll_commands("adapter-stable", "adapter-run-1", 60)
                .expect("no Host commands")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn retired_wizard_buttons_are_rejected_without_host_commands() {
        let fixture = Fixture::new_ahp();
        let buttons = [
            ("target_0", "newt_OLDTARGET"),
            ("model_0", "newm_OLDMODEL"),
            ("approval_0", "newa_OLDAPPROVAL"),
            ("nw_0", "nw_OLDWIZARD"),
            ("choice_0", "newx_OLDOPTION"),
            ("session_0", "newt_OLDTARGET"),
            ("approve", "newa_OLDAPPROVAL"),
        ];
        for (index, (button_id, button_data)) in buttons.iter().enumerate() {
            fixture
                .service
                .handle_interaction(
                    &format!("removed-button-event-{index}"),
                    &format!("removed-button-{index}"),
                    "app-1",
                    "owner-openid",
                    "c2c",
                    2,
                    11,
                    11,
                    button_id,
                    button_data,
                )
                .await
                .expect("reject retired button");
        }
        let acknowledgements = fixture.qq.acknowledgements().await;
        assert_eq!(acknowledgements.len(), buttons.len());
        assert!(acknowledgements.iter().all(|(_, code)| *code == 4));
        let messages = fixture.qq.messages().await;
        assert_eq!(messages.len(), buttons.len());
        assert!(
            messages
                .iter()
                .all(|message| message.content.contains("PC"))
        );
        assert!(
            fixture
                .service
                .database()
                .ahp_poll_commands("adapter-stable", "adapter-run-1", 60)
                .expect("no Host commands")
                .is_empty()
        );
        assert_eq!(
            fixture
                .service
                .database()
                .ahp_binding()
                .expect("foreground")
                .expect("binding")
                .session_uri,
            "copilot:/session-1"
        );
    }

    #[tokio::test]
    async fn failed_newer_switch_does_not_revive_an_older_pending_switch() {
        let fixture = Fixture::new_ahp();
        let first = test_ahp_session(1, &fixture.workspace, 1);
        let stale_intent = fixture.service.begin_foreground_switch();
        let newer_intent = fixture.service.begin_foreground_switch();
        assert!(
            fixture
                .service
                .commit_foreground_switch(stale_intent, &first.endpoint_id, &first.session_uri)
                .is_err()
        );
        let mut forbidden = first.clone();
        forbidden.workspace_uris = vec!["file:///Z:/outside-workspace".to_owned()];
        forbidden.target_path = Some("Z:\\outside-workspace".to_owned());
        assert!(
            fixture
                .service
                .switch_ahp_session(forbidden, newer_intent)
                .await
                .is_err()
        );
        assert!(
            fixture
                .service
                .commit_foreground_switch(stale_intent, &first.endpoint_id, &first.session_uri)
                .is_err()
        );
        assert_eq!(
            fixture
                .service
                .database()
                .ahp_binding()
                .expect("foreground")
                .expect("binding")
                .session_uri,
            first.session_uri
        );
    }

    #[tokio::test]
    async fn stale_switch_intent_cannot_replace_a_newer_foreground_choice() {
        let fixture = Fixture::new_ahp();
        let first = test_ahp_session(1, &fixture.workspace, 1);
        let second = test_ahp_session(2, &fixture.workspace, 2);
        fixture
            .service
            .dispatch(BridgeRequest::AhpCatalogReplace {
                adapter_id: "adapter-stable".to_owned(),
                adapter_instance_id: "adapter-run-1".to_owned(),
                hosts: vec![test_ahp_host()],
                sessions: vec![first.clone(), second.clone()],
                removed_session_uris: vec![],
                full_snapshot: true,
            })
            .await
            .expect("replace catalogue");

        let stale_intent = fixture.service.begin_foreground_switch();
        let newer_intent = fixture.service.begin_foreground_switch();
        fixture
            .service
            .commit_foreground_switch(newer_intent, &second.endpoint_id, &second.session_uri)
            .expect("newer switch");
        assert!(
            fixture
                .service
                .commit_foreground_switch(stale_intent, &first.endpoint_id, &first.session_uri)
                .is_err()
        );
        assert_eq!(
            fixture
                .service
                .database()
                .ahp_binding()
                .expect("foreground")
                .expect("binding")
                .session_uri,
            second.session_uri
        );
    }

    #[tokio::test]
    async fn targeted_send_tracks_session_without_changing_foreground() {
        let fixture = Fixture::new_ahp();
        fixture
            .service
            .database()
            .ahp_replace_catalog(
                "adapter-stable",
                "adapter-run-1",
                &[test_ahp_host()],
                &[
                    test_ahp_session(1, &fixture.workspace, 1),
                    test_ahp_session(2, &fixture.workspace, 1),
                ],
            )
            .expect("catalogue");
        let second = fixture
            .service
            .database()
            .ahp_session_by_uri("copilot:/session-2")
            .expect("session query")
            .expect("second session");
        let response = fixture
            .service
            .process_ahp_owner_message(
                "targeted-send",
                &format!(
                    "/send {} continue in background",
                    second.short_code.as_deref().expect("short code")
                ),
            )
            .await
            .expect("targeted send")
            .expect("response");
        assert!(response.contains("前台 Session 未改变"));
        assert_eq!(
            fixture
                .service
                .database()
                .ahp_binding()
                .expect("foreground")
                .expect("binding")
                .session_uri,
            "copilot:/session-1"
        );
        let commands = fixture
            .service
            .database()
            .ahp_poll_commands("adapter-stable", "adapter-run-1", 60)
            .expect("commands");
        assert_eq!(commands.len(), 2);
        assert_eq!(
            commands[0].kind,
            crate::protocol::AhpCommandKind::BindSession
        );
        assert_eq!(
            commands[1].kind,
            crate::protocol::AhpCommandKind::SendMessage
        );
        assert_eq!(commands[0].binding_id, commands[1].binding_id);
    }

    #[tokio::test]
    async fn cancel_after_removed_new_targets_the_foreground_turn() {
        let fixture = Fixture::new_ahp();
        let (_, background) = track_ready_test_ahp_session(&fixture, 2);
        let database = fixture.service.database();
        let foreground = database
            .ahp_binding()
            .expect("foreground")
            .expect("binding");
        for binding in [&foreground, &background] {
            database
                .ahp_publish_events(
                    "adapter-stable",
                    "adapter-run-1",
                    &binding.binding_id,
                    binding.generation,
                    &[AhpPublishedEvent {
                        event_id: sha256_hex(binding.binding_id.as_bytes()),
                        host_instance_id: "host-1".to_owned(),
                        server_sequence: Some(2),
                        session_uri: binding.session_uri.clone(),
                        chat_uri: binding.chat_uri.clone(),
                        turn_id: Some(format!("turn-{}", binding.binding_id)),
                        kind: AhpEventKind::TurnStarted,
                        origin_client_id: None,
                        occurred_at: "2026-09-16T00:00:00Z".to_owned(),
                        data: json!({}),
                    }],
                )
                .expect("start Turn");
        }
        fixture
            .service
            .handle_inbound_message("new-before-cancel", "owner-openid", "/new advanced")
            .await
            .expect("reject retired command");
        fixture
            .service
            .handle_inbound_message("foreground-cancel", "owner-openid", "/cancel")
            .await
            .expect("cancel foreground");
        let commands = database
            .ahp_poll_commands("adapter-stable", "adapter-run-1", 60)
            .expect("cancel command");
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].kind,
            crate::protocol::AhpCommandKind::CancelTurn
        );
        assert_eq!(commands[0].binding_id, foreground.binding_id);
        assert_eq!(
            database
                .ahp_binding()
                .expect("foreground")
                .expect("binding")
                .binding_id,
            foreground.binding_id
        );
    }

    #[tokio::test]
    async fn targeted_cancel_routes_to_a_background_session() {
        let fixture = Fixture::new_ahp();
        let (session, binding) = track_ready_test_ahp_session(&fixture, 2);
        fixture
            .service
            .database()
            .ahp_publish_events(
                "adapter-stable",
                "adapter-run-1",
                &binding.binding_id,
                binding.generation,
                &[AhpPublishedEvent {
                    event_id: sha256_hex(b"background-turn-started"),
                    host_instance_id: "host-1".to_owned(),
                    server_sequence: Some(2),
                    session_uri: session.session_uri.clone(),
                    chat_uri: Some("ahp-chat://default/session-2".to_owned()),
                    turn_id: Some("turn-background".to_owned()),
                    kind: AhpEventKind::TurnStarted,
                    origin_client_id: None,
                    occurred_at: "2026-09-02T00:02:30Z".to_owned(),
                    data: json!({}),
                }],
            )
            .expect("start background Turn");

        let response = fixture
            .service
            .process_ahp_owner_message(
                "targeted-cancel",
                &format!(
                    "/cancel {}",
                    session.short_code.as_deref().expect("short code")
                ),
            )
            .await
            .expect("targeted cancel")
            .expect("response");
        assert!(response.contains("取消请求已排队"));
        let commands = fixture
            .service
            .database()
            .ahp_poll_commands("adapter-stable", "adapter-run-1", 60)
            .expect("cancel command");
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].kind,
            crate::protocol::AhpCommandKind::CancelTurn
        );
        assert_eq!(commands[0].binding_id, binding.binding_id);
        assert_eq!(
            fixture
                .service
                .database()
                .ahp_binding()
                .expect("foreground")
                .expect("binding")
                .session_uri,
            "copilot:/session-1"
        );
    }

    #[tokio::test]
    async fn detach_safely_unbinds_an_idle_background_session() {
        let fixture = Fixture::new_ahp();
        let (session, binding) = track_ready_test_ahp_session(&fixture, 2);
        let response = fixture
            .service
            .process_ahp_owner_message(
                "targeted-detach",
                &format!(
                    "/detach {}",
                    session.short_code.as_deref().expect("short code")
                ),
            )
            .await
            .expect("targeted detach")
            .expect("response");
        assert!(response.contains("安全解绑队列"));
        let commands = fixture
            .service
            .database()
            .ahp_poll_commands("adapter-stable", "adapter-run-1", 60)
            .expect("unbind command");
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].kind,
            crate::protocol::AhpCommandKind::UnbindSession
        );
        assert_eq!(commands[0].binding_id, binding.binding_id);
        assert!(
            fixture
                .service
                .database()
                .ahp_bindings()
                .expect("bindings")
                .iter()
                .all(|tracked| tracked.binding_id != binding.binding_id)
        );
        assert_eq!(
            fixture
                .service
                .database()
                .ahp_binding()
                .expect("foreground")
                .expect("binding")
                .session_uri,
            "copilot:/session-1"
        );
    }

    #[tokio::test]
    async fn concurrent_turns_in_same_workspace_send_conflict_warning() {
        let fixture = Fixture::new_ahp();
        fixture
            .service
            .database()
            .ahp_replace_catalog(
                "adapter-stable",
                "adapter-run-1",
                &[test_ahp_host()],
                &[
                    test_ahp_session(1, &fixture.workspace, 1),
                    test_ahp_session(2, &fixture.workspace, 1),
                ],
            )
            .expect("catalogue");
        let second = fixture
            .service
            .database()
            .ahp_track_session("endpoint-1", "copilot:/session-2")
            .expect("track second");
        let bind_command = fixture
            .service
            .database()
            .ahp_poll_commands("adapter-stable", "adapter-run-1", 60)
            .expect("bind command");
        fixture
            .service
            .database()
            .ahp_ack_command(
                "adapter-stable",
                "adapter-run-1",
                bind_command[0].command_id,
                crate::protocol::AhpCommandOutcome::Applied,
                None,
                None,
            )
            .expect("ack bind");
        fixture
            .service
            .database()
            .ahp_binding_ready(
                "adapter-stable",
                "adapter-run-1",
                &second.binding_id,
                "endpoint-1",
                "host-1",
                second.generation,
                "copilot:/session-2",
                "ahp-chat://default/session-2",
                1,
            )
            .expect("second ready");
        let first = fixture
            .service
            .database()
            .ahp_binding_for_session("copilot:/session-1")
            .expect("first query")
            .expect("first binding");

        for (binding, turn, sequence) in [
            (&first, "turn-first", 2_u64),
            (&second, "turn-second", 3_u64),
        ] {
            fixture
                .service
                .dispatch(BridgeRequest::AhpPublishEvents {
                    adapter_id: "adapter-stable".to_owned(),
                    adapter_instance_id: "adapter-run-1".to_owned(),
                    binding_id: binding.binding_id.clone(),
                    binding_generation: binding.generation,
                    events: vec![AhpPublishedEvent {
                        event_id: sha256_hex(format!("{turn}-started").as_bytes()),
                        host_instance_id: "host-1".to_owned(),
                        server_sequence: Some(sequence),
                        session_uri: binding.session_uri.clone(),
                        chat_uri: binding.chat_uri.clone(),
                        turn_id: Some(turn.to_owned()),
                        kind: AhpEventKind::TurnStarted,
                        origin_client_id: None,
                        occurred_at: "2026-09-02T00:00:00Z".to_owned(),
                        data: json!({}),
                    }],
                })
                .await
                .expect("publish turn");
        }

        let messages = fixture.qq.messages().await;
        assert!(messages.iter().any(|message| {
            message.kind == "text"
                && message.content.contains("同工作区并发警告")
                && message.content.contains("Git worktree")
        }));
    }

    fn test_ahp_host() -> AhpHostDescriptor {
        AhpHostDescriptor {
            endpoint_id: "endpoint-1".to_owned(),
            host_instance_id: "host-1".to_owned(),
            pid: 42,
            advertised_protocol: "1.0.0".to_owned(),
            selected_protocol: Some("1.0.0".to_owned()),
            state: crate::protocol::AhpHostState::Connected,
            host_label: Some("local".to_owned()),
            ssh_alias: None,
            target_kind: Some(AhpTargetKind::Local),
            target_path: None,
            endpoint_type: Some("socket".to_owned()),
            editor_client_tools_available: Some(true),
            last_seen_at: None,
        }
    }

    fn track_ready_test_ahp_session(
        fixture: &Fixture,
        number: usize,
    ) -> (AhpSessionDescriptor, crate::protocol::AhpBindingRecord) {
        let database = fixture.service.database();
        database
            .ahp_replace_catalog(
                "adapter-stable",
                "adapter-run-1",
                &[test_ahp_host()],
                &[
                    test_ahp_session(1, &fixture.workspace, 1),
                    test_ahp_session(number, &fixture.workspace, 1),
                ],
            )
            .expect("catalogue");
        let session = database
            .ahp_session_by_uri(&format!("copilot:/session-{number}"))
            .expect("session query")
            .expect("target Session");
        let binding = database
            .ahp_track_session(&session.endpoint_id, &session.session_uri)
            .expect("track Session");
        let command = database
            .ahp_poll_commands("adapter-stable", "adapter-run-1", 60)
            .expect("bind command")
            .into_iter()
            .find(|command| {
                command.binding_id == binding.binding_id
                    && command.kind == crate::protocol::AhpCommandKind::BindSession
            })
            .expect("target bind command");
        database
            .ahp_ack_command(
                "adapter-stable",
                "adapter-run-1",
                command.command_id,
                crate::protocol::AhpCommandOutcome::Applied,
                None,
                None,
            )
            .expect("ack bind");
        database
            .ahp_binding_ready(
                "adapter-stable",
                "adapter-run-1",
                &binding.binding_id,
                &binding.endpoint_id,
                binding.host_instance_id.as_deref().expect("host instance"),
                binding.generation,
                &binding.session_uri,
                &format!("ahp-chat://default/session-{number}"),
                1,
            )
            .expect("binding ready");
        (session, binding)
    }

    fn test_ahp_session(number: usize, workspace: &Path, status: u32) -> AhpSessionDescriptor {
        AhpSessionDescriptor {
            short_code: None,
            endpoint_id: "endpoint-1".to_owned(),
            host_instance_id: "host-1".to_owned(),
            session_uri: format!("copilot:/session-{number}"),
            provider: "copilot".to_owned(),
            title: format!("Session {number}"),
            status,
            workspace_uris: vec![
                url::Url::from_file_path(workspace)
                    .expect("workspace URI")
                    .to_string(),
            ],
            created_at: format!("2026-09-02T00:{number:02}:00Z"),
            modified_at: format!("2026-09-02T00:{number:02}:00Z"),
            host_label: Some("local".to_owned()),
            ssh_alias: None,
            target_kind: Some(AhpTargetKind::Local),
            target_path: Some(workspace.display().to_string()),
            editor_client_tools_available: Some(true),
            host_state: None,
            host_last_seen_at: None,
        }
    }

    struct Fixture {
        _directory: tempfile::TempDir,
        workspace: PathBuf,
        other_workspace: Option<PathBuf>,
        service: Arc<BridgeService>,
        qq: Arc<MockQqMessenger>,
    }

    async fn queue_failed_assistant_projection(
        fixture: &Fixture,
        event_seed: &[u8],
        content: &str,
        server_sequence: u64,
    ) {
        fixture.qq.set_send_failure(true);
        fixture
            .service
            .dispatch(BridgeRequest::AhpPublishEvents {
                adapter_id: "adapter-stable".to_owned(),
                adapter_instance_id: "adapter-run-1".to_owned(),
                binding_id: fixture.ahp_binding_id(),
                binding_generation: 1,
                events: vec![AhpPublishedEvent {
                    event_id: sha256_hex(event_seed),
                    host_instance_id: "host-1".to_owned(),
                    server_sequence: Some(server_sequence),
                    session_uri: "copilot:/session-1".to_owned(),
                    chat_uri: Some("ahp-chat://default/session-1".to_owned()),
                    turn_id: Some(format!("turn-{server_sequence}")),
                    kind: AhpEventKind::AssistantMessage,
                    origin_client_id: None,
                    occurred_at: "2026-08-27T00:00:01Z".to_owned(),
                    data: json!({
                        "message_id": format!("assistant-{server_sequence}"),
                        "content": content,
                        "complete": true,
                        "historical": false
                    }),
                }],
            })
            .await
            .expect("publish offline event");
        assert!(
            fixture
                .service
                .database()
                .ahp_status(60)
                .expect("status")
                .pending_projections
                >= 1
        );
    }

    impl Fixture {
        fn new(bind_owner: bool) -> Self {
            let directory = tempfile::tempdir().expect("tempdir");
            let workspace = directory.path().join("workspace");
            let config_directory = directory.path().join("config");
            fs::create_dir_all(workspace.join("src")).expect("workspace");
            fs::create_dir_all(&config_directory).expect("config directory");
            let workspace = fs::canonicalize(workspace).expect("canonical workspace");
            let config_path = config_directory.join("config.toml");
            let config =
                AppConfig::write_new(&config_path, vec![workspace.clone()]).expect("config");
            let database = Database::open(&config.bridge.database_path).expect("database");
            if bind_owner {
                let code = database.create_binding_code(600).expect("binding code");
                assert_eq!(
                    database
                        .try_bind_owner("owner-openid", &code, "bind-message")
                        .expect("bind owner"),
                    BindOutcome::Bound
                );
            }
            let qq = Arc::new(MockQqMessenger::new());
            let messenger: Arc<dyn QqMessenger> = qq.clone();
            let service = Arc::new(BridgeService::new(
                Arc::new(config),
                config_path,
                database,
                messenger,
            ));
            Self {
                _directory: directory,
                workspace,
                other_workspace: None,
                service,
                qq,
            }
        }

        fn new_ahp() -> Self {
            Self::new_ahp_with_options(false, false, AhpToolNotificationMode::Compact)
        }

        fn new_ahp_with_ssh() -> Self {
            let mut fixture = Self::new_ahp();
            let mut config = (*fixture.service.config()).clone();
            config.ahp.ssh_executable = Some(fixture.workspace.join("ssh.exe"));
            config
                .save(&fixture.service.config_path)
                .expect("SSH fixture config");
            fixture.service = Arc::new(BridgeService::new(
                Arc::new(config),
                fixture.service.config_path.clone(),
                fixture.service.database().clone(),
                fixture.qq.clone(),
            ));
            fixture
        }

        fn ahp_binding_id(&self) -> String {
            self.service
                .database()
                .ahp_binding()
                .expect("binding query")
                .expect("foreground binding")
                .binding_id
        }

        fn new_ahp_multi_workspace() -> Self {
            Self::new_ahp_with_options(false, true, AhpToolNotificationMode::Compact)
        }

        fn new_ahp_with_voice_input(voice_input_enabled: bool) -> Self {
            Self::new_ahp_with_options(voice_input_enabled, false, AhpToolNotificationMode::Compact)
        }

        fn new_ahp_with_notification_mode(tool_notification_mode: AhpToolNotificationMode) -> Self {
            Self::new_ahp_with_options(false, false, tool_notification_mode)
        }

        fn new_ahp_with_options(
            voice_input_enabled: bool,
            multi_workspace: bool,
            tool_notification_mode: AhpToolNotificationMode,
        ) -> Self {
            let directory = tempfile::tempdir().expect("tempdir");
            let workspace = directory.path().join("workspace");
            let other_workspace = directory.path().join("other-workspace");
            let config_directory = directory.path().join("config");
            fs::create_dir_all(workspace.join("src")).expect("workspace");
            if multi_workspace {
                fs::create_dir_all(other_workspace.join("src")).expect("other workspace");
            }
            fs::create_dir_all(&config_directory).expect("config directory");
            let workspace = fs::canonicalize(workspace).expect("canonical workspace");
            let other_workspace = multi_workspace
                .then(|| fs::canonicalize(other_workspace).expect("canonical other workspace"));
            let config_path = config_directory.join("config.toml");
            let mut workspace_roots = vec![workspace.clone()];
            workspace_roots.extend(other_workspace.iter().cloned());
            let mut config = AppConfig::write_new(&config_path, workspace_roots).expect("config");
            config.qq.app_id = "app-1".to_owned();
            config.qq.approval_buttons_enabled = true;
            config.qq.voice_input_enabled = voice_input_enabled;
            config.qq.intents |= 1_u64 << 26;
            config.ahp.enabled = true;
            config.ahp.shared_workspaces = vec![workspace.clone()];
            config.ahp.tool_notification_mode = tool_notification_mode;
            config
                .ahp
                .shared_workspaces
                .extend(other_workspace.iter().cloned());
            config.save(&config_path).expect("save AHP fixture config");
            let database = Database::open(&config.bridge.database_path).expect("database");
            let code = database.create_binding_code(600).expect("binding code");
            assert_eq!(
                database
                    .try_bind_owner("owner-openid", &code, "bind-message")
                    .expect("bind owner"),
                BindOutcome::Bound
            );
            database
                .ahp_register_adapter(&AhpAdapterRegistration {
                    adapter_id: "adapter-stable".to_owned(),
                    adapter_instance_id: "adapter-run-1".to_owned(),
                    version: "0.1.0".to_owned(),
                    supported_protocols: vec!["1.0.0".to_owned()],
                })
                .expect("register adapter");
            database
                .ahp_replace_catalog(
                    "adapter-stable",
                    "adapter-run-1",
                    &[AhpHostDescriptor {
                        endpoint_id: "endpoint-1".to_owned(),
                        host_instance_id: "host-1".to_owned(),
                        pid: 42,
                        advertised_protocol: "1.0.0".to_owned(),
                        selected_protocol: Some("1.0.0".to_owned()),
                        state: crate::protocol::AhpHostState::Connected,
                        host_label: Some("local".to_owned()),
                        ssh_alias: None,
                        target_kind: Some(AhpTargetKind::Local),
                        target_path: Some(workspace.display().to_string()),
                        endpoint_type: Some("socket".to_owned()),
                        editor_client_tools_available: Some(true),
                        last_seen_at: Some(Utc::now().timestamp()),
                    }],
                    &[AhpSessionDescriptor {
                        short_code: None,
                        endpoint_id: "endpoint-1".to_owned(),
                        host_instance_id: "host-1".to_owned(),
                        session_uri: "copilot:/session-1".to_owned(),
                        provider: "copilot".to_owned(),
                        title: "Shared".to_owned(),
                        status: 1,
                        workspace_uris: vec![
                            url::Url::from_file_path(&workspace)
                                .expect("workspace URI")
                                .to_string(),
                        ],
                        created_at: "2026-08-27T00:00:00Z".to_owned(),
                        modified_at: "2026-08-27T00:00:00Z".to_owned(),
                        host_label: Some("local".to_owned()),
                        ssh_alias: None,
                        target_kind: Some(AhpTargetKind::Local),
                        target_path: Some(workspace.display().to_string()),
                        editor_client_tools_available: Some(true),
                        host_state: None,
                        host_last_seen_at: None,
                    }],
                )
                .expect("catalogue");
            let binding = database
                .ahp_bind_session("endpoint-1", "copilot:/session-1")
                .expect("bind session");
            let command = database
                .ahp_poll_commands("adapter-stable", "adapter-run-1", 60)
                .expect("bind command");
            database
                .ahp_ack_command(
                    "adapter-stable",
                    "adapter-run-1",
                    command[0].command_id,
                    crate::protocol::AhpCommandOutcome::Applied,
                    None,
                    None,
                )
                .expect("ack bind");
            database
                .ahp_binding_ready(
                    "adapter-stable",
                    "adapter-run-1",
                    &binding.binding_id,
                    "endpoint-1",
                    "host-1",
                    binding.generation,
                    "copilot:/session-1",
                    "ahp-chat://default/session-1",
                    1,
                )
                .expect("binding ready");
            let qq = Arc::new(MockQqMessenger::new());
            let messenger: Arc<dyn QqMessenger> = qq.clone();
            let service = Arc::new(BridgeService::new(
                Arc::new(config),
                config_path,
                database,
                messenger,
            ));
            Self {
                _directory: directory,
                workspace,
                other_workspace,
                service,
                qq,
            }
        }

        async fn register_session(&self) -> String {
            let result = self
                .service
                .dispatch(BridgeRequest::RegisterPrompt {
                    session_id: "session-1".to_owned(),
                    cwd: self.workspace.clone(),
                })
                .await
                .expect("register session");
            result["session_label"]
                .as_str()
                .expect("session label")
                .to_owned()
        }

        async fn wait_for_pending_approval(&self) -> String {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let status = self.service.database().status_snapshot().expect("status");
                if let Some(approval) = status.pending_approvals.first() {
                    return approval.approval_code.clone();
                }
                assert!(Instant::now() < deadline, "approval was never created");
                sleep(Duration::from_millis(20)).await;
            }
        }
    }
}
