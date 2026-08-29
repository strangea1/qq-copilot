use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{
    sync::Mutex,
    task::JoinHandle,
    time::{Instant, sleep},
};
use uuid::Uuid;

use crate::{
    ahp_store::{AhpApprovalRecord, AhpInputRecord, AhpStatus, NewAhpApproval, NewAhpInput},
    config::{AhpToolNotificationMode, AppConfig},
    db::{
        ApprovalRecord, BeginDelivery, BindOutcome, Database, NewApproval, NewDelivery,
        NewQuestion, QuestionRecord,
    },
    protocol::{
        AhpAdapterRegistration, AhpCommandPollResult, AhpEventKind, AhpHostDescriptor,
        AhpPublishedEvent, AhpSessionDescriptor, ApprovalState, BridgeRequest, PermissionDecision,
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

#[derive(Clone, Copy)]
enum InboundMessage<'a> {
    Text(&'a str),
    Voice(Option<&'a str>),
}

pub struct BridgeService {
    config: Arc<AppConfig>,
    config_path: PathBuf,
    database: Database,
    qq: Arc<dyn QqMessenger>,
    final_delivery_lock: Mutex<()>,
    typing_tasks: Mutex<HashMap<String, JoinHandle<()>>>,
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

impl BridgeService {
    pub fn new(
        config: Arc<AppConfig>,
        config_path: PathBuf,
        database: Database,
        qq: Arc<dyn QqMessenger>,
    ) -> Self {
        Self {
            config,
            config_path,
            database,
            qq,
            final_delivery_lock: Mutex::new(()),
            typing_tasks: Mutex::new(HashMap::new()),
        }
    }

    pub fn database(&self) -> &Database {
        &self.database
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
                if self.config.ahp.enabled {
                    status["ahp"] = serde_json::to_value(
                        self.database
                            .ahp_status(self.config.ahp.adapter_stale_seconds)?,
                    )
                    .context("failed to serialize AHP status")?;
                }
                Ok(status)
            }
            BridgeRequest::NewBindCode => {
                let code = self
                    .database
                    .create_binding_code(self.config.bridge.bind_code_ttl_seconds)?;
                Ok(json!({
                    "binding_code": code,
                    "expires_in_seconds": self.config.bridge.bind_code_ttl_seconds,
                }))
            }
            BridgeRequest::SetEmergencyEnabled { enabled } => {
                self.database.set_owner_enabled(enabled)?;
                Ok(json!({"enabled": enabled}))
            }
            BridgeRequest::AhpAdapterRegister { registration } => {
                self.require_ahp_enabled()?;
                validate_ahp_registration(&registration)?;
                serde_json::to_value(self.database.ahp_register_adapter(&registration)?)
                    .context("failed to serialize AHP adapter registration")
            }
            BridgeRequest::AhpCatalogReplace {
                adapter_id,
                adapter_instance_id,
                hosts,
                sessions,
            } => {
                self.require_ahp_enabled()?;
                validate_ahp_catalog(&hosts, &sessions)?;
                self.database.ahp_replace_catalog(
                    &adapter_id,
                    &adapter_instance_id,
                    &hosts,
                    &sessions,
                )?;
                Ok(json!({"accepted": true}))
            }
            BridgeRequest::AhpBindingReady {
                adapter_id,
                adapter_instance_id,
                endpoint_id,
                host_instance_id,
                binding_generation,
                session_uri,
                chat_uri,
                last_server_sequence,
            } => {
                self.require_ahp_enabled()?;
                self.database.ahp_binding_ready(
                    &adapter_id,
                    &adapter_instance_id,
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
                binding_generation,
                reason_code,
            } => {
                self.require_ahp_enabled()?;
                validate_short_code("reason_code", &reason_code)?;
                self.database.ahp_binding_failed(
                    &adapter_id,
                    &adapter_instance_id,
                    binding_generation,
                    &reason_code,
                )?;
                Ok(json!({"accepted": true}))
            }
            BridgeRequest::AhpPublishEvents {
                adapter_id,
                adapter_instance_id,
                binding_generation,
                events,
            } => {
                self.require_ahp_enabled()?;
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
            } => {
                self.require_ahp_enabled()?;
                if timeout_seconds == 0 || timeout_seconds > self.config.ahp.poll_seconds {
                    bail!(
                        "AHP poll timeout must be between 1 and {} seconds",
                        self.config.ahp.poll_seconds
                    );
                }
                let deadline = Instant::now() + Duration::from_secs(timeout_seconds);
                loop {
                    let commands = self.database.ahp_poll_commands(
                        &adapter_id,
                        &adapter_instance_id,
                        self.config.ahp.command_lease_seconds,
                    )?;
                    if !commands.is_empty() || Instant::now() >= deadline {
                        break serde_json::to_value(AhpCommandPollResult { commands })
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
                )?;
                Ok(json!({"accepted": true}))
            }
            BridgeRequest::AhpSessions => {
                self.require_ahp_enabled()?;
                serde_json::to_value(
                    self.database
                        .ahp_status(self.config.ahp.adapter_stale_seconds)?,
                )
                .context("failed to serialize AHP status")
            }
            BridgeRequest::AhpBindSession {
                endpoint_id,
                session_uri,
            } => {
                self.require_ahp_enabled()?;
                self.validate_ahp_session_workspace(&endpoint_id, &session_uri)?;
                serde_json::to_value(self.database.ahp_bind_session(&endpoint_id, &session_uri)?)
                    .context("failed to serialize AHP binding")
            }
            BridgeRequest::AhpUnbindSession => {
                self.require_ahp_enabled()?;
                Ok(json!({"unbound": self.database.ahp_unbind_session()?}))
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

        let response = match message {
            InboundMessage::Text(_) if self.config.ahp.enabled => {
                self.process_ahp_owner_message(message_id, command).await?
            }
            InboundMessage::Text(_) => self.process_owner_command(message_id, command).await?,
            InboundMessage::Voice(transcript) => {
                self.process_owner_voice_message(message_id, transcript)
                    .await?
            }
        };
        let (replay, replay_event_ids) = if self.config.ahp.enabled {
            self.pending_ahp_replay(
                response
                    .as_ref()
                    .map_or(0, |response| response.chars().count()),
            )
        } else {
            (None, Vec::new())
        };
        let response = match (response, replay) {
            (Some(response), Some(replay)) => Some(format!("{response}\n\n{replay}")),
            (Some(response), None) => Some(response),
            (None, Some(replay)) => Some(replay),
            (None, None) => None,
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
            self.database
                .ahp_mark_projections_delivered(&replay_event_ids)?;
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
        let structurally_valid = self.config.qq.approval_buttons_enabled
            && application_id == self.config.qq.app_id
            && scene == "c2c"
            && chat_type == 2
            && interaction_type == 11
            && data_type == 11
            && (expected_approved.is_some() || is_choice || is_session_switch)
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

        let (ack_code, confirmation) = if let Some(expected_approved) = expected_approved {
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
                .filter(|session| ahp_session_matches_workspace(&self.config, session))
                .map(|session| session.session_uri)
                .collect::<Vec<_>>();
            match self
                .database
                .ahp_switch_session_by_button(button_data, &allowed_session_uris)
            {
                Ok(Some(submission)) if submission.accepted => {
                    if let Some(workspace) =
                        ahp_session_target_workspace(&self.config, &submission.session)
                    {
                        (
                            0,
                            Some(format!(
                                "正在切换到 {}：{}\n目录: {}\nGeneration: {}",
                                submission
                                    .session
                                    .short_code
                                    .as_deref()
                                    .unwrap_or("[unknown]"),
                                submission.session.title,
                                workspace.display(),
                                submission.binding.generation
                            )),
                        )
                    } else {
                        (3, None)
                    }
                }
                Ok(Some(_)) | Ok(None) => (3, None),
                Err(error) => {
                    tracing::warn!(
                        interaction_id,
                        error = %error,
                        "QQ Session switch button was rejected"
                    );
                    (1, None)
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
        let protected_paths = [
            self.config_path.clone(),
            self.config.bridge.database_path.clone(),
        ];
        let classifier = Classifier::new(
            &self.config.policy,
            &self.config.bridge.workspace_roots,
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
            + i64::try_from(self.config.bridge.approval_ttl_seconds)
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
        let maximum_wait = Duration::from_secs(self.config.bridge.bridge_wait_seconds);
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
        let content = validate_agent_content(content, self.config.bridge.message_total_chars)
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
        let content = validate_agent_content(content, self.config.bridge.message_total_chars)
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
        if timeout_seconds == 0 || timeout_seconds > self.config.bridge.question_ttl_seconds {
            bail!(
                "timeout_seconds must be between 1 and {}",
                self.config.bridge.question_ttl_seconds
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
            "/ask" if !argument.is_empty() => {
                let content = if remainder.is_empty() {
                    argument.to_owned()
                } else {
                    format!("{argument} {remainder}")
                };
                self.queue_ahp_owner_message(message_id, &content)
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
                Ok(Some(format_ahp_sessions(
                    &self.config,
                    self.database.ahp_binding()?.as_ref(),
                    &self.database.ahp_list_sessions()?,
                )))
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
                if !ahp_session_matches_workspace(&self.config, &session) {
                    self.database
                        .mark_inbound_kind(message_id, "ahp_switch_forbidden")?;
                    return Ok(Some("该 Session 不属于配置的目标目录。".to_owned()));
                }
                let already_bound = self.database.ahp_binding()?.is_some_and(|binding| {
                    binding.endpoint_id == session.endpoint_id
                        && binding.session_uri == session.session_uri
                });
                if !already_bound && !ahp_session_is_idle(&session) {
                    self.database
                        .mark_inbound_kind(message_id, "ahp_switch_busy_target")?;
                    return Ok(Some(format!(
                        "目标 Session {} 当前正忙，请等待其空闲后再切换。",
                        session.short_code.as_deref().unwrap_or("[unknown]")
                    )));
                }
                match self
                    .database
                    .ahp_bind_session(&session.endpoint_id, &session.session_uri)
                {
                    Ok(binding) => {
                        self.database.mark_inbound_kind(message_id, "ahp_switch")?;
                        Ok(Some(format!(
                            "正在切换到 {}：{}\n目录: {}\nGeneration: {}",
                            session.short_code.as_deref().unwrap_or("[unknown]"),
                            session.title,
                            ahp_session_target_workspace(&self.config, &session)
                                .expect("filtered target workspace")
                                .display(),
                            binding.generation
                        )))
                    }
                    Err(error) => {
                        tracing::warn!(error = %error, "AHP Session switch rejected");
                        self.database
                            .mark_inbound_kind(message_id, "ahp_switch_rejected")?;
                        Ok(Some("当前绑定或目标 Session 正忙，暂不能切换。".to_owned()))
                    }
                }
            }
            "/status" if argument.is_empty() && remainder.is_empty() => {
                self.database.mark_inbound_kind(message_id, "ahp_status")?;
                Ok(Some(format_ahp_status(
                    &self
                        .database
                        .ahp_status(self.config.ahp.adapter_stale_seconds)?,
                )))
            }
            "/help" if argument.is_empty() && remainder.is_empty() => {
                self.database.mark_inbound_kind(message_id, "ahp_help")?;
                Ok(Some(ahp_help_text()))
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
        if !self.config.qq.voice_input_enabled {
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
        if !self.config.ahp.enabled {
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
        match self.database.ahp_enqueue_message(message_id, transcript) {
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
        match self.database.ahp_enqueue_message(message_id, content) {
            Ok(_) => {
                self.database.mark_inbound_kind(message_id, "ahp_message")?;
                Ok(Some("消息已进入共享会话队列。".to_owned()))
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

    async fn handle_ahp_event(&self, adapter_id: &str, event: &AhpPublishedEvent) -> Result<()> {
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
                    &format!("[PC]\n{}", data.content),
                )
                .await;
                if let Some(turn_id) = event.turn_id.as_deref() {
                    self.start_ahp_typing(turn_id).await;
                }
            }
            AhpEventKind::AssistantMessage => {
                let data: AhpTextEventData = serde_json::from_value(event.data.clone())?;
                if !data.complete || data.historical {
                    return Ok(());
                }
                self.stop_ahp_typing(event.turn_id.as_deref()).await;
                self.try_deliver_ahp_event_projection(
                    &event.event_id,
                    "ahp_assistant_message",
                    &data.content,
                )
                .await;
            }
            AhpEventKind::ToolStatus => {
                let data: AhpToolEventData = serde_json::from_value(event.data.clone())?;
                if !should_notify_tool(self.config.ahp.tool_notification_mode, &data.status) {
                    return Ok(());
                }
                self.try_deliver_ahp_event_projection(
                    &event.event_id,
                    "ahp_tool_status",
                    &format!(
                        "[工具 {}]\n状态: {}\n{}",
                        data.tool_name, data.status, data.summary
                    ),
                )
                .await;
                if let Some(turn_id) = event.turn_id.as_deref() {
                    self.start_ahp_typing(turn_id).await;
                }
            }
            AhpEventKind::ApprovalPending => {
                self.stop_ahp_typing(event.turn_id.as_deref()).await;
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
                        + i64::try_from(self.config.bridge.approval_ttl_seconds)?,
                })?;
                if approval.created {
                    self.send_ahp_approval_notification(&approval.record).await;
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
                            "审批 {} 已由 {} {}。",
                            record.short_code,
                            data.client_id.as_deref().unwrap_or("Agent Host"),
                            if data.approved { "批准" } else { "拒绝" }
                        ),
                    )
                    .await;
                    if let Some(turn_id) = event.turn_id.as_deref() {
                        self.start_ahp_typing(turn_id).await;
                    }
                }
            }
            AhpEventKind::InputPending => {
                self.stop_ahp_typing(event.turn_id.as_deref()).await;
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
                        + i64::try_from(self.config.bridge.question_ttl_seconds)?,
                })?;
                if input.created {
                    self.send_ahp_input_notification(&input.record).await;
                }
            }
            AhpEventKind::InputResolved => {
                let data: AhpInputResolvedData = serde_json::from_value(event.data.clone())?;
                if let Some(input) = self.database.ahp_resolve_input(
                    &data.input_key,
                    &data.outcome,
                    data.client_id.as_deref(),
                )? {
                    self.try_deliver_ahp_event_projection(
                        &event.event_id,
                        "ahp_input_resolved",
                        &format!("问题 {} 已由另一端处理。", input.short_code),
                    )
                    .await;
                    if let Some(turn_id) = event.turn_id.as_deref() {
                        self.start_ahp_typing(turn_id).await;
                    }
                }
            }
            AhpEventKind::TurnCancelled
            | AhpEventKind::TurnFailed
            | AhpEventKind::HostDisconnected => {
                self.stop_ahp_typing(event.turn_id.as_deref()).await;
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
                    data.summary.as_deref().unwrap_or(default_summary),
                )
                .await;
            }
            AhpEventKind::TurnStarted => {
                if let Some(turn_id) = event.turn_id.as_deref() {
                    self.start_ahp_typing(turn_id).await;
                }
            }
            AhpEventKind::TurnCompleted => {
                self.stop_ahp_typing(event.turn_id.as_deref()).await;
            }
            AhpEventKind::SessionSnapshot | AhpEventKind::ChatSnapshot => {}
        }
        Ok(())
    }

    async fn try_send_ahp_projection(&self, kind: &str, key: &str, content: &str) {
        if let Err(error) = self.send_if_owner(kind, None, key, content, None).await {
            tracing::warn!(kind, error = %error, "AHP event was stored but not delivered to QQ");
        }
    }

    async fn start_ahp_typing(&self, turn_id: &str) {
        if !self.config.ahp.typing_indicator_enabled {
            return;
        }
        let owner = match self.database.owner() {
            Ok(Some(owner)) if owner.enabled => owner,
            _ => return,
        };
        let mut tasks = self.typing_tasks.lock().await;
        for (_, task) in tasks.drain() {
            task.abort();
        }
        let qq = self.qq.clone();
        let user_openid = owner.user_openid;
        let duration = self.config.ahp.typing_duration_seconds;
        let refresh = self.config.ahp.typing_refresh_seconds;
        let task = tokio::spawn(async move {
            loop {
                if let Err(error) = qq.send_typing(&user_openid, duration).await {
                    tracing::debug!(error = %error, "QQ typing indicator stopped");
                    break;
                }
                sleep(Duration::from_secs(u64::from(refresh))).await;
            }
        });
        tasks.insert(turn_id.to_owned(), task);
    }

    async fn stop_ahp_typing(&self, turn_id: Option<&str>) {
        let mut tasks = self.typing_tasks.lock().await;
        if let Some(turn_id) = turn_id {
            if let Some(task) = tasks.remove(turn_id) {
                task.abort();
            }
        } else {
            for (_, task) in tasks.drain() {
                task.abort();
            }
        }
    }

    async fn try_deliver_ahp_event_projection(&self, event_id: &str, kind: &str, content: &str) {
        if let Err(error) = self.database.ahp_queue_projection(event_id, kind, content) {
            tracing::error!(event_id, error = %error, "failed to queue AHP projection");
            return;
        }
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

    fn pending_ahp_replay(&self, existing_chars: usize) -> (Option<String>, Vec<String>) {
        let pending = match self.database.ahp_pending_projections(20) {
            Ok(pending) => pending,
            Err(error) => {
                tracing::error!(error = %error, "failed to load pending AHP projections");
                return (None, Vec::new());
            }
        };
        if pending.is_empty() {
            return (None, Vec::new());
        }
        let mut selected = Vec::new();
        let mut content = String::from("[QQ 离线期间未实时送达]\n");
        for projection in pending {
            let section = format!("\n[{}]\n{}", projection.kind, projection.content.trim());
            if existing_chars + content.chars().count() + section.chars().count()
                > self.config.bridge.message_total_chars
            {
                break;
            }
            content.push_str(&section);
            selected.push(projection.event_id);
        }
        if selected.is_empty() {
            let projection = match self.database.ahp_pending_projections(1) {
                Ok(mut pending) => pending.pop(),
                Err(_) => None,
            };
            let Some(projection) = projection else {
                return (None, Vec::new());
            };
            let available = self
                .config
                .bridge
                .message_total_chars
                .saturating_sub(existing_chars)
                .saturating_sub(80);
            if available == 0 {
                return (None, Vec::new());
            }
            content = format!(
                "[QQ 离线期间未实时送达]\n[{}]\n{}",
                projection.kind,
                truncate_for_qq(&projection.content, available)
            );
            selected.push(projection.event_id);
        }
        (Some(content), selected)
    }

    async fn send_ahp_approval_notification(&self, approval: &AhpApprovalRecord) {
        if self.config.qq.approval_buttons_enabled {
            match self.send_ahp_approval_buttons(approval).await {
                Ok(()) => return,
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
            &format_ahp_approval(approval),
        )
        .await;
    }

    async fn send_ahp_approval_buttons(&self, approval: &AhpApprovalRecord) -> Result<()> {
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
            "## 工具{}审批 {}\n工具：{}\n{}\n\n按钮不可用时：`/allow {}` 或 `/deny {}`",
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

    async fn send_ahp_input_notification(&self, input: &AhpInputRecord) {
        if self.config.qq.approval_buttons_enabled
            && input.selection_mode == "single"
            && !input.buttons.is_empty()
        {
            match self.send_ahp_input_buttons(input).await {
                Ok(()) => return,
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
            &format_ahp_input(input),
        )
        .await;
    }

    async fn send_ahp_input_buttons(&self, input: &AhpInputRecord) -> Result<()> {
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
                        "## Agent 提问 {}\n{}\n\n{}\n\n按钮不可用时可直接回复选项文本。",
                        escape_qq_markdown(&input.short_code),
                        escape_qq_markdown(&input.prompt),
                        options
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

    async fn send_ahp_session_switch_menu(&self, message_id: &str) -> Result<Option<String>> {
        let binding = self.database.ahp_binding()?;
        let sessions: Vec<_> = self
            .database
            .ahp_list_sessions()?
            .into_iter()
            .filter(|session| ahp_session_matches_workspace(&self.config, session))
            .collect();
        if sessions.is_empty() {
            return Ok(Some("目标目录中没有可展示的 AHP Session。".to_owned()));
        }
        if sessions.len() > 100 {
            return Ok(Some(
                "目标目录中超过 100 个 Session，QQ 单条命令最多被动回复 4 次，无法安全展示全部按钮；请先归档旧 Session。"
                    .to_owned(),
            ));
        }
        let session_uris: Vec<_> = sessions
            .iter()
            .map(|session| session.session_uri.clone())
            .collect();
        let buttons = self.database.ahp_create_session_switch_buttons(
            &session_uris,
            self.config.bridge.question_ttl_seconds,
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
            let session_lines = page
                .iter()
                .map(|button| {
                    let current = binding
                        .as_ref()
                        .is_some_and(|binding| binding.session_uri == button.session.session_uri);
                    let state = if current {
                        "当前"
                    } else if ahp_session_is_idle(&button.session) {
                        "可切换"
                    } else {
                        "忙碌"
                    };
                    let workspace = ahp_session_target_workspace(&self.config, &button.session)
                        .expect("filtered target workspace");
                    format!(
                        "{} `{}` {} · {}",
                        state,
                        escape_qq_markdown(
                            button.session.short_code.as_deref().unwrap_or("[unknown]")
                        ),
                        escape_qq_markdown(&button.session.title),
                        escape_qq_markdown(&workspace.display().to_string())
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");
            let choices = page
                .iter()
                .map(|button| ChoiceButton {
                    label: button
                        .session
                        .short_code
                        .clone()
                        .unwrap_or_else(|| "Session".to_owned()),
                    button_data: button.button_data.clone(),
                })
                .collect();
            let receipt = self
                .qq
                .send_choice_buttons(
                    &owner.user_openid,
                    &ChoiceButtons {
                        markdown: format!(
                            "## 切换 AHP Session（{}/{page_count}）\n{session_lines}\n\n当前绑定和目标 Session 均空闲时可切换；按钮有效期 {} 秒。",
                            page_index + 1,
                            self.config.bridge.question_ttl_seconds
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
        if safe_content.chars().count() > self.config.bridge.message_total_chars {
            bail!("QQ message exceeds the configured per-delivery length limit");
        }
        let chunks = split_message(&safe_content, self.config.bridge.message_chunk_chars);
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
        if minute_count.saturating_add(chunk_count) > self.config.bridge.messages_per_minute
            || day_count.saturating_add(chunk_count) > self.config.bridge.messages_per_day
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
            .config
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

    fn require_ahp_enabled(&self) -> Result<()> {
        if !self.config.ahp.enabled {
            bail!("AHP mode is disabled");
        }
        Ok(())
    }

    fn validate_ahp_session_workspace(&self, endpoint_id: &str, session_uri: &str) -> Result<()> {
        validate_identifier("endpoint_id", endpoint_id)?;
        validate_identifier("session_uri", session_uri)?;
        if self.config.ahp.shared_workspaces.is_empty() {
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
        if ahp_session_target_workspace(&self.config, &session).is_none() {
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
) -> Result<()> {
    if hosts.len() > 32 || sessions.len() > 2_000 {
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
    mode == AhpToolNotificationMode::Full || matches!(status, "completed" | "cancelled")
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

fn format_ahp_status(status: &AhpStatus) -> String {
    let adapter = status.adapter.as_ref().map_or_else(
        || "未连接".to_owned(),
        |adapter| format!("{} ({})", adapter.state, adapter.version),
    );
    let binding = status.binding.as_ref().map_or_else(
        || "未绑定".to_owned(),
        |binding| {
            format!(
                "{} / generation {} / {}",
                binding.state, binding.generation, binding.session_uri
            )
        },
    );
    format!(
        "AHP Adapter: {adapter}\n共享 Session: {binding}\n可见 Host: {}\n可见 Session: {}\n待处理命令: {}\nQQ 待补发事件: {}",
        status.hosts.len(),
        status.sessions.len(),
        status.pending_commands,
        status.pending_projections
    )
}

fn format_ahp_approval(approval: &AhpApprovalRecord) -> String {
    format!(
        "[工具{}审批 {}]\n工具: {}\n{}\n\n批准一次: /allow {}\n拒绝: /deny {}",
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
    let mut characters = value.chars();
    let truncated: String = characters.by_ref().take(maximum).collect();
    if characters.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

fn format_ahp_input(input: &AhpInputRecord) -> String {
    let choices = if input.choices.is_empty() {
        String::new()
    } else {
        format!("\n选项: {}", input.choices.join(" / "))
    };
    format!(
        "[Agent 提问 {}]\n{}{}\n\n直接回复可回答当前问题；或使用 /answer {} <文本>。",
        input.short_code, input.prompt, choices, input.short_code
    )
}

fn ahp_help_text() -> String {
    [
        "AHP 双端共享会话命令:",
        "普通文本：发送到共享对话；若 Agent 正等待澄清，则优先作为回答",
        "QQ 语音：使用内置 ASR；优先回答澄清，否则发送到共享对话；不会执行控制命令",
        "/ask <文本>：即使存在澄清问题，也把文本排队为新消息",
        "/sessions：列出所有目标目录内的 Session",
        "/switch：显示 Session 切换按钮",
        "/switch <编号>：文本兜底，仅在当前绑定和目标 Session 均空闲时切换",
        "/allow <审批码>：单次批准",
        "/deny <审批码>：拒绝",
        "/answer <问题码> <文本>：显式回答 Agent 问题",
        "/cancel：取消当前 Turn",
        "/status",
        "/help",
    ]
    .join("\n")
}

fn format_ahp_sessions(
    config: &AppConfig,
    binding: Option<&crate::protocol::AhpBindingRecord>,
    sessions: &[AhpSessionDescriptor],
) -> String {
    let visible: Vec<_> = sessions
        .iter()
        .filter(|session| ahp_session_matches_workspace(config, session))
        .collect();
    if visible.is_empty() {
        return "目标目录中没有可绑定的 AHP Session。".to_owned();
    }
    let lines = visible
        .into_iter()
        .map(|session| {
            let current = binding.is_some_and(|binding| binding.session_uri == session.session_uri);
            let state = if current {
                "当前"
            } else if ahp_session_is_idle(session) {
                "可切换"
            } else {
                "忙碌"
            };
            let workspace =
                ahp_session_target_workspace(config, session).expect("filtered target workspace");
            format!(
                "{} {} | {} | {} | {}",
                if current { "*" } else { " " },
                session.short_code.as_deref().unwrap_or("[unknown]"),
                session.title,
                workspace.display(),
                state
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!("AHP Sessions:\n{lines}\n\n发送 /switch 显示按钮。")
}

fn ahp_session_matches_workspace(config: &AppConfig, session: &AhpSessionDescriptor) -> bool {
    ahp_session_target_workspace(config, session).is_some()
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

    #[test]
    fn split_message_preserves_unicode_and_limits_chunks() {
        let content = "你好世界\nabcdef";
        let chunks = split_message(content, 4);
        assert_eq!(chunks.concat(), content);
        assert!(chunks.iter().all(|chunk| chunk.chars().count() <= 4));
    }

    #[test]
    fn compact_tool_notifications_emit_only_terminal_states() {
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
            .send_ahp_approval_notification(&approval.record)
            .await;
        let messages = fixture.qq.messages().await;
        assert_eq!(
            messages.last().expect("approval message").kind,
            "approval_buttons"
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
            .send_ahp_input_notification(&input.record)
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
        let binding = fixture
            .service
            .database()
            .ahp_binding()
            .expect("binding")
            .expect("bound");
        assert_eq!(binding.session_uri, "copilot:/session-2");
        assert_eq!(binding.state, "binding");
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

    struct Fixture {
        _directory: tempfile::TempDir,
        workspace: PathBuf,
        other_workspace: Option<PathBuf>,
        service: Arc<BridgeService>,
        qq: Arc<MockQqMessenger>,
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
            Self::new_ahp_with_voice_input(false)
        }

        fn new_ahp_multi_workspace() -> Self {
            Self::new_ahp_with_options(false, true)
        }

        fn new_ahp_with_voice_input(voice_input_enabled: bool) -> Self {
            Self::new_ahp_with_options(voice_input_enabled, false)
        }

        fn new_ahp_with_options(voice_input_enabled: bool, multi_workspace: bool) -> Self {
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
            config
                .ahp
                .shared_workspaces
                .extend(other_workspace.iter().cloned());
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
                )
                .expect("ack bind");
            database
                .ahp_binding_ready(
                    "adapter-stable",
                    "adapter-run-1",
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
