use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::IPC_PROTOCOL_VERSION;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcEnvelope {
    pub version: u32,
    pub request_id: Uuid,
    pub auth_token: String,
    pub request: BridgeRequest,
}

impl RpcEnvelope {
    pub fn new(auth_token: String, request: BridgeRequest) -> Self {
        Self {
            version: IPC_PROTOCOL_VERSION,
            request_id: Uuid::new_v4(),
            auth_token,
            request,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum BridgeRequest {
    Health,
    RegisterPrompt {
        session_id: String,
        cwd: PathBuf,
    },
    PreTool {
        session_id: String,
        tool_use_id: String,
        tool_name: String,
        tool_input: Value,
        cwd: PathBuf,
    },
    PostTool {
        session_id: String,
        tool_use_id: String,
        tool_name: String,
    },
    Stop {
        session_id: String,
        stop_hook_active: bool,
    },
    SendProgress {
        session_id: Option<String>,
        session_label: Option<String>,
        content: String,
    },
    SendFinal {
        session_id: Option<String>,
        session_label: Option<String>,
        content: String,
        idempotency_key: String,
    },
    AskUser {
        session_id: Option<String>,
        session_label: Option<String>,
        question: String,
        choices: Vec<String>,
        allow_freeform: bool,
        timeout_seconds: u64,
    },
    WaitForMessage {
        session_id: Option<String>,
        session_label: Option<String>,
        timeout_seconds: u64,
    },
    Status,
    NewBindCode,
    SetEmergencyEnabled {
        enabled: bool,
    },
    AhpAdapterRegister {
        registration: AhpAdapterRegistration,
    },
    AhpCatalogReplace {
        adapter_id: String,
        adapter_instance_id: String,
        hosts: Vec<AhpHostDescriptor>,
        sessions: Vec<AhpSessionDescriptor>,
    },
    AhpBindingReady {
        adapter_id: String,
        adapter_instance_id: String,
        binding_id: String,
        endpoint_id: String,
        host_instance_id: String,
        binding_generation: i64,
        session_uri: String,
        chat_uri: String,
        last_server_sequence: u64,
    },
    AhpBindingFailed {
        adapter_id: String,
        adapter_instance_id: String,
        binding_id: String,
        binding_generation: i64,
        reason_code: String,
    },
    AhpPublishEvents {
        adapter_id: String,
        adapter_instance_id: String,
        binding_id: String,
        binding_generation: i64,
        events: Vec<AhpPublishedEvent>,
    },
    AhpPollCommands {
        adapter_id: String,
        adapter_instance_id: String,
        timeout_seconds: u64,
    },
    AhpAckCommand {
        adapter_id: String,
        adapter_instance_id: String,
        command_id: i64,
        outcome: AhpCommandOutcome,
        error_code: Option<String>,
    },
    AhpSessions,
    AhpBindSession {
        endpoint_id: String,
        session_uri: String,
    },
    AhpUnbindSession,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AhpAdapterRegistration {
    pub adapter_id: String,
    pub adapter_instance_id: String,
    pub version: String,
    pub supported_protocols: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AhpHostDescriptor {
    pub endpoint_id: String,
    pub host_instance_id: String,
    pub pid: u32,
    pub advertised_protocol: String,
    pub selected_protocol: Option<String>,
    pub state: AhpHostState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AhpHostState {
    Connected,
    ReadOnly,
    Incompatible,
    Unreachable,
}

impl AhpHostState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Connected => "connected",
            Self::ReadOnly => "read_only",
            Self::Incompatible => "incompatible",
            Self::Unreachable => "unreachable",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AhpSessionDescriptor {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub short_code: Option<String>,
    pub endpoint_id: String,
    pub host_instance_id: String,
    pub session_uri: String,
    pub provider: String,
    pub title: String,
    pub status: u32,
    pub workspace_uris: Vec<String>,
    pub created_at: String,
    pub modified_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AhpPublishedEvent {
    pub event_id: String,
    pub host_instance_id: String,
    pub server_sequence: Option<u64>,
    pub session_uri: String,
    pub chat_uri: Option<String>,
    pub turn_id: Option<String>,
    pub kind: AhpEventKind,
    pub origin_client_id: Option<String>,
    pub occurred_at: String,
    pub data: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AhpEventKind {
    SessionSnapshot,
    ChatSnapshot,
    UserMessage,
    AssistantMessage,
    ToolStatus,
    ApprovalPending,
    ApprovalResolved,
    InputPending,
    InputResolved,
    TurnStarted,
    TurnCompleted,
    TurnCancelled,
    TurnFailed,
    HostDisconnected,
}

impl AhpEventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SessionSnapshot => "session_snapshot",
            Self::ChatSnapshot => "chat_snapshot",
            Self::UserMessage => "user_message",
            Self::AssistantMessage => "assistant_message",
            Self::ToolStatus => "tool_status",
            Self::ApprovalPending => "approval_pending",
            Self::ApprovalResolved => "approval_resolved",
            Self::InputPending => "input_pending",
            Self::InputResolved => "input_resolved",
            Self::TurnStarted => "turn_started",
            Self::TurnCompleted => "turn_completed",
            Self::TurnCancelled => "turn_cancelled",
            Self::TurnFailed => "turn_failed",
            Self::HostDisconnected => "host_disconnected",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AhpAdapterCommand {
    pub command_id: i64,
    pub command_key: String,
    pub binding_id: String,
    pub binding_generation: i64,
    pub kind: AhpCommandKind,
    pub data: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AhpCommandKind {
    BindSession,
    UnbindSession,
    SendMessage,
    CancelTurn,
    ApproveTool,
    ReviewToolResult,
    CompleteInput,
}

impl AhpCommandKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BindSession => "bind_session",
            Self::UnbindSession => "unbind_session",
            Self::SendMessage => "send_message",
            Self::CancelTurn => "cancel_turn",
            Self::ApproveTool => "approve_tool",
            Self::ReviewToolResult => "review_tool_result",
            Self::CompleteInput => "complete_input",
        }
    }
}

impl TryFrom<&str> for AhpCommandKind {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "bind_session" => Ok(Self::BindSession),
            "unbind_session" => Ok(Self::UnbindSession),
            "send_message" => Ok(Self::SendMessage),
            "cancel_turn" => Ok(Self::CancelTurn),
            "approve_tool" => Ok(Self::ApproveTool),
            "review_tool_result" => Ok(Self::ReviewToolResult),
            "complete_input" => Ok(Self::CompleteInput),
            other => Err(format!("unknown AHP command kind {other}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AhpCommandOutcome {
    Applied,
    Rejected,
    Failed,
}

impl AhpCommandOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "acked",
            Self::Rejected => "rejected",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AhpBindingRecord {
    pub binding_id: String,
    pub generation: i64,
    pub endpoint_id: String,
    pub host_instance_id: Option<String>,
    pub session_uri: String,
    pub chat_uri: Option<String>,
    pub state: String,
    pub last_server_sequence: u64,
    pub active_turn_id: Option<String>,
    pub queued_message_count: u32,
    pub last_activity_at: i64,
    pub foreground: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AhpCommandPollResult {
    pub commands: Vec<AhpAdapterCommand>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcResponse {
    pub version: u32,
    pub request_id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

impl RpcResponse {
    pub fn ok<T: Serialize>(request_id: Uuid, result: T) -> Self {
        match serde_json::to_value(result) {
            Ok(result) => Self {
                version: IPC_PROTOCOL_VERSION,
                request_id,
                result: Some(result),
                error: None,
            },
            Err(error) => Self::error(
                request_id,
                "serialization_error",
                format!("failed to serialize response: {error}"),
            ),
        }
    }

    pub fn error(request_id: Uuid, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            version: IPC_PROTOCOL_VERSION,
            request_id,
            result: None,
            error: Some(RpcError {
                code: code.into(),
                message: message.into(),
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcError {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionDecision {
    Allow,
    Deny,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PermissionResult {
    pub decision: PermissionDecision,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopDecision {
    Allow,
    Block,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopResult {
    pub decision: StopDecision,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalState {
    Pending,
    Allowed,
    Denied,
    Expired,
    Completed,
    UnknownFailure,
}

impl ApprovalState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Allowed => "allowed",
            Self::Denied => "denied",
            Self::Expired => "expired",
            Self::Completed => "completed",
            Self::UnknownFailure => "unknown_failure",
        }
    }
}

impl TryFrom<&str> for ApprovalState {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "pending" => Ok(Self::Pending),
            "allowed" => Ok(Self::Allowed),
            "denied" => Ok(Self::Denied),
            "expired" => Ok(Self::Expired),
            "completed" => Ok(Self::Completed),
            "unknown_failure" => Ok(Self::UnknownFailure),
            other => Err(format!("unknown approval state {other}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskLevel {
    Low,
    Medium,
    High,
    Forbidden,
}

impl RiskLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Forbidden => "forbidden",
        }
    }
}
