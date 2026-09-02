use std::collections::HashSet;

use anyhow::{Context, Result, bail};
use rusqlite::{Connection, Error as SqliteError, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    db::Database,
    protocol::{
        AhpAdapterCommand, AhpAdapterRegistration, AhpBindingRecord, AhpCommandKind,
        AhpCommandOutcome, AhpHostDescriptor, AhpPublishedEvent, AhpSessionDescriptor,
        AhpTargetKind,
    },
    security::{canonical_json, random_code, redact_json, sha256_hex},
};

const MAX_COMMAND_BATCH: usize = 32;
pub const MAX_TRACKED_AHP_SESSIONS: usize = 5;

#[derive(Debug, Clone, Serialize)]
pub struct AhpAdapterStatus {
    pub adapter_id: String,
    pub adapter_instance_id: String,
    pub version: String,
    pub state: String,
    pub last_seen_at: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct AhpStatus {
    pub adapter: Option<AhpAdapterStatus>,
    pub binding: Option<AhpBindingRecord>,
    pub bindings: Vec<AhpBindingRecord>,
    pub foreground_binding_id: Option<String>,
    pub hosts: Vec<AhpHostDescriptor>,
    pub sessions: Vec<AhpSessionDescriptor>,
    pub pending_commands: u32,
    pub pending_approvals: u32,
    pub pending_inputs: u32,
    pub pending_projections: u32,
    pub creation: Option<AhpCreationWizardStatus>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AhpRegisterResult {
    pub bindings: Vec<AhpBindingRecord>,
    pub foreground_binding_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct NewAhpApproval {
    pub approval_key: String,
    pub stage: String,
    pub session_uri: String,
    pub chat_uri: String,
    pub turn_id: String,
    pub tool_call_id: String,
    pub tool_name: String,
    pub summary: String,
    pub expires_at: i64,
}

#[derive(Debug, Clone)]
pub struct AhpApprovalRecord {
    pub approval_key: String,
    pub short_code: String,
    pub stage: String,
    pub session_uri: String,
    pub chat_uri: String,
    pub turn_id: String,
    pub tool_call_id: String,
    pub tool_name: String,
    pub summary: String,
    pub state: String,
    pub allow_button_data: String,
    pub deny_button_data: String,
    pub expires_at: i64,
}

#[derive(Debug, Clone)]
pub struct BeginAhpApproval {
    pub record: AhpApprovalRecord,
    pub created: bool,
}

#[derive(Debug, Clone)]
pub struct AhpApprovalSubmission {
    pub record: AhpApprovalRecord,
    pub accepted: bool,
    pub approved: bool,
}

#[derive(Debug, Clone)]
pub struct NewAhpInput {
    pub input_key: String,
    pub session_uri: String,
    pub chat_uri: String,
    pub request_id: String,
    pub prompt: String,
    pub choices: Vec<String>,
    pub allow_freeform: bool,
    pub selection_mode: String,
    pub expires_at: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct AhpInputButton {
    pub label: String,
    #[serde(skip_serializing)]
    pub button_data: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AhpInputRecord {
    pub input_key: String,
    pub short_code: String,
    pub session_uri: String,
    pub chat_uri: String,
    pub request_id: String,
    pub prompt: String,
    pub choices: Vec<String>,
    pub allow_freeform: bool,
    pub selection_mode: String,
    pub buttons: Vec<AhpInputButton>,
    pub state: String,
    pub expires_at: i64,
}

#[derive(Debug, Clone)]
pub struct BeginAhpInput {
    pub record: AhpInputRecord,
    pub created: bool,
}

#[derive(Debug, Clone)]
pub struct AhpInputSubmission {
    pub record: AhpInputRecord,
    pub accepted: bool,
    pub answer: String,
}

#[derive(Debug, Clone)]
pub struct AhpInputResolution {
    pub record: AhpInputRecord,
    pub transitioned: bool,
    pub decided_by_surface: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AhpSessionSwitchButton {
    pub button_data: String,
    pub session: AhpSessionDescriptor,
}

#[derive(Debug, Clone)]
pub struct AhpSessionSwitchSubmission {
    pub session: AhpSessionDescriptor,
    pub binding: AhpBindingRecord,
    pub accepted: bool,
}

#[derive(Debug, Clone)]
pub struct AhpProjectionRecord {
    pub event_id: String,
    pub kind: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AhpCommandProgressRecord {
    pub progress: u64,
    pub total: Option<u64>,
    pub message: Option<String>,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AhpCommandStatusRecord {
    pub command_id: i64,
    pub kind: AhpCommandKind,
    pub state: String,
    pub error_code: Option<String>,
    pub result: Option<Value>,
    pub progress: Option<AhpCommandProgressRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AhpCreationWizardRecord {
    pub wizard_id: String,
    pub mode: String,
    pub state: String,
    pub context: Option<Value>,
    pub pending_task: Option<String>,
    pub create_command_id: Option<i64>,
    pub new_session_uri: Option<String>,
    pub old_binding_endpoint_id: Option<String>,
    pub old_binding_session_uri: Option<String>,
    pub old_binding_host_instance_id: Option<String>,
    pub cancel_requested: bool,
    pub expires_at: i64,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AhpCreationWizardStatus {
    pub wizard_id: String,
    pub mode: String,
    pub state: String,
    pub expires_at: i64,
    pub cancel_requested: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AhpWizardButtonRecord {
    pub button_data: String,
    pub wizard_id: String,
    pub action_kind: String,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AhpTrustRequestRecord {
    pub request_id: String,
    pub workspace_uri: String,
    pub open_trust_ui: bool,
    pub trusted: bool,
    pub expires_at: i64,
}

pub(crate) fn initialize_schema(connection: &Connection) -> Result<()> {
    connection
        .execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS ahp_adapter (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                adapter_id TEXT NOT NULL,
                adapter_instance_id TEXT NOT NULL,
                version TEXT NOT NULL,
                supported_protocols_json TEXT NOT NULL,
                state TEXT NOT NULL CHECK (state IN ('connected', 'stale', 'stopped')),
                last_seen_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ahp_hosts (
                endpoint_id TEXT PRIMARY KEY,
                host_instance_id TEXT NOT NULL,
                pid INTEGER NOT NULL,
                advertised_protocol TEXT NOT NULL,
                selected_protocol TEXT,
                state TEXT NOT NULL CHECK (
                    state IN ('connected', 'read_only', 'incompatible', 'unreachable')
                ),
                host_label TEXT,
                ssh_alias TEXT,
                target_kind TEXT CHECK (target_kind IN ('local', 'ssh')),
                target_path TEXT,
                endpoint_type TEXT,
                editor_client_tools_available INTEGER
                    NOT NULL DEFAULT 1 CHECK (editor_client_tools_available IN (0, 1)),
                last_seen_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ahp_session_catalog (
                session_uri TEXT PRIMARY KEY,
                short_code TEXT NOT NULL UNIQUE,
                endpoint_id TEXT NOT NULL REFERENCES ahp_hosts(endpoint_id) ON DELETE CASCADE,
                host_instance_id TEXT NOT NULL,
                provider TEXT NOT NULL,
                title TEXT NOT NULL,
                status INTEGER NOT NULL,
                workspace_uris_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                modified_at TEXT NOT NULL,
                host_label TEXT,
                ssh_alias TEXT,
                target_kind TEXT CHECK (target_kind IN ('local', 'ssh')),
                target_path TEXT,
                editor_client_tools_available INTEGER
                    NOT NULL DEFAULT 1 CHECK (editor_client_tools_available IN (0, 1)),
                available INTEGER NOT NULL DEFAULT 1 CHECK (available IN (0, 1)),
                last_seen_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_ahp_sessions_endpoint
                ON ahp_session_catalog(endpoint_id, modified_at);

            CREATE TABLE IF NOT EXISTS ahp_bindings (
                binding_id TEXT PRIMARY KEY,
                generation INTEGER NOT NULL,
                endpoint_id TEXT NOT NULL,
                host_instance_id TEXT,
                session_uri TEXT NOT NULL UNIQUE,
                chat_uri TEXT,
                state TEXT NOT NULL CHECK (
                    state IN ('binding', 'bound', 'detaching', 'lost', 'failed')
                ),
                last_server_sequence INTEGER NOT NULL DEFAULT 0,
                active_turn_id TEXT,
                queued_message_count INTEGER NOT NULL DEFAULT 0,
                error_code TEXT,
                last_activity_at INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_ahp_bindings_activity
                ON ahp_bindings(state, last_activity_at);

            CREATE TABLE IF NOT EXISTS ahp_foreground_binding (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                binding_id TEXT NOT NULL
                    REFERENCES ahp_bindings(binding_id) ON DELETE CASCADE,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ahp_commands (
                command_id INTEGER PRIMARY KEY AUTOINCREMENT,
                command_key TEXT NOT NULL UNIQUE,
                binding_id TEXT NOT NULL,
                binding_generation INTEGER NOT NULL,
                kind TEXT NOT NULL CHECK (
                    kind IN (
                        'bind_session', 'unbind_session', 'send_message', 'cancel_turn',
                        'approve_tool', 'review_tool_result', 'complete_input',
                        'prepare_target', 'create_session', 'dispose_session'
                    )
                ),
                data_json TEXT NOT NULL,
                state TEXT NOT NULL CHECK (
                    state IN ('pending', 'leased', 'acked', 'rejected', 'failed')
                ),
                lease_owner TEXT,
                lease_expires_at INTEGER,
                error_code TEXT,
                result_json TEXT,
                progress_json TEXT,
                progress_updated_at INTEGER,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_ahp_commands_ready
                ON ahp_commands(state, lease_expires_at, command_id);

            CREATE TABLE IF NOT EXISTS ahp_events (
                event_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                host_instance_id TEXT NOT NULL,
                binding_id TEXT NOT NULL,
                binding_generation INTEGER NOT NULL,
                server_sequence INTEGER,
                session_uri TEXT NOT NULL,
                chat_uri TEXT,
                turn_id TEXT,
                kind TEXT NOT NULL,
                origin_client_id TEXT,
                occurred_at TEXT NOT NULL,
                data_redacted_json TEXT NOT NULL,
                processed_at INTEGER,
                processing_error TEXT,
                created_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_ahp_events_binding_sequence
                ON ahp_events(binding_generation, event_sequence);
            CREATE INDEX IF NOT EXISTS idx_ahp_events_created
                ON ahp_events(created_at);

            CREATE TABLE IF NOT EXISTS ahp_approvals (
                approval_key TEXT PRIMARY KEY,
                short_code TEXT NOT NULL UNIQUE,
                stage TEXT NOT NULL CHECK (stage IN ('parameter', 'result')),
                session_uri TEXT NOT NULL,
                chat_uri TEXT NOT NULL,
                turn_id TEXT NOT NULL,
                tool_call_id TEXT NOT NULL,
                tool_name TEXT NOT NULL,
                summary TEXT NOT NULL,
                state TEXT NOT NULL CHECK (
                    state IN ('pending', 'submitted', 'allowed', 'denied', 'expired', 'failed')
                ),
                allow_button_data TEXT NOT NULL UNIQUE,
                deny_button_data TEXT NOT NULL UNIQUE,
                command_id INTEGER REFERENCES ahp_commands(command_id),
                decided_by_surface TEXT,
                decided_by_message_id TEXT,
                expires_at INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_ahp_approvals_state
                ON ahp_approvals(state, expires_at);

            CREATE TABLE IF NOT EXISTS ahp_inputs (
                input_key TEXT PRIMARY KEY,
                short_code TEXT NOT NULL UNIQUE,
                session_uri TEXT NOT NULL,
                chat_uri TEXT NOT NULL,
                request_id TEXT NOT NULL,
                prompt TEXT NOT NULL,
                choices_json TEXT NOT NULL,
                allow_freeform INTEGER NOT NULL CHECK (allow_freeform IN (0, 1)),
                selection_mode TEXT NOT NULL DEFAULT 'none' CHECK (
                    selection_mode IN ('none', 'single', 'multi')
                ),
                state TEXT NOT NULL CHECK (
                    state IN ('pending', 'submitted', 'answered', 'declined', 'cancelled', 'expired', 'failed')
                ),
                command_id INTEGER REFERENCES ahp_commands(command_id),
                decided_by_surface TEXT,
                decided_by_message_id TEXT,
                expires_at INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_ahp_inputs_state
                ON ahp_inputs(state, expires_at);

            CREATE TABLE IF NOT EXISTS ahp_input_buttons (
                button_data TEXT PRIMARY KEY,
                input_key TEXT NOT NULL REFERENCES ahp_inputs(input_key) ON DELETE CASCADE,
                label TEXT NOT NULL,
                position INTEGER NOT NULL,
                UNIQUE(input_key, position)
            );

            CREATE TABLE IF NOT EXISTS ahp_session_switch_buttons (
                button_data TEXT PRIMARY KEY,
                group_id TEXT NOT NULL,
                session_uri TEXT NOT NULL
                    REFERENCES ahp_session_catalog(session_uri) ON DELETE CASCADE,
                workspace_uris_json TEXT NOT NULL,
                expires_at INTEGER NOT NULL,
                used_at INTEGER,
                created_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_ahp_session_switch_expiry
                ON ahp_session_switch_buttons(expires_at, used_at);

            CREATE TABLE IF NOT EXISTS qq_interactions (
                interaction_id TEXT PRIMARY KEY,
                outer_event_id TEXT NOT NULL,
                user_openid TEXT NOT NULL,
                button_data_hash TEXT NOT NULL,
                ack_code INTEGER,
                created_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ahp_projections (
                event_id TEXT PRIMARY KEY REFERENCES ahp_events(event_id) ON DELETE CASCADE,
                kind TEXT NOT NULL,
                content TEXT NOT NULL,
                state TEXT NOT NULL CHECK (state IN ('pending', 'delivered')),
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_ahp_projections_pending
                ON ahp_projections(state, created_at);

            CREATE TABLE IF NOT EXISTS ahp_creation_wizard (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                wizard_id TEXT NOT NULL,
                mode TEXT NOT NULL CHECK (mode IN ('quick', 'advanced')),
                state TEXT NOT NULL CHECK (
                    state IN (
                        'select_target', 'select_model', 'select_approval',
                        'await_task', 'creating', 'cancelled'
                    )
                ),
                context_json TEXT,
                pending_task TEXT,
                create_command_id INTEGER REFERENCES ahp_commands(command_id),
                new_session_uri TEXT,
                old_binding_endpoint_id TEXT,
                old_binding_session_uri TEXT,
                old_binding_host_instance_id TEXT,
                cancel_requested INTEGER NOT NULL DEFAULT 0 CHECK (cancel_requested IN (0, 1)),
                expires_at INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ahp_creation_buttons (
                button_data TEXT PRIMARY KEY,
                wizard_id TEXT NOT NULL,
                action_kind TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                expires_at INTEGER NOT NULL,
                used_at INTEGER,
                created_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_ahp_creation_buttons_expiry
                ON ahp_creation_buttons(expires_at, used_at);

            CREATE TABLE IF NOT EXISTS ahp_trust_requests (
                request_id TEXT PRIMARY KEY,
                workspace_uri TEXT NOT NULL,
                open_trust_ui INTEGER NOT NULL CHECK (open_trust_ui IN (0, 1)),
                trusted INTEGER NOT NULL DEFAULT 0 CHECK (trusted IN (0, 1)),
                expires_at INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_ahp_trust_requests_pending
                ON ahp_trust_requests(expires_at, trusted);
            "#,
        )
        .context("failed to initialize AHP database schema")?;
    migrate_ahp_schema(connection)
}

fn migrate_ahp_schema(connection: &Connection) -> Result<()> {
    if table_exists(connection, "ahp_binding")? {
        ensure_column(
            connection,
            "ahp_binding",
            "active_turn_id",
            "ALTER TABLE ahp_binding ADD COLUMN active_turn_id TEXT",
        )?;
        ensure_column(
            connection,
            "ahp_binding",
            "queued_message_count",
            "ALTER TABLE ahp_binding ADD COLUMN queued_message_count INTEGER NOT NULL DEFAULT 0",
        )?;
    }
    ensure_column(
        connection,
        "ahp_commands",
        "binding_id",
        "ALTER TABLE ahp_commands ADD COLUMN binding_id TEXT",
    )?;
    ensure_column(
        connection,
        "ahp_events",
        "binding_id",
        "ALTER TABLE ahp_events ADD COLUMN binding_id TEXT",
    )?;
    ensure_column(
        connection,
        "ahp_session_switch_buttons",
        "group_id",
        "ALTER TABLE ahp_session_switch_buttons ADD COLUMN group_id TEXT NOT NULL DEFAULT ''",
    )?;
    migrate_legacy_binding(connection)?;
    ensure_column(
        connection,
        "ahp_events",
        "processed_at",
        "ALTER TABLE ahp_events ADD COLUMN processed_at INTEGER",
    )?;
    ensure_column(
        connection,
        "ahp_session_catalog",
        "available",
        "ALTER TABLE ahp_session_catalog ADD COLUMN available INTEGER NOT NULL DEFAULT 1",
    )?;
    ensure_column(
        connection,
        "ahp_session_catalog",
        "short_code",
        "ALTER TABLE ahp_session_catalog ADD COLUMN short_code TEXT NOT NULL DEFAULT ''",
    )?;
    ensure_column(
        connection,
        "ahp_inputs",
        "selection_mode",
        "ALTER TABLE ahp_inputs ADD COLUMN selection_mode TEXT NOT NULL DEFAULT 'none'",
    )?;
    ensure_column(
        connection,
        "ahp_events",
        "processing_error",
        "ALTER TABLE ahp_events ADD COLUMN processing_error TEXT",
    )?;
    ensure_column(
        connection,
        "ahp_hosts",
        "host_label",
        "ALTER TABLE ahp_hosts ADD COLUMN host_label TEXT",
    )?;
    ensure_column(
        connection,
        "ahp_hosts",
        "ssh_alias",
        "ALTER TABLE ahp_hosts ADD COLUMN ssh_alias TEXT",
    )?;
    ensure_column(
        connection,
        "ahp_hosts",
        "target_kind",
        "ALTER TABLE ahp_hosts ADD COLUMN target_kind TEXT",
    )?;
    ensure_column(
        connection,
        "ahp_hosts",
        "target_path",
        "ALTER TABLE ahp_hosts ADD COLUMN target_path TEXT",
    )?;
    ensure_column(
        connection,
        "ahp_hosts",
        "endpoint_type",
        "ALTER TABLE ahp_hosts ADD COLUMN endpoint_type TEXT",
    )?;
    ensure_column(
        connection,
        "ahp_hosts",
        "editor_client_tools_available",
        "ALTER TABLE ahp_hosts ADD COLUMN editor_client_tools_available INTEGER NOT NULL DEFAULT 1",
    )?;
    ensure_column(
        connection,
        "ahp_session_catalog",
        "host_label",
        "ALTER TABLE ahp_session_catalog ADD COLUMN host_label TEXT",
    )?;
    ensure_column(
        connection,
        "ahp_session_catalog",
        "ssh_alias",
        "ALTER TABLE ahp_session_catalog ADD COLUMN ssh_alias TEXT",
    )?;
    ensure_column(
        connection,
        "ahp_session_catalog",
        "target_kind",
        "ALTER TABLE ahp_session_catalog ADD COLUMN target_kind TEXT",
    )?;
    ensure_column(
        connection,
        "ahp_session_catalog",
        "target_path",
        "ALTER TABLE ahp_session_catalog ADD COLUMN target_path TEXT",
    )?;
    ensure_column(
        connection,
        "ahp_session_catalog",
        "editor_client_tools_available",
        "ALTER TABLE ahp_session_catalog ADD COLUMN editor_client_tools_available INTEGER NOT NULL DEFAULT 1",
    )?;
    ensure_column(
        connection,
        "ahp_approvals",
        "allow_button_data",
        "ALTER TABLE ahp_approvals ADD COLUMN allow_button_data TEXT NOT NULL DEFAULT ''",
    )?;
    ensure_column(
        connection,
        "ahp_approvals",
        "deny_button_data",
        "ALTER TABLE ahp_approvals ADD COLUMN deny_button_data TEXT NOT NULL DEFAULT ''",
    )?;
    ensure_column(
        connection,
        "ahp_commands",
        "result_json",
        "ALTER TABLE ahp_commands ADD COLUMN result_json TEXT",
    )?;
    ensure_column(
        connection,
        "ahp_commands",
        "progress_json",
        "ALTER TABLE ahp_commands ADD COLUMN progress_json TEXT",
    )?;
    ensure_column(
        connection,
        "ahp_commands",
        "progress_updated_at",
        "ALTER TABLE ahp_commands ADD COLUMN progress_updated_at INTEGER",
    )?;
    migrate_ahp_command_kinds(connection)?;
    let mut missing = {
        let mut statement = connection.prepare(
            "SELECT approval_key FROM ahp_approvals
             WHERE allow_button_data = '' OR deny_button_data = ''",
        )?;
        statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };
    missing.sort();
    for approval_key in missing {
        connection.execute(
            "UPDATE ahp_approvals
             SET allow_button_data = ?1, deny_button_data = ?2
             WHERE approval_key = ?3",
            params![
                format!("appr_{}", random_code(None, 20)),
                format!("rejt_{}", random_code(None, 20)),
                approval_key
            ],
        )?;
    }
    let mut sessions_without_codes = {
        let mut statement = connection
            .prepare("SELECT session_uri FROM ahp_session_catalog WHERE short_code = ''")?;
        statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };
    sessions_without_codes.sort();
    for session_uri in sessions_without_codes {
        set_unique_session_code(connection, &session_uri)?;
    }
    connection.execute_batch(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_ahp_approvals_allow_button
             ON ahp_approvals(allow_button_data);
         CREATE UNIQUE INDEX IF NOT EXISTS idx_ahp_approvals_deny_button
             ON ahp_approvals(deny_button_data);
         CREATE UNIQUE INDEX IF NOT EXISTS idx_ahp_sessions_short_code
             ON ahp_session_catalog(short_code);
         CREATE INDEX IF NOT EXISTS idx_ahp_commands_binding
             ON ahp_commands(binding_id, state, command_id);
         CREATE INDEX IF NOT EXISTS idx_ahp_events_binding_id_sequence
             ON ahp_events(binding_id, event_sequence);",
    )?;
    Ok(())
}

fn table_exists(connection: &Connection, table: &str) -> Result<bool> {
    connection
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [table],
            |_| Ok(()),
        )
        .optional()
        .map(|row| row.is_some())
        .context("failed to inspect SQLite schema")
}

fn migrate_legacy_binding(connection: &Connection) -> Result<()> {
    if !table_exists(connection, "ahp_binding")? {
        return Ok(());
    }
    let legacy = connection
        .query_row(
            "SELECT generation, endpoint_id, host_instance_id, session_uri, chat_uri,
                    state, last_server_sequence, active_turn_id, queued_message_count,
                    error_code, updated_at
             FROM ahp_binding WHERE singleton = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, i64>(8)?,
                    row.get::<_, Option<String>>(9)?,
                    row.get::<_, i64>(10)?,
                ))
            },
        )
        .optional()?;
    if let Some((
        generation,
        endpoint_id,
        host_instance_id,
        session_uri,
        chat_uri,
        state,
        last_server_sequence,
        active_turn_id,
        queued_message_count,
        error_code,
        updated_at,
    )) = legacy
    {
        let binding_id = format!("legacy-{}", &sha256_hex(session_uri.as_bytes())[..24]);
        connection.execute(
            "INSERT OR IGNORE INTO ahp_bindings(
                binding_id, generation, endpoint_id, host_instance_id, session_uri, chat_uri,
                state, last_server_sequence, active_turn_id, queued_message_count, error_code,
                last_activity_at, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?12, ?12)",
            params![
                binding_id,
                generation,
                endpoint_id,
                host_instance_id,
                session_uri,
                chat_uri,
                state,
                last_server_sequence,
                active_turn_id,
                queued_message_count,
                error_code,
                updated_at
            ],
        )?;
        let migrated_binding_id: String = connection.query_row(
            "SELECT binding_id FROM ahp_bindings WHERE session_uri = ?1",
            [&session_uri],
            |row| row.get(0),
        )?;
        connection.execute(
            "INSERT INTO ahp_foreground_binding(singleton, binding_id, updated_at)
             VALUES (1, ?1, ?2)
             ON CONFLICT(singleton) DO UPDATE SET
                binding_id = excluded.binding_id, updated_at = excluded.updated_at",
            params![migrated_binding_id, updated_at],
        )?;
        connection.execute(
            "UPDATE ahp_commands SET binding_id = ?1
             WHERE binding_id IS NULL AND binding_generation = ?2",
            params![migrated_binding_id, generation],
        )?;
        connection.execute(
            "UPDATE ahp_events SET binding_id = ?1
             WHERE binding_id IS NULL AND binding_generation = ?2",
            params![migrated_binding_id, generation],
        )?;
    }
    connection.execute(
        "UPDATE ahp_commands
         SET state = 'failed', error_code = 'binding_migration_unresolved', updated_at = ?1
         WHERE binding_id IS NULL AND state IN ('pending', 'leased')",
        [now()],
    )?;
    connection.execute("DROP TABLE ahp_binding", [])?;
    Ok(())
}

fn migrate_ahp_command_kinds(connection: &Connection) -> Result<()> {
    let schema: String = connection.query_row(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'ahp_commands'",
        [],
        |row| row.get(0),
    )?;
    if schema.contains("'prepare_target'")
        && schema.contains("'create_session'")
        && schema.contains("'dispose_session'")
    {
        return Ok(());
    }

    let foreign_keys_enabled =
        connection.query_row("PRAGMA foreign_keys", [], |row| row.get::<_, i64>(0))? != 0;
    if foreign_keys_enabled {
        connection.execute_batch("PRAGMA foreign_keys = OFF")?;
    }
    let migration = connection.execute_batch(
        r#"
        BEGIN IMMEDIATE;
        DROP TABLE IF EXISTS ahp_commands_new;
        CREATE TABLE ahp_commands_new (
            command_id INTEGER PRIMARY KEY AUTOINCREMENT,
            command_key TEXT NOT NULL UNIQUE,
            binding_id TEXT NOT NULL,
            binding_generation INTEGER NOT NULL,
            kind TEXT NOT NULL CHECK (
                kind IN (
                    'bind_session', 'unbind_session', 'send_message', 'cancel_turn',
                    'approve_tool', 'review_tool_result', 'complete_input',
                    'prepare_target', 'create_session', 'dispose_session'
                )
            ),
            data_json TEXT NOT NULL,
            state TEXT NOT NULL CHECK (
                state IN ('pending', 'leased', 'acked', 'rejected', 'failed')
            ),
            lease_owner TEXT,
            lease_expires_at INTEGER,
            error_code TEXT,
            result_json TEXT,
            progress_json TEXT,
            progress_updated_at INTEGER,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );
        INSERT INTO ahp_commands_new(
            command_id, command_key, binding_id, binding_generation, kind, data_json, state,
            lease_owner, lease_expires_at, error_code, result_json, progress_json,
            progress_updated_at, created_at, updated_at
        )
        SELECT
            command_id, command_key, COALESCE(binding_id, ''), binding_generation, kind,
            data_json, state,
            lease_owner, lease_expires_at, error_code, result_json, progress_json,
            progress_updated_at, created_at, updated_at
        FROM ahp_commands;
        DROP TABLE ahp_commands;
        ALTER TABLE ahp_commands_new RENAME TO ahp_commands;
        CREATE INDEX idx_ahp_commands_ready
            ON ahp_commands(state, lease_expires_at, command_id);
        COMMIT;
        "#,
    );
    if migration.is_err() {
        let _ = connection.execute_batch("ROLLBACK");
    }
    let restore_foreign_keys = if foreign_keys_enabled {
        connection.execute_batch("PRAGMA foreign_keys = ON")
    } else {
        Ok(())
    };
    migration.context("failed to migrate AHP command kinds")?;
    restore_foreign_keys.context("failed to restore SQLite foreign key enforcement")?;
    Ok(())
}

fn ensure_column(
    connection: &Connection,
    table: &str,
    column: &str,
    statement: &str,
) -> Result<()> {
    let pragma = format!("PRAGMA table_info({table})");
    let exists = {
        let mut query = connection.prepare(&pragma)?;
        query
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?
            .iter()
            .any(|existing| existing == column)
    };
    if !exists {
        connection.execute(statement, [])?;
    }
    Ok(())
}

impl Database {
    pub fn ahp_register_adapter(
        &self,
        registration: &AhpAdapterRegistration,
    ) -> Result<AhpRegisterResult> {
        let now = now();
        let protocols = serde_json::to_string(&registration.supported_protocols)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let previous: Option<(String, String)> = transaction
            .query_row(
                "SELECT adapter_id, adapter_instance_id FROM ahp_adapter WHERE singleton = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((adapter_id, adapter_instance_id)) = previous
            && (adapter_id != registration.adapter_id
                || adapter_instance_id != registration.adapter_instance_id)
        {
            transaction.execute(
                "UPDATE ahp_commands
                 SET state = 'pending', lease_owner = NULL, lease_expires_at = NULL,
                     updated_at = ?1
                 WHERE state = 'leased'",
                [now],
            )?;
        }
        transaction.execute(
            "INSERT INTO ahp_adapter(
                singleton, adapter_id, adapter_instance_id, version,
                supported_protocols_json, state, last_seen_at, updated_at
             ) VALUES (1, ?1, ?2, ?3, ?4, 'connected', ?5, ?5)
             ON CONFLICT(singleton) DO UPDATE SET
                adapter_id = excluded.adapter_id,
                adapter_instance_id = excluded.adapter_instance_id,
                version = excluded.version,
                supported_protocols_json = excluded.supported_protocols_json,
                state = 'connected',
                last_seen_at = excluded.last_seen_at,
                updated_at = excluded.updated_at",
            params![
                registration.adapter_id,
                registration.adapter_instance_id,
                registration.version,
                protocols,
                now
            ],
        )?;
        transaction.commit()?;
        drop(connection);
        Ok(AhpRegisterResult {
            bindings: self.ahp_bindings()?,
            foreground_binding_id: self.ahp_binding()?.map(|binding| binding.binding_id),
        })
    }

    pub fn ahp_replace_catalog(
        &self,
        adapter_id: &str,
        adapter_instance_id: &str,
        hosts: &[AhpHostDescriptor],
        sessions: &[AhpSessionDescriptor],
    ) -> Result<()> {
        self.ahp_replace_catalog_scoped(adapter_id, adapter_instance_id, hosts, sessions, true)
    }

    pub fn ahp_replace_catalog_scoped(
        &self,
        adapter_id: &str,
        adapter_instance_id: &str,
        hosts: &[AhpHostDescriptor],
        sessions: &[AhpSessionDescriptor],
        full_snapshot: bool,
    ) -> Result<()> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        require_adapter(&transaction, adapter_id, adapter_instance_id)?;
        if full_snapshot {
            transaction.execute("UPDATE ahp_session_catalog SET available = 0", [])?;
            transaction.execute("UPDATE ahp_hosts SET state = 'unreachable'", [])?;
        } else {
            for endpoint_id in hosts.iter().map(|host| host.endpoint_id.as_str()) {
                transaction.execute(
                    "UPDATE ahp_session_catalog SET available = 0 WHERE endpoint_id = ?1",
                    [endpoint_id],
                )?;
            }
        }
        for host in hosts {
            transaction.execute(
                "INSERT INTO ahp_hosts(
                    endpoint_id, host_instance_id, pid, advertised_protocol,
                    selected_protocol, state, host_label, ssh_alias, target_kind,
                    target_path, endpoint_type, editor_client_tools_available, last_seen_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
                 ON CONFLICT(endpoint_id) DO UPDATE SET
                    host_instance_id = excluded.host_instance_id,
                    pid = excluded.pid,
                    advertised_protocol = excluded.advertised_protocol,
                    selected_protocol = excluded.selected_protocol,
                    state = excluded.state,
                    host_label = excluded.host_label,
                    ssh_alias = excluded.ssh_alias,
                    target_kind = excluded.target_kind,
                    target_path = excluded.target_path,
                    endpoint_type = excluded.endpoint_type,
                    editor_client_tools_available = excluded.editor_client_tools_available,
                    last_seen_at = excluded.last_seen_at",
                params![
                    host.endpoint_id,
                    host.host_instance_id,
                    i64::from(host.pid),
                    host.advertised_protocol,
                    host.selected_protocol.as_deref(),
                    host.state.as_str(),
                    host.host_label.as_deref(),
                    host.ssh_alias.as_deref(),
                    host.target_kind.map(|kind| match kind {
                        AhpTargetKind::Local => "local",
                        AhpTargetKind::Ssh => "ssh",
                    }),
                    host.target_path.as_deref(),
                    host.endpoint_type.as_deref(),
                    if host.editor_client_tools_available.unwrap_or(true) {
                        1_i64
                    } else {
                        0_i64
                    },
                    now
                ],
            )?;
        }
        for session in sessions {
            if !hosts.iter().any(|host| {
                host.endpoint_id == session.endpoint_id
                    && host.host_instance_id == session.host_instance_id
            }) {
                bail!("AHP session references an unknown host");
            }
            upsert_catalog_session(&transaction, session, now)?;
        }
        fail_bindings_for_host_changes(&transaction, hosts, full_snapshot, now)?;
        transaction.execute(
            "UPDATE ahp_adapter SET last_seen_at = ?1, updated_at = ?1, state = 'connected'
             WHERE singleton = 1",
            [now],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn ahp_list_sessions(&self) -> Result<Vec<AhpSessionDescriptor>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT endpoint_id, host_instance_id, session_uri, provider, title,
                    status, workspace_uris_json, created_at, modified_at, short_code,
                    host_label, ssh_alias, target_kind, target_path,
                    editor_client_tools_available, NULL, NULL
             FROM ahp_session_catalog
             WHERE available = 1 OR target_kind IS NOT NULL
             ORDER BY modified_at DESC",
        )?;
        statement
            .query_map([], map_session_descriptor)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("failed to list AHP sessions")
    }

    pub fn ahp_session_by_code(&self, short_code: &str) -> Result<Option<AhpSessionDescriptor>> {
        self.connection()?
            .query_row(
                "SELECT endpoint_id, host_instance_id, session_uri, provider, title,
                        status, workspace_uris_json, created_at, modified_at, short_code,
                        host_label, ssh_alias, target_kind, target_path,
                        editor_client_tools_available, NULL, NULL
                 FROM ahp_session_catalog
                 WHERE short_code = ?1 AND (available = 1 OR target_kind IS NOT NULL)",
                [short_code.to_ascii_uppercase()],
                map_session_descriptor,
            )
            .optional()
            .context("failed to query AHP Session code")
    }

    pub fn ahp_session_by_uri(&self, session_uri: &str) -> Result<Option<AhpSessionDescriptor>> {
        let connection = self.connection()?;
        session_by_uri(&connection, session_uri)
    }

    pub fn ahp_session_is_available(&self, session_uri: &str) -> Result<bool> {
        self.connection()?
            .query_row(
                "SELECT available FROM ahp_session_catalog WHERE session_uri = ?1",
                [session_uri],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map(|available| available.is_some_and(|value| value != 0))
            .context("failed to query AHP Session availability")
    }

    pub fn ahp_create_session_switch_buttons(
        &self,
        session_uris: &[String],
        ttl_seconds: u64,
    ) -> Result<Vec<AhpSessionSwitchButton>> {
        if session_uris.is_empty() {
            return Ok(Vec::new());
        }
        if session_uris.len() > 2_000 {
            bail!("too many AHP Sessions for a switch menu");
        }
        let now = now();
        let expires_at =
            now + i64::try_from(ttl_seconds).context("Session switch TTL exceeds range")?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "DELETE FROM ahp_session_switch_buttons WHERE expires_at <= ?1",
            [now],
        )?;
        let group_id = Uuid::new_v4().to_string();
        let mut buttons = Vec::with_capacity(session_uris.len());
        let mut seen = HashSet::new();
        for session_uri in session_uris {
            if !seen.insert(session_uri) {
                continue;
            }
            let session = session_by_uri(&transaction, session_uri)?
                .context("AHP Session disappeared while creating switch menu")?;
            let button_data = allocate_session_switch_token(&transaction)?;
            transaction.execute(
                "INSERT INTO ahp_session_switch_buttons(
                    button_data, group_id, session_uri, workspace_uris_json, expires_at, created_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    button_data,
                    group_id,
                    session_uri,
                    serde_json::to_string(&session.workspace_uris)?,
                    expires_at,
                    now
                ],
            )?;
            buttons.push(AhpSessionSwitchButton {
                button_data,
                session,
            });
        }
        transaction.commit()?;
        Ok(buttons)
    }

    pub fn ahp_switch_session_by_button(
        &self,
        button_data: &str,
        allowed_session_uris: &[String],
    ) -> Result<Option<AhpSessionSwitchSubmission>> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let token: Option<(String, String, String, String, i64, Option<i64>)> = transaction
            .query_row(
                "SELECT b.group_id, b.session_uri, b.workspace_uris_json, s.workspace_uris_json,
                        b.expires_at, b.used_at
                 FROM ahp_session_switch_buttons b
                 JOIN ahp_session_catalog s ON s.session_uri = b.session_uri
                 WHERE b.button_data = ?1
                   AND (s.available = 1 OR s.target_kind IS NOT NULL)",
                [button_data],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .optional()?;
        let Some((
            group_id,
            session_uri,
            menu_workspace_uris,
            current_workspace_uris,
            expires_at,
            used_at,
        )) = token
        else {
            return Ok(None);
        };
        let session = session_by_uri(&transaction, &session_uri)?
            .context("AHP Session for switch button is no longer available")?;
        if !allowed_session_uris
            .iter()
            .any(|allowed| allowed == &session.session_uri)
        {
            transaction.execute(
                "UPDATE ahp_session_switch_buttons
                 SET used_at = ?1 WHERE button_data = ?2 AND used_at IS NULL",
                params![now, button_data],
            )?;
            transaction.commit()?;
            return Ok(None);
        }
        if used_at.is_some() || expires_at <= now || menu_workspace_uris != current_workspace_uris {
            let binding = binding_by_session(&transaction, &session_uri)?
                .or(binding_from_connection(&transaction)?)
                .context("no AHP Session is currently tracked")?;
            return Ok(Some(AhpSessionSwitchSubmission {
                session,
                binding,
                accepted: false,
            }));
        }
        let binding = focus_session_transaction(
            &transaction,
            &session.endpoint_id,
            &session.session_uri,
            MAX_TRACKED_AHP_SESSIONS,
            now,
        )?;
        transaction.execute(
            "UPDATE ahp_session_switch_buttons
             SET used_at = ?1 WHERE group_id = ?2 AND used_at IS NULL",
            params![now, group_id],
        )?;
        transaction.commit()?;
        Ok(Some(AhpSessionSwitchSubmission {
            session,
            binding,
            accepted: true,
        }))
    }

    pub fn ahp_consume_session_switch_button(
        &self,
        button_data: &str,
        allowed_session_uris: &[String],
    ) -> Result<Option<AhpSessionDescriptor>> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let token: Option<(String, String, String, String, i64, Option<i64>)> = transaction
            .query_row(
                "SELECT b.group_id, b.session_uri, b.workspace_uris_json, s.workspace_uris_json,
                        b.expires_at, b.used_at
                 FROM ahp_session_switch_buttons b
                 JOIN ahp_session_catalog s ON s.session_uri = b.session_uri
                 WHERE b.button_data = ?1
                   AND (s.available = 1 OR s.target_kind IS NOT NULL)",
                [button_data],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .optional()?;
        let Some((
            group_id,
            session_uri,
            menu_workspace_uris,
            current_workspace_uris,
            expires_at,
            used_at,
        )) = token
        else {
            return Ok(None);
        };
        let session = session_by_uri(&transaction, &session_uri)?
            .context("AHP Session for switch button is no longer available")?;
        if !allowed_session_uris
            .iter()
            .any(|allowed| allowed == &session.session_uri)
        {
            transaction.execute(
                "UPDATE ahp_session_switch_buttons
                 SET used_at = ?1 WHERE button_data = ?2 AND used_at IS NULL",
                params![now, button_data],
            )?;
            transaction.commit()?;
            return Ok(None);
        }
        if used_at.is_some() || expires_at <= now || menu_workspace_uris != current_workspace_uris {
            return Ok(None);
        }
        transaction.execute(
            "UPDATE ahp_session_switch_buttons
             SET used_at = ?1 WHERE group_id = ?2 AND used_at IS NULL",
            params![now, group_id],
        )?;
        transaction.commit()?;
        Ok(Some(session))
    }

    pub fn ahp_bind_session(
        &self,
        endpoint_id: &str,
        session_uri: &str,
    ) -> Result<AhpBindingRecord> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let binding = focus_session_transaction(
            &transaction,
            endpoint_id,
            session_uri,
            MAX_TRACKED_AHP_SESSIONS,
            now,
        )?;
        transaction.commit()?;
        Ok(binding)
    }

    pub fn ahp_track_session(
        &self,
        endpoint_id: &str,
        session_uri: &str,
    ) -> Result<AhpBindingRecord> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let binding = ensure_session_binding(
            &transaction,
            endpoint_id,
            session_uri,
            MAX_TRACKED_AHP_SESSIONS,
            false,
            now,
        )?;
        ensure_foreground_binding(&transaction, &binding.binding_id, now)?;
        transaction.commit()?;
        Ok(binding)
    }

    pub fn ahp_detach_session(&self, session_uri: &str) -> Result<bool> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(binding) = binding_by_session(&transaction, session_uri)? else {
            return Ok(false);
        };
        detach_binding(&transaction, &binding, now, "manual_detach")?;
        select_replacement_foreground(&transaction, now)?;
        transaction.commit()?;
        Ok(true)
    }

    pub fn ahp_unbind_session(&self) -> Result<bool> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let binding = binding_from_connection(&transaction)?;
        let Some(binding) = binding else {
            return Ok(false);
        };
        detach_binding(&transaction, &binding, now, "manual_unbind")?;
        select_replacement_foreground(&transaction, now)?;
        transaction.commit()?;
        Ok(true)
    }

    pub fn ahp_binding(&self) -> Result<Option<AhpBindingRecord>> {
        let connection = self.connection()?;
        binding_from_connection(&connection)
    }

    pub fn ahp_bindings(&self) -> Result<Vec<AhpBindingRecord>> {
        let connection = self.connection()?;
        bindings_from_connection(&connection)
    }

    pub fn ahp_binding_for_session(&self, session_uri: &str) -> Result<Option<AhpBindingRecord>> {
        let connection = self.connection()?;
        binding_by_session(&connection, session_uri)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn ahp_binding_ready(
        &self,
        adapter_id: &str,
        adapter_instance_id: &str,
        binding_id: &str,
        endpoint_id: &str,
        host_instance_id: &str,
        binding_generation: i64,
        session_uri: &str,
        chat_uri: &str,
        last_server_sequence: u64,
    ) -> Result<()> {
        let sequence =
            i64::try_from(last_server_sequence).context("AHP server sequence exceeds SQLite")?;
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        require_adapter(&transaction, adapter_id, adapter_instance_id)?;
        let changed = transaction.execute(
            "UPDATE ahp_bindings
             SET state = 'bound', chat_uri = ?1,
                 last_server_sequence = MAX(last_server_sequence, ?2),
                 error_code = NULL, updated_at = ?3
             WHERE binding_id = ?4 AND generation = ?5 AND endpoint_id = ?6
               AND host_instance_id = ?7 AND session_uri = ?8
               AND state IN ('binding', 'bound')",
            params![
                chat_uri,
                sequence,
                now,
                binding_id,
                binding_generation,
                endpoint_id,
                host_instance_id,
                session_uri
            ],
        )?;
        if changed != 1 {
            bail!("AHP binding acknowledgement is stale or mismatched");
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn ahp_binding_failed(
        &self,
        adapter_id: &str,
        adapter_instance_id: &str,
        binding_id: &str,
        binding_generation: i64,
        reason_code: &str,
    ) -> Result<()> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        require_adapter(&transaction, adapter_id, adapter_instance_id)?;
        let changed = transaction.execute(
            "UPDATE ahp_bindings
             SET state = 'failed', error_code = ?1, updated_at = ?2
             WHERE binding_id = ?3 AND generation = ?4",
            params![reason_code, now, binding_id, binding_generation],
        )?;
        if changed != 1 {
            bail!("AHP binding failure is stale");
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn ahp_publish_events(
        &self,
        adapter_id: &str,
        adapter_instance_id: &str,
        binding_id: &str,
        binding_generation: i64,
        events: &[AhpPublishedEvent],
    ) -> Result<Vec<String>> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        require_adapter(&transaction, adapter_id, adapter_instance_id)?;
        let binding = binding_by_id(&transaction, binding_id)?
            .context("AHP event batch targets an unknown binding")?;
        if binding.generation != binding_generation
            || !matches!(binding.state.as_str(), "bound" | "detaching")
        {
            bail!("AHP event batch targets a stale or inactive binding");
        }
        let mut inserted = Vec::new();
        for event in events {
            if event.session_uri != binding.session_uri {
                bail!("AHP event targets a different session");
            }
            if binding.host_instance_id.as_deref() != Some(event.host_instance_id.as_str()) {
                bail!("AHP event targets a stale Host instance");
            }
            if let Some(event_chat_uri) = event.chat_uri.as_deref()
                && binding.chat_uri.as_deref() != Some(event_chat_uri)
            {
                bail!("AHP event targets a different Chat");
            }
            let data = canonical_json(&redact_json(&event.data));
            let server_sequence = event
                .server_sequence
                .map(i64::try_from)
                .transpose()
                .context("AHP server sequence exceeds SQLite")?;
            let changed = transaction.execute(
                "INSERT OR IGNORE INTO ahp_events(
                    event_id, host_instance_id, binding_id, binding_generation, server_sequence,
                    session_uri, chat_uri, turn_id, kind, origin_client_id,
                    occurred_at, data_redacted_json, created_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                params![
                    event.event_id,
                    event.host_instance_id,
                    binding_id,
                    binding_generation,
                    server_sequence,
                    event.session_uri,
                    event.chat_uri,
                    event.turn_id,
                    event.kind.as_str(),
                    event.origin_client_id,
                    event.occurred_at,
                    data,
                    now
                ],
            )?;
            if changed == 1 {
                inserted.push(event.event_id.clone());
            } else {
                let unprocessed = transaction
                    .query_row(
                        "SELECT 1 FROM ahp_events
                         WHERE event_id = ?1 AND processed_at IS NULL",
                        [&event.event_id],
                        |_| Ok(()),
                    )
                    .optional()?
                    .is_some();
                if unprocessed {
                    inserted.push(event.event_id.clone());
                }
            }
            match event.kind {
                crate::protocol::AhpEventKind::ChatSnapshot => {
                    let active_turn_id = event.data.get("active_turn_id").and_then(Value::as_str);
                    let queued_message_count = event
                        .data
                        .get("queued_message_count")
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
                    transaction.execute(
                        "UPDATE ahp_bindings
                         SET active_turn_id = ?1, queued_message_count = ?2, updated_at = ?3
                         WHERE binding_id = ?4 AND generation = ?5",
                        params![
                            active_turn_id,
                            i64::try_from(queued_message_count)?,
                            now,
                            binding_id,
                            binding_generation
                        ],
                    )?;
                }
                crate::protocol::AhpEventKind::TurnStarted => {
                    transaction.execute(
                        "UPDATE ahp_bindings
                         SET active_turn_id = ?1, last_activity_at = ?2, updated_at = ?2
                         WHERE binding_id = ?3 AND generation = ?4",
                        params![event.turn_id, now, binding_id, binding_generation],
                    )?;
                }
                crate::protocol::AhpEventKind::TurnCompleted
                | crate::protocol::AhpEventKind::TurnCancelled
                | crate::protocol::AhpEventKind::TurnFailed => {
                    transaction.execute(
                        "UPDATE ahp_bindings
                         SET active_turn_id = NULL, last_activity_at = ?1, updated_at = ?1
                         WHERE binding_id = ?2 AND generation = ?3
                          AND (?4 IS NULL OR active_turn_id = ?4)",
                        params![now, binding_id, binding_generation, event.turn_id],
                    )?;
                }
                _ => {}
            }
        }
        if let Some(sequence) = events
            .iter()
            .filter_map(|event| event.server_sequence)
            .max()
        {
            transaction.execute(
                "UPDATE ahp_bindings
                 SET last_server_sequence = MAX(last_server_sequence, ?1), updated_at = ?2
                 WHERE binding_id = ?3 AND generation = ?4",
                params![
                    i64::try_from(sequence)?,
                    now,
                    binding_id,
                    binding_generation
                ],
            )?;
        }
        transaction.execute(
            "UPDATE ahp_adapter SET last_seen_at = ?1, updated_at = ?1
             WHERE singleton = 1",
            [now],
        )?;
        transaction.commit()?;
        Ok(inserted)
    }

    pub fn ahp_mark_event_processed(&self, event_id: &str) -> Result<()> {
        let connection = self.connection()?;
        let changed = connection.execute(
            "UPDATE ahp_events
             SET processed_at = ?1, processing_error = NULL
             WHERE event_id = ?2",
            params![now(), event_id],
        )?;
        if changed != 1 {
            bail!("AHP event not found while marking it processed");
        }
        Ok(())
    }

    pub fn ahp_enqueue_message(&self, message_id: &str, content: &str) -> Result<i64> {
        let binding = self
            .ahp_binding()?
            .context("no foreground AHP session is selected")?;
        self.ahp_enqueue_message_to_session(message_id, &binding.session_uri, content)
    }

    pub fn ahp_enqueue_message_to_session(
        &self,
        message_id: &str,
        session_uri: &str,
        content: &str,
    ) -> Result<i64> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let binding = require_routable_binding_for_session(&transaction, session_uri)?;
        let command_id = enqueue_command(
            &transaction,
            &format!("qq-message:{message_id}"),
            &binding.binding_id,
            binding.generation,
            AhpCommandKind::SendMessage,
            &json!({
                "client_message_id": message_id,
                "session_uri": binding.session_uri,
                "chat_uri": binding.chat_uri,
                "content": content,
            }),
            now,
        )?;
        touch_binding(&transaction, &binding.binding_id, now)?;
        transaction.commit()?;
        Ok(command_id)
    }

    pub fn ahp_enqueue_cancel(&self, message_id: &str) -> Result<i64> {
        let binding = self
            .ahp_binding()?
            .context("no foreground AHP session is selected")?;
        self.ahp_enqueue_cancel_for_session(message_id, &binding.session_uri)
    }

    pub fn ahp_enqueue_cancel_for_session(
        &self,
        message_id: &str,
        session_uri: &str,
    ) -> Result<i64> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let binding = require_bound_binding_for_session(&transaction, session_uri)?;
        let command_id = enqueue_command(
            &transaction,
            &format!("qq-cancel:{message_id}"),
            &binding.binding_id,
            binding.generation,
            AhpCommandKind::CancelTurn,
            &json!({
                "session_uri": binding.session_uri,
                "chat_uri": binding.chat_uri,
            }),
            now,
        )?;
        touch_binding(&transaction, &binding.binding_id, now)?;
        transaction.commit()?;
        Ok(command_id)
    }

    pub fn ahp_enqueue_prepare_target(&self, command_key: &str, data: &Value) -> Result<i64> {
        self.ahp_enqueue_unbound_command(command_key, AhpCommandKind::PrepareTarget, data)
    }

    pub fn ahp_enqueue_create_session(&self, command_key: &str, data: &Value) -> Result<i64> {
        self.ahp_enqueue_unbound_command(command_key, AhpCommandKind::CreateSession, data)
    }

    pub fn ahp_enqueue_dispose_session(&self, command_key: &str, data: &Value) -> Result<i64> {
        self.ahp_enqueue_unbound_command(command_key, AhpCommandKind::DisposeSession, data)
    }

    fn ahp_enqueue_unbound_command(
        &self,
        command_key: &str,
        kind: AhpCommandKind,
        data: &Value,
    ) -> Result<i64> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let command_id =
            enqueue_command(&transaction, command_key, "__unbound__", 0, kind, data, now)?;
        transaction.commit()?;
        Ok(command_id)
    }

    pub fn ahp_begin_approval(&self, approval: &NewAhpApproval) -> Result<BeginAhpApproval> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        expire_ahp_interactions(&transaction, now)?;
        if let Some(record) = approval_by_key(&transaction, &approval.approval_key)? {
            transaction.commit()?;
            return Ok(BeginAhpApproval {
                record,
                created: false,
            });
        }
        let short_code = insert_approval_with_unique_code(&transaction, approval, now)?;
        transaction.commit()?;
        drop(connection);
        Ok(BeginAhpApproval {
            record: self
                .ahp_approval_by_code(&short_code)?
                .context("AHP approval disappeared after insert")?,
            created: true,
        })
    }

    pub fn ahp_approval_by_code(&self, short_code: &str) -> Result<Option<AhpApprovalRecord>> {
        self.connection()?
            .query_row(
                "SELECT approval_key, short_code, stage, session_uri, chat_uri,
                        turn_id, tool_call_id, tool_name, summary, state,
                        allow_button_data, deny_button_data, expires_at
                 FROM ahp_approvals WHERE short_code = ?1",
                [short_code.to_ascii_uppercase()],
                map_ahp_approval,
            )
            .optional()
            .context("failed to query AHP approval")
    }

    pub fn ahp_submit_approval(
        &self,
        short_code: &str,
        approved: bool,
        message_id: &str,
    ) -> Result<Option<AhpApprovalSubmission>> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        expire_ahp_interactions(&transaction, now)?;
        let record = transaction
            .query_row(
                "SELECT approval_key, short_code, stage, session_uri, chat_uri,
                        turn_id, tool_call_id, tool_name, summary, state,
                        allow_button_data, deny_button_data, expires_at
                 FROM ahp_approvals WHERE short_code = ?1",
                [short_code.to_ascii_uppercase()],
                map_ahp_approval,
            )
            .optional()?;
        let Some(record) = record else {
            return Ok(None);
        };
        let accepted =
            submit_approval_record(&transaction, &record, approved, message_id, "qq_text", now)?;
        transaction.commit()?;
        drop(connection);
        let record = self
            .ahp_approval_by_code(&record.short_code)?
            .context("AHP approval disappeared after submission")?;
        Ok(Some(AhpApprovalSubmission {
            record,
            accepted,
            approved,
        }))
    }

    pub fn ahp_submit_approval_by_button(
        &self,
        button_data: &str,
        expected_approved: bool,
        interaction_id: &str,
    ) -> Result<Option<AhpApprovalSubmission>> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        expire_ahp_interactions(&transaction, now)?;
        let record = transaction
            .query_row(
                "SELECT approval_key, short_code, stage, session_uri, chat_uri,
                        turn_id, tool_call_id, tool_name, summary, state,
                        allow_button_data, deny_button_data, expires_at
                 FROM ahp_approvals
                 WHERE allow_button_data = ?1 OR deny_button_data = ?1",
                [button_data],
                map_ahp_approval,
            )
            .optional()?;
        let Some(record) = record else {
            return Ok(None);
        };
        let approved = record.allow_button_data == button_data;
        if approved != expected_approved {
            bail!("QQ button ID does not match approval action data");
        }
        let accepted = submit_approval_record(
            &transaction,
            &record,
            approved,
            interaction_id,
            "qq_button",
            now,
        )?;
        transaction.commit()?;
        drop(connection);
        let record = self
            .ahp_approval_by_code(&record.short_code)?
            .context("AHP approval disappeared after button submission")?;
        Ok(Some(AhpApprovalSubmission {
            record,
            accepted,
            approved,
        }))
    }

    pub fn ahp_resolve_approval(
        &self,
        approval_key: &str,
        approved: bool,
        client_id: Option<&str>,
    ) -> Result<Option<AhpApprovalRecord>> {
        let now = now();
        let connection = self.connection()?;
        connection.execute(
            "UPDATE ahp_approvals
             SET state = ?1,
                 decided_by_surface = COALESCE(decided_by_surface, ?2),
                 updated_at = ?3
             WHERE approval_key = ?4 AND state IN ('pending', 'submitted')",
            params![
                if approved { "allowed" } else { "denied" },
                client_id,
                now,
                approval_key
            ],
        )?;
        approval_by_key(&connection, approval_key)
    }

    pub fn ahp_begin_input(&self, input: &NewAhpInput) -> Result<BeginAhpInput> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        expire_ahp_interactions(&transaction, now)?;
        if let Some(record) = input_by_key(&transaction, &input.input_key)? {
            transaction.commit()?;
            return Ok(BeginAhpInput {
                record,
                created: false,
            });
        }
        let short_code = insert_input_with_unique_code(&transaction, input, now)?;
        transaction.commit()?;
        drop(connection);
        Ok(BeginAhpInput {
            record: self
                .ahp_input_by_code(&short_code)?
                .context("AHP input request disappeared after insert")?,
            created: true,
        })
    }

    pub fn ahp_input_by_code(&self, short_code: &str) -> Result<Option<AhpInputRecord>> {
        let connection = self.connection()?;
        let record = connection
            .query_row(
                "SELECT input_key, short_code, session_uri, chat_uri, request_id,
                        prompt, choices_json, allow_freeform, selection_mode, state, expires_at
                 FROM ahp_inputs WHERE short_code = ?1",
                [short_code.to_ascii_uppercase()],
                map_ahp_input,
            )
            .optional()
            .context("failed to query AHP input request")?;
        record
            .map(|record| attach_input_buttons(&connection, record))
            .transpose()
    }

    pub fn ahp_pending_input(&self) -> Result<Option<AhpInputRecord>> {
        let Some(binding) = self.ahp_binding()? else {
            return Ok(None);
        };
        self.ahp_pending_input_for_session(&binding.session_uri)
    }

    pub fn ahp_pending_input_for_session(
        &self,
        session_uri: &str,
    ) -> Result<Option<AhpInputRecord>> {
        let now = now();
        let connection = self.connection()?;
        expire_ahp_interactions(&connection, now)?;
        connection
            .query_row(
                "SELECT input_key, short_code, session_uri, chat_uri, request_id,
                        prompt, choices_json, allow_freeform, selection_mode, state, expires_at
                 FROM ahp_inputs
                 WHERE session_uri = ?1 AND state = 'pending' AND expires_at > ?2
                 ORDER BY created_at DESC LIMIT 1",
                params![session_uri, now],
                map_ahp_input,
            )
            .optional()
            .context("failed to query pending AHP input")?
            .map(|record| attach_input_buttons(&connection, record))
            .transpose()
    }

    pub fn ahp_has_pending_interactions(&self) -> Result<bool> {
        let now = now();
        let connection = self.connection()?;
        expire_ahp_interactions(&connection, now)?;
        let pending: i64 = connection.query_row(
            "SELECT
                (SELECT COUNT(*) FROM ahp_approvals
                 WHERE state IN ('pending', 'submitted') AND expires_at > ?1) +
                (SELECT COUNT(*) FROM ahp_inputs
                 WHERE state IN ('pending', 'submitted') AND expires_at > ?1)",
            [now],
            |row| row.get(0),
        )?;
        Ok(pending != 0)
    }

    pub fn ahp_submit_input(
        &self,
        input_key: &str,
        answer: &str,
        message_id: &str,
    ) -> Result<bool> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        expire_ahp_interactions(&transaction, now)?;
        let input =
            input_by_key(&transaction, input_key)?.context("AHP input request not found")?;
        let changed =
            submit_input_record(&transaction, &input, answer, message_id, "qq_text", now)?;
        transaction.commit()?;
        Ok(changed)
    }

    pub fn ahp_submit_input_by_button(
        &self,
        button_data: &str,
        interaction_id: &str,
    ) -> Result<Option<AhpInputSubmission>> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        expire_ahp_interactions(&transaction, now)?;
        let button: Option<(String, String)> = transaction
            .query_row(
                "SELECT input_key, label FROM ahp_input_buttons WHERE button_data = ?1",
                [button_data],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((input_key, answer)) = button else {
            return Ok(None);
        };
        let input = input_by_key(&transaction, &input_key)?
            .context("AHP input button references a missing request")?;
        let accepted = submit_input_record(
            &transaction,
            &input,
            &answer,
            interaction_id,
            "qq_button",
            now,
        )?;
        transaction.commit()?;
        drop(connection);
        let record = self
            .ahp_input_by_code(&input.short_code)?
            .context("AHP input disappeared after button submission")?;
        Ok(Some(AhpInputSubmission {
            record,
            accepted,
            answer,
        }))
    }

    pub fn ahp_resolve_input(
        &self,
        input_key: &str,
        outcome: &str,
        client_id: Option<&str>,
    ) -> Result<Option<AhpInputResolution>> {
        if !matches!(outcome, "answered" | "declined" | "cancelled") {
            bail!("invalid AHP input outcome");
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = transaction.execute(
            "UPDATE ahp_inputs
             SET state = ?1,
                 decided_by_surface = COALESCE(decided_by_surface, ?2),
                 updated_at = ?3
             WHERE input_key = ?4 AND state IN ('pending', 'submitted')",
            params![outcome, client_id, now(), input_key],
        )?;
        let decided_by_surface = transaction
            .query_row(
                "SELECT decided_by_surface FROM ahp_inputs WHERE input_key = ?1",
                [input_key],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten();
        let record = input_by_key(&transaction, input_key)?;
        transaction.commit()?;
        Ok(record.map(|record| AhpInputResolution {
            record,
            transitioned: changed == 1,
            decided_by_surface,
        }))
    }

    pub fn record_qq_interaction(
        &self,
        interaction_id: &str,
        outer_event_id: &str,
        user_openid: &str,
        button_data: &str,
    ) -> Result<bool> {
        let changed = self.connection()?.execute(
            "INSERT OR IGNORE INTO qq_interactions(
                interaction_id, outer_event_id, user_openid, button_data_hash, created_at
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                interaction_id,
                outer_event_id,
                user_openid,
                sha256_hex(button_data.as_bytes()),
                now()
            ],
        )?;
        Ok(changed == 1)
    }

    pub fn set_qq_interaction_ack(&self, interaction_id: &str, code: u8) -> Result<()> {
        self.connection()?.execute(
            "UPDATE qq_interactions SET ack_code = ?1 WHERE interaction_id = ?2",
            params![i64::from(code), interaction_id],
        )?;
        Ok(())
    }

    pub fn ahp_poll_commands(
        &self,
        adapter_id: &str,
        adapter_instance_id: &str,
        lease_seconds: u64,
    ) -> Result<Vec<AhpAdapterCommand>> {
        let now = now();
        let lease_expires_at = now
            + i64::try_from(lease_seconds).context("AHP command lease exceeds supported range")?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        require_adapter(&transaction, adapter_id, adapter_instance_id)?;
        transaction.execute(
            "UPDATE ahp_commands
             SET state = 'pending', lease_owner = NULL, lease_expires_at = NULL,
                 updated_at = ?1
             WHERE state = 'leased' AND lease_expires_at <= ?1",
            [now],
        )?;
        let rows = {
            let mut statement = transaction.prepare(
                "SELECT command_id, command_key, binding_id, binding_generation, kind, data_json
                 FROM ahp_commands
                 WHERE state = 'pending' AND binding_id IS NOT NULL
                 ORDER BY command_id ASC LIMIT ?1",
            )?;
            statement
                .query_map([i64::try_from(MAX_COMMAND_BATCH)?], map_command)?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        for command in &rows {
            transaction.execute(
                "UPDATE ahp_commands
                 SET state = 'leased', lease_owner = ?1, lease_expires_at = ?2,
                     updated_at = ?3
                 WHERE command_id = ?4 AND state = 'pending'",
                params![
                    adapter_instance_id,
                    lease_expires_at,
                    now,
                    command.command_id
                ],
            )?;
        }
        transaction.execute(
            "UPDATE ahp_adapter SET last_seen_at = ?1, updated_at = ?1
             WHERE singleton = 1",
            [now],
        )?;
        transaction.commit()?;
        Ok(rows)
    }

    pub fn ahp_ack_command(
        &self,
        adapter_id: &str,
        adapter_instance_id: &str,
        command_id: i64,
        outcome: AhpCommandOutcome,
        error_code: Option<&str>,
        result: Option<&Value>,
    ) -> Result<()> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        require_adapter(&transaction, adapter_id, adapter_instance_id)?;
        let command: Option<(String, String)> = transaction
            .query_row(
                "SELECT binding_id, kind FROM ahp_commands
                 WHERE command_id = ?1 AND state = 'leased' AND lease_owner = ?2",
                params![command_id, adapter_instance_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((binding_id, command_kind)) = command else {
            bail!("AHP command acknowledgement is stale or mismatched");
        };
        let result = result.map(canonical_json);
        let changed = transaction.execute(
            "UPDATE ahp_commands
             SET state = ?1, error_code = ?2, result_json = ?3,
                 lease_expires_at = NULL, progress_json = NULL,
                 progress_updated_at = NULL, updated_at = ?4
             WHERE command_id = ?5 AND state = 'leased' AND lease_owner = ?6",
            params![
                outcome.as_str(),
                error_code,
                result,
                now,
                command_id,
                adapter_instance_id
            ],
        )?;
        if changed != 1 {
            bail!("AHP command acknowledgement is stale or mismatched");
        }
        if command_kind == AhpCommandKind::UnbindSession.as_str() {
            match outcome {
                AhpCommandOutcome::Applied | AhpCommandOutcome::Rejected => {
                    transaction.execute(
                        "DELETE FROM ahp_bindings
                         WHERE binding_id = ?1 AND state = 'detaching'",
                        [&binding_id],
                    )?;
                    select_replacement_foreground(&transaction, now)?;
                }
                AhpCommandOutcome::Failed => {
                    transaction.execute(
                        "UPDATE ahp_bindings
                         SET state = 'failed', error_code = COALESCE(?1, 'unbind_failed'),
                             updated_at = ?2
                         WHERE binding_id = ?3 AND state = 'detaching'",
                        params![error_code, now, binding_id],
                    )?;
                }
            }
        }
        match outcome {
            AhpCommandOutcome::Applied => {}
            AhpCommandOutcome::Rejected => {
                expire_ahp_interactions(&transaction, now)?;
                transaction.execute(
                    "UPDATE ahp_approvals
                     SET state = 'pending', command_id = NULL,
                         decided_by_surface = NULL, decided_by_message_id = NULL,
                         updated_at = ?1
                     WHERE command_id = ?2 AND state = 'submitted' AND expires_at > ?1",
                    params![now, command_id],
                )?;
                transaction.execute(
                    "UPDATE ahp_inputs
                     SET state = 'pending', command_id = NULL,
                         decided_by_surface = NULL, decided_by_message_id = NULL,
                         updated_at = ?1
                     WHERE command_id = ?2 AND state = 'submitted' AND expires_at > ?1",
                    params![now, command_id],
                )?;
            }
            AhpCommandOutcome::Failed => {
                transaction.execute(
                    "UPDATE ahp_approvals SET state = 'failed', updated_at = ?1
                     WHERE command_id = ?2 AND state = 'submitted'",
                    params![now, command_id],
                )?;
                transaction.execute(
                    "UPDATE ahp_inputs SET state = 'failed', updated_at = ?1
                     WHERE command_id = ?2 AND state = 'submitted'",
                    params![now, command_id],
                )?;
            }
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn ahp_record_command_progress(
        &self,
        adapter_id: &str,
        adapter_instance_id: &str,
        command_id: i64,
        progress: u64,
        total: Option<u64>,
        message: Option<&str>,
    ) -> Result<()> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        require_adapter(&transaction, adapter_id, adapter_instance_id)?;
        let payload = canonical_json(&json!({
            "progress": progress,
            "total": total,
            "message": message,
        }));
        let changed = transaction.execute(
            "UPDATE ahp_commands
             SET progress_json = ?1, progress_updated_at = ?2, updated_at = ?2
             WHERE command_id = ?3 AND state = 'leased' AND lease_owner = ?4",
            params![payload, now, command_id, adapter_instance_id],
        )?;
        if changed != 1 {
            bail!("AHP command progress targets a stale or inactive lease");
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn ahp_command_status(&self, command_id: i64) -> Result<Option<AhpCommandStatusRecord>> {
        self.connection()?
            .query_row(
                "SELECT command_id, kind, state, error_code, result_json,
                        progress_json, progress_updated_at
                 FROM ahp_commands WHERE command_id = ?1",
                [command_id],
                |row| {
                    let kind: String = row.get(1)?;
                    let result: Option<String> = row.get(4)?;
                    let progress: Option<String> = row.get(5)?;
                    let progress_updated_at: Option<i64> = row.get(6)?;
                    Ok(AhpCommandStatusRecord {
                        command_id: row.get(0)?,
                        kind: AhpCommandKind::try_from(kind.as_str()).map_err(|error| {
                            SqliteError::FromSqlConversionFailure(
                                1,
                                rusqlite::types::Type::Text,
                                Box::new(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    error,
                                )),
                            )
                        })?,
                        state: row.get(2)?,
                        error_code: row.get(3)?,
                        result: result
                            .map(|value| {
                                serde_json::from_str(&value).map_err(|error| {
                                    SqliteError::FromSqlConversionFailure(
                                        4,
                                        rusqlite::types::Type::Text,
                                        Box::new(error),
                                    )
                                })
                            })
                            .transpose()?,
                        progress: match (progress, progress_updated_at) {
                            (Some(progress), Some(updated_at)) => {
                                let progress: Value =
                                    serde_json::from_str(&progress).map_err(|error| {
                                        SqliteError::FromSqlConversionFailure(
                                            5,
                                            rusqlite::types::Type::Text,
                                            Box::new(error),
                                        )
                                    })?;
                                Some(AhpCommandProgressRecord {
                                    progress: progress
                                        .get("progress")
                                        .and_then(Value::as_u64)
                                        .unwrap_or(0),
                                    total: progress.get("total").and_then(Value::as_u64),
                                    message: progress
                                        .get("message")
                                        .and_then(Value::as_str)
                                        .map(str::to_owned),
                                    updated_at,
                                })
                            }
                            _ => None,
                        },
                    })
                },
            )
            .optional()
            .context("failed to query AHP command status")
    }

    pub fn ahp_has_pending_commands(&self) -> Result<bool> {
        let pending: i64 = self.connection()?.query_row(
            "SELECT COUNT(*) FROM ahp_commands WHERE state IN ('pending', 'leased')",
            [],
            |row| row.get(0),
        )?;
        Ok(pending != 0)
    }

    pub fn ahp_status(&self, stale_seconds: u64) -> Result<AhpStatus> {
        let now = now();
        let stale_cutoff = now
            - i64::try_from(stale_seconds).context("AHP stale interval exceeds supported range")?;
        let connection = self.connection()?;
        let adapter = connection
            .query_row(
                "SELECT adapter_id, adapter_instance_id, version, state, last_seen_at
                 FROM ahp_adapter WHERE singleton = 1",
                [],
                |row| {
                    let last_seen_at: i64 = row.get(4)?;
                    let stored_state: String = row.get(3)?;
                    Ok(AhpAdapterStatus {
                        adapter_id: row.get(0)?,
                        adapter_instance_id: row.get(1)?,
                        version: row.get(2)?,
                        state: if last_seen_at < stale_cutoff {
                            "stale".to_owned()
                        } else {
                            stored_state
                        },
                        last_seen_at,
                    })
                },
            )
            .optional()?;
        let mut host_statement = connection.prepare(
            "SELECT endpoint_id, host_instance_id, pid, advertised_protocol,
                    selected_protocol, state, host_label, ssh_alias, target_kind,
                    target_path, endpoint_type, editor_client_tools_available, last_seen_at
             FROM ahp_hosts ORDER BY endpoint_id",
        )?;
        let hosts = host_statement
            .query_map([], |row| {
                let state: String = row.get(5)?;
                let last_seen_at: i64 = row.get(12)?;
                let stored_state = parse_host_state(&state).map_err(|error| {
                    SqliteError::FromSqlConversionFailure(
                        5,
                        rusqlite::types::Type::Text,
                        error.into(),
                    )
                })?;
                Ok(AhpHostDescriptor {
                    endpoint_id: row.get(0)?,
                    host_instance_id: row.get(1)?,
                    pid: u32::try_from(row.get::<_, i64>(2)?).map_err(|error| {
                        SqliteError::FromSqlConversionFailure(
                            2,
                            rusqlite::types::Type::Integer,
                            Box::new(error),
                        )
                    })?,
                    advertised_protocol: row.get(3)?,
                    selected_protocol: row.get(4)?,
                    state: if last_seen_at < stale_cutoff
                        && matches!(
                            stored_state,
                            crate::protocol::AhpHostState::Connected
                                | crate::protocol::AhpHostState::ReadOnly
                        ) {
                        crate::protocol::AhpHostState::Unreachable
                    } else {
                        stored_state
                    },
                    host_label: row.get(6)?,
                    ssh_alias: row.get(7)?,
                    target_kind: row
                        .get::<_, Option<String>>(8)?
                        .map(|value| match value.as_str() {
                            "local" => Ok(AhpTargetKind::Local),
                            "ssh" => Ok(AhpTargetKind::Ssh),
                            _ => Err(SqliteError::FromSqlConversionFailure(
                                8,
                                rusqlite::types::Type::Text,
                                Box::new(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    format!("unknown target kind {value}"),
                                )),
                            )),
                        })
                        .transpose()?,
                    target_path: row.get(9)?,
                    endpoint_type: row.get(10)?,
                    editor_client_tools_available: row
                        .get::<_, Option<i64>>(11)?
                        .map(|value| value != 0),
                    last_seen_at: Some(last_seen_at),
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let pending_commands = connection.query_row(
            "SELECT COUNT(*) FROM ahp_commands WHERE state IN ('pending', 'leased')",
            [],
            |row| row.get(0),
        )?;
        let pending_approvals = connection.query_row(
            "SELECT COUNT(*) FROM ahp_approvals WHERE state IN ('pending', 'submitted')",
            [],
            |row| row.get(0),
        )?;
        let pending_inputs = connection.query_row(
            "SELECT COUNT(*) FROM ahp_inputs WHERE state IN ('pending', 'submitted')",
            [],
            |row| row.get(0),
        )?;
        let pending_projections = connection.query_row(
            "SELECT COUNT(*) FROM ahp_projections WHERE state = 'pending'",
            [],
            |row| row.get(0),
        )?;
        drop(host_statement);
        drop(connection);
        let bindings = self.ahp_bindings()?;
        let binding = bindings.iter().find(|binding| binding.foreground).cloned();
        let foreground_binding_id = binding.as_ref().map(|binding| binding.binding_id.clone());
        Ok(AhpStatus {
            adapter,
            binding,
            bindings,
            foreground_binding_id,
            hosts,
            sessions: self.ahp_list_sessions()?,
            pending_commands,
            pending_approvals,
            pending_inputs,
            pending_projections,
            creation: self.ahp_creation_wizard_status()?,
        })
    }

    pub fn ahp_creation_wizard(&self) -> Result<Option<AhpCreationWizardRecord>> {
        let now = now();
        let connection = self.connection()?;
        expire_ahp_auxiliary_state(&connection, now)?;
        connection
            .query_row(
                "SELECT wizard_id, mode, state, context_json, pending_task, create_command_id,
                        new_session_uri, old_binding_endpoint_id, old_binding_session_uri,
                        old_binding_host_instance_id, cancel_requested, expires_at,
                        created_at, updated_at
                 FROM ahp_creation_wizard WHERE singleton = 1",
                [],
                map_creation_wizard,
            )
            .optional()
            .context("failed to query AHP creation wizard")
    }

    pub fn ahp_creation_wizard_status(&self) -> Result<Option<AhpCreationWizardStatus>> {
        self.ahp_creation_wizard().map(|wizard| {
            wizard.map(|wizard| AhpCreationWizardStatus {
                wizard_id: wizard.wizard_id,
                mode: wizard.mode,
                state: wizard.state,
                expires_at: wizard.expires_at,
                cancel_requested: wizard.cancel_requested,
            })
        })
    }

    pub fn ahp_save_creation_wizard(&self, wizard: &AhpCreationWizardRecord) -> Result<()> {
        if !matches!(wizard.mode.as_str(), "quick" | "advanced") {
            bail!("invalid AHP creation wizard mode");
        }
        if !matches!(
            wizard.state.as_str(),
            "select_target"
                | "select_model"
                | "select_approval"
                | "await_task"
                | "creating"
                | "cancelled"
        ) {
            bail!("invalid AHP creation wizard state");
        }
        let context = wizard.context.as_ref().map(canonical_json);
        self.connection()?.execute(
            "INSERT INTO ahp_creation_wizard(
                singleton, wizard_id, mode, state, context_json, pending_task,
                create_command_id, new_session_uri, old_binding_endpoint_id,
                old_binding_session_uri, old_binding_host_instance_id, cancel_requested,
                expires_at, created_at, updated_at
             ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
             ON CONFLICT(singleton) DO UPDATE SET
                wizard_id = excluded.wizard_id,
                mode = excluded.mode,
                state = excluded.state,
                context_json = excluded.context_json,
                pending_task = excluded.pending_task,
                create_command_id = excluded.create_command_id,
                new_session_uri = excluded.new_session_uri,
                old_binding_endpoint_id = excluded.old_binding_endpoint_id,
                old_binding_session_uri = excluded.old_binding_session_uri,
                old_binding_host_instance_id = excluded.old_binding_host_instance_id,
                cancel_requested = excluded.cancel_requested,
                expires_at = excluded.expires_at,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at",
            params![
                wizard.wizard_id,
                wizard.mode,
                wizard.state,
                context,
                wizard.pending_task,
                wizard.create_command_id,
                wizard.new_session_uri,
                wizard.old_binding_endpoint_id,
                wizard.old_binding_session_uri,
                wizard.old_binding_host_instance_id,
                if wizard.cancel_requested {
                    1_i64
                } else {
                    0_i64
                },
                wizard.expires_at,
                wizard.created_at,
                wizard.updated_at
            ],
        )?;
        Ok(())
    }

    pub fn ahp_clear_creation_wizard(&self) -> Result<()> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let wizard_id: Option<String> = transaction
            .query_row(
                "SELECT wizard_id FROM ahp_creation_wizard WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(wizard_id) = wizard_id {
            transaction.execute(
                "DELETE FROM ahp_creation_buttons WHERE wizard_id = ?1",
                [wizard_id],
            )?;
        }
        transaction.execute("DELETE FROM ahp_creation_wizard WHERE singleton = 1", [])?;
        transaction.commit()?;
        Ok(())
    }

    pub fn ahp_create_wizard_buttons(
        &self,
        wizard_id: &str,
        action_kind: &str,
        payloads: &[Value],
        ttl_seconds: u64,
    ) -> Result<Vec<AhpWizardButtonRecord>> {
        if payloads.len() > 100 {
            bail!("too many AHP creation wizard buttons");
        }
        let now = now();
        let expires_at =
            now + i64::try_from(ttl_seconds).context("wizard button TTL exceeds range")?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        expire_ahp_auxiliary_state(&transaction, now)?;
        transaction.execute(
            "DELETE FROM ahp_creation_buttons WHERE wizard_id = ?1",
            [wizard_id],
        )?;
        let mut buttons = Vec::with_capacity(payloads.len());
        for payload in payloads {
            let button_data = allocate_creation_button_token(&transaction, action_kind)?;
            transaction.execute(
                "INSERT INTO ahp_creation_buttons(
                    button_data, wizard_id, action_kind, payload_json, expires_at, created_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    button_data,
                    wizard_id,
                    action_kind,
                    canonical_json(payload),
                    expires_at,
                    now
                ],
            )?;
            buttons.push(AhpWizardButtonRecord {
                button_data,
                wizard_id: wizard_id.to_owned(),
                action_kind: action_kind.to_owned(),
                payload: payload.clone(),
            });
        }
        transaction.commit()?;
        Ok(buttons)
    }

    pub fn ahp_consume_wizard_button(
        &self,
        button_data: &str,
    ) -> Result<Option<AhpWizardButtonRecord>> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        expire_ahp_auxiliary_state(&transaction, now)?;
        let record: Option<(String, String, String, String, i64, Option<i64>)> = transaction
            .query_row(
                "SELECT button_data, wizard_id, action_kind, payload_json, expires_at, used_at
                 FROM ahp_creation_buttons WHERE button_data = ?1",
                [button_data],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .optional()?;
        let Some((button_data, wizard_id, action_kind, payload_json, expires_at, used_at)) = record
        else {
            return Ok(None);
        };
        if used_at.is_some() || expires_at <= now {
            return Ok(None);
        }
        transaction.execute(
            "UPDATE ahp_creation_buttons
             SET used_at = ?1
             WHERE wizard_id = ?2 AND action_kind = ?3 AND used_at IS NULL",
            params![now, wizard_id, action_kind],
        )?;
        transaction.commit()?;
        Ok(Some(AhpWizardButtonRecord {
            button_data,
            wizard_id,
            action_kind,
            payload: serde_json::from_str(&payload_json)
                .context("stored wizard button payload is invalid")?,
        }))
    }

    pub fn ahp_begin_trust_request(
        &self,
        request_id: &str,
        workspace_uri: &str,
        open_trust_ui: bool,
        ttl_seconds: u64,
    ) -> Result<AhpTrustRequestRecord> {
        let now = now();
        let expires_at =
            now + i64::try_from(ttl_seconds).context("trust request TTL exceeds range")?;
        let connection = self.connection()?;
        expire_ahp_auxiliary_state(&connection, now)?;
        connection.execute(
            "INSERT INTO ahp_trust_requests(
                request_id, workspace_uri, open_trust_ui, trusted, expires_at, created_at, updated_at
             ) VALUES (?1, ?2, ?3, 0, ?4, ?5, ?5)
             ON CONFLICT(request_id) DO UPDATE SET
                workspace_uri = excluded.workspace_uri,
                open_trust_ui = excluded.open_trust_ui,
                trusted = 0,
                expires_at = excluded.expires_at,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at",
            params![
                request_id,
                workspace_uri,
                if open_trust_ui { 1_i64 } else { 0_i64 },
                expires_at,
                now
            ],
        )?;
        Ok(AhpTrustRequestRecord {
            request_id: request_id.to_owned(),
            workspace_uri: workspace_uri.to_owned(),
            open_trust_ui,
            trusted: false,
            expires_at,
        })
    }

    pub fn ahp_pending_trust_requests(&self) -> Result<Vec<AhpTrustRequestRecord>> {
        let now = now();
        let connection = self.connection()?;
        expire_ahp_auxiliary_state(&connection, now)?;
        let mut statement = connection.prepare(
            "SELECT request_id, workspace_uri, open_trust_ui, trusted, expires_at
             FROM ahp_trust_requests
             WHERE expires_at > ?1
             ORDER BY created_at ASC",
        )?;
        statement
            .query_map([now], map_trust_request)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("failed to query AHP trust requests")
    }

    pub fn ahp_trust_request(&self, request_id: &str) -> Result<Option<AhpTrustRequestRecord>> {
        let now = now();
        let connection = self.connection()?;
        expire_ahp_auxiliary_state(&connection, now)?;
        connection
            .query_row(
                "SELECT request_id, workspace_uri, open_trust_ui, trusted, expires_at
                 FROM ahp_trust_requests WHERE request_id = ?1",
                [request_id],
                map_trust_request,
            )
            .optional()
            .context("failed to query AHP trust request")
    }

    pub fn ahp_delete_trust_request(&self, request_id: &str) -> Result<()> {
        self.connection()?.execute(
            "DELETE FROM ahp_trust_requests WHERE request_id = ?1",
            [request_id],
        )?;
        Ok(())
    }

    pub fn ahp_report_workspace_trust(
        &self,
        workspace_uris: &[String],
        trusted: bool,
    ) -> Result<u32> {
        if workspace_uris.is_empty() {
            return Ok(0);
        }
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        expire_ahp_auxiliary_state(&transaction, now)?;
        let mut changed = 0_u32;
        for workspace_uri in workspace_uris {
            let updated = transaction.execute(
                "UPDATE ahp_trust_requests
                 SET trusted = ?1, updated_at = ?2
                 WHERE workspace_uri = ?3 AND expires_at > ?2",
                params![if trusted { 1_i64 } else { 0_i64 }, now, workspace_uri],
            )?;
            changed = changed.saturating_add(u32::try_from(updated).unwrap_or(0));
        }
        transaction.commit()?;
        Ok(changed)
    }

    pub fn ahp_queue_projection(&self, event_id: &str, kind: &str, content: &str) -> Result<()> {
        self.connection()?.execute(
            "INSERT INTO ahp_projections(
                event_id, kind, content, state, created_at, updated_at
             ) VALUES (?1, ?2, ?3, 'pending', ?4, ?4)
             ON CONFLICT(event_id) DO NOTHING",
            params![event_id, kind, content, now()],
        )?;
        Ok(())
    }

    pub fn ahp_projection_failed(&self, event_id: &str, error_code: &str) -> Result<()> {
        self.connection()?.execute(
            "UPDATE ahp_projections
             SET attempts = attempts + 1, last_error = ?1, updated_at = ?2
             WHERE event_id = ?3 AND state = 'pending'",
            params![error_code, now(), event_id],
        )?;
        Ok(())
    }

    pub fn ahp_mark_projections_delivered(&self, event_ids: &[String]) -> Result<()> {
        if event_ids.is_empty() {
            return Ok(());
        }
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for event_id in event_ids {
            transaction.execute(
                "UPDATE ahp_projections
                 SET state = 'delivered', last_error = NULL, updated_at = ?1
                 WHERE event_id = ?2",
                params![now, event_id],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn ahp_pending_projections(&self, limit: u32) -> Result<Vec<AhpProjectionRecord>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT event_id, kind, content FROM ahp_projections
             WHERE state = 'pending'
             ORDER BY created_at ASC LIMIT ?1",
        )?;
        statement
            .query_map([i64::from(limit)], |row| {
                Ok(AhpProjectionRecord {
                    event_id: row.get(0)?,
                    kind: row.get(1)?,
                    content: row.get(2)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("failed to query pending AHP projections")
    }

    pub fn ahp_purge_events(&self, retention_days: u32) -> Result<()> {
        let now = now();
        let cutoff = now - i64::from(retention_days) * 86_400;
        let connection = self.connection()?;
        expire_ahp_auxiliary_state(&connection, now)?;
        connection.execute("DELETE FROM ahp_events WHERE created_at < ?1", [cutoff])?;
        connection.execute(
            "DELETE FROM ahp_commands
             WHERE state IN ('acked', 'rejected', 'failed') AND updated_at < ?1",
            [cutoff],
        )?;
        connection.execute(
            "DELETE FROM qq_interactions WHERE created_at < ?1",
            [cutoff],
        )?;
        connection.execute(
            "DELETE FROM ahp_projections
             WHERE state = 'delivered' AND updated_at < ?1",
            [cutoff],
        )?;
        connection.execute(
            "UPDATE ahp_approvals SET state = 'expired', updated_at = ?1
             WHERE state IN ('pending', 'submitted') AND expires_at <= ?1",
            [now],
        )?;
        connection.execute(
            "UPDATE ahp_inputs SET state = 'expired', updated_at = ?1
             WHERE state IN ('pending', 'submitted') AND expires_at <= ?1",
            [now],
        )?;
        connection.execute(
            "DELETE FROM ahp_session_switch_buttons
             WHERE expires_at <= ?1 OR used_at IS NOT NULL",
            [now],
        )?;
        connection.execute(
            "DELETE FROM ahp_session_catalog
             WHERE available = 0 AND last_seen_at < ?1
               AND session_uri NOT IN (
                   SELECT session_uri FROM ahp_bindings
               )",
            [cutoff],
        )?;
        connection.execute(
            "DELETE FROM ahp_hosts
             WHERE last_seen_at < ?1
               AND endpoint_id NOT IN (
                   SELECT endpoint_id FROM ahp_session_catalog
               )
               AND endpoint_id NOT IN (
                   SELECT endpoint_id FROM ahp_bindings
               )",
            [cutoff],
        )?;
        Ok(())
    }
}

fn require_adapter(
    connection: &Connection,
    adapter_id: &str,
    adapter_instance_id: &str,
) -> Result<()> {
    let exists = connection
        .query_row(
            "SELECT 1 FROM ahp_adapter
             WHERE singleton = 1 AND adapter_id = ?1 AND adapter_instance_id = ?2",
            params![adapter_id, adapter_instance_id],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    if !exists {
        bail!("AHP adapter identity is not registered");
    }
    Ok(())
}

fn require_bound_binding_for_session(
    connection: &Connection,
    session_uri: &str,
) -> Result<AhpBindingRecord> {
    let binding = require_routable_binding_for_session(connection, session_uri)?;
    if binding.state != "bound" || binding.chat_uri.is_none() {
        bail!("AHP session binding is not ready");
    }
    Ok(binding)
}

fn require_routable_binding_for_session(
    connection: &Connection,
    session_uri: &str,
) -> Result<AhpBindingRecord> {
    let binding = binding_by_session(connection, session_uri)?
        .context("the target AHP session is not tracked")?;
    if !matches!(binding.state.as_str(), "binding" | "bound") {
        bail!("AHP session binding is not ready");
    }
    Ok(binding)
}

fn binding_from_connection(connection: &Connection) -> Result<Option<AhpBindingRecord>> {
    connection
        .query_row(
            "SELECT b.binding_id, b.generation, b.endpoint_id, b.host_instance_id,
                    b.session_uri, b.chat_uri, b.state, b.last_server_sequence,
                    b.active_turn_id, b.queued_message_count, b.last_activity_at, 1,
                    s.host_label, s.ssh_alias, s.target_kind, s.target_path,
                    s.editor_client_tools_available
             FROM ahp_foreground_binding f
             JOIN ahp_bindings b ON b.binding_id = f.binding_id
             LEFT JOIN ahp_session_catalog s ON s.session_uri = b.session_uri
             WHERE f.singleton = 1 AND b.state != 'detaching'",
            [],
            map_binding,
        )
        .optional()
        .context("failed to query AHP binding")
}

fn bindings_from_connection(connection: &Connection) -> Result<Vec<AhpBindingRecord>> {
    let mut statement = connection.prepare(
        "SELECT b.binding_id, b.generation, b.endpoint_id, b.host_instance_id,
                b.session_uri, b.chat_uri, b.state, b.last_server_sequence,
                b.active_turn_id, b.queued_message_count, b.last_activity_at,
                CASE WHEN f.binding_id IS NULL THEN 0 ELSE 1 END,
                s.host_label, s.ssh_alias, s.target_kind, s.target_path,
                s.editor_client_tools_available
         FROM ahp_bindings b
         LEFT JOIN ahp_foreground_binding f
            ON f.singleton = 1 AND f.binding_id = b.binding_id
         LEFT JOIN ahp_session_catalog s ON s.session_uri = b.session_uri
         WHERE b.state != 'detaching'
         ORDER BY CASE WHEN f.binding_id IS NULL THEN 0 ELSE 1 END DESC,
                  b.last_activity_at DESC, b.binding_id",
    )?;
    statement
        .query_map([], map_binding)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("failed to query AHP bindings")
}

fn binding_by_id(connection: &Connection, binding_id: &str) -> Result<Option<AhpBindingRecord>> {
    connection
        .query_row(
            "SELECT b.binding_id, b.generation, b.endpoint_id, b.host_instance_id,
                    b.session_uri, b.chat_uri, b.state, b.last_server_sequence,
                    b.active_turn_id, b.queued_message_count, b.last_activity_at,
                    CASE WHEN f.binding_id IS NULL THEN 0 ELSE 1 END,
                    s.host_label, s.ssh_alias, s.target_kind, s.target_path,
                    s.editor_client_tools_available
             FROM ahp_bindings b
             LEFT JOIN ahp_foreground_binding f
                ON f.singleton = 1 AND f.binding_id = b.binding_id
             LEFT JOIN ahp_session_catalog s ON s.session_uri = b.session_uri
             WHERE b.binding_id = ?1",
            [binding_id],
            map_binding,
        )
        .optional()
        .context("failed to query AHP binding ID")
}

fn binding_by_session(
    connection: &Connection,
    session_uri: &str,
) -> Result<Option<AhpBindingRecord>> {
    connection
        .query_row(
            "SELECT b.binding_id, b.generation, b.endpoint_id, b.host_instance_id,
                    b.session_uri, b.chat_uri, b.state, b.last_server_sequence,
                    b.active_turn_id, b.queued_message_count, b.last_activity_at,
                    CASE WHEN f.binding_id IS NULL THEN 0 ELSE 1 END,
                    s.host_label, s.ssh_alias, s.target_kind, s.target_path,
                    s.editor_client_tools_available
             FROM ahp_bindings b
             LEFT JOIN ahp_foreground_binding f
                ON f.singleton = 1 AND f.binding_id = b.binding_id
             LEFT JOIN ahp_session_catalog s ON s.session_uri = b.session_uri
             WHERE b.session_uri = ?1",
            [session_uri],
            map_binding,
        )
        .optional()
        .context("failed to query AHP Session binding")
}

fn map_binding(row: &rusqlite::Row<'_>) -> rusqlite::Result<AhpBindingRecord> {
    let sequence: i64 = row.get(7)?;
    let queued_message_count: i64 = row.get(9)?;
    Ok(AhpBindingRecord {
        binding_id: row.get(0)?,
        generation: row.get(1)?,
        endpoint_id: row.get(2)?,
        host_instance_id: row.get(3)?,
        session_uri: row.get(4)?,
        chat_uri: row.get(5)?,
        state: row.get(6)?,
        last_server_sequence: sequence.max(0) as u64,
        active_turn_id: row.get(8)?,
        queued_message_count: u32::try_from(queued_message_count.max(0)).map_err(|error| {
            SqliteError::FromSqlConversionFailure(
                9,
                rusqlite::types::Type::Integer,
                Box::new(error),
            )
        })?,
        last_activity_at: row.get(10)?,
        foreground: row.get::<_, i64>(11)? != 0,
        host_label: row.get(12)?,
        ssh_alias: row.get(13)?,
        target_kind: row
            .get::<_, Option<String>>(14)?
            .map(|value| match value.as_str() {
                "local" => Ok(AhpTargetKind::Local),
                "ssh" => Ok(AhpTargetKind::Ssh),
                _ => Err(SqliteError::FromSqlConversionFailure(
                    14,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("unknown target kind {value}"),
                    )),
                )),
            })
            .transpose()?,
        target_path: row.get(15)?,
        editor_client_tools_available: row.get::<_, Option<i64>>(16)?.map(|value| value != 0),
    })
}

fn enqueue_command(
    connection: &Connection,
    command_key: &str,
    binding_id: &str,
    binding_generation: i64,
    kind: AhpCommandKind,
    data: &Value,
    now: i64,
) -> Result<i64> {
    let data = canonical_json(&redact_json(data));
    connection.execute(
        "INSERT OR IGNORE INTO ahp_commands(
            command_key, binding_id, binding_generation, kind, data_json,
            state, created_at, updated_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, 'pending', ?6, ?6)",
        params![
            command_key,
            binding_id,
            binding_generation,
            kind.as_str(),
            data,
            now
        ],
    )?;
    connection
        .query_row(
            "SELECT command_id FROM ahp_commands WHERE command_key = ?1",
            [command_key],
            |row| row.get(0),
        )
        .context("AHP command disappeared after enqueue")
}

fn touch_binding(connection: &Connection, binding_id: &str, now: i64) -> Result<()> {
    let changed = connection.execute(
        "UPDATE ahp_bindings SET last_activity_at = ?1, updated_at = ?1
         WHERE binding_id = ?2 AND state != 'detaching'",
        params![now, binding_id],
    )?;
    if changed != 1 {
        bail!("AHP binding disappeared while updating activity");
    }
    Ok(())
}

fn approval_by_key(
    connection: &Connection,
    approval_key: &str,
) -> Result<Option<AhpApprovalRecord>> {
    connection
        .query_row(
            "SELECT approval_key, short_code, stage, session_uri, chat_uri,
                    turn_id, tool_call_id, tool_name, summary, state,
                    allow_button_data, deny_button_data, expires_at
             FROM ahp_approvals WHERE approval_key = ?1",
            [approval_key],
            map_ahp_approval,
        )
        .optional()
        .context("failed to query AHP approval")
}

fn map_ahp_approval(row: &rusqlite::Row<'_>) -> rusqlite::Result<AhpApprovalRecord> {
    Ok(AhpApprovalRecord {
        approval_key: row.get(0)?,
        short_code: row.get(1)?,
        stage: row.get(2)?,
        session_uri: row.get(3)?,
        chat_uri: row.get(4)?,
        turn_id: row.get(5)?,
        tool_call_id: row.get(6)?,
        tool_name: row.get(7)?,
        summary: row.get(8)?,
        state: row.get(9)?,
        allow_button_data: row.get(10)?,
        deny_button_data: row.get(11)?,
        expires_at: row.get(12)?,
    })
}

fn insert_approval_with_unique_code(
    connection: &Connection,
    approval: &NewAhpApproval,
    now: i64,
) -> Result<String> {
    if !matches!(approval.stage.as_str(), "parameter" | "result") {
        bail!("invalid AHP approval stage");
    }
    for _ in 0..32 {
        let code = random_code(None, 5);
        let allow_button_data = format!("appr_{}", random_code(None, 20));
        let deny_button_data = format!("rejt_{}", random_code(None, 20));
        match connection.execute(
            "INSERT INTO ahp_approvals(
                approval_key, short_code, stage, session_uri, chat_uri, turn_id,
                tool_call_id, tool_name, summary, state, allow_button_data,
                deny_button_data, expires_at, created_at, updated_at
             ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'pending',
                ?10, ?11, ?12, ?13, ?13
             )",
            params![
                approval.approval_key,
                code,
                approval.stage,
                approval.session_uri,
                approval.chat_uri,
                approval.turn_id,
                approval.tool_call_id,
                approval.tool_name,
                approval.summary,
                allow_button_data,
                deny_button_data,
                approval.expires_at,
                now
            ],
        ) {
            Ok(_) => return Ok(code),
            Err(error) if is_constraint(&error) => continue,
            Err(error) => return Err(error.into()),
        }
    }
    bail!("failed to allocate a unique AHP approval code")
}

fn submit_approval_record(
    connection: &Connection,
    record: &AhpApprovalRecord,
    approved: bool,
    decision_id: &str,
    surface: &str,
    now: i64,
) -> Result<bool> {
    if record.state != "pending" || record.expires_at <= now {
        return Ok(false);
    }
    let binding = require_bound_binding_for_session(connection, &record.session_uri)?;
    if binding.chat_uri.as_deref() != Some(record.chat_uri.as_str()) {
        bail!("AHP approval targets a stale Chat");
    }
    let kind = if record.stage == "parameter" {
        AhpCommandKind::ApproveTool
    } else {
        AhpCommandKind::ReviewToolResult
    };
    let command_id = enqueue_command(
        connection,
        &format!(
            "approval:{}:{}",
            record.approval_key,
            if approved { "allow" } else { "deny" }
        ),
        &binding.binding_id,
        binding.generation,
        kind,
        &json!({
            "approval_key": record.approval_key,
            "session_uri": record.session_uri,
            "chat_uri": record.chat_uri,
            "turn_id": record.turn_id,
            "tool_call_id": record.tool_call_id,
            "approved": approved,
        }),
        now,
    )?;
    touch_binding(connection, &binding.binding_id, now)?;
    let changed = connection.execute(
        "UPDATE ahp_approvals
         SET state = 'submitted', command_id = ?1, decided_by_surface = ?2,
             decided_by_message_id = ?3, updated_at = ?4
         WHERE approval_key = ?5 AND state = 'pending'",
        params![command_id, surface, decision_id, now, record.approval_key],
    )?;
    Ok(changed == 1)
}

fn input_by_key(connection: &Connection, input_key: &str) -> Result<Option<AhpInputRecord>> {
    let record = connection
        .query_row(
            "SELECT input_key, short_code, session_uri, chat_uri, request_id,
                    prompt, choices_json, allow_freeform, selection_mode, state, expires_at
             FROM ahp_inputs WHERE input_key = ?1",
            [input_key],
            map_ahp_input,
        )
        .optional()
        .context("failed to query AHP input request")?;
    record
        .map(|record| attach_input_buttons(connection, record))
        .transpose()
}

fn map_ahp_input(row: &rusqlite::Row<'_>) -> rusqlite::Result<AhpInputRecord> {
    let choices: String = row.get(6)?;
    Ok(AhpInputRecord {
        input_key: row.get(0)?,
        short_code: row.get(1)?,
        session_uri: row.get(2)?,
        chat_uri: row.get(3)?,
        request_id: row.get(4)?,
        prompt: row.get(5)?,
        choices: serde_json::from_str(&choices).map_err(|error| {
            SqliteError::FromSqlConversionFailure(6, rusqlite::types::Type::Text, Box::new(error))
        })?,
        allow_freeform: row.get::<_, i64>(7)? != 0,
        selection_mode: row.get(8)?,
        buttons: Vec::new(),
        state: row.get(9)?,
        expires_at: row.get(10)?,
    })
}

fn insert_input_with_unique_code(
    connection: &Connection,
    input: &NewAhpInput,
    now: i64,
) -> Result<String> {
    if !matches!(input.selection_mode.as_str(), "none" | "single" | "multi") {
        bail!("invalid AHP input selection mode");
    }
    for _ in 0..32 {
        let code = random_code(Some('Q'), 4);
        match connection.execute(
            "INSERT INTO ahp_inputs(
                input_key, short_code, session_uri, chat_uri, request_id, prompt,
                choices_json, allow_freeform, selection_mode, state, expires_at,
                created_at, updated_at
             ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'pending', ?10, ?11, ?11
             )",
            params![
                input.input_key,
                code,
                input.session_uri,
                input.chat_uri,
                input.request_id,
                input.prompt,
                serde_json::to_string(&input.choices)?,
                i64::from(input.allow_freeform),
                input.selection_mode,
                input.expires_at,
                now
            ],
        ) {
            Ok(_) => {
                if input.selection_mode == "single" {
                    for (position, label) in input.choices.iter().enumerate().take(25) {
                        connection.execute(
                            "INSERT INTO ahp_input_buttons(
                                button_data, input_key, label, position
                             ) VALUES (?1, ?2, ?3, ?4)",
                            params![
                                format!("inpt_{}", random_code(None, 20)),
                                input.input_key,
                                label,
                                i64::try_from(position)?
                            ],
                        )?;
                    }
                }
                return Ok(code);
            }
            Err(error) if is_constraint(&error) => continue,
            Err(error) => return Err(error.into()),
        }
    }
    bail!("failed to allocate a unique AHP input code")
}

fn attach_input_buttons(
    connection: &Connection,
    mut input: AhpInputRecord,
) -> Result<AhpInputRecord> {
    let mut statement = connection.prepare(
        "SELECT label, button_data FROM ahp_input_buttons
         WHERE input_key = ?1 ORDER BY position",
    )?;
    input.buttons = statement
        .query_map([&input.input_key], |row| {
            Ok(AhpInputButton {
                label: row.get(0)?,
                button_data: row.get(1)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(input)
}

fn submit_input_record(
    connection: &Connection,
    input: &AhpInputRecord,
    answer: &str,
    decision_id: &str,
    surface: &str,
    now: i64,
) -> Result<bool> {
    if input.state != "pending" || input.expires_at <= now {
        return Ok(false);
    }
    if !input.allow_freeform && !input.choices.iter().any(|choice| choice == answer) {
        bail!("answer is not one of the allowed choices");
    }
    let binding = require_bound_binding_for_session(connection, &input.session_uri)?;
    if binding.chat_uri.as_deref() != Some(input.chat_uri.as_str()) {
        bail!("AHP input targets a stale Chat");
    }
    let command_id = enqueue_command(
        connection,
        &format!("input:{}:{decision_id}", input.input_key),
        &binding.binding_id,
        binding.generation,
        AhpCommandKind::CompleteInput,
        &json!({
            "input_key": input.input_key,
            "session_uri": input.session_uri,
            "chat_uri": input.chat_uri,
            "request_id": input.request_id,
            "answer": answer,
        }),
        now,
    )?;
    touch_binding(connection, &binding.binding_id, now)?;
    let changed = connection.execute(
        "UPDATE ahp_inputs
         SET state = 'submitted', command_id = ?1, decided_by_surface = ?2,
             decided_by_message_id = ?3, updated_at = ?4
         WHERE input_key = ?5 AND state = 'pending'",
        params![command_id, surface, decision_id, now, input.input_key],
    )?;
    Ok(changed == 1)
}

fn expire_ahp_interactions(connection: &Connection, now: i64) -> Result<()> {
    connection.execute(
        "UPDATE ahp_approvals SET state = 'expired', updated_at = ?1
         WHERE state IN ('pending', 'submitted') AND expires_at <= ?1",
        [now],
    )?;
    connection.execute(
        "UPDATE ahp_inputs SET state = 'expired', updated_at = ?1
         WHERE state IN ('pending', 'submitted') AND expires_at <= ?1",
        [now],
    )?;
    Ok(())
}

fn expire_ahp_auxiliary_state(connection: &Connection, now: i64) -> Result<()> {
    connection.execute(
        "DELETE FROM ahp_creation_buttons
         WHERE expires_at <= ?1 OR used_at IS NOT NULL",
        [now],
    )?;
    let expired_wizard_id: Option<String> = connection
        .query_row(
            "SELECT wizard_id FROM ahp_creation_wizard
             WHERE singleton = 1 AND expires_at <= ?1",
            [now],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(wizard_id) = expired_wizard_id {
        connection.execute(
            "DELETE FROM ahp_creation_buttons WHERE wizard_id = ?1",
            [wizard_id],
        )?;
        connection.execute("DELETE FROM ahp_creation_wizard WHERE singleton = 1", [])?;
    }
    connection.execute(
        "DELETE FROM ahp_trust_requests WHERE expires_at <= ?1",
        [now],
    )?;
    Ok(())
}

fn is_constraint(error: &SqliteError) -> bool {
    matches!(
        error,
        SqliteError::SqliteFailure(details, _)
            if details.code == rusqlite::ErrorCode::ConstraintViolation
    )
}

fn map_command(row: &rusqlite::Row<'_>) -> rusqlite::Result<AhpAdapterCommand> {
    let kind: String = row.get(4)?;
    let data: String = row.get(5)?;
    Ok(AhpAdapterCommand {
        command_id: row.get(0)?,
        command_key: row.get(1)?,
        binding_id: row.get(2)?,
        binding_generation: row.get(3)?,
        kind: AhpCommandKind::try_from(kind.as_str()).map_err(|error| {
            SqliteError::FromSqlConversionFailure(4, rusqlite::types::Type::Text, error.into())
        })?,
        data: serde_json::from_str(&data).map_err(|error| {
            SqliteError::FromSqlConversionFailure(5, rusqlite::types::Type::Text, Box::new(error))
        })?,
    })
}

fn map_creation_wizard(row: &rusqlite::Row<'_>) -> rusqlite::Result<AhpCreationWizardRecord> {
    let context: Option<String> = row.get(3)?;
    Ok(AhpCreationWizardRecord {
        wizard_id: row.get(0)?,
        mode: row.get(1)?,
        state: row.get(2)?,
        context: context
            .map(|value| {
                serde_json::from_str(&value).map_err(|error| {
                    SqliteError::FromSqlConversionFailure(
                        3,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })
            })
            .transpose()?,
        pending_task: row.get(4)?,
        create_command_id: row.get(5)?,
        new_session_uri: row.get(6)?,
        old_binding_endpoint_id: row.get(7)?,
        old_binding_session_uri: row.get(8)?,
        old_binding_host_instance_id: row.get(9)?,
        cancel_requested: row.get::<_, i64>(10)? != 0,
        expires_at: row.get(11)?,
        created_at: row.get(12)?,
        updated_at: row.get(13)?,
    })
}

fn map_trust_request(row: &rusqlite::Row<'_>) -> rusqlite::Result<AhpTrustRequestRecord> {
    Ok(AhpTrustRequestRecord {
        request_id: row.get(0)?,
        workspace_uri: row.get(1)?,
        open_trust_ui: row.get::<_, i64>(2)? != 0,
        trusted: row.get::<_, i64>(3)? != 0,
        expires_at: row.get(4)?,
    })
}

fn map_session_descriptor(row: &rusqlite::Row<'_>) -> rusqlite::Result<AhpSessionDescriptor> {
    let workspace_uris: String = row.get(6)?;
    Ok(AhpSessionDescriptor {
        short_code: Some(row.get(9)?),
        endpoint_id: row.get(0)?,
        host_instance_id: row.get(1)?,
        session_uri: row.get(2)?,
        provider: row.get(3)?,
        title: row.get(4)?,
        status: u32::try_from(row.get::<_, i64>(5)?).map_err(|error| {
            SqliteError::FromSqlConversionFailure(
                5,
                rusqlite::types::Type::Integer,
                Box::new(error),
            )
        })?,
        workspace_uris: serde_json::from_str(&workspace_uris).map_err(|error| {
            SqliteError::FromSqlConversionFailure(6, rusqlite::types::Type::Text, Box::new(error))
        })?,
        created_at: row.get(7)?,
        modified_at: row.get(8)?,
        host_label: row.get(10)?,
        ssh_alias: row.get(11)?,
        target_kind: row
            .get::<_, Option<String>>(12)?
            .map(|value| match value.as_str() {
                "local" => Ok(AhpTargetKind::Local),
                "ssh" => Ok(AhpTargetKind::Ssh),
                _ => Err(SqliteError::FromSqlConversionFailure(
                    12,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("unknown target kind {value}"),
                    )),
                )),
            })
            .transpose()?,
        target_path: row.get(13)?,
        editor_client_tools_available: row.get::<_, Option<i64>>(14)?.map(|value| value != 0),
        host_state: row
            .get::<_, Option<String>>(15)?
            .map(|value| {
                parse_host_state(&value).map_err(|error| {
                    SqliteError::FromSqlConversionFailure(
                        15,
                        rusqlite::types::Type::Text,
                        error.into(),
                    )
                })
            })
            .transpose()?,
        host_last_seen_at: row.get(16)?,
    })
}

fn session_by_uri(
    connection: &Connection,
    session_uri: &str,
) -> Result<Option<AhpSessionDescriptor>> {
    connection
        .query_row(
            "SELECT endpoint_id, host_instance_id, session_uri, provider, title,
                    status, workspace_uris_json, created_at, modified_at, short_code,
                    host_label, ssh_alias, target_kind, target_path,
                    editor_client_tools_available, NULL, NULL
             FROM ahp_session_catalog
             WHERE session_uri = ?1 AND (available = 1 OR target_kind IS NOT NULL)",
            [session_uri],
            map_session_descriptor,
        )
        .optional()
        .context("failed to query AHP Session URI")
}

fn focus_session_transaction(
    connection: &Connection,
    endpoint_id: &str,
    session_uri: &str,
    max_bindings: usize,
    now: i64,
) -> Result<AhpBindingRecord> {
    let binding = ensure_session_binding(
        connection,
        endpoint_id,
        session_uri,
        max_bindings,
        true,
        now,
    )?;
    connection.execute(
        "INSERT INTO ahp_foreground_binding(singleton, binding_id, updated_at)
         VALUES (1, ?1, ?2)
         ON CONFLICT(singleton) DO UPDATE SET
            binding_id = excluded.binding_id, updated_at = excluded.updated_at",
        params![binding.binding_id, now],
    )?;
    binding_by_id(connection, &binding.binding_id)?.context("foreground AHP binding disappeared")
}

fn ensure_session_binding(
    connection: &Connection,
    endpoint_id: &str,
    session_uri: &str,
    max_bindings: usize,
    touch_existing: bool,
    now: i64,
) -> Result<AhpBindingRecord> {
    let host_instance_id: String = connection
        .query_row(
            "SELECT host_instance_id FROM ahp_session_catalog
             WHERE endpoint_id = ?1 AND session_uri = ?2 AND available = 1",
            params![endpoint_id, session_uri],
            |row| row.get(0),
        )
        .optional()?
        .context("AHP Session is not present in the current catalogue")?;

    if let Some(existing) = binding_by_session(connection, session_uri)? {
        if existing.state == "detaching" {
            bail!("AHP Session is currently detaching");
        }
        if existing.endpoint_id == endpoint_id
            && existing.host_instance_id.as_deref() == Some(host_instance_id.as_str())
            && matches!(existing.state.as_str(), "binding" | "bound")
        {
            if touch_existing {
                touch_binding(connection, &existing.binding_id, now)?;
            }
            return binding_by_id(connection, &existing.binding_id)?
                .context("tracked AHP binding disappeared");
        }

        let generation = next_binding_generation(connection)?;
        connection.execute(
            "UPDATE ahp_commands
             SET state = 'failed', error_code = 'binding_replaced', updated_at = ?1
             WHERE binding_id = ?2 AND state IN ('pending', 'leased')",
            params![now, existing.binding_id],
        )?;
        connection.execute(
            "UPDATE ahp_bindings
             SET generation = ?1, endpoint_id = ?2, host_instance_id = ?3,
                 chat_uri = NULL, state = 'binding', last_server_sequence = 0,
                 active_turn_id = NULL, queued_message_count = 0, error_code = NULL,
                 last_activity_at = ?4, updated_at = ?4
             WHERE binding_id = ?5",
            params![
                generation,
                endpoint_id,
                host_instance_id,
                now,
                existing.binding_id
            ],
        )?;
        enqueue_bind_command(
            connection,
            &existing.binding_id,
            generation,
            endpoint_id,
            &host_instance_id,
            session_uri,
            now,
        )?;
        return binding_by_id(connection, &existing.binding_id)?
            .context("rebound AHP binding disappeared");
    }

    make_binding_capacity(connection, max_bindings, now)?;
    let binding_id = Uuid::new_v4().to_string();
    let generation = next_binding_generation(connection)?;
    connection.execute(
        "INSERT INTO ahp_bindings(
            binding_id, generation, endpoint_id, host_instance_id, session_uri,
            state, last_server_sequence, active_turn_id, queued_message_count,
            last_activity_at, created_at, updated_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, 'binding', 0, NULL, 0, ?6, ?6, ?6)",
        params![
            binding_id,
            generation,
            endpoint_id,
            host_instance_id,
            session_uri,
            now
        ],
    )?;
    enqueue_bind_command(
        connection,
        &binding_id,
        generation,
        endpoint_id,
        &host_instance_id,
        session_uri,
        now,
    )?;
    binding_by_id(connection, &binding_id)?.context("new AHP binding disappeared")
}

fn enqueue_bind_command(
    connection: &Connection,
    binding_id: &str,
    generation: i64,
    endpoint_id: &str,
    host_instance_id: &str,
    session_uri: &str,
    now: i64,
) -> Result<i64> {
    enqueue_command(
        connection,
        &format!("bind:{binding_id}:{generation}"),
        binding_id,
        generation,
        AhpCommandKind::BindSession,
        &json!({
            "endpoint_id": endpoint_id,
            "host_instance_id": host_instance_id,
            "session_uri": session_uri,
        }),
        now,
    )
}

fn next_binding_generation(connection: &Connection) -> Result<i64> {
    connection
        .query_row(
            "SELECT COALESCE(MAX(generation), 0) + 1 FROM ahp_bindings",
            [],
            |row| row.get(0),
        )
        .context("failed to allocate AHP binding generation")
}

fn make_binding_capacity(connection: &Connection, max_bindings: usize, now: i64) -> Result<()> {
    if max_bindings == 0 {
        bail!("AHP tracked Session limit must be non-zero");
    }
    let tracked: i64 = connection.query_row(
        "SELECT COUNT(*) FROM ahp_bindings WHERE state != 'detaching'",
        [],
        |row| row.get(0),
    )?;
    if tracked < i64::try_from(max_bindings)? {
        return Ok(());
    }
    let candidate = {
        let mut statement = connection.prepare(
            "SELECT b.binding_id, b.generation, b.endpoint_id, b.host_instance_id,
                    b.session_uri, b.chat_uri, b.state, b.last_server_sequence,
                    b.active_turn_id, b.queued_message_count, b.last_activity_at, 0,
                    s.host_label, s.ssh_alias, s.target_kind, s.target_path,
                    s.editor_client_tools_available
             FROM ahp_bindings b
             LEFT JOIN ahp_foreground_binding f
                ON f.singleton = 1 AND f.binding_id = b.binding_id
             LEFT JOIN ahp_session_catalog s ON s.session_uri = b.session_uri
             WHERE f.binding_id IS NULL
               AND b.state IN ('bound', 'lost', 'failed')
               AND b.active_turn_id IS NULL
               AND b.queued_message_count = 0
               AND NOT EXISTS (
                    SELECT 1 FROM ahp_commands c
                    WHERE c.binding_id = b.binding_id AND c.state IN ('pending', 'leased')
               )
               AND NOT EXISTS (
                    SELECT 1 FROM ahp_approvals a
                    WHERE a.session_uri = b.session_uri
                      AND a.state IN ('pending', 'submitted')
               )
               AND NOT EXISTS (
                    SELECT 1 FROM ahp_inputs i
                    WHERE i.session_uri = b.session_uri
                      AND i.state IN ('pending', 'submitted')
               )
             ORDER BY CASE b.state WHEN 'failed' THEN 0 WHEN 'lost' THEN 1 ELSE 2 END,
                      b.last_activity_at ASC, b.binding_id ASC
             LIMIT 1",
        )?;
        statement.query_row([], map_binding).optional()?
    };
    let candidate = candidate.context(
        "all five tracked AHP Sessions are active, foreground, queued, or awaiting interaction",
    )?;
    detach_binding(connection, &candidate, now, "lru_eviction")
}

fn detach_binding(
    connection: &Connection,
    binding: &AhpBindingRecord,
    now: i64,
    reason: &str,
) -> Result<()> {
    if binding.state == "detaching" {
        return Ok(());
    }
    if binding.active_turn_id.is_some() || binding.queued_message_count != 0 {
        bail!("cannot detach an AHP Session with an active Turn or queued message");
    }
    let pending: i64 = connection.query_row(
        "SELECT
            (SELECT COUNT(*) FROM ahp_commands
             WHERE binding_id = ?1 AND state IN ('pending', 'leased')) +
            (SELECT COUNT(*) FROM ahp_approvals
             WHERE session_uri = ?2 AND state IN ('pending', 'submitted')) +
            (SELECT COUNT(*) FROM ahp_inputs
             WHERE session_uri = ?2 AND state IN ('pending', 'submitted'))",
        params![binding.binding_id, binding.session_uri],
        |row| row.get(0),
    )?;
    if pending != 0 {
        bail!("cannot detach an AHP Session with pending commands or interactions");
    }
    connection.execute(
        "UPDATE ahp_bindings SET state = 'detaching', error_code = ?1, updated_at = ?2
         WHERE binding_id = ?3",
        params![reason, now, binding.binding_id],
    )?;
    connection.execute(
        "DELETE FROM ahp_foreground_binding WHERE singleton = 1 AND binding_id = ?1",
        [&binding.binding_id],
    )?;
    let command_id = enqueue_command(
        connection,
        &format!("unbind:{}:{}", binding.binding_id, binding.generation),
        &binding.binding_id,
        binding.generation,
        AhpCommandKind::UnbindSession,
        &json!({
            "session_uri": binding.session_uri,
            "chat_uri": binding.chat_uri,
        }),
        now,
    )?;
    connection.execute(
        "UPDATE ahp_commands
         SET state = 'pending', lease_owner = NULL, lease_expires_at = NULL,
             error_code = NULL, updated_at = ?1
         WHERE command_id = ?2 AND state = 'failed'",
        params![now, command_id],
    )?;
    Ok(())
}

fn ensure_foreground_binding(connection: &Connection, binding_id: &str, now: i64) -> Result<()> {
    connection.execute(
        "INSERT OR IGNORE INTO ahp_foreground_binding(singleton, binding_id, updated_at)
         VALUES (1, ?1, ?2)",
        params![binding_id, now],
    )?;
    Ok(())
}

fn select_replacement_foreground(connection: &Connection, now: i64) -> Result<()> {
    let has_foreground = connection
        .query_row(
            "SELECT 1 FROM ahp_foreground_binding WHERE singleton = 1",
            [],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    if has_foreground {
        return Ok(());
    }
    let replacement = connection
        .query_row(
            "SELECT binding_id FROM ahp_bindings
             WHERE state != 'detaching'
             ORDER BY CASE WHEN active_turn_id IS NULL THEN 0 ELSE 1 END DESC,
                      last_activity_at DESC, binding_id
             LIMIT 1",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    if let Some(binding_id) = replacement {
        ensure_foreground_binding(connection, &binding_id, now)?;
    }
    Ok(())
}

fn upsert_catalog_session(
    connection: &Connection,
    session: &AhpSessionDescriptor,
    now: i64,
) -> Result<()> {
    let short_code = connection
        .query_row(
            "SELECT short_code FROM ahp_session_catalog WHERE session_uri = ?1",
            [&session.session_uri],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .filter(|code| !code.is_empty())
        .map_or_else(|| allocate_session_code(connection), Ok)?;
    connection.execute(
        "INSERT INTO ahp_session_catalog(
            session_uri, short_code, endpoint_id, host_instance_id, provider, title,
            status, workspace_uris_json, created_at, modified_at, host_label, ssh_alias,
            target_kind, target_path, editor_client_tools_available, available, last_seen_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, 1, ?16)
         ON CONFLICT(session_uri) DO UPDATE SET
            endpoint_id = excluded.endpoint_id,
            host_instance_id = excluded.host_instance_id,
            provider = excluded.provider,
            title = excluded.title,
            status = excluded.status,
            workspace_uris_json = excluded.workspace_uris_json,
            created_at = excluded.created_at,
            modified_at = excluded.modified_at,
            host_label = excluded.host_label,
            ssh_alias = excluded.ssh_alias,
            target_kind = excluded.target_kind,
            target_path = excluded.target_path,
            editor_client_tools_available = excluded.editor_client_tools_available,
            available = 1,
            last_seen_at = excluded.last_seen_at",
        params![
            session.session_uri,
            short_code,
            session.endpoint_id,
            session.host_instance_id,
            session.provider,
            session.title,
            i64::from(session.status),
            serde_json::to_string(&session.workspace_uris)?,
            session.created_at,
            session.modified_at,
            session.host_label.as_deref(),
            session.ssh_alias.as_deref(),
            session.target_kind.map(|kind| match kind {
                AhpTargetKind::Local => "local",
                AhpTargetKind::Ssh => "ssh",
            }),
            session.target_path.as_deref(),
            if session.editor_client_tools_available.unwrap_or(true) {
                1_i64
            } else {
                0_i64
            },
            now
        ],
    )?;
    Ok(())
}

fn set_unique_session_code(connection: &Connection, session_uri: &str) -> Result<()> {
    let code = allocate_session_code(connection)?;
    connection.execute(
        "UPDATE ahp_session_catalog SET short_code = ?1 WHERE session_uri = ?2",
        params![code, session_uri],
    )?;
    Ok(())
}

fn allocate_session_code(connection: &Connection) -> Result<String> {
    for _ in 0..32 {
        let code = random_code(Some('A'), 4);
        let exists = connection
            .query_row(
                "SELECT 1 FROM ahp_session_catalog WHERE short_code = ?1",
                [&code],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !exists {
            return Ok(code);
        }
    }
    bail!("failed to allocate a unique AHP Session code")
}

fn allocate_session_switch_token(connection: &Connection) -> Result<String> {
    for _ in 0..32 {
        let token = format!("sess_{}", random_code(None, 20));
        let exists = connection
            .query_row(
                "SELECT 1 FROM ahp_session_switch_buttons WHERE button_data = ?1",
                [&token],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !exists {
            return Ok(token);
        }
    }
    bail!("failed to allocate a unique Session switch token")
}

fn allocate_creation_button_token(connection: &Connection, action_kind: &str) -> Result<String> {
    let prefix = match action_kind {
        "target" => "newt".to_owned(),
        "model" => "newm".to_owned(),
        "approval" => "newa".to_owned(),
        other => {
            let shortened: String = other
                .chars()
                .filter(|character| character.is_ascii_alphanumeric())
                .take(4)
                .collect();
            if shortened.is_empty() {
                "newx".to_owned()
            } else {
                format!("n{shortened}")
            }
        }
    };
    for _ in 0..32 {
        let token = format!("{prefix}_{}", random_code(None, 20));
        let exists = connection
            .query_row(
                "SELECT 1 FROM ahp_creation_buttons WHERE button_data = ?1",
                [&token],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !exists {
            return Ok(token);
        }
    }
    bail!("failed to allocate a unique creation wizard token")
}

fn parse_host_state(value: &str) -> Result<crate::protocol::AhpHostState> {
    use crate::protocol::AhpHostState;
    match value {
        "connected" => Ok(AhpHostState::Connected),
        "read_only" => Ok(AhpHostState::ReadOnly),
        "incompatible" => Ok(AhpHostState::Incompatible),
        "unreachable" => Ok(AhpHostState::Unreachable),
        _ => bail!("unknown AHP host state {value}"),
    }
}

fn fail_bindings_for_host_changes(
    connection: &Connection,
    hosts: &[AhpHostDescriptor],
    full_snapshot: bool,
    now: i64,
) -> Result<()> {
    let bindings = bindings_from_connection(connection)?;
    for binding in bindings {
        let next_host_instance = hosts
            .iter()
            .find(|host| host.endpoint_id == binding.endpoint_id)
            .map(|host| host.host_instance_id.as_str());
        if next_host_instance.is_none() && !full_snapshot {
            continue;
        }
        if binding.host_instance_id.as_deref() == next_host_instance {
            continue;
        }
        connection.execute(
            "UPDATE ahp_bindings
             SET state = 'lost', chat_uri = NULL, active_turn_id = NULL,
                 queued_message_count = 0, error_code = 'host_restarted', updated_at = ?1
             WHERE binding_id = ?2 AND state != 'detaching'",
            params![now, binding.binding_id],
        )?;
        connection.execute(
            "UPDATE ahp_commands
             SET state = 'failed', error_code = 'host_restarted', updated_at = ?1
             WHERE binding_id = ?2 AND state IN ('pending', 'leased')",
            params![now, binding.binding_id],
        )?;
        connection.execute(
            "UPDATE ahp_approvals
             SET state = 'failed', updated_at = ?1
             WHERE session_uri = ?2 AND state IN ('pending', 'submitted')",
            params![now, binding.session_uri],
        )?;
        connection.execute(
            "UPDATE ahp_inputs
             SET state = 'failed', updated_at = ?1
             WHERE session_uri = ?2 AND state IN ('pending', 'submitted')",
            params![now, binding.session_uri],
        )?;
    }
    Ok(())
}

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{AhpHostState, AhpSessionDescriptor};

    fn database() -> (tempfile::TempDir, Database) {
        let directory = tempfile::tempdir().expect("tempdir");
        let database = Database::open(&directory.path().join("ahp.sqlite3")).expect("database");
        (directory, database)
    }

    fn registration(instance: &str) -> AhpAdapterRegistration {
        AhpAdapterRegistration {
            adapter_id: "adapter-stable".to_owned(),
            adapter_instance_id: instance.to_owned(),
            version: "0.1.0".to_owned(),
            supported_protocols: vec!["1.0.0".to_owned()],
        }
    }

    fn host(instance: &str) -> AhpHostDescriptor {
        AhpHostDescriptor {
            endpoint_id: "endpoint-1".to_owned(),
            host_instance_id: instance.to_owned(),
            pid: 42,
            advertised_protocol: "1.0.0".to_owned(),
            selected_protocol: Some("1.0.0".to_owned()),
            state: AhpHostState::Connected,
            host_label: Some("local".to_owned()),
            ssh_alias: None,
            target_kind: Some(AhpTargetKind::Local),
            target_path: Some(r"C:\test".to_owned()),
            endpoint_type: Some("socket".to_owned()),
            editor_client_tools_available: Some(true),
            last_seen_at: Some(now()),
        }
    }

    fn session(instance: &str) -> AhpSessionDescriptor {
        AhpSessionDescriptor {
            short_code: None,
            endpoint_id: "endpoint-1".to_owned(),
            host_instance_id: instance.to_owned(),
            session_uri: "copilot:/session-1".to_owned(),
            provider: "copilot".to_owned(),
            title: "Shared session".to_owned(),
            status: 1,
            workspace_uris: vec!["file:///c%3A/test".to_owned()],
            created_at: "2026-08-27T00:00:00Z".to_owned(),
            modified_at: "2026-08-27T00:00:00Z".to_owned(),
            host_label: Some("local".to_owned()),
            ssh_alias: None,
            target_kind: Some(AhpTargetKind::Local),
            target_path: Some(r"C:\test".to_owned()),
            editor_client_tools_available: Some(true),
            host_state: None,
            host_last_seen_at: None,
        }
    }

    #[test]
    fn migration_expands_existing_command_kind_constraint() {
        let connection = Connection::open_in_memory().expect("database");
        connection
            .execute_batch(
                r#"
                PRAGMA foreign_keys = ON;
                CREATE TABLE ahp_commands (
                    command_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    command_key TEXT NOT NULL UNIQUE,
                    binding_generation INTEGER NOT NULL,
                    kind TEXT NOT NULL CHECK (
                        kind IN (
                            'bind_session', 'unbind_session', 'send_message', 'cancel_turn',
                            'approve_tool', 'review_tool_result', 'complete_input'
                        )
                    ),
                    data_json TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (
                        state IN ('pending', 'leased', 'acked', 'rejected', 'failed')
                    ),
                    lease_owner TEXT,
                    lease_expires_at INTEGER,
                    error_code TEXT,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                );
                INSERT INTO ahp_commands(
                    command_key, binding_generation, kind, data_json, state, created_at, updated_at
                ) VALUES ('legacy-bind', 1, 'bind_session', '{}', 'acked', 1, 1);
                "#,
            )
            .expect("legacy schema");

        initialize_schema(&connection).expect("migrate schema");

        connection
            .execute(
                "INSERT INTO ahp_commands(
                    command_key, binding_id, binding_generation, kind, data_json, state,
                    created_at, updated_at
                 ) VALUES ('prepare', '__unbound__', 0, 'prepare_target', '{}', 'pending', 2, 2)",
                [],
            )
            .expect("new command kind");
        let legacy_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM ahp_commands WHERE command_key = 'legacy-bind'",
                [],
                |row| row.get(0),
            )
            .expect("legacy command");
        let foreign_keys: i64 = connection
            .query_row("PRAGMA foreign_keys", [], |row| row.get(0))
            .expect("foreign keys");
        assert_eq!(legacy_count, 1);
        assert_eq!(foreign_keys, 1);
    }

    #[test]
    fn catalog_bind_and_command_ack_round_trip() {
        let (_directory, database) = database();
        database
            .ahp_register_adapter(&registration("adapter-run-1"))
            .expect("register");
        database
            .ahp_replace_catalog(
                "adapter-stable",
                "adapter-run-1",
                &[host("host-1")],
                &[session("host-1")],
            )
            .expect("catalogue");
        let binding = database
            .ahp_bind_session("endpoint-1", "copilot:/session-1")
            .expect("bind");
        assert_eq!(binding.state, "binding");
        let commands = database
            .ahp_poll_commands("adapter-stable", "adapter-run-1", 60)
            .expect("poll");
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].kind, AhpCommandKind::BindSession);
        database
            .ahp_ack_command(
                "adapter-stable",
                "adapter-run-1",
                commands[0].command_id,
                AhpCommandOutcome::Applied,
                None,
                None,
            )
            .expect("ack");
    }

    #[test]
    fn host_instance_change_fails_binding_and_pending_commands() {
        let (_directory, database) = database();
        database
            .ahp_register_adapter(&registration("adapter-run-1"))
            .expect("register");
        database
            .ahp_replace_catalog(
                "adapter-stable",
                "adapter-run-1",
                &[host("host-1")],
                &[session("host-1")],
            )
            .expect("first catalogue");
        database
            .ahp_bind_session("endpoint-1", "copilot:/session-1")
            .expect("bind");
        database
            .ahp_replace_catalog(
                "adapter-stable",
                "adapter-run-1",
                &[host("host-2")],
                &[session("host-2")],
            )
            .expect("replacement catalogue");
        let binding = database.ahp_binding().expect("binding").expect("bound");
        assert_eq!(binding.state, "lost");
        assert_eq!(database.ahp_status(60).expect("status").pending_commands, 0);
    }

    #[test]
    fn partial_catalogue_does_not_fail_bindings_on_other_hosts() {
        let (_directory, database) = database();
        database
            .ahp_register_adapter(&registration("adapter-run-1"))
            .expect("register");
        database
            .ahp_replace_catalog(
                "adapter-stable",
                "adapter-run-1",
                &[host("host-1")],
                &[session("host-1")],
            )
            .expect("first catalogue");
        let binding = database
            .ahp_bind_session("endpoint-1", "copilot:/session-1")
            .expect("bind");

        let mut other_host = host("host-2");
        other_host.endpoint_id = "endpoint-2".to_owned();
        let mut other_session = session("host-2");
        other_session.endpoint_id = "endpoint-2".to_owned();
        other_session.session_uri = "copilot:/session-2".to_owned();
        database
            .ahp_replace_catalog_scoped(
                "adapter-stable",
                "adapter-run-1",
                &[other_host],
                &[other_session],
                false,
            )
            .expect("partial catalogue");

        let unchanged = database
            .ahp_binding_for_session(&binding.session_uri)
            .expect("binding query")
            .expect("binding");
        assert_eq!(unchanged.state, "binding");
        assert_eq!(database.ahp_status(60).expect("status").pending_commands, 1);
    }

    #[test]
    fn event_insert_is_idempotent_and_redacted() {
        let (_directory, database) = database();
        database
            .ahp_register_adapter(&registration("adapter-run-1"))
            .expect("register");
        database
            .ahp_replace_catalog(
                "adapter-stable",
                "adapter-run-1",
                &[host("host-1")],
                &[session("host-1")],
            )
            .expect("catalogue");
        let binding = database
            .ahp_bind_session("endpoint-1", "copilot:/session-1")
            .expect("bind");
        database
            .ahp_binding_ready(
                "adapter-stable",
                "adapter-run-1",
                &binding.binding_id,
                "endpoint-1",
                "host-1",
                binding.generation,
                "copilot:/session-1",
                "ahp-chat://default/session",
                10,
            )
            .expect("ready");
        let event = AhpPublishedEvent {
            event_id: sha256_hex(b"event-1"),
            host_instance_id: "host-1".to_owned(),
            server_sequence: Some(11),
            session_uri: "copilot:/session-1".to_owned(),
            chat_uri: Some("ahp-chat://default/session".to_owned()),
            turn_id: Some("turn-1".to_owned()),
            kind: crate::protocol::AhpEventKind::ToolStatus,
            origin_client_id: None,
            occurred_at: "2026-08-27T00:00:01Z".to_owned(),
            data: json!({"password": "do-not-store"}),
        };
        assert_eq!(
            database
                .ahp_publish_events(
                    "adapter-stable",
                    "adapter-run-1",
                    &binding.binding_id,
                    binding.generation,
                    std::slice::from_ref(&event),
                )
                .expect("first publish"),
            vec![sha256_hex(b"event-1")]
        );
        database
            .ahp_mark_event_processed(&sha256_hex(b"event-1"))
            .expect("mark processed");
        assert_eq!(
            database
                .ahp_publish_events(
                    "adapter-stable",
                    "adapter-run-1",
                    &binding.binding_id,
                    binding.generation,
                    &[event],
                )
                .expect("duplicate publish"),
            Vec::<String>::new()
        );
    }

    #[test]
    fn qq_approval_submission_is_first_response_only() {
        let (_directory, database, binding) = bound_database();
        let approval = database
            .ahp_begin_approval(&NewAhpApproval {
                approval_key: "approval-1".to_owned(),
                stage: "parameter".to_owned(),
                session_uri: binding.session_uri,
                chat_uri: binding.chat_uri.expect("chat URI"),
                turn_id: "turn-1".to_owned(),
                tool_call_id: "tool-1".to_owned(),
                tool_name: "terminal".to_owned(),
                summary: "run tests".to_owned(),
                expires_at: now() + 600,
            })
            .expect("begin approval");
        assert!(approval.created);
        let submitted = database
            .ahp_submit_approval(&approval.record.short_code, true, "qq-message-1")
            .expect("submit approval")
            .expect("approval exists");
        assert_eq!(submitted.record.state, "submitted");
        assert!(submitted.accepted);
        let duplicate = database
            .ahp_submit_approval(&approval.record.short_code, false, "qq-message-2")
            .expect("duplicate decision")
            .expect("approval exists");
        assert_eq!(duplicate.record.state, "submitted");
        assert!(!duplicate.accepted);
        let commands = database
            .ahp_poll_commands("adapter-stable", "adapter-run-1", 60)
            .expect("approval command");
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].kind, AhpCommandKind::ApproveTool);
        let resolved = database
            .ahp_resolve_approval("approval-1", true, Some("vscode-client"))
            .expect("resolve")
            .expect("approval");
        assert_eq!(resolved.state, "allowed");
    }

    #[test]
    fn rejected_approval_submission_returns_to_pending_for_retry() {
        let (_directory, database, binding) = bound_database();
        let approval = database
            .ahp_begin_approval(&NewAhpApproval {
                approval_key: "approval-retry".to_owned(),
                stage: "parameter".to_owned(),
                session_uri: binding.session_uri,
                chat_uri: binding.chat_uri.expect("chat URI"),
                turn_id: "turn-retry".to_owned(),
                tool_call_id: "tool-retry".to_owned(),
                tool_name: "terminal".to_owned(),
                summary: "run tests".to_owned(),
                expires_at: now() + 600,
            })
            .expect("begin approval");
        let first = database
            .ahp_submit_approval(&approval.record.short_code, true, "qq-message-1")
            .expect("submit approval")
            .expect("approval exists");
        assert!(first.accepted);
        let commands = database
            .ahp_poll_commands("adapter-stable", "adapter-run-1", 60)
            .expect("approval command");
        database
            .ahp_ack_command(
                "adapter-stable",
                "adapter-run-1",
                commands[0].command_id,
                AhpCommandOutcome::Rejected,
                Some("stale-request"),
                None,
            )
            .expect("reject command");

        let retried = database
            .ahp_submit_approval(&approval.record.short_code, false, "qq-message-2")
            .expect("retry approval")
            .expect("approval exists");
        assert!(retried.accepted);
        assert_eq!(retried.record.state, "submitted");
    }

    #[test]
    fn pending_input_validates_choices_and_enqueues_answer() {
        let (_directory, database, binding) = bound_database();
        let input = database
            .ahp_begin_input(&NewAhpInput {
                input_key: "input-1".to_owned(),
                session_uri: binding.session_uri,
                chat_uri: binding.chat_uri.expect("chat URI"),
                request_id: "request-1".to_owned(),
                prompt: "Choose environment".to_owned(),
                choices: vec!["test".to_owned(), "production".to_owned()],
                allow_freeform: false,
                selection_mode: "single".to_owned(),
                expires_at: now() + 600,
            })
            .expect("begin input");
        assert!(input.created);
        assert!(
            database
                .ahp_submit_input("input-1", "invalid", "qq-message-1")
                .is_err()
        );
        assert!(
            database
                .ahp_submit_input("input-1", "test", "qq-message-2")
                .expect("valid input")
        );
        let commands = database
            .ahp_poll_commands("adapter-stable", "adapter-run-1", 60)
            .expect("input command");
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].kind, AhpCommandKind::CompleteInput);
        let resolution = database
            .ahp_resolve_input("input-1", "answered", Some("adapter-stable"))
            .expect("resolve input")
            .expect("input exists");
        assert!(resolution.transitioned);
        assert_eq!(resolution.record.state, "answered");
        assert_eq!(resolution.decided_by_surface.as_deref(), Some("qq_text"));
        let duplicate = database
            .ahp_resolve_input("input-1", "answered", Some("adapter-stable"))
            .expect("repeat input resolution")
            .expect("input exists");
        assert!(!duplicate.transitioned);
        assert_eq!(duplicate.decided_by_surface.as_deref(), Some("qq_text"));
    }

    #[test]
    fn rejected_input_completion_returns_input_to_pending_for_retry() {
        let (_directory, database, binding) = bound_database();
        let input = database
            .ahp_begin_input(&NewAhpInput {
                input_key: "input-retry".to_owned(),
                session_uri: binding.session_uri,
                chat_uri: binding.chat_uri.expect("chat URI"),
                request_id: "request-retry".to_owned(),
                prompt: "Choose or enter a value".to_owned(),
                choices: vec!["test".to_owned()],
                allow_freeform: true,
                selection_mode: "single".to_owned(),
                expires_at: now() + 600,
            })
            .expect("begin input");
        assert!(
            database
                .ahp_submit_input("input-retry", "custom", "qq-message-rejected")
                .expect("submit input")
        );
        let commands = database
            .ahp_poll_commands("adapter-stable", "adapter-run-1", 60)
            .expect("input command");
        database
            .ahp_ack_command(
                "adapter-stable",
                "adapter-run-1",
                commands[0].command_id,
                AhpCommandOutcome::Rejected,
                Some("invalid-command"),
                None,
            )
            .expect("reject command");

        let pending = database
            .ahp_pending_input()
            .expect("pending input")
            .expect("input restored");
        assert_eq!(pending.short_code, input.record.short_code);
        assert!(
            database
                .ahp_submit_input("input-retry", "test", "qq-message-retry")
                .expect("retry input")
        );
    }

    #[test]
    fn session_codes_are_stable_and_active_turn_survives_focus_switch() {
        let (_directory, database, binding) = bound_database();
        let original = database
            .ahp_list_sessions()
            .expect("sessions")
            .into_iter()
            .find(|session| session.session_uri == "copilot:/session-1")
            .expect("original session");
        let original_code = original.short_code.expect("short code");
        let second = AhpSessionDescriptor {
            session_uri: "copilot:/session-2".to_owned(),
            title: "Second session".to_owned(),
            created_at: "2026-08-27T00:01:00Z".to_owned(),
            modified_at: "2026-08-27T00:01:00Z".to_owned(),
            ..session("host-1")
        };
        database
            .ahp_replace_catalog(
                "adapter-stable",
                "adapter-run-1",
                &[host("host-1")],
                &[session("host-1"), second.clone()],
            )
            .expect("refresh catalogue");
        assert_eq!(
            database
                .ahp_session_by_code(&original_code)
                .expect("lookup")
                .expect("original")
                .session_uri,
            "copilot:/session-1"
        );
        database
            .ahp_publish_events(
                "adapter-stable",
                "adapter-run-1",
                &binding.binding_id,
                binding.generation,
                &[AhpPublishedEvent {
                    event_id: sha256_hex(b"active-chat"),
                    host_instance_id: "host-1".to_owned(),
                    server_sequence: Some(11),
                    session_uri: "copilot:/session-1".to_owned(),
                    chat_uri: Some("ahp-chat://default/session".to_owned()),
                    turn_id: Some("turn-active".to_owned()),
                    kind: crate::protocol::AhpEventKind::ChatSnapshot,
                    origin_client_id: None,
                    occurred_at: "2026-08-27T00:02:00Z".to_owned(),
                    data: json!({
                        "active_turn_id": "turn-active",
                        "queued_message_count": 0
                    }),
                }],
            )
            .expect("active snapshot");
        let second_binding = database
            .ahp_bind_session("endpoint-1", "copilot:/session-2")
            .expect("focus second session");
        assert!(second_binding.foreground);
        let bindings = database.ahp_bindings().expect("tracked bindings");
        assert_eq!(bindings.len(), 2);
        assert_eq!(
            bindings
                .iter()
                .find(|binding| binding.session_uri == "copilot:/session-1")
                .and_then(|binding| binding.active_turn_id.as_deref()),
            Some("turn-active")
        );
        database
            .ahp_publish_events(
                "adapter-stable",
                "adapter-run-1",
                &binding.binding_id,
                binding.generation,
                &[AhpPublishedEvent {
                    event_id: sha256_hex(b"idle-chat"),
                    host_instance_id: "host-1".to_owned(),
                    server_sequence: Some(12),
                    session_uri: "copilot:/session-1".to_owned(),
                    chat_uri: Some("ahp-chat://default/session".to_owned()),
                    turn_id: None,
                    kind: crate::protocol::AhpEventKind::ChatSnapshot,
                    origin_client_id: None,
                    occurred_at: "2026-08-27T00:03:00Z".to_owned(),
                    data: json!({
                        "active_turn_id": null,
                        "queued_message_count": 0
                    }),
                }],
            )
            .expect("idle snapshot");
        let session_uris = vec![
            "copilot:/session-1".to_owned(),
            "copilot:/session-2".to_owned(),
        ];
        let buttons = database
            .ahp_create_session_switch_buttons(&session_uris, 600)
            .expect("switch buttons");
        let second_button = buttons
            .iter()
            .find(|button| button.session.session_uri == "copilot:/session-2")
            .expect("second button");
        let switched = database
            .ahp_switch_session_by_button(&second_button.button_data, &session_uris)
            .expect("switch")
            .expect("button exists");
        assert!(switched.accepted);
        assert_eq!(switched.session.session_uri, "copilot:/session-2");
        assert_eq!(switched.binding.generation, binding.generation + 1);
        let replay = database
            .ahp_switch_session_by_button(&second_button.button_data, &session_uris)
            .expect("replay")
            .expect("button exists");
        assert!(!replay.accepted);
    }

    #[test]
    fn switch_allows_busy_target_and_keeps_previous_binding() {
        let (_directory, database, original_binding) = bound_database();
        let busy = AhpSessionDescriptor {
            session_uri: "copilot:/busy-session".to_owned(),
            title: "Busy session".to_owned(),
            status: 1 << 3,
            workspace_uris: vec!["file:///c%3A/other".to_owned()],
            created_at: "2026-08-27T00:01:00Z".to_owned(),
            modified_at: "2026-08-27T00:01:00Z".to_owned(),
            ..session("host-1")
        };
        database
            .ahp_replace_catalog(
                "adapter-stable",
                "adapter-run-1",
                &[host("host-1")],
                &[session("host-1"), busy],
            )
            .expect("catalogue");

        let focused = database
            .ahp_bind_session("endpoint-1", "copilot:/busy-session")
            .expect("focus busy target");
        assert_eq!(focused.session_uri, "copilot:/busy-session");
        assert!(focused.foreground);
        let binding = database
            .ahp_binding()
            .expect("binding query")
            .expect("foreground binding");
        assert_eq!(binding.binding_id, focused.binding_id);
        let bindings = database.ahp_bindings().expect("tracked bindings");
        assert_eq!(bindings.len(), 2);
        assert!(bindings.iter().any(|binding| {
            binding.binding_id == original_binding.binding_id && !binding.foreground
        }));
    }

    #[test]
    fn switch_button_cannot_bind_outside_allowed_session_set() {
        let (_directory, database, original_binding) = bound_database();
        let outside = AhpSessionDescriptor {
            session_uri: "copilot:/outside-session".to_owned(),
            title: "Outside session".to_owned(),
            workspace_uris: vec!["file:///c%3A/outside".to_owned()],
            created_at: "2026-08-27T00:01:00Z".to_owned(),
            modified_at: "2026-08-27T00:01:00Z".to_owned(),
            ..session("host-1")
        };
        database
            .ahp_replace_catalog(
                "adapter-stable",
                "adapter-run-1",
                &[host("host-1")],
                &[session("host-1"), outside],
            )
            .expect("catalogue");
        let buttons = database
            .ahp_create_session_switch_buttons(&["copilot:/outside-session".to_owned()], 600)
            .expect("switch button");

        let rejected = database
            .ahp_switch_session_by_button(
                &buttons[0].button_data,
                &["copilot:/session-1".to_owned()],
            )
            .expect("reject switch");
        assert!(rejected.is_none());
        let binding = database
            .ahp_binding()
            .expect("binding query")
            .expect("single binding");
        assert_eq!(binding.generation, original_binding.generation);
        assert_eq!(binding.session_uri, "copilot:/session-1");
    }

    #[test]
    fn independent_focus_button_groups_do_not_invalidate_each_other() {
        let (_directory, database, _) = bound_database();
        let session_uris = vec!["copilot:/session-1".to_owned()];
        let first = database
            .ahp_create_session_switch_buttons(&session_uris, 600)
            .expect("first focus button")
            .remove(0);
        let second = database
            .ahp_create_session_switch_buttons(&session_uris, 600)
            .expect("second focus button")
            .remove(0);

        assert!(
            database
                .ahp_switch_session_by_button(&first.button_data, &session_uris)
                .expect("first switch")
                .expect("first button")
                .accepted
        );
        assert!(
            database
                .ahp_switch_session_by_button(&second.button_data, &session_uris)
                .expect("second switch")
                .expect("second button")
                .accepted
        );
    }

    #[test]
    fn session_code_survives_transient_empty_catalogue() {
        let (_directory, database) = database();
        database
            .ahp_register_adapter(&registration("adapter-run-1"))
            .expect("register");
        database
            .ahp_replace_catalog(
                "adapter-stable",
                "adapter-run-1",
                &[host("host-1")],
                &[session("host-1")],
            )
            .expect("catalogue");
        let original = database
            .ahp_list_sessions()
            .expect("sessions")
            .into_iter()
            .next()
            .expect("session")
            .short_code
            .expect("short code");
        database
            .ahp_replace_catalog("adapter-stable", "adapter-run-1", &[], &[])
            .expect("empty catalogue");
        assert!(matches!(
            database
                .ahp_status(60)
                .expect("status")
                .hosts
                .first()
                .expect("cached host")
                .state,
            AhpHostState::Unreachable
        ));
        assert_eq!(
            database
                .ahp_list_sessions()
                .expect("cached sessions")
                .into_iter()
                .next()
                .expect("cached session")
                .short_code
                .expect("cached code"),
            original
        );
        database
            .ahp_replace_catalog(
                "adapter-stable",
                "adapter-run-1",
                &[host("host-1")],
                &[session("host-1")],
            )
            .expect("restored catalogue");
        assert_eq!(
            database
                .ahp_list_sessions()
                .expect("restored sessions")
                .into_iter()
                .next()
                .expect("restored session")
                .short_code
                .expect("restored code"),
            original
        );
    }

    #[test]
    fn approval_command_targets_origin_binding_after_focus_change() {
        let (_directory, database, first_binding) = bound_database();
        let second = numbered_session(2, 1);
        database
            .ahp_replace_catalog(
                "adapter-stable",
                "adapter-run-1",
                &[host("host-1")],
                &[session("host-1"), second],
            )
            .expect("catalogue");
        let second_binding = database
            .ahp_bind_session("endpoint-1", "copilot:/session-2")
            .expect("focus second");
        acknowledge_and_ready_binding(&database, &second_binding, "ahp-chat://default/session-2");

        let approval = database
            .ahp_begin_approval(&NewAhpApproval {
                approval_key: "approval-origin".to_owned(),
                stage: "parameter".to_owned(),
                session_uri: first_binding.session_uri.clone(),
                chat_uri: first_binding.chat_uri.clone().expect("first chat"),
                turn_id: "turn-first".to_owned(),
                tool_call_id: "tool-first".to_owned(),
                tool_name: "Terminal".to_owned(),
                summary: "Run command".to_owned(),
                expires_at: now() + 600,
            })
            .expect("approval");
        database
            .ahp_submit_approval(&approval.record.short_code, true, "qq-approval-origin")
            .expect("submit approval")
            .expect("approval exists");
        let commands = database
            .ahp_poll_commands("adapter-stable", "adapter-run-1", 60)
            .expect("approval command");
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].binding_id, first_binding.binding_id);
        assert_eq!(commands[0].kind, AhpCommandKind::ApproveTool);
        assert_eq!(
            database
                .ahp_binding()
                .expect("foreground")
                .expect("binding")
                .binding_id,
            second_binding.binding_id
        );
    }

    #[test]
    fn sixth_session_evicts_only_an_idle_background_binding() {
        let (_directory, database, first_binding) = bound_database();
        let mut sessions = vec![session("host-1")];
        sessions.extend((2..=6).map(|number| numbered_session(number, 1)));
        database
            .ahp_replace_catalog(
                "adapter-stable",
                "adapter-run-1",
                &[host("host-1")],
                &sessions,
            )
            .expect("catalogue");

        for number in 2..=5 {
            let binding = database
                .ahp_track_session("endpoint-1", &format!("copilot:/session-{number}"))
                .expect("track background Session");
            acknowledge_and_ready_binding(
                &database,
                &binding,
                &format!("ahp-chat://default/session-{number}"),
            );
        }
        assert_eq!(database.ahp_bindings().expect("five bindings").len(), 5);

        let sixth = database
            .ahp_track_session("endpoint-1", "copilot:/session-6")
            .expect("track sixth Session");
        let bindings = database.ahp_bindings().expect("tracked bindings");
        assert_eq!(bindings.len(), 5);
        assert!(
            bindings
                .iter()
                .any(|binding| binding.binding_id == sixth.binding_id)
        );
        assert!(
            bindings
                .iter()
                .any(|binding| binding.binding_id == first_binding.binding_id
                    && binding.foreground)
        );
        let commands = database
            .ahp_poll_commands("adapter-stable", "adapter-run-1", 60)
            .expect("eviction commands");
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0].kind, AhpCommandKind::UnbindSession);
        assert_eq!(commands[1].kind, AhpCommandKind::BindSession);
        assert_eq!(commands[1].binding_id, sixth.binding_id);
    }

    #[test]
    fn sixth_session_is_rejected_when_all_tracked_bindings_are_protected() {
        let (_directory, database, _) = bound_database();
        let mut sessions = vec![session("host-1")];
        sessions.extend((2..=6).map(|number| numbered_session(number, 1)));
        database
            .ahp_replace_catalog(
                "adapter-stable",
                "adapter-run-1",
                &[host("host-1")],
                &sessions,
            )
            .expect("catalogue");

        for number in 2..=5 {
            let binding = database
                .ahp_track_session("endpoint-1", &format!("copilot:/session-{number}"))
                .expect("track background Session");
            acknowledge_and_ready_binding(
                &database,
                &binding,
                &format!("ahp-chat://default/session-{number}"),
            );
            database
                .ahp_publish_events(
                    "adapter-stable",
                    "adapter-run-1",
                    &binding.binding_id,
                    binding.generation,
                    &[AhpPublishedEvent {
                        event_id: sha256_hex(format!("active-{number}").as_bytes()),
                        host_instance_id: "host-1".to_owned(),
                        server_sequence: Some(10 + number as u64),
                        session_uri: binding.session_uri.clone(),
                        chat_uri: binding.chat_uri.clone(),
                        turn_id: Some(format!("turn-{number}")),
                        kind: crate::protocol::AhpEventKind::TurnStarted,
                        origin_client_id: None,
                        occurred_at: format!("2026-08-27T00:{number:02}:30Z"),
                        data: json!({}),
                    }],
                )
                .expect("mark background Session active");
        }

        let error = database
            .ahp_track_session("endpoint-1", "copilot:/session-6")
            .expect_err("all five protected bindings must reject the sixth Session");
        assert!(error.to_string().contains("all five tracked AHP Sessions"));
        let bindings = database.ahp_bindings().expect("tracked bindings");
        assert_eq!(bindings.len(), MAX_TRACKED_AHP_SESSIONS);
        assert!(
            bindings
                .iter()
                .all(|binding| binding.session_uri != "copilot:/session-6")
        );
    }

    #[test]
    fn failed_unbind_command_can_be_retried() {
        let (_directory, database, _) = bound_database();
        database
            .ahp_replace_catalog(
                "adapter-stable",
                "adapter-run-1",
                &[host("host-1")],
                &[session("host-1"), numbered_session(2, 1)],
            )
            .expect("catalogue");
        let binding = database
            .ahp_track_session("endpoint-1", "copilot:/session-2")
            .expect("track background Session");
        acknowledge_and_ready_binding(&database, &binding, "ahp-chat://default/session-2");

        assert!(
            database
                .ahp_detach_session(&binding.session_uri)
                .expect("detach")
        );
        let first = database
            .ahp_poll_commands("adapter-stable", "adapter-run-1", 60)
            .expect("first unbind command")
            .into_iter()
            .find(|command| command.kind == AhpCommandKind::UnbindSession)
            .expect("unbind command");
        database
            .ahp_ack_command(
                "adapter-stable",
                "adapter-run-1",
                first.command_id,
                AhpCommandOutcome::Failed,
                Some("temporary_failure"),
                None,
            )
            .expect("fail unbind");

        assert!(
            database
                .ahp_detach_session(&binding.session_uri)
                .expect("retry detach")
        );
        let retried = database
            .ahp_poll_commands("adapter-stable", "adapter-run-1", 60)
            .expect("retried unbind command")
            .into_iter()
            .find(|command| command.kind == AhpCommandKind::UnbindSession)
            .expect("retried command");
        assert_eq!(retried.command_id, first.command_id);
        database
            .ahp_ack_command(
                "adapter-stable",
                "adapter-run-1",
                retried.command_id,
                AhpCommandOutcome::Applied,
                None,
                None,
            )
            .expect("complete retry");
        assert!(
            database
                .ahp_binding_for_session(&binding.session_uri)
                .expect("binding query")
                .is_none()
        );
    }

    #[test]
    fn legacy_singleton_binding_is_migrated_to_foreground_binding() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("legacy.sqlite3");
        let connection = Connection::open(&path).expect("legacy database");
        connection
            .execute_batch(
                "CREATE TABLE ahp_binding (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    generation INTEGER NOT NULL,
                    endpoint_id TEXT NOT NULL,
                    host_instance_id TEXT,
                    session_uri TEXT NOT NULL,
                    chat_uri TEXT,
                    state TEXT NOT NULL,
                    last_server_sequence INTEGER NOT NULL DEFAULT 0,
                    active_turn_id TEXT,
                    queued_message_count INTEGER NOT NULL DEFAULT 0,
                    error_code TEXT,
                    updated_at INTEGER NOT NULL
                 );
                 INSERT INTO ahp_binding(
                    singleton, generation, endpoint_id, host_instance_id, session_uri,
                    chat_uri, state, last_server_sequence, updated_at
                 ) VALUES (
                    1, 7, 'endpoint-legacy', 'host-legacy', 'copilot:/legacy',
                    'ahp-chat://legacy', 'bound', 42, 1234
                 );",
            )
            .expect("legacy schema");
        drop(connection);

        let database = Database::open(&path).expect("migrate database");
        let binding = database
            .ahp_binding()
            .expect("foreground query")
            .expect("migrated binding");
        assert!(binding.binding_id.starts_with("legacy-"));
        assert_eq!(binding.generation, 7);
        assert_eq!(binding.session_uri, "copilot:/legacy");
        assert_eq!(binding.last_server_sequence, 42);
        assert!(binding.foreground);
        assert_eq!(database.ahp_bindings().expect("bindings").len(), 1);
    }

    fn numbered_session(number: usize, status: u32) -> AhpSessionDescriptor {
        AhpSessionDescriptor {
            short_code: None,
            endpoint_id: "endpoint-1".to_owned(),
            host_instance_id: "host-1".to_owned(),
            session_uri: format!("copilot:/session-{number}"),
            provider: "copilot".to_owned(),
            title: format!("Session {number}"),
            status,
            workspace_uris: vec![format!("file:///c%3A/session-{number}")],
            created_at: format!("2026-08-27T00:{number:02}:00Z"),
            modified_at: format!("2026-08-27T00:{number:02}:00Z"),
            host_label: Some("local".to_owned()),
            ssh_alias: None,
            target_kind: Some(AhpTargetKind::Local),
            target_path: Some(format!(r"C:\session-{number}")),
            editor_client_tools_available: Some(true),
            host_state: None,
            host_last_seen_at: None,
        }
    }

    fn acknowledge_and_ready_binding(
        database: &Database,
        binding: &AhpBindingRecord,
        chat_uri: &str,
    ) {
        let commands = database
            .ahp_poll_commands("adapter-stable", "adapter-run-1", 60)
            .expect("bind command");
        let command = commands
            .iter()
            .find(|command| {
                command.binding_id == binding.binding_id
                    && command.kind == AhpCommandKind::BindSession
            })
            .expect("target bind command");
        database
            .ahp_ack_command(
                "adapter-stable",
                "adapter-run-1",
                command.command_id,
                AhpCommandOutcome::Applied,
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
                chat_uri,
                1,
            )
            .expect("binding ready");
    }

    #[test]
    fn creation_wizard_buttons_and_trust_requests_round_trip() {
        let (_directory, database) = database();
        let now = now();
        let wizard = AhpCreationWizardRecord {
            wizard_id: "wizard-1".to_owned(),
            mode: "advanced".to_owned(),
            state: "select_target".to_owned(),
            context: Some(json!({"step": "target"})),
            pending_task: None,
            create_command_id: None,
            new_session_uri: None,
            old_binding_endpoint_id: None,
            old_binding_session_uri: None,
            old_binding_host_instance_id: None,
            cancel_requested: false,
            expires_at: now + 600,
            created_at: now,
            updated_at: now,
        };
        database
            .ahp_save_creation_wizard(&wizard)
            .expect("save wizard");
        let buttons = database
            .ahp_create_wizard_buttons(
                "wizard-1",
                "target",
                &[
                    json!({"kind": "local", "path": r"C:\test"}),
                    json!({"kind": "local", "path": r"C:\other"}),
                ],
                600,
            )
            .expect("create wizard buttons");
        assert_eq!(buttons.len(), 2);
        let consumed = database
            .ahp_consume_wizard_button(&buttons[0].button_data)
            .expect("consume button")
            .expect("button exists");
        assert_eq!(consumed.wizard_id, "wizard-1");
        assert_eq!(consumed.action_kind, "target");
        assert!(
            database
                .ahp_consume_wizard_button(&buttons[1].button_data)
                .expect("sibling replay")
                .is_none()
        );

        let request = database
            .ahp_begin_trust_request("trust-1", "file:///C:/test", true, 600)
            .expect("begin trust request");
        assert!(!request.trusted);
        let listed = database
            .ahp_pending_trust_requests()
            .expect("list trust requests");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].request_id, "trust-1");
        assert_eq!(
            database
                .ahp_report_workspace_trust(&["file:///C:/test".to_owned()], true)
                .expect("report trust"),
            1
        );
        assert!(
            database
                .ahp_trust_request("trust-1")
                .expect("lookup")
                .expect("request")
                .trusted
        );
    }

    fn bound_database() -> (tempfile::TempDir, Database, AhpBindingRecord) {
        let (directory, database) = database();
        database
            .ahp_register_adapter(&registration("adapter-run-1"))
            .expect("register");
        database
            .ahp_replace_catalog(
                "adapter-stable",
                "adapter-run-1",
                &[host("host-1")],
                &[session("host-1")],
            )
            .expect("catalogue");
        let binding = database
            .ahp_bind_session("endpoint-1", "copilot:/session-1")
            .expect("bind");
        let bind_commands = database
            .ahp_poll_commands("adapter-stable", "adapter-run-1", 60)
            .expect("bind command");
        database
            .ahp_ack_command(
                "adapter-stable",
                "adapter-run-1",
                bind_commands[0].command_id,
                AhpCommandOutcome::Applied,
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
                "ahp-chat://default/session",
                10,
            )
            .expect("binding ready");
        let binding = database.ahp_binding().expect("binding").expect("bound");
        (directory, database, binding)
    }
}
