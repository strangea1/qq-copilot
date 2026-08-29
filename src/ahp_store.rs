use std::collections::HashSet;

use anyhow::{Context, Result, bail};
use rusqlite::{Connection, Error as SqliteError, OptionalExtension, TransactionBehavior, params};
use serde::Serialize;
use serde_json::{Value, json};

use crate::{
    db::Database,
    protocol::{
        AhpAdapterCommand, AhpAdapterRegistration, AhpBindingRecord, AhpCommandKind,
        AhpCommandOutcome, AhpHostDescriptor, AhpPublishedEvent, AhpSessionDescriptor,
    },
    security::{canonical_json, random_code, redact_json, sha256_hex},
};

const MAX_COMMAND_BATCH: usize = 32;

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
    pub hosts: Vec<AhpHostDescriptor>,
    pub sessions: Vec<AhpSessionDescriptor>,
    pub pending_commands: u32,
    pub pending_projections: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct AhpRegisterResult {
    pub binding: Option<AhpBindingRecord>,
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
                available INTEGER NOT NULL DEFAULT 1 CHECK (available IN (0, 1)),
                last_seen_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_ahp_sessions_endpoint
                ON ahp_session_catalog(endpoint_id, modified_at);

            CREATE TABLE IF NOT EXISTS ahp_binding (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                generation INTEGER NOT NULL,
                endpoint_id TEXT NOT NULL,
                host_instance_id TEXT,
                session_uri TEXT NOT NULL,
                chat_uri TEXT,
                state TEXT NOT NULL CHECK (
                    state IN ('binding', 'bound', 'lost', 'failed')
                ),
                last_server_sequence INTEGER NOT NULL DEFAULT 0,
                active_turn_id TEXT,
                queued_message_count INTEGER NOT NULL DEFAULT 0,
                error_code TEXT,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ahp_commands (
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
            CREATE INDEX IF NOT EXISTS idx_ahp_commands_ready
                ON ahp_commands(state, lease_expires_at, command_id);

            CREATE TABLE IF NOT EXISTS ahp_events (
                event_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                host_instance_id TEXT NOT NULL,
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
            "#,
        )
        .context("failed to initialize AHP database schema")?;
    migrate_ahp_schema(connection)
}

fn migrate_ahp_schema(connection: &Connection) -> Result<()> {
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
             ON ahp_session_catalog(short_code);",
    )?;
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
            binding: self.ahp_binding()?,
        })
    }

    pub fn ahp_replace_catalog(
        &self,
        adapter_id: &str,
        adapter_instance_id: &str,
        hosts: &[AhpHostDescriptor],
        sessions: &[AhpSessionDescriptor],
    ) -> Result<()> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        require_adapter(&transaction, adapter_id, adapter_instance_id)?;
        let binding = binding_from_connection(&transaction)?;
        let previous_bound_host = binding
            .as_ref()
            .and_then(|record| record.host_instance_id.as_deref());
        let next_bound_host = binding.as_ref().and_then(|record| {
            hosts
                .iter()
                .find(|host| host.endpoint_id == record.endpoint_id)
                .map(|host| host.host_instance_id.as_str())
        });
        if previous_bound_host.is_some() && previous_bound_host != next_bound_host {
            fail_binding_for_host_change(&transaction, now)?;
        }

        transaction.execute("UPDATE ahp_session_catalog SET available = 0", [])?;
        transaction.execute("UPDATE ahp_hosts SET state = 'unreachable'", [])?;
        for host in hosts {
            transaction.execute(
                "INSERT INTO ahp_hosts(
                    endpoint_id, host_instance_id, pid, advertised_protocol,
                    selected_protocol, state, last_seen_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(endpoint_id) DO UPDATE SET
                    host_instance_id = excluded.host_instance_id,
                    pid = excluded.pid,
                    advertised_protocol = excluded.advertised_protocol,
                    selected_protocol = excluded.selected_protocol,
                    state = excluded.state,
                    last_seen_at = excluded.last_seen_at",
                params![
                    host.endpoint_id,
                    host.host_instance_id,
                    i64::from(host.pid),
                    host.advertised_protocol,
                    host.selected_protocol,
                    host.state.as_str(),
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
                    status, workspace_uris_json, created_at, modified_at, short_code
             FROM ahp_session_catalog
             WHERE available = 1
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
                        status, workspace_uris_json, created_at, modified_at, short_code
                 FROM ahp_session_catalog WHERE short_code = ?1 AND available = 1",
                [short_code.to_ascii_uppercase()],
                map_session_descriptor,
            )
            .optional()
            .context("failed to query AHP Session code")
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
            "UPDATE ahp_session_switch_buttons SET used_at = ?1
             WHERE used_at IS NULL",
            [now],
        )?;
        transaction.execute(
            "DELETE FROM ahp_session_switch_buttons WHERE expires_at <= ?1",
            [now],
        )?;
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
                    button_data, session_uri, workspace_uris_json, expires_at, created_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    button_data,
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
        let token: Option<(String, String, String, i64, Option<i64>)> = transaction
            .query_row(
                "SELECT b.session_uri, b.workspace_uris_json, s.workspace_uris_json,
                        b.expires_at, b.used_at
                 FROM ahp_session_switch_buttons b
                 JOIN ahp_session_catalog s ON s.session_uri = b.session_uri
                 WHERE b.button_data = ?1 AND s.available = 1",
                [button_data],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()?;
        let Some((session_uri, menu_workspace_uris, current_workspace_uris, expires_at, used_at)) =
            token
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
            let binding = binding_from_connection(&transaction)?
                .context("no AHP Session is currently bound")?;
            return Ok(Some(AhpSessionSwitchSubmission {
                session,
                binding,
                accepted: false,
            }));
        }
        let binding = bind_session_transaction(
            &transaction,
            &session.endpoint_id,
            &session.session_uri,
            now,
        )?;
        transaction.execute(
            "UPDATE ahp_session_switch_buttons
             SET used_at = ?1 WHERE used_at IS NULL",
            [now],
        )?;
        transaction.commit()?;
        Ok(Some(AhpSessionSwitchSubmission {
            session,
            binding,
            accepted: true,
        }))
    }

    pub fn ahp_bind_session(
        &self,
        endpoint_id: &str,
        session_uri: &str,
    ) -> Result<AhpBindingRecord> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let binding = bind_session_transaction(&transaction, endpoint_id, session_uri, now)?;
        transaction.commit()?;
        Ok(binding)
    }

    pub fn ahp_unbind_session(&self) -> Result<bool> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let binding = binding_from_connection(&transaction)?;
        let Some(binding) = binding else {
            return Ok(false);
        };
        enqueue_command(
            &transaction,
            &format!("unbind:{}", binding.generation),
            binding.generation,
            AhpCommandKind::UnbindSession,
            &json!({
                "session_uri": binding.session_uri,
                "chat_uri": binding.chat_uri,
            }),
            now,
        )?;
        transaction.execute("DELETE FROM ahp_binding WHERE singleton = 1", [])?;
        transaction.commit()?;
        Ok(true)
    }

    pub fn ahp_binding(&self) -> Result<Option<AhpBindingRecord>> {
        let connection = self.connection()?;
        binding_from_connection(&connection)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn ahp_binding_ready(
        &self,
        adapter_id: &str,
        adapter_instance_id: &str,
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
            "UPDATE ahp_binding
             SET state = 'bound', chat_uri = ?1, last_server_sequence = ?2,
                 error_code = NULL, updated_at = ?3
             WHERE singleton = 1 AND generation = ?4 AND endpoint_id = ?5
               AND host_instance_id = ?6 AND session_uri = ?7",
            params![
                chat_uri,
                sequence,
                now,
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
        binding_generation: i64,
        reason_code: &str,
    ) -> Result<()> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        require_adapter(&transaction, adapter_id, adapter_instance_id)?;
        let changed = transaction.execute(
            "UPDATE ahp_binding SET state = 'failed', error_code = ?1, updated_at = ?2
             WHERE singleton = 1 AND generation = ?3",
            params![reason_code, now, binding_generation],
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
        binding_generation: i64,
        events: &[AhpPublishedEvent],
    ) -> Result<Vec<String>> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        require_adapter(&transaction, adapter_id, adapter_instance_id)?;
        let binding = binding_from_connection(&transaction)?.context("no AHP session is bound")?;
        if binding.generation != binding_generation || binding.state != "bound" {
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
                    event_id, host_instance_id, binding_generation, server_sequence,
                    session_uri, chat_uri, turn_id, kind, origin_client_id,
                    occurred_at, data_redacted_json, created_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    event.event_id,
                    event.host_instance_id,
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
                        "UPDATE ahp_binding
                         SET active_turn_id = ?1, queued_message_count = ?2, updated_at = ?3
                         WHERE singleton = 1 AND generation = ?4",
                        params![
                            active_turn_id,
                            i64::try_from(queued_message_count)?,
                            now,
                            binding_generation
                        ],
                    )?;
                }
                crate::protocol::AhpEventKind::TurnStarted => {
                    transaction.execute(
                        "UPDATE ahp_binding
                         SET active_turn_id = ?1, updated_at = ?2
                         WHERE singleton = 1 AND generation = ?3",
                        params![event.turn_id, now, binding_generation],
                    )?;
                }
                crate::protocol::AhpEventKind::TurnCompleted
                | crate::protocol::AhpEventKind::TurnCancelled
                | crate::protocol::AhpEventKind::TurnFailed => {
                    transaction.execute(
                        "UPDATE ahp_binding
                         SET active_turn_id = NULL, updated_at = ?1
                         WHERE singleton = 1 AND generation = ?2
                           AND (?3 IS NULL OR active_turn_id = ?3)",
                        params![now, binding_generation, event.turn_id],
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
                "UPDATE ahp_binding
                 SET last_server_sequence = MAX(last_server_sequence, ?1), updated_at = ?2
                 WHERE singleton = 1 AND generation = ?3",
                params![i64::try_from(sequence)?, now, binding_generation],
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
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let binding = require_bound_binding(&transaction)?;
        let command_id = enqueue_command(
            &transaction,
            &format!("qq-message:{message_id}"),
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
        transaction.commit()?;
        Ok(command_id)
    }

    pub fn ahp_enqueue_cancel(&self, message_id: &str) -> Result<i64> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let binding = require_bound_binding(&transaction)?;
        let command_id = enqueue_command(
            &transaction,
            &format!("qq-cancel:{message_id}"),
            binding.generation,
            AhpCommandKind::CancelTurn,
            &json!({
                "session_uri": binding.session_uri,
                "chat_uri": binding.chat_uri,
            }),
            now,
        )?;
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
        let now = now();
        let connection = self.connection()?;
        expire_ahp_interactions(&connection, now)?;
        connection
            .query_row(
                "SELECT input_key, short_code, session_uri, chat_uri, request_id,
                        prompt, choices_json, allow_freeform, selection_mode, state, expires_at
                 FROM ahp_inputs
                 WHERE state = 'pending' AND expires_at > ?1
                 ORDER BY created_at DESC LIMIT 1",
                [now],
                map_ahp_input,
            )
            .optional()
            .context("failed to query pending AHP input")?
            .map(|record| attach_input_buttons(&connection, record))
            .transpose()
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
    ) -> Result<Option<AhpInputRecord>> {
        if !matches!(outcome, "answered" | "declined" | "cancelled") {
            bail!("invalid AHP input outcome");
        }
        let connection = self.connection()?;
        connection.execute(
            "UPDATE ahp_inputs
             SET state = ?1,
                 decided_by_surface = COALESCE(decided_by_surface, ?2),
                 updated_at = ?3
             WHERE input_key = ?4 AND state IN ('pending', 'submitted')",
            params![outcome, client_id, now(), input_key],
        )?;
        input_by_key(&connection, input_key)
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
                "SELECT command_id, command_key, binding_generation, kind, data_json
                 FROM ahp_commands
                 WHERE state = 'pending'
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
    ) -> Result<()> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        require_adapter(&transaction, adapter_id, adapter_instance_id)?;
        let changed = transaction.execute(
            "UPDATE ahp_commands
             SET state = ?1, error_code = ?2, lease_expires_at = NULL, updated_at = ?3
             WHERE command_id = ?4 AND state = 'leased' AND lease_owner = ?5",
            params![
                outcome.as_str(),
                error_code,
                now,
                command_id,
                adapter_instance_id
            ],
        )?;
        if changed != 1 {
            bail!("AHP command acknowledgement is stale or mismatched");
        }
        match outcome {
            AhpCommandOutcome::Applied => {}
            AhpCommandOutcome::Rejected => {
                expire_ahp_interactions(&transaction, now)?;
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
                    "UPDATE ahp_inputs SET state = 'failed', updated_at = ?1
                     WHERE command_id = ?2 AND state = 'submitted'",
                    params![now, command_id],
                )?;
            }
        }
        transaction.commit()?;
        Ok(())
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
                    selected_protocol, state FROM ahp_hosts ORDER BY endpoint_id",
        )?;
        let hosts = host_statement
            .query_map([], |row| {
                let state: String = row.get(5)?;
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
                    state: parse_host_state(&state).map_err(|error| {
                        SqliteError::FromSqlConversionFailure(
                            5,
                            rusqlite::types::Type::Text,
                            error.into(),
                        )
                    })?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let pending_commands = connection.query_row(
            "SELECT COUNT(*) FROM ahp_commands WHERE state IN ('pending', 'leased')",
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
        Ok(AhpStatus {
            adapter,
            binding: self.ahp_binding()?,
            hosts,
            sessions: self.ahp_list_sessions()?,
            pending_commands,
            pending_projections,
        })
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
        let cutoff = now() - i64::from(retention_days) * 86_400;
        let connection = self.connection()?;
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
            [now()],
        )?;
        connection.execute(
            "UPDATE ahp_inputs SET state = 'expired', updated_at = ?1
             WHERE state IN ('pending', 'submitted') AND expires_at <= ?1",
            [now()],
        )?;
        connection.execute(
            "DELETE FROM ahp_session_switch_buttons
             WHERE expires_at <= ?1 OR used_at IS NOT NULL",
            [now()],
        )?;
        connection.execute(
            "DELETE FROM ahp_session_catalog
             WHERE available = 0 AND last_seen_at < ?1
               AND session_uri NOT IN (
                   SELECT session_uri FROM ahp_binding WHERE singleton = 1
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
                   SELECT endpoint_id FROM ahp_binding WHERE singleton = 1
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

fn require_bound_binding(connection: &Connection) -> Result<AhpBindingRecord> {
    let binding = binding_from_connection(connection)?.context("no AHP session is bound")?;
    if binding.state != "bound" || binding.chat_uri.is_none() {
        bail!("AHP session binding is not ready");
    }
    Ok(binding)
}

fn binding_from_connection(connection: &Connection) -> Result<Option<AhpBindingRecord>> {
    connection
        .query_row(
            "SELECT generation, endpoint_id, host_instance_id, session_uri, chat_uri,
                    state, last_server_sequence, active_turn_id, queued_message_count
             FROM ahp_binding WHERE singleton = 1",
            [],
            |row| {
                let sequence: i64 = row.get(6)?;
                let queued_message_count: i64 = row.get(8)?;
                Ok(AhpBindingRecord {
                    generation: row.get(0)?,
                    endpoint_id: row.get(1)?,
                    host_instance_id: row.get(2)?,
                    session_uri: row.get(3)?,
                    chat_uri: row.get(4)?,
                    state: row.get(5)?,
                    last_server_sequence: sequence.max(0) as u64,
                    active_turn_id: row.get(7)?,
                    queued_message_count: u32::try_from(queued_message_count.max(0)).map_err(
                        |error| {
                            SqliteError::FromSqlConversionFailure(
                                8,
                                rusqlite::types::Type::Integer,
                                Box::new(error),
                            )
                        },
                    )?,
                })
            },
        )
        .optional()
        .context("failed to query AHP binding")
}

fn enqueue_command(
    connection: &Connection,
    command_key: &str,
    binding_generation: i64,
    kind: AhpCommandKind,
    data: &Value,
    now: i64,
) -> Result<i64> {
    let data = canonical_json(&redact_json(data));
    connection.execute(
        "INSERT OR IGNORE INTO ahp_commands(
            command_key, binding_generation, kind, data_json, state, created_at, updated_at
         ) VALUES (?1, ?2, ?3, ?4, 'pending', ?5, ?5)",
        params![command_key, binding_generation, kind.as_str(), data, now],
    )?;
    connection
        .query_row(
            "SELECT command_id FROM ahp_commands WHERE command_key = ?1",
            [command_key],
            |row| row.get(0),
        )
        .context("AHP command disappeared after enqueue")
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
    let binding = require_bound_binding(connection)?;
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
    let binding = require_bound_binding(connection)?;
    let command_id = enqueue_command(
        connection,
        &format!("input:{}:{decision_id}", input.input_key),
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

fn is_constraint(error: &SqliteError) -> bool {
    matches!(
        error,
        SqliteError::SqliteFailure(details, _)
            if details.code == rusqlite::ErrorCode::ConstraintViolation
    )
}

fn map_command(row: &rusqlite::Row<'_>) -> rusqlite::Result<AhpAdapterCommand> {
    let kind: String = row.get(3)?;
    let data: String = row.get(4)?;
    Ok(AhpAdapterCommand {
        command_id: row.get(0)?,
        command_key: row.get(1)?,
        binding_generation: row.get(2)?,
        kind: AhpCommandKind::try_from(kind.as_str()).map_err(|error| {
            SqliteError::FromSqlConversionFailure(3, rusqlite::types::Type::Text, error.into())
        })?,
        data: serde_json::from_str(&data).map_err(|error| {
            SqliteError::FromSqlConversionFailure(4, rusqlite::types::Type::Text, Box::new(error))
        })?,
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
    })
}

fn session_by_uri(
    connection: &Connection,
    session_uri: &str,
) -> Result<Option<AhpSessionDescriptor>> {
    connection
        .query_row(
            "SELECT endpoint_id, host_instance_id, session_uri, provider, title,
                    status, workspace_uris_json, created_at, modified_at, short_code
             FROM ahp_session_catalog WHERE session_uri = ?1 AND available = 1",
            [session_uri],
            map_session_descriptor,
        )
        .optional()
        .context("failed to query AHP Session URI")
}

fn bind_session_transaction(
    connection: &Connection,
    endpoint_id: &str,
    session_uri: &str,
    now: i64,
) -> Result<AhpBindingRecord> {
    let previous = binding_from_connection(connection)?;
    let switching_session = previous.as_ref().is_some_and(|binding| {
        binding.endpoint_id != endpoint_id || binding.session_uri != session_uri
    });
    if let Some(previous) = previous.as_ref() {
        if previous.endpoint_id == endpoint_id
            && previous.session_uri == session_uri
            && previous.state == "bound"
        {
            return Ok(previous.clone());
        }
        if previous.active_turn_id.is_some() || previous.queued_message_count != 0 {
            bail!("cannot switch AHP Session while a Turn or queued message is active");
        }
        let pending_interactions: i64 = connection.query_row(
            "SELECT
                (SELECT COUNT(*) FROM ahp_approvals
                 WHERE state IN ('pending', 'submitted')) +
                (SELECT COUNT(*) FROM ahp_inputs
                 WHERE state IN ('pending', 'submitted'))",
            [],
            |row| row.get(0),
        )?;
        if pending_interactions != 0 {
            bail!("cannot switch AHP Session while input is pending");
        }
    }
    let (host_instance_id, target_status): (String, i64) = connection
        .query_row(
            "SELECT host_instance_id, status FROM ahp_session_catalog
             WHERE endpoint_id = ?1 AND session_uri = ?2 AND available = 1",
            params![endpoint_id, session_uri],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?
        .context("AHP Session is not present in the current catalogue")?;
    if switching_session && target_status & 1 == 0 {
        bail!("cannot switch to an AHP Session that is not idle");
    }
    let generation = previous.map_or(1, |binding| binding.generation + 1);
    connection.execute(
        "UPDATE ahp_commands
         SET state = 'failed', error_code = 'session_switched', updated_at = ?1
         WHERE state IN ('pending', 'leased')",
        [now],
    )?;
    connection.execute(
        "INSERT INTO ahp_binding(
            singleton, generation, endpoint_id, host_instance_id, session_uri,
            state, last_server_sequence, active_turn_id, queued_message_count, updated_at
         ) VALUES (1, ?1, ?2, ?3, ?4, 'binding', 0, NULL, 0, ?5)
         ON CONFLICT(singleton) DO UPDATE SET
            generation = excluded.generation,
            endpoint_id = excluded.endpoint_id,
            host_instance_id = excluded.host_instance_id,
            session_uri = excluded.session_uri,
            chat_uri = NULL,
            state = 'binding',
            last_server_sequence = 0,
            active_turn_id = NULL,
            queued_message_count = 0,
            error_code = NULL,
            updated_at = excluded.updated_at",
        params![generation, endpoint_id, host_instance_id, session_uri, now],
    )?;
    enqueue_command(
        connection,
        &format!("bind:{generation}"),
        generation,
        AhpCommandKind::BindSession,
        &json!({
            "endpoint_id": endpoint_id,
            "host_instance_id": host_instance_id,
            "session_uri": session_uri,
        }),
        now,
    )?;
    Ok(AhpBindingRecord {
        generation,
        endpoint_id: endpoint_id.to_owned(),
        host_instance_id: Some(host_instance_id),
        session_uri: session_uri.to_owned(),
        chat_uri: None,
        state: "binding".to_owned(),
        last_server_sequence: 0,
        active_turn_id: None,
        queued_message_count: 0,
    })
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
            status, workspace_uris_json, created_at, modified_at, available, last_seen_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 1, ?11)
         ON CONFLICT(session_uri) DO UPDATE SET
            endpoint_id = excluded.endpoint_id,
            host_instance_id = excluded.host_instance_id,
            provider = excluded.provider,
            title = excluded.title,
            status = excluded.status,
            workspace_uris_json = excluded.workspace_uris_json,
            created_at = excluded.created_at,
            modified_at = excluded.modified_at,
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

fn fail_binding_for_host_change(connection: &Connection, now: i64) -> Result<()> {
    connection.execute(
        "UPDATE ahp_binding
         SET generation = generation + 1, state = 'lost', chat_uri = NULL,
             error_code = 'host_restarted', updated_at = ?1
         WHERE singleton = 1",
        [now],
    )?;
    connection.execute(
        "UPDATE ahp_commands
         SET state = 'failed', error_code = 'host_restarted', updated_at = ?1
         WHERE state IN ('pending', 'leased')",
        [now],
    )?;
    connection.execute(
        "UPDATE ahp_approvals
         SET state = 'failed', updated_at = ?1
         WHERE state IN ('pending', 'submitted')",
        [now],
    )?;
    connection.execute(
        "UPDATE ahp_inputs
         SET state = 'failed', updated_at = ?1
         WHERE state IN ('pending', 'submitted')",
        [now],
    )?;
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
        }
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
    fn session_codes_are_stable_and_active_turn_blocks_switching() {
        let (_directory, database, binding) = bound_database();
        let original = database
            .ahp_list_sessions()
            .expect("sessions")
            .into_iter()
            .find(|session| session.session_uri == "copilot:/session-1")
            .expect("original session");
        let original_code = original.short_code.expect("short code");
        let second = AhpSessionDescriptor {
            short_code: None,
            endpoint_id: "endpoint-1".to_owned(),
            host_instance_id: "host-1".to_owned(),
            session_uri: "copilot:/session-2".to_owned(),
            provider: "copilot".to_owned(),
            title: "Second session".to_owned(),
            status: 1,
            workspace_uris: vec!["file:///c%3A/test".to_owned()],
            created_at: "2026-08-27T00:01:00Z".to_owned(),
            modified_at: "2026-08-27T00:01:00Z".to_owned(),
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
        assert!(
            database
                .ahp_bind_session("endpoint-1", "copilot:/session-2")
                .is_err()
        );
        database
            .ahp_publish_events(
                "adapter-stable",
                "adapter-run-1",
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
    fn switch_rejects_busy_target_and_keeps_single_binding() {
        let (_directory, database, original_binding) = bound_database();
        let busy = AhpSessionDescriptor {
            short_code: None,
            endpoint_id: "endpoint-1".to_owned(),
            host_instance_id: "host-1".to_owned(),
            session_uri: "copilot:/busy-session".to_owned(),
            provider: "copilot".to_owned(),
            title: "Busy session".to_owned(),
            status: 1 << 3,
            workspace_uris: vec!["file:///c%3A/other".to_owned()],
            created_at: "2026-08-27T00:01:00Z".to_owned(),
            modified_at: "2026-08-27T00:01:00Z".to_owned(),
        };
        database
            .ahp_replace_catalog(
                "adapter-stable",
                "adapter-run-1",
                &[host("host-1")],
                &[session("host-1"), busy],
            )
            .expect("catalogue");

        assert!(
            database
                .ahp_bind_session("endpoint-1", "copilot:/busy-session")
                .is_err()
        );
        let binding = database
            .ahp_binding()
            .expect("binding query")
            .expect("single binding");
        assert_eq!(binding.generation, original_binding.generation);
        assert_eq!(binding.session_uri, "copilot:/session-1");
    }

    #[test]
    fn switch_button_cannot_bind_outside_allowed_session_set() {
        let (_directory, database, original_binding) = bound_database();
        let outside = AhpSessionDescriptor {
            short_code: None,
            endpoint_id: "endpoint-1".to_owned(),
            host_instance_id: "host-1".to_owned(),
            session_uri: "copilot:/outside-session".to_owned(),
            provider: "copilot".to_owned(),
            title: "Outside session".to_owned(),
            status: 1,
            workspace_uris: vec!["file:///c%3A/outside".to_owned()],
            created_at: "2026-08-27T00:01:00Z".to_owned(),
            modified_at: "2026-08-27T00:01:00Z".to_owned(),
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
        assert!(
            database
                .ahp_list_sessions()
                .expect("empty sessions")
                .is_empty()
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
                "ahp-chat://default/session",
                10,
            )
            .expect("binding ready");
        let binding = database.ahp_binding().expect("binding").expect("bound");
        (directory, database, binding)
    }
}
