use std::{
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use rusqlite::{
    Connection, Error as SqliteError, ErrorCode, OptionalExtension, Transaction,
    TransactionBehavior, params,
};
use serde::Serialize;
use uuid::Uuid;

use crate::{
    protocol::{ApprovalState, RiskLevel, StopDecision, StopResult},
    security::{constant_time_eq, random_code, sha256_hex},
};

#[derive(Clone)]
pub struct Database {
    connection: Arc<Mutex<Connection>>,
}

#[derive(Debug, Clone)]
pub struct Owner {
    pub user_openid: String,
    pub enabled: bool,
}

#[derive(Debug, Clone)]
pub struct SessionRecord {
    pub session_id: String,
    pub short_code: String,
    pub state: String,
    pub final_sent: bool,
}

#[derive(Debug, Clone)]
pub struct NewApproval {
    pub approval_id: Uuid,
    pub idempotency_key: String,
    pub session_id: String,
    pub tool_use_id: String,
    pub tool_name: String,
    pub input_hash: String,
    pub redacted_summary: String,
    pub risk: RiskLevel,
    pub expires_at: i64,
}

#[derive(Debug, Clone)]
pub struct ApprovalRecord {
    pub approval_id: Uuid,
    pub short_code: String,
    pub idempotency_key: String,
    pub session_id: String,
    pub tool_use_id: String,
    pub tool_name: String,
    pub input_hash: String,
    pub redacted_summary: String,
    pub risk: RiskLevel,
    pub state: ApprovalState,
    pub expires_at: i64,
}

#[derive(Debug, Clone)]
pub struct BeginApproval {
    pub record: ApprovalRecord,
    pub created: bool,
}

#[derive(Debug, Clone)]
pub struct ApprovalDecision {
    pub approval_id: Uuid,
    pub session_id: String,
    pub short_code: String,
    pub state: ApprovalState,
}

#[derive(Debug, Clone)]
pub struct NewQuestion {
    pub question_id: Uuid,
    pub session_id: String,
    pub question_hash: String,
    pub choices_json: String,
    pub allow_freeform: bool,
    pub expires_at: i64,
}

#[derive(Debug, Clone)]
pub struct QuestionRecord {
    pub question_id: Uuid,
    pub short_code: String,
    pub session_id: String,
    pub choices_json: String,
    pub allow_freeform: bool,
    pub state: String,
    pub answer: Option<String>,
    pub expires_at: i64,
}

#[derive(Debug, Clone)]
pub struct NewDelivery {
    pub delivery_id: Uuid,
    pub idempotency_key: String,
    pub kind: String,
    pub session_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DeliveryRecord {
    pub delivery_id: Uuid,
    pub status: String,
    pub qq_message_id: Option<String>,
    pub error_code: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BeginDelivery {
    pub record: DeliveryRecord,
    pub created: bool,
}

#[derive(Debug, Clone)]
pub struct GatewayResume {
    pub session_id: String,
    pub sequence: u64,
    pub resume_gateway_url: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusSnapshot {
    pub owner_bound: bool,
    pub owner_enabled: bool,
    pub qq_gateway: GatewayRuntimeStatus,
    pub sessions: Vec<SessionStatus>,
    pub pending_approvals: Vec<PendingApprovalStatus>,
}

#[derive(Debug, Clone, Serialize)]
pub struct GatewayRuntimeStatus {
    pub state: String,
    pub last_seen_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionStatus {
    pub session_label: String,
    pub state: String,
    pub final_sent: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct PendingApprovalStatus {
    pub approval_code: String,
    pub session_label: String,
    pub tool_name: String,
    pub expires_at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindOutcome {
    Bound,
    AlreadyBound,
    InvalidOrExpired,
}

impl Database {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("failed to create database directory {}", parent.display())
            })?;
        }
        let connection = Connection::open(path)
            .with_context(|| format!("failed to open database {}", path.display()))?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .context("failed to set SQLite busy timeout")?;
        connection
            .execute_batch(
                r#"
                PRAGMA foreign_keys = ON;
                PRAGMA journal_mode = WAL;
                PRAGMA synchronous = FULL;
                PRAGMA secure_delete = ON;

                CREATE TABLE IF NOT EXISTS owner (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    user_openid TEXT NOT NULL UNIQUE,
                    bound_at INTEGER NOT NULL,
                    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1))
                );

                CREATE TABLE IF NOT EXISTS binding_codes (
                    code_hash TEXT PRIMARY KEY,
                    created_at INTEGER NOT NULL,
                    expires_at INTEGER NOT NULL,
                    consumed_at INTEGER
                );

                CREATE TABLE IF NOT EXISTS agent_sessions (
                    session_id TEXT PRIMARY KEY,
                    short_code TEXT NOT NULL UNIQUE,
                    workspace_hash TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (state IN ('active', 'waiting', 'stopped', 'lost')),
                    final_sent INTEGER NOT NULL DEFAULT 0 CHECK (final_sent IN (0, 1)),
                    stop_blocked_once INTEGER NOT NULL DEFAULT 0 CHECK (stop_blocked_once IN (0, 1)),
                    cancel_requested INTEGER NOT NULL DEFAULT 0 CHECK (cancel_requested IN (0, 1)),
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS approvals (
                    approval_id TEXT PRIMARY KEY,
                    short_code TEXT NOT NULL UNIQUE,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    session_id TEXT NOT NULL REFERENCES agent_sessions(session_id),
                    tool_use_id TEXT NOT NULL,
                    tool_name TEXT NOT NULL,
                    input_hash TEXT NOT NULL,
                    redacted_summary TEXT NOT NULL,
                    risk TEXT NOT NULL CHECK (risk IN ('low', 'medium', 'high', 'forbidden')),
                    state TEXT NOT NULL CHECK (
                        state IN ('pending', 'allowed', 'denied', 'expired', 'completed', 'unknown_failure')
                    ),
                    decided_by_message_id TEXT,
                    expires_at INTEGER NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_approvals_session_state
                    ON approvals(session_id, state);
                CREATE INDEX IF NOT EXISTS idx_approvals_tool_use
                    ON approvals(session_id, tool_use_id);

                CREATE TABLE IF NOT EXISTS questions (
                    question_id TEXT PRIMARY KEY,
                    short_code TEXT NOT NULL UNIQUE,
                    session_id TEXT NOT NULL REFERENCES agent_sessions(session_id),
                    question_hash TEXT NOT NULL,
                    choices_json TEXT NOT NULL,
                    allow_freeform INTEGER NOT NULL CHECK (allow_freeform IN (0, 1)),
                    state TEXT NOT NULL CHECK (state IN ('pending', 'answered', 'expired', 'cancelled')),
                    answer_message_id TEXT,
                    answer TEXT,
                    expires_at INTEGER NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_questions_session_state
                    ON questions(session_id, state);

                CREATE TABLE IF NOT EXISTS inbound_messages (
                    message_id TEXT PRIMARY KEY,
                    user_openid TEXT NOT NULL,
                    session_id TEXT REFERENCES agent_sessions(session_id),
                    kind TEXT NOT NULL DEFAULT 'received',
                    content TEXT,
                    consumed_at INTEGER,
                    expires_at INTEGER,
                    created_at INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_inbound_queue
                    ON inbound_messages(session_id, kind, consumed_at, created_at);

                CREATE TABLE IF NOT EXISTS deliveries (
                    delivery_id TEXT PRIMARY KEY,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    kind TEXT NOT NULL,
                    session_id TEXT REFERENCES agent_sessions(session_id),
                    qq_message_id TEXT,
                    status TEXT NOT NULL CHECK (status IN ('pending', 'sent', 'failed', 'in_doubt')),
                    error_code TEXT,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_deliveries_status_time
                    ON deliveries(status, created_at);

                CREATE TABLE IF NOT EXISTS sent_message_events (
                    delivery_id TEXT NOT NULL REFERENCES deliveries(delivery_id),
                    message_index INTEGER NOT NULL,
                    sent_at INTEGER NOT NULL,
                    PRIMARY KEY(delivery_id, message_index)
                );
                CREATE INDEX IF NOT EXISTS idx_sent_message_events_time
                    ON sent_message_events(sent_at);

                CREATE TABLE IF NOT EXISTS gateway_state (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    session_id TEXT NOT NULL,
                    sequence INTEGER NOT NULL,
                    resume_gateway_url TEXT,
                    updated_at INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS gateway_runtime (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    state TEXT NOT NULL CHECK (
                        state IN ('starting', 'connecting', 'connected', 'reconnecting', 'stopped')
                    ),
                    last_seen_at INTEGER,
                    updated_at INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS audit_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    occurred_at INTEGER NOT NULL,
                    session_code TEXT,
                    event_type TEXT NOT NULL,
                    tool_name TEXT,
                    input_hash TEXT,
                    risk TEXT,
                    outcome TEXT NOT NULL,
                    qq_message_id TEXT,
                    detail_redacted TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_audit_occurred_at ON audit_events(occurred_at);
                "#,
            )
            .context("failed to initialize database schema")?;
        crate::ahp_store::initialize_schema(&connection)?;

        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    pub fn recover_interrupted_state(&self) -> Result<()> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction
            .execute(
                "UPDATE deliveries SET status = 'in_doubt', error_code = 'daemon_restarted',
                     updated_at = ?1
                 WHERE status = 'pending'",
                [now],
            )
            .context("failed to recover interrupted deliveries")?;
        transaction
            .execute(
                "UPDATE ahp_projections
                 SET attempts = attempts + 1, last_error = 'daemon_restarted', updated_at = ?1
                 WHERE state = 'pending' AND last_error IS NULL AND replay_key IS NULL
                   AND NOT EXISTS (
                        SELECT 1 FROM deliveries
                        WHERE idempotency_key =
                                  'ahp-event-projection:' || ahp_projections.event_id
                   )",
                [now],
            )
            .context("failed to recover orphaned AHP projections")?;
        transaction
            .execute(
                "UPDATE approvals SET state = 'denied', updated_at = ?1
                 WHERE state = 'pending'",
                [now],
            )
            .context("failed to deny approvals interrupted by daemon restart")?;
        transaction
            .execute(
                "UPDATE approvals SET state = 'unknown_failure', updated_at = ?1
                 WHERE state = 'allowed'",
                [now],
            )
            .context("failed to close interrupted approved tools")?;
        transaction
            .execute(
                "UPDATE questions SET state = 'cancelled', answer = NULL, updated_at = ?1
                 WHERE state = 'pending'",
                [now],
            )
            .context("failed to cancel interrupted questions")?;
        transaction
            .execute(
                "UPDATE inbound_messages
                 SET kind = 'discarded', content = NULL, consumed_at = ?1
                 WHERE kind = 'queued' AND consumed_at IS NULL",
                [now],
            )
            .context("failed to discard interrupted remote message queue")?;
        transaction
            .execute(
                "UPDATE agent_sessions SET state = 'lost', updated_at = ?1
                 WHERE state IN ('active', 'waiting')",
                [now],
            )
            .context("failed to mark interrupted Agent sessions lost")?;
        transaction.commit()?;
        Ok(())
    }

    pub fn owner(&self) -> Result<Option<Owner>> {
        self.connection()?
            .query_row(
                "SELECT user_openid, enabled FROM owner WHERE singleton = 1",
                [],
                |row| {
                    Ok(Owner {
                        user_openid: row.get(0)?,
                        enabled: row.get::<_, i64>(1)? != 0,
                    })
                },
            )
            .optional()
            .context("failed to query owner")
    }

    pub fn create_binding_code(&self, ttl_seconds: u64) -> Result<String> {
        if self.owner()?.is_some() {
            bail!("an owner is already bound");
        }
        let now = now();
        let expires_at = now + seconds(ttl_seconds)?;
        let code = random_code(Some('B'), 7);
        let code_hash = sha256_hex(code.as_bytes());
        let connection = self.connection()?;
        connection.execute(
            "DELETE FROM binding_codes WHERE consumed_at IS NOT NULL OR expires_at <= ?1",
            [now],
        )?;
        connection.execute(
            "INSERT INTO binding_codes(code_hash, created_at, expires_at) VALUES (?1, ?2, ?3)",
            params![code_hash, now, expires_at],
        )?;
        audit(
            &connection,
            None,
            "binding_code_created",
            None,
            None,
            None,
            "created",
            None,
            None,
        )?;
        Ok(code)
    }

    pub fn try_bind_owner(
        &self,
        user_openid: &str,
        code: &str,
        message_id: &str,
    ) -> Result<BindOutcome> {
        let now = now();
        let code_hash = sha256_hex(code.trim().to_ascii_uppercase().as_bytes());
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;

        if transaction
            .query_row("SELECT 1 FROM owner WHERE singleton = 1", [], |_| Ok(()))
            .optional()?
            .is_some()
        {
            return Ok(BindOutcome::AlreadyBound);
        }

        let stored_hash: Option<String> = transaction
            .query_row(
                "SELECT code_hash FROM binding_codes
                 WHERE consumed_at IS NULL AND expires_at > ?1
                 ORDER BY created_at DESC LIMIT 1",
                [now],
                |row| row.get(0),
            )
            .optional()?;
        let Some(stored_hash) = stored_hash else {
            return Ok(BindOutcome::InvalidOrExpired);
        };
        if !constant_time_eq(&stored_hash, &code_hash) {
            return Ok(BindOutcome::InvalidOrExpired);
        }

        transaction.execute(
            "INSERT INTO owner(singleton, user_openid, bound_at, enabled)
             VALUES (1, ?1, ?2, 1)",
            params![user_openid, now],
        )?;
        transaction.execute(
            "UPDATE binding_codes SET consumed_at = ?1 WHERE code_hash = ?2",
            params![now, stored_hash],
        )?;
        audit(
            &transaction,
            None,
            "owner_bound",
            None,
            None,
            None,
            "bound",
            Some(message_id),
            None,
        )?;
        transaction.commit()?;
        Ok(BindOutcome::Bound)
    }

    pub fn set_owner_enabled(&self, enabled: bool) -> Result<()> {
        let connection = self.connection()?;
        let changed = connection.execute(
            "UPDATE owner SET enabled = ?1 WHERE singleton = 1",
            [i64::from(enabled)],
        )?;
        if changed == 0 {
            bail!("no owner is bound");
        }
        audit(
            &connection,
            None,
            "emergency_switch",
            None,
            None,
            None,
            if enabled { "enabled" } else { "disabled" },
            None,
            None,
        )?;
        Ok(())
    }

    pub fn register_session(
        &self,
        session_id: &str,
        workspace_hash: &str,
    ) -> Result<SessionRecord> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) = session_by_id(&transaction, session_id)? {
            transaction.execute(
                "UPDATE agent_sessions
                 SET workspace_hash = ?1, state = 'active', final_sent = 0,
                     stop_blocked_once = 0, cancel_requested = 0, updated_at = ?2
                 WHERE session_id = ?3",
                params![workspace_hash, now, session_id],
            )?;
            audit(
                &transaction,
                Some(&existing.short_code),
                "session_started",
                None,
                None,
                None,
                "restarted",
                None,
                None,
            )?;
            transaction.commit()?;
            drop(connection);
            return self
                .session_by_id(session_id)?
                .context("session disappeared after update");
        }

        let short_code =
            insert_session_with_unique_code(&transaction, session_id, workspace_hash, now)?;
        audit(
            &transaction,
            Some(&short_code),
            "session_started",
            None,
            None,
            None,
            "created",
            None,
            None,
        )?;
        transaction.commit()?;
        drop(connection);
        self.session_by_id(session_id)?
            .context("session disappeared after insert")
    }

    pub fn session_by_id(&self, session_id: &str) -> Result<Option<SessionRecord>> {
        session_by_id(&*self.connection()?, session_id)
    }

    pub fn resolve_session(
        &self,
        session_id: Option<&str>,
        session_label: Option<&str>,
    ) -> Result<SessionRecord> {
        let connection = self.connection()?;
        if let Some(session_id) = session_id.filter(|value| !value.trim().is_empty()) {
            let session = session_by_id(&connection, session_id)?
                .with_context(|| format!("unknown session ID {session_id}"))?;
            if let Some(label) = session_label.filter(|value| !value.trim().is_empty())
                && !session.short_code.eq_ignore_ascii_case(label)
            {
                bail!("session label does not match the authenticated MCP session");
            }
            if !matches!(session.state.as_str(), "active" | "waiting") {
                bail!("session {} is not active", session.short_code);
            }
            return Ok(session);
        }

        if let Some(label) = session_label.filter(|value| !value.trim().is_empty()) {
            return connection
                .query_row(
                    "SELECT session_id, short_code, state, final_sent
                     FROM agent_sessions
                     WHERE short_code = ?1 AND state IN ('active', 'waiting')",
                    [label.to_ascii_uppercase()],
                    map_session,
                )
                .optional()?
                .with_context(|| format!("unknown or inactive session {label}"));
        }

        let mut statement = connection.prepare(
            "SELECT session_id, short_code, state, final_sent
             FROM agent_sessions
             WHERE state IN ('active', 'waiting')
             ORDER BY updated_at DESC LIMIT 2",
        )?;
        let sessions: Vec<_> = statement
            .query_map([], map_session)?
            .collect::<rusqlite::Result<_>>()?;
        match sessions.as_slice() {
            [session] => Ok(session.clone()),
            [] => bail!("there is no active Agent session"),
            _ => bail!("multiple Agent sessions are active; provide a session label"),
        }
    }

    pub fn begin_approval(&self, approval: NewApproval) -> Result<BeginApproval> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        expire_pending(&transaction, now)?;

        if let Some(existing) = approval_by_idempotency(&transaction, &approval.idempotency_key)? {
            transaction.commit()?;
            return Ok(BeginApproval {
                record: existing,
                created: false,
            });
        }

        let session_state: Option<String> = transaction
            .query_row(
                "SELECT state FROM agent_sessions WHERE session_id = ?1",
                [&approval.session_id],
                |row| row.get(0),
            )
            .optional()?;
        if !matches!(session_state.as_deref(), Some("active" | "waiting")) {
            bail!("Agent session is not active");
        }

        let pending_count: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM approvals WHERE session_id = ?1 AND state = 'pending'",
            [&approval.session_id],
            |row| row.get(0),
        )?;
        if pending_count != 0 {
            bail!("this Agent session already has a pending approval");
        }

        let short_code = insert_approval_with_unique_code(&transaction, &approval, now)?;
        let session_code: String = transaction.query_row(
            "SELECT short_code FROM agent_sessions WHERE session_id = ?1",
            [&approval.session_id],
            |row| row.get(0),
        )?;
        audit(
            &transaction,
            Some(&session_code),
            "approval_created",
            Some(&approval.tool_name),
            Some(&approval.input_hash),
            Some(approval.risk.as_str()),
            "pending",
            None,
            Some(&short_code),
        )?;
        transaction.commit()?;
        drop(connection);
        let record = self
            .approval_by_id(approval.approval_id)?
            .context("approval disappeared after insert")?;
        Ok(BeginApproval {
            record,
            created: true,
        })
    }

    pub fn approval_by_id(&self, approval_id: Uuid) -> Result<Option<ApprovalRecord>> {
        self.connection()?
            .query_row(
                "SELECT approval_id, short_code, idempotency_key, session_id, tool_use_id,
                        tool_name, input_hash, redacted_summary, risk, state, expires_at
                 FROM approvals WHERE approval_id = ?1",
                [approval_id.to_string()],
                map_approval,
            )
            .optional()
            .context("failed to query approval")
    }

    pub fn expire_approval(&self, approval_id: Uuid) -> Result<()> {
        let now = now();
        let connection = self.connection()?;
        connection.execute(
            "UPDATE approvals SET state = 'expired', updated_at = ?1
             WHERE approval_id = ?2 AND state = 'pending'",
            params![now, approval_id.to_string()],
        )?;
        Ok(())
    }

    pub fn deny_approval_delivery_failure(&self, approval_id: Uuid, outcome: &str) -> Result<()> {
        let now = now();
        let connection = self.connection()?;
        connection.execute(
            "UPDATE approvals SET state = 'denied', updated_at = ?1
             WHERE approval_id = ?2 AND state = 'pending'",
            params![now, approval_id.to_string()],
        )?;
        audit(
            &connection,
            None,
            "approval_notification_failed",
            None,
            None,
            None,
            outcome,
            None,
            None,
        )?;
        Ok(())
    }

    pub fn decide_approval(
        &self,
        short_code: &str,
        allow: bool,
        message_id: &str,
    ) -> Result<Option<ApprovalDecision>> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        expire_pending(&transaction, now)?;
        let record: Option<ApprovalRecord> = transaction
            .query_row(
                "SELECT approval_id, short_code, idempotency_key, session_id, tool_use_id,
                        tool_name, input_hash, redacted_summary, risk, state, expires_at
                 FROM approvals WHERE short_code = ?1",
                [short_code.to_ascii_uppercase()],
                map_approval,
            )
            .optional()?;
        let Some(record) = record else {
            return Ok(None);
        };
        if record.state != ApprovalState::Pending || record.expires_at <= now {
            return Ok(Some(ApprovalDecision {
                approval_id: record.approval_id,
                session_id: record.session_id,
                short_code: record.short_code,
                state: record.state,
            }));
        }

        let state = if allow {
            ApprovalState::Allowed
        } else {
            ApprovalState::Denied
        };
        let changed = transaction.execute(
            "UPDATE approvals
             SET state = ?1, decided_by_message_id = ?2, updated_at = ?3
             WHERE approval_id = ?4 AND state = 'pending'",
            params![
                state.as_str(),
                message_id,
                now,
                record.approval_id.to_string()
            ],
        )?;
        if changed != 1 {
            bail!("approval state changed concurrently");
        }
        let session_code: String = transaction.query_row(
            "SELECT short_code FROM agent_sessions WHERE session_id = ?1",
            [&record.session_id],
            |row| row.get(0),
        )?;
        audit(
            &transaction,
            Some(&session_code),
            "approval_decided",
            Some(&record.tool_name),
            Some(&record.input_hash),
            Some(record.risk.as_str()),
            state.as_str(),
            Some(message_id),
            Some(&record.short_code),
        )?;
        transaction.commit()?;
        Ok(Some(ApprovalDecision {
            approval_id: record.approval_id,
            session_id: record.session_id,
            short_code: record.short_code,
            state,
        }))
    }

    pub fn complete_approval(&self, session_id: &str, tool_use_id: &str) -> Result<bool> {
        let now = now();
        let connection = self.connection()?;
        let changed = connection.execute(
            "UPDATE approvals SET state = 'completed', updated_at = ?1
             WHERE session_id = ?2 AND tool_use_id = ?3 AND state = 'allowed'",
            params![now, session_id, tool_use_id],
        )?;
        if changed != 0 {
            let session_code = session_code(&connection, session_id)?;
            audit(
                &connection,
                session_code.as_deref(),
                "tool_completed",
                None,
                None,
                None,
                "completed",
                None,
                Some(tool_use_id),
            )?;
        }
        Ok(changed != 0)
    }

    pub fn approval_for_tool(
        &self,
        session_id: &str,
        tool_use_id: &str,
    ) -> Result<Option<ApprovalRecord>> {
        self.connection()?
            .query_row(
                "SELECT approval_id, short_code, idempotency_key, session_id, tool_use_id,
                        tool_name, input_hash, redacted_summary, risk, state, expires_at
                 FROM approvals
                 WHERE session_id = ?1 AND tool_use_id = ?2
                 ORDER BY created_at DESC LIMIT 1",
                params![session_id, tool_use_id],
                map_approval,
            )
            .optional()
            .context("failed to query approval for tool call")
    }

    pub fn set_final_sent(&self, session_id: &str) -> Result<()> {
        let now = now();
        let connection = self.connection()?;
        let changed = connection.execute(
            "UPDATE agent_sessions SET final_sent = 1, updated_at = ?1
             WHERE session_id = ?2 AND state IN ('active', 'waiting')",
            params![now, session_id],
        )?;
        if changed != 1 {
            bail!("cannot mark final delivery for an inactive session");
        }
        Ok(())
    }

    pub fn set_session_waiting(&self, session_id: &str, waiting: bool) -> Result<()> {
        let from = if waiting { "active" } else { "waiting" };
        let to = if waiting { "waiting" } else { "active" };
        self.connection()?.execute(
            "UPDATE agent_sessions SET state = ?1, updated_at = ?2
             WHERE session_id = ?3 AND state = ?4",
            params![to, now(), session_id, from],
        )?;
        Ok(())
    }

    pub fn stop_session(&self, session_id: &str, stop_hook_active: bool) -> Result<StopResult> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let state: Option<(String, bool, bool)> = transaction
            .query_row(
                "SELECT state, final_sent, stop_blocked_once
                 FROM agent_sessions WHERE session_id = ?1",
                [session_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get::<_, i64>(1)? != 0,
                        row.get::<_, i64>(2)? != 0,
                    ))
                },
            )
            .optional()?;
        let Some((state, final_sent, blocked_once)) = state else {
            bail!("unknown Agent session");
        };
        if state == "stopped" || state == "lost" || final_sent {
            transaction.execute(
                "UPDATE approvals SET state = 'unknown_failure', updated_at = ?1
                 WHERE session_id = ?2 AND state = 'allowed'",
                params![now, session_id],
            )?;
            transaction.execute(
                "UPDATE agent_sessions SET state = 'stopped', updated_at = ?1
                 WHERE session_id = ?2",
                params![now, session_id],
            )?;
            transaction.commit()?;
            return Ok(StopResult {
                decision: StopDecision::Allow,
                reason: None,
            });
        }

        if !blocked_once && !stop_hook_active {
            transaction.execute(
                "UPDATE agent_sessions SET stop_blocked_once = 1, updated_at = ?1
                 WHERE session_id = ?2",
                params![now, session_id],
            )?;
            transaction.commit()?;
            return Ok(StopResult {
                decision: StopDecision::Block,
                reason: Some(
                    "Before stopping, call qq_send_final once with the exact final answer intended for the user. Do not send hidden reasoning, secrets, tool inputs, or transcripts."
                        .to_owned(),
                ),
            });
        }

        transaction.execute(
            "UPDATE approvals SET state = 'unknown_failure', updated_at = ?1
             WHERE session_id = ?2 AND state = 'allowed'",
            params![now, session_id],
        )?;
        transaction.execute(
            "UPDATE agent_sessions SET state = 'stopped', updated_at = ?1
             WHERE session_id = ?2",
            params![now, session_id],
        )?;
        transaction.commit()?;
        Ok(StopResult {
            decision: StopDecision::Allow,
            reason: Some(
                "Final QQ delivery was not confirmed; the one-time Stop guard is exhausted."
                    .to_owned(),
            ),
        })
    }

    pub fn create_question(&self, question: NewQuestion) -> Result<QuestionRecord> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let pending: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM questions WHERE session_id = ?1 AND state = 'pending'",
            [&question.session_id],
            |row| row.get(0),
        )?;
        if pending != 0 {
            bail!("this Agent session already has a pending question");
        }
        let short_code = insert_question_with_unique_code(&transaction, &question, now)?;
        transaction.execute(
            "UPDATE agent_sessions SET state = 'waiting', updated_at = ?1
             WHERE session_id = ?2 AND state = 'active'",
            params![now, question.session_id],
        )?;
        let session_code = session_code(&transaction, &question.session_id)?;
        audit(
            &transaction,
            session_code.as_deref(),
            "question_created",
            None,
            Some(&question.question_hash),
            None,
            "pending",
            None,
            Some(&short_code),
        )?;
        transaction.commit()?;
        drop(connection);
        self.question_by_id(question.question_id)?
            .context("question disappeared after insert")
    }

    pub fn question_by_id(&self, question_id: Uuid) -> Result<Option<QuestionRecord>> {
        self.connection()?
            .query_row(
                "SELECT question_id, short_code, session_id, choices_json, allow_freeform,
                        state, answer, expires_at
                 FROM questions WHERE question_id = ?1",
                [question_id.to_string()],
                map_question,
            )
            .optional()
            .context("failed to query question")
    }

    pub fn question_by_code(&self, short_code: &str) -> Result<Option<QuestionRecord>> {
        self.connection()?
            .query_row(
                "SELECT question_id, short_code, session_id, choices_json, allow_freeform,
                        state, answer, expires_at
                 FROM questions WHERE short_code = ?1",
                [short_code.to_ascii_uppercase()],
                map_question,
            )
            .optional()
            .context("failed to query question")
    }

    pub fn answer_question(
        &self,
        question_id: Uuid,
        answer: &str,
        message_id: &str,
    ) -> Result<bool> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let session_id: Option<String> = transaction
            .query_row(
                "SELECT session_id FROM questions
                 WHERE question_id = ?1 AND state = 'pending' AND expires_at > ?2",
                params![question_id.to_string(), now],
                |row| row.get(0),
            )
            .optional()?;
        let Some(session_id) = session_id else {
            return Ok(false);
        };
        let changed = transaction.execute(
            "UPDATE questions
             SET state = 'answered', answer_message_id = ?1, answer = ?2, updated_at = ?3
             WHERE question_id = ?4 AND state = 'pending'",
            params![message_id, answer, now, question_id.to_string()],
        )?;
        transaction.execute(
            "UPDATE agent_sessions SET state = 'active', updated_at = ?1
             WHERE session_id = ?2 AND state = 'waiting'",
            params![now, session_id],
        )?;
        transaction.commit()?;
        Ok(changed == 1)
    }

    pub fn expire_question(&self, question_id: Uuid) -> Result<()> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let session_id: Option<String> = transaction
            .query_row(
                "SELECT session_id FROM questions
                 WHERE question_id = ?1 AND state = 'pending'",
                [question_id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        transaction.execute(
            "UPDATE questions SET state = 'expired', updated_at = ?1
             WHERE question_id = ?2 AND state = 'pending'",
            params![now, question_id.to_string()],
        )?;
        if let Some(session_id) = session_id {
            transaction.execute(
                "UPDATE agent_sessions SET state = 'active', updated_at = ?1
                 WHERE session_id = ?2 AND state = 'waiting'",
                params![now, session_id],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn consume_question_answer(&self, question_id: Uuid) -> Result<()> {
        self.connection()?.execute(
            "UPDATE questions SET answer = NULL, updated_at = ?1
             WHERE question_id = ?2 AND state = 'answered'",
            params![now(), question_id.to_string()],
        )?;
        Ok(())
    }

    pub fn record_inbound(&self, message_id: &str, user_openid: &str) -> Result<bool> {
        let connection = self.connection()?;
        let changed = connection.execute(
            "INSERT OR IGNORE INTO inbound_messages(message_id, user_openid, created_at)
             VALUES (?1, ?2, ?3)",
            params![message_id, user_openid, now()],
        )?;
        Ok(changed == 1)
    }

    pub fn queue_message(
        &self,
        message_id: &str,
        session_id: &str,
        content: &str,
        ttl_seconds: u64,
    ) -> Result<()> {
        let now = now();
        let expires_at = now + seconds(ttl_seconds)?;
        let connection = self.connection()?;
        let changed = connection.execute(
            "UPDATE inbound_messages
             SET session_id = ?1, kind = 'queued', content = ?2, expires_at = ?3
             WHERE message_id = ?4 AND kind = 'received'",
            params![session_id, content, expires_at, message_id],
        )?;
        if changed != 1 {
            bail!("inbound message is not available for queueing");
        }
        Ok(())
    }

    pub fn mark_inbound_kind(&self, message_id: &str, kind: &str) -> Result<()> {
        self.connection()?.execute(
            "UPDATE inbound_messages SET kind = ?1, consumed_at = ?2 WHERE message_id = ?3",
            params![kind, now(), message_id],
        )?;
        Ok(())
    }

    pub fn take_next_message(&self, session_id: &str) -> Result<Option<(String, String)>> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let message: Option<(String, String)> = transaction
            .query_row(
                "SELECT message_id, content FROM inbound_messages
                 WHERE session_id = ?1 AND kind = 'queued' AND consumed_at IS NULL
                   AND (expires_at IS NULL OR expires_at > ?2)
                 ORDER BY created_at ASC LIMIT 1",
                params![session_id, now],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((message_id, _)) = &message {
            transaction.execute(
                "UPDATE inbound_messages
                 SET consumed_at = ?1, content = NULL, kind = 'consumed'
                 WHERE message_id = ?2 AND consumed_at IS NULL",
                params![now, message_id],
            )?;
        }
        transaction.commit()?;
        Ok(message)
    }

    pub fn cancel_session(&self, session_label: &str, message_id: &str) -> Result<bool> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let session_id: Option<String> = transaction
            .query_row(
                "SELECT session_id FROM agent_sessions
                 WHERE short_code = ?1 AND state IN ('active', 'waiting')",
                [session_label.to_ascii_uppercase()],
                |row| row.get(0),
            )
            .optional()?;
        let Some(session_id) = session_id else {
            return Ok(false);
        };
        transaction.execute(
            "UPDATE approvals SET state = 'denied', decided_by_message_id = ?1, updated_at = ?2
             WHERE session_id = ?3 AND state = 'pending'",
            params![message_id, now, session_id],
        )?;
        transaction.execute(
            "UPDATE questions SET state = 'cancelled', answer_message_id = ?1, updated_at = ?2
             WHERE session_id = ?3 AND state = 'pending'",
            params![message_id, now, session_id],
        )?;
        transaction.execute(
            "UPDATE agent_sessions
             SET state = 'stopped', cancel_requested = 1, updated_at = ?1
             WHERE session_id = ?2",
            params![now, session_id],
        )?;
        audit(
            &transaction,
            Some(&session_label.to_ascii_uppercase()),
            "session_cancelled",
            None,
            None,
            None,
            "cancelled",
            Some(message_id),
            None,
        )?;
        transaction.commit()?;
        Ok(true)
    }

    pub fn take_cancel_requested(&self, session_id: &str) -> Result<bool> {
        let connection = self.connection()?;
        let changed = connection.execute(
            "UPDATE agent_sessions SET cancel_requested = 0
             WHERE session_id = ?1 AND cancel_requested = 1",
            [session_id],
        )?;
        Ok(changed == 1)
    }

    pub fn begin_delivery(&self, delivery: NewDelivery) -> Result<BeginDelivery> {
        let now = now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) = delivery_by_key(&transaction, &delivery.idempotency_key)? {
            transaction.commit()?;
            return Ok(BeginDelivery {
                record: existing,
                created: false,
            });
        }
        transaction.execute(
            "INSERT INTO deliveries(
                delivery_id, idempotency_key, kind, session_id, status, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, 'pending', ?5, ?5)",
            params![
                delivery.delivery_id.to_string(),
                delivery.idempotency_key,
                delivery.kind,
                delivery.session_id,
                now
            ],
        )?;
        transaction.commit()?;
        Ok(BeginDelivery {
            record: DeliveryRecord {
                delivery_id: delivery.delivery_id,
                status: "pending".to_owned(),
                qq_message_id: None,
                error_code: None,
            },
            created: true,
        })
    }

    pub fn finish_delivery(
        &self,
        delivery_id: Uuid,
        status: &str,
        qq_message_id: Option<&str>,
        error_code: Option<&str>,
    ) -> Result<()> {
        if !matches!(status, "sent" | "failed" | "in_doubt") {
            bail!("invalid delivery terminal status");
        }
        let connection = self.connection()?;
        let changed = connection.execute(
            "UPDATE deliveries
             SET status = ?1, qq_message_id = ?2, error_code = ?3, updated_at = ?4
             WHERE delivery_id = ?5 AND status = 'pending'",
            params![
                status,
                qq_message_id,
                error_code,
                now(),
                delivery_id.to_string()
            ],
        )?;
        if changed != 1 {
            bail!("delivery is not pending");
        }
        Ok(())
    }

    pub fn delivery_by_id(&self, delivery_id: Uuid) -> Result<Option<DeliveryRecord>> {
        self.connection()?
            .query_row(
                "SELECT delivery_id, status, qq_message_id, error_code
                 FROM deliveries WHERE delivery_id = ?1",
                [delivery_id.to_string()],
                map_delivery,
            )
            .optional()
            .context("failed to query delivery")
    }

    pub fn delivery_by_idempotency_key(&self, key: &str) -> Result<Option<DeliveryRecord>> {
        let connection = self.connection()?;
        delivery_by_key(&connection, key)
    }

    pub fn sent_delivery_count_since(&self, since: i64) -> Result<u32> {
        self.connection()?
            .query_row(
                "SELECT COUNT(*) FROM sent_message_events WHERE sent_at >= ?1",
                [since],
                |row| row.get(0),
            )
            .context("failed to count deliveries")
    }

    pub fn record_sent_message(&self, delivery_id: Uuid, message_index: u32) -> Result<()> {
        self.connection()?.execute(
            "INSERT INTO sent_message_events(delivery_id, message_index, sent_at)
             VALUES (?1, ?2, ?3)",
            params![delivery_id.to_string(), i64::from(message_index), now()],
        )?;
        Ok(())
    }

    pub fn gateway_resume(&self) -> Result<Option<GatewayResume>> {
        self.connection()?
            .query_row(
                "SELECT session_id, sequence, resume_gateway_url
                 FROM gateway_state WHERE singleton = 1",
                [],
                |row| {
                    let sequence: i64 = row.get(1)?;
                    Ok(GatewayResume {
                        session_id: row.get(0)?,
                        sequence: sequence.max(0) as u64,
                        resume_gateway_url: row.get(2)?,
                    })
                },
            )
            .optional()
            .context("failed to read gateway state")
    }

    pub fn save_gateway_resume(
        &self,
        session_id: &str,
        sequence: u64,
        resume_gateway_url: Option<&str>,
    ) -> Result<()> {
        let sequence = i64::try_from(sequence).context("gateway sequence exceeds SQLite range")?;
        self.connection()?.execute(
            "INSERT INTO gateway_state(singleton, session_id, sequence, resume_gateway_url, updated_at)
             VALUES (1, ?1, ?2, ?3, ?4)
             ON CONFLICT(singleton) DO UPDATE SET
                 session_id = excluded.session_id,
                 sequence = excluded.sequence,
                 resume_gateway_url = COALESCE(excluded.resume_gateway_url, gateway_state.resume_gateway_url),
                 updated_at = excluded.updated_at",
            params![session_id, sequence, resume_gateway_url, now()],
        )?;
        Ok(())
    }

    pub fn clear_gateway_resume(&self) -> Result<()> {
        self.connection()?
            .execute("DELETE FROM gateway_state WHERE singleton = 1", [])?;
        Ok(())
    }

    pub fn set_gateway_runtime(&self, state: &str, seen: bool) -> Result<()> {
        if !matches!(
            state,
            "starting" | "connecting" | "connected" | "reconnecting" | "stopped"
        ) {
            bail!("invalid QQ Gateway runtime state");
        }
        let now = now();
        self.connection()?.execute(
            "INSERT INTO gateway_runtime(singleton, state, last_seen_at, updated_at)
             VALUES (1, ?1, ?2, ?3)
             ON CONFLICT(singleton) DO UPDATE SET
                state = excluded.state,
                last_seen_at = CASE
                    WHEN ?4 = 1 THEN excluded.updated_at
                    ELSE gateway_runtime.last_seen_at
                END,
                updated_at = excluded.updated_at",
            params![state, seen.then_some(now), now, i64::from(seen)],
        )?;
        Ok(())
    }

    pub fn status_snapshot(&self) -> Result<StatusSnapshot> {
        let connection = self.connection()?;
        let owner = connection
            .query_row("SELECT enabled FROM owner WHERE singleton = 1", [], |row| {
                Ok(row.get::<_, i64>(0)? != 0)
            })
            .optional()?;
        let mut session_statement = connection.prepare(
            "SELECT short_code, state, final_sent FROM agent_sessions
             ORDER BY updated_at DESC LIMIT 20",
        )?;
        let sessions = session_statement
            .query_map([], |row| {
                Ok(SessionStatus {
                    session_label: row.get(0)?,
                    state: row.get(1)?,
                    final_sent: row.get::<_, i64>(2)? != 0,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut approval_statement = connection.prepare(
            "SELECT a.short_code, s.short_code, a.tool_name, a.expires_at
             FROM approvals a
             JOIN agent_sessions s ON s.session_id = a.session_id
             WHERE a.state = 'pending' AND a.expires_at > ?1
             ORDER BY a.created_at ASC",
        )?;
        let pending_approvals = approval_statement
            .query_map([now()], |row| {
                Ok(PendingApprovalStatus {
                    approval_code: row.get(0)?,
                    session_label: row.get(1)?,
                    tool_name: row.get(2)?,
                    expires_at: row.get(3)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        Ok(StatusSnapshot {
            owner_bound: owner.is_some(),
            owner_enabled: owner.unwrap_or(false),
            qq_gateway: connection
                .query_row(
                    "SELECT state, last_seen_at FROM gateway_runtime WHERE singleton = 1",
                    [],
                    |row| {
                        Ok(GatewayRuntimeStatus {
                            state: row.get(0)?,
                            last_seen_at: row.get(1)?,
                        })
                    },
                )
                .optional()?
                .unwrap_or(GatewayRuntimeStatus {
                    state: "stopped".to_owned(),
                    last_seen_at: None,
                }),
            sessions,
            pending_approvals,
        })
    }

    pub fn purge_expired(&self, retention_days: u32) -> Result<()> {
        let now = now();
        let audit_cutoff = now - i64::from(retention_days) * 86_400;
        let connection = self.connection()?;
        connection.execute(
            "UPDATE approvals SET state = 'expired', updated_at = ?1
             WHERE state = 'pending' AND expires_at <= ?1",
            [now],
        )?;
        connection.execute(
            "UPDATE questions SET state = 'expired', answer = NULL, updated_at = ?1
             WHERE state = 'pending' AND expires_at <= ?1",
            [now],
        )?;
        connection.execute(
            "DELETE FROM inbound_messages
             WHERE (expires_at IS NOT NULL AND expires_at <= ?1)
                OR (consumed_at IS NOT NULL AND consumed_at <= ?2)",
            params![now, now - 86_400],
        )?;
        connection.execute(
            "DELETE FROM binding_codes
             WHERE consumed_at IS NOT NULL OR expires_at <= ?1",
            [now],
        )?;
        connection.execute(
            "DELETE FROM sent_message_events
             WHERE sent_at < ?1
               AND NOT EXISTS (
                    SELECT 1
                    FROM deliveries d
                    JOIN ahp_projections p
                      ON p.state = 'pending'
                     AND (
                          d.idempotency_key =
                              'ahp-event-projection:' || p.event_id
                          OR d.idempotency_key = p.replay_key
                     )
                    WHERE d.delivery_id = sent_message_events.delivery_id
               )",
            [audit_cutoff],
        )?;
        connection.execute(
            "DELETE FROM deliveries
             WHERE status != 'pending' AND updated_at < ?1
               AND idempotency_key NOT IN (
                    SELECT 'ahp-event-projection:' || event_id
                    FROM ahp_projections WHERE state = 'pending'
                    UNION
                    SELECT replay_key FROM ahp_projections
                    WHERE state = 'pending' AND replay_key IS NOT NULL
               )",
            [audit_cutoff],
        )?;
        connection.execute(
            "DELETE FROM questions
             WHERE state != 'pending' AND updated_at < ?1",
            [audit_cutoff],
        )?;
        connection.execute(
            "DELETE FROM approvals
             WHERE state != 'pending' AND updated_at < ?1",
            [audit_cutoff],
        )?;
        connection.execute(
            "DELETE FROM agent_sessions
             WHERE state IN ('stopped', 'lost') AND updated_at < ?1",
            [audit_cutoff],
        )?;
        connection.execute(
            "DELETE FROM audit_events WHERE occurred_at < ?1",
            [audit_cutoff],
        )?;
        Ok(())
    }

    pub fn audit_security_event(
        &self,
        event_type: &str,
        outcome: &str,
        message_id: Option<&str>,
    ) -> Result<()> {
        audit(
            &*self.connection()?,
            None,
            event_type,
            None,
            None,
            None,
            outcome,
            message_id,
            None,
        )
    }

    pub(crate) fn connection(&self) -> Result<MutexGuard<'_, Connection>> {
        self.connection
            .lock()
            .map_err(|_| anyhow!("database mutex was poisoned"))
    }
}

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

fn seconds(value: u64) -> Result<i64> {
    i64::try_from(value).context("duration exceeds supported range")
}

fn session_by_id(connection: &Connection, session_id: &str) -> Result<Option<SessionRecord>> {
    connection
        .query_row(
            "SELECT session_id, short_code, state, final_sent
             FROM agent_sessions WHERE session_id = ?1",
            [session_id],
            map_session,
        )
        .optional()
        .context("failed to query Agent session")
}

fn map_session(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionRecord> {
    Ok(SessionRecord {
        session_id: row.get(0)?,
        short_code: row.get(1)?,
        state: row.get(2)?,
        final_sent: row.get::<_, i64>(3)? != 0,
    })
}

fn map_approval(row: &rusqlite::Row<'_>) -> rusqlite::Result<ApprovalRecord> {
    let approval_id: String = row.get(0)?;
    let risk: String = row.get(8)?;
    let state: String = row.get(9)?;
    Ok(ApprovalRecord {
        approval_id: Uuid::parse_str(&approval_id).map_err(|error| {
            SqliteError::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
        })?,
        short_code: row.get(1)?,
        idempotency_key: row.get(2)?,
        session_id: row.get(3)?,
        tool_use_id: row.get(4)?,
        tool_name: row.get(5)?,
        input_hash: row.get(6)?,
        redacted_summary: row.get(7)?,
        risk: parse_risk(&risk).map_err(|error| {
            SqliteError::FromSqlConversionFailure(8, rusqlite::types::Type::Text, error.into())
        })?,
        state: ApprovalState::try_from(state.as_str()).map_err(|error| {
            SqliteError::FromSqlConversionFailure(9, rusqlite::types::Type::Text, error.into())
        })?,
        expires_at: row.get(10)?,
    })
}

fn map_question(row: &rusqlite::Row<'_>) -> rusqlite::Result<QuestionRecord> {
    let question_id: String = row.get(0)?;
    Ok(QuestionRecord {
        question_id: Uuid::parse_str(&question_id).map_err(|error| {
            SqliteError::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
        })?,
        short_code: row.get(1)?,
        session_id: row.get(2)?,
        choices_json: row.get(3)?,
        allow_freeform: row.get::<_, i64>(4)? != 0,
        state: row.get(5)?,
        answer: row.get(6)?,
        expires_at: row.get(7)?,
    })
}

fn parse_risk(value: &str) -> Result<RiskLevel> {
    match value {
        "low" => Ok(RiskLevel::Low),
        "medium" => Ok(RiskLevel::Medium),
        "high" => Ok(RiskLevel::High),
        "forbidden" => Ok(RiskLevel::Forbidden),
        _ => bail!("unknown risk level {value}"),
    }
}

fn insert_session_with_unique_code(
    transaction: &Transaction<'_>,
    session_id: &str,
    workspace_hash: &str,
    now: i64,
) -> Result<String> {
    for _ in 0..32 {
        let code = random_code(Some('S'), 4);
        match transaction.execute(
            "INSERT INTO agent_sessions(
                session_id, short_code, workspace_hash, state, created_at, updated_at
             ) VALUES (?1, ?2, ?3, 'active', ?4, ?4)",
            params![session_id, code, workspace_hash, now],
        ) {
            Ok(_) => return Ok(code),
            Err(error) if is_constraint(&error) => continue,
            Err(error) => return Err(error.into()),
        }
    }
    bail!("failed to allocate a unique session code")
}

fn insert_approval_with_unique_code(
    transaction: &Transaction<'_>,
    approval: &NewApproval,
    now: i64,
) -> Result<String> {
    for _ in 0..32 {
        let code = random_code(None, 5);
        match transaction.execute(
            "INSERT INTO approvals(
                approval_id, short_code, idempotency_key, session_id, tool_use_id,
                tool_name, input_hash, redacted_summary, risk, state, expires_at,
                created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'pending', ?10, ?11, ?11)",
            params![
                approval.approval_id.to_string(),
                code,
                approval.idempotency_key,
                approval.session_id,
                approval.tool_use_id,
                approval.tool_name,
                approval.input_hash,
                approval.redacted_summary,
                approval.risk.as_str(),
                approval.expires_at,
                now
            ],
        ) {
            Ok(_) => return Ok(code),
            Err(error) if is_constraint(&error) => continue,
            Err(error) => return Err(error.into()),
        }
    }
    bail!("failed to allocate a unique approval code")
}

fn insert_question_with_unique_code(
    transaction: &Transaction<'_>,
    question: &NewQuestion,
    now: i64,
) -> Result<String> {
    for _ in 0..32 {
        let code = random_code(Some('Q'), 4);
        match transaction.execute(
            "INSERT INTO questions(
                question_id, short_code, session_id, question_hash, choices_json,
                allow_freeform, state, expires_at, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', ?7, ?8, ?8)",
            params![
                question.question_id.to_string(),
                code,
                question.session_id,
                question.question_hash,
                question.choices_json,
                i64::from(question.allow_freeform),
                question.expires_at,
                now
            ],
        ) {
            Ok(_) => return Ok(code),
            Err(error) if is_constraint(&error) => continue,
            Err(error) => return Err(error.into()),
        }
    }
    bail!("failed to allocate a unique question code")
}

fn is_constraint(error: &SqliteError) -> bool {
    matches!(
        error,
        SqliteError::SqliteFailure(details, _)
            if details.code == ErrorCode::ConstraintViolation
    )
}

fn approval_by_idempotency(connection: &Connection, key: &str) -> Result<Option<ApprovalRecord>> {
    connection
        .query_row(
            "SELECT approval_id, short_code, idempotency_key, session_id, tool_use_id,
                    tool_name, input_hash, redacted_summary, risk, state, expires_at
             FROM approvals WHERE idempotency_key = ?1",
            [key],
            map_approval,
        )
        .optional()
        .context("failed to query idempotent approval")
}

fn expire_pending(connection: &Connection, now: i64) -> Result<()> {
    connection.execute(
        "UPDATE approvals SET state = 'expired', updated_at = ?1
         WHERE state = 'pending' AND expires_at <= ?1",
        [now],
    )?;
    Ok(())
}

fn delivery_by_key(connection: &Connection, key: &str) -> Result<Option<DeliveryRecord>> {
    connection
        .query_row(
            "SELECT delivery_id, status, qq_message_id, error_code
             FROM deliveries WHERE idempotency_key = ?1",
            [key],
            map_delivery,
        )
        .optional()
        .context("failed to query idempotent delivery")
}

fn map_delivery(row: &rusqlite::Row<'_>) -> rusqlite::Result<DeliveryRecord> {
    let delivery_id: String = row.get(0)?;
    Ok(DeliveryRecord {
        delivery_id: Uuid::parse_str(&delivery_id).map_err(|error| {
            SqliteError::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
        })?,
        status: row.get(1)?,
        qq_message_id: row.get(2)?,
        error_code: row.get(3)?,
    })
}

fn session_code(connection: &Connection, session_id: &str) -> Result<Option<String>> {
    connection
        .query_row(
            "SELECT short_code FROM agent_sessions WHERE session_id = ?1",
            [session_id],
            |row| row.get(0),
        )
        .optional()
        .context("failed to query session code")
}

#[allow(clippy::too_many_arguments)]
fn audit(
    connection: &Connection,
    session_code: Option<&str>,
    event_type: &str,
    tool_name: Option<&str>,
    input_hash: Option<&str>,
    risk: Option<&str>,
    outcome: &str,
    qq_message_id: Option<&str>,
    detail_redacted: Option<&str>,
) -> Result<()> {
    connection.execute(
        "INSERT INTO audit_events(
            occurred_at, session_code, event_type, tool_name, input_hash,
            risk, outcome, qq_message_id, detail_redacted
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            now(),
            session_code,
            event_type,
            tool_name,
            input_hash,
            risk,
            outcome,
            qq_message_id,
            detail_redacted
        ],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn database() -> (tempfile::TempDir, Database) {
        let directory = tempfile::tempdir().expect("tempdir");
        let database = Database::open(&directory.path().join("test.sqlite3")).expect("database");
        (directory, database)
    }

    #[test]
    fn daemon_recovery_makes_interrupted_projection_replayable() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("test.sqlite3");
        let event_id = sha256_hex(b"interrupted-projection");
        let orphan_event_id = sha256_hex(b"orphaned-projection");
        let direct_delivery_key = format!("ahp-event-projection:{event_id}");
        {
            let database = Database::open(&path).expect("database");
            database
                .connection()
                .expect("connection")
                .execute(
                    "INSERT INTO ahp_events(
                        event_id, host_instance_id, binding_id, binding_generation,
                        session_uri, kind, occurred_at, data_redacted_json, created_at
                     ) VALUES (?1, 'host-1', 'binding-1', 1, 'copilot:/session-1',
                               'assistant_message', '2026-09-04T00:00:00Z', '{}', ?2)",
                    params![event_id, now()],
                )
                .expect("event");
            database
                .connection()
                .expect("connection")
                .execute(
                    "INSERT INTO ahp_events(
                        event_id, host_instance_id, binding_id, binding_generation,
                        session_uri, kind, occurred_at, data_redacted_json, created_at
                     ) VALUES (?1, 'host-1', 'binding-1', 1, 'copilot:/session-1',
                               'assistant_message', '2026-09-04T00:00:00Z', '{}', ?2)",
                    params![orphan_event_id, now()],
                )
                .expect("orphan event");
            database
                .ahp_queue_projection(&event_id, "ahp_assistant_message", "missed")
                .expect("projection");
            database
                .ahp_queue_projection(&orphan_event_id, "ahp_assistant_message", "orphaned")
                .expect("orphan projection");
            database
                .connection()
                .expect("connection")
                .execute(
                    "UPDATE ahp_projections SET created_at = ?1 WHERE event_id = ?2",
                    params![now() - 120, orphan_event_id],
                )
                .expect("age orphan projection");
            database
                .begin_delivery(NewDelivery {
                    delivery_id: Uuid::new_v4(),
                    idempotency_key: direct_delivery_key.clone(),
                    kind: "ahp_assistant_message".to_owned(),
                    session_id: None,
                })
                .expect("direct delivery");
            assert!(
                database
                    .ahp_pending_projections(10)
                    .expect("pending projections")
                    .is_empty()
            );
        }

        let recovered = Database::open(&path).expect("recovered database");
        assert_eq!(
            recovered
                .delivery_by_idempotency_key(&direct_delivery_key)
                .expect("delivery")
                .expect("direct delivery")
                .status,
            "pending"
        );
        assert!(
            recovered
                .ahp_pending_projections(10)
                .expect("pending projections")
                .is_empty()
        );
        recovered
            .recover_interrupted_state()
            .expect("daemon recovery");
        assert_eq!(
            recovered
                .delivery_by_idempotency_key(&direct_delivery_key)
                .expect("delivery")
                .expect("direct delivery")
                .status,
            "in_doubt"
        );
        let pending = recovered
            .ahp_pending_projections(10)
            .expect("recovered projections");
        assert_eq!(pending.len(), 2);
        assert!(
            pending
                .iter()
                .any(|projection| projection.event_id == event_id)
        );
        assert!(
            pending
                .iter()
                .any(|projection| projection.event_id == orphan_event_id)
        );
    }

    #[test]
    fn purge_retains_deliveries_referenced_by_pending_projections() {
        let (_directory, database) = database();
        let direct_event_id = sha256_hex(b"pending-direct-delivery");
        let replay_event_id = sha256_hex(b"pending-replay-delivery");
        for event_id in [&direct_event_id, &replay_event_id] {
            database
                .connection()
                .expect("connection")
                .execute(
                    "INSERT INTO ahp_events(
                        event_id, host_instance_id, binding_id, binding_generation,
                        session_uri, kind, occurred_at, data_redacted_json, created_at
                     ) VALUES (?1, 'host-1', 'binding-1', 1, 'copilot:/session-1',
                               'assistant_message', '2026-09-04T00:00:00Z', '{}', ?2)",
                    params![event_id, now()],
                )
                .expect("event");
            database
                .ahp_queue_projection(event_id, "ahp_assistant_message", "missed")
                .expect("projection");
        }

        let direct_key = format!("ahp-event-projection:{direct_event_id}");
        let replay_key = "ahp-offline-replay:retention";
        database
            .connection()
            .expect("connection")
            .execute(
                "UPDATE ahp_projections SET replay_key = ?1 WHERE event_id = ?2",
                params![replay_key, replay_event_id],
            )
            .expect("claim replay");
        for (key, status) in [(direct_key.as_str(), "sent"), (replay_key, "in_doubt")] {
            let delivery = database
                .begin_delivery(NewDelivery {
                    delivery_id: Uuid::new_v4(),
                    idempotency_key: key.to_owned(),
                    kind: "ahp_offline_replay".to_owned(),
                    session_id: None,
                })
                .expect("begin delivery");
            if key == replay_key {
                database
                    .record_sent_message(delivery.record.delivery_id, 1)
                    .expect("record replay send evidence");
            }
            database
                .finish_delivery(delivery.record.delivery_id, status, None, None)
                .expect("finish delivery");
        }
        database
            .connection()
            .expect("connection")
            .execute(
                "UPDATE deliveries SET updated_at = ?1
                 WHERE idempotency_key IN (?2, ?3)",
                params![now() - 172_800, direct_key, replay_key],
            )
            .expect("age deliveries");
        database
            .connection()
            .expect("connection")
            .execute(
                "UPDATE sent_message_events SET sent_at = ?1",
                [now() - 172_800],
            )
            .expect("age sent evidence");

        database.purge_expired(1).expect("purge pending references");
        assert!(
            database
                .delivery_by_idempotency_key(&direct_key)
                .expect("direct delivery")
                .is_some()
        );
        let replay_evidence: i64 = database
            .connection()
            .expect("connection")
            .query_row(
                "SELECT COUNT(*)
                 FROM sent_message_events s
                 JOIN deliveries d ON d.delivery_id = s.delivery_id
                 WHERE d.idempotency_key = ?1",
                [replay_key],
                |row| row.get(0),
            )
            .expect("replay evidence");
        assert_eq!(replay_evidence, 1);
        assert!(
            database
                .delivery_by_idempotency_key(replay_key)
                .expect("replay delivery")
                .is_some()
        );

        database
            .ahp_mark_projections_delivered(&[direct_event_id.clone(), replay_event_id.clone()])
            .expect("complete projections");
        database
            .purge_expired(1)
            .expect("purge terminal references");
        assert!(
            database
                .delivery_by_idempotency_key(&direct_key)
                .expect("direct delivery")
                .is_none()
        );
        assert!(
            database
                .delivery_by_idempotency_key(replay_key)
                .expect("replay delivery")
                .is_none()
        );
        let replay_evidence: i64 = database
            .connection()
            .expect("connection")
            .query_row("SELECT COUNT(*) FROM sent_message_events", [], |row| {
                row.get(0)
            })
            .expect("purged replay evidence");
        assert_eq!(replay_evidence, 0);
    }

    #[test]
    fn projection_with_recorded_direct_chunk_is_not_replayed() {
        let (_directory, database) = database();
        let event_id = sha256_hex(b"partially-sent-direct-projection");
        database
            .connection()
            .expect("connection")
            .execute(
                "INSERT INTO ahp_events(
                    event_id, host_instance_id, binding_id, binding_generation,
                    session_uri, kind, occurred_at, data_redacted_json, created_at
                 ) VALUES (?1, 'host-1', 'binding-1', 1, 'copilot:/session-1',
                           'assistant_message', '2026-09-04T00:00:00Z', '{}', ?2)",
                params![event_id, now()],
            )
            .expect("event");
        database
            .ahp_queue_projection(&event_id, "ahp_assistant_message", "missed")
            .expect("projection");
        database
            .ahp_projection_failed(&event_id, "qq_delivery_failed")
            .expect("projection failure");
        let delivery = database
            .begin_delivery(NewDelivery {
                delivery_id: Uuid::new_v4(),
                idempotency_key: format!("ahp-event-projection:{event_id}"),
                kind: "ahp_assistant_message".to_owned(),
                session_id: None,
            })
            .expect("begin direct delivery");
        database
            .record_sent_message(delivery.record.delivery_id, 1)
            .expect("record sent chunk");
        database
            .finish_delivery(
                delivery.record.delivery_id,
                "in_doubt",
                Some("message-1"),
                Some("qq_delivery_error"),
            )
            .expect("finish uncertain delivery");

        assert!(
            database
                .ahp_pending_projections(10)
                .expect("pending projections")
                .is_empty()
        );
        assert_eq!(
            database.ahp_status(60).expect("status").pending_projections,
            0
        );
    }

    #[test]
    fn binding_code_is_one_time_and_owner_is_immutable() {
        let (_directory, database) = database();
        let code = database.create_binding_code(600).expect("binding code");
        assert_eq!(
            database
                .try_bind_owner("owner-a", &code, "message-1")
                .expect("bind"),
            BindOutcome::Bound
        );
        assert_eq!(
            database
                .try_bind_owner("owner-b", &code, "message-2")
                .expect("second bind"),
            BindOutcome::AlreadyBound
        );
        assert_eq!(
            database.owner().expect("owner").expect("bound").user_openid,
            "owner-a"
        );
    }

    #[test]
    fn duplicate_approval_reuses_same_record() {
        let (_directory, database) = database();
        database
            .register_session("session-1", "workspace-hash")
            .expect("session");
        let approval = NewApproval {
            approval_id: Uuid::new_v4(),
            idempotency_key: "same-key".to_owned(),
            session_id: "session-1".to_owned(),
            tool_use_id: "tool-use-1".to_owned(),
            tool_name: "apply_patch".to_owned(),
            input_hash: "input-hash".to_owned(),
            redacted_summary: "summary".to_owned(),
            risk: RiskLevel::Medium,
            expires_at: now() + 600,
        };
        let first = database
            .begin_approval(approval.clone())
            .expect("first approval");
        let second = database
            .begin_approval(NewApproval {
                approval_id: Uuid::new_v4(),
                ..approval
            })
            .expect("duplicate approval");
        assert!(first.created);
        assert!(!second.created);
        assert_eq!(first.record.approval_id, second.record.approval_id);
    }

    #[test]
    fn only_first_approval_decision_wins() {
        let (_directory, database) = database();
        database
            .register_session("session-1", "workspace-hash")
            .expect("session");
        let approval = database
            .begin_approval(NewApproval {
                approval_id: Uuid::new_v4(),
                idempotency_key: "key".to_owned(),
                session_id: "session-1".to_owned(),
                tool_use_id: "tool-use-1".to_owned(),
                tool_name: "apply_patch".to_owned(),
                input_hash: "hash".to_owned(),
                redacted_summary: "summary".to_owned(),
                risk: RiskLevel::Medium,
                expires_at: now() + 600,
            })
            .expect("approval");
        let allowed = database
            .decide_approval(&approval.record.short_code, true, "message-1")
            .expect("allow")
            .expect("decision");
        let replay = database
            .decide_approval(&approval.record.short_code, false, "message-2")
            .expect("replay")
            .expect("existing");
        assert_eq!(allowed.state, ApprovalState::Allowed);
        assert_eq!(replay.state, ApprovalState::Allowed);
    }

    #[test]
    fn stop_hook_blocks_at_most_once() {
        let (_directory, database) = database();
        database
            .register_session("session-1", "workspace-hash")
            .expect("session");
        let first = database
            .stop_session("session-1", false)
            .expect("first stop");
        let second = database
            .stop_session("session-1", true)
            .expect("second stop");
        assert_eq!(first.decision, StopDecision::Block);
        assert_eq!(second.decision, StopDecision::Allow);
    }
}
