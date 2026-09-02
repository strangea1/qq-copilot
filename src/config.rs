use std::{
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use rand::{RngCore, rngs::OsRng};
use serde::{Deserialize, Deserializer, Serialize};

use crate::security::path_is_within;

const DEFAULT_CONFIG_FILE: &str = "config.toml";

#[derive(Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub bridge: BridgeConfig,
    pub qq: QqConfig,
    #[serde(default)]
    pub ahp: AhpConfig,
    #[serde(default)]
    pub policy: PolicyConfig,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct BridgeConfig {
    pub pipe_name: String,
    pub ipc_token: String,
    pub database_path: PathBuf,
    #[serde(default)]
    pub workspace_roots: Vec<PathBuf>,
    #[serde(default = "default_approval_ttl_seconds")]
    pub approval_ttl_seconds: u64,
    #[serde(default = "default_bridge_wait_seconds")]
    pub bridge_wait_seconds: u64,
    #[serde(default = "default_bind_code_ttl_seconds")]
    pub bind_code_ttl_seconds: u64,
    #[serde(default = "default_question_ttl_seconds")]
    pub question_ttl_seconds: u64,
    #[serde(default = "default_chunk_chars")]
    pub message_chunk_chars: usize,
    #[serde(default = "default_total_chars")]
    pub message_total_chars: usize,
    #[serde(default = "default_per_minute_limit")]
    pub messages_per_minute: u32,
    #[serde(default = "default_daily_limit")]
    pub messages_per_day: u32,
    #[serde(default = "default_audit_retention_days")]
    pub audit_retention_days: u32,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct QqConfig {
    pub app_id: String,
    #[serde(default)]
    pub app_secret_source: AppSecretSource,
    #[serde(default = "default_app_secret_env")]
    pub app_secret_env: String,
    #[serde(default = "default_credential_service")]
    pub credential_service: String,
    #[serde(default = "default_token_endpoint")]
    pub token_endpoint: String,
    #[serde(default = "default_api_base")]
    pub api_base: String,
    #[serde(default = "default_gateway_endpoint")]
    pub gateway_endpoint: String,
    #[serde(default = "default_intents")]
    pub intents: u64,
    #[serde(default = "default_token_refresh_skew_seconds")]
    pub token_refresh_skew_seconds: u64,
    #[serde(default)]
    pub approval_buttons_enabled: bool,
    #[serde(default)]
    pub voice_input_enabled: bool,
}

#[derive(Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AppSecretSource {
    #[default]
    CredentialManager,
    Environment,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct AhpConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(
        default,
        alias = "shared_workspace",
        deserialize_with = "deserialize_path_or_paths"
    )]
    pub shared_workspaces: Vec<PathBuf>,
    #[serde(default)]
    pub authorized_targets: Vec<AhpAuthorizedTarget>,
    #[serde(default = "default_ahp_event_retention_days")]
    pub event_retention_days: u32,
    #[serde(default = "default_ahp_command_lease_seconds")]
    pub command_lease_seconds: u64,
    #[serde(default = "default_ahp_poll_seconds")]
    pub poll_seconds: u64,
    #[serde(default = "default_ahp_stale_seconds")]
    pub adapter_stale_seconds: u64,
    #[serde(default)]
    pub adapter_auto_start: bool,
    #[serde(default)]
    pub node_executable: Option<PathBuf>,
    #[serde(default)]
    pub adapter_script: Option<PathBuf>,
    #[serde(default)]
    pub code_executable: Option<PathBuf>,
    #[serde(default)]
    pub code_launcher: Option<PathBuf>,
    #[serde(default)]
    pub ssh_executable: Option<PathBuf>,
    #[serde(default)]
    pub tool_notification_mode: AhpToolNotificationMode,
    #[serde(default = "default_typing_indicator_enabled")]
    pub typing_indicator_enabled: bool,
    #[serde(default = "default_typing_duration_seconds")]
    pub typing_duration_seconds: u8,
    #[serde(default = "default_typing_refresh_seconds")]
    pub typing_refresh_seconds: u8,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AhpToolNotificationMode {
    Full,
    #[default]
    Compact,
    ApprovalOnly,
}

impl AhpToolNotificationMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Compact => "compact",
            Self::ApprovalOnly => "approval_only",
        }
    }
}

impl std::str::FromStr for AhpToolNotificationMode {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "full" => Ok(Self::Full),
            "compact" => Ok(Self::Compact),
            "approval_only" => Ok(Self::ApprovalOnly),
            _ => Err(()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AhpAuthorizedTarget {
    Local {
        path: PathBuf,
    },
    Ssh {
        alias: String,
        path: String,
        user: String,
        host: String,
        port: u16,
        #[serde(default)]
        host_key_fingerprints: Vec<String>,
    },
}

impl Default for AhpConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            shared_workspaces: Vec::new(),
            authorized_targets: Vec::new(),
            event_retention_days: default_ahp_event_retention_days(),
            command_lease_seconds: default_ahp_command_lease_seconds(),
            poll_seconds: default_ahp_poll_seconds(),
            adapter_stale_seconds: default_ahp_stale_seconds(),
            adapter_auto_start: false,
            node_executable: None,
            adapter_script: None,
            code_executable: None,
            code_launcher: None,
            ssh_executable: None,
            tool_notification_mode: AhpToolNotificationMode::Compact,
            typing_indicator_enabled: default_typing_indicator_enabled(),
            typing_duration_seconds: default_typing_duration_seconds(),
            typing_refresh_seconds: default_typing_refresh_seconds(),
        }
    }
}

impl AhpAuthorizedTarget {
    pub fn local(path: PathBuf) -> Self {
        Self::Local { path }
    }

    pub fn display_label(&self) -> String {
        match self {
            Self::Local { .. } => "local".to_owned(),
            Self::Ssh { alias, .. } => format!("ssh:{alias}"),
        }
    }

    pub fn display_workspace(&self) -> String {
        match self {
            Self::Local { path } => path.display().to_string(),
            Self::Ssh { path, .. } => path.clone(),
        }
    }

    pub fn matches(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Local { path: left }, Self::Local { path: right }) => {
                path_is_within(left, right) && path_is_within(right, left)
            }
            (
                Self::Ssh {
                    alias: left_alias,
                    path: left_path,
                    user: left_user,
                    host: left_host,
                    port: left_port,
                    host_key_fingerprints: left_fingerprints,
                },
                Self::Ssh {
                    alias: right_alias,
                    path: right_path,
                    user: right_user,
                    host: right_host,
                    port: right_port,
                    host_key_fingerprints: right_fingerprints,
                },
            ) => {
                left_alias == right_alias
                    && left_path == right_path
                    && left_user == right_user
                    && left_host == right_host
                    && left_port == right_port
                    && left_fingerprints == right_fingerprints
            }
            _ => false,
        }
    }
}

impl AhpConfig {
    pub fn effective_authorized_targets(&self) -> Vec<AhpAuthorizedTarget> {
        let mut targets = self.authorized_targets.clone();
        for workspace in &self.shared_workspaces {
            let target = AhpAuthorizedTarget::local(workspace.clone());
            if !targets.iter().any(|configured| configured.matches(&target)) {
                targets.push(target);
            }
        }
        targets
    }

    pub fn sync_authorized_targets(&mut self) {
        let mut targets = Vec::new();
        for target in self.effective_authorized_targets() {
            if !targets
                .iter()
                .any(|configured: &AhpAuthorizedTarget| configured.matches(&target))
            {
                targets.push(target);
            }
        }
        let shared_workspaces = targets
            .iter()
            .filter_map(|target| match target {
                AhpAuthorizedTarget::Local { path } => Some(path.clone()),
                AhpAuthorizedTarget::Ssh { .. } => None,
            })
            .collect();
        self.authorized_targets = targets;
        self.shared_workspaces = shared_workspaces;
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PolicyConfig {
    #[serde(default = "default_auto_allow_tools")]
    pub auto_allow_tools: Vec<String>,
    #[serde(default = "default_approval_tools")]
    pub approval_tools: Vec<String>,
    #[serde(default = "default_command_prefixes")]
    pub approved_command_prefixes: Vec<String>,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self {
            auto_allow_tools: default_auto_allow_tools(),
            approval_tools: default_approval_tools(),
            approved_command_prefixes: default_command_prefixes(),
        }
    }
}

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("failed to read config {}", path.display()))?;
        let mut config: Self = toml::from_str(&content)
            .with_context(|| format!("failed to parse config {}", path.display()))?;
        config.ahp.sync_authorized_targets();
        config.validate()?;
        Ok(config)
    }

    pub fn write_new(path: &Path, workspace_roots: Vec<PathBuf>) -> Result<Self> {
        if path.exists() {
            bail!("refusing to overwrite existing config {}", path.display());
        }

        let parent = path
            .parent()
            .context("config path must have a parent directory")?;
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;

        let mut token_bytes = [0_u8; 32];
        OsRng.fill_bytes(&mut token_bytes);
        let mut pipe_suffix = [0_u8; 8];
        OsRng.fill_bytes(&mut pipe_suffix);

        let config = Self {
            bridge: BridgeConfig {
                pipe_name: format!("copilot-qq-bridge-{}", hex::encode(pipe_suffix)),
                ipc_token: hex::encode(token_bytes),
                database_path: parent.join("bridge.sqlite3"),
                workspace_roots,
                approval_ttl_seconds: default_approval_ttl_seconds(),
                bridge_wait_seconds: default_bridge_wait_seconds(),
                bind_code_ttl_seconds: default_bind_code_ttl_seconds(),
                question_ttl_seconds: default_question_ttl_seconds(),
                message_chunk_chars: default_chunk_chars(),
                message_total_chars: default_total_chars(),
                messages_per_minute: default_per_minute_limit(),
                messages_per_day: default_daily_limit(),
                audit_retention_days: default_audit_retention_days(),
            },
            qq: QqConfig {
                app_id: "CHANGE_ME".to_owned(),
                app_secret_source: AppSecretSource::CredentialManager,
                app_secret_env: default_app_secret_env(),
                credential_service: default_credential_service(),
                token_endpoint: default_token_endpoint(),
                api_base: default_api_base(),
                gateway_endpoint: default_gateway_endpoint(),
                intents: default_intents(),
                token_refresh_skew_seconds: default_token_refresh_skew_seconds(),
                approval_buttons_enabled: false,
                voice_input_enabled: false,
            },
            ahp: AhpConfig::default(),
            policy: PolicyConfig::default(),
        };

        let serialized = toml::to_string_pretty(&config).context("failed to serialize config")?;
        fs::write(path, serialized)
            .with_context(|| format!("failed to write config {}", path.display()))?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.bridge.pipe_name.trim().is_empty()
            || self.bridge.pipe_name.contains('\\')
            || self.bridge.pipe_name.contains('/')
        {
            bail!("bridge.pipe_name must be a non-empty pipe name, not a path");
        }
        if self.bridge.ipc_token.len() < 64 {
            bail!("bridge.ipc_token must contain at least 256 bits");
        }
        if !self.bridge.database_path.is_absolute() {
            bail!("bridge.database_path must be absolute");
        }
        if self
            .bridge
            .workspace_roots
            .iter()
            .any(|path| !path.is_absolute())
        {
            bail!("all bridge.workspace_roots entries must be absolute");
        }
        if self.bridge.approval_ttl_seconds == 0
            || self.bridge.bridge_wait_seconds == 0
            || self.bridge.bridge_wait_seconds > self.bridge.approval_ttl_seconds
        {
            bail!("bridge wait must be non-zero and no longer than approval TTL");
        }
        if self.bridge.message_chunk_chars == 0
            || self.bridge.message_total_chars < self.bridge.message_chunk_chars
        {
            bail!("message length limits are invalid");
        }
        if self.qq.intents & (1_u64 << 25) == 0 {
            bail!("qq.intents must include GROUP_AND_C2C_EVENT (1 << 25)");
        }
        if self.qq.approval_buttons_enabled && self.qq.intents & (1_u64 << 26) == 0 {
            bail!("QQ approval buttons require INTERACTION intent (1 << 26)");
        }
        if self.ahp.enabled {
            let targets = self.ahp.effective_authorized_targets();
            for target in &targets {
                match target {
                    AhpAuthorizedTarget::Local { path } => {
                        if !path.is_absolute() {
                            bail!("AHP local target paths must be absolute");
                        }
                        if !self
                            .bridge
                            .workspace_roots
                            .iter()
                            .any(|root| path_is_within(path, root))
                        {
                            bail!(
                                "AHP target workspace {} is outside bridge.workspace_roots",
                                path.display()
                            );
                        }
                    }
                    AhpAuthorizedTarget::Ssh {
                        alias,
                        path,
                        user,
                        host,
                        port,
                        host_key_fingerprints,
                    } => {
                        if alias.is_empty()
                            || alias.starts_with('-')
                            || alias.len() > 255
                            || alias.chars().any(|character| {
                                !(character.is_ascii_alphanumeric()
                                    || matches!(character, '.' | '_' | '-'))
                            })
                        {
                            bail!("AHP SSH target alias contains unsupported characters");
                        }
                        if !path.starts_with('/')
                            || path
                                .chars()
                                .any(|character| character.is_control() || character == '\0')
                        {
                            bail!("AHP SSH target path must be an absolute POSIX path");
                        }
                        if user.trim().is_empty() || host.trim().is_empty() || *port == 0 {
                            bail!("AHP SSH target identity is incomplete");
                        }
                        if host_key_fingerprints.is_empty()
                            || host_key_fingerprints.iter().any(|fingerprint| {
                                !fingerprint.starts_with("SHA256:")
                                    || fingerprint.chars().any(char::is_whitespace)
                            })
                        {
                            bail!("AHP SSH target host key fingerprints are invalid");
                        }
                    }
                }
            }
            if self.ahp.event_retention_days == 0
                || self.ahp.command_lease_seconds == 0
                || self.ahp.poll_seconds == 0
                || self.ahp.poll_seconds > 60
                || self.ahp.adapter_stale_seconds == 0
            {
                bail!("AHP retention, lease, poll, and stale intervals must be non-zero");
            }
            if self.ahp.adapter_auto_start
                && (self.ahp.node_executable.is_none()
                    || self.ahp.adapter_script.is_none()
                    || self.ahp.code_executable.is_none()
                    || self.ahp.code_launcher.is_none())
            {
                bail!(
                    "ahp.node_executable, ahp.adapter_script, ahp.code_executable, and ahp.code_launcher are required for auto-start"
                );
            }
            for path in [
                self.ahp.node_executable.as_ref(),
                self.ahp.adapter_script.as_ref(),
                self.ahp.code_executable.as_ref(),
                self.ahp.code_launcher.as_ref(),
                self.ahp.ssh_executable.as_ref(),
            ]
            .into_iter()
            .flatten()
            {
                if !path.is_absolute() {
                    bail!("AHP executable and script paths must be absolute");
                }
            }
            if targets
                .iter()
                .any(|target| matches!(target, AhpAuthorizedTarget::Ssh { .. }))
                && self.ahp.ssh_executable.is_none()
            {
                bail!("ahp.ssh_executable is required when AHP SSH targets are configured");
            }
            if self.ahp.typing_duration_seconds == 0
                || self.ahp.typing_duration_seconds > 60
                || self.ahp.typing_refresh_seconds == 0
                || self.ahp.typing_refresh_seconds >= self.ahp.typing_duration_seconds
            {
                bail!("AHP typing duration/refresh settings are invalid");
            }
        }
        Ok(())
    }

    pub fn validate_daemon(&self) -> Result<()> {
        if self.qq.app_id.trim().is_empty() || self.qq.app_id == "CHANGE_ME" {
            bail!("qq.app_id is not configured");
        }
        if self.qq.app_secret_source == AppSecretSource::Environment
            && self.qq.app_secret_env.trim().is_empty()
        {
            bail!("qq.app_secret_env must be configured for the environment source");
        }
        if self.qq.app_secret_source == AppSecretSource::CredentialManager
            && self.qq.credential_service.trim().is_empty()
        {
            bail!("qq.credential_service must not be empty");
        }
        if self.ahp.enabled && self.ahp.adapter_auto_start {
            let node = self
                .ahp
                .node_executable
                .as_ref()
                .context("AHP Node executable is not configured")?;
            let script = self
                .ahp
                .adapter_script
                .as_ref()
                .context("AHP Adapter script is not configured")?;
            if !node.is_file() {
                bail!("AHP Node executable does not exist");
            }
            if !script.is_file() {
                bail!("AHP Adapter script does not exist");
            }
            let script =
                fs::canonicalize(script).context("failed to resolve AHP Adapter script")?;
            for root in &self.bridge.workspace_roots {
                if let Ok(root) = fs::canonicalize(root)
                    && script.starts_with(root)
                {
                    bail!("AHP Adapter script must be installed outside Agent workspaces");
                }
            }
        }
        Ok(())
    }

    pub fn pipe_path(&self) -> String {
        format!(r"\\.\pipe\{}", self.bridge.pipe_name)
    }

    pub fn app_secret(&self) -> Result<String> {
        let secret = match self.qq.app_secret_source {
            AppSecretSource::CredentialManager => self
                .credential_entry()?
                .get_password()
                .context(
                    "QQ AppSecret was not found in Windows Credential Manager; run `qq-bridge store-secret` with the same --config path",
                )?,
            AppSecretSource::Environment => {
                env::var(&self.qq.app_secret_env).with_context(|| {
                    format!("environment variable {} is not set", self.qq.app_secret_env)
                })?
            }
        };
        if secret.trim().is_empty() {
            bail!("QQ AppSecret is empty");
        }
        Ok(secret)
    }

    pub fn store_app_secret(&self, secret: &str) -> Result<()> {
        if self.qq.app_secret_source != AppSecretSource::CredentialManager {
            bail!("qq.app_secret_source is not credential_manager");
        }
        if secret.trim().is_empty() {
            bail!("QQ AppSecret must not be empty");
        }
        let entry = self.credential_entry()?;
        entry
            .set_password(secret)
            .context("failed to store QQ AppSecret in Windows Credential Manager")?;
        let stored = entry
            .get_password()
            .context("QQ AppSecret could not be read back after storage")?;
        if stored != secret {
            bail!("QQ AppSecret verification failed after storage");
        }
        Ok(())
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        let mut normalized = self.clone();
        normalized.ahp.sync_authorized_targets();
        normalized.validate()?;
        let serialized =
            toml::to_string_pretty(&normalized).context("failed to serialize config")?;
        fs::write(path, serialized)
            .with_context(|| format!("failed to update config {}", path.display()))
    }

    fn credential_entry(&self) -> Result<keyring::Entry> {
        use keyring::credential::CredentialPersistence;

        let persistence = keyring::default::default_credential_builder().persistence();
        if !matches!(persistence, CredentialPersistence::UntilDelete) {
            bail!(
                "the configured credential backend is not persistent; rebuild with the keyring windows-native feature"
            );
        }
        keyring::Entry::new(&self.qq.credential_service, &self.qq.app_id)
            .context("failed to open Windows Credential Manager entry")
    }
}

pub fn default_config_path() -> Result<PathBuf> {
    if let Some(path) = env::var_os("QQ_COPILOT_CONFIG") {
        return Ok(PathBuf::from(path));
    }
    let local_app_data = env::var_os("LOCALAPPDATA").context("LOCALAPPDATA is not set")?;
    Ok(PathBuf::from(local_app_data)
        .join("CopilotQQBridge")
        .join(DEFAULT_CONFIG_FILE))
}

fn default_approval_ttl_seconds() -> u64 {
    600
}

fn default_bridge_wait_seconds() -> u64 {
    600
}

fn default_bind_code_ttl_seconds() -> u64 {
    600
}

fn default_question_ttl_seconds() -> u64 {
    600
}

fn default_chunk_chars() -> usize {
    1800
}

fn default_total_chars() -> usize {
    6000
}

fn default_per_minute_limit() -> u32 {
    20
}

fn default_daily_limit() -> u32 {
    500
}

fn default_audit_retention_days() -> u32 {
    30
}

fn default_app_secret_env() -> String {
    "QQ_BOT_APP_SECRET".to_owned()
}

fn default_credential_service() -> String {
    "qq-copilot-remote".to_owned()
}

fn default_token_endpoint() -> String {
    "https://api.bot.qq.com/app/getAppAccessToken".to_owned()
}

fn default_api_base() -> String {
    "https://api.bot.qq.com".to_owned()
}

fn default_gateway_endpoint() -> String {
    "https://api.bot.qq.com/gateway".to_owned()
}

fn default_intents() -> u64 {
    1_u64 << 25
}

fn default_token_refresh_skew_seconds() -> u64 {
    60
}

fn default_ahp_event_retention_days() -> u32 {
    30
}

fn deserialize_path_or_paths<'de, D>(deserializer: D) -> std::result::Result<Vec<PathBuf>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum PathOrPaths {
        One(PathBuf),
        Many(Vec<PathBuf>),
    }

    Ok(match PathOrPaths::deserialize(deserializer)? {
        PathOrPaths::One(path) => vec![path],
        PathOrPaths::Many(paths) => paths,
    })
}

fn default_ahp_command_lease_seconds() -> u64 {
    60
}

fn default_ahp_poll_seconds() -> u64 {
    25
}

fn default_ahp_stale_seconds() -> u64 {
    45
}

fn default_typing_indicator_enabled() -> bool {
    true
}

fn default_typing_duration_seconds() -> u8 {
    60
}

fn default_typing_refresh_seconds() -> u8 {
    45
}

fn default_auto_allow_tools() -> Vec<String> {
    [
        "view",
        "rg",
        "glob",
        "oraios-serena-find-declaration",
        "oraios-serena-find-implementations",
        "oraios-serena-find-referencing-symbols",
        "oraios-serena-find-symbol",
        "oraios-serena-get-diagnostics-for-file",
        "oraios-serena-get-symbols-overview",
        "qq_send_progress",
        "qq_send_final",
        "qq_ask_user",
        "qq_wait_for_message",
        "mcp_qq-copilot-re_qq_send_progress",
        "mcp_qq-copilot-re_qq_send_final",
        "mcp_qq-copilot-re_qq_ask_user",
        "mcp_qq-copilot-re_qq_wait_for_message",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

fn default_approval_tools() -> Vec<String> {
    [
        "apply_patch",
        "powershell",
        "web_fetch",
        "web_search",
        "oraios-serena-insert-after-symbol",
        "oraios-serena-insert-before-symbol",
        "oraios-serena-rename-symbol",
        "oraios-serena-replace-content",
        "oraios-serena-replace-in-files",
        "oraios-serena-replace-symbol-body",
        "oraios-serena-safe-delete-symbol",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

fn default_command_prefixes() -> Vec<String> {
    [
        "cargo check",
        "cargo clippy",
        "cargo fmt",
        "cargo test",
        "dotnet build",
        "dotnet test",
        "go test",
        "npm run build",
        "npm run lint",
        "npm test",
        "pnpm build",
        "pnpm lint",
        "pnpm test",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

#[cfg(test)]
mod tests {
    #[cfg(windows)]
    use std::process::Command;

    #[cfg(windows)]
    use rand::{RngCore, rngs::OsRng};

    use super::*;

    #[test]
    fn generated_config_has_strong_local_token() {
        let directory = tempfile::tempdir().expect("tempdir");
        let config_path = directory.path().join("config.toml");
        let root = directory.path().to_path_buf();

        let config = AppConfig::write_new(&config_path, vec![root.clone()]).expect("write config");

        assert_eq!(config.bridge.ipc_token.len(), 64);
        assert_eq!(config.bridge.workspace_roots, vec![root]);
        assert!(config.bridge.database_path.is_absolute());
        assert!(!config.qq.voice_input_enabled);
        AppConfig::load(&config_path).expect("reload config");
    }

    #[test]
    fn approval_only_notification_mode_round_trips() {
        let directory = tempfile::tempdir().expect("tempdir");
        let config_path = directory.path().join("config.toml");
        let mut config = AppConfig::write_new(&config_path, vec![directory.path().to_path_buf()])
            .expect("write config");
        config.ahp.tool_notification_mode = AhpToolNotificationMode::ApprovalOnly;

        config.save(&config_path).expect("save config");

        let serialized = std::fs::read_to_string(&config_path).expect("read config");
        assert!(serialized.contains("tool_notification_mode = \"approval_only\""));
        let loaded = AppConfig::load(&config_path).expect("reload config");
        assert!(loaded.ahp.tool_notification_mode == AhpToolNotificationMode::ApprovalOnly);
    }

    #[test]
    fn notification_mode_command_values_parse_and_format() {
        for mode in [
            AhpToolNotificationMode::ApprovalOnly,
            AhpToolNotificationMode::Compact,
            AhpToolNotificationMode::Full,
        ] {
            assert_eq!(mode.as_str().parse(), Ok(mode));
            assert_eq!(mode.as_str().to_ascii_uppercase().parse(), Ok(mode));
        }
        assert_eq!("unknown".parse::<AhpToolNotificationMode>(), Err(()));
    }

    #[test]
    fn loads_legacy_shared_workspace_and_migrates_on_save() {
        let directory = tempfile::tempdir().expect("tempdir");
        let config_path = directory.path().join("config.toml");
        let workspace = directory.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let mut config =
            AppConfig::write_new(&config_path, vec![workspace.clone()]).expect("new config");
        config.ahp.enabled = true;
        config.ahp.shared_workspaces = vec![workspace.clone()];

        let mut legacy = toml::Value::try_from(&config).expect("serialize config");
        let ahp = legacy
            .get_mut("ahp")
            .and_then(toml::Value::as_table_mut)
            .expect("ahp table");
        let shared_workspaces = ahp
            .remove("shared_workspaces")
            .and_then(|value| value.as_array().cloned())
            .expect("shared workspaces");
        ahp.insert(
            "shared_workspace".to_owned(),
            shared_workspaces.first().cloned().expect("workspace value"),
        );
        std::fs::write(
            &config_path,
            toml::to_string_pretty(&legacy).expect("legacy TOML"),
        )
        .expect("write legacy config");

        let loaded = AppConfig::load(&config_path).expect("load legacy config");
        assert_eq!(loaded.ahp.shared_workspaces, vec![workspace.clone()]);
        assert_eq!(
            loaded.ahp.authorized_targets,
            vec![AhpAuthorizedTarget::Local {
                path: workspace.clone()
            }]
        );
        loaded.save(&config_path).expect("save migrated config");
        let migrated = std::fs::read_to_string(&config_path).expect("read migrated config");
        assert!(migrated.contains("shared_workspaces = ["));
        assert!(migrated.contains("[[ahp.authorized_targets]]"));
        assert!(!migrated.contains("\nshared_workspace ="));
    }

    #[test]
    fn rejects_ahp_target_outside_authorized_roots() {
        let directory = tempfile::tempdir().expect("tempdir");
        let config_path = directory.path().join("config.toml");
        let allowed = directory.path().join("allowed");
        let outside = directory.path().join("outside");
        std::fs::create_dir(&allowed).expect("allowed workspace");
        std::fs::create_dir(&outside).expect("outside workspace");
        let mut config = AppConfig::write_new(&config_path, vec![allowed]).expect("new config");
        config.ahp.enabled = true;
        config.ahp.shared_workspaces = vec![outside];

        let error = config.validate().expect_err("outside target must fail");
        assert!(error.to_string().contains("outside bridge.workspace_roots"));
    }

    #[cfg(windows)]
    #[test]
    fn credential_manager_persists_across_processes() {
        const CHILD_FLAG: &str = "QQ_COPILOT_CREDENTIAL_TEST_CHILD";
        const CONFIG_ENV: &str = "QQ_COPILOT_CREDENTIAL_TEST_CONFIG";
        const HASH_ENV: &str = "QQ_COPILOT_CREDENTIAL_TEST_HASH";

        if std::env::var_os(CHILD_FLAG).is_some() {
            let config_path = std::env::var(CONFIG_ENV).expect("credential test config");
            let expected_hash = std::env::var(HASH_ENV).expect("credential test hash");
            let config =
                AppConfig::load(Path::new(&config_path)).expect("load child credential config");
            let secret = config.app_secret().expect("cross-process credential read");
            assert_eq!(
                crate::security::sha256_hex(secret.as_bytes()),
                expected_hash
            );
            return;
        }

        let persistence = keyring::default::default_credential_builder().persistence();
        assert!(matches!(
            persistence,
            keyring::credential::CredentialPersistence::UntilDelete
        ));

        let directory = tempfile::tempdir().expect("tempdir");
        let config_path = directory.path().join("config.toml");
        let workspace = directory.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let mut config =
            AppConfig::write_new(&config_path, vec![workspace]).expect("credential test config");
        config.qq.app_id = format!("credential-test-{}", uuid::Uuid::new_v4());
        std::fs::write(
            &config_path,
            toml::to_string_pretty(&config).expect("serialize credential test config"),
        )
        .expect("write credential test config");

        let mut secret_bytes = [0_u8; 32];
        OsRng.fill_bytes(&mut secret_bytes);
        let secret = hex::encode(secret_bytes);
        let expected_hash = crate::security::sha256_hex(secret.as_bytes());
        config
            .store_app_secret(&secret)
            .expect("store cross-process credential");

        let child = Command::new(std::env::current_exe().expect("current test executable"))
            .arg("config::tests::credential_manager_persists_across_processes")
            .arg("--exact")
            .env(CHILD_FLAG, "1")
            .env(CONFIG_ENV, &config_path)
            .env(HASH_ENV, expected_hash)
            .status()
            .expect("run credential reader child process");
        let cleanup = config
            .credential_entry()
            .expect("parent credential entry")
            .delete_credential();
        assert!(child.success(), "credential reader child process failed");
        cleanup.expect("remove cross-process test credential");
    }
}
